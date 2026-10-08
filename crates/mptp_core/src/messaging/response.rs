use std::{mem::MaybeUninit, slice};

use serde::Serialize;

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrConsumerState},
    x_deps::abs_cancel,
};
use abs_buff_stdio_adapt::{AsStdRead, x_deps::abs_buff};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::{
    messaging,
    specs,
    std_io_adapt_::StdReadAdapter,
};

pub struct RespPrefix(pub specs::Status, pub Option<specs::Headers>);

/// 写入 response 的 status, headers。
/// 内部使用一个 Vec 来计算写了多少字节
// 服务端回写路径（`serving`）当前被暂时摘除，本函数暂时无人使用；等 `serving` 按
// `abs_smux` 的 channel 重做回来即可移除本 allow。
#[allow(dead_code)]
pub(crate) async fn send_response_prefix_async<'f, TyResp, TyTx, TyTok>(
    resp: &'f TyResp,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, std::io::Error>
where
    TyResp: messaging::TrRpcResponse,
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    fn serialize_to<Resp: messaging::TrRpcResponse>(
        resp: &Resp,
        buff: &mut Vec<u8>,
    ) -> Result<(), rmp_serde::encode::Error> {
        let mut serializer = rmp_serde::Serializer::new(buff);
        resp.status().serialize(&mut serializer)?;
        resp.headers().serialize(&mut serializer)?;
        Result::Ok(())
    }

    let mut buf = Vec::new();
    if let Result::Err(error) = serialize_to(resp, &mut buf) {
        return Result::Err(std::io::Error::other(error))
    };
    let size = buf.len();
    if size == 0 {
        let err = "Zero bytes written during serialization.";
        return Result::Err(std::io::Error::other(err));
    }
    let demand = Demand::less_than(size);
    let mut opt_segm = tx
        .write_async(&demand)
        .may_cancel_with(tok)
        .await;
    if let Option::Some(segm) = opt_segm.as_mut().pick_left() {
        // SAFETY: `buf` 是本函数独占的 `Vec<u8>`，其前 `size` 个字节已初始化；
        // `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无 niche），因此按
        // `size` 长度把这段已初始化内存重新解释为 `MaybeUninit<u8>` 切片是健全的。
        let buff = unsafe {
            let p = buf.as_mut_ptr() as *mut MaybeUninit<u8>;
            slice::from_raw_parts_mut(p, size)
        };
        let sent_size = segm.move_items_from_buff(buff);
        return Result::Ok(sent_size);
    }
    if let Option::Some(err) = opt_segm.pick_right() {
        let err = err.to_string();
        return Result::Err(std::io::Error::other(err));
    }
    Result::Ok(0usize)
}

/// Receive and deserialize the request prefix from stream
pub(crate) async fn recv_response_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    _siz: usize, // 解析状态和头部的最大长度，超过就丢弃（当前未消费）
    tok: TyTok,
) -> Result<RespPrefix, std::io::Error>
where
    TyRx: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    fn deserialize_prefix<R, C>(
        r: &mut R,
        c: C,
    ) -> Result<RespPrefix, rmp_serde::decode::Error>
    where
        R: TrBuffRead<u8> + TrConsumerState,
        C: TrCancellationToken,
    {
        // TODO(重构): 这里用 `AsStdRead` 解码被标注为有 bug（历史遗留），待解码路径
        // 重做时一并处理。
        // `AsStdRead` 按值持有令牌；再包一层转发，就不必要求令牌可克隆。
        let mut std_read = StdReadAdapter::new_(AsStdRead::new(r, c));
        let des_status = rmp_serde::from_read::<_, specs::Status>(&mut std_read);
        let status = match des_status {
            Result::Err(err) => return Result::Err(err),
            Result::Ok(m) => m,
        };
        let des_headers = rmp_serde::from_read::<_, Option<specs::Headers>>(&mut std_read);
        let headers = match des_headers {
            Result::Err(err) => return Result::Err(err),
            Result::Ok(m) => m,
        };
        Result::Ok(RespPrefix(status, headers))
    }

    deserialize_prefix(rx, tok).map_err(std::io::Error::other)
}
