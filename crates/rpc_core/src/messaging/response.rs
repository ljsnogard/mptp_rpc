use std::{mem::MaybeUninit, slice};

use serde::Serialize;

use abs_buff::{
    Demand, TrBuffTryRead, TrBuffTryWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView},
};
use abs_buff_stdio_adapt::AsStdRead;
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::{
    messaging,
    specs,
};

pub struct RespPrefix(pub specs::Status, pub Option<specs::Headers>);

/// 写入 response 的 status, headers。
/// 内部使用一个 Vec 来计算写了多少字节
pub(crate) async fn send_response_prefix_async<'f, TyResp, TyTx, TyTok>(
    resp: &'f TyResp,
    tx: &'f mut TyTx,
    tok: &'f mut TyTok,
) -> Result<usize, std::io::Error>
where
    TyResp: messaging::TrRpcResponse,
    TyTx: TrBuffTryWrite,
    TyTok: TrCancellationToken + Clone,
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
    let mut opt_segm = tx
        .write_async(&Demand::less_than(size))
        .may_cancel_with(tok)
        .await;
    if let Option::Some(segm) = opt_segm.as_mut().pick_left() {
        let buff = unsafe {
            let p = buf.as_mut_ptr() as *mut MaybeUninit<u8>;
            slice::from_raw_parts_mut(p, size)
        };
        let sent_size = unsafe { segm.move_items_from_buff(buff) };
        return Result::Ok(sent_size);
    }
    if let Option::Some(err) = opt_segm.pick_right() {
        let err = err.to_string();
        return Result::Err(std::io::Error::other(err));
    }
    Result::Ok(0usize)
}

/// Receive and deserialize the request prefix from stream
pub(crate) async fn recv_response_prefix_async<'f, TyRx, TyTok>(
    rx: &'f mut TyRx,
    siz: usize, // 解析状态和头部的最大长度，超过就丢弃
    tok: &'f mut TyTok,
) -> Result<RespPrefix, std::io::Error>
where
    TyRx: TrBuffTryRead,
    TyTok: TrCancellationToken + Clone,
{
    fn deserialize_prefix<R, C>(
        r: &mut R,
        c: &mut C,
    ) -> Result<RespPrefix, rmp_serde::decode::Error>
    where
        R: TrBuffTryRead,
        C: TrCancellationToken + Clone,
    {
        // FIXME: decoding using AsStdRead is buggy.
        let mut std_read = AsStdRead::new(r, c);
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
