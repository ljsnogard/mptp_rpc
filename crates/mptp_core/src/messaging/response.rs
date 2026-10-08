use abs_buff::{TrBuffRead, TrBuffWrite, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use serde::Serialize;

use crate::{decode_::read_value_async_, messaging, specs};

pub struct RespPrefix(pub specs::Status, pub Option<specs::Headers>);

/// 写入 response 的 status, headers。
///
/// 与请求前缀同一套路：先序列化到本地 `Vec` 得到长度，再完整写进发送半边。
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
        return Result::Err(std::io::Error::other(error));
    }
    let size = buf.len();
    if size == 0 {
        let err = "Zero bytes written during serialization.";
        return Result::Err(std::io::Error::other(err));
    }
    crate::encode_::write_all_async_(tx, &buf, &tok).await?;
    Result::Ok(size)
}

/// Receive and deserialize the request prefix from stream
pub(crate) async fn recv_response_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    _siz: usize, // 解析状态和头部的最大长度，超过就丢弃（当前未消费）
    tok: TyTok,
) -> Result<RespPrefix, std::io::Error>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    // 两个前缀值共用一份累积缓冲与「已消费字节数」，因此不会重复解析前面的字节。
    let mut buf: Vec<u8> = Vec::new();
    let mut consumed = 0usize;
    let status: specs::Status = read_value_async_(rx, &mut buf, &mut consumed, &tok).await?;
    let headers: Option<specs::Headers> =
        read_value_async_(rx, &mut buf, &mut consumed, &tok).await?;
    Result::Ok(RespPrefix(status, headers))
}
