//! 面向 [`ServingSession`] 的 RPC body 编解码辅助（新框架 handler 用）。
//!
//! 新 controller 形态的 handler 通过 `MptpContext` 拿到 `&mut ServingSession`，再用这里的
//! 辅助函数解码请求体、写回复体。业务体按 MessagePack 编解码；回复体走定长（sized）写，
//! 与 `mptp_smux_v1_demo` 的简单路径一致——不预编码、不引入 ring/chunked 复杂度。
//!
//! # `body_`: RPC body helpers over `ServingSession`
//!
//! The new controller-style handlers reach `&mut ServingSession` through `MptpContext` and
//! use these helpers to decode the request body and write replies. Bodies are MessagePack;
//! replies use the fixed-size (sized) path, matching the demo's simple approach.

use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use mp_denco::{MpDecoder, MpEncoder};
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::{ServingSession, SuffixStream};
use mptp_smux::ServeError;

use crate::x_deps::abs_cancel::{NonCancellableToken, TrMayCancel};
use crate::x_deps::abs_mptp::spec::Status;

/// 编码暂存区容量：业务体超过它时明确失败而不是悄悄截断。
const K_ENCODE_CAP_: usize = 64usize * 1024usize;

/// 业务错误 → 状态回复的契约。
///
/// 宏生成的 handler 用 [`status`](Self::status) 决定状态码、用 [`message`](Self::message)
/// 决定错误体文本；这样业务错误能映射成具体的 4xx / 5xx，而不是一律 500。
pub trait TrReplyError {
    /// 本错误对应的状态码。
    fn status(&self) -> Status;

    /// 本错误对应的线上 message（作为 UTF-8 错误体）。
    fn message(&self) -> String;
}

/// 从请求体解码出一个 owned 值。
///
/// # Errors
///
/// 读 body 失败返回 [`ServeError::Transport`] / 协议错误；MessagePack 解码失败返回
/// [`ServeError::Target`]。
pub async fn decode_body_async<C, T>(session: &mut ServingSession<C>) -> Result<T, ServeError>
where
    C: TrConfig,
    T: TrDecodableOwned,
{
    let mut body = session.req_body_async().await?;
    T::decode_async(MpDecoder::new(&mut body))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })
}

/// 编码一个业务值并写成 `200 Ok` 回复。
///
/// # Errors
///
/// MessagePack 编码失败返回 [`ServeError::Target`]；写回复失败返回
/// [`ServeError::Transport`]。
pub async fn write_ok_async<C, T>(
    session: &mut ServingSession<C>,
    value: &T,
) -> Result<(), ServeError>
where
    C: TrConfig,
    T: ?Sized + TrEncodable,
{
    let mut bytes = encode_value_async_(value).await?;
    let len = bytes.len();
    let mut src: &mut [u8] = bytes.as_mut_slice();
    session
        .response(Status::Ok)
        .sized_async(&mut src, len)
        .await?;
    Result::Ok(())
}

/// 写一条非成功状态回复；`message` 作为 UTF-8 错误体。
///
/// # Errors
///
/// 写回复失败返回 [`ServeError::Transport`]。
pub async fn write_status_async<C>(
    session: &mut ServingSession<C>,
    status: Status,
    message: String,
) -> Result<(), ServeError>
where
    C: TrConfig,
{
    let mut bytes = message.into_bytes();
    let len = bytes.len();
    let mut src: &mut [u8] = bytes.as_mut_slice();
    session
        .response(status)
        .sized_async(&mut src, len)
        .await?;
    Result::Ok(())
}

/// 把一个业务值编码并作为后缀流的一个 chunk 写出（controller 形态 `Pull` 端点用）。
///
/// # Errors
///
/// MessagePack 编码失败返回 [`ServeError::Target`]；写 chunk 失败返回
/// [`ServeError::Transport`]。
pub async fn write_stream_item_async<'a, C, T>(
    stream: &mut SuffixStream<'a, C>,
    value: &T,
) -> Result<(), ServeError>
where
    C: TrConfig,
    T: ?Sized + TrEncodable,
{
    let bytes = encode_value_async_(value).await?;
    stream.write_chunk_async(bytes.as_slice()).await?;
    Result::Ok(())
}

/// 把业务值编码成一段内存字节。
async fn encode_value_async_<T>(value: &T) -> Result<Vec<u8>, ServeError>
where
    T: ?Sized + TrEncodable,
{
    let mut storage = vec![0u8; K_ENCODE_CAP_];
    let written = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let before = sink.len();
        value
            .encode_async(MpEncoder::new(&mut sink))
            .may_cancel_with(NonCancellableToken::new())
            .await
            .map_err(|err| ServeError::Target {
                detail: err.to_string(),
            })?;
        before - sink.len()
    };
    storage.truncate(written);
    Result::Ok(storage)
}
