//! 面向 [`ServingSession`] 的 RPC body 编解码辅助（新框架 handler 用）。
//!
//! 新 controller 形态的 handler 通过 `MptpContext` 拿到 `&mut ServingSession`，再用这里的
//! 辅助函数解码请求体、写回复体。业务体按 MessagePack 编解码。
//!
//! # 为什么回复体也走 ring / chunked
//!
//! 业务消息**没有字节大小上限**：denco 的编码器只需要一个可写的 `TrBuffWrite`，底层
//! buff 的容量只影响「一次能借出多少」，不影响能否编码完。旧实现为了走定长（sized）
//! 回复把整个值编码进一块 **64 KiB 的 `Vec` 暂存区**，超过就报错——那不是协议限制，
//! 是实现引入的 bug。现在回复体与请求体同构：
//!
//! - 编码器把字节写进一个很小的栈上 SPSC ring；
//! - 发送侧（`chunked_async` / `write_chunks_async`）从另一半读走并成帧；
//! - 两者在同一个 task 内 `join`，ring 只是并发窗口，**不是消息上限**。
//!
//! # `body_`: RPC body helpers over `ServingSession`
//!
//! Bodies are MessagePack. Replies stream through a small stack ring exactly like request
//! bodies: the ring is a concurrency window, never a message-size limit.

use alloc::string::{String, ToString};
use core::mem::MaybeUninit;

use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use futures::future::join;
use mp_denco::MpDecoder;
use mptp_smux::{
    ServeError,
    serving::{ServingSession, SuffixStream, config::TrConfig},
};

use crate::{
    messaging::body_::{BodyRing_, K_BODY_RING_CAP_, encode_into_async_},
    serving::types_::{RpcDataTypeInfo, typed_response},
    x_deps::{abs_cancel, abs_mptp},
};

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
    let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
    let (mut writer, mut reader) = BodyRing_::split(&mut ring);
    let encode = async {
        let result = encode_into_async_(value, &mut writer, NonCancellableToken::new()).await;
        // 无论成功失败都关闭写端：读端据此看到 EOF，不会永久等待。
        writer.close();
        result
    };
    let send = async {
        session
            .response(Status::Ok)
            .chunked_async(&mut reader)
            .await
    };
    let (encoded, sent) = join(encode, send).await;
    encoded.map_err(|err| ServeError::Target {
        detail: err.to_string(),
    })?;
    sent?;
    Result::Ok(())
}

/// 按端点元信息写一个带类型头的回复体，任意长度。
///
/// 与 [`write_ok_async`] 的区别：回复前缀由 [`typed_response`] 写全
/// （`Body_Type` / `Data_Type_Id`，以及该项所表达方向的其它头），供 `impl_of` 生成的
/// Call service dispatcher 使用——它必须按**方法级** [`RpcDataTypeInfo`] 写响应头。
///
/// # Examples
///
/// ```ignore
/// use rpc_core_v1::serving::write_typed_value_async;
/// use rpc_core_v1::x_deps::abs_mptp::spec::{Status, access_method::AccessMethod};
///
/// # async fn demo<C>(
/// #     session: &mut mptp_smux::serving::ServingSession<C>,
/// #     info: &rpc_core_v1::stream::RpcDataTypeInfo,
/// # ) -> Result<(), mptp_smux::ServeError>
/// # where C: mptp_smux::serving::config::TrConfig,
/// # {
/// write_typed_value_async(session, Status::Ok, AccessMethod::Call, info, &42u64).await
/// # }
/// ```
///
/// # Errors
///
/// 编码失败返回 [`ServeError::Target`]；写回复前缀 / 体失败返回底层错误。
pub async fn write_typed_value_async<C, T>(
    session: &mut ServingSession<C>,
    status: Status,
    method: AccessMethod,
    info: &RpcDataTypeInfo,
    value: &T,
) -> Result<(), ServeError>
where
    C: TrConfig,
    T: ?Sized + TrEncodable,
{
    let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
    let (mut writer, mut reader) = BodyRing_::split(&mut ring);
    let encode = async {
        let result = encode_into_async_(value, &mut writer, NonCancellableToken::new()).await;
        // 无论成功失败都关闭写端：读端据此看到 EOF，不会永久等待。
        writer.close();
        result
    };
    let send = async {
        typed_response(session, status, method, info)
            .chunked_async(&mut reader)
            .await
    };
    let (encoded, sent) = join(encode, send).await;
    encoded.map_err(|err| ServeError::Target {
        detail: err.to_string(),
    })?;
    sent?;
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
    session.response(status).sized_async(&mut src, len).await?;
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
    let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
    let (mut writer, mut reader) = BodyRing_::split(&mut ring);
    let encode = async {
        let result = encode_into_async_(value, &mut writer, NonCancellableToken::new()).await;
        writer.close();
        result
    };
    let send = async { stream.write_chunks_async(&mut reader).await };
    let (encoded, sent) = join(encode, send).await;
    encoded.map_err(|err| ServeError::Target {
        detail: err.to_string(),
    })?;
    sent?;
    Result::Ok(())
}
