//! 类型化 Pull / Push 的客户端入口。
//!
//! 这两个 helper 把「方法 + 路径 + 端点类型元信息」落到线上头表，再返回
//! [`TypedJustFlux`] / [`TypedSlotFlux`]。它们只依赖 `mptp_smux` 的公开会话 API，
//! 不把 denco / 业务类型压进传输层。
//!
//! # `typed_`: typed Pull / Push client entries
//!
//! The helpers write the endpoint type metadata onto the request prefix and hand back a
//! typed flux. They only use the public `mptp_smux` session API; the transport stays
//! byte-only.

use alloc::{format, string::ToString};

use abs_buff::{TrBuffRead, buffer::TrConsumerState};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mptp::{
    client::TrMptpClient,
    spec::{Status, access_method::AccessMethod},
};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::client::{MptpClient, RequestSuffixStream, Response, config::ChannelRx};
use mptp_wire::{Headers, RequestPrefix};

use crate::{
    client::TrRpcClientConfig,
    messaging::{RpcError, RpcResult, body_::read_body_async_},
    stream::{
        AcceptType, RpcDataTypeInfo, TypedJustFlux, TypedSlotFlux, apply_request_type_headers,
        validate_response_types, write_accept,
    },
    x_deps::{abs_buff, abs_cancel, abs_denco, abs_mptp, gen_mcf2, mptp_smux, mptp_wire},
};

/// 打开一条类型化 Pull 流。
///
/// 请求头写 `Body_Type`（suffix codec）与 `Accept`（期望的 item codec + 类型 ID）；
/// 回复为 `200` 时交出拥有接收半边的 [`TypedJustFlux`]，否则把错误体读成
/// [`RpcError::Remote`]。
///
/// # Errors
///
/// 建流 / 发前缀失败、远端非 `200`、回复前缀解码失败、排空回复体失败都返回
/// [`RpcError`]；具体变体与 [`call_endpoint_async`](crate::client::call_endpoint_async)
/// 一致。响应前缀的 `Body_Type` / `Data_Type_Id` 与请求的 `Accept` 不一致时返回
/// [`RpcError::Decode`]（对端违反类型契约），不会把错误类型的 suffix 交给 flux。
#[allow(clippy::needless_lifetimes)]
pub fn open_pull_async<'f, C, T>(
    client: &'f MptpClient<C>,
    path: &'static str,
    info: RpcDataTypeInfo,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<TypedJustFlux<'static, ChannelRx<C>, T>>> + 'f
where
    C: TrRpcClientConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState + 'static,
    T: TrDecodableOwned + 'static,
{
    OpenPullAsync::new(client, path, info)
}

/// [`open_pull_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(OpenPull, new(pub(crate)))]
async fn open_pull_async_<'f, C, T, K>(
    client: &'f MptpClient<C>,
    path: &'static str,
    info: RpcDataTypeInfo,
    cancel: K,
) -> RpcResult<TypedJustFlux<'static, ChannelRx<C>, T>>
where
    C: TrRpcClientConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState + 'static,
    T: TrDecodableOwned + 'static,
    K: TrCancellationToken,
{
    let accept = info
        .accept()
        .unwrap_or_else(|| AcceptType::new(info.body_codec(), info.response_suffix()));
    let mut headers = Headers::new();
    apply_request_type_headers(&mut headers, AccessMethod::Pull, &info);
    write_accept(&mut headers, accept);
    let mut builder = client.request(AccessMethod::Pull, path);
    for (key, val) in headers.iter() {
        builder = builder.header(key.clone(), val.clone());
    }
    let mut response = builder
        .send_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("发起 Pull {path} 失败：{err}")))?;
    ensure_ok_async_(&mut response, path, cancel.child_token()).await?;

    // 从响应前缀核对 suffix codec 与 item 类型：对端返回 200 不代表类型契约成立。
    {
        let resp_headers = response
            .prefix()
            .headers()
            .cloned()
            .unwrap_or_default();
        if let Result::Err(rejection) =
            validate_response_types(AccessMethod::Pull, &resp_headers, &info)
        {
            return Result::Err(RpcError::Decode(format!(
                "Pull {path} 响应类型与请求的 Accept 不一致：{rejection}"
            )));
        }
    }

    let reader = response
        .into_resp_suffix_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("取 {path} 回复后缀失败：{err}")))?;
    Result::Ok(TypedJustFlux::new(reader))
}

/// 打开一条类型化有限 Push 流。
///
/// 请求头写 `Body_Type`（suffix codec）与 `Data_Type_Id`（请求 suffix item 类型）；
/// 请求**没有 body**，调用者通过 `TypedSlotFlux::pull_async` 逐槽
/// [`Slot::set_async`](crate::stream::Slot::set_async) 写入，最后
/// [`SlotIter::finish_async`](crate::stream::SlotIter::finish_async) 半关闭请求并取回复。
///
/// # Errors
///
/// 元信息没有声明请求 suffix 类型时返回 [`RpcError::Unsupported`]；建流 / 发前缀失败
/// 返回 [`RpcError::Transport`]。
#[allow(clippy::needless_lifetimes)]
pub fn open_push_async<'f, C, T>(
    client: &'f MptpClient<C>,
    path: &'static str,
    info: RpcDataTypeInfo,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<TypedSlotFlux<'static, RequestSuffixStream<C>, T>>>
+ 'f
where
    C: TrRpcClientConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState + 'static,
    T: TrEncodable + 'static,
{
    OpenPushAsync::new(client, path, info)
}

/// [`open_push_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(OpenPush, new(pub(crate)))]
async fn open_push_async_<'f, C, T, K>(
    client: &'f MptpClient<C>,
    path: &'static str,
    info: RpcDataTypeInfo,
    cancel: K,
) -> RpcResult<TypedSlotFlux<'static, RequestSuffixStream<C>, T>>
where
    C: TrRpcClientConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState + 'static,
    T: TrEncodable + 'static,
    K: TrCancellationToken,
{
    let Some(_id) = info.request_suffix() else {
        return Result::Err(RpcError::Unsupported(format!(
            "Push {path} 的端点元信息没有声明 request_suffix 类型 ID"
        )));
    };
    let mut headers = Headers::new();
    apply_request_type_headers(&mut headers, AccessMethod::Push, &info);
    let prefix =
        RequestPrefix::with_headers(AccessMethod::Push, path.to_string(), Option::Some(headers));

    let session = client
        .request_async(prefix)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("发起 Push {path} 失败：{err}")))?;
    let stream = RequestSuffixStream::new(session)
        .map_err(|err| RpcError::Transport(format!("打开 {path} 请求后缀写端失败：{err}")))?;
    Result::Ok(TypedSlotFlux::new(stream))
}

/// 非 `200` 时读出错误体并转成 [`RpcError::Remote`]。
async fn ensure_ok_async_<C, K>(
    response: &mut Response<C>,
    path: &'static str,
    cancel: K,
) -> RpcResult<()>
where
    C: TrRpcClientConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    if response.status() == Status::Ok {
        return Result::Ok(());
    }
    let status = response.status();
    let size = response
        .headers()
        .and_then(Headers::body_size)
        .unwrap_or(0usize);
    let mut body = response
        .body_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("取 {path} 错误体视图失败：{err}")))?;
    let body = read_body_async_(&mut body, size, cancel.child_token()).await?;
    Result::Err(RpcError::Remote { status, body })
}
