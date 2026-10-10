//! 客户端的会话对象。
//!
//! 一个 [`Session`] 围绕 `abs_smux` 概念里的一条 channel 进行：它持有该子流的收发
//! 半边（`accept_async` 的产物），因此调用者可以在同一条子流上持续交互（一问一答，
//! 或推送 / 拉取到推流结束）。
//!
//! 会话的每次读取都是 `async` 的。同步只发生在 `serde` 与 `AsStdRead` 那一层——它们
//! 是本框架唯一允许暂时用同步代码的地方，这份妥协不外溢成调用方的语义。

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use serde::de::DeserializeOwned;

use super::{
    ClientError,
    config::{self, ChannelRx, ChannelTx, RespPrefix, TrClientConfig, TrSession},
};
use crate::messaging::{self, ResponseBodyDecision, TrRpcRequest};

/// 一次请求/回复过程（一问一答）的会话。
///
/// 字段不对外公开：调用者只能通过 [`TrSession`] 的契约驱动它。
pub struct Session<C>
where
    C: TrClientConfig,
{
    /// 本会话对应的请求。按值持有：读回复体时要回到它，看请求用的是哪个 access
    /// method——`Head` / `Drop` 的回复按协议不带本体内容。
    request_: Option<config::Request<C>>,

    /// 子流的发送半边。
    ///
    /// TODO(重构): 请求体（推送 / 流式上传）的写路径尚未落地，因此本字段暂时只被
    /// 保管、未被读取；等推送路径落地后即可移除本 allow。
    #[allow(dead_code)]
    tx_: ChannelTx<C>,

    /// 子流的接收半边。读回复时直接接 `AsStdRead`，中间不留缓冲。
    rx_: ChannelRx<C>,
}

impl<C> Session<C>
where
    C: TrClientConfig,
{
    /// 由「请求 + 该子流的收发半边」构造。
    ///
    /// 只允许客户端实现（[`super::client_`]）在最终裁决成功之后调用。
    pub(crate) const fn new(
        request: config::Request<C>,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> Self {
        Session {
            request_: Option::Some(request),
            tx_: tx,
            rx_: rx,
        }
    }

    /// 取回本会话对应的请求。
    pub const fn request(&self) -> Option<&config::Request<C>> {
        self.request_.as_ref()
    }
}

impl<C> TrSession<C> for Session<C>
where
    C: TrClientConfig,
{
    type RecvRespHeaderAsync<'f>
        = SessionRecvRespAsync<'f, 'f, C>
    where
        Self: 'f;

    fn recv_resp_header_async<'f>(&'f mut self) -> Self::RecvRespHeaderAsync<'f> {
        SessionRecvRespAsync::new(self)
    }

    type RecvRespBodyAsync<'f, T>
        = SessionRecvRespBodyAsync<'f, 'f, C, T>
    where
        Self: 'f,
        T: 'f + DeserializeOwned + 'static;

    fn recv_resp_body_async<'f, T>(
        &'f mut self,
        prefix: &'f RespPrefix,
    ) -> Self::RecvRespBodyAsync<'f, T>
    where
        T: DeserializeOwned + 'static,
    {
        SessionRecvRespBodyAsync::new(self, prefix)
    }
}

/// 读取响应前缀（状态码 + 头）。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的
// `'f` 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvResp, pub, new(pub(crate)))]
async fn session_recv_resp_async_<'f, C, TyTok>(
    session: &'f mut Session<C>,
    cancel: TyTok,
) -> Result<RespPrefix, ClientError>
where
    C: TrClientConfig,
    TyTok: TrCancellationToken,
{
    messaging::response::recv_response_prefix_async(&mut session.rx_, cancel)
        .await
        .map_err(|err| ClientError::RespErr(format!("接收响应前缀失败：{err}")))
}

/// 读取本次回复的报文体，并解成一个业务类型。
///
/// 先按协议决策边界，再按边界读：没有体的回复不会碰接收半边分毫；解码直接发生在 ring
/// 的接收半边上，中间没有中转缓冲。
// `gen_may_cancel_future` 要求显式生命周期，理由同 `session_recv_resp_async_`。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvRespBody, pub, new(pub(crate)))]
async fn session_recv_resp_body_async_<'f, C, T, TyTok>(
    session: &'f mut Session<C>,
    prefix: &'f RespPrefix,
    cancel: TyTok,
) -> Result<Option<T>, ClientError>
where
    C: TrClientConfig,
    T: DeserializeOwned + 'static,
    TyTok: TrCancellationToken,
{
    let Option::Some(request) = session.request_.as_ref() else {
        return Result::Err(ClientError::RespErr(
            "会话已经不认识自己的请求，无法判定回复体边界".to_string(),
        ));
    };
    // 边界先由协议判一次：`Head` / `Drop` 带体、只有 `Body_Type` 没有 `Body_Size`
    // 都属违规，这里宁可当场失败，也不要带着错位的流继续走。
    let decision = ResponseBodyDecision::decide(request.method(), prefix)
        .map_err(|err| ClientError::RespErr(err.to_string()))?;
    if decision == ResponseBodyDecision::Absent {
        // 没有体：**一个字节都不读**，接收半边上后续数据的对齐因此不受影响。
        return Result::Ok(Option::None);
    }
    messaging::response::recv_response_body_async(&mut session.rx_, prefix, cancel)
        .await
        .map_err(|err| ClientError::RespErr(format!("接收响应体失败：{err}")))
}
