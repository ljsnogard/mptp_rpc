//! 客户端的会话对象。
//!
//! 一个 [`Session`] 围绕 `abs_smux` 概念里的一条 channel 进行：它持有该子流的收发
//! 半边（`accept_async` 的产物），因此调用者可以在同一子上持续交互（一问一答，或
//! 推送 / 拉取到推流结束）。

use abs_buff::{buffer::TrConsumerState, gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;

use super::{
    ClientError,
    config::{self, ChannelRx, ChannelTx, RespPrefix, TrClientConfig, TrSession},
};
use crate::messaging;

/// 一次请求/回复过程（一问一答）的会话。
///
/// 字段不对外公开：调用者只能通过 [`TrSession`] 的契约驱动它。
pub struct Session<C>
where
    C: TrClientConfig,
{
    /// 本会话对应的请求。按值持有：请求体的读取 / 推送都要回到它（按 `Body_Size`
    /// 头声明的长度）。当前尚未使用，先随会话一起保管。
    request_: Option<config::Request<C>>,

    /// 子流的发送半边。
    ///
    /// TODO(重构): 请求体（推送 / 流式上传）的写路径尚未落地，因此本字段暂时只被
    /// 保管、未被读取；等推送路径落地后即可移除本 allow。
    #[allow(dead_code)]
    tx_: ChannelTx<C>,

    /// 子流的接收半边。
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
    // 读取响应前缀要走 `AsStdRead`，它要求接收半边能报告「已耗尽 / 对端已关闭」。
    ChannelRx<C>: TrConsumerState,
{
    type RecvRespAsync<'f>
        = SessionRecvRespAsync<'f, 'f, C>
    where
        Self: 'f;

    fn recv_response_async<'f>(&'f mut self) -> Self::RecvRespAsync<'f> {
        /// 解析响应状态与头部的最大长度，超过就丢弃。
        const MAX_PREFIX_LEN: usize = 1024usize;
        SessionRecvRespAsync::new(self, MAX_PREFIX_LEN)
    }
}

/// 读取响应前缀（状态码 + 头）。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的
// `'f` 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvResp, pub, new(pub(crate)))]
async fn session_recv_resp_async_<'f, C, TyTok>(
    session: &'f mut Session<C>,
    max_len: usize,
    cancel: TyTok,
) -> Result<RespPrefix, ClientError>
where
    C: TrClientConfig,
    ChannelRx<C>: TrConsumerState,
    TyTok: TrCancellationToken,
{
    let recv_res =
        messaging::response::recv_response_prefix_async(&mut session.rx_, max_len, cancel).await;
    match recv_res {
        Result::Err(err) => {
            let info = format!("Error ({err}) in receiving response");
            Result::Err(ClientError::RespErr(info))
        }
        Result::Ok(prefix) => Result::Ok(prefix),
    }
}
