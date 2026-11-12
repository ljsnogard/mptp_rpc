//! 旧 [`TrReqHandler`] 到 [`TrEndpointHandler`] 的适配器。
//!
//! 旧 handler 按值拿走两个 channel 半边并自行收尾；本适配器把 [`ServingSession::take_parts`]
//! 的产物交给它，再把其错误映射为 [`ServeError::Target`]，从而让旧 handler 无改动地挂在
//! [`RadixRouter`] 上。
//!
//! # `req_handler_target_`: adapt the legacy handler
//!
//! Wraps an old `TrReqHandler` as a `TrEndpointHandler` so legacy handlers can be mounted on
//! the new router without change.
//!
//! 注意：`TrEndpointHandler` 的 future 是 `Send` 的，因此本适配器只适用于**发送型**传输
//! （如 tokio）；`!Send` 的 compio 单线程传输需要另一个非 `Send` 的 handler 变体，属后续
//! round 的传输中立工作。

use alloc::boxed::Box;
use alloc::string::ToString;
use core::future::Future;
use core::pin::Pin;

use abs_mptp::serving::TrReqHandler;
use mptp_smux::serving::config::{ChannelRx, ChannelTx, TrConfig};
use mptp_smux::{ServeError, StateError};

use super::context_::MptpContext;
use super::handler_::TrEndpointHandler;

/// 把一个旧 [`TrReqHandler`] 包成新框架的终端处理器。
pub struct ReqHandlerTarget<H> {
    /// 被适配的旧 handler。
    handler_: H,
}

impl<H> ReqHandlerTarget<H> {
    /// 包住一个旧 handler。
    pub const fn new(handler: H) -> Self {
        ReqHandlerTarget { handler_: handler }
    }

    /// 取回被适配的 handler。
    pub const fn handler(&self) -> &H {
        &self.handler_
    }
}

impl<C, H, S> TrEndpointHandler<C, S> for ReqHandlerTarget<H>
where
    C: TrConfig + 'static,
    S: 'static,
    H: TrReqHandler<C> + Send + Sync + 'static,
    H::Err: 'static,
    ChannelTx<C>: Send,
    ChannelRx<C>: Send,
    for<'f> <<H as TrReqHandler<C>>::HandleAsync<'f> as core::future::IntoFuture>::IntoFuture:
        Send,
{
    fn handle<'a, 'f>(
        &'a self,
        ctx: &'a mut MptpContext<'f, C, S>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServeError>> + Send + 'a>> {
        let method = ctx.method();
        // 先同步取走两半，再装箱：异步块只捕获拥有所有权的两半与 handler 引用，
        // 不需要 `&mut ServingSession` 满足 `Send`。
        let Option::Some((tx, rx, mut headers, location)) = ctx.session_mut().take_parts() else {
            return Box::pin(async { Result::Err(ServeError::State(StateError::ChannelTaken)) });
        };

        Box::pin(async move {
            self.handler_
                .handle_async(method, &location, &mut headers, tx, rx)
                .await
                .map_err(|err| ServeError::Target {
                    detail: err.to_string(),
                })
        })
    }
}
