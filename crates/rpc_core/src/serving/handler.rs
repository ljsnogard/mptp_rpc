//! Handler 与 HandlerChain。
//!
//! [`TrReqHandler`] 是单个 handler 的抽象；[`HandlerChain`] 把多个 handler
//! 串成一条链，让同一个请求有机会被按顺序、按兴趣依次处理。
//!
//! # FlowCtrl 语义
//!
//! - [`FlowCtrl::CallNext`]：继续交给链中下一个 handler；
//! - [`FlowCtrl::Review`]：当前 handler 只做“检视/后处理”，不生成最终回复，
//!   继续交给下一个 handler；
//! - [`FlowCtrl::SkipRest`]：停止向后传递请求；如果带 `Response`，则由上层
//!   负责把该回复写回客户端；
//! - [`FlowCtrl::Ceased`]：立即终止整个链，不再执行任何 handler。

use std::{
    future::Future,
    pin::Pin,
    vec::Vec,
};

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::{
    access_method::AccessMethod,
    messaging::Response,
    specs::Headers,
};
use super::{
    cancel_tok_::ServiceCancelToken,
    channel::ServiceChannel,
    server::SessionContext,
};

/// Handler 返回的异步 future 类型。
///
/// 这里使用 `BoxFuture` 作为内部擦除后的返回类型，让 `HandlerChain` 可以把
/// 不同类型的 `TrReqHandler` 统一保存成 `Box<dyn ErasedHandler>`。
pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// 给处理链条发送信号，告知 HandlerChain 打算如何处理 request 本身在链条内的流动。
#[derive(Debug)]
pub enum FlowCtrl {
    /// 只进行后处理，不生成最终回复；继续交给下一个 handler。
    Review,

    /// 停止向后传递 request，但会话仍有可能被此前的 handler 检视，
    /// 尤其是那些检查 Response 的 handler。
    SkipRest(Option<Response>),

    /// 跳出处理链条，与停止向后传递不同的是，不会再有任何 handler 处理这个会话。
    Ceased(Option<Response>),

    /// 继续调用链中下一个 handler。
    CallNext,
}

impl FlowCtrl {
    /// 如果该控制信号携带了一个需要由服务器写回客户端的回复，返回其引用。
    pub const fn response(&self) -> Option<&Response> {
        match self {
            FlowCtrl::SkipRest(resp) | FlowCtrl::Ceased(resp) => resp.as_ref(),
            FlowCtrl::Review | FlowCtrl::CallNext => None,
        }
    }

    /// 判断是否应该停止继续调用后续 handler。
    pub const fn should_stop(&self) -> bool {
        matches!(self, FlowCtrl::SkipRest(_) | FlowCtrl::Ceased(_))
    }
}

/// Handler 处理过程中的错误。
#[derive(Debug)]
pub enum HandlerError {
    /// IO 错误（例如读写内存 channel 失败）。
    IoError,
}

/// 路径级 handler：在一个 stream 上响应某个具体路径的任意方法请求。
///
/// 实现者应当：
///
/// 1. 根据 `method` 决定如何处理；
/// 2. 从 `channel` 的读半通道读取请求体 / 持续流；
/// 3. 向 `channel` 的写半通道写入 `Response` 头、body 或持续流。
pub trait TrReqHandler {
    /// 处理一次已经完成请求头解码的 MPTP 请求。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>>;
}

type TySvcCanTok = ServiceCancelToken;

/// 内部擦除 trait：让 `HandlerChain` 可以保存任意 `TrReqHandler` 的具体类型。
trait TrDynReqDispatch: Send + Sync {
    /// 封装 TrReqHandler 的调用方法，使得可以动态分派。将会被 HandlerChain 调用
    fn dispatch_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut SessionContext,
        cancel: &'f mut TySvcCanTok,
    ) -> BoxedFuture<'f, Result<FlowCtrl, HandlerError>>;
}

impl<H> TrDynReqDispatch for H
where
    H: TrReqHandler + Send + Sync,
{
    fn dispatch_async<'f>(
        &'f self,
        method  : AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut SessionContext,
        cancel  : &'f mut TySvcCanTok,
    ) -> BoxedFuture<'f, Result<FlowCtrl, HandlerError>> {
        Box::pin(
            // 链内部暂时使用不可取消令牌驱动单个 handler；
            // 整个 HandlerChain 仍然可以由上层通过 `may_cancel_with` 取消。
            // 这里的 cancel 类型只可能是 TySvcCanTok 但由于 Rust 不能偏特化。
            TrReqHandler::handle_async(self, method, location, headers, channel, context)
                .may_cancel_with(cancel)
                .into_future()
        )
    }
}

/// HandlerChain 保存一个 handler 链条，被路由器匹配到的请求会进入这个 handler
/// 链条，被一个或者多个 handler 依次处理。
pub struct HandlerChain {
    dispatchers_: Vec<Box<dyn TrDynReqDispatch>>,
}

impl HandlerChain {
    /// 创建空链。
    pub const fn new() -> Self {
        HandlerChain {
            dispatchers_: Vec::new(),
        }
    }

    /// 在链尾追加一个 handler。
    pub fn add_handler<H>(&mut self, handler: H)
    where
        H: TrReqHandler + Send + Sync + 'static,
    {
        self.dispatchers_.push(Box::new(handler));
    }

    /// 当前链中的 handler 数量。
    pub fn len(&self) -> usize {
        self.dispatchers_.len()
    }

    /// 链是否为空。
    pub fn is_empty(&self) -> bool {
        self.dispatchers_.is_empty()
    }

    /// 开始按顺序处理请求。
    pub const fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut SessionContext,
    ) -> DispatchRequestAsync<'f> {
        let tok = TySvcCanTok::dummy_new();
        DispatchRequestAsync(self, method, location, headers, channel, context, tok)
    }
}

impl TrReqHandler for HandlerChain {
    #[inline]
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        // Simply called the
        HandlerChain::handle_async(self, method, location, headers, channel, context)
    }
}

#[allow(clippy::too_many_arguments)]
#[gen_may_cancel_future(DispatchRequest)]
async fn dispatch_request_async_<'f, C>(
    chain: &'f HandlerChain,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    channel: &'f mut ServiceChannel,
    context: &'f mut SessionContext,
    mut cancel: TySvcCanTok,
    _dummy_: &'f mut C, // This is not used by design
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken + Clone,
{
    for dispatcher in chain.dispatchers_.iter() {
        if cancel.is_cancelled() {
            break; // consider return FlowCtrl::Ceased
        }
        let ctrl = dispatcher
            .dispatch_async(method, location, headers, channel, context, &mut cancel)
            .await?;
        if matches!(ctrl, FlowCtrl::CallNext) || matches!(ctrl, FlowCtrl::Review) {
            continue;
        } else {
            return Ok(ctrl)
        }
    };
    // 所有 handler 都放行，但没有生成最终回复。
    Ok(FlowCtrl::CallNext)
}
