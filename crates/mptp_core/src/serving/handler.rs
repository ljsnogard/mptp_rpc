//! Handler 与 HandlerChain。
//!
//! [`TrReqHandler`] 是单个 handler 的抽象；[`HandlerChain`] 把多个 handler
//! 串成一条链，让同一个请求有机会被按顺序、按兴趣依次处理。
//!
//! # 与客户端配置无关
//!
//! 这里所有类型都挂在 [`TrServingConfig`] 上，用的是**服务端那份**配置派生出的
//! 收发半边类型。服务端与客户端可能跑在不同进程里，两边的配置彼此独立，见
//! [`super::config`] 的模块文档。
//!
//! # FlowCtrl 语义
//!
//! - [`FlowCtrl::CallNext`]：继续交给链中下一个 handler；
//! - [`FlowCtrl::Review`]：当前 handler 只做“检视/后处理”，不生成最终回复，
//!   继续交给下一个 handler；
//! - [`FlowCtrl::SkipRest`]：停止向后传递请求；如果带 `Response`，则由上层
//!   负责把该回复写回客户端；
//! - [`FlowCtrl::Ceased`]：立即终止整个链，不再执行任何 handler。

use core::pin::Pin;
use std::{future::Future, vec::Vec};

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};

use super::{
    config::{ChannelRx, ChannelTx, Response, TrServingConfig},
    server::SessionContext,
};
use crate::{access_method::AccessMethod, specs::Headers};

/// Handler 返回的异步 future 类型。
///
/// 这里使用 `BoxFuture` 作为内部擦除后的返回类型，让 [`HandlerChain`] 可以把
/// 不同类型的 `TrReqHandler` 统一保存成 `Box<dyn TrDynReqDispatch>`。
pub type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// 给处理链条发送信号，告知 HandlerChain 打算如何处理 request 本身在链条内的流动。
///
/// 泛型参数是**响应类型**本身，而不是整个服务端配置：本枚举只关心「要不要回一个
/// 响应、回的什么样」，与连接、内存策略都无关。这样做也让它的语义可以脱离真实连接
/// 单独验证。
pub enum FlowCtrl<TyResp> {
    /// 只进行后处理，不生成最终回复；继续交给下一个 handler。
    Review,

    /// 停止向后传递 request，但会话仍有可能被此前的 handler 检视，
    /// 尤其是那些检查 Response 的 handler。
    SkipRest(Option<TyResp>),

    /// 跳出处理链条，与停止向后传递不同的是，不会再有任何 handler 处理这个会话。
    Ceased(Option<TyResp>),

    /// 继续调用链中下一个 handler。
    CallNext,
}

impl<TyResp> FlowCtrl<TyResp> {
    /// 如果该控制信号携带了一个需要由服务器写回客户端的回复，返回其引用。
    pub const fn response(&self) -> Option<&TyResp> {
        match self {
            FlowCtrl::SkipRest(resp) | FlowCtrl::Ceased(resp) => resp.as_ref(),
            FlowCtrl::Review | FlowCtrl::CallNext => Option::None,
        }
    }

    /// 判断是否应该停止继续调用后续 handler。
    pub const fn should_stop(&self) -> bool {
        matches!(self, FlowCtrl::SkipRest(_) | FlowCtrl::Ceased(_))
    }
}

impl<TyResp> core::fmt::Debug for FlowCtrl<TyResp> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FlowCtrl::Review => f.write_str("Review"),
            FlowCtrl::SkipRest(resp) => f.debug_tuple("SkipRest").field(&resp.is_some()).finish(),
            FlowCtrl::Ceased(resp) => f.debug_tuple("Ceased").field(&resp.is_some()).finish(),
            FlowCtrl::CallNext => f.write_str("CallNext"),
        }
    }
}

/// Handler 处理过程中的错误。
#[derive(Debug)]
pub enum HandlerError {
    /// IO 错误（例如读写子流失败）。
    IoError,
}

impl core::fmt::Display for HandlerError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            HandlerError::IoError => f.write_str("handler io error"),
        }
    }
}

impl core::error::Error for HandlerError {}

/// 路径级 handler：在一条子流上响应某个具体路径的任意方法请求。
///
/// 实现者应当：
///
/// 1. 根据 `method` 决定如何处理；
/// 2. 从 `rx`（子流接收半边）读取请求体 / 持续流；
/// 3. 向 `tx`（子流发送半边）写入响应头、body 或持续流。
pub trait TrReqHandler<C>
where
    C: TrServingConfig,
{
    /// 处理一次已经完成请求头解码的 MPTP 请求。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl<Response<C>>, HandlerError>>;
}

/// 内部擦除 trait：让 `HandlerChain` 可以保存任意 `TrReqHandler` 的具体类型。
trait TrDynReqDispatch<C>: Send + Sync
where
    C: TrServingConfig,
{
    /// 封装 `TrReqHandler` 的调用方法，使得可以动态分派。将会被 HandlerChain 调用。
    // 参数多是因为它必须把「请求信息 + 双向半边 + 上下文 + 令牌」原样转发给 handler；
    // 这正是擦除层存在的意义，拆成结构体只会让每个 handler 多一层解构。
    #[allow(clippy::too_many_arguments)]
    fn dispatch_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
        cancel: NonCancellableToken,
    ) -> BoxedFuture<'f, Result<FlowCtrl<Response<C>>, HandlerError>>;
}

impl<C, H> TrDynReqDispatch<C> for H
where
    C: TrServingConfig,
    H: TrReqHandler<C> + Send + Sync,
{
    fn dispatch_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
        cancel: NonCancellableToken,
    ) -> BoxedFuture<'f, Result<FlowCtrl<Response<C>>, HandlerError>> {
        Box::pin(
            // 链内部用不可取消令牌驱动单个 handler；整条链仍然可以由上层通过
            // `HandlerChain::handle_async(..).may_cancel_with(token)` 取消。
            TrReqHandler::handle_async(self, method, location, headers, tx, rx, context)
                .may_cancel_with(cancel)
                .into_future(),
        )
    }
}

/// HandlerChain 保存一个 handler 链条，被路由器匹配到的请求会进入这个 handler
/// 链条，被一个或者多个 handler 依次处理。
pub struct HandlerChain<C>
where
    C: TrServingConfig,
{
    dispatchers_: Vec<Box<dyn TrDynReqDispatch<C>>>,
}

impl<C> HandlerChain<C>
where
    C: TrServingConfig,
{
    /// 创建空链。
    pub const fn new() -> Self {
        HandlerChain {
            dispatchers_: Vec::new(),
        }
    }

    /// 在链尾追加一个 handler。
    pub fn add_handler<H>(&mut self, handler: H)
    where
        H: TrReqHandler<C> + Send + Sync + 'static,
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
    pub fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
    ) -> DispatchRequestAsync<'f, 'f, C> {
        DispatchRequestAsync::new(self, method, location, headers, tx, rx, context)
    }
}

impl<C> Default for HandlerChain<C>
where
    C: TrServingConfig,
{
    fn default() -> Self {
        HandlerChain::new()
    }
}

impl<C> TrReqHandler<C> for HandlerChain<C>
where
    C: TrServingConfig,
{
    #[inline]
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl<Response<C>>, HandlerError>> {
        HandlerChain::handle_async(self, method, location, headers, tx, rx, context)
    }
}

/// 依次驱动链上的 handler，直到某个 handler 要求停止（或整条链放行完毕）。
#[allow(clippy::too_many_arguments)]
#[gen_may_cancel_future(DispatchRequest, pub, new(pub(crate)))]
async fn dispatch_request_async_<'f, C, TyTok>(
    chain: &'f HandlerChain<C>,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: &'f mut ChannelTx<C>,
    rx: &'f mut ChannelRx<C>,
    context: &'f mut SessionContext,
    cancel: TyTok,
) -> Result<FlowCtrl<Response<C>>, HandlerError>
where
    C: TrServingConfig,
    TyTok: TrCancellationToken,
{
    for dispatcher in chain.dispatchers_.iter() {
        if cancel.is_cancelled() {
            // 已被上层取消：不再调用任何 handler，按「链终止且无回复」处理。
            return Result::Ok(FlowCtrl::Ceased(Option::None));
        }
        let ctrl = dispatcher
            .dispatch_async(
                method,
                location,
                headers,
                tx,
                rx,
                context,
                NonCancellableToken::new(),
            )
            .await?;
        if ctrl.should_stop() {
            return Result::Ok(ctrl);
        }
    }
    // 所有 handler 都放行，但没有生成最终回复。
    Result::Ok(FlowCtrl::CallNext)
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::{messaging, specs::Status};

    /// 本测试用的响应类型：空 body、空 push。
    type TyResp = messaging::Response<(), ()>;

    /// 测试 `FlowCtrl` 对「是否携带回复」「是否停止链条」这两个问题的回答。
    /// - 手段：分别构造 `CallNext` / `Review` / `SkipRest` / `Ceased` 四种取值，
    ///   其中后两者各再造一个携带 `Some` 与一个携带 `None` 的版本，然后逐个调用
    ///   `response()` 与 `should_stop()`。
    /// - 判断：`CallNext` 与 `Review` 都是「无回复且不停止」；`SkipRest` / `Ceased`
    ///   无论是否携带回复都**停止**链条；且仅当携带 `Some` 时 `response()` 才给出引用
    ///   —— 用状态码比对，确认给回的确实是那一个响应而不是凭空构造的。
    #[test]
    fn flow_ctrl_reports_response_and_stop_semantics_() {
        let call_next: FlowCtrl<TyResp> = FlowCtrl::CallNext;
        assert!(call_next.response().is_none(), "CallNext 不应携带回复");
        assert!(!call_next.should_stop(), "CallNext 不应停止链条");

        let review: FlowCtrl<TyResp> = FlowCtrl::Review;
        assert!(review.response().is_none(), "Review 不应携带回复");
        assert!(!review.should_stop(), "Review 不应停止链条");

        let skip_some: FlowCtrl<TyResp> =
            FlowCtrl::SkipRest(Option::Some(messaging::Response::new(Status::Ok)));
        assert_eq!(
            skip_some.response().map(|resp| resp.status()),
            Option::Some(Status::Ok),
            "SkipRest(Some) 应当给出所携带的那个响应"
        );
        assert!(skip_some.should_stop(), "SkipRest 应当停止链条");

        let skip_none: FlowCtrl<TyResp> = FlowCtrl::SkipRest(Option::None);
        assert!(
            skip_none.response().is_none(),
            "SkipRest(None) 不应携带回复"
        );
        assert!(skip_none.should_stop(), "SkipRest 应当停止链条");

        let ceased_some: FlowCtrl<TyResp> =
            FlowCtrl::Ceased(Option::Some(messaging::Response::new(Status::Created)));
        assert_eq!(
            ceased_some.response().map(|resp| resp.status()),
            Option::Some(Status::Created),
            "Ceased(Some) 应当给出所携带的那个响应"
        );
        assert!(ceased_some.should_stop(), "Ceased 应当停止链条");

        let ceased_none: FlowCtrl<TyResp> = FlowCtrl::Ceased(Option::None);
        assert!(
            ceased_none.response().is_none(),
            "Ceased(None) 不应携带回复"
        );
        assert!(ceased_none.should_stop(), "Ceased 应当停止链条");
    }
}
