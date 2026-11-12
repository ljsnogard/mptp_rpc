//! `mptp_serving`：MPTP 服务端框架——端点、元数据、中间件与分层路由。
//!
//! 本 crate 把「一次请求该交给哪个端点、端点携带什么元数据、中间件按元数据执行什么策略、
//! 终端处理器只写业务」这四件事分开。它**不**重做会话驱动：一次请求的前缀 / 体 / 后缀顺序、
//! 半关闭与收尾仍由 [`mptp_smux`] 承担；本 crate 只负责“在会话之上如何路由与分派”。
//!
//! 目标运行环境是 `#![no_std]` + `alloc`。所有堆上对象要么带分配器参数、要么进
//! [`matchit_route`] 的 `bumpalo` arena；`String` 一律用 [`abs_str::String<A>`]，
//! 不引入 `std::sync::OnceLock` / `RwLock` / `Mutex`。
//!
//! # 分层位置
//!
//! ```text
//! 应用 / RPC：service 实现、资源 handler（只写业务）
//!       ▲
//! mptp_serving：Endpoint / 元数据 / 中间件 / 分层 router（本 crate）
//!       ▲
//! mptp_smux：一次请求的会话驱动 + smux 绑定
//!       ▲
//! mptp_wire / abs_mptp / smux_v1
//! ```
//!
//! # 当前状态（round 1–6 / P0–P2 + Phase A–C）
//!
//! 已落地：`Endpoint`（含类型化 [`EndpointMetadata`]）、[`TrEndpointHandler`]（收
//! [`MptpContext`]）、[`TrMiddleware`] + [`Pipeline`]（线性“前置”中间件，按元数据短路）、
//! [`ReqHandlerTarget`]（旧 `TrReqHandler` 适配）、`RouteParams`、[`TrRouteMatcher`] 与
//! 默认实现 [`RadixRouter`]（复用 `matchit_route`，同一路径可挂多个方法不同的端点），以及：
//!
//! - 连接作用域状态 `S`（经 [`MptpContext::state`] 注入）；
//! - **请求级运行期状态** `R`（经 [`MptpContext::request_state`] /
//!   [`MptpContext::request_state_mut`] 访问，每请求 `Default` 新建）；
//! - 请求级取消句柄 [`RequestCancel`]（经 [`MptpContext::cancel`] 注入，复用 `cantok_rs`
//!   的 `Copy` 令牌）；
//! - 请求 / 回复一级 helper（[`MptpContext::headers_mut`]、
//!   [`MptpContext::response`]、[`MptpContext::req_body_async`]、
//!   [`MptpContext::req_suffix_async`]）与「是否已回复」状态
//!   [`MptpContext::reply_written`]。

#![no_std]
#![feature(impl_trait_in_assoc_type)]

extern crate alloc;

mod context_;
mod endpoint_;
mod handler_;
mod metadata_;
mod middleware_;
mod req_handler_target_;
mod route_param_;
mod route_params_;
mod router_;
mod serve_handler_;

/// 请求级取消句柄：直接复用 [`cantok_rs`] 的 `Copy` 令牌，其 `cancellation()` 返回具体
/// future 而非 `Box<dyn Future>`，因此不带 `Send` / `'static` 擦除负担。
pub use cantok_rs::CancellationToken as RequestCancel;
pub use cantok_rs::{CancellationFuture, CancellationSource};
pub use context_::MptpContext;
pub use endpoint_::Endpoint;
pub use handler_::{HandlerFuture, TrEndpointHandler, box_future_};
pub use metadata_::EndpointMetadata;
pub use middleware_::{MiddlewareFuture, Pipeline, TrMiddleware};
/// 访问方法集合：来自 [`mptp_smux`]，本 crate 不重复定义。
pub use mptp_smux::MethodSet;
pub use req_handler_target_::ReqHandlerTarget;
pub use route_param_::{RouteParamError, TrFromRouteParam};
pub use route_params_::RouteParams;
pub use router_::{MatchOutcome, RadixRouter, TrRouteMatcher};

/// 本 crate 依赖的外部 crate 转出，供下游不必逐条声明版本。
///
/// 这不是稳定的公共 API 面；下游需要时优先使用具体模块。
pub mod x_deps {
    pub use abs_cancel;
    pub use abs_mptp;
    pub use cantok_rs;
    pub use matchit_route;
    pub use mptp_smux;
}

#[cfg(test)]
mod tests_ {
    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use abs_cancel::TrCancellationToken;

    use crate::{CancellationSource, RequestCancel};

    /// 测试 re-export 的 `RequestCancel` 是 `Copy`，取消 future 与底层 source 共享信号。
    /// Test that the re-exported `RequestCancel` is `Copy` and its future shares the source.
    ///
    /// - 手段：从 source 派生令牌、断言 `Copy`、poll 取消 future 后再触发 `source.cancel()`。
    /// - 判断：取消前 future 为 `Pending`；取消后 `is_cancelled` 为 `true` 且 future `Ready`。
    ///
    /// - Means: derive a token, assert `Copy`, poll its cancellation future, then cancel.
    /// - Verdict: the future is `Pending` before cancellation and `Ready` after it.
    #[test]
    fn request_cancel_is_copy_and_shares_signal_() {
        fn assert_copy_<T: Copy>() {}
        assert_copy_::<RequestCancel<'static>>();

        fn assert_token_<T: TrCancellationToken>() {}
        assert_token_::<RequestCancel<'static>>();

        let source = CancellationSource::new();
        let token: RequestCancel<'_> = source.child_token();
        assert!(!token.is_cancelled());

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut wait = pin!(token.cancellation());
        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Pending));

        assert!(source.cancel());
        assert!(token.is_cancelled());
        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(())));
    }
}
