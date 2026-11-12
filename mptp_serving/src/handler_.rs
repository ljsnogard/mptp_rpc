//! 终端处理器契约：只关心业务语义。
//!
//! [`TrEndpointHandler`] 是路由链的终点。它拿到 [`MptpContext`]，写回复即可；不知道路由树、
//! 不知道中间件、不知道元数据。收尾仍由 `mptp_smux` 的 servlet 兜底。
//!
//! # `handler_`: the terminal handler contract
//!
//! The terminal handler only writes business replies. It sees the request context and knows
//! nothing about the routing tree or middleware.

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

use mptp_smux::serving::config::TrConfig;
use mptp_smux::ServeError;

use super::context_::MptpContext;

/// 一个路由目标：只做业务。
///
/// 对象安全：返回类型是擦除的 `Box<dyn Future>`，因此可以放进
/// [`alloc::sync::Arc<dyn TrEndpointHandler>`](alloc::sync::Arc)。取消传播由会话内部的
/// await 点自行处理；per-request 取消令牌在 P3 随 [`MptpContext`] 显式引入。
pub trait TrEndpointHandler<C, S = ()>: Send + Sync
where
    C: TrConfig,
{
    /// 处理一次已完成路由的请求。
    ///
    /// `'a` 是本次借用的生命周期，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，
    /// 这样调用方可以在一次 `.await` 之后重新借用 `ctx`（中间件链的关键）。
    fn handle<'a, 'f>(
        &'a self,
        ctx: &'a mut MptpContext<'f, C, S>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServeError>> + Send + 'a>>;
}
