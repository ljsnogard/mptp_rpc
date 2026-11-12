//! 终端处理器契约：只关心业务语义。
//!
//! [`TrEndpointHandler`] 是路由链的终点。它拿到「方法 + 会话 + 路径参数」，写回复即可；
//! 不知道路由树、不知道中间件、不知道元数据。收尾仍由 `mptp_smux` 的 servlet 兜底。
//!
//! # `handler_`: the terminal handler contract
//!
//! The terminal handler only writes business replies. It sees the method, the session and
//! the route params, and knows nothing about the routing tree or middleware.

use alloc::boxed::Box;
use core::future::Future;
use core::pin::Pin;

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::ServingSession;
use mptp_smux::ServeError;

use super::route_params_::RouteParams;

/// 一个路由目标：只做业务。
///
/// 对象安全：返回类型是擦除的 `Box<dyn Future>`，因此可以放进
/// [`alloc::sync::Arc<dyn TrEndpointHandler>`](alloc::sync::Arc)。取消传播由会话内部的
/// await 点自行处理；per-request 取消令牌在 P3 显式引入。
pub trait TrEndpointHandler<C>: Send + Sync
where
    C: TrConfig,
{
    /// 处理一次已完成路由的请求。
    ///
    /// `params` 是零拷贝的路径参数（参数名借路由表、参数值借 `location`），只在本次
    /// future 生命周期内有效。
    fn handle<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<C>,
        params: RouteParams<'f>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ServeError>> + Send + 'f>>;
}
