//! 请求上下文：把「方法 + 位置 + 路由参数 + 会话」收拢到一个对象。
//!
//! [`MptpContext`] 对应 ASP.NET Core 的 `HttpContext`：处理器与中间件都通过它访问请求 /
//! 回复，不直接操作两个 channel 半边。会话是**借用**的（`mptp_smux` 的 servlet 拥有它），
//! 位置与路由参数借用调用方持有的 `location` 克隆，因此不会出现自引用。
//!
//! 连接状态 `S` 与请求级取消令牌属 P3，届时在这里补 `state` / `cancel` 两个字段。
//!
//! # `context_`: the per-request context
//!
//! `MptpContext` is the analogue of ASP.NET Core's `HttpContext`. The session is borrowed
//! (the servlet owns it); location and route params borrow the caller's `location` clone, so
//! there is no self-reference. Connection state and cancellation arrive in P3.

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::ServingSession;

use super::route_params_::RouteParams;

/// 一次请求的上下文。
///
/// `'f` 是请求生命周期：会话借用、位置借用与路由参数都活到 `'f`。
pub struct MptpContext<'f, C>
where
    C: TrConfig,
{
    /// 本次请求的方法。
    method_: AccessMethod,

    /// 本次请求的位置（借用调用方的克隆）。
    location_: &'f str,

    /// 零拷贝路径参数（借用调用方的克隆与路由表）。
    route_: RouteParams<'f>,

    /// 服务端会话（借用自 servlet）。
    session_: &'f mut ServingSession<C>,
}

impl<'f, C> MptpContext<'f, C>
where
    C: TrConfig,
{
    /// 由「方法 + 位置 + 路由参数 + 会话」构造。
    pub(crate) fn new_(
        method: AccessMethod,
        location: &'f str,
        route: RouteParams<'f>,
        session: &'f mut ServingSession<C>,
    ) -> Self {
        MptpContext {
            method_: method,
            location_: location,
            route_: route,
            session_: session,
        }
    }

    /// 本次请求的方法。
    pub fn method(&self) -> AccessMethod {
        self.method_
    }

    /// 本次请求的位置。
    pub fn location(&self) -> &str {
        self.location_
    }

    /// 零拷贝路径参数。
    pub fn route(&self) -> &RouteParams<'f> {
        &self.route_
    }

    /// 只读会话视图。
    pub fn session(&self) -> &ServingSession<C> {
        self.session_
    }

    /// 可变会话视图（读请求体 / 写回复）。
    pub fn session_mut(&mut self) -> &mut ServingSession<C> {
        self.session_
    }
}
