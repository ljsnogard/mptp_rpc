//! 请求上下文：把「方法 + 位置 + 路由参数 + 会话 + 连接状态」收拢到一个对象。
//!
//! [`MptpContext`] 对应 ASP.NET Core 的 `HttpContext`：处理器与中间件都通过它访问请求 /
//! 回复，不直接操作两个 channel 半边。会话是**借用**的（`mptp_smux` 的 servlet 拥有它），
//! 位置与路由参数借用调用方持有的 `location` 克隆，因此不会出现自引用。
//!
//! 连接状态 `S` 是**每连接一份**的共享状态（join 身份、订阅表、保活……），由连接对象持有，
//! 处理器 / 中间件只拿 `&S`（共享借用）；需要可变性时 `S` 内部用 `RefCell` / 原子。
//!
//! 请求级取消令牌属 P3 的后续部分，届时在这里补 `cancel` 字段。
//!
//! # `context_`: the per-request context
//!
//! `MptpContext` is the analogue of ASP.NET Core's `HttpContext`. The session is borrowed
//! (the servlet owns it); location and route params borrow the caller's `location` clone, so
//! there is no self-reference. `S` is per-connection state owned by the connection object,
//! handed to handlers/middleware as a shared `&S`.

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::ServingSession;

use super::route_params_::RouteParams;

/// 一次请求的上下文。
///
/// `'f` 是请求生命周期：会话借用、位置借用、路由参数与连接状态借用都活到 `'f`。`S` 是
/// 连接作用域状态，缺省为 `()`。
pub struct MptpContext<'f, C, S = ()>
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

    /// 连接作用域状态（共享借用）。
    state_: &'f S,
}

impl<'f, C, S> MptpContext<'f, C, S>
where
    C: TrConfig,
{
    /// 由「方法 + 位置 + 路由参数 + 会话 + 状态」构造。
    pub(crate) fn new_(
        method: AccessMethod,
        location: &'f str,
        route: RouteParams<'f>,
        session: &'f mut ServingSession<C>,
        state: &'f S,
    ) -> Self {
        MptpContext {
            method_: method,
            location_: location,
            route_: route,
            session_: session,
            state_: state,
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

    /// 请求头表（只读）。
    ///
    /// 处理器 / 中间件经它读取请求头（例如 `#[from_header]` 参数来源）；需要改写头表时用
    /// [`session_mut`](Self::session_mut)。
    pub fn headers(&self) -> &mptp_smux::Headers {
        self.session_.headers()
    }

    /// 只读会话视图。
    pub fn session(&self) -> &ServingSession<C> {
        self.session_
    }

    /// 可变会话视图（读请求体 / 写回复）。
    pub fn session_mut(&mut self) -> &mut ServingSession<C> {
        self.session_
    }

    /// 连接作用域状态（共享借用）。
    pub fn state(&self) -> &S {
        self.state_
    }
}
