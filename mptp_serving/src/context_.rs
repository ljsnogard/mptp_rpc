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

use abs_buff::{TrBuffRead, buffer::TrConsumerState};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use bumpalo::Bump;
use mptp_smux::serving::{
    RequestBodyAsync, RequestSuffixAsync, ResponseBuilder, ServingSession,
    config::{ChannelRx, TrConfig},
};

use super::{RequestCancel, route_params_::RouteParams};

/// 一次请求的上下文。
///
/// `'f` 是请求生命周期：会话借用、位置借用、路由参数、连接状态与请求级状态借用都活到
/// `'f`。`S` 是连接作用域状态，`R` 是**请求级运行期状态**（中间件写、处理器读），两者
/// 缺省都是 `()`。
///
/// `R` 与端点元数据分工明确：元数据是**装配期声明**（鉴权策略、超时……），`R` 是**本请求
/// 的运行期事实**（已通过鉴权、主体是谁、配额余量……）。它由装配者定义，框架不解释它。
pub struct MptpContext<'f, C, S = (), R = ()>
where
    C: TrConfig,
{
    /// 本次请求的方法。
    method_: AccessMethod,

    /// 请求级 arena（借用自请求作用域）：handler future 从它分配。
    arena_: &'f Bump,

    /// 本次请求的位置（借用调用方的克隆）。
    location_: &'f str,

    /// 零拷贝路径参数（借用调用方的克隆与路由表）。
    route_: RouteParams<'f>,

    /// 服务端会话（借用自 servlet）。
    session_: &'f mut ServingSession<C>,

    /// 连接作用域状态（共享借用）。
    state_: &'f S,

    /// 请求级运行期状态（可变借用）：中间件 stash、处理器读。
    request_state_: &'f mut R,

    /// 请求级取消句柄（`Copy` 令牌，借用连接持有的取消源）。
    cancel_: RequestCancel<'f>,
}

impl<'f, C, S, R> MptpContext<'f, C, S, R>
where
    C: TrConfig,
{
    /// 由「方法 + 位置 + 路由参数 + 会话 + 连接状态 + 请求级状态 + 取消句柄」构造。
    ///
    // 这是 crate 内唯一的装配点：全部输入就是一次请求的全部借用，拆成结构体只会把同一组
    // 借用再包一层，因此显式允许超参数。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_(
        arena: &'f Bump,
        method: AccessMethod,
        location: &'f str,
        route: RouteParams<'f>,
        session: &'f mut ServingSession<C>,
        state: &'f S,
        request_state: &'f mut R,
        cancel: RequestCancel<'f>,
    ) -> Self {
        MptpContext {
            method_: method,
            arena_: arena,
            location_: location,
            route_: route,
            session_: session,
            state_: state,
            request_state_: request_state,
            cancel_: cancel,
        }
    }

    /// 本次请求的方法。
    pub fn method(&self) -> AccessMethod {
        self.method_
    }

    /// 请求级 arena 句柄（`&'f Bump`，`Copy`）。
    ///
    /// 终端处理器用它把 future 分配进 arena：先取出句柄、再构造 future、最后经
    /// [`crate::handler_::box_future_`] 装箱。
    #[doc(hidden)]
    pub fn arena(&self) -> &'f Bump {
        self.arena_
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
    /// [`headers_mut`](Self::headers_mut)。
    pub fn headers(&self) -> &mptp_smux::Headers {
        self.session_.headers()
    }

    /// 请求头表（可变）。
    ///
    /// 改写的是会话持有的请求头副本，**不**写回已经解码好的请求前缀；因此可以用它做
    /// 「中间件把规范化后的头交给后续处理器」这类请求内改写。
    pub fn headers_mut(&mut self) -> &mut mptp_smux::Headers {
        self.session_.headers_mut()
    }

    /// 本次请求是否已经写出回复前缀（即回复已经开始）。
    ///
    /// 供中间件 / 收尾逻辑判断「处理器是否真的回复了」：返回 `false` 说明还来得及写兜底
    /// 回复（Phase E 的兜底 500 依赖它）。判定复用会话的只读查询
    /// [`ServingSession::sent_response_prefix`]，装配期零成本。
    pub fn reply_written(&self) -> bool {
        self.session_.sent_response_prefix().is_some()
    }

    /// 只读会话视图。
    pub fn session(&self) -> &ServingSession<C> {
        self.session_
    }

    /// 可变会话视图（读请求体 / 写回复）。
    ///
    /// 这是逃逸口：多数处理器应优先用 [`req_body_async`](Self::req_body_async) /
    /// [`req_suffix_async`](Self::req_suffix_async) / [`response`](Self::response) 这些
    /// 一级 helper，只有需要会话层面的操作（如 `finish_async`、`take_parts`）时才用它。
    pub fn session_mut(&mut self) -> &mut ServingSession<C> {
        self.session_
    }

    /// 按请求前缀声明的体边界取请求体读视图。
    ///
    /// 委托 [`ServingSession::req_body_async`]：返回的 future 可 `.await` 得到
    /// `RequestBodyRead`，也可经 `may_cancel_with` 施加请求级取消令牌。
    ///
    /// # Errors
    ///
    /// `await` 后可能返回 [`mptp_smux::ServeError`]：会话状态不允许读体、请求前缀缺失或
    /// 底层传输读失败。
    pub fn req_body_async<'a>(&'a mut self) -> RequestBodyAsync<'a, 'a, C>
    where
        C: 'a,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        self.session_.req_body_async()
    }

    /// 丢弃未读完的请求体，然后把接收半边当裸后缀流交出去。
    ///
    /// 委托 [`ServingSession::req_suffix_async`]；请求级的「先排空体、再读后缀」顺序由会话
    /// 保证，处理器不需要自己处理。
    ///
    /// # Errors
    ///
    /// `await` 后可能返回 [`mptp_smux::ServeError`]：会话状态不允许读后缀、请求前缀缺失或
    /// 排空请求体时传输失败。
    pub fn req_suffix_async<'a>(&'a mut self) -> RequestSuffixAsync<'a, 'a, C>
    where
        C: 'a,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        self.session_.req_suffix_async()
    }

    /// 构造回复 helper（状态码 + 头 + 无体 / 定长体 / 分块体 / 后缀流）。
    ///
    /// 委托 [`ServingSession::response`]；前缀在 helper 被终结时才写出，因此在 `.await`
    /// 之前都可以继续加回复头。
    ///
    /// ```text
    /// ctx.response(Status::Ok).no_body_async().await?;
    /// ```
    pub fn response(&mut self, status: Status) -> ResponseBuilder<'_, C> {
        self.session_.response(status)
    }

    /// 连接作用域状态（共享借用）。
    pub fn state(&self) -> &S {
        self.state_
    }

    /// 请求级运行期状态（只读）。
    ///
    /// 处理器用它读取中间件 stash 的请求事实（例如鉴权通过后的主体、配额余量）。
    pub fn request_state(&self) -> &R {
        self.request_state_
    }

    /// 请求级运行期状态（可变）。
    ///
    /// 中间件用它写入本请求的运行期事实；处理器通常只读。状态随请求结束丢弃，不跨请求
    /// 共享，也不写回端点元数据。
    pub fn request_state_mut(&mut self) -> &mut R {
        self.request_state_
    }

    /// 请求级取消句柄（`Copy`）。
    ///
    /// 处理器 / 中间件用它派生子令牌，交给需要取消的 future（`may_cancel_with`）。
    pub fn cancel(&self) -> RequestCancel<'f> {
        self.cancel_
    }
}
