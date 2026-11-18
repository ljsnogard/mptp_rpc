//! 终端处理器契约：只关心业务语义。
//!
//! [`TrEndpointHandler`] 是路由链的终点。它拿到 [`MptpContext`]，写回复即可；不知道路由树、
//! 不知道中间件、不知道元数据。收尾仍由 `mptp_smux` 的 servlet 兜底。
//!
//! # `handler_`: the terminal handler contract
//!
//! The terminal handler only writes business replies. It sees the request context and knows
//! nothing about the routing tree or middleware.

use core::future::Future;

use mptp_smux::{RequestAlloc, ServeError};

use super::{
    arena_::{ArenaFuture, arena_future_},
    context_::MptpContext,
};

/// handler 返回的 future：分配在请求级 arena 里、类型擦除的 arena box。
///
/// 不带 `Send`：与设计一致，future 链天然 `!Send`，`Send` 边界在分派层。
///
/// The future returned by a handler: a type-erased box allocated in the request-scoped
/// arena. The allocator handle is owned by the future, so the handler does not borrow the
/// context in the future's allocator type.
pub type HandlerFuture<'f, 'a> = ArenaFuture<'f, 'a, Result<(), ServeError>>;

/// 一个路由目标：只做业务。
///
/// `R` 是请求级运行期状态（缺省 `()`），处理器经 [`MptpContext::request_state`] 只读
/// 访问它；不关心请求级状态的处理器把 `R` 留空即可。
///
/// 对象安全：返回类型是类型擦除的 arena box（[`HandlerFuture`]），因此可以放进
/// [`alloc::sync::Arc<dyn TrEndpointHandler>`](alloc::sync::Arc)。future 从 [`MptpContext`]
/// 自带的请求级 arena 分配，不产生 Global 堆分配。
///
/// 注意：返回的 future **不带 `Send`**（见 integration-decisions 的 !Send 结论）——
/// `gen_mcf2` 生成的带高阶类型的 future 无法被编译器穿透，future 链天然 `!Send`，`Send`
/// 边界放在分派层，不在 handler / 中间件这一层。
pub trait TrEndpointHandler<C, S = (), R = ()>: Send + Sync
where
    C: mptp_smux::serving::config::TrConfig,
{
    /// 处理一次已完成路由的请求。
    ///
    /// `'a` 是本次借用的生命周期，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，
    /// 这样调用方可以在一次 `.await` 之后重新借用 `ctx`（中间件链的关键）。
    fn handle<'f, 'x, 'a>(
        &'x self,
        ctx: &'x mut MptpContext<'f, 'a, C, S, R>,
    ) -> HandlerFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x;
}

/// 把 future 分配进请求级 arena，返回类型擦除的 arena box。
///
/// 终端处理器 / 中间件先经 [`MptpContext::arena`] 取出 allocator 句柄克隆，构造好 future
/// 后交给本函数；allocator 句柄随 future 一起持有，因此 future 的 `'f` 只描述它捕获的
/// 数据，不要求 `&MptpContext` 活到 `'f`；`'a` 是请求级 arena 的生命周期，必须覆盖
/// future（`'a: 'f`）。
pub fn box_future_<'f, 'a, F, O>(arena: RequestAlloc<'a>, fut: F) -> ArenaFuture<'f, 'a, O>
where
    F: Future<Output = O> + 'f,
    'a: 'f,
{
    arena_future_(arena, fut)
}
