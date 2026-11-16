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

use mptp_smux::{ServeError, serving::config::TrConfig};

use super::{
    arena_::{ArenaFuture, arena_future_},
    context_::MptpContext,
};

/// handler 返回的 future：分配在会话 allocator 里、类型擦除的 arena box。
///
/// 不带 `Send`：与设计一致，future 链天然 `!Send`，`Send` 边界在分派层。
///
/// The future returned by a handler: a type-erased box allocated in the session's allocator.
pub type HandlerFuture<'a, A> = ArenaFuture<'a, A, Result<(), ServeError>>;

/// 一个路由目标：只做业务。
///
/// `R` 是请求级运行期状态（缺省 `()`），处理器经 [`MptpContext::request_state`] 只读
/// 访问它；不关心请求级状态的处理器把 `R` 留空即可。
///
/// 对象安全：返回类型是类型擦除的 arena box（[`HandlerFuture`]），因此可以放进
/// [`alloc::sync::Arc<dyn TrEndpointHandler>`](alloc::sync::Arc)。future 从 [`MptpContext`]
/// 自带的请求级 allocator 分配，不产生 Global 堆分配。
///
/// 注意：返回的 future **不带 `Send`**（见 integration-decisions 的 !Send 结论）——
/// `gen_mcf2` 生成的带高阶类型的 future 无法被编译器穿透，future 链天然 `!Send`，`Send`
/// 边界放在分派层，不在 handler / 中间件这一层。
pub trait TrEndpointHandler<C, S = (), R = ()>: Send + Sync
where
    C: TrConfig,
{
    /// 处理一次已完成路由的请求。
    ///
    /// `'a` 是本次借用的生命周期，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，
    /// 这样调用方可以在一次 `.await` 之后重新借用 `ctx`（中间件链的关键）。
    fn handle<'a, 'f>(
        &'a self,
        ctx: &'a mut MptpContext<'f, C, S, R>,
    ) -> HandlerFuture<'a, C::Alloc>;
}

/// 把 future 分配进请求级 allocator，返回类型擦除的 arena box。
///
/// 终端处理器 / 中间件先经 [`MptpContext::arena`] 取出 allocator 句柄（`&'f C::Alloc`），
/// 构造好 future 后交给本函数；本函数把借用收窄到 `'a` 再分配，避免 `Box<'f, F>` 要求
/// `F: 'f`（future 只捕获 `'a` 数据）。`O` 是 future 的输出类型，泛型以同时服务 handler
/// 与中间件。
pub fn box_future_<'a, 'f, C, F, O>(arena: &'f C::Alloc, fut: F) -> ArenaFuture<'a, C::Alloc, O>
where
    C: TrConfig,
    F: Future<Output = O> + 'a,
    'f: 'a,
{
    arena_future_(arena, fut)
}
