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
use core::pin::Pin;

use bumpalo::Bump;
use bumpalo::boxed::Box as BumpBox;
use mptp_smux::ServeError;
use mptp_smux::serving::config::TrConfig;

use super::context_::MptpContext;

/// handler 返回的 future：分配在请求级 arena 里、类型擦除的 [`BumpBox`]。
///
/// 不带 `Send`：与设计一致，future 链天然 `!Send`，`Send` 边界在分派层。
pub type HandlerFuture<'a> = Pin<BumpBox<'a, dyn Future<Output = Result<(), ServeError>> + 'a>>;

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
    C: TrConfig,
{
    /// 处理一次已完成路由的请求。
    ///
    /// `'a` 是本次借用的生命周期，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，
    /// 这样调用方可以在一次 `.await` 之后重新借用 `ctx`（中间件链的关键）。
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S, R>) -> HandlerFuture<'a>;
}

/// 把 future 分配进请求级 arena，返回类型擦除的 arena box。
///
/// 终端处理器 / 中间件先经 [`MptpContext::arena`] 取出 arena 句柄（`&'f Bump`，`Copy`），
/// 构造好 future 后交给本函数；本函数把 arena 借用收窄到 `'a` 再分配，避免 `BumpBox<'f, F>`
/// 要求 `F: 'f`（future 只捕获 `'a` 数据）。`O` 是 future 的输出类型，泛型以同时服务
/// handler 与中间件。
pub fn box_future_<'a, 'f, F, O>(
    arena: &'f Bump,
    fut: F,
) -> Pin<BumpBox<'a, dyn Future<Output = O> + 'a>>
where
    F: Future<Output = O> + 'a,
    'f: 'a,
{
    let arena: &'a Bump = arena;
    box_unsize_future_(BumpBox::new_in(fut, arena))
}

/// 把一个具名 future 的 arena box 拓宽成 `dyn Future` 的 arena box。
///
/// `bumpalo::boxed::Box` 不实现 [`core::ops::CoerceUnsized`]，无法自动 unsize；这里用裸
/// 指针显式拓宽。
///
/// # Safety
///
/// `b` 与返回值是**同一个值、同一份所有权**，只是把 `*mut F` 拓宽成 `*mut dyn Future`；
/// 内存仍在 arena 里，生命周期 `'a` 不变。
fn box_unsize_future_<'a, F, O>(b: BumpBox<'a, F>) -> Pin<BumpBox<'a, dyn Future<Output = O> + 'a>>
where
    F: Future<Output = O> + 'a,
{
    let raw: *mut F = BumpBox::into_raw(b);
    let raw: *mut (dyn Future<Output = O> + 'a) = raw;
    // SAFETY: 见本函数文档；`raw` 仍指向 arena 内的同一块内存，vtable 由 dyn 提供。
    unsafe { Pin::from(BumpBox::from_raw(raw)) }
}
