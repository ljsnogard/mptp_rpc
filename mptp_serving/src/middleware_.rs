//! 中间件与管线：按端点元数据执行横切策略。
//!
//! 中间件是**洋葱**：每个 [`TrMiddleware::call`] 拿到一个 `next` 回调，调用它就把控制权
//! 交给下一环（最后一环是终端处理器），自己可以在 `.await` 前后做事。这与设计稿 §6.5 的
//! `next` 形状一致，但不用 `Service` / `Layer` 泛型栈：`next` 是 arena 里的普通
//! `FnMut`，链上没有一个泛型包装器。
//!
//! 短路有两种写法：返回 `Ok(Some(status))` 让管线写该状态的无体回复（省事、统一），或者
//! 自己写完回复后返回 `Ok(None)` 且不调用 `next`（需要写头 / 写体时）。
//!
//! # `middleware_`: middleware and the pipeline
//!
//! Middleware is an onion: each [`TrMiddleware::call`] receives a `next` callback, invokes it
//! to hand control to the next layer (the terminal handler is the innermost one), and may do
//! work before and after the `.await`. Short-circuiting either returns `Ok(Some(status))`
//! (the pipeline writes a no-body rejection) or writes the reply itself and returns
//! `Ok(None)` without calling `next`.

use alloc::alloc::Global;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::alloc::Allocator;
use core::future::Future;
use core::pin::Pin;

use abs_mptp::spec::Status;
use bumpalo::boxed::Box as BumpBox;
use mptp_smux::ServeError;
use mptp_smux::serving::config::TrConfig;

use super::context_::MptpContext;
use super::endpoint_::Endpoint;
use super::handler_::box_future_;

/// 中间件 `call` 返回的 future：分配在请求级 arena 里、类型擦除的 [`BumpBox`]。
///
/// 不带 `Send`：与 [`TrEndpointHandler`] 一致，future 链天然 `!Send`，`Send` 边界在分派层。
pub type MiddlewareFuture<'f> =
    Pin<BumpBox<'f, dyn Future<Output = Result<Option<Status>, ServeError>> + 'f>>;

/// 中间件的 `next` 回调：把控制权交给链上的下一环。
///
/// `'a` 是本次借用的生命周期，`'f` 是 [`MptpContext`] 内部借用的生命周期。回调由
/// [`Pipeline`] 在请求作用域、从请求级 arena 里构造，因此可以在中间件的 future 里跨
/// `.await` 存活；中间件调用 `next(ctx)` 即放行，不调用即短路。
pub type NextFn<'a, 'f, C, S = (), R = ()> =
    dyn FnMut(&'a mut MptpContext<'f, C, S, R>) -> MiddlewareFuture<'a> + 'a;

/// 一个横切策略中间件。
///
/// 对象安全：`call` 返回类型擦除的 arena box（[`MiddlewareFuture`]）。它通常先读
/// `endpoint.metadata()` 决定是否介入，然后调用 `next(ctx).await` 放行；也可以在 `next`
/// 返回之后再做清理 / 计量（洋葱的「后置」半边）。
///
/// 返回值约定：
///
/// - `Ok(None)`：本中间件（或它调用的 `next`）已经处理完，管线不再补写回复；
/// - `Ok(Some(status))`：短路，且由管线写该状态的无体回复（中间件不必自己写）；
/// - `Err(err)`：按 [`ServeError`] 上抛，收尾仍由 servlet 兜底。
///
/// `'a` 是本次借用，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，使管线在一次
/// `.await` 之后能重新借用 `ctx`。
pub trait TrMiddleware<C, S = (), R = ()>: Send + Sync
where
    C: TrConfig,
{
    /// 包裹一次请求处理：在调用 `next` 之前 / 之后做本中间件的事。
    fn call<'a, 'f>(
        &'a self,
        endpoint: &'a Endpoint<C, S, R>,
        ctx: &'a mut MptpContext<'f, C, S, R>,
        next: &'a mut NextFn<'a, 'f, C, S, R>,
    ) -> MiddlewareFuture<'a>;
}

/// 一条有序的中间件链，末端接终端处理器。
///
/// `S` / `R` 与 router 的连接作用域状态、请求级状态一致；`A` 是中间件列表的分配器
/// （缺省 [`Global`]）。装配期构建、请求路径只读。
pub struct Pipeline<C, S = (), R = (), A = Global>
where
    C: TrConfig + 'static,
    A: Allocator,
{
    /// 按装配顺序排列的中间件。
    middlewares_: Vec<Arc<dyn TrMiddleware<C, S, R>>, A>,
}

impl<C, S, R> Pipeline<C, S, R, Global>
where
    C: TrConfig + 'static,
{
    /// 造一条空管线（中间件列表用全局分配器）。
    pub fn new() -> Self {
        Pipeline {
            middlewares_: Vec::new_in(Global),
        }
    }
}

impl<C, S, R> Default for Pipeline<C, S, R, Global>
where
    C: TrConfig + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C, S, R, A> Pipeline<C, S, R, A>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
{
    /// 用指定分配器造一条空管线。
    pub fn new_in(alloc: A) -> Self {
        Pipeline {
            middlewares_: Vec::new_in(alloc),
        }
    }

    /// 追加一个中间件（装配顺序即执行顺序）。
    pub fn use_middleware(mut self, middleware: impl TrMiddleware<C, S, R> + 'static) -> Self {
        self.middlewares_.push(Arc::new(middleware));
        self
    }

    /// 从最外层开始跑整条洋葱链，末端是终端处理器。
    ///
    /// 任一中间件返回 `Some(status)`（且没有继续调用 `next`）时，管线写该状态的无体回复；
    /// 中间件返回错误或处理器返回错误都按 [`ServeError`] 上抛，收尾仍由 servlet 兜底。
    pub async fn run<'a, 'f>(
        &'a self,
        endpoint: &'a Endpoint<C, S, R>,
        ctx: &'a mut MptpContext<'f, C, S, R>,
    ) -> Result<(), ServeError> {
        // 只把 ctx 借给整条链一次：链跑完后还要用同一份 ctx 写短路回复。
        let short_circuit = {
            let ctx = &mut *ctx;
            self.run_from_(0usize, endpoint, ctx).await?
        };
        if let Option::Some(status) = short_circuit {
            ctx.session_mut().response(status).no_body_async().await?;
        }
        Result::Ok(())
    }

    /// 从第 `index` 环开始跑：把「下一环」装成 arena 里的 `next` 回调交给当前中间件。
    ///
    /// 之所以把回调放进请求级 arena 而不是栈上局部变量：`next` 的借用生命周期是 `'a`
    /// （中间件的 future 持有它跨 `.await`），而栈上局部变量活不到调用者选定的 `'a`。
    /// 闭包只捕获 `&self` / `&endpoint` / `usize`，没有 `Drop` 需要执行，因此用
    /// [`Bump::alloc`](bumpalo::Bump::alloc) 放进 arena 不会遗漏析构。
    async fn run_from_<'a, 'f>(
        &'a self,
        index: usize,
        endpoint: &'a Endpoint<C, S, R>,
        ctx: &'a mut MptpContext<'f, C, S, R>,
    ) -> Result<Option<Status>, ServeError> {
        let Some(middleware) = self.middlewares_.get(index) else {
            endpoint.handler().handle(ctx).await?;
            return Result::Ok(Option::None);
        };
        let arena = ctx.arena();
        let next = move |ctx: &'a mut MptpContext<'f, C, S, R>| -> MiddlewareFuture<'a> {
            let arena = ctx.arena();
            box_future_(arena, self.run_from_(index + 1usize, endpoint, ctx))
        };
        let next: &'a mut NextFn<'a, 'f, C, S, R> = arena.alloc(next);
        middleware.call(endpoint, ctx, next).await
    }
}
