//! 中间件与管线：按端点元数据执行横切策略。
//!
//! 本 crate 采用**线性“前置”中间件**而不是洋葱式的 `Service`/`Layer` 服务栈：每个中间件
//! 在终端处理器之前运行，返回 [`Status`] 表示「短路」（管线写拒绝回复并停止），返回 `None`
//! 表示放行。这样 `next` 是普通循环里的直接调用，不需要 Tower 式的高阶闭包类型体操。
//!
//! # `middleware_`: middleware and the pipeline
//!
//! We use a linear *before*-middleware instead of an onion `Service`/`Layer` stack. Each
//! middleware runs before the terminal handler; `Some(status)` short-circuits (the pipeline
//! writes a rejection), `None` passes through. This keeps `next` as an ordinary loop, not a
//! higher-ranked closure chain.

use alloc::alloc::Global;
use alloc::boxed::Box;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::alloc::Allocator;
use core::future::Future;
use core::pin::Pin;

use abs_mptp::spec::Status;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::ServeError;

use super::context_::MptpContext;
use super::endpoint_::Endpoint;

/// 中间件 `before` 返回的 future。
///
/// 不带 `Send`：与 [`TrEndpointHandler`] 一致，future 链天然 `!Send`，`Send` 边界在分派层。
pub type MiddlewareFuture<'f> =
    Pin<Box<dyn Future<Output = Result<Option<Status>, ServeError>> + 'f>>;

/// 一个横切策略中间件。
///
/// 对象安全：`before` 返回擦除的 `Box<dyn Future>`。它读 `endpoint.metadata()` 决定是否
/// 介入；返回 `Some(status)` 表示短路（管线写该状态的无体回复并停止），`None` 表示放行。
///
/// `'a` 是本次借用，`'f` 是 [`MptpContext`] 内部借用的生命周期；两者分离，使管线在一次
/// `.await` 之后能重新借用 `ctx`。
pub trait TrMiddleware<C, S = ()>: Send + Sync
where
    C: TrConfig,
{
    /// 在终端处理器之前执行。
    fn before<'a, 'f>(
        &'a self,
        endpoint: &'a Endpoint<C, S>,
        ctx: &'a mut MptpContext<'f, C, S>,
    ) -> MiddlewareFuture<'a>;
}

/// 一条有序的中间件链，末端接终端处理器。
///
/// `A` 是中间件列表的分配器（缺省 [`Global`]）。装配期构建、请求路径只读。
pub struct Pipeline<C, S = (), A = Global>
where
    C: TrConfig + 'static,
    A: Allocator,
{
    /// 按装配顺序排列的中间件。
    middlewares_: Vec<Arc<dyn TrMiddleware<C, S>>, A>,
}

impl<C, S> Pipeline<C, S, Global>
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

impl<C, S> Default for Pipeline<C, S, Global>
where
    C: TrConfig + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<C, S, A> Pipeline<C, S, A>
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
    pub fn use_middleware(mut self, middleware: impl TrMiddleware<C, S> + 'static) -> Self {
        self.middlewares_.push(Arc::new(middleware));
        self
    }

    /// 依次运行中间件，全部放行后调用终端处理器。
    ///
    /// 任一中间件返回 `Some(status)` 即写该状态的无体回复并停止；中间件返回错误或
    /// 处理器返回错误都按 [`ServeError`] 上抛，收尾仍由 servlet 兜底。
    pub async fn run<'f>(
        &'f self,
        endpoint: &'f Endpoint<C, S>,
        ctx: &'f mut MptpContext<'f, C, S>,
    ) -> Result<(), ServeError> {
        for middleware in &self.middlewares_ {
            if let Option::Some(status) = middleware.before(endpoint, ctx).await? {
                ctx.session_mut().response(status).no_body_async().await?;
                return Result::Ok(());
            }
        }
        endpoint.handler().handle(ctx).await
    }
}
