//! 路径前缀路由：把请求的 `location` 映射到一个 handler。
//!
//! 这是「单 handler 分派」的载体——路由命中之后框架直接调用**那一个** handler，不提供链式
//! 中间件。理由：`abs_mptp` 的 handler 契约已经把「写回复」的权力交给了 handler 自己
//! （它拿到收发两边），链式语义（谁来写最终回复、谁来收尾）在没有回复类型的前提下没有唯一
//! 答案，硬造一条链只会把这件事变成框架的猜谜。
//!
//! 匹配规则是**最长前缀**：登记 `/topic` 与 `/topic/chat` 时，`/topic/chat/42` 命中后者。
//! 同一前缀重复登记时，**后登记的那条**生效。内部把每条前缀展开成 `matchit_route` 的
//! 精确规则 + `{*rest}` catch-all，因此项目里只有一套路径匹配算法。
//!
//! # 两个层次的 router
//!
//! - [`Router`]：MPTP 协议层的顶层 router。它把 `location` 按前缀交给一个具体的 handler，
//!   自身也实现 [`TrReqHandler`]，因此可以直接交给
//!   [`MptpServer`](crate::serving::MptpServer)；
//! - RPC 层的 `RpcRouter`（在 `rpc_core_v1::serving`）：实现
//!   [`TrPathHandler`]，提供 interface 前缀；`Router::call` 用它登记一个 interface。
//!
//! # 任意数量的 interface
//!
//! 顶层 handler 的类型列表使用 [`tlist`] 的 [`TCons`] / [`TNil`]，不再按元组长度展开宏。
//! 运行期 handler 实例放在由类型列表索引的 [`RouteValues`] 递归枚举里，与类型列表头尾
//! 对应；`Router` 递归到命中的前缀后直接调用具体 handler 的 `handle_async`。
//!
//! 递归分派用 [`EitherMayCancel`] 把「命中当前节点」与「继续查尾节点」合成同一个可取消
//! future；每个 handler 的错误类型经 [`MapErrMayCancel`] 映射为 [`RouterError`]。请求路径
//! 上没有 `Pin<Box<dyn Future>>`。
//!
//! # `router_`: path routers and the top-level MPTP router
//!
//! [`Router`] matches by longest path prefix and owns a `tlist` type-level list of handler
//! types. Runtime handler values live in the recursive [`RouteValues`] enum indexed by that
//! list, so there is no `dyn Any` and no downcast. Dispatch uses an `Either`-based
//! cancellable future and maps per-handler errors into [`RouterError`]; no future is boxed
//! on the request path.

use core::marker::PhantomData;
use std::sync::OnceLock;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{serving::TrReqHandler, spec::access_method::AccessMethod};
use gen_mcf2::gen_may_cancel_future;
use matchit_route::Router as MatchitRouter;
use thiserror::Error;
use tlist::{TCons, TList, TNil};

use crate::{
    messaging::Headers,
    serving::{
        cancel_future_::{EitherMayCancel, MapErrMayCancel, ReadyMayCancel},
        config::{ChannelRx, ChannelTx, TrConfig},
    },
    x_deps::{abs_cancel, abs_mptp, gen_mcf2},
};

/// 一个可以被 [`Router`] 按路径前缀登记的类型。
///
/// RPC 层的 `RpcRouter<I, S>` 实现它，并把 `PATH` 取成 `I::PATH`；用户也可以为自己的
/// handler 实现它，然后通过 [`Router::call`] 登记。
pub trait TrPathHandler {
    /// 本 handler 的路径前缀；不含首尾 `/`。
    const PATH: &'static str;
}

/// 路径 router：按前缀把请求分派给一个 handler。
pub trait TrPathRouter {
    /// 命中的 handler 类型。
    type Handler;

    /// 按路径前缀匹配一个 handler；未命中返回 `None`。
    fn match_path<'f>(&'f self, path: &'f str) -> Option<&'f Self::Handler>;
}

/// 可在装配期继续登记的路径 router。
pub trait TrPathRouterMut
where
    Self: TrPathRouter,
{
    /// 注册失败时的错误类型。
    type Err: core::error::Error;

    /// 注册一条路径前缀路由。
    fn add_rule<'f, F>(&'f mut self, rule: &'f str, make_handler: F) -> Result<(), Self::Err>
    where
        Self: Sized,
        F: FnOnce(&'f str) -> Self::Handler;
}

/// 顶层 router 的装配上下文。
///
/// 目前它不携带运行期数据，只为 `Router::call` / `RpcRouter::new` 提供一个**有类型**的
/// 入口：后续要注入应用状态时，可以在不改变调用形状的前提下扩展它。`C` 是服务端配置
/// 类型。
pub struct RouterCtx<C> {
    _cfg_: PhantomData<fn() -> C>,
}

impl<C> RouterCtx<C> {
    /// 造一个空的装配上下文。
    pub const fn new() -> Self {
        RouterCtx { _cfg_: PhantomData }
    }
}

impl<C> Default for RouterCtx<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// 由 `tlist` 类型列表索引的运行期路由容器。
///
/// `TNil` 对应 [`RouteValues::Nil`]；`TCons<H, T>` 对应 `RouteValues<H, T::Values>`，
/// `Cons` 直接持有该路由的前缀、handler 实体与剩余列表。
#[doc(hidden)]
pub enum RouteValues<H, T> {
    /// 空列表。
    Nil,

    /// 一条路由：前缀 + handler 实体 + 剩余列表。
    Cons {
        /// 路径前缀；不含首尾 `/`。
        prefix_: &'static str,

        /// handler 实体。
        handler_: H,

        /// 剩余路由。
        tail_: T,
    },
}

/// 把 `tlist` 类型列表映射到运行期路由容器类型。
#[doc(hidden)]
pub trait TrRouteValues: TList {
    /// 运行期值容器。
    type Values;
}

impl TrRouteValues for TNil {
    type Values = RouteValues<(), ()>;
}

impl<H, T> TrRouteValues for TCons<H, T>
where
    T: TrRouteValues,
{
    type Values = RouteValues<H, T::Values>;
}

/// MPTP 协议层的顶层 router。
///
/// 它按最长路径前缀把请求分派给一个 handler，自身也实现 [`TrReqHandler`]；因此
/// `MptpServer::new(router)` 不需要再包一层适配器。`C` 是服务端配置类型，`R` 是类型级
/// 路由列表（由 [`Router::call`] / [`Router::route`] 逐步扩展）。
///
/// 路由数量没有人为上限：`route` / `call` 只做一次类型级 cons 与一次运行期枚举构造。
/// 使用者通常不需要写 `R`；默认值是 [`TNil`]。
///
/// handler 实体直接存放在 [`RouteValues`] 递归枚举里；因此没有 `dyn Any`、没有 downcast，
/// `Router` 的 `Send` / `Sync` 也由实际 handler 类型决定。
///
/// # Examples
///
/// ```
/// use mptp_smux::serving::{Router, TrPathHandler, TrPathRouter};
///
/// struct Echo;
///
/// impl TrPathHandler for Echo {
///     const PATH: &'static str = "echo";
/// }
///
/// let router = Router::<(), _>::new().call(|_ctx| Echo);
/// assert!(router.match_path("/echo/1").is_some());
/// assert!(router.match_path("/other").is_some()); // 顶层 Router 总是返回自身
/// ```
pub struct Router<C, R: TrRouteValues = TNil> {
    /// 已登记的路由运行期值；顺序与类型列表的头尾对应。
    routes_: R::Values,

    /// 第一次匹配时由类型列表构建的 `matchit_route` 索引表。
    matcher_: OnceLock<MatchitRouter<usize>>,

    /// `C` 只出现在 handler 的约束里。
    _cfg_: PhantomData<fn() -> C>,
}

impl<C> Router<C, TNil> {
    /// 造一个空的顶层 router。
    pub const fn new() -> Self {
        Router {
            routes_: RouteValues::Nil,
            matcher_: OnceLock::new(),
            _cfg_: PhantomData,
        }
    }
}

impl<C> Default for Router<C, TNil> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C, R: TrRouteValues> Router<C, R> {
    /// 登记一条带显式路径前缀的路由。
    ///
    /// 前缀按完整的 `/` 段匹配；同一前缀重复登记时，**后登记的那条**生效（最长前缀匹配
    /// 优先，长度相同时保留后登记的）。
    pub fn route<H>(
        self,
        prefix: &'static str,
        make_handler: impl FnOnce(&RouterCtx<C>) -> H,
    ) -> Router<C, TCons<H, R>> {
        let ctx = RouterCtx::<C>::new();
        let Router {
            routes_,
            matcher_: _,
            _cfg_: _,
        } = self;

        Router {
            routes_: RouteValues::Cons {
                prefix_: prefix.trim_matches('/'),
                handler_: make_handler(&ctx),
                tail_: routes_,
            },
            matcher_: OnceLock::new(),
            _cfg_: PhantomData,
        }
    }

    /// 登记一个实现 [`TrPathHandler`] 的 handler，路径取它的 `PATH`。
    ///
    /// 名字沿用 RPC 场景里的 `Call`；本层只按路径分派，不检查访问方法——真正的
    /// `AccessMethod::Call` 检查由 RPC handler（如 `RpcRouter`）完成。
    pub fn call<H: TrPathHandler>(
        self,
        make_handler: impl FnOnce(&RouterCtx<C>) -> H,
    ) -> Router<C, TCons<H, R>> {
        self.route(H::PATH, make_handler)
    }
}

impl<C, R: TrRouteValues> TrPathRouter for Router<C, R> {
    type Handler = Self;

    fn match_path<'f>(&'f self, _path: &'f str) -> Option<&'f Self::Handler> {
        Option::Some(self)
    }
}

/// 顶层路由失败。
///
/// 未命中路径时**不写回复**：MPTP 层没有 RPC 错误体格式；上层可以把
/// [`RouterError::NotFound`] 当作连接级错误处理。命中 handler 之后，回复由 handler 负责。
#[derive(Debug, Error)]
pub enum RouterError {
    /// 没有路由前缀命中该 `location`。
    #[error("没有匹配的路径：{0}")]
    NotFound(String),

    /// 命中的 handler 返回错误。
    #[error("handler 处理失败：{0}")]
    Handler(String),
}

/// 把任意 handler 错误映射成 [`RouterError`]。
fn map_handler_error_<E>(err: E) -> RouterError
where
    E: core::error::Error,
{
    RouterError::Handler(err.to_string())
}

/// 一个顶层 handler 类型列表：按前缀匹配，并按索引分派。
///
/// 本 trait 由 `tlist` 的 [`TNil`] / [`TCons`] 实现；使用者不需要手写实现。
#[doc(hidden)]
pub trait TrPathRouterList<C>: TrRouteValues
where
    C: TrConfig,
{
    /// 把本列表的前缀登记到 `matchit_route` 索引表；`offset` 是当前头节点在列表中的索引。
    fn register_routes(values: &Self::Values, registry: &mut MatchitRouter<usize>, offset: usize);

    /// 按索引分派；`index` 来自 `matchit_route` 索引表。
    fn dispatch_at<'f>(
        routes: &'f Self::Values,
        index: usize,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), RouterError>> + 'f
    where
        C: 'f;
}

impl<C> TrPathRouterList<C> for TNil
where
    C: TrConfig,
{
    fn register_routes(
        _values: &Self::Values,
        _registry: &mut MatchitRouter<usize>,
        _offset: usize,
    ) {
    }

    fn dispatch_at<'f>(
        _routes: &'f Self::Values,
        _index: usize,
        _method: AccessMethod,
        _location: &'f str,
        _headers: &'f mut Headers,
        _tx: ChannelTx<C>,
        _rx: ChannelRx<C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), RouterError>> + 'f
    where
        C: 'f,
    {
        ReadyMayCancel::new(Result::Ok(()))
    }
}

impl<C, H, T> TrPathRouterList<C> for TCons<H, T>
where
    C: TrConfig,
    H: TrReqHandler<C>,
    H::Err: 'static,
    T: TrPathRouterList<C>,
{
    fn register_routes(values: &Self::Values, registry: &mut MatchitRouter<usize>, offset: usize) {
        let RouteValues::Cons { prefix_, tail_, .. } = values else {
            unreachable!("Router 内部不变量：TCons 对应的运行期路由不是 Cons")
        };

        register_prefix_(registry, prefix_, offset);
        T::register_routes(tail_, registry, offset + 1);
    }

    fn dispatch_at<'f>(
        routes: &'f Self::Values,
        index: usize,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), RouterError>> + 'f
    where
        C: 'f,
    {
        let RouteValues::Cons {
            handler_, tail_, ..
        } = routes
        else {
            unreachable!("Router 内部不变量：TCons 对应的运行期路由不是 Cons")
        };

        if index == 0usize {
            EitherMayCancel::Left(MapErrMayCancel::new(
                handler_.handle_async(method, location, headers, tx, rx),
                map_handler_error_::<H::Err>,
            ))
        } else {
            EitherMayCancel::Right(T::dispatch_at(
                tail_,
                index - 1usize,
                method,
                location,
                headers,
                tx,
                rx,
            ))
        }
    }
}

impl<C, R> Router<C, R>
where
    C: TrConfig,
    R: TrPathRouterList<C>,
{
    /// 返回命中的路由索引；首次调用时构建索引表。
    fn match_index_(&self, location: &str) -> Option<usize> {
        let registry = self.matcher_.get_or_init(|| {
            let mut registry = MatchitRouter::new();
            R::register_routes(&self.routes_, &mut registry, 0);
            registry
        });

        registry
            .at(location.trim_matches('/'))
            .ok()
            .map(|matched| *matched.value)
    }
}

/// 把一条静态前缀登记成“精确路径 + catch-all”两条规则。
///
/// 重复前缀由调用顺序决定：列表头（后登记）先注册，冲突时 matchit_route 返回
/// `InsertError::Conflict`，这里直接忽略，于是后登记的那条生效。
fn register_prefix_(registry: &mut MatchitRouter<usize>, prefix: &str, index: usize) {
    let path = escape_static_(prefix);

    if path.is_empty() {
        let _ = registry.insert("", index);
        let _ = registry.insert("{*rest}", index);
    } else {
        let _ = registry.insert(path.as_str(), index);
        let _ = registry.insert(format!("{path}/{{*rest}}"), index);
    }
}

/// 把前缀里的 `{` / `}` 转义成字面量，保持“前缀是静态路径”的旧语义。
fn escape_static_(path: &str) -> String {
    let mut escaped = String::with_capacity(path.len());

    for c in path.trim_matches('/').chars() {
        match c {
            '{' => escaped.push_str("{{"),
            '}' => escaped.push_str("}}"),
            _ => escaped.push(c),
        }
    }

    escaped
}

impl<C, R> TrReqHandler<C> for Router<C, R>
where
    C: TrConfig + 'static,
    R: TrPathRouterList<C>,
{
    type HandleAsync<'f>
        = TopRouterHandleAsync<'f, 'f, C, R>
    where
        Self: 'f;

    type Err = RouterError;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        chan_tx: ChannelTx<C>,
        chan_rx: ChannelRx<C>,
    ) -> Self::HandleAsync<'f> {
        TopRouterHandleAsync::<'f, 'f, C, R>::new(self, method, location, headers, chan_tx, chan_rx)
    }
}

/// [`Router`] 的 step 函数。
#[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
#[gen_may_cancel_future(TopRouterHandle, pub, new(pub(crate)))]
async fn top_router_handle_async_<'f, C, R, K>(
    router: &'f Router<C, R>,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: K,
) -> Result<(), RouterError>
where
    C: TrConfig + 'f,
    R: TrPathRouterList<C> + 'f,
    K: TrCancellationToken,
{
    let Option::Some(index) = router.match_index_(location) else {
        return Result::Err(RouterError::NotFound(location.to_string()));
    };

    R::dispatch_at(&router.routes_, index, method, location, headers, tx, rx)
        .may_cancel_with(cancel.child_token())
        .await
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试前缀索引表按完整路径段选择最长前缀。
    /// Test that the prefix registry selects the longest prefix on segment boundaries.
    ///
    /// - 手段：把 `api/v1`、`api` 与空前缀（回退）登记到 `matchit_route` 索引表。
    /// - 判断：`api/v1/goods` 命中更长的 `api/v1`，`api/v10` 命中 `api`，其它路径命中回退。
    ///
    /// - Means: register `api/v1`, `api` and the empty fallback prefix.
    /// - Verdict: deeper paths hit the longer prefix, lookalikes hit `api`, and the rest
    ///   fall back to the empty prefix.
    #[test]
    fn prefix_registry_matches_longest_() {
        let mut registry = MatchitRouter::new();
        register_prefix_(&mut registry, "api", 1usize);
        register_prefix_(&mut registry, "api/v1", 2usize);
        register_prefix_(&mut registry, "", 0usize);

        let index = |path: &str| {
            registry
                .at(path.trim_matches('/'))
                .ok()
                .map(|matched| *matched.value)
        };

        assert_eq!(index("api/v1/goods"), Some(2));
        assert_eq!(index("api/v1"), Some(2));
        assert_eq!(index("api/v10"), Some(1));
        assert_eq!(index("other"), Some(0));
        assert_eq!(index(""), Some(0));
    }

    /// 测试顶层 Router 能登记多条路由并返回自身作为 handler。
    /// Test that the top-level Router registers routes and returns itself as handler.
    ///
    /// - 手段：构造一个带 `api` 与 `api/v1` 两条路由的 Router。
    /// - 判断：`match_path` 返回自身，`route` 的链式构造可用。
    ///
    /// - Means: build a Router with `api` and `api/v1` rules.
    /// - Verdict: `match_path` returns itself and chained construction works.
    #[test]
    fn builds_top_router_() {
        struct A;
        struct B;

        impl TrPathHandler for A {
            const PATH: &'static str = "api";
        }

        impl TrPathHandler for B {
            const PATH: &'static str = "api/v1";
        }

        let router = Router::<(), _>::new().call(|_| A).call(|_| B);
        assert!(router.match_path("api/v1/goods").is_some());
    }

    /// 测试 tlist 路由列表没有固定元组长度上限。
    /// Test that the tlist-based route list has no fixed tuple-length limit.
    ///
    /// - 手段：构造 20 条 `DummyHandler` 路由，再断言 `Router` 实现 `TrReqHandler<C>`。
    /// - 判断：编译通过；运行期不建立连接。
    ///
    /// - Means: build a `Router` with 20 `DummyHandler` rules and assert it implements
    ///   `TrReqHandler<C>`.
    /// - Verdict: it compiles; no connection is opened.
    #[allow(dead_code)]
    fn supports_many_routes_<C>()
    where
        C: TrConfig + 'static,
    {
        struct DummyHandler;

        impl<C> TrReqHandler<C> for DummyHandler
        where
            C: TrConfig,
        {
            type HandleAsync<'f>
                = ReadyMayCancel<Result<(), RouterError>>
            where
                Self: 'f;

            type Err = RouterError;

            fn handle_async<'f>(
                &'f self,
                _method: AccessMethod,
                _location: &'f str,
                _headers: &'f mut Headers,
                _chan_tx: ChannelTx<C>,
                _chan_rx: ChannelRx<C>,
            ) -> Self::HandleAsync<'f> {
                ReadyMayCancel::new(Result::Ok(()))
            }
        }

        fn assert_handler<C, H>(_handler: &H)
        where
            C: TrConfig + 'static,
            H: TrReqHandler<C>,
        {
        }

        let router = Router::<C, _>::new()
            .route("r0", |_| DummyHandler)
            .route("r1", |_| DummyHandler)
            .route("r2", |_| DummyHandler)
            .route("r3", |_| DummyHandler)
            .route("r4", |_| DummyHandler)
            .route("r5", |_| DummyHandler)
            .route("r6", |_| DummyHandler)
            .route("r7", |_| DummyHandler)
            .route("r8", |_| DummyHandler)
            .route("r9", |_| DummyHandler)
            .route("r10", |_| DummyHandler)
            .route("r11", |_| DummyHandler)
            .route("r12", |_| DummyHandler)
            .route("r13", |_| DummyHandler)
            .route("r14", |_| DummyHandler)
            .route("r15", |_| DummyHandler)
            .route("r16", |_| DummyHandler)
            .route("r17", |_| DummyHandler)
            .route("r18", |_| DummyHandler)
            .route("r19", |_| DummyHandler);

        assert_handler::<C, _>(&router);
    }
}
