//! RPC over MPTP 的 interface 级 router。
//!
//! [`RpcRouter`] 只负责一个 interface（`I`）：它按 service 的相对路径把请求分派给一个
//! 具体的 service 实现。service 实现由用户在自己的 crate 里编写，并通过
//! `#[rpc(service_of(I))]` 获得 [`TrRpcService`] 实现；用户再用
//! [`RpcRouter::add_service`] 把它们逐个装进 router。
//!
//! 与旧的「worker + 队列」模型相比，本类型是**内联**分派的：
//!
//! - router 持有 service 实现本身，不生成队列，也不替使用者 spawn；
//! - 一个请求在 router 的 `handle_async` 里直接解码、调用 service、写回复、收尾；
//! - 需要队列 / 背压的使用者可以在外层包自己的 `TrReqHandler`，或继续使用
//!   `rpc_router_trie` 的队列版本。
//!
//! # 任意数量的 service
//!
//! service 类型列表使用 [`tlist`] 的 [`TCons`] / [`TNil`]：`add_service` 只把新类型
//! cons 到列表头，因此没有「元组最多 N 个」的上限。
//!
//! `tlist` 本身是纯类型级 ZST 列表，不保存运行期值；因此这里再定义一个由它索引的
//! **运行期枚举** [`ServiceValues`]：
//!
//! - `TNil` 对应 `ServiceValues::Nil`；
//! - `TCons<H, T>` 对应 `ServiceValues<H, T::Values>`，即 `Cons(实际 service, 尾部)`。
//!
//! 这样 service 实体按具体类型内联存放在递归枚举里，不需要 `dyn Any`、不需要 downcast，
//! 也不会丢失 `Send` / `Sync` 的自动约束。
//!
//! 分派 future 用 [`EitherMayCancel`] 把「命中头节点」与「继续查尾节点」合成同一个可取消
//! future，没有 `Pin<Box<dyn Future>>`。
//!
//! # `rpc_router_`: the interface-level inline RPC router
//!
//! One [`RpcRouter`] serves one interface. It owns the concrete service implementations,
//! dispatches inline (no queues, no spawning), strips the interface prefix, and writes
//! `404` / `405` replies itself. Service types live in a `tlist` type-level list; the actual
//! values live in a recursive [`ServiceValues`] enum indexed by that list, so no `dyn Any`
//! and no downcast are involved.

use core::marker::PhantomData;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    serving::TrReqHandler,
    spec::{Status, access_method::AccessMethod},
};
use abs_rpc_serv::service::TrRpcService;
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    messaging::Headers,
    serving::{
        EitherMayCancel, ReadyMayCancel, RouterCtx,
        config::{ChannelRx, ChannelTx},
    },
};
use tlist::{TCons, TList, TNil};

use crate::{
    messaging::{RpcError, RpcResult, TrRpcInterface},
    serving::{config::TrServingConfig, ctx_::RpcCallCtx},
    x_deps::{abs_cancel, abs_mptp, abs_rpc_serv, gen_mcf2, mptp_smux},
};

/// service 构造闭包收到的上下文。
///
/// 目前它不携带运行期数据；构造 service 所需的业务数据应由闭包捕获。保留这个参数是为了
/// 让 `add_service` 的调用形状稳定：以后要注入框架级状态时，不需要改用户代码。
pub struct RpcServiceCtx {
    _private_: (),
}

impl RpcServiceCtx {
    /// 造一个空的构造上下文。
    pub const fn new() -> Self {
        RpcServiceCtx { _private_: () }
    }
}

impl Default for RpcServiceCtx {
    fn default() -> Self {
        Self::new()
    }
}

/// 由 `tlist` 类型列表索引的运行期 service 容器。
///
/// `TNil` 对应 [`ServiceValues::Nil`]；`TCons<H, T>` 对应
/// `ServiceValues<H, T::Values>`，其中 `Cons` 直接持有 `H` 的实体。
#[doc(hidden)]
pub enum ServiceValues<H, T> {
    /// 空列表。
    Nil,

    /// 头节点实体 + 剩余列表。
    Cons(H, T),
}

/// 把 `tlist` 类型列表映射到运行期值容器类型。
#[doc(hidden)]
pub trait TrRpcServiceValues: TList {
    /// 运行期值容器。
    type Values;
}

impl TrRpcServiceValues for TNil {
    type Values = ServiceValues<(), ()>;
}

impl<H, T> TrRpcServiceValues for TCons<H, T>
where
    T: TrRpcServiceValues,
{
    type Values = ServiceValues<H, T::Values>;
}

/// 一个 service 类型列表：按路径匹配，并按索引分派。
///
/// 本 trait 由 `tlist` 的 [`TNil`] / [`TCons`] 实现；使用者不需要手写实现，也不需要在
/// 业务代码里命名它。
#[doc(hidden)]
pub trait TrRpcServiceList<I, C>: TrRpcServiceValues
where
    I: TrRpcInterface,
    C: TrServingConfig,
{
    /// 返回命中的 `(索引, 命中路径长度)`；索引从列表头开始。
    fn match_service(rest: &str) -> Option<(usize, usize)>;

    /// 按索引分派；`index` 必须来自 [`Self::match_service`]。
    fn dispatch_at<'a, 'f>(
        services: &'f Self::Values,
        index: usize,
        ctx: &'f mut RpcCallCtx<'a, C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
    where
        'a: 'f,
        C: 'f;
}

impl<I, C> TrRpcServiceList<I, C> for TNil
where
    I: TrRpcInterface,
    C: TrServingConfig,
{
    fn match_service(_rest: &str) -> Option<(usize, usize)> {
        Option::None
    }

    fn dispatch_at<'a, 'f>(
        _services: &'f Self::Values,
        _index: usize,
        _ctx: &'f mut RpcCallCtx<'a, C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
    where
        'a: 'f,
        C: 'f,
    {
        ReadyMayCancel::new(Result::Ok(()))
    }
}

impl<I, C, H, T> TrRpcServiceList<I, C> for TCons<H, T>
where
    I: TrRpcInterface,
    C: TrServingConfig,
    H: TrRpcService<I, C, Err = RpcError>,
    T: TrRpcServiceList<I, C>,
{
    fn match_service(rest: &str) -> Option<(usize, usize)> {
        let head_len = path_prefix_len_(<H as TrRpcService<I, C>>::SERVICE_PATH, rest);
        let tail = T::match_service(rest);

        match (head_len, tail) {
            (Option::Some(head_len), Option::Some((tail_index, tail_len))) => {
                if head_len >= tail_len {
                    Option::Some((0usize, head_len))
                } else {
                    Option::Some((tail_index + 1usize, tail_len))
                }
            }
            (Option::Some(head_len), Option::None) => Option::Some((0usize, head_len)),
            (Option::None, Option::Some((tail_index, tail_len))) => {
                Option::Some((tail_index + 1usize, tail_len))
            }
            (Option::None, Option::None) => Option::None,
        }
    }

    fn dispatch_at<'a, 'f>(
        services: &'f Self::Values,
        index: usize,
        ctx: &'f mut RpcCallCtx<'a, C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
    where
        'a: 'f,
        C: 'f,
    {
        // `TCons` 的运行期容器只可能是 `Cons`；`Nil` 分支是类型层面的底座。
        let ServiceValues::Cons(head, tail) = services else {
            unreachable!("RpcRouter 内部不变量：TCons 对应的运行期值不是 Cons")
        };

        if index == 0usize {
            EitherMayCancel::Left(<H as TrRpcService<I, C>>::dispatch_async(head, ctx))
        } else {
            EitherMayCancel::Right(T::dispatch_at(tail, index - 1usize, ctx))
        }
    }
}

/// 一个 interface 的 RPC router。
///
/// `I` 是 interface marker，`S` 是 service 实现的类型级 `tlist` 列表（由
/// [`add_service`](Self::add_service) 逐步增长）。router 只在装配期构建；请求处理只读地
/// 使用已装好的 service。
///
/// 列表长度没有人为上限：`add_service` 只做一次类型级 cons 与一次运行期枚举构造。
/// 使用者通常不需要写 `S`；默认值是 [`TNil`]。
///
/// service 实体直接存放在 [`ServiceValues`] 递归枚举里；因此没有 `dyn Any`、没有
/// downcast，`RpcRouter` 的 `Send` / `Sync` 也由实际 service 类型决定。
///
/// # Examples
///
/// ```ignore
/// let router = RpcRouter::<ApiV1>::new(ctx)
///     .add_service::<GoodsCustomerServiceImpl>(move |_| GoodsCustomerServiceImpl::new())
///     .add_service::<GoodsAdminServiceImpl>(move |_| GoodsAdminServiceImpl::new());
/// ```
pub struct RpcRouter<I, S: TrRpcServiceValues = TNil> {
    /// 已装入的 service 运行期值；顺序与类型列表的头尾对应。
    services_: S::Values,

    /// `I` 只出现在路径与 trait 约束里。
    _interface_: PhantomData<fn() -> I>,
}

impl<I> RpcRouter<I, TNil> {
    /// 用顶层 router 的装配上下文造一个空 router。
    ///
    /// `ctx` 目前不携带数据；保留参数是为了与 `Router::call(|ctx| ...)` 的装配链一致。
    pub const fn new<C>(_ctx: &RouterCtx<C>) -> Self {
        RpcRouter {
            services_: ServiceValues::Nil,
            _interface_: PhantomData,
        }
    }
}

impl<I, S> RpcRouter<I, S>
where
    S: TrRpcServiceValues,
{
    /// 用一个构造闭包装入一个 service 实现。
    ///
    /// `T` 是 service 实现类型；它必须通过 `#[rpc(service_of(I))]` 获得
    /// [`TrRpcService`] 实现。闭包在装配期调用一次，返回的实例由 router 持有。
    ///
    /// 本方法只做一次类型级 cons 与一次运行期枚举构造，因此不受固定元组长度限制。
    pub fn add_service<T>(
        self,
        make_service: impl FnOnce(&RpcServiceCtx) -> T,
    ) -> RpcRouter<I, TCons<T, S>> {
        let ctx = RpcServiceCtx::new();
        let RpcRouter {
            services_,
            _interface_: _,
        } = self;

        RpcRouter {
            services_: ServiceValues::Cons(make_service(&ctx), services_),
            _interface_: PhantomData,
        }
    }
}

impl<I, S> mptp_smux::serving::TrPathHandler for RpcRouter<I, S>
where
    I: TrRpcInterface,
    S: TrRpcServiceValues,
{
    const PATH: &'static str = I::PATH;
}

/// 返回 `prefix` 在 `path` 上的前缀长度；按完整路径段比较。
fn path_prefix_len_(prefix: &str, path: &str) -> Option<usize> {
    let prefix = prefix.trim_matches('/');
    let path = path.trim_matches('/');
    if prefix.is_empty() {
        return Option::Some(0usize);
    }
    match path.strip_prefix(prefix) {
        Option::Some(rest) if rest.is_empty() || rest.starts_with('/') => {
            Option::Some(prefix.len())
        }
        _ => Option::None,
    }
}

/// 剥掉 interface 前缀，返回 service 相对路径。
fn strip_interface_prefix_<'p>(interface: &str, location: &'p str) -> Option<&'p str> {
    let interface = interface.trim_matches('/');
    let location = location.trim_matches('/');
    if interface.is_empty() {
        return Option::Some(location);
    }
    let rest = location.strip_prefix(interface)?;
    if rest.is_empty() {
        return Option::Some("");
    }
    rest.strip_prefix('/')
}

/// 写一条拒绝回复并收尾。
async fn reject_async_<C, K>(
    status: Status,
    message: String,
    location: &str,
    headers: &Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: K,
) -> RpcResult<()>
where
    C: TrServingConfig,
    K: TrCancellationToken,
{
    let mut ctx = RpcCallCtx::<C>::new(location, headers, tx, rx);
    let reply = ctx
        .reply_status_async(status, message)
        .may_cancel_with(cancel.child_token())
        .await;
    ctx.close_async().await;
    reply
}

impl<I, C, S> TrReqHandler<C> for RpcRouter<I, S>
where
    I: TrRpcInterface,
    C: TrServingConfig + 'static,
    S: TrRpcServiceList<I, C>,
{
    type HandleAsync<'f>
        = RpcRouterHandleAsync<'f, 'f, I, C, S>
    where
        Self: 'f;

    type Err = RpcError;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        chan_tx: ChannelTx<C>,
        chan_rx: ChannelRx<C>,
    ) -> Self::HandleAsync<'f> {
        RpcRouterHandleAsync::<'f, 'f, I, C, S>::new(
            self, method, location, headers, chan_tx, chan_rx,
        )
    }
}

/// [`RpcRouter`] 的 step 函数。
#[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
#[gen_may_cancel_future(RpcRouterHandle, pub, new(pub(crate)))]
async fn rpc_router_handle_async_<'f, I, C, S, K>(
    router: &'f RpcRouter<I, S>,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: K,
) -> RpcResult<()>
where
    I: TrRpcInterface,
    C: TrServingConfig + 'f,
    S: TrRpcServiceList<I, C> + 'f,
    K: TrCancellationToken,
{
    if method != AccessMethod::Call {
        return reject_async_::<C, K>(
            Status::MethodNotAllowed,
            "只接受 Call 访问方法".to_string(),
            location,
            headers,
            tx,
            rx,
            cancel,
        )
        .await;
    }

    let Option::Some(rest) = strip_interface_prefix_(I::PATH, location) else {
        return reject_async_::<C, K>(
            Status::NotFound,
            location.to_string(),
            location,
            headers,
            tx,
            rx,
            cancel,
        )
        .await;
    };

    let Option::Some((index, _)) = S::match_service(rest) else {
        return reject_async_::<C, K>(
            Status::NotFound,
            location.to_string(),
            location,
            headers,
            tx,
            rx,
            cancel,
        )
        .await;
    };

    let mut ctx = RpcCallCtx::<C>::new(location, headers, tx, rx);
    let result = S::dispatch_at(&router.services_, index, &mut ctx)
        .may_cancel_with(cancel.child_token())
        .await;
    ctx.close_async().await;
    result
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 interface 前缀剥离按完整路径段工作，并把剩余路径交给 service 匹配。
    /// Test that interface stripping is segment-aware and yields the service-relative path.
    ///
    /// - 手段：用 `api/v1` 剥离 `api/v1/goods`、`api/v10/goods` 与完全相同的路径。
    /// - 判断：第一条得到 `goods`；第二条为 `None`；完全相同得到空串。
    ///
    /// - Means: strip `api/v1` from a normal path, a look-alike prefix and the exact path.
    /// - Verdict: the first yields `goods`, the second `None`, the exact one an empty slice.
    #[test]
    fn strips_interface_prefix_() {
        assert_eq!(
            strip_interface_prefix_("api/v1", "/api/v1/goods"),
            Some("goods")
        );
        assert_eq!(strip_interface_prefix_("api/v1", "/api/v10/goods"), None);
        assert_eq!(strip_interface_prefix_("api/v1", "/api/v1"), Some(""));
    }

    /// 测试 service 前缀匹配按完整路径段工作。
    /// Test that service-prefix matching works on complete segments.
    ///
    /// - 手段：用 `goods` 匹配 `goods`、`goods/1`、`goods2`。
    /// - 判断：前两者命中，最后一个不命中。
    ///
    /// - Means: match `goods` against `goods`, `goods/1` and `goods2`.
    /// - Verdict: the first two hit and the last one does not.
    #[test]
    fn matches_service_prefix_() {
        assert_eq!(path_prefix_len_("goods", "goods"), Some(5usize));
        assert_eq!(path_prefix_len_("goods", "goods/1"), Some(5usize));
        assert_eq!(path_prefix_len_("goods", "goods2"), None);
    }

    /// 测试 tlist 列表没有固定元组长度上限，且运行期用枚举存放实体。
    /// Test that the tlist-based list has no fixed tuple-length limit and stores values in
    /// the recursive enum.
    ///
    /// - 手段：构造 20 个 `DummyService` 的 `RpcRouter`，再断言它实现 `TrReqHandler<C>`。
    /// - 判断：编译通过；运行期不建立连接。
    ///
    /// - Means: build an `RpcRouter` with 20 `DummyService` entries and assert it implements
    ///   `TrReqHandler<C>`.
    /// - Verdict: it compiles; no connection is opened.
    #[allow(dead_code)]
    fn supports_many_services_<C>(ctx: &RouterCtx<C>)
    where
        C: TrServingConfig + 'static,
    {
        struct Api;
        struct DummyService;

        impl TrRpcInterface for Api {
            const PATH: &'static str = "api/v1";
        }

        impl<C> TrRpcService<Api, C> for DummyService
        where
            C: TrServingConfig,
        {
            const SERVICE_PATH: &'static str = "dummy";

            type Err = RpcError;

            fn dispatch_async<'f, X>(
                &'f self,
                _ctx: &'f mut X,
            ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>> + 'f
            where
                C: 'f,
                X: abs_rpc_serv::service::TrRpcCallCtx<C, Err = Self::Err> + 'f,
            {
                ReadyMayCancel::new(Result::Ok(()))
            }
        }

        fn assert_handler<C, H>(_handler: &H)
        where
            C: TrServingConfig + 'static,
            H: TrReqHandler<C>,
        {
        }

        let router = RpcRouter::<Api>::new(ctx)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService)
            .add_service::<DummyService>(|_| DummyService);

        assert_handler::<C, _>(&router);
    }
}
