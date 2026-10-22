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
//! 顶层 interface 前缀由 [`Router`](mptp_smux::serving::Router) 按
//! [`TrPathHandler::PATH`](mptp_smux::serving::TrPathHandler::PATH) 分派；`RpcRouter`
//! 自身在匹配 service 前会剥掉 `I::PATH`。
//!
//! 当前实现用类型级元组保存 service，元组长度展开到 8；需要一个 interface 下超过 8 个
//! service 时，可以扩展本文件的宏，或改用自定义 `TrReqHandler`。
//!
//! # `rpc_router_`: the interface-level inline RPC router
//!
//! One [`RpcRouter`] serves one interface. It owns the concrete service implementations,
//! dispatches inline (no queues, no spawning), strips the interface prefix, and writes
//! `404` / `405` replies itself. Users register services with [`RpcRouter::add_service`].

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
        RouterCtx,
        config::{ChannelRx, ChannelTx},
    },
};

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

/// 一个 interface 的 RPC router。
///
/// `I` 是 interface marker，`S` 是 service 实现的元组（由 [`add_service`](Self::add_service)
/// 逐步增长）。router 只在装配期构建；请求处理只读地使用已装好的 service。
///
/// # Examples
///
/// ```ignore
/// let router = RpcRouter::<ApiV1>::new(ctx)
///     .add_service::<GoodsCustomerServiceImpl>(move |_| GoodsCustomerServiceImpl::new())
///     .add_service::<GoodsAdminServiceImpl>(move |_| GoodsAdminServiceImpl::new());
/// ```
pub struct RpcRouter<I, S = ()> {
    /// 已装入的 service 实现元组。
    services_: S,

    /// `I` 只出现在路径与 trait 约束里。
    _interface_: PhantomData<fn() -> I>,
}

impl<I> RpcRouter<I, ()> {
    /// 用顶层 router 的装配上下文造一个空 router。
    ///
    /// `ctx` 目前不携带数据；保留参数是为了与 `Router::call(|ctx| ...)` 的装配链一致。
    pub const fn new<C>(_ctx: &RouterCtx<C>) -> Self {
        RpcRouter {
            services_: (),
            _interface_: PhantomData,
        }
    }
}

/// 为不同元组长度生成 `add_service`。
macro_rules! impl_rpc_router_build {
    ($($idx:tt => $ty:ident),*) => {
        impl<I, $($ty),*> RpcRouter<I, ($($ty,)*)> {
            /// 用一个构造闭包装入一个 service 实现。
            ///
            /// `T` 是 service 实现类型；它必须通过 `#[rpc(service_of(I))]` 获得
            /// [`TrRpcService`] 实现。闭包在装配期调用一次，返回的实例由 router 持有。
            pub fn add_service<T>(
                self,
                make_service: impl FnOnce(&RpcServiceCtx) -> T,
            ) -> RpcRouter<I, ($($ty,)* T,)> {
                let ctx = RpcServiceCtx::new();
                RpcRouter {
                    services_: (
                        $(self.services_.$idx,)*
                        make_service(&ctx),
                    ),
                    _interface_: PhantomData,
                }
            }
        }
    };
}

impl_rpc_router_build!();
impl_rpc_router_build!(0 => S0);
impl_rpc_router_build!(0 => S0, 1 => S1);
impl_rpc_router_build!(0 => S0, 1 => S1, 2 => S2);
impl_rpc_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3);
impl_rpc_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4);
impl_rpc_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5);
impl_rpc_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5, 6 => S6);
impl_rpc_router_build!(
    0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5, 6 => S6, 7 => S7
);

impl<I, S> mptp_smux::serving::TrPathHandler for RpcRouter<I, S>
where
    I: TrRpcInterface,
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

/// 为不同元组长度生成 `TrReqHandler` 实现。
macro_rules! impl_rpc_router_handler {
    ($name:ident, $handle_ty:ident, $handle_fn:ident $(, $idx:tt => $ty:ident)*) => {
        impl<I, C, $($ty),*> TrReqHandler<C> for RpcRouter<I, ($($ty,)*)>
        where
            I: TrRpcInterface,
            C: TrServingConfig + 'static,
            $($ty: TrRpcService<I, C, Err = RpcError>),*
        {
            type HandleAsync<'f>
                = $handle_ty<'f, 'f, I, C, $($ty),*>
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
                $handle_ty::<'f, 'f, I, C, $($ty),*>::new(
                    self,
                    method,
                    location,
                    headers,
                    chan_tx,
                    chan_rx,
                )
            }
        }

        /// [`RpcRouter`] 的 step 函数。
        #[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
        #[gen_may_cancel_future($name, pub, new(pub(crate)))]
        async fn $handle_fn<'f, I, C, $($ty,)* K>(
            router: &'f RpcRouter<I, ($($ty,)*)>,
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
            $($ty: TrRpcService<I, C, Err = RpcError>,)*
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

            let mut best: Option<(usize, usize)> = Option::None;
            $(
                if let Option::Some(len) =
                    path_prefix_len_(<$ty as TrRpcService<I, C>>::SERVICE_PATH, rest)
                {
                    if best.is_none_or(|(_, best_len)| len > best_len) {
                        best = Option::Some(($idx, len));
                    }
                }
            )*

            let Option::Some((index, _)) = best else {
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
            let result = match index {
                $(
                    $idx => <$ty as TrRpcService<I, C>>::dispatch_async(
                        &router.services_.$idx,
                        &mut ctx,
                    )
                    .may_cancel_with(cancel.child_token())
                    .await,
                )*
                _ => Result::Ok(()),
            };
            ctx.close_async().await;
            result
        }
    };
}

impl_rpc_router_handler!(RpcRouterHandle1, RpcRouterHandle1Async, rpc_router_handle_1_async_, 0 => S0);
impl_rpc_router_handler!(RpcRouterHandle2, RpcRouterHandle2Async, rpc_router_handle_2_async_, 0 => S0, 1 => S1);
impl_rpc_router_handler!(
    RpcRouterHandle3,
    RpcRouterHandle3Async,
    rpc_router_handle_3_async_,
    0 => S0,
    1 => S1,
    2 => S2
);
impl_rpc_router_handler!(
    RpcRouterHandle4,
    RpcRouterHandle4Async,
    rpc_router_handle_4_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3
);
impl_rpc_router_handler!(
    RpcRouterHandle5,
    RpcRouterHandle5Async,
    rpc_router_handle_5_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4
);
impl_rpc_router_handler!(
    RpcRouterHandle6,
    RpcRouterHandle6Async,
    rpc_router_handle_6_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4,
    5 => S5
);
impl_rpc_router_handler!(
    RpcRouterHandle7,
    RpcRouterHandle7Async,
    rpc_router_handle_7_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4,
    5 => S5,
    6 => S6
);
impl_rpc_router_handler!(
    RpcRouterHandle8,
    RpcRouterHandle8Async,
    rpc_router_handle_8_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4,
    5 => S5,
    6 => S6,
    7 => S7
);

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
}
