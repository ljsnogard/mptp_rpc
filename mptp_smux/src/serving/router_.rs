//! 路径前缀路由：把请求的 `location` 映射到一个 handler。
//!
//! 这是「单 handler 分派」的载体——路由命中之后框架直接调用**那一个** handler，不提供链式
//! 中间件。理由：`abs_mptp` 的 handler 契约已经把「写回复」的权力交给了 handler 自己
//! （它拿到收发两边），链式语义（谁来写最终回复、谁来收尾）在没有回复类型的前提下没有唯一
//! 答案，硬造一条链只会把这件事变成框架的猜谜。
//!
//! 匹配规则是**最长前缀**：登记 `/topic` 与 `/topic/chat` 时，`/topic/chat/42` 命中后者。
//! 同一前缀重复登记时，**先登记的那条**生效。
//!
//! # 两个层次的 router
//!
//! - [`Router`]：MPTP 协议层的顶层 router。它把 `location` 按前缀交给一个具体的 handler，
//!   自身也实现 [`TrReqHandler`]，因此可以直接交给
//!   [`MptpServer`](crate::serving::MptpServer)；
//! - RPC 层的 `RpcRouter`（在 `rpc_core_v1::serving`）：实现
//!   [`TrPathHandler`]，提供 interface 前缀；`Router::call` 用它登记一个 interface。
//!
//! `Router` 只回答「哪个 handler」，不解释 RPC 错误体；未命中时返回
//! [`RouterError::NotFound`]，由上层决定如何上报。
//!
//! # `router_`: path routers and the top-level MPTP router
//!
//! [`Router`] is the MPTP-level router. It matches by longest path prefix, owns a
//! heterogeneous tuple of handlers, and itself implements [`TrReqHandler`] so it can be
//! passed to [`MptpServer`](crate::serving::MptpServer). The RPC-level `RpcRouter` lives in
//! the core crate and implements [`TrPathHandler`] to advertise its interface prefix.

use core::marker::PhantomData;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{serving::TrReqHandler, spec::access_method::AccessMethod};
use gen_mcf2::gen_may_cancel_future;
use thiserror::Error;

use crate::{
    messaging::Headers,
    serving::config::{ChannelRx, ChannelTx, TrConfig},
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
    type Handler;

    fn match_path<'f>(&'f self, path: &'f str) -> Option<&'f Self::Handler>;
}

/// 可在装配期继续登记的路径 router。
pub trait TrPathRouterMut
where
    Self: TrPathRouter,
{
    type Err: core::error::Error;

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

/// 一条已登记的顶层路由。
///
/// 它只出现在 [`Router`] 的类型参数里；使用者不需要直接构造。
#[doc(hidden)]
pub struct Route<H> {
    /// 路径前缀。
    prefix_: &'static str,

    /// 命中的 handler。
    handler_: H,
}

/// MPTP 协议层的顶层 router。
///
/// 它按最长路径前缀把请求分派给一个 handler，自身也实现 [`TrReqHandler`]；因此
/// `MptpServer::new(router)` 不需要再包一层适配器。`C` 是服务端配置类型，`R` 是内部的
/// 路由元组（由 [`Router::call`] / [`Router::route`] 逐步扩展）。
///
/// 当前实现把路由元组展开到 8 条；更多 interface 时扩展本文件的宏，或改用自定义
/// `TrPathRouter` / `TrReqHandler`。
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
pub struct Router<C, R = ()> {
    /// 已登记的路由元组。
    routes_: R,

    /// `C` 只出现在 handler 的约束里。
    _cfg_: PhantomData<fn() -> C>,
}

impl<C> Router<C, ()> {
    /// 造一个空的顶层 router。
    pub const fn new() -> Self {
        Router {
            routes_: (),
            _cfg_: PhantomData,
        }
    }
}

impl<C> Default for Router<C, ()> {
    fn default() -> Self {
        Self::new()
    }
}

/// 为不同元组长度生成 `route` / `call` 的装配方法。
macro_rules! impl_router_build {
    ($($idx:tt => $ty:ident),*) => {
        impl<C, $($ty),*> Router<C, ($(Route<$ty>,)*)> {
            /// 登记一条带显式路径前缀的路由。
            ///
            /// 前缀按完整的 `/` 段匹配；同一前缀重复登记时，**先登记的那条**生效。
            pub fn route<H, F>(
                self,
                prefix: &'static str,
                make_handler: F,
            ) -> Router<C, ($(Route<$ty>,)* Route<H>,)>
            where
                F: FnOnce(&RouterCtx<C>) -> H,
            {
                let ctx = RouterCtx::<C>::new();
                Router {
                    routes_: (
                        $(self.routes_.$idx,)*
                        Route {
                            prefix_: prefix.trim_matches('/'),
                            handler_: make_handler(&ctx),
                        },
                    ),
                    _cfg_: PhantomData,
                }
            }

            /// 登记一个实现 [`TrPathHandler`] 的 handler，路径取它的 `PATH`。
            ///
            /// 名字沿用 RPC 场景里的 `Call`；本层只按路径分派，不检查访问方法——真正的
            /// `AccessMethod::Call` 检查由 RPC handler（如 `RpcRouter`）完成。
            pub fn call<H, F>(
                self,
                make_handler: F,
            ) -> Router<C, ($(Route<$ty>,)* Route<H>,)>
            where
                H: TrPathHandler,
                F: FnOnce(&RouterCtx<C>) -> H,
            {
                self.route(H::PATH, make_handler)
            }
        }
    };
}

impl_router_build!();
impl_router_build!(0 => S0);
impl_router_build!(0 => S0, 1 => S1);
impl_router_build!(0 => S0, 1 => S1, 2 => S2);
impl_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3);
impl_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4);
impl_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5);
impl_router_build!(0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5, 6 => S6);
impl_router_build!(
    0 => S0, 1 => S1, 2 => S2, 3 => S3, 4 => S4, 5 => S5, 6 => S6, 7 => S7
);

impl<C, R> TrPathRouter for Router<C, R> {
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

/// 返回 `prefix` 在 `path` 上的前缀长度；按完整路径段比较。
///
/// 空前缀匹配一切（长度为 0）；`api/v1` 不会命中 `api/v10`。
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

/// 为不同元组长度生成顶层 `TrReqHandler` 实现。
macro_rules! impl_router_handler {
    ($name:ident, $handle_ty:ident, $handle_fn:ident, $($idx:tt => $ty:ident),+) => {
        impl<C, $($ty),+> TrReqHandler<C> for Router<C, ($(Route<$ty>,)+)>
        where
            C: TrConfig,
            $($ty: TrReqHandler<C>),+
        {
            type HandleAsync<'f>
                = $handle_ty<'f, 'f, C, $($ty),+>
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
                $handle_ty::<'f, 'f, C, $($ty),+>::new(
                    self,
                    method,
                    location,
                    headers,
                    chan_tx,
                    chan_rx,
                )
            }
        }

        /// [`Router`] 的 step 函数。
        #[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
        #[gen_may_cancel_future($name, pub, new(pub(crate)))]
        async fn $handle_fn<'f, C, $($ty),+, K>(
            router: &'f Router<C, ($(Route<$ty>,)+)>,
            method: AccessMethod,
            location: &'f str,
            headers: &'f mut Headers,
            tx: ChannelTx<C>,
            rx: ChannelRx<C>,
            cancel: K,
        ) -> Result<(), RouterError>
        where
            C: TrConfig,
            $($ty: TrReqHandler<C>),+,
            K: TrCancellationToken,
        {
            let mut best: Option<(usize, usize)> = Option::None;
            $(
                if let Option::Some(len) =
                    path_prefix_len_(router.routes_.$idx.prefix_, location)
                {
                    if best.is_none_or(|(_, best_len)| len > best_len) {
                        best = Option::Some(($idx, len));
                    }
                }
            )+

            let Option::Some((index, _)) = best else {
                return Result::Err(RouterError::NotFound(location.to_string()));
            };

            match index {
                $(
                    $idx => router.routes_.$idx.handler_
                        .handle_async(method, location, headers, tx, rx)
                        .may_cancel_with(cancel.child_token())
                        .await
                        .map_err(|err| RouterError::Handler(err.to_string())),
                )+
                _ => Result::Err(RouterError::NotFound(location.to_string())),
            }
        }
    };
}

impl_router_handler!(TopRouterHandle1, TopRouterHandle1Async, top_router_handle_1_async_, 0 => S0);
impl_router_handler!(TopRouterHandle2, TopRouterHandle2Async, top_router_handle_2_async_, 0 => S0, 1 => S1);
impl_router_handler!(
    TopRouterHandle3,
    TopRouterHandle3Async,
    top_router_handle_3_async_,
    0 => S0,
    1 => S1,
    2 => S2
);
impl_router_handler!(
    TopRouterHandle4,
    TopRouterHandle4Async,
    top_router_handle_4_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3
);
impl_router_handler!(
    TopRouterHandle5,
    TopRouterHandle5Async,
    top_router_handle_5_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4
);
impl_router_handler!(
    TopRouterHandle6,
    TopRouterHandle6Async,
    top_router_handle_6_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4,
    5 => S5
);
impl_router_handler!(
    TopRouterHandle7,
    TopRouterHandle7Async,
    top_router_handle_7_async_,
    0 => S0,
    1 => S1,
    2 => S2,
    3 => S3,
    4 => S4,
    5 => S5,
    6 => S6
);
impl_router_handler!(
    TopRouterHandle8,
    TopRouterHandle8Async,
    top_router_handle_8_async_,
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

    /// 测试前缀匹配按完整路径段工作。
    /// Test that prefix matching works on complete path segments.
    ///
    /// - 手段：分别用 `api/v1` 匹配 `api/v1`、`/api/v1/`、`api/v10` 与 `api`。
    /// - 判断：前两者得到前缀长度，后两者不命中。
    ///
    /// - Means: match `api/v1` against an exact path, a slashed path, `api/v10` and `api`.
    /// - Verdict: the first two hit; the last two do not.
    #[test]
    fn matches_complete_segments_() {
        assert_eq!(path_prefix_len_("api/v1", "api/v1"), Some(6usize));
        assert_eq!(path_prefix_len_("/api/v1/", "/api/v1/goods"), Some(6usize));
        assert_eq!(path_prefix_len_("api/v1", "api/v10"), None);
        assert_eq!(path_prefix_len_("api/v1", "api"), None);
        assert_eq!(path_prefix_len_("", "anything"), Some(0usize));
    }

    /// 测试顶层 Router 按最长前缀分派并返回自身作为 handler。
    /// Test that the top-level Router dispatches by longest prefix and returns itself.
    ///
    /// - 手段：构造一个带 `api` 与 `api/v1` 两条路由的 Router。
    /// - 判断：`match_path` 返回自身；路径匹配辅助函数选择更长前缀。
    ///
    /// - Means: build a Router with `api` and `api/v1` rules.
    /// - Verdict: `match_path` returns itself and the helper picks the longer prefix.
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
        assert!(
            path_prefix_len_("api/v1", "api/v1/goods").unwrap()
                > path_prefix_len_("api", "api/v1/goods").unwrap()
        );
    }
}
