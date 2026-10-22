//! 路径 router 契约与把它接入 MPTP 服务端的 handler 适配器。
//!
//! [`MptpServer`](mptp_smux::serving::MptpServer) 在解码请求前缀之后，把 `location`、头表
//! 与子流的收发半边按值交给一个 [`TrReqHandler`]。RPC 层把这件事拆成两半：
//!
//! - [`TrRpcRouter`]：**匹配**。把 interface / service 注册项组织起来，并把 `location`
//!   解析成一条 [`TrRpcServiceRoute`]；
//! - [`RpcRouterHandler`]：**接入**。负责 `Call` 检查、404 / 405 拒绝、构造 [`RpcJob`]、
//!   入队与错误映射。
//!
//! 这样使用者可以只实现自己的 router（例如正则、静态表、前缀树），而拒绝回复与收尾逻辑
//! 不必重写。具体实现见 `rpc_router_trie`。
//!
//! 这是**队列版**装配链；不需要队列的内联 router 见
//! [`RpcRouter`](crate::serving::RpcRouter)。宏只面向后者生成 `TrRpcService` 关联。
//!
//! # `router_`: the router contract and its MPTP handler adapter
//!
//! [`TrRpcRouter`] owns only matching. [`RpcRouterHandler`] owns the MPTP-facing behavior:
//! method check, 404 / 405 rejection, job construction and delivery. A user can therefore
//! replace the matching strategy without reimplementing rejection and channel cleanup.

use core::marker::PhantomData;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    serving::TrReqHandler,
    spec::{Status, access_method::AccessMethod},
};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    messaging::Headers,
    serving::config::{ChannelRx, ChannelTx},
};
use thiserror::Error;

use crate::{
    serving::{
        config::TrServingConfig,
        ctx_::RpcCallCtx,
        route_::{RpcJob, RpcRouteError, TrRpcServiceRoute},
    },
    x_deps::{abs_cancel, abs_mptp, gen_mcf2},
};

/// 路径路由器：按 interface / service 匹配一个 `location`。
///
/// 实现者只需要回答两件事：
///
/// 1. 如何把一组 service 注册项挂到某个 interface 下；
/// 2. 如何把一个完整 `location` 解析成命中的 service。
///
/// `method` 检查、未命中时的 `404` / `405` 回复、[`RpcJob`] 构造与入队由
/// [`RpcRouterHandler`] 统一完成。
///
/// # Errors
///
/// [`add_interface`](Self::add_interface) 的错误类型由实现决定；重复注册、路径冲突等
/// 由实现自行报告。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::serving::{TrRpcRouter, TrServingConfig};
///
/// fn register<C, R, I>(
///     router: &mut R,
///     interface: &str,
///     routes: I,
/// ) -> Result<(), R::RegisterErr>
/// where
///     C: TrServingConfig,
///     R: TrRpcRouter<C>,
///     I: IntoIterator<Item = R::Route>,
/// {
///     router.add_interface(interface, routes)
/// }
/// ```
///
/// # 对象安全
///
/// `add_interface` 是泛型方法，因此本 trait 不是对象安全的；router 在装配期已知具体类型，
/// 不需要 `dyn`。若将来确实需要动态 router，应另加一个对象安全的注册接口。
pub trait TrRpcRouter<C>: Sized
where
    C: TrServingConfig,
{
    /// 本 router 接受的 service 注册项。
    type Route: TrRpcServiceRoute<C>;

    /// 注册失败时的错误。
    type RegisterErr: core::error::Error;

    /// 注册一个 interface 及其全部 service。
    ///
    /// 如何解释路径、重复注册是否报错、允许怎样的前缀重叠，由实现决定；缺省 trie 实现
    /// （`rpc_router_trie`）按完整路径段匹配。
    fn add_interface<I>(&mut self, interface: &str, routes: I) -> Result<(), Self::RegisterErr>
    where
        I: IntoIterator<Item = Self::Route>;

    /// 解析一个完整 `location`，返回命中的 service 注册项。
    ///
    /// 返回 `None` 时，[`RpcRouterHandler`] 会写 `404 NotFound`。
    fn resolve(&self, location: &str) -> Option<&Self::Route>;
}

/// 把 [`TrRpcRouter`] 适配成 MPTP 的 [`TrReqHandler`]。
///
/// 它拥有 router，并负责：
///
/// 1. 只接受 [`AccessMethod::Call`]，其它访问方法回 `405`；
/// 2. 用 [`TrRpcRouter::resolve`] 匹配 `location`，未命中回 `404`；
/// 3. 命中后把请求打包成 [`RpcJob`]，交给对应的 [`TrRpcServiceRoute`]；
/// 4. 拒绝路径自己写错误回复并按「写回复 → 半关闭 tx → 读完 rx」收尾。
///
/// 命中 service 后，channel 的收尾由 worker 负责；handler 只完成入队。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::serving::{RpcRouterHandler, TrRpcRouter, TrServingConfig};
///
/// fn wrap<C, R>(router: R) -> RpcRouterHandler<R, C>
/// where
///     C: TrServingConfig,
///     R: TrRpcRouter<C>,
/// {
///     RpcRouterHandler::new(router)
/// }
/// ```
pub struct RpcRouterHandler<R, C>
where
    C: TrServingConfig,
    R: TrRpcRouter<C>,
{
    /// 被适配的 router。
    router_: R,

    /// `C` 只出现在 trait 参数里，不占运行期空间。
    _cfg_: PhantomData<fn() -> C>,
}

impl<R, C> RpcRouterHandler<R, C>
where
    C: TrServingConfig,
    R: TrRpcRouter<C>,
{
    /// 用一个 router 构造 handler。
    pub const fn new(router: R) -> Self {
        RpcRouterHandler {
            router_: router,
            _cfg_: PhantomData,
        }
    }

    /// 取被适配的 router。
    pub const fn router(&self) -> &R {
        &self.router_
    }

    /// 取被适配 router 的可变引用（用于装配期注册）。
    pub fn router_mut(&mut self) -> &mut R {
        &mut self.router_
    }

    /// 转发到 [`TrRpcRouter::add_interface`]。
    pub fn add_interface<I>(&mut self, interface: &str, routes: I) -> Result<(), R::RegisterErr>
    where
        I: IntoIterator<Item = R::Route>,
    {
        self.router_.add_interface(interface, routes)
    }

    /// 转发到 [`TrRpcRouter::resolve`]。
    pub fn resolve(&self, location: &str) -> Option<&R::Route> {
        self.router_.resolve(location)
    }
}

/// 路由与拒绝过程中的错误。
///
/// 注册错误由 router 自己的 [`TrRpcRouter::RegisterErr`] 表达；这里只覆盖**接入
/// MPTP 服务端之后**会发生的失败。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::serving::{RpcDispatchError, RpcRouteError};
///
/// let err = RpcDispatchError::from(RpcRouteError::Closed);
/// assert!(err.to_string().contains("队列已关闭"));
/// ```
#[derive(Debug, Error)]
pub enum RpcDispatchError {
    /// 把 job 交给 service 失败。
    #[error(transparent)]
    Route(#[from] RpcRouteError),

    /// 写拒绝回复失败。
    #[error("写拒绝回复失败：{0}")]
    Reject(String),
}

impl<R, C> TrReqHandler<C> for RpcRouterHandler<R, C>
where
    C: TrServingConfig,
    R: TrRpcRouter<C>,
{
    type HandleAsync<'f>
        = RouterHandleAsync<'f, 'f, R, C>
    where
        Self: 'f;

    type Err = RpcDispatchError;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        chan_tx: ChannelTx<C>,
        chan_rx: ChannelRx<C>,
    ) -> Self::HandleAsync<'f> {
        RouterHandleAsync::new(self, method, location, headers, chan_tx, chan_rx)
    }
}

/// [`RpcRouterHandler`] 的 step 函数。
#[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
#[gen_may_cancel_future(RouterHandle, pub, new(pub(crate)))]
async fn router_handle_async_<'f, R, C, TyTok>(
    handler: &'f RpcRouterHandler<R, C>,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: TyTok,
) -> Result<(), RpcDispatchError>
where
    R: TrRpcRouter<C>,
    C: TrServingConfig,
    TyTok: TrCancellationToken,
{
    if method != AccessMethod::Call {
        return reject_async_::<C, TyTok>(
            Status::MethodNotAllowed,
            "只接受 Call 访问方法",
            location,
            headers,
            tx,
            rx,
            cancel,
        )
        .await;
    }

    let Some(route) = handler.router_.resolve(location) else {
        return reject_async_::<C, TyTok>(
            Status::NotFound,
            location,
            location,
            headers,
            tx,
            rx,
            cancel,
        )
        .await;
    };

    let job = RpcJob::new(location.to_owned(), headers.clone(), tx, rx);
    route
        .enqueue_async(job)
        .may_cancel_with(cancel.child_token())
        .await?;
    Result::Ok(())
}

/// 写一条错误回复并按序关闭 channel。
///
/// 这是 router 自己收尾的路径：请求没有进入任何 worker，所以收尾也在这里完成。
async fn reject_async_<C, TyTok>(
    status: Status,
    message: &str,
    location: &str,
    headers: &Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: TyTok,
) -> Result<(), RpcDispatchError>
where
    C: TrServingConfig,
    TyTok: TrCancellationToken,
{
    let mut ctx = RpcCallCtx::<C>::new(location, headers, tx, rx);
    let reply = ctx
        .reply_status_async(status, message.to_owned())
        .may_cancel_with(cancel.child_token())
        .await;
    ctx.close_async().await;
    reply.map_err(|err| RpcDispatchError::Reject(err.to_string()))
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::serving::ServiceRoute;

    /// 一个只用于编译期检查的自定义 router。
    struct DummyRouter_;

    impl<C: TrServingConfig> TrRpcRouter<C> for DummyRouter_ {
        type Route = ServiceRoute<C>;

        type RegisterErr = core::convert::Infallible;

        fn add_interface<I>(
            &mut self,
            _interface: &str,
            _routes: I,
        ) -> Result<(), Self::RegisterErr>
        where
            I: IntoIterator<Item = Self::Route>,
        {
            Result::Ok(())
        }

        fn resolve(&self, _location: &str) -> Option<&Self::Route> {
            Option::None
        }
    }

    /// 编译期验证任意 `TrRpcRouter` 都能被 `RpcRouterHandler` 适配成 `TrReqHandler`。
    /// Compile-time check that any `TrRpcRouter` can be adapted into a `TrReqHandler`.
    ///
    /// - 手段：用一个只返回 `None` 的 `DummyRouter_` 构造 `RpcRouterHandler`，再把它交给
    ///   一个只要求 `H: TrReqHandler<C>` 的泛型断言函数。
    /// - 判断：编译通过；运行期不建立连接，因此这里没有 `#[test]` 入口。
    ///
    /// - Means: build an `RpcRouterHandler` around `DummyRouter_` and pass it to a generic
    ///   assertion that only requires `H: TrReqHandler<C>`.
    /// - Verdict: it compiles; no connection is opened, so this is a compile-only helper.
    #[allow(dead_code)]
    fn custom_router_fits_handler_<C>(router: DummyRouter_)
    where
        C: TrServingConfig,
    {
        fn assert_handler_<C, H>(_handler: &H)
        where
            C: TrServingConfig,
            H: TrReqHandler<C>,
        {
        }

        let handler = RpcRouterHandler::<DummyRouter_, C>::new(router);
        assert_handler_::<C, _>(&handler);
    }

    /// 测试接入阶段的错误能区分入队失败与拒绝回复失败。
    /// Test that dispatch errors distinguish delivery failures from rejection failures.
    ///
    /// - 手段：构造 `Route(Closed)` 与 `Reject` 两种错误并格式化。
    /// - 判断：输出分别包含“队列已关闭”和“拒绝回复”。
    ///
    /// - Means: build `Route(Closed)` and `Reject` and format them.
    /// - Verdict: the messages mention the closed queue and the rejection reply respectively.
    #[test]
    fn displays_dispatch_errors_() {
        let route = RpcDispatchError::from(RpcRouteError::Closed);
        assert!(route.to_string().contains("队列已关闭"));

        let reject = RpcDispatchError::Reject("boom".to_string());
        assert!(reject.to_string().contains("拒绝回复"));
    }
}
