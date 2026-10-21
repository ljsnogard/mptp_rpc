//! 服务端 job、service 注册项与“把 job 交给 service”的契约。
//!
//! 这一层只描述**一次调用如何被交付**，不规定路径如何匹配：
//!
//! - [`RpcJob`]：一次调用的完整载荷（location、头表、子流收发半边）；
//! - [`ServiceRoute`]：`futures::mpsc` 有界队列的缺省注册项；
//! - [`TrRpcServiceRoute`]：把 job 交给 service 的抽象，允许使用者换成自己的队列或调度器。
//!
//! 路径匹配由 [`TrRpcRouter`](super::router_::TrRpcRouter) 负责；两者刻意分开，这样
//! “router 怎么找”与“找到之后怎么送”可以各自替换。
//!
//! # `route_`: jobs, service registrations and the delivery contract
//!
//! This module only describes how one call is **delivered**, not how a location is matched.
//! [`RpcJob`] is the complete payload, [`ServiceRoute`] is the default bounded-queue
//! registration, and [`TrRpcServiceRoute`] lets a user replace the queue or scheduler.
//! Path matching lives in [`TrRpcRouter`](super::router_::TrRpcRouter).

use abs_cancel::{TrCancellationToken, TrMayCancel};
use futures::{SinkExt, channel::mpsc, future::Either};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    messaging::Headers,
    serving::config::{ChannelRx, ChannelTx},
};
use thiserror::Error;

use crate::{
    serving::TrServingConfig,
    x_deps::{abs_cancel, futures, gen_mcf2, mptp_smux},
};

/// 每个 service 队列的缺省容量。
///
/// 有界队列提供背压：worker 处理不过来时，router 的 `send` 会等待，而不是无限堆积
/// job。容量只是**本实现**的缺省值，使用者可以在 [`ServiceRoute`] / worker 构造时
/// 覆盖它。
pub const K_DEFAULT_SERVICE_QUEUE_CAP: usize = 32usize;

/// 一次交给 service worker 的完整调用。
///
/// `RpcJob` 按值持有两个 channel 半边，因此它可以跨线程移进 worker；worker 在
/// dispatch 结束后负责收尾。
pub struct RpcJob<C: TrServingConfig> {
    /// 完整 location，例如 `/api/v1/customer/goods`；仅用于错误信息与日志。
    location_: String,

    /// 请求前缀里的头表。
    headers_: Headers,

    /// 本子流的发送半边。
    tx_: ChannelTx<C>,

    /// 本子流的接收半边。
    rx_: ChannelRx<C>,
}

impl<C: TrServingConfig> RpcJob<C> {
    /// 由路由结果构造一个 job。
    pub fn new(location: String, headers: Headers, tx: ChannelTx<C>, rx: ChannelRx<C>) -> Self {
        RpcJob {
            location_: location,
            headers_: headers,
            tx_: tx,
            rx_: rx,
        }
    }

    /// 完整 location。
    pub fn location(&self) -> &str {
        &self.location_
    }

    /// 请求头表。
    pub fn headers(&self) -> &Headers {
        &self.headers_
    }

    /// 拆出 worker 需要的全部部分。
    ///
    /// 顺序是 `(location, headers, tx, rx)`；worker 用前两者构造
    /// [`RpcCallCtx`](super::RpcCallCtx)，再用后两者完成收发与收尾。
    pub fn into_parts(self) -> (String, Headers, ChannelTx<C>, ChannelRx<C>) {
        (self.location_, self.headers_, self.tx_, self.rx_)
    }
}

/// 把 job 交给一个 service 时的错误。
///
/// 这是**交付阶段**的错误，不是业务错误：队列关闭、入队被取消，或自定义队列拒绝了
/// job。业务错误仍然由 [`RpcError`](crate::RpcError) 表达。
///
/// # Examples
///
/// ```
/// use core_rpc_mptp_smux_v1::RpcRouteError;
///
/// let err = RpcRouteError::Closed;
/// assert_eq!(err.to_string(), "service 队列已关闭");
/// ```
#[derive(Debug, Error)]
pub enum RpcRouteError {
    /// service 队列已关闭，通常意味着对应 worker 已停止。
    #[error("service 队列已关闭")]
    Closed,

    /// 入队等待被取消令牌中止。
    #[error("入队等待已取消")]
    Cancelled,

    /// 自定义队列或调度器拒绝了 job。
    #[error("service 队列拒绝接收：{0}")]
    Other(String),
}

/// 一条可被 router 注册的 service 目标。
///
/// 缺省实现是 [`ServiceRoute`]（`futures::mpsc` 有界队列）；使用者可以换成自己的
/// 队列、线程池或负载均衡器。`C` 是服务端配置，`path` 是 service 相对路径
/// （例如 `customer/goods`，不含 interface 前缀）。
///
/// 入队等待会传播取消令牌：取消时 job 被丢弃并返回
/// [`RpcRouteError::Cancelled`]。这与“服务端正在关闭，不必再回复”的语义一致。
///
/// # Errors
///
/// [`enqueue_async`](Self::enqueue_async) 在队列关闭、入队被取消或自定义队列拒绝时
/// 返回 [`RpcRouteError`]。
///
/// # Examples
///
/// ```
/// use core_rpc_mptp_smux_v1::serving::{TrRpcServiceRoute, TrServingConfig};
///
/// fn service_path<C, R>(route: &R) -> &'static str
/// where
///     C: TrServingConfig,
///     R: TrRpcServiceRoute<C>,
/// {
///     route.path()
/// }
/// ```
pub trait TrRpcServiceRoute<C>: Sized
where
    C: TrServingConfig,
{
    /// service 相对路径。
    fn path(&self) -> &'static str;

    /// 把一次调用交给该 service。
    fn enqueue_async<'f>(
        &'f self,
        job: RpcJob<C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), RpcRouteError>> + 'f
    where
        C: 'f;
}

/// 一个 service 的缺省注册项：service 相对路径与它的有界队列发送端。
///
/// `#[rpc(service = "...")]` 生成的 worker 用 `Worker::route()` 交出本结构；interface
/// 的 worker 组把它收集成 `Vec<ServiceRoute<C>>`，交给 router 实现注册。
pub struct ServiceRoute<C: TrServingConfig> {
    /// service 相对路径；来自 service trait 的 `SERVICE_PATH`。
    path_: &'static str,

    /// 该 service 的队列发送端。
    tx_: mpsc::Sender<RpcJob<C>>,
}

impl<C: TrServingConfig> ServiceRoute<C> {
    /// 由 service 路径与队列发送端构造注册项。
    ///
    /// 普通使用者不需要直接调用：service 宏生成的 `Worker::route` 已经封装了它。
    pub fn new(path: &'static str, tx: mpsc::Sender<RpcJob<C>>) -> Self {
        ServiceRoute {
            path_: path,
            tx_: tx,
        }
    }

    /// service 相对路径。
    pub const fn path(&self) -> &'static str {
        self.path_
    }

    /// 克隆一份队列发送端。
    pub fn sender(&self) -> mpsc::Sender<RpcJob<C>> {
        self.tx_.clone()
    }
}

impl<C: TrServingConfig> Clone for ServiceRoute<C> {
    fn clone(&self) -> Self {
        ServiceRoute {
            path_: self.path_,
            tx_: self.tx_.clone(),
        }
    }
}

impl<C: TrServingConfig> TrRpcServiceRoute<C> for ServiceRoute<C> {
    fn path(&self) -> &'static str {
        self.path_
    }

    fn enqueue_async<'f>(
        &'f self,
        job: RpcJob<C>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), RpcRouteError>> + 'f
    where
        C: 'f,
    {
        EnqueueJobAsync::<'f, 'f, C>::new(self, job)
    }
}

/// [`TrRpcServiceRoute::enqueue_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(EnqueueJob, pub, new(pub(crate)))]
async fn enqueue_job_async_<'f, C, TyTok>(
    route: &'f ServiceRoute<C>,
    job: RpcJob<C>,
    cancel: TyTok,
) -> Result<(), RpcRouteError>
where
    C: TrServingConfig,
    TyTok: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(RpcRouteError::Cancelled);
    }

    // `futures::mpsc::Sender::send` 需要 `&mut self`，而注册项只暴露 `&self`；克隆发送端
    // 是廉价的（内部是共享队列），并且让这次入队拥有自己的发送端。
    let mut sender = route.tx_.clone();
    let send_fut = sender.send(job);
    let cancel_fut = cancel.child_token().cancellation();
    futures::pin_mut!(send_fut, cancel_fut);

    match futures::future::select(send_fut, cancel_fut).await {
        Either::Left((result, _)) => result.map_err(|_| RpcRouteError::Closed),
        Either::Right((..)) => Result::Err(RpcRouteError::Cancelled),
    }
}
