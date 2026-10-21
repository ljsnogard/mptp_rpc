//! 服务端 job、service 注册项与两级 Trie 路由器。
//!
//! [`MptpServer`](mptp_smux::serving::MptpServer) 在解码请求前缀之后，把 `location`、头表
//! 与子流的收发半边按值交给一个 [`TrReqHandler`]。RPC 层的那一个 handler 就是
//! [`RpcRouter`]：
//!
//! 1. 只接受 `AccessMethod::Call`；其它访问方法直接回 `405`；
//! 2. 用 interface 前缀在 [`PathTrie`](super::trie_::PathTrie) 中做最长段匹配；
//! 3. 在命中的 interface 内部，用剩余路径匹配 service 前缀；
//! 4. 把一次请求打包成 [`RpcJob`]，通过该 service 的队列交给 worker。
//!
//! 宏不生成任何方法级 `match`：interface / service 两级分派全部由运行期注册表完成，
//! 方法分派仍然是 worker 里 body enum 的 `match`。
//!
//! # 收尾责任
//!
//! 命中 service 时，两个 channel 半边随 [`RpcJob`] 一起交给 worker，收尾也由 worker
//! 完成（写回复 → 半关闭 `tx` → 读完 `rx`）。路由未命中或方法不允许时，router 自己写
//! 错误回复并按同样顺序关闭 channel。
//!
//! # `router_`: jobs, service routes and the two-level trie router
//!
//! The router is the single [`TrReqHandler`] registered under the empty prefix. It rejects
//! non-`Call` methods, matches the interface and then the service by complete path segments,
//! and forwards one [`RpcJob`] to the service queue. Channel cleanup belongs to the worker for
//! routed jobs, and to the router itself for rejected requests.

use futures::{SinkExt, channel::mpsc};

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

use crate::x_deps::{abs_cancel, abs_mptp, gen_mcf2};
use super::{
    config::TrServingConfig,
    ctx_::RpcCallCtx,
    trie_::{PathTrie, split_segments_},
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
    pub fn new(
        location: String,
        headers: Headers,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> Self {
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
    /// 顺序是 `(location, headers, tx, rx)`；worker 用前两者构造 [`RpcCallCtx`]，再用后
    /// 两者完成收发与收尾。
    pub fn into_parts(self) -> (String, Headers, ChannelTx<C>, ChannelRx<C>) {
        (self.location_, self.headers_, self.tx_, self.rx_)
    }
}

/// 一个 service 的注册项：service 相对路径与它的有界队列发送端。
///
/// `#[rpc(service = "...")]` 生成的 worker 用 `Worker::route()` 交出本结构；interface
/// 的 worker 组把它收集成 `Vec<ServiceRoute<C>>`，交给
/// [`RpcRouter::add_interface`]。
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

/// interface 级路由项。
struct InterfaceEntry_<C: TrServingConfig> {
    /// 该 interface 下的 service Trie。
    services_: PathTrie<ServiceRoute<C>>,
}

/// 服务端两级 Trie 路由器；它本身是一个 [`TrReqHandler`]。
///
/// 一个 server 可以注册多个 interface（`ApiV1`、`ApiV2`……），每个 interface 内部再按
/// service 的 `SERVICE_PATH` 注册。路由匹配失败不会把 channel 交给 worker，而是由本结构
/// 写 `404` / `405` 后收尾。
pub struct RpcRouter<C: TrServingConfig> {
    /// interface 级 Trie。
    interfaces_: PathTrie<InterfaceEntry_<C>>,
}

impl<C: TrServingConfig> RpcRouter<C> {
    /// 造一个空 router。
    pub fn new() -> Self {
        RpcRouter {
            interfaces_: PathTrie::new(),
        }
    }

    /// 注册一个 interface 及其全部 service。
    ///
    /// interface 路径与 service 路径都按完整路径段比较；同一个 interface 或同一个
    /// interface 下的同一个 service 重复注册会返回错误，且不会改动已有注册表。
    ///
    /// # Errors
    ///
    /// - [`RpcRouterError::DuplicateInterface`]：interface 路径已经注册；
    /// - [`RpcRouterError::DuplicateService`]：同一 interface 下的 service 路径重复。
    pub fn add_interface<I>(&mut self, interface: &str, routes: I) -> Result<(), RpcRouterError>
    where
        I: IntoIterator<Item = ServiceRoute<C>>,
    {
        if self.interfaces_.contains(interface) {
            return Err(RpcRouterError::DuplicateInterface(interface.to_string()));
        }

        let services = build_service_trie_(routes.into_iter().map(|route| (route.path(), route)))
            .map_err(|service| RpcRouterError::DuplicateService {
            interface: interface.to_string(),
            service: service.to_string(),
        })?;

        self.interfaces_.insert(
            interface,
            InterfaceEntry_ {
                services_: services,
            },
        );
        Ok(())
    }

    /// 解析一个完整 location，返回命中 service 的注册项。
    ///
    /// 先匹配 interface 的最长前缀，再在剩余段上匹配 service 的最长前缀。剩余段之后
    /// 还有内容时仍然命中——service 路径不是请求的终点，真正的方法由 body enum 决定。
    pub fn resolve(&self, location: &str) -> Option<&ServiceRoute<C>> {
        let segments = split_segments_(location);
        let (interface, depth) = self.interfaces_.match_longest(&segments)?;
        let (service, _) = interface.services_.match_longest(&segments[depth..])?;
        Some(service)
    }
}

impl<C: TrServingConfig> Default for RpcRouter<C> {
    fn default() -> Self {
        Self::new()
    }
}

/// 路由器注册与分派过程中的错误。
#[derive(Debug, Error)]
pub enum RpcRouterError {
    /// interface 路径已经注册。
    #[error("interface 路径 `{0}` 已注册")]
    DuplicateInterface(String),

    /// 同一 interface 下的 service 路径重复。
    #[error("interface `{interface}` 下的 service 路径 `{service}` 已注册")]
    DuplicateService {
        /// interface 路径。
        interface: String,

        /// 重复的 service 路径。
        service: String,
    },

    /// service 队列已关闭，通常意味着对应 worker 已停止。
    #[error("service 队列已关闭")]
    QueueClosed,

    /// 写拒绝回复失败。
    #[error("写拒绝回复失败：{0}")]
    Reject(String),
}

/// 用 service 路径与值建一棵 service Trie；路径重复时返回重复的那条路径。
///
/// 抽成泛型函数是为了能用整数做单元测试：路由表的重复检测与路径规范化不依赖
/// `ServiceRoute<C>` 里的 channel 类型。
fn build_service_trie_<T>(
    routes: impl IntoIterator<Item = (&'static str, T)>,
) -> Result<PathTrie<T>, &'static str> {
    let mut services = PathTrie::new();
    for (path, value) in routes {
        if services.insert(path, value).is_some() {
            return Err(path);
        }
    }
    Ok(services)
}

impl<C: TrServingConfig> TrReqHandler<C> for RpcRouter<C> {
    type HandleAsync<'f> = RouterHandleAsync<'f, 'f, C>
    where
        Self: 'f;

    type Err = RpcRouterError;

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

/// [`RpcRouter::handle_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes, clippy::too_many_arguments)]
#[gen_may_cancel_future(RouterHandle, pub, new(pub(crate)))]
async fn router_handle_async_<'f, C, TyTok>(
    router: &'f RpcRouter<C>,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: ChannelTx<C>,
    rx: ChannelRx<C>,
    cancel: TyTok,
) -> Result<(), RpcRouterError>
where
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

    let Some(route) = router.resolve(location) else {
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
        .sender()
        .send(job)
        .await
        .map_err(|_| RpcRouterError::QueueClosed)
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
) -> Result<(), RpcRouterError>
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
    reply.map_err(|err| RpcRouterError::Reject(err.to_string()))
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 service Trie 构建会拒绝重复路径并保留规范化后的最长匹配。
    /// Test that service-trie construction rejects duplicates and keeps normalised paths.
    ///
    /// - 手段：分别用 `["/api/v1/", 1]`、`["api", 2], ["api/v1", 3]` 与重复 `"api"`
    ///   构建 Trie。
    /// - 判断：前两者成功且最长匹配正确；重复路径返回 `Err("api")`。
    ///
    /// - Means: build tries from a slashed path, an overlapping pair, and a duplicate pair.
    /// - Verdict: the first two succeed with the right longest match; the duplicate reports
    ///   the offending path.
    #[test]
    fn builds_service_trie_() {
        let trie = build_service_trie_([("/api/v1/", 1u32)]).expect("路径应当合法");
        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&1u32, 2usize)));

        let trie = build_service_trie_([("api", 2u32), ("api/v1", 3u32)])
            .expect("重叠路径按最长匹配合法共存");
        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&3u32, 2usize)));
        assert_eq!(trie.match_longest(&["api", "v2"]), Some((&2u32, 1usize)));

        let duplicate = build_service_trie_([("api", 1u32), ("/api/", 2u32)]);
        assert_eq!(duplicate.err(), Some("/api/"));
    }
}
