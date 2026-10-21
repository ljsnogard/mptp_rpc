//! 服务端侧：**按路径路由 + 按类型标识分派**。
//!
//! 这里演示真实 RPC 里最容易被含糊过去的一步：**对端如何确定发过来的东西是什么类型**。
//!
//! 两条路径走的是同一套流程：
//!
//! 1. 头里没声明类型标识 → 回 `400`（不猜）；
//! 2. 声明的标识与本 handler 期待的类型不符 → 回 `406`，**不解码**；
//! 3. 对上了才解码，解出来的就是本 handler 写下的那个类型。
//!
//! 标识由类型自己给出（[`TypeId64`](crate::protocol_::TypeId64) 的稳定哈希），这里没有任何
//! 手写常量。
//!
//! # 服务端这一侧由 `mptp_smux` 提供什么
//!
//! 建流、**ring 内存的准备**与请求前缀解码由 [`MptpServer`] 负责；路由表由本 demo 的
//! [`Router`] 提供，`MptpServer` 经
//! [`TrPathRouter`](mptp_smux::serving::TrPathRouter) 契约调用它。本文件只做业务：读体、
//! 校验标识、解码、装配回复、写回复。前缀的读 / 写入口都来自 `mptp_smux::messaging`，
//! demo 不再自己实现线格式。
//!
//! # `server_`: routing plus type-directed dispatch
//!
//! Channel setup, ring preparation and request-prefix decoding belong to `mptp_smux`; the
//! demo provides the router via the `TrPathRouter` contract. This file only does business:
//! read the body, check the declared identifier, decode, build and write the reply.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_mptp::{
    serving::TrReqHandler,
    spec::{Status, access_method::AccessMethod},
};
use abs_smux::conn::TrConnection;
use anyhow::{Result, anyhow};
use futures::future::join_all;
use gen_mcf2::gen_may_cancel_future;
use mm_ptr::Shared;
use mptp_smux::{
    messaging::{Headers, ResponsePrefix, write_response_prefix_async},
    serving::{
        MptpServer, close_channel_async,
        config::{ChannelRx, ChannelTx},
    },
    x_deps::{abs_cancel, abs_mm, abs_smux, gen_mcf2, mm_ptr},
};
use thiserror::Error;

use crate::{
    body_::{decode_slice_async_, read_body_async_, write_body_async_},
    cfg_::{Dock, ServingCfg},
    conn_::Conn,
    protocol_::{
        EchoReply, EchoRequest, K_ECHO_PATH, K_SUM_PATH, SumReply, SumRequest, response_empty_,
        response_with_, type_id_of_,
    },
    router_::Router,
};

/// 入向邀请最多同时挂起多少条。
const K_LISTEN_RESERVE: usize = 8usize;

/// 等各条监听循环结束的兜底超时。
const K_SERVER_IDLE: Duration = Duration::from_secs(5u64);

/// 计数达到之后、收尾之前的静默期。
///
/// 见 `serve_` 的收尾说明：handler 返回时响应可能还没排空上网。
const K_DRAIN_PAUSE: Duration = Duration::from_millis(500u64);

/// handler 处理过程中的错误。
#[derive(Debug, Error)]
pub enum HandlerError {
    /// 读请求体失败。
    #[error("读请求体失败：{0}")]
    ReadBody(String),

    /// 请求体解码失败。
    #[error("解码请求体失败：{0}")]
    Decode(String),

    /// 装配回复失败。
    #[error("装配回复失败：{0}")]
    Build(String),

    /// 写回复失败。
    #[error("写回复失败：{0}")]
    Write(String),
}

/// 处理计数：服务端用它判断「客户端要的这几条都办完了」。
///
/// 它是 demo 的收尾手段，不是 `mptp_smux` 的一部分：框架的监听循环是**常驻**的
/// （一条连接上可以持续来子流），所以「什么时候可以退出」只有使用者知道。
pub struct Metrics {
    /// 已处理完的子流条数。
    handled_: AtomicUsize,
}

impl Metrics {
    /// 从零开始计数。
    pub const fn new() -> Self {
        Metrics {
            handled_: AtomicUsize::new(0usize),
        }
    }

    /// 记一条处理完毕。
    pub fn incr_(&self) {
        self.handled_.fetch_add(1usize, Ordering::SeqCst);
    }

    /// 已处理完多少条。
    pub fn handled_(&self) -> usize {
        self.handled_.load(Ordering::SeqCst)
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Metrics::new()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 两条业务路径
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回声 handler：收 [`EchoRequest`]，回 [`EchoReply`]。
pub struct EchoHandler {
    /// 处理计数。
    metrics_: Arc<Metrics>,
}

/// 求和 handler：收 [`SumRequest`]，回 [`SumReply`]。
pub struct SumHandler {
    /// 处理计数。
    metrics_: Arc<Metrics>,
}

/// 路由表里装的**唯一** handler 类型。
///
/// `Router<T>` 只有一个 `T`，而两条路径要各自的业务逻辑，因此用一个枚举把两者装进同一个
/// 类型，由它分派。链式中间件不在本轮范围内（服务端只做单 handler 分派）。
pub enum DemoHandler {
    /// 回声。
    Echo(EchoHandler),

    /// 求和。
    Sum(SumHandler),
}

impl DemoHandler {
    /// 本 handler 共用的处理计数。
    fn metrics_(&self) -> &Arc<Metrics> {
        match self {
            DemoHandler::Echo(inner) => &inner.metrics_,
            DemoHandler::Sum(inner) => &inner.metrics_,
        }
    }
}

impl TrReqHandler<ServingCfg> for DemoHandler {
    type HandleAsync<'f>
        = HandleDemoAsync<'f, 'f>
    where
        Self: 'f;

    type Err = HandlerError;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        chan_tx: ChannelTx<ServingCfg>,
        chan_rx: ChannelRx<ServingCfg>,
    ) -> Self::HandleAsync<'f> {
        HandleDemoAsync::new(self, method, location, headers, chan_tx, chan_rx)
    }
}

/// [`DemoHandler::handle_async`] 的 step 函数：按变体分派。
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(HandleDemo, pub, new(pub(crate)))]
async fn handle_demo_async_<'h, TyTok>(
    handler: &'h DemoHandler,
    method: AccessMethod,
    location: &'h str,
    headers: &'h mut Headers,
    mut tx: ChannelTx<ServingCfg>,
    mut rx: ChannelRx<ServingCfg>,
    cancel: TyTok,
) -> Result<(), HandlerError>
where
    TyTok: TrCancellationToken,
{
    let handled = match handler {
        DemoHandler::Echo(inner) => {
            handle_echo_async_(
                inner,
                method,
                location,
                headers,
                &mut tx,
                &mut rx,
                cancel.child_token(),
            )
            .await
        }
        DemoHandler::Sum(inner) => {
            handle_sum_async_(
                inner,
                method,
                location,
                headers,
                &mut tx,
                &mut rx,
                cancel.child_token(),
            )
            .await
        }
    };
    // 收尾由 handler 负责：先半关闭 tx，再读到对端关闭。
    close_channel_async(tx, &mut rx).await;
    // 「处理完一条子流」的计数放在**统一出口**：正常回、拒绝、报错都算一条——否则按计数
    // 收尾的那一方会在「有一次被拒绝」时永远等不到。
    handler.metrics_().incr_();
    handled
}

/// 回声路径的处理步骤。
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(HandleEcho, pub, new(pub(crate)))]
async fn handle_echo_async_<'h, TyTok>(
    _handler: &'h EchoHandler,
    _method: AccessMethod,
    _location: &'h str,
    headers: &'h mut Headers,
    tx: &'h mut ChannelTx<ServingCfg>,
    rx: &'h mut ChannelRx<ServingCfg>,
    cancel: TyTok,
) -> Result<(), HandlerError>
where
    TyTok: TrCancellationToken,
{
    let bytes = read_request_body_(headers, rx, cancel.child_token()).await?;

    // 类型标识由**类型自己**给出；对不上就明确拒绝，绝不解码。
    let expected = type_id_of_::<EchoRequest>();
    match headers.data_type_id() {
        Option::Some(actual) if actual == expected => {}
        Option::Some(actual) => {
            println!("服务端：对端声明 {actual:#x}，echo 期待 {expected:#x}，回 406（不解码）");
            return reply_status_(Status::NotAcceptable, tx, cancel).await;
        }
        Option::None => {
            println!("服务端：echo 请求没有声明 Data_Type_Id，回 400");
            return reply_status_(Status::BadRequest, tx, cancel).await;
        }
    }

    let request: EchoRequest = decode_slice_async_(&bytes)
        .await
        .map_err(|err| HandlerError::Decode(err.to_string()))?;
    println!(
        "服务端：校验 Data_Type_Id = {expected:#x} 通过，解出 EchoRequest，text = {:?}",
        request.text
    );

    let (prefix, body) = response_with_(Status::Ok, &EchoReply { text: request.text })
        .await
        .map_err(|err| HandlerError::Build(err.to_string()))?;
    println!(
        "服务端：回 EchoReply（标识 {:#x}）",
        type_id_of_::<EchoReply>()
    );
    send_response_(prefix, body, tx, cancel).await?;
    Result::Ok(())
}

/// 求和路径的处理步骤。
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(HandleSum, pub, new(pub(crate)))]
async fn handle_sum_async_<'h, TyTok>(
    _handler: &'h SumHandler,
    _method: AccessMethod,
    _location: &'h str,
    headers: &'h mut Headers,
    tx: &'h mut ChannelTx<ServingCfg>,
    rx: &'h mut ChannelRx<ServingCfg>,
    cancel: TyTok,
) -> Result<(), HandlerError>
where
    TyTok: TrCancellationToken,
{
    let bytes = read_request_body_(headers, rx, cancel.child_token()).await?;

    let expected = type_id_of_::<SumRequest>();
    match headers.data_type_id() {
        Option::Some(actual) if actual == expected => {}
        Option::Some(actual) => {
            println!("服务端：对端声明 {actual:#x}，求和期待 {expected:#x}，回 406（不解码）");
            return reply_status_(Status::NotAcceptable, tx, cancel).await;
        }
        Option::None => {
            println!("服务端：求和请求没有声明 Data_Type_Id，回 400");
            return reply_status_(Status::BadRequest, tx, cancel).await;
        }
    }

    let request: SumRequest = decode_slice_async_(&bytes)
        .await
        .map_err(|err| HandlerError::Decode(err.to_string()))?;
    println!(
        "服务端：校验 Data_Type_Id = {expected:#x} 通过，解出 SumRequest，{} + {}",
        request.a, request.b
    );

    let total = request.a + request.b;
    let (prefix, body) = response_with_(Status::Ok, &SumReply { total })
        .await
        .map_err(|err| HandlerError::Build(err.to_string()))?;
    println!(
        "服务端：回 SumReply（标识 {:#x}）",
        type_id_of_::<SumReply>()
    );
    send_response_(prefix, body, tx, cancel).await?;
    Result::Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读体 / 写回复
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 按请求头声明的长度把请求体读出来。
///
/// 边界**完全由头决定**：没有 `Body_Size` 就一个字节都不读（本 demo 的请求都带体，因此这种
/// 情况直接按 `400` 处理的意义不大，这里当「空体」交出去，由类型标识那一步去拒绝）。
///
/// # Errors
///
/// 读 ring 失败或对端提前关闭时返回 [`HandlerError::ReadBody`]。
async fn read_request_body_<TyTok>(
    headers: &Headers,
    rx: &mut ChannelRx<ServingCfg>,
    cancel: TyTok,
) -> Result<Vec<u8>, HandlerError>
where
    TyTok: TrCancellationToken,
{
    let size = headers.body_size().unwrap_or(0usize);
    if size == 0usize {
        return Result::Ok(Vec::new());
    }
    read_body_async_(rx, size, cancel)
        .await
        .map_err(|err| HandlerError::ReadBody(err.to_string()))
}

/// 装配好的回复由框架之外**这里**写出：先前缀，再体。
///
/// 顺序由 `mptp_smux` 的前缀契约负责（前缀一次写完），体紧随其后。
async fn send_response_<TyTok>(
    prefix: ResponsePrefix,
    body: Vec<u8>,
    tx: &mut ChannelTx<ServingCfg>,
    cancel: TyTok,
) -> Result<(), HandlerError>
where
    TyTok: TrCancellationToken,
{
    write_response_prefix_async(&prefix, tx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| HandlerError::Write(err.to_string()))?;
    if !body.is_empty() {
        write_body_async_(tx, &body, cancel.child_token())
            .await
            .map_err(|err| HandlerError::Write(err.to_string()))?;
    }
    Result::Ok(())
}

/// 回一条**不带体**的状态（本 demo 里是 `400` / `406`）。
async fn reply_status_<TyTok>(
    status: Status,
    tx: &mut ChannelTx<ServingCfg>,
    cancel: TyTok,
) -> Result<(), HandlerError>
where
    TyTok: TrCancellationToken,
{
    let (prefix, body) = response_empty_(status);
    send_response_(prefix, body, tx, cancel).await
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 装配
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 造路由表：每条路径一个 handler，各自认自己的类型标识。
pub fn build_router_(metrics: Arc<Metrics>) -> Router<DemoHandler> {
    let mut router = Router::new();
    router.add_target(
        K_ECHO_PATH,
        DemoHandler::Echo(EchoHandler {
            metrics_: metrics.clone(),
        }),
    );
    router.add_target(
        K_SUM_PATH,
        DemoHandler::Sum(SumHandler { metrics_: metrics }),
    );
    router
}

/// 服务端：在从 `base_dock` 起的 `count` 个 dock 上各监听一条子流，并逐条处理。
///
/// # 为什么不是「同一个 dock 上连开多条子流」
///
/// 客户端每完成一次一问一答就会拆掉那条子流，而 `smux_v1` 会把这个 dock 对放进**拆流宽限
/// 期**内不得复用。各用各的 dock，既绕开了这个复用保护，也顺带演示了「一条连接上多 dock、
/// 多子流」。
///
/// # ring 内存在哪里准备
///
/// 就在 [`MptpServer::serve_listener_async`] 内部：每条入向子流裁决之前，它用本条子流的环境
/// （两个 dock + 响应方角色）现场构造 [`ServingCfg`] 的 `DefaultPrepFactory`，再让它生产
/// 一份新的准备器。本函数因此**看不到**任何 ring 相关的代码。
///
/// # 什么时候收尾
///
/// 客户端的连接断开时，各条监听循环会以「接受失败」结束——那就是收尾信号（框架的监听循环
/// 本身是常驻的）。
///
/// # Errors
///
/// 绑定 dock 失败时返回错误；监听循环自身的结束原因只被打印，不当成致命错误。
pub async fn serve_(conn: Conn, base_dock: u32, count: usize) -> Result<()> {
    let shared = Shared::new(conn, CoreAlloc);
    let metrics = Arc::new(Metrics::new());
    let server = MptpServer::<ServingCfg, Router<DemoHandler>>::new(build_router_(metrics.clone()));

    // 先把所有 dock 的 binding 都拿到手，再去等入向——否则对端可能在我们还没绑定的 dock 上
    // 发起建流。
    let mut bindings = Vec::with_capacity(count);
    for offset in 0..count {
        let dock = Dock::new(base_dock + offset as u32);
        let binding = shared
            .bind_async(dock)
            .await
            .map_err(|err| anyhow!("绑定 dock {dock:?} 失败：{err}"))?;
        bindings.push(binding);
    }

    // 每条 binding 上跑一条监听循环；它们都在同一个 task 里被轮询，不需要跨线程。
    //
    // 收尾由**计数**决定：框架的监听循环是常驻的（同一条连接上可以持续来子流），所以
    // 「这一轮要几条」只有使用者知道。客户端断开连接也会让各条循环以错误结束——那是另一条
    // 退路（例如对端崩了）。
    let listeners = bindings.iter_mut().map(|binding| {
        server
            .serve_listener_async(binding, K_LISTEN_RESERVE)
            .may_cancel_with(NonCancellableToken::new())
    });

    // 收尾：**两条路，谁先到算谁**。
    //
    // 1. 各条监听循环自己结束——对端断开时它应当以错误收场（实测 `smux_v1` 的
    //    `income_async` 并不会因为 socket 断开而报错，所以这条路通常走不到）；
    // 2. 处理计数达到 `count`，再静默一小段——**不能**一达到就收尾：此刻最后一条响应可能
    //    还在环里没排空上网，关掉连接会把它的尾巴抹掉（实测症状是客户端报
    //    `IO error while reading marker: failed to fill whole buffer`）。
    //
    // 最后那 5 秒超时是兜底：对端若被强杀，上面两条路都等不到。
    //
    // TODO(重构): 「一次监听接受 N 条就返回」本该是框架给的形状
    // （`serve_listener_async(.., max_channels)`），那样使用者既不需要自己计数，也不需要
    // 拿静默期去猜「排空了没有」。本 demo 先用计数 + 静默把这条路走通。
    let waited = tokio::time::timeout(K_SERVER_IDLE, async {
        tokio::select! {
            results = join_all(listeners) => results,
            _ = wait_count_(metrics.clone(), count) => {
                tokio::time::sleep(K_DRAIN_PAUSE).await;
                Vec::new()
            }
        }
    })
    .await;
    match waited {
        Result::Ok(results) => {
            for (index, res) in results.into_iter().enumerate() {
                if let Result::Err(err) = res {
                    println!("服务端：第 {} 条监听循环结束（{err}）", index + 1);
                }
            }
        }
        Result::Err(_) => {
            println!("服务端：等待入向超时（{K_SERVER_IDLE:?}），收尾。");
        }
    }
    println!("服务端：本轮共处理 {} 条子流，退出。", metrics.handled_());
    Result::Ok(())
}

/// 等到处理计数达到 `target`。
///
/// 轮询是 demo 的收尾手段，**不是**框架的一部分：框架只负责把每条子流交给 handler，不替
/// 使用者定义「什么时候算跑完」。
async fn wait_count_(metrics: Arc<Metrics>, target: usize) {
    while metrics.handled_() < target {
        tokio::time::sleep(Duration::from_millis(20u64)).await;
    }
}
