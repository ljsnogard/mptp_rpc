//! 基础服务器组件。
//!
//! [`Server`] 负责：
//!
//! 1. 从子流的接收半边解码 `Request` 前缀；
//! 2. 使用路由表找到对应的 [`HandlerChain`]；
//! 3. 调用 `HandlerChain` 让请求按顺序经过感兴趣的 handler；
//! 4. 如果 handler 通过 `FlowCtrl` 返回了一个 `Response`，则把回复头写回子流。
//!
//! 它有两种驱动方式：
//!
//! - [`Server::serve_channel_async`]：处理一条**已经建成**的子流（收发半边由调用方
//!   给出，通常是 `ChannelHandle::accept_async` 的产物）；
//! - [`Server::serve_listener_async`]：在某个 `DockBinding` 上 `listen_async`，然后
//!   循环 `income_async` → `accept_async`，把每条被接受的子流交给上面那条路径。
//!
//! 后者需要本端在最终裁决时交出两块 ring 内存，所以它依赖
//! [`TrServingAllocConfig`](super::alloc_config_::TrServingAllocConfig)。

use std::io;

use abs_buff::{
    buffer::TrProducerState,
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrChannelListener, TrDockBinding},
};
use thiserror::Error;

use super::{
    alloc_config_::{ServingRingPrepare, TrServingAllocConfig},
    config::{ChannelRx, ChannelTx, DockBinding, TrServingConfig},
    handler::HandlerChain,
};
use crate::messaging;

type Router<T> = crate::routing::prefix_router::Router<T>;

/// 会话上下文，后续可以存放连接信息、鉴权结果、日志等。
/// 在客户端连接到服务器时被创建，判定断线后被回收。
pub struct SessionContext;

/// 服务端处理过程中可能出现的错误。
#[derive(Debug, Error)]
pub enum ServeError {
    /// 在本端 dock 上派生会话（binding）失败。
    #[error("bind dock failed: {0}")]
    Bind(String),

    /// 进入监听状态失败。
    #[error("listen failed: {0}")]
    Listen(String),

    /// 等待入向建流或最终裁决失败。
    #[error("accept channel failed: {0}")]
    Accept(String),

    /// 请求前缀解码失败。
    #[error("decode request failed: {0}")]
    Decode(String),

    /// 路由表里没有匹配该路径的目标。
    #[error("resource not found: {0}")]
    NotFound(String),

    /// handler 链返回了错误。
    #[error("handler error: {0}")]
    Handler(String),

    /// 与子流读写相关的 IO 错误。
    #[error("io error: {0}")]
    Io(String),
}

impl From<io::Error> for ServeError {
    fn from(value: io::Error) -> Self {
        ServeError::Io(value.to_string())
    }
}

/// 基础服务器组件：负责“从子流解码请求 → 路由 → 调用 handler → 写回回复”。
///
/// 它不关心具体网络传输，只依赖 `abs_smux` 契约给出的收发半边；连接与内存策略由
/// [`TrServingConfig`] 决定。
pub struct Server<C>
where
    C: TrServingConfig,
{
    router_: Router<HandlerChain<C>>,
}

impl<C> Server<C>
where
    C: TrServingConfig,
    // 回写响应前缀要经过 `AsStdWrite`，见 `serve_channel_async_`。
    ChannelTx<C>: TrProducerState,
{
    /// 使用指定路由表创建服务器。
    pub const fn new(router: Router<HandlerChain<C>>) -> Self {
        Server { router_: router }
    }

    /// 返回路由表引用，方便继续注册或检查。
    pub const fn router(&self) -> &Router<HandlerChain<C>> {
        &self.router_
    }

    /// 在一条**已经建成**的子流上处理一个请求。
    ///
    /// 流程：
    /// 1. 从 `rx` 解码请求前缀；
    /// 2. 用路由表找到匹配的 `HandlerChain`；
    /// 3. 调用 `HandlerChain` 让请求依次经过 handler（handler 可以读写 `tx` / `rx`）；
    /// 4. 若 handler 返回 `SkipRest(Some(resp))` 或 `Ceased(Some(resp))`，
    ///    则把该回复前缀写回 `tx`。
    pub fn serve_channel_async<'f>(
        &'f self,
        tx: &'f mut ChannelTx<C>,
        rx: &'f mut ChannelRx<C>,
        context: &'f mut SessionContext,
    ) -> ServeChannelAsync<'f, 'f, C> {
        ServeChannelAsync::new(self, tx, rx, context)
    }

    /// 在一个 `DockBinding` 上监听，并循环接受子流逐个处理。
    ///
    /// `reserve` 是上游 `listen_async` 的「最多同时挂起多少条入向邀请」上限。
    /// 每条被接受的子流都会由本端当场交出两块 ring 内存
    /// （[`TrServingAllocConfig::make_ring_buffs`](super::alloc_config_::TrServingAllocConfig::make_ring_buffs)）。
    ///
    /// 本方法在**任一**子流处理失败或取消信号到达时返回；已经在处理的子流不会被
    /// 中途打断（它的取消由它自己那一层的令牌决定）。
    pub fn serve_listener_async<'f>(
        &'f self,
        binding: &'f mut DockBinding<C>,
        reserve: usize,
        context: &'f mut SessionContext,
    ) -> ServeListenerAsync<'f, 'f, C> {
        ServeListenerAsync::new(self, binding, reserve, context)
    }
}

/// 处理一条已建成子流的 step 函数。
#[gen_may_cancel_future(ServeChannel, pub, new(pub(crate)))]
async fn serve_channel_async_<'f, C, TyTok>(
    server: &'f Server<C>,
    tx: &'f mut ChannelTx<C>,
    rx: &'f mut ChannelRx<C>,
    context: &'f mut SessionContext,
    cancel: TyTok,
) -> Result<(), ServeError>
where
    C: TrServingConfig,
    // 回写响应前缀要经过 `AsStdWrite`，它要求写半边能报告「环是否已满」。
    ChannelTx<C>: TrProducerState,
    TyTok: TrCancellationToken,
{
    // 1. 解码请求前缀。请求体 / suffix stream 由 handler 自行从 `rx` 读取。
    let prefix = messaging::request::recv_request_prefix_async(rx, cancel.child_token())
        .await
        .map_err(|err| ServeError::Decode(err.to_string()))?;

    let messaging::request::ReqPrefix(method, location, headers) = prefix;

    // 2. 路由。
    let handler = server
        .router_
        .try_match(location.as_str())
        .ok_or_else(|| ServeError::NotFound(location.clone()))?;

    // 3. 调用 HandlerChain。
    let mut headers = headers.unwrap_or_default();
    let ctrl = handler
        .handle_async(method, &location, &mut headers, tx, rx, context)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Handler(err.to_string()))?;

    // 4. 如果 handler 通过 FlowCtrl 返回了 Response，则写回客户端。
    if let Option::Some(resp) = ctrl.response() {
        messaging::response::send_response_prefix_async(resp, tx, cancel.child_token())
            .await
            .map_err(|err| ServeError::Io(err.to_string()))?;
    }

    Result::Ok(())
}

/// 监听并循环接受子流的 step 函数。
#[gen_may_cancel_future(ServeListener, pub, new(pub(crate)))]
async fn serve_listener_async_<'f, C, TyTok>(
    server: &'f Server<C>,
    binding: &'f mut DockBinding<C>,
    reserve: usize,
    context: &'f mut SessionContext,
    cancel: TyTok,
) -> Result<(), ServeError>
where
    C: TrServingConfig,
    ChannelTx<C>: TrProducerState,
    TyTok: TrCancellationToken,
{
    let mut listener = binding
        .listen_async(reserve)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Listen(err.to_string()))?;

    loop {
        // 1. 等一条入向建流请求。
        let mut handle = listener
            .income_async()
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ServeError::Accept(err.to_string()))?;

        // 2. 最终裁决：交出本端两块 ring 内存，换回这条子流的收发半边。
        let (tx_buff, rx_buff) = <C::AllocCfg as TrServingAllocConfig>::make_ring_buffs();
        let prepare = ServingRingPrepare::new(tx_buff, rx_buff);
        // 欢迎信息当前不由 MPTP 使用（`smux_v1` 按空载荷发出）。
        let mut welcome: &mut [u8] = &mut [];
        let (mut tx, mut rx) = handle
            .accept_async(&mut welcome, prepare)
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ServeError::Accept(err.to_string()))?;

        // 3. 在这条子流上处理一个请求。
        //
        // TODO(重构): 当前是「一条子流处理一个请求」的一问一答模型；等会话复用
        // 语义确定后，这里可能改成在同一条子流上循环处理。
        serve_channel_async_(server, &mut tx, &mut rx, context, cancel.child_token()).await?;
    }
}
