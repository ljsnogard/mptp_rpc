//! 基础服务器组件：把一条（或一串）入向子流变成「解码请求 → 路由 → 处理」。
//!
//! [`MptpServer`] 负责：
//!
//! 1. 从子流的接收半边解码请求前缀（交给 [`crate::messaging`]，本层不猜线上格式）；
//! 2. 用路由表按 `location` 找到那**一个** handler（最长前缀匹配）；
//! 3. 把 `method` / `location` / 头表的可变引用与子流的两个半边**按值**交给 handler；
//! 4. 收尾由 handler 负责：handler 可以把两个半边交给 worker，也可以在本层直接收尾。
//!
//! 它有两种驱动方式：
//!
//! - [`MptpServer::serve_channel_async`]：处理一条**已经建成**的子流（收发半边由调用方给出，
//!   通常是 `ChannelHandle::accept_async` 的产物）；
//! - [`MptpServer::serve_listener_async`]：在某个 `DockBinding` 上 `listen_async`，然后循环
//!   `income_async` → 准备 ring → `accept_async`，把每条被接受的子流交给上面那条路径。
//!
//! # 收尾顺序为什么是「先关写、再等读」
//!
//! 放掉接收半边（`ChannelRx::drop`）会发 `RESET` 拆掉该方向，解复用循环据此**静默丢弃在途
//! 数据**。若响应还堵在环里就被这一发抹掉，对端只会看到 EOF。所以收尾必须按
//! 「先半关闭写端（`drop(tx)`，排空后发 `FIN`）→ 等对端关闭 → 再放掉读端」的次序。
//! 框架不再替 handler 做这一步；handler 可以直接调用 [`close_channel_async`]。
//!
//! # `server_`: decoding, routing and serving one channel
//!
//! [`MptpServer`] decodes a request prefix through [`crate::messaging`], routes by longest
//! path prefix to a single handler, and hands that handler both channel halves **by value**.
//! Cleanup belongs to the handler (or to the worker it forwards the channel to).

use core::{marker::PhantomData, mem::MaybeUninit};

use thiserror::Error;

use abs_buff::{
    Demand, TrBuffRead,
    buffer::{TrBuffSegmRef, TrBuffSegmView},
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::serving::{TrReqHandler, config::MuxCfg};
use abs_smux::{
    chan::{TrChannelHalf, TrChannelHandle},
    conn::{TrChannelListener, TrDockBinding},
};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    messaging,
    ring_prepare_::TrPrepFactory,
    x_deps::{abs_buff, abs_cancel, abs_mptp, abs_smux, gen_mcf2},
};
use super::{
    config::{ChannelHandle, ChannelRx, ChannelTx, DockBinding, TrConfig},
    prep_ctx_::ServingPrepCtx,
    router_::TrPathRouter,
};

/// 服务端处理过程中可能出现的错误。
#[derive(Debug, Error)]
pub enum ServeError {
    /// 进入监听状态失败。
    #[error("进入监听失败：{0}")]
    Listen(String),

    /// 等待入向建流或最终裁决失败。
    #[error("接受子流失败：{0}")]
    Accept(String),

    /// 为某条子流准备 ring 内存失败。
    #[error("准备 ring 内存失败：{0}")]
    RingPrepare(String),

    /// 请求前缀解码失败。
    #[error("解码请求前缀失败：{0}")]
    Decode(String),

    /// 路由表里没有匹配该路径的目标。
    #[error("找不到资源：{0}")]
    NotFound(String),

    /// handler 报出的错误。
    #[error("处理请求失败：{0}")]
    Handler(String),

    /// 与子流读写相关的错误。
    #[error("子流读写失败：{0}")]
    Io(String),
}

/// 基础服务器组件：持有一张路由表，把每条入向子流分派给它命中的那一个 handler。
pub struct MptpServer<C, R>
where
    C: TrConfig,
    R: TrPathRouter,
    R::Handler: TrReqHandler<C>,
{
    /// 路径前缀 → handler。
    router_: R,

    /// `C` 只出现在约束里（handler 的类型由 C 决定，但字段本身不含 C），因此需要一个标记。
    _use_c_: PhantomData<fn() -> C>,
}

impl<C, R> MptpServer<C, R>
where
    C: TrConfig,
    R: TrPathRouter,
    R::Handler: TrReqHandler<C>,
{
    /// 使用指定路由表创建服务器。
    ///
    /// 这里**没有**策略参数：默认 ring 准备工厂是一个**类型**
    /// （[`TrConfig::DefaultPrepFactory`]），由本 crate 在每条入向子流裁决前用环境现场构造，
    /// 没有实例需要提前注入。
    pub const fn new(router: R) -> Self {
        MptpServer {
            router_: router,
            _use_c_: PhantomData,
        }
    }

    /// 返回路由表引用，方便继续注册或检查。
    pub const fn router(&self) -> &R {
        &self.router_
    }

    /// 在一条**已经建成**的子流上处理一个请求。
    ///
    /// 流程见模块文档；两个半边按值传给 handler，**handler 负责最终收尾**（可以直接调用
    /// [`close_channel_async`]，或把两个半边交给 worker）。
    pub fn serve_channel_async<'f>(
        &'f self,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> ServeChannelAsync<'f, 'f, C, R>
    where
        C: 'f,
        R: 'f,
    {
        ServeChannelAsync::<'f, 'f, C, R>::new(self, tx, rx)
    }

    /// 在一个 `DockBinding` 上监听，并循环接受子流逐个处理。
    ///
    /// `reserve` 是上游 `listen_async` 的「最多同时挂起多少条入向邀请」上限。**每条**被接受
    /// 的子流都会由本端当场交出两块 ring 内存，来源是配置声明的
    /// [`TrConfig::DefaultPrepFactory`]：框架用本条子流的环境（两个 dock + 响应方角色）现场
    /// 构造它，再让它生产一份准备器。
    ///
    /// 本方法在**任一**子流处理失败或取消信号到达时返回；已经在处理的子流不会被中途打断
    /// （它的取消由它自己那一层的令牌决定）。
    pub fn serve_listener_async<'f>(
        &'f self,
        binding: &'f mut DockBinding<C>,
        reserve: usize,
    ) -> ServeListenerAsync<'f, 'f, C, R>
    where
        C: 'f,
        R: 'f,
        // 环境的构造要从句柄读两个 dock，见 step 函数上的同一条约束。
        ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
    {
        ServeListenerAsync::<'f, 'f, C, R>::new(self, binding, reserve)
    }
}

/// 处理一条已建成子流的 step 函数。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的 `'f`
// 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ServeChannel, pub, new(pub(crate)))]
async fn serve_channel_async_<'f, C, R, K>(
    server: &'f MptpServer<C, R>,
    tx: ChannelTx<C>,
    mut rx: ChannelRx<C>,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
    R: TrPathRouter,
    R::Handler: TrReqHandler<C>,
    K: TrCancellationToken,
{
    // 1. 解码请求前缀。请求体 / 续流由 handler 自行从 `rx` 读取——前缀解码**只读前缀所需的
    //    字节**，体的第一个字节仍在环里。
    let prefix = messaging::read_request_prefix_async(&mut rx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Decode(err.to_string()))?;

    // 2. 路由。
    let method = prefix.method();
    let location = prefix.location();
    let handler = server
        .router_
        .match_path(location)
        .ok_or_else(|| ServeError::NotFound(location.to_string()))?;

    // 3. 头表要交**可变**引用，而前缀只借得出只读引用，因此在这里取一份独立的头表：
    //    没有头就是一张空表。handler 往里加的头不会被写回前缀（前缀是线上形态的中间态，
    //    不承担「被 handler 修改」的职责）。
    let mut headers = prefix.headers().cloned().unwrap_or_default();

    // 4. 两个半边按值交给 handler；收尾也由 handler 负责。整条链路由上层令牌驱动。
    handler
        .handle_async(method, location, &mut headers, tx, rx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Handler(err.to_string()))?;

    Result::Ok(())
}

/// 关闭一条已经处理完的子流：先半关闭写端，再读到对端关闭为止。
///
/// 这是 handler（或 worker）在写完回复后的标准收尾入口。顺序不能反：先放掉 `rx` 会发
/// `RESET`，可能把还没排空的响应抹掉。
///
/// 返回本条子流上被丢弃的字节数；收尾阶段的读错误**不**上报——它已经不影响已经写出的响应。
///
/// # Errors
///
/// 本函数不返回错误；读阶段的失败被静默忽略。收尾本身是纯等待，调用方可以直接 drop
/// 返回的 future 来中止等待。
pub async fn close_channel_async<TyTx, TyRx>(tx: TyTx, rx: &mut TyRx) -> usize
where
    TyRx: TrBuffRead<u8>,
{
    drop(tx);
    drain_to_eof_inner_async_(rx, NonCancellableToken::new()).await
}

/// 读到对端关闭为止，顺带丢弃途中收到的多余字节。
///
/// 这里不走 `std::io` 适配层：只需要「把段借出来、不消费就放回」——放回即视为已消费（段的
/// 消费量在回收时结算），因此直接借段即可。
///
/// 收尾阶段的任何读错误都**不**上报：这一步的职责是「把这条子流读干净」，而它失败与否已经
/// 不影响本端要发的响应（响应早就写完并半关闭了）。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DrainToEofInner, pub(crate), new(pub(crate)))]
async fn drain_to_eof_inner_async_<'f, R, K>(rx: &'f mut R, cancel: K) -> usize
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    let mut dropped = 0usize;
    let demand = Demand::exactly(4096usize);
    // 借出的段必须**真正搬走**才会推进消费量；只 `iter_slices()` 看一眼就 drop
    // 会把同一段永远读回来（空转）。收尾时数据已经没人要，搬进栈上暂存再丢弃。
    let mut sink = [MaybeUninit::<u8>::uninit(); 4096usize];
    loop {
        if cancel.is_cancelled() {
            return dropped;
        }
        let some_of = rx
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if some_of.contains_left() {
            let mut segm = some_of.pick_left().expect("刚刚确认过含左值");
            while segm.least_count() > 0usize {
                let moved = segm.move_items_to_buff(&mut sink);
                if moved == 0usize {
                    break;
                }
                dropped += moved;
            }
            continue;
        }
        // 对端关闭（`Closing`）是这条路径的正常终点；其余错误也一并收手——收尾不改变已经
        // 写出的响应。
        return dropped;
    }
}

/// 监听并循环接受子流的 step 函数。
#[gen_may_cancel_future(ServeListener, pub, new(pub(crate)))]
async fn serve_listener_async_<'f, C, R, K>(
    server: &'f MptpServer<C, R>,
    binding: &'f mut DockBinding<C>,
    reserve: usize,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
    R: TrPathRouter,
    R::Handler: TrReqHandler<C>,
    // 环境的构造要从**句柄**读两个 dock，因此句柄必须能回答它们——这是 `abs_smux` 的
    // `TrChannelHalf` 契约，与「最终裁决」那条契约彼此独立。
    ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
    K: TrCancellationToken,
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

        // 2. 准备 ring 内存：环境从**本条子流**的句柄读两个 dock（按值给出，先落成局部变量
        //    再借），工厂现场构造、当场生产一份准备器。每条子流都要新的一份——上游
        //    `accept_async` 会拿走它的所有权。
        let local_dock = handle.local_dock();
        let remote_dock = handle.remote_dock();
        let ctx = ServingPrepCtx::new(&local_dock, &remote_dock);
        let mut factory = C::DefaultPrepFactory::with_ctx(&ctx);
        let prepare = factory
            .get_prepared_async()
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ServeError::RingPrepare(err.to_string()))?;

        // 3. 最终裁决：交出准备器，换回这条子流的收发半边。
        let mut welcome: &mut [u8] = &mut [];
        let (tx, rx) = handle
            .accept_async(&mut welcome, prepare)
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ServeError::Accept(err.to_string()))?;

        // 4. 在这条子流上处理一个请求。
        //
        // TODO(重构): 当前是「一条子流处理一个请求」的一问一答模型；等会话复用语义确定后，
        // 这里可能改成在同一条子流上循环处理。
        serve_channel_async_(server, tx, rx, cancel.child_token()).await?;
    }
}
