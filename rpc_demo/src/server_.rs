//! 服务端侧：配置实现、echo handler、路由装配，以及服务一条子流。
//!
//! 这里展示 Phase 2 补齐的读体路径：handler 拿到的是子流的**接收半边**，用
//! [`recv_request_body_async`] 按 `Body_Size` 头把体**直接解成一个业务类型**——解码就
//! 发生在 ring 上，中间没有中转缓冲；回体则交给
//! [`Response::with_measured_body`]，长度用「只数不写」量好，编码同样直接落进 ring。

use core::mem::MaybeUninit;

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrChannelListener, TrDockBinding},
};
use anyhow::{Result, anyhow};
use mm_ptr::{Owned, x_deps::abs_mm};
use mptp_core::{
    access_method::AccessMethod,
    messaging::{Nothing, Request, Response, recv_request_body_async},
    routing::prefix_router::Router,
    serving::{
        ServingRingPrepare, TrServingAllocConfig, TrServingConfig,
        config::{ChannelRx, ChannelTx, DockBinding},
        handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
        server::{Server, SessionContext},
    },
    specs::{Headers, Status},
};
use smux_v1::x_deps::{abs_buff, abs_smux, mm_ptr};

use crate::conn_::Conn;

/// 服务端对外提供的路径：把请求体原样回传。
pub const K_ECHO_PATH: &str = "/rpc/echo";

/// 一条子流向一个方向的 ring 容量（字节）。
const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 入向邀请最多同时挂起多少条。
const K_LISTEN_RESERVE: usize = 8usize;

/// 子流 ring 存储的拥有者类型。
type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 服务端侧的资源分配约定。
pub struct DemoAllocCfg;

impl TrServingAllocConfig for DemoAllocCfg {
    type SharedConnAlloc = CoreAlloc;

    type RingAlloc = CoreAlloc;

    type RingBuff = RingBuff;

    const RING_CAPACITY: usize = K_CHANNEL_CAP;

    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff) {
        (
            Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
            Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
        )
    }
}

/// 本 demo 的服务端配置。
///
/// 请求体与服务端回的体都是 MessagePack 编出来的字符串：协议层只负责把它们按
/// `Body_Size` 搬过去，编解码发生在 `serde` 那一层。
pub struct DemoServingCfg;

impl TrServingConfig for DemoServingCfg {
    type MuxConn = Conn;

    type AllocCfg = DemoAllocCfg;

    type Request = Request<String, Nothing>;

    type Response = Response<String, Nothing>;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求体原样回写的 handler。
pub struct EchoHandler;

// handler 的入参本来就宽（请求信息 + 双向半边 + 上下文 + 令牌）。
#[allow(clippy::too_many_arguments)]
#[gen_may_cancel_future(HandleEcho, pub, new(pub(crate)))]
async fn handle_echo_async_<'h, TyTok>(
    _handler: &'h EchoHandler,
    _method: AccessMethod,
    _location: &'h str,
    headers: &'h mut Headers,
    _tx: &'h mut ChannelTx<DemoServingCfg>,
    rx: &'h mut ChannelRx<DemoServingCfg>,
    _context: &'h mut SessionContext,
    cancel: TyTok,
) -> Result<FlowCtrl<Response<String, Nothing>>, HandlerError>
where
    TyTok: TrCancellationToken,
{
    // 调用者可以通过取消令牌中止这次处理；这里没有等待点，但仍要如实响应它，
    // 而不是假定「反正没人会取消」。
    if cancel.is_cancelled() {
        return Result::Ok(FlowCtrl::Ceased(Option::None));
    }

    // 按 `Body_Size` 头把请求体**直接解出来**：`AsStdRead` 接在 ring 的接收半边上，
    // 中间没有一块「先读进来再说」的缓冲。头缺省或为 0 时一个字节都不会读。
    let msg: Option<String> =
        recv_request_body_async(rx, Option::Some(headers), cancel.child_token())
            .await
            .map_err(|_| HandlerError::IoError)?;

    // 回体时用 `with_measured_body`：长度只数不写地量一遍，`Body_Size` 与实际写出量
    // 必然一致，`Server` 写回时不会因为对不上而拒绝。
    let resp = Response::<String, Nothing>::with_measured_body(Status::Ok, msg.unwrap_or_default())
        .map_err(|_| HandlerError::IoError)?;
    Result::Ok(FlowCtrl::Ceased(Option::Some(resp)))
}

impl TrReqHandler<DemoServingCfg> for EchoHandler {
    fn handle_async<'h>(
        &'h self,
        method: AccessMethod,
        location: &'h str,
        headers: &'h mut Headers,
        tx: &'h mut ChannelTx<DemoServingCfg>,
        rx: &'h mut ChannelRx<DemoServingCfg>,
        context: &'h mut SessionContext,
    ) -> impl TrMayCancel<'h, MayCancelOutput = Result<FlowCtrl<Response<String, Nothing>>, HandlerError>>
    {
        HandleEchoAsync::new(self, method, location, headers, tx, rx, context)
    }
}

/// 装配路由与服务端：把 [`K_ECHO_PATH`] 指向只装了 [`EchoHandler`] 的链。
pub fn build_server_() -> Server<DemoServingCfg> {
    let mut chain = HandlerChain::new();
    chain.add_handler(EchoHandler);

    let mut router = Router::new();
    router.add_target(K_ECHO_PATH, chain);
    Server::new(router)
}

/// 服务端：在 `binding` 上监听，接受**一条**子流并处理一个请求。
///
/// 这里没有直接用 [`Server::serve_listener_async`]：那是无限的 accept 循环，而本 demo
/// 每次运行只跑一次往返。两者在「listen → income → accept → 处理」这条路径上完全一致。
///
/// # Errors
///
/// 监听、等待入向、裁决建流或处理子流任一步失败都会带上下文返回 `Err`。
pub async fn serve_one_channel_(
    server: &Server<DemoServingCfg>,
    binding: &mut DockBinding<DemoServingCfg>,
    context: &mut SessionContext,
) -> Result<()> {
    let mut listener = binding
        .listen_async(K_LISTEN_RESERVE)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("进入监听失败：{err}"))?;

    let mut handle = listener
        .income_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("等待入向建流失败：{err}"))?;

    // 最终裁决：本端当场交出两块 ring 内存。
    let (tx_buff, rx_buff) = <DemoAllocCfg as TrServingAllocConfig>::make_ring_buffs();
    let prepare = ServingRingPrepare::new(tx_buff, rx_buff);
    // 欢迎信息当前不由 MPTP 使用（`smux_v1` 按空载荷发出）。
    let mut welcome: &mut [u8] = &mut [];
    let (tx, rx) = handle
        .accept_async(&mut welcome, prepare)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("裁决建流失败：{err}"))?;

    server
        .serve_channel_async(tx, rx, context)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("处理子流失败：{err}"))?;

    Result::Ok(())
}
