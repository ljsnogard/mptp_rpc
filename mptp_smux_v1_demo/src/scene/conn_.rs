//! compio TCP 与 smux_v1 的装配：拨号 / 接受 + 握手 + 建连接。
//!
//! `BuffRead` / `BuffWrite` 必须在**正在运行的 compio runtime 内**构造，且类型 `!Send`。
//! 因此本模块的每个函数都只能在 compio 线程上 await；调用者负责保证整条 MPTP 栈不跨线程。
//!
//! # `conn_`: compio TCP + smux_v1 assembly
//!
//! The adapters must be built inside a running compio runtime and are `!Send`; everything
//! in this module therefore stays on the compio thread.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Result, anyhow};
use buffex_compio_adapt::{BuffRead, BuffWrite, DefaultAllocConfig};
use compio::net::TcpStream;
use smux_v1::{
    connection::{DefaultConnCfg, MuxConnection},
    flow_ctrl::DefaultPolicy,
    metrics::NoMetrics,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
};

use crate::scene::cfg_::{Conn, K_CHANNEL_CAP, Rx, SceneRt, SharedConn, Tx, compio_rt_};

use abs_mm::CoreAlloc;
use mm_ptr::Shared;

/// 本 demo 的握手条件。
///
/// `max_channel_timeout` 取 2 秒：对端进程消失后，连接级保活会在两秒左右把连接判死，
/// 服务端据此删除行星并广播 `Removed`。活跃的 `Pull` 由服务端每 300ms 心跳续命，
/// 因此 2 秒足够宽松。真实部署应把这两个数按网络 RTT 调大。
fn scene_opts_() -> BasicOpts {
    BasicOpts {
        max_channel_timeout: Duration::from_secs(2u64),
        max_channel_wait_close: Duration::from_secs(1u64),
        ..BasicOpts::DEFAULT
    }
}

/// 把一条 compio TCP 流接成两条 ring 适配器。
fn halves_(stream: TcpStream) -> Result<(Tx, Rx)> {
    let (wr, rd) = stream.into_split();
    let rx = BuffRead::<_, DefaultAllocConfig>::try_new(rd, K_CHANNEL_CAP, DefaultAllocConfig)
        .map_err(|err| anyhow!("构造读环失败：{err}"))?;
    let tx = BuffWrite::<_, DefaultAllocConfig>::try_new(wr, K_CHANNEL_CAP, DefaultAllocConfig)
        .map_err(|err| anyhow!("构造写环失败：{err}"))?;
    Ok((tx, rx))
}

/// 客户端：拨号、握手并建出复用连接。
///
/// # Errors
///
/// TCP 连接、适配器构造或握手任一步失败时返回错误。
pub async fn dial_async_(addr: SocketAddr) -> Result<Conn> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|err| anyhow!("连接 {addr} 失败：{err}"))?;
    stream
        .set_nodelay(true)
        .map_err(|err| anyhow!("设置 TCP_NODELAY 失败：{err}"))?;
    let (tx, rx) = halves_(stream)?;
    let delivery = HandshakeAgent::new(tx, rx)
        .invite_async(&scene_opts_(), AcceptAllEntries)
        .await
        .map_err(|err| anyhow!("客户端握手失败：{err:?}"))?;
    build_conn_(delivery)
}

/// 服务端：在一条已接受的 TCP 流上完成握手并建出复用连接。
///
/// # Errors
///
/// 适配器构造或握手失败时返回错误。
pub async fn accept_conn_async_(stream: TcpStream) -> Result<Conn> {
    stream
        .set_nodelay(true)
        .map_err(|err| anyhow!("设置 TCP_NODELAY 失败：{err}"))?;
    let (tx, rx) = halves_(stream)?;
    let delivery = HandshakeAgent::new(tx, rx)
        .listen_async(&scene_opts_(), AcceptAllEntries)
        .await
        .map_err(|err| anyhow!("服务端握手失败：{err:?}"))?;
    build_conn_(delivery)
}

/// 用显式 compio 运行时值与作用域建连。
///
/// 不走 `MuxConnection::new`：那要求配置的运行时值实现门面的 `ScopeHost`，而门面的
/// `ScopeHost` 只对**裸名** `Runtime` 实现——工作区 feature 并集把它切到 tokio 时，
/// `CompioRuntime` 拿不到它。`new_with_rt` 只要求一个 `TrLocalScope`，正好绕开这条
/// 全局选择。
fn build_conn_(delivery: smux_v1::handshake::agent::HandshakeDelivery<Tx, Rx>) -> Result<Conn> {
    let rt = compio_rt_();
    let scope = rt.local_scope();
    let (delivery, config) = DefaultConnCfg::<
        Tx,
        Rx,
        NoMetrics,
        DefaultPolicy,
        SceneRt,
    >::new_with_rt(delivery, DefaultPolicy, rt.clone());
    Ok(MuxConnection::new_with_rt(&rt, &scope, delivery, config))
}

/// 把连接交给 `MptpClient` 需要的共享智能指针。
pub fn share_conn_(conn: Conn) -> SharedConn {
    Shared::new(conn, CoreAlloc)
}
