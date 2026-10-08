//! 进程内的复用连接装配。
//!
//! 两条全被动传输环分别承载 A→B 与 B→A，四个环半部直接交给两个 `MuxConnection`
//! 当作 `Rx` / `Tx`，中间**没有任何 socket、也没有泵**。这样端到端验证只覆盖
//! 「mptp_core 的协议行为」，不会被传输层的未知问题干扰。
//!
//! 所有依赖都从 `smux_v1::x_deps` 取：本 demo 只声明 `smux_v1` 一个实现依赖，
//! 避免同一个 crate 出现第二个来源、或后端 feature 出现第二个决定点。

use core::mem::MaybeUninit;

use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow};
use mm_ptr::{Owned, x_deps::abs_mm};
use smux_v1::{
    connection::{
        BufferedRx, BufferedTx, DefaultConnCfg, MuxChanBuffOwnedBy, MuxConnection,
        new_buffered_channel,
    },
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent},
        opts::BasicOpts,
    },
    x_deps::mm_ptr,
};

/// 传输环容量（字节）：只影响吞吐，不影响正确性。
const K_TRANSPORT_CAP: usize = 64usize * 1024usize;

/// 连接级帧暂存容量（字节）。
const K_STAGE_CAP: usize = 64usize * 1024usize;

/// 一条子流向一个方向的环容量（字节）。
pub(crate) const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 服务端监听的 dock。
pub(crate) const K_LISTEN_DOCK: u32 = 1u32;

/// 入向邀请最多同时挂起多少条。
pub(crate) const K_LISTEN_RESERVE: usize = 8usize;

/// 子流 ring 存储的拥有者类型。
pub type DemoRingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 连接级帧暂存的拥有者类型。
type StageBuff = MuxChanBuffOwnedBy<DemoRingBuff>;

/// 本 demo 使用的连接配置。
pub type DemoMuxCfg = DefaultConnCfg<BufferedTx, BufferedRx>;

/// 本 demo 使用的复用连接。
pub type DemoConn = MuxConnection<DemoMuxCfg>;

/// 造一条全被动传输环，返回 `(写半边, 读半边)`。
fn make_transport_ring_() -> Result<(BufferedTx, BufferedRx)> {
    let owner = Owned::new_uninit_slice(K_TRANSPORT_CAP, CoreAlloc);
    new_buffered_channel(owner).map_err(|err| anyhow!("建传输环失败: {err:?}"))
}

/// 造一对连接级帧暂存缓冲。
fn make_stage_buffs_() -> (StageBuff, StageBuff) {
    (
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc)),
        MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc)),
    )
}

/// 为一条子流造出 `(发送方向, 接收方向)` 两块 ring 内存。
///
/// 客户端与服务端两套分配配置都转发到这里，保证两侧的容量与来源完全一致。
pub(crate) fn make_channel_buffs_() -> (DemoRingBuff, DemoRingBuff) {
    (
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
        Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
    )
}

/// 在进程内建一对互连的复用连接（A 主动、B 被动）。
///
/// 握手在本地完成，连接的五个循环经**本地作用域**投递，因此调用点必须在
/// `TrLocalScope::run_until` 里被驱动，否则循环不会被推进。
pub async fn connect_pair_() -> Result<(DemoConn, DemoConn)> {
    // 每条环拆成 (写半边, 读半边)，**交叉**交给两端：A 的写接到 B 的读，反之亦然。
    let (a_tx, b_rx) = make_transport_ring_()?;
    let (b_tx, a_rx) = make_transport_ring_()?;

    let opts = BasicOpts::default();
    let invite = HandshakeAgent::new(a_tx, a_rx).invite_async(&opts, AcceptAllEntries);
    let listen = HandshakeAgent::new(b_tx, b_rx).listen_async(&opts, AcceptAllEntries);
    let (invited, accepted) = tokio::join!(invite, listen);
    let delivery_a = invited.map_err(|err| anyhow!("发起方握手失败: {err:?}"))?;
    let delivery_b = accepted.map_err(|err| anyhow!("等待方握手失败: {err:?}"))?;

    // 运行时值与作用域都由配置自己解决（`TrConnCfg::runtime` + `ScopeHost`），
    // 因此这里只需要给出一个流控策略。
    let (delivery_a, cfg_a) = DefaultConnCfg::new(delivery_a, DefaultPolicy);
    let (delivery_b, cfg_b) = DefaultConnCfg::new(delivery_b, DefaultPolicy);

    let (a_sr, a_sw) = make_stage_buffs_();
    let (b_sr, b_sw) = make_stage_buffs_();
    Result::Ok((
        MuxConnection::new(delivery_a, cfg_a, a_sr, a_sw),
        MuxConnection::new(delivery_b, cfg_b, b_sr, b_sw),
    ))
}
