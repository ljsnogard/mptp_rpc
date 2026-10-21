//! 客户端侧的公开契约：**使用者**要回答什么，RPC 层才不会替它决定传输与内存。
//!
//! Phase 0 里这个契约很短：客户端只需要在 [`mptp_smux::client::config::TrConfig`] 之上
//! 再回答本端的**内存分配约定**。请求 / 回复前缀的线上格式已经由 `mptp_smux` 拥有，不再是
//! 本 crate 的配置项；编解码器也不再出现在这里。
//!
//! # 为什么仍然需要本 trait
//!
//! 上游的配置只回答「连接从哪来」「ring 怎么准备」「缺省服务垛口是哪一个」，没有回答
//! **共享连接对象与每条子流的 ring 从哪个分配器来**。把这条约束收进一个 trait，使用点就
//! 不必同时抄 `mptp_smux` 与分配约定两条约束，也不会出现「配置能构造、内存来源却没声明」
//! 的半成品。
//!
//! # `client::config`: what the caller must answer on the client side
//!
//! On top of `mptp_smux`'s client configuration, this trait only adds the allocation policy for
//! the shared connection and per-channel ring memory. The wire format is owned by `mptp_smux`
//! now, so it is no longer a knob here.

use crate::x_deps::{abs_mm, abs_mptp, abs_smux, mptp_smux};
use super::alloc_config_::TrRpcClientAllocConfig;

/// 一个 MPTP RPC 客户端所需的配置。
///
/// 它只补 `mptp_smux` 没有回答的那一块：本端的内存分配约定。
pub trait TrRpcClientConfig: mptp_smux::client::config::TrConfig + Sized {
    /// 本端的内存分配约定（共享连接对象与子流 ring 各自的分配器）。
    type AllocCfg: TrRpcClientAllocConfig;
}

/// **缺省**的客户端分配约定：共享连接对象与 ring 都走系统分配器。
///
/// 它只为「不关心内存来源」的场合存在；要精确指定分配器时，写自己的
/// [`TrRpcClienAllocConfig`] 实现，并把它放进 `ClientConfig` 的第三个类型参数。
pub struct DefaultRpcAllocConfig;

impl TrRpcClientAllocConfig for DefaultRpcAllocConfig {
    type SharedConnAlloc = abs_mm::CoreAlloc;

    type RingAlloc = abs_mm::CoreAlloc;
}

/// 客户端配置所对应的垛口类型。
///
/// 它从连接配置推导而来，本 crate 不重新定义垛口。
pub type Dock<TyRpcCliCfg> =
    <<<TyRpcCliCfg as abs_mptp::client::config::TrConfig>::MuxConn as abs_smux::conn::TrConnection>::Config
        as abs_smux::conf::TrMuxConfig>::Dock;
