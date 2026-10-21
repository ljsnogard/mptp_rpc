//! 服务端侧的公开契约：**使用者**要回答什么，RPC 层才不会替它决定监听与内存。
//!
//! Phase 0 里这个契约与客户端那份对称：服务端只需要在
//! [`mptp_smux::serving::config::TrConfig`] 之上再回答本端的**内存分配约定**。请求 / 回复
//! 前缀与头表的线上形态由 `mptp_smux` 拥有，不再是本 crate 的配置项。
//!
//! # 为什么与客户端那份完全独立
//!
//! 服务端与客户端**可能跑在两个不同的进程里**，由两份不同的代码编译、可以各自独立升级。
//! 它们的连接类型与内存策略也各自独立。把两者合成一个配置，就等于假定「客户端与服务端永远
//! 是同一份二进制」，而这恰恰是 RPC 框架不该假定的东西。
//!
//! # `serving::config`: what the caller must answer on the serving side
//!
//! Deliberately independent from the client's counterpart: the two ends may be built from two
//! different codebases. This trait only adds the serving side's allocation policy.

use super::alloc_config_::TrServingAllocConfig;
use crate::x_deps::{abs_mm, mptp_smux};

/// 一个 MPTP RPC 服务端所需的配置。
///
/// 与 [`TrRpcClientConfig`](crate::client::TrRpcClientConfig) 对称：只补 `mptp_smux` 没有
/// 回答的内存分配约定。服务端的处理器类型由**路由表**在使用点给出，因此这里不把它钉死。
pub trait TrServingConfig: mptp_smux::serving::config::TrConfig + Sized {
    /// 本端的内存分配约定（共享连接对象与子流 ring 各自的分配器）。
    type AllocCfg: TrServingAllocConfig;
}

/// **缺省**的服务端分配约定：共享连接对象与 ring 都走系统分配器。
///
/// 与客户端的 [`DefaultRpcAllocConfig`](crate::client::DefaultRpcAllocConfig) 是**两个类型**：
/// 两端各自独立演进，共用一份默认值只会把「两端可以不同」这件事悄悄抹掉。
pub struct DefaultServingAllocConfig;

impl TrServingAllocConfig for DefaultServingAllocConfig {
    type SharedConnAlloc = abs_mm::CoreAlloc;

    type RingAlloc = abs_mm::CoreAlloc;
}
