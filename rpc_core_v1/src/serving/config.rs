//! 服务端侧的公开契约：**使用者**要回答什么，RPC 层才不会替它决定监听与内存。
//!
//! 两条 trait 的**定义**在 `abs_rpc_serv::config`：那里不依赖本 crate，宏生成的代码也能
//! 认识它们。本模块只把它们转出，并提供一份现成的缺省分配约定
//! [`DefaultServingAllocConfig`]。
//!
//! 服务端与客户端**可能跑在两个不同的进程里**，由两份不同的代码编译、可以各自独立升级。
//! 它们的连接类型与内存策略也各自独立；把两者合成一个配置，就等于假定「客户端与服务端永远
//! 是同一份二进制」，而这恰恰是 RPC 框架不该假定的东西。
//!
//! # `serving::config`: what the caller must answer on the serving side
//!
//! The trait definitions live in `abs_rpc_serv::config` so macro-generated code can name them
//! without depending on this crate. This module re-exports them and adds a ready-made default
//! allocation policy.

use crate::x_deps::{abs_mm, abs_rpc_serv};

pub use abs_rpc_serv::config::{TrServingAllocConfig, TrServingConfig};

/// **缺省**的服务端分配约定：共享连接对象与 ring 都走系统分配器。
///
/// 与客户端的 [`DefaultRpcAllocConfig`](crate::client::DefaultRpcAllocConfig) 是**两个类型**：
/// 两端各自独立演进，共用一份默认值只会把「两端可以不同」这件事悄悄抹掉。
pub struct DefaultServingAllocConfig;

impl TrServingAllocConfig for DefaultServingAllocConfig {
    type SharedConnAlloc = abs_mm::CoreAlloc;

    type RingAlloc = abs_mm::CoreAlloc;
}
