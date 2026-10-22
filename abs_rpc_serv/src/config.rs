//! 服务端侧的资源分配约定与服务端配置契约。
//!
//! 这些 trait 由宏生成代码与核心实现共同依赖，因此定义放在本 crate，而不是
//! `rpc_core_v1`。具体配置类型（如 `ServingConfig` / `DefaultServingAllocConfig`）在 core。
//!
//! # `config`: abstract serving configuration
//!
//! These traits define the serving allocation and configuration contract; concrete
//! configuration types live in `rpc_core_v1`.
use core::alloc::AllocatorClone;

/// 服务端侧的资源分配约定。
pub trait TrServingAllocConfig {
    /// 用于共享连接对象（`mm_ptr::Shared`）的分配器。
    type SharedConnAlloc: AllocatorClone;

    /// 子流 ring 存储的分配器。
    ///
    /// 每条入向子流都由本端当场交出两块**新的**内存（上游 `accept_async` 会拿走准备器的
    /// 所有权），因此这里的分配器每条子流都会被用到一次。
    ///
    /// 三条附加约束的理由与客户端那份相同（见 `rpc_core_v1` 的
    /// `TrRpcClientAllocConfig::RingAlloc`）：默认工厂由 `with_ctx` 无参构造，并要能搬进
    /// 连接的循环。
    type RingAlloc: AllocatorClone + Default + Send + Sync + 'static;
}

/// 一个 MPTP RPC 服务端所需的配置。
///
/// 与 `rpc_core_v1` 的 `TrRpcClientConfig` 对称：只补 `mptp_smux` 没有回答的内存分配约定。
/// 服务端的处理器类型由**路由表**在使用点给出，因此这里不把它钉死。
pub trait TrServingConfig: mptp_smux::serving::config::TrConfig + Sized {
    /// 本端的内存分配约定（共享连接对象与子流 ring 各自的分配器）。
    type AllocCfg: TrServingAllocConfig;
}
