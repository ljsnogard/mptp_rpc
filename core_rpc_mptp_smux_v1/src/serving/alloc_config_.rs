//! 服务端侧的资源分配约定。
//!
//! 与客户端那份 [`TrRpcClienAllocConfig`](crate::client::TrRpcClienAllocConfig)
//! **刻意不共享**：两端的进程、内存策略与生命周期各自独立（见
//! [`TrServingConfig`](super::config::TrServingConfig) 的文档）。形状相同只是巧合——
//! 因为 `abs_smux` 对两端的 ring 内存契约本来就是同一个。
//!
//! 服务端比客户端多一层必要性：它必须在**最终裁决**（`ChannelHandle::accept_async`）时当场
//! 交出两条 ring，所以这份约定是服务端 accept 循环的必需输入，而不是可选的。
//!
//! # `alloc_config_`: where the server's memory comes from
//!
//! Deliberately not shared with the client's counterpart: the two ends may live in different
//! processes with different memory policies. The shape being identical is a coincidence of
//! `abs_smux`'s per-end ring contract, not a shared decision.

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
    /// 三条附加约束的理由与客户端那份相同（见
    /// [`TrRpcClientAllocConfig::RingAlloc`](crate::client::TrRpcClientAllocConfig::RingAlloc)）：
    /// 默认工厂由 `with_ctx` 无参构造，并要能搬进连接的循环。
    type RingAlloc: AllocatorClone + Default + Send + Sync + 'static;
}
