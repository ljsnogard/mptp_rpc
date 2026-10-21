//! 客户端侧的资源分配约定。
//!
//! 与 `abs_smux` 的契约对齐：一条子流的 ring 存储由调用方在**最终裁决**
//! （[`TrChannelHandle::accept_async`](abs_smux::chan::TrChannelHandle::accept_async)）时当场
//! 交出，连接只校验容量能否建出环，不替调用方决定尺寸与来源。因此客户端配置必须回答三件事：
//! ring 存储用哪种智能指针、每个方向多大、**从哪分配**。
//!
//! 共享连接对象（`mm_ptr::Shared`）的分配器与子流 ring 的分配器分开声明：前者在客户端构造时
//! 就要用，后者每条子流用一次，二者的来源未必相同。这条分开是刻意的——把两者合成一个分配器，
//! 就等于假定「连接的生命周期与每条子流的生命周期同源」，而长连接接短子流的场合恰恰相反。
//!
//! # `alloc_config_`: where the client's memory comes from
//!
//! The shared connection object and the per-channel ring storage are two separate allocation
//! questions: the first is answered once when the client is built, the second once per channel.
//! Keeping them apart means a long-lived connection may draw from a different pool than the
//! short-lived channels riding on it.

use core::alloc::AllocatorClone;

/// 客户端侧的资源分配约定。
///
/// 它只回答**类型**：分配器的实例由使用者在构造处当场给出（共享连接对象）或由准备工厂在
/// 最终裁决前取出（ring 存储）。这条分工与 `abs_smux` 一致——内存的来源属于调用方，不属于
/// 连接。
pub trait TrRpcClientAllocConfig {
    /// 用于共享连接对象（`mm_ptr::Shared`）的分配器。
    ///
    /// 实例由使用者在构造处当场给出，因此只需要「可克隆的分配器」这一条。
    type SharedConnAlloc: AllocatorClone;

    /// 子流 ring 存储的分配器。
    ///
    /// ring 是「每条子流两块、随子流结束归还」的内存，因此它与连接对象的分配器分开声明：
    /// 前者可以走内存池，后者常常就是栈上的一个 zero-sized 句柄。
    ///
    /// # 为什么多三条约束
    ///
    /// 默认路径下 ring 是由**本 crate 的工厂**造的（见 [`crate::ClientPrepFactory`]），而工厂实例由
    /// `with_ctx` 无参现场构造、随后连同准备器一起被搬进连接的循环。因此这个分配器必须
    /// **无参可造**（`Default`）且**能跨线程共享**（`Send + Sync + 'static`）。带状态、需要
    /// 外部句柄的分配器走不了这条默认路径——那种场合请连同工厂一起自己实现。
    type RingAlloc: AllocatorClone + Default + Send + Sync + 'static;
}
