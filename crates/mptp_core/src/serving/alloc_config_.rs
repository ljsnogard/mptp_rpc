//! 服务端侧的资源分配约定。
//!
//! 与客户端那份 [`TrClienAllocConfig`](crate::client::alloc_config_::TrClienAllocConfig)
//! **刻意不共享**：两端的进程、内存策略与生命周期各自独立（见
//! [`TrServingConfig`](super::config::TrServingConfig) 的文档）。形状相同只是巧合——
//! 因为 `abs_smux` 对两端的 ring 内存契约本来就是同一个。
//!
//! 服务端比客户端多一层必要性：它必须在**最终裁决**（`ChannelHandle::accept_async`）
//! 时当场交出两条 ring，所以这份配置是服务端 accept 循环的必需输入，而不是可选的。

use core::{alloc::AllocatorClone, mem::MaybeUninit};

use abs_mm::res_man::TrUnique;
use abs_smux::{
    chan::{RingBuffAlloc, TrPrepareRing},
    x_deps::abs_mm,
};

/// 服务端侧的资源分配约定。
pub trait TrServingAllocConfig {
    /// 用于共享连接对象（`mm_ptr::Shared`）的分配器。
    type SharedConnAlloc: AllocatorClone;

    /// 子流 ring 存储的分配器。
    type RingAlloc: AllocatorClone;

    /// 本端为一条子流交出的 ring 存储智能指针（发送、接收方向各一块）。
    type RingBuff: 'static
        + Send
        + Sync
        + TrUnique<Item = [MaybeUninit<u8>], Alloc = Self::RingAlloc>;

    /// 一条子流向一个方向的 ring 容量（字节）。
    ///
    /// 它同时决定本端在 `OPEN` 帧里通告的接收窗口，因此**不能**为零。
    const RING_CAPACITY: usize;

    /// 为一条子流造出 `(发送方向, 接收方向)` 两块 ring 内存。
    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff);
}

/// 把两块 ring 内存包成上游 [`TrPrepareRing`] 契约的通用准备策略。
pub struct ServingRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    tx_: B,
    rx_: B,
}

impl<B> ServingRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    /// 由发送方向与接收方向的两块 ring 内存构造。
    pub const fn new(tx_buff: B, rx_buff: B) -> Self {
        ServingRingPrepare {
            tx_: tx_buff,
            rx_: rx_buff,
        }
    }
}

impl<B> TrPrepareRing<B, u8> for ServingRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    fn prepare(self) -> RingBuffAlloc<B, u8> {
        RingBuffAlloc::new(self.tx_, self.rx_)
    }
}
