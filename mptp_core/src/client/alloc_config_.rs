//! 客户端侧的资源分配约定。
//!
//! 与 `abs_smux` 的契约对齐：一条子流的 ring 存储由调用方在**最终裁决**
//! （[`TrChannelHandle::accept_async`](abs_smux::chan::TrChannelHandle::accept_async)）
//! 时当场交出，连接只校验容量能否建出环，不替调用方决定尺寸与来源。因此客户端配置
//! 必须回答三件事：ring 存储用哪种智能指针、每个方向多大、从哪分配。
//!
//! 共享连接对象（`mm_ptr::Shared`）的分配器与子流 ring 的分配器分开声明：前者在
//! 客户端构造时就要用，后者每条子流用一次，二者的来源未必相同。

use core::{alloc::AllocatorClone, mem::MaybeUninit};

use abs_mm::res_man::TrUnique;
use abs_smux::{
    chan::{RingBuffAlloc, TrPrepareRing},
    x_deps::abs_mm,
};

/// 客户端侧的资源分配约定。
pub trait TrClienAllocConfig {
    /// 用于共享连接对象（`mm_ptr::Shared`）的分配器。
    type SharedConnAlloc: AllocatorClone;

    /// 子流 ring 存储的分配器。
    type RingAlloc: AllocatorClone;

    /// 本端为一条子流交出的 ring 存储智能指针（发送、接收方向各一块）。
    ///
    /// 「两块内存各自用一个智能指针持有」这个形状来自上游
    /// [`RingBuffAlloc`](abs_smux::chan::RingBuffAlloc)：连接只要求 `B` 能报出
    /// 自己的分配器（[`TrUnique::Alloc`] 是 [`AllocatorClone`]），因为连接会把
    /// `B` 连同 ring 一起搬进自己的记账结构，之后只能靠 `B` 自己归还。
    type RingBuff: 'static
        + Send
        + Sync
        + TrUnique<Item = [MaybeUninit<u8>], Alloc = Self::RingAlloc>;

    /// 一条子流向一个方向的 ring 容量（字节）。
    ///
    /// 它同时决定本端在 `OPEN` 帧里通告的接收窗口，因此**不能**为零。
    const RING_CAPACITY: usize;

    /// 为一条子流造出 `(发送方向, 接收方向)` 两块 ring 内存。
    ///
    /// 两块内存的容量都应当是 [`Self::RING_CAPACITY`]；容量不合法时连接会在
    /// `accept_async` 里拒绝接受，而不是替调用方改尺寸。
    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff);
}

/// 把两块 ring 内存包成上游 [`TrPrepareRing`] 契约的通用准备策略。
///
/// 客户端与 [`TrClienAllocConfig::make_ring_buffs`] 配合使用：后者决定内存从哪来、
/// 多大，本类型只负责把「两块内存」按上游期望的形状交出去（上游刻意把两块内存打包成
/// 一个 `self` 参数，好让调用点的类型参数保持在 `B` 一个上）。
pub struct ClientRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    tx_: B,
    rx_: B,
}

impl<B> ClientRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    /// 由发送方向与接收方向的两块 ring 内存构造。
    pub const fn new(tx_buff: B, rx_buff: B) -> Self {
        ClientRingPrepare {
            tx_: tx_buff,
            rx_: rx_buff,
        }
    }
}

impl<B> TrPrepareRing<B, u8> for ClientRingPrepare<B>
where
    B: 'static + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
{
    fn prepare(self) -> RingBuffAlloc<B, u8> {
        RingBuffAlloc::new(self.tx_, self.rx_)
    }
}
