//! 内存回环 Channel。
//!
//! 这个模块提供不依赖网络的 [`RpcChannel`]，用于：
//!
//! - 在测试中直接模拟一次请求/回复的完整收发；
//! - 让 handler 在纯内存环境里读写请求体 / 回复体；
//! - 后续接入真实传输层时，`RpcChannel` 可以替换为 Iroh/QUIC Channel。
//!
//! # 设计
//!
//! [`RpcChannel`] 内部使用一个 `buffex::circular_buff` 的**双端被动**
//! （passive × passive）环形缓冲：
//!
//! - 写半通道（[`RpcTx`]）包装 `circular_buff` 的被动生产端 `Producer`；
//! - 读半通道（[`RpcRx`]）包装 `circular_buff` 的被动消费端 `Consumer`；
//! - `split()` 返回这两个借用包装，写端与读端共享同一个环形核心，因此写入
//!   `Tx` 的数据可以从同一次 `split()` 返回的 `Rx` 读回。
//!
//! `CircularBuff` 的「被动 × 被动」模式正好对应这里的纯内存回环：不需要后台
//! 任务或设备，调用者通过 `abs_buff` 的 `TrBuffWrite` / `TrBuffRead` 直接
//! 驱动读写。上游传输层适配（`buffex_iroh`）已经先行从 `ring_buffer` 迁到
//! `circular_buff`，这里也跟随同样的设计，让 `RpcChannel` 与真实传输层的
//! 底层缓冲模型保持一致。

use std::{
    mem::MaybeUninit,
    sync::Arc,
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::anylr,
};
use anylr::SomeOf;
use buffex::{
    ring_buffer::{RingBuffer, RingRx, RingTx},
    x_deps::abs_buff,
};

use crate::transport::TrChannel;

/// 重构前使用的共享环形存储类型。
///
/// 在切换到 `circular_buff` 后，这个类型不再需要：`RpcChannel` 直接持有
/// 构建器产出的被动 `Producer` / `Consumer` 半部，无需再手动创建 `Arc<Ring>`
/// 并 `try_split_shared`。
type Ring = RingBuffer<Box<[MaybeUninit<u8>]>>;
/// 重构前的写半通道内部类型。
///
/// 重构后对应 `circular_buff::Producer<BufConsumer<u8>, ...>`（被动生产端）。
type TxHalf = RingTx<Arc<Ring>, Box<[MaybeUninit<u8>]>>;
/// 重构前的读半通道内部类型。
///
/// 重构后对应 `circular_buff::Consumer<BufProducer<u8>, ...>`（被动消费端）。
type RxHalf = RingRx<Arc<Ring>, Box<[MaybeUninit<u8>]>>;

/// 默认缓冲容量，足够测试和小型消息使用。
const RING_CAPACITY: usize = 64 * 1024;

/// 重构前的构造辅助：创建一个 `ring_buffer::RingBuffer`。
///
/// 切换到 `circular_buff` 后由 `CircularBuffBuilder::with_capacity(...)`
/// 的异步构建取代；`RpcChannel::new_pair()` 将直接拿到构建器产出的半部对。
fn new_ring() -> Ring {
    RingBuffer::try_new(Box::new_uninit_slice(RING_CAPACITY))
        .expect("in-memory ring capacity must be valid")
}

/// 重构前的拆分辅助：把共享 ring 拆成写 / 读两个半部。
///
/// `circular_buff` 的构建器在 `build_async()` 中已经返回
/// `(Producer, Consumer)` 半部对，因此重构后不再需要手动 `Arc` +
/// `try_split_shared`。
fn split_ring() -> (TxHalf, RxHalf) {
    let ring = Arc::new(new_ring());
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("new ring must be uniquely owned")
}

/// 内存回环 Channel。
///
/// 内部持有 `circular_buff` 的**双端被动**半部：`tx_` 是被动生产端（写），
/// `rx_` 是被动消费端（读）。两个半部共享同一个环形核心，因此写入 `tx_`
/// 的数据可以在 `rx_` 读回。
pub struct RpcChannel {
    /// 被动生产端半部（写入方）。
    tx_: TxHalf,
    /// 被动消费端半部（读取方）。
    rx_: RxHalf,
}

impl RpcChannel {
    /// 创建一个内存回环 Channel。
    ///
    /// 通过 `CircularBuffBuilder::with_capacity` 构建一个双端被动
    /// （passive × passive）的 `SpscPair`；该模式不接入任何设备 / 后台任务，
    /// 正好满足纯内存回环语义。
    ///
    /// 写入 `split()` 返回的 `Tx` 的数据，可以从同一次 `split()` 返回的 `Rx`
    /// 读回；适合在测试和 Demo 中模拟一次请求/回复的完整收发。
    pub fn new_pair() -> RpcChannel {
        let (tx, rx) = split_ring();
        RpcChannel { tx_: tx, rx_: rx }
    }
}

impl TrChannel for RpcChannel {
    type Tx<'f> = RpcTx<'f> where Self: 'f;
    type Rx<'f> = RpcRx<'f> where Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        (
            RpcTx(&mut self.tx_),
            RpcRx(&mut self.rx_),
        )
    }
}

// ---------------------------------------------------------------------------
// 服务端半通道包装
// ---------------------------------------------------------------------------

/// 写入半通道。
///
/// 借用 `circular_buff` 的**被动生产端**半部，并把 `abs_buff` 的
/// `TrBuffWrite` / `TrBuffTryWrite` 段操作转发给内部的 `Producer`。
///
/// 上游 `abs_buff` 已把写侧关闭判断统一为 `TrBuffWrite::is_stuffed_closing`；
/// 重构时 `RpcTx` 的 trait 实现也要同步改为转发该新方法（旧的
/// `is_blocked_closing` 已不存在）。
pub struct RpcTx<'f>(&'f mut TxHalf);

/// 读取半通道。
///
/// 借用 `circular_buff` 的**被动消费端**半部，并把 `abs_buff` 的
/// `TrBuffRead` / `TrBuffTryRead` 段操作转发给内部的 `Consumer`。
pub struct RpcRx<'f>(&'f mut RxHalf);

impl TrBuffWrite for RpcTx<'_> {
    type SegmMut<'a> = <TxHalf as TrBuffWrite>::SegmMut<'a> where Self: 'a;
    type Err = <TxHalf as TrBuffWrite>::Err;

    type WriteAsync<'f> = <TxHalf as TrBuffWrite>::WriteAsync<'f>
        where Self: 'f;

    fn is_stuffed_closing(&self) -> bool {
        self.0.is_stuffed_closing()
    }

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        <TxHalf as TrBuffWrite>::write_async(self.0, demand)
    }
}

impl TrBuffTryWrite for RpcTx<'_> {
    fn try_write<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <TxHalf as TrBuffTryWrite>::try_write(self.0, demand)
    }
}

impl TrBuffRead for RpcRx<'_> {
    type SegmRef<'a> = <RxHalf as TrBuffRead>::SegmRef<'a> where Self: 'a;
    type Err = <RxHalf as TrBuffRead>::Err;

    type ReadAsync<'f> = <RxHalf as TrBuffRead>::ReadAsync<'f>
        where Self: 'f;

    fn is_drained_closing(&self) -> bool {
        self.0.is_drained_closing()
    }

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        <RxHalf as TrBuffRead>::read_async(self.0, demand)
    }
}

impl TrBuffTryRead for RpcRx<'_> {
    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <RxHalf as TrBuffTryRead>::try_read(self.0, demand)
    }
}
