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
//! [`RpcChannel`] 内部使用一个 ring buffer：
//!
//! - `split()` 返回的 `Tx` 写入数据；
//! - `split()` 返回的 `Rx` 读回数据。
//!
//! 这样单个 [`RpcChannel`] 就能在测试中同时扮演“发送方”和“接收方”，
//! 适合当前请求/回复一问一答的 Demo 场景。

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

type Ring = RingBuffer<Box<[MaybeUninit<u8>]>>;
type TxHalf = RingTx<Arc<Ring>, Box<[MaybeUninit<u8>]>>;
type RxHalf = RingRx<Arc<Ring>, Box<[MaybeUninit<u8>]>>;

/// 默认 ring 容量，足够测试和小型消息使用。
const RING_CAPACITY: usize = 64 * 1024;

fn new_ring() -> Ring {
    RingBuffer::try_new(Box::new_uninit_slice(RING_CAPACITY))
        .expect("in-memory ring capacity must be valid")
}

fn split_ring() -> (TxHalf, RxHalf) {
    let ring = Arc::new(new_ring());
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("new ring must be uniquely owned")
}

/// 内存回环 Channel。
pub struct RpcChannel {
    /// 写入半通道。
    tx_: TxHalf,
    /// 读取半通道。
    rx_: RxHalf,
}

impl RpcChannel {
    /// 创建一个内存回环 Channel。
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
pub struct RpcTx<'f>(&'f mut TxHalf);

/// 读取半通道。
pub struct RpcRx<'f>(&'f mut RxHalf);

impl TrBuffWrite for RpcTx<'_> {
    type SegmMut<'a> = <TxHalf as TrBuffWrite>::SegmMut<'a> where Self: 'a;
    type Err = <TxHalf as TrBuffWrite>::Err;

    type WriteAsync<'f> = <TxHalf as TrBuffWrite>::WriteAsync<'f>
        where Self: 'f;

    fn is_blocked_closing(&self) -> bool {
        self.0.is_blocked_closing()
    }

    fn write_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::WriteAsync<'f> {
        <TxHalf as TrBuffWrite>::write_async(self.0, demand)
    }
}

impl TrBuffTryWrite for RpcTx<'_> {
    fn try_write<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
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

    fn read_async<'f>(&'f mut self, demand: &Demand<usize>) -> Self::ReadAsync<'f> {
        <RxHalf as TrBuffRead>::read_async(self.0, demand)
    }
}

impl TrBuffTryRead for RpcRx<'_> {
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <RxHalf as TrBuffTryRead>::try_read(self.0, demand)
    }
}
