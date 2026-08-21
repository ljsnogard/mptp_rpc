//! 服务端内存 Channel 与客户端模拟 Channel。
//!
//! 这个模块提供不依赖网络的 `ServiceChannel` / `ClientChannel` 对，用于：
//!
//! - 在测试中直接模拟客户端和服务端收发请求；
//! - 让 handler 在纯内存环境里读写请求体 / 回复体；
//! - 后续接入真实传输层时，`ServiceChannel` 可以替换为 Iroh/QUIC Channel。
//!
//! # 设计
//!
//! 每一对 `(ServiceChannel, ClientChannel)` 内部包含两个 ring buffer：
//!
//! - `request ring`：客户端 `ClientChannel.tx` 写入请求，服务端 `ServiceChannel.rx` 读取；
//! - `response ring`：服务端 `ServiceChannel.tx` 写入回复，客户端 `ClientChannel.rx` 读取。
//!
//! `ServiceChannel` 实现 [`TrChannel`]，因此可以像真实传输层一样 `split()` 出
//! 服务端视角的 `Tx`（回复）和 `Rx`（请求）。

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

/// 服务端视角的内存 Channel。
pub struct RpcChannel {
    /// 服务端 -> 客户端（回复）。
    tx_: TxHalf,
    /// 客户端 -> 服务端（请求）。
    rx_: RxHalf,
}

impl RpcChannel {
    /// 创建一个新的服务端/客户端内存 Channel 对。
    pub fn new_pair() -> RpcChannel {
        todo!()
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
