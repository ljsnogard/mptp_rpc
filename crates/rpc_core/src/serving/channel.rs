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

use std::sync::Arc;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    x_deps::{abs_cancel::TrMayCancel, anylr::SomeOf},
};
use buffex::ring_buffer::{RingBuffer, RingRx, RingTx};

use crate::transport::TrChannel;

type Ring = RingBuffer<Box<[u8]>>;
type TxHalf = RingTx<Arc<Ring>, Box<[u8]>>;
type RxHalf = RingRx<Arc<Ring>, Box<[u8]>>;

/// 默认 ring 容量，足够测试和小型消息使用。
const RING_CAPACITY: usize = 64 * 1024;

fn new_ring() -> Ring {
    RingBuffer::try_new(Box::from(vec![0u8; RING_CAPACITY]))
        .expect("in-memory ring capacity must be valid")
}

fn split_ring() -> (TxHalf, RxHalf) {
    let ring = Arc::new(new_ring());
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("new ring must be uniquely owned")
}

/// 服务端视角的内存 Channel。
pub struct ServiceChannel {
    /// 服务端 -> 客户端（回复）。
    server_tx_: TxHalf,
    /// 客户端 -> 服务端（请求）。
    server_rx_: RxHalf,
}

/// 客户端视角的内存 Channel，与 [`ServiceChannel`] 成对出现。
pub struct ClientChannel {
    /// 客户端 -> 服务端（请求）。
    client_tx_: TxHalf,
    /// 服务端 -> 客户端（回复）。
    client_rx_: RxHalf,
}

impl ServiceChannel {
    /// 创建一个新的服务端/客户端内存 Channel 对。
    pub fn new_pair() -> (ServiceChannel, ClientChannel) {
        // response ring: server writes, client reads.
        let (server_tx_, client_rx_) = split_ring();
        // request ring: client writes, server reads.
        let (client_tx_, server_rx_) = split_ring();

        (
            ServiceChannel {
                server_tx_,
                server_rx_,
            },
            ClientChannel {
                client_tx_,
                client_rx_,
            },
        )
    }
}

impl ClientChannel {
    /// 获取客户端写入请求的半通道（`TrBuffTryWrite`）。
    pub fn split_tx(&mut self) -> ClientTx<'_> {
        ClientTx(&mut self.client_tx_)
    }

    /// 获取客户端读取回复的半通道（`TrBuffTryRead`）。
    pub fn split_rx(&mut self) -> ClientRx<'_> {
        ClientRx(&mut self.client_rx_)
    }
}

impl TrChannel for ServiceChannel {
    type Tx<'f>
        = ServiceTx<'f>
    where
        Self: 'f;
    type Rx<'f>
        = ServiceRx<'f>
    where
        Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        (
            ServiceTx(&mut self.server_tx_),
            ServiceRx(&mut self.server_rx_),
        )
    }
}

// ---------------------------------------------------------------------------
// 服务端半通道包装
// ---------------------------------------------------------------------------

/// 服务端回复写入半通道。
pub struct ServiceTx<'f>(&'f mut TxHalf);

/// 服务端请求读取半通道。
pub struct ServiceRx<'f>(&'f mut RxHalf);

impl TrBuffWrite for ServiceTx<'_> {
    type SegmMut<'a> = <TxHalf as TrBuffWrite>::SegmMut<'a>
    where
        Self: 'a;
    type Err = <TxHalf as TrBuffWrite>::Err;

    fn is_blocked_closing(&self) -> bool {
        self.0.is_blocked_closing()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>> {
        <TxHalf as TrBuffWrite>::write_async(self.0, demand)
    }
}

impl TrBuffTryWrite for ServiceTx<'_> {
    fn try_write<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <TxHalf as TrBuffTryWrite>::try_write(self.0, demand)
    }
}

impl TrBuffRead for ServiceRx<'_> {
    type SegmRef<'a> = <RxHalf as TrBuffRead>::SegmRef<'a>
    where
        Self: 'a;
    type Err = <RxHalf as TrBuffRead>::Err;

    fn is_drained_closing(&self) -> bool {
        self.0.is_drained_closing()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>> {
        <RxHalf as TrBuffRead>::read_async(self.0, demand)
    }
}

impl TrBuffTryRead for ServiceRx<'_> {
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <RxHalf as TrBuffTryRead>::try_read(self.0, demand)
    }
}

// ---------------------------------------------------------------------------
// 客户端半通道包装
// ---------------------------------------------------------------------------

/// 客户端请求写入半通道。
pub struct ClientTx<'f>(&'f mut TxHalf);

/// 客户端回复读取半通道。
pub struct ClientRx<'f>(&'f mut RxHalf);

impl TrBuffWrite for ClientTx<'_> {
    type SegmMut<'a> = <TxHalf as TrBuffWrite>::SegmMut<'a>
    where
        Self: 'a;
    type Err = <TxHalf as TrBuffWrite>::Err;

    fn is_blocked_closing(&self) -> bool {
        self.0.is_blocked_closing()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>> {
        <TxHalf as TrBuffWrite>::write_async(self.0, demand)
    }
}

impl TrBuffTryWrite for ClientTx<'_> {
    fn try_write<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <TxHalf as TrBuffTryWrite>::try_write(self.0, demand)
    }
}

impl TrBuffRead for ClientRx<'_> {
    type SegmRef<'a> = <RxHalf as TrBuffRead>::SegmRef<'a>
    where
        Self: 'a;
    type Err = <RxHalf as TrBuffRead>::Err;

    fn is_drained_closing(&self) -> bool {
        self.0.is_drained_closing()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>> {
        <RxHalf as TrBuffRead>::read_async(self.0, demand)
    }
}

impl TrBuffTryRead for ClientRx<'_> {
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <RxHalf as TrBuffTryRead>::try_read(self.0, demand)
    }
}
