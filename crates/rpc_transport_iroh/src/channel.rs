//! Iroh QUIC 流到 MPTP `TrChannel` 的桥接层。
//!
//! 本模块把一条 iroh 双向 QUIC stream（`SendStream` + `RecvStream`）封装成
//! [`IrohChannel`]，并让它实现 `mptp_rpc_core::transport::TrChannel`。
//!
//! # 设计思路
//!
//! - 每个方向各使用一个 `buffex::ring_buffer::RingBuffer`：
//!   - 发送方向：调用方（RPC 编码器）向 `RingTx` 写入字节，后台 send pump
//!     从 `RingRx` 读出并写入 iroh `SendStream`；
//!   - 接收方向：后台 recv pump 从 iroh `RecvStream` 读出字节并写入
//!     `RingTx`，调用方（RPC 解码器）从 `RingRx` 读取。
//! - `IrohSend` / `IrohRecv` 只是对 `RingTx` / `RingRx` 的借用包装，
//!   这样 `TrChannel::split` 可以返回借用型半通道，而不需要把内部对象 move 出去。
//! - 两个后台任务拥有 ring 的另一半；当 channel 被 drop 时 abort 后台任务，
//!   避免流泄漏。

use std::sync::Arc;

use iroh::endpoint::{RecvStream, SendStream};
use mptp_rpc_core::{
    transport::TrChannel,
    x_deps::buffex::{
        ring_buffer::{RingBuffer, RingRx, RingTx},
        x_deps::{
            abs_buff::{Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite},
            abs_cancel::TrMayCancel,
            anylr::SomeOf,
        },
    },
};

use crate::conn::IrohConnError;

/// 发送方向 ring 的类型：底层存储是 `Box<[u8]>`，通过 `Arc` 共享给前后台。
type SendRing = RingBuffer<Box<[u8]>>;
type SendTx = RingTx<Arc<SendRing>, Box<[u8]>>;
type SendRx = RingRx<Arc<SendRing>, Box<[u8]>>;

/// 接收方向 ring 的类型。
type RecvRing = RingBuffer<Box<[u8]>>;
type RecvTx = RingTx<Arc<RecvRing>, Box<[u8]>>;
type RecvRx = RingRx<Arc<RecvRing>, Box<[u8]>>;

/// ring 的默认容量。
///
/// 64 KiB 是一个兼顾吞吐与背压的起始值；后续可以根据实测调整。
const RING_CAPACITY: usize = 64 * 1024;
/// 后台 pump 单次搬运的最大字节数，避免一次性借出过大段。
const PUMP_CHUNK: usize = 32 * 1024;

/// 把一条 iroh 双向流封装成 MPTP Channel。
///
/// 创建后，`split()` 可以得到：
/// - [`IrohSend`]：向对端写入数据；
/// - [`IrohRecv`]：从对端读取数据。
pub struct IrohChannel {
    send_tx_: SendTx,
    recv_rx_: RecvRx,
}

impl IrohChannel {
    /// 由 iroh 双向流的收发端构造 channel，并启动两个后台 pump。
    pub(crate) fn new(send: SendStream, recv: RecvStream) -> Self {
        // 发送方向：调用方写入 -> send_tx_，pump 从 send_rx 读并写到网络。
        let (send_tx_, send_rx) = new_send_ring();
        // 接收方向：pump 从网络读并写入 recv_tx，调用方从 recv_rx_ 读。
        let (recv_tx, recv_rx_) = new_recv_ring();

        // 后台任务持有 ring 的另一半；channel 被 drop 时，发送 ring 的 tx 会关闭，
        // send pump 会在排空数据后自然结束，从而让对端读到 EOF。
        tokio::spawn(send_pump(send, send_rx));
        tokio::spawn(recv_pump(recv, recv_tx));

        IrohChannel { send_tx_, recv_rx_ }
    }
}

impl TrChannel for IrohChannel {
    type Tx<'f>
        = IrohSend<'f>
    where
        Self: 'f;
    type Rx<'f>
        = IrohRecv<'f>
    where
        Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        (IrohSend(&mut self.send_tx_), IrohRecv(&mut self.recv_rx_))
    }
}

// ---------------------------------------------------------------------------
// Ring 构造
// ---------------------------------------------------------------------------

fn new_send_ring() -> (SendTx, SendRx) {
    let ring = Arc::new(
        RingBuffer::try_new(Box::from(vec![0u8; RING_CAPACITY]))
            .expect("send ring capacity must be valid"),
    );
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("send ring must be uniquely owned before split")
}

fn new_recv_ring() -> (RecvTx, RecvRx) {
    let ring = Arc::new(
        RingBuffer::try_new(Box::from(vec![0u8; RING_CAPACITY]))
            .expect("recv ring capacity must be valid"),
    );
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("recv ring must be uniquely owned before split")
}

// ---------------------------------------------------------------------------
// 后台 pump
// ---------------------------------------------------------------------------

/// 发送 pump：把调用方写入发送 ring 的数据搬到 iroh `SendStream`。
async fn send_pump(mut send: SendStream, mut rx: SendRx) {
    loop {
        // 发送 ring 的 tx 已关闭且数据已排空 => 对端将看到 EOF。
        if rx.ring().data_size() == 0 && rx.ring().is_tx_closed() {
            break;
        }

        let mut segm = match rx.read_async(PUMP_CHUNK).await.pick_left() {
            Some(segm) => segm,
            None => break,
        };

        // 把 ring 中这段数据“移动”到临时缓冲，再写入 iroh。
        // 必须通过 move_items_to_buff 推进 segment 内部 offset，
        // 否则 drop 时 reclaim 0 字节，ring 的读位置不会前进。
        let len = segm.least_count();
        let mut tmp: Vec<core::mem::MaybeUninit<u8>> = Vec::with_capacity(len);
        tmp.resize_with(len, core::mem::MaybeUninit::uninit);
        let moved = unsafe { segm.move_items_to_buff(&mut tmp) };
        let bytes: Vec<u8> = tmp[..moved]
            .iter()
            .map(|m| unsafe { m.assume_init_read() })
            .collect();
        if send.write_all(&bytes).await.is_err() {
            break;
        }
        // segm 在这里 drop，通过 reclaim 提交消费，等价于从 ring 中移除这些字节。
    }
}

/// 接收 pump：把 iroh `RecvStream` 的数据搬到接收 ring，供调用方读取。
async fn recv_pump(mut recv: RecvStream, mut tx: RecvTx) {
    let mut buf = vec![0u8; PUMP_CHUNK];
    loop {
        let n = match recv.read(&mut buf).await {
            Ok(None) => {
                // iroh 对端 finish：关闭接收 ring 的写入端，调用方读到 EOF。
                tx.close();
                break;
            }
            Ok(Some(n)) => n,
            Err(_) => {
                // 网络错误同样按 EOF 处理，保证调用方不会永久等待。
                tx.close();
                break;
            }
        };

        let mut written = 0usize;
        while written < n {
            // 等待 ring 有可写空间。若返回错误（例如调用方已关闭读端），退出。
            let segm_opt = tx.write_async(n - written).await.pick_left();
            let mut segm = match segm_opt {
                Some(segm) => segm,
                None => {
                    // 返回时 `tx` 会 drop，drop 会关闭接收 ring 的写入端。
                    return;
                }
            };

            let take = segm.least_count().min(n - written);
            let mut child = segm.as_segm_mut();
            let moved = child.clone_items_from_buff(&buf[written..written + take]);
            debug_assert_eq!(moved, take);
            drop(child);
            // drop segm 提交 `take` 字节到接收 ring。
            drop(segm);
            written += take;
        }
    }
}

// ---------------------------------------------------------------------------
// 半通道：实现 abs_buff 的读写 trait
// ---------------------------------------------------------------------------

/// 发送半通道：包装发送 ring 的生产端。
pub struct IrohSend<'f>(&'f mut SendTx);

impl IrohSend<'_> {
    /// 便捷方法：把整个 `data` 写入对端。
    ///
    /// 这个方法会处理 ring 可能只提供部分连续空间的情况，适合 Demo 和测试直接使用。
    pub async fn write_all(&mut self, mut data: &[u8]) -> Result<(), IrohConnError> {
        while !data.is_empty() {
            let mut segm = self
                .0
                .write_async(data.len())
                .await
                .pick_left()
                .ok_or_else(|| IrohConnError::StreamIo("send ring closed".to_string()))?;
            let take = segm.least_count().min(data.len());
            // 通过 as_segm_mut + clone_items_from_buff 写入并推进 offset，
            // 这样 drop 父 segment 时才会把 take 字节真正提交到 ring。
            let mut child = segm.as_segm_mut();
            let moved = child.clone_items_from_buff(&data[..take]);
            debug_assert_eq!(moved, take);
            drop(child);
            drop(segm);
            data = &data[take..];
        }
        Ok(())
    }

    /// 关闭发送方向。
    ///
    /// 调用后，后台 send pump 会在把 ring 中剩余数据全部发出后结束 iroh
    /// `SendStream`，对端会读到 EOF。MPTP 中“请求头发送完毕且没有更多请求体”
    /// 或“Push 流发送完毕”时，应调用本方法。
    pub fn close(&mut self) {
        self.0.close();
    }
}

impl TrBuffWrite for IrohSend<'_> {
    type SegmMut<'a>
        = <SendTx as TrBuffWrite>::SegmMut<'a>
    where
        Self: 'a;
    type Err = <SendTx as TrBuffWrite>::Err;

    fn is_blocked(&self) -> bool {
        self.0.is_blocked()
    }

    fn write_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmMut<'f>, Self::Err>> {
        <SendTx as TrBuffWrite>::write_async(self.0, demand)
    }
}

impl TrBuffTryWrite for IrohSend<'_> {
    fn try_write<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <SendTx as TrBuffTryWrite>::try_write(self.0, demand)
    }
}

/// 接收半通道：包装接收 ring 的消费端。
pub struct IrohRecv<'f>(&'f mut RecvRx);

impl IrohRecv<'_> {
    /// 便捷方法：读取对端发来的全部数据直到 EOF。
    ///
    /// 注意：这不是“读一条消息”，而是把当前流上后续的所有字节追加到 `out`。
    /// 在 MPTP 中，普通 RPC 的 body 长度由 `Body_Size` 控制，调用方应该按需
    /// 读取，而不是总是调用本方法；本方法主要用于传输层测试和流式场景。
    pub async fn read_to_end(&mut self, out: &mut Vec<u8>) -> Result<usize, IrohConnError> {
        let mut total = 0usize;
        loop {
            if self.is_drained() {
                break;
            }
            let mut segm = match self.0.read_async(PUMP_CHUNK).await.pick_left() {
                Some(segm) => segm,
                None => break,
            };
            // 同样必须移动数据并推进 offset，否则读位置不会前进，read_to_end 会死循环。
            let len = segm.least_count();
            let mut tmp: Vec<core::mem::MaybeUninit<u8>> = Vec::with_capacity(len);
            tmp.resize_with(len, core::mem::MaybeUninit::uninit);
            let moved = unsafe { segm.move_items_to_buff(&mut tmp) };
            out.extend(tmp[..moved].iter().map(|m| unsafe { m.assume_init_read() }));
            total += moved;
        }
        Ok(total)
    }
}

impl TrBuffRead for IrohRecv<'_> {
    type SegmRef<'a>
        = <RecvRx as TrBuffRead>::SegmRef<'a>
    where
        Self: 'a;
    type Err = <RecvRx as TrBuffRead>::Err;

    fn is_drained(&self) -> bool {
        // 对调用方而言，“不会再有数据”= 接收 ring 的写入端（recv pump）已关闭，
        // 且 ring 中的数据已经全部被读走。
        self.0.ring().data_size() == 0 && self.0.ring().is_tx_closed()
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>> {
        <RecvRx as TrBuffRead>::read_async(self.0, demand)
    }
}

impl TrBuffTryRead for IrohRecv<'_> {
    fn try_read<'f>(&'f mut self, demand: &Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        <RecvRx as TrBuffTryRead>::try_read(self.0, demand)
    }
}

#[cfg(test)]
mod tests_ {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use iroh::{Endpoint, EndpointAddr, TransportAddr, endpoint::presets::N0};
    use mptp_rpc_core::{
        transport::TrMuxConn, x_deps::buffex::x_deps::abs_cancel::NonCancellableToken,
    };

    use super::*;
    use crate::IrohConnection;

    const ALPN: &[u8] = b"mptp-rpc-iroh/test/1";

    /// 服务端：接受一条连接和一条 channel，读取全部请求字节后返回给调用者。
    async fn server_read_all(
        server_endpoint: Endpoint,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = IrohConnection::accept(server_endpoint).await?;
        let mut channel = conn.accept_channel_async().await?;
        let (_tx, mut rx) = channel.split();
        let mut out = Vec::new();
        rx.read_to_end(&mut out).await?;
        Ok(out)
    }

    /// 端到端直连测试：客户端写一段数据，服务端完整读回。
    #[tokio::test(flavor = "multi_thread")]
    async fn direct_connect_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 找一个空闲端口，服务端只监听 localhost。
        let free_port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            l.local_addr()?.port()
        };
        let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, free_port);

        let server_endpoint = Endpoint::builder(N0)
            .alpns(vec![ALPN.to_vec()])
            .clear_ip_transports()
            .clear_relay_transports()
            .bind_addr(server_addr)?
            .bind()
            .await?;
        let server_id = server_endpoint.id();

        let server_task = tokio::spawn(server_read_all(server_endpoint));

        let client_endpoint = Endpoint::builder(N0)
            .alpns(vec![ALPN.to_vec()])
            .clear_relay_transports()
            .bind()
            .await?;
        let server_ep_addr = EndpointAddr::from_parts(
            server_id,
            vec![TransportAddr::Ip(std::net::SocketAddr::V4(server_addr))],
        );
        let conn = IrohConnection::connect_by_addr(client_endpoint, server_ep_addr, ALPN).await?;
        assert_eq!(conn.remote_id(), Some(&server_id));

        let mut channel = conn
            .open_channel_async()
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await?;
        let (mut tx, _rx) = channel.split();
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        tx.write_all(&payload).await?;

        // 关闭发送端，让服务端读到 EOF。
        tx.close();
        drop(tx);
        drop(_rx);

        let got = server_task.await??;
        assert_eq!(got, payload);

        // channel 在服务端结束后再 drop，确保后台 pump 不会过早 abort。
        drop(channel);
        Ok(())
    }

    /// 服务端：接受连接上的两条 channel，分别读回数据并返回。
    async fn server_read_two_channels(
        server_endpoint: Endpoint,
    ) -> Result<(Vec<u8>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        let conn = IrohConnection::accept(server_endpoint).await?;
        let mut ch1 = conn.accept_channel_async().await?;
        let mut ch2 = conn.accept_channel_async().await?;

        let (_tx1, mut rx1) = ch1.split();
        let (_tx2, mut rx2) = ch2.split();

        let read1 = async {
            let mut data = Vec::new();
            rx1.read_to_end(&mut data).await?;
            Ok::<_, IrohConnError>(data)
        };
        let read2 = async {
            let mut data = Vec::new();
            rx2.read_to_end(&mut data).await?;
            Ok::<_, IrohConnError>(data)
        };
        let (d1, d2) = tokio::try_join!(read1, read2)?;
        Ok((d1, d2))
    }

    /// 并发 channel 测试：同一条连接上开两条 channel，各自读写且互不串流。
    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_channels_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    {
        let free_port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0")?;
            l.local_addr()?.port()
        };
        let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, free_port);

        let server_endpoint = Endpoint::builder(N0)
            .alpns(vec![ALPN.to_vec()])
            .clear_ip_transports()
            .clear_relay_transports()
            .bind_addr(server_addr)?
            .bind()
            .await?;
        let server_id = server_endpoint.id();

        let server_task = tokio::spawn(server_read_two_channels(server_endpoint));

        let client_endpoint = Endpoint::builder(N0)
            .alpns(vec![ALPN.to_vec()])
            .clear_relay_transports()
            .bind()
            .await?;
        let server_ep_addr = EndpointAddr::from_parts(
            server_id,
            vec![TransportAddr::Ip(std::net::SocketAddr::V4(server_addr))],
        );
        let conn = IrohConnection::connect_by_addr(client_endpoint, server_ep_addr, ALPN).await?;

        let mut ch1 = conn
            .open_channel_async()
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await?;
        let mut ch2 = conn
            .open_channel_async()
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await?;

        let (mut tx1, _rx1) = ch1.split();
        let (mut tx2, _rx2) = ch2.split();

        let payload1: Vec<u8> = (0..50_000u32).map(|i| (i % 13) as u8).collect();
        let payload2: Vec<u8> = (0..80_000u32).map(|i| (i % 7) as u8).collect();

        let write1 = tx1.write_all(&payload1);
        let write2 = tx2.write_all(&payload2);
        tokio::try_join!(write1, write2)?;

        // 关闭两个发送端，让服务端两个 channel 都读到 EOF。
        tx1.close();
        tx2.close();
        drop(tx1);
        drop(tx2);

        let (got1, got2) = server_task.await??;
        assert_eq!(got1, payload1);
        assert_eq!(got2, payload2);

        drop(ch1);
        drop(ch2);
        Ok(())
    }
}
