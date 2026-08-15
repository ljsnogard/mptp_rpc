#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(async_fn_traits)]

//! MsgPk RPC over IROH networks.
//!
//! 本 crate 为 `msgpk_rpc_core` 提供 iroh 传输实现：
//! - [`IrohConnection`]：实现 [`TrMuxConn`]，封装一个已建立的 iroh 连接
//!   （连同其 `Endpoint`），支持两种互联方式——
//!   1. 借助 relay / 地址发现，仅凭服务端 `EndpointId` 连接；
//!   2. 不借助任何 relay，直接连接指定 IP 地址上的节点。
//!   两种方式共用同一套底层（`Endpoint` + `Connection` + `open_bi`），
//!   只是建立连接时给出的对端地址规格不同（`EndpointId` 或 `EndpointAddr`）。
//! - [`IrohChannel`]：实现 [`TrChannel`]，封装 iroh 的双向 QUIC stream。
//!   读写两端分别通过后台泵任务把异步流桥接成 abs_buff 的
//!   [`TrBuffTryRead`]/[`TrBuffTryWrite`] 分段语义（段类型来自 `segm_buff`）。

use std::{
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    slice,
    task::{Context, Poll},
};

use abs_buff::{
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr::SomeOf},
    Demand, TrBuffRead, TrBuffTryRead, TrBuffWrite, TrBuffTryWrite,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use iroh::endpoint::{Connection, RecvStream, SendStream};
use iroh::{Endpoint, EndpointAddr, EndpointId};
use msgpk_rpc_core::conn::{TrChannel, TrMuxConn};
use segm_buff::{SegmMut, SegmRef, TrReclaim};
use tokio::sync::mpsc;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 错误类型
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `IrohConnection` 与 `IrohChannel` 共用的错误类型。
#[derive(Debug, thiserror::Error)]
pub enum IrohConnError {
    #[error("connect to remote failed: {0}")]
    Connect(String),

    #[error("accept incoming connection failed: {0}")]
    Accept(String),

    #[error("endpoint closed")]
    EndpointClosed,

    #[error("open bidirectional stream failed: {0}")]
    OpenStream(String),

    #[error("stream io failed: {0}")]
    StreamIo(String),
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 常量
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读泵每次从 iroh 接收流读取的块大小。
const READ_CHUNK: usize = 8 * 1024;

/// 写侧单次借出的最大段长度。
///
/// 意图：`Demand` 可能是无上界的（`at_least`），直接按其 `len()` 扩容
/// 可能撑爆内存；这里封顶，让编码器分多次写入。
const MAX_WRITE_CHUNK: usize = 64 * 1024;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 消费回收（reclaim）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读取侧的消费回收：把被消费的字节数累加到本地读缓冲的游标上。
///
/// 意图：`SegmRef` 被消费者部分取走时，段的 drop 会以“容量即产量”的
/// 语义回调这里；累加的结果用于 `IrohRecvHalf` 在下次借出前的压缩，
/// 保证同一批字节不会被重复读出。
pub struct IrohReadReclaim<'a>(&'a mut usize);

impl TrReclaim for IrohReadReclaim<'_> {
    #[inline]
    fn reclaim(&mut self, amount: usize) {
        *self.0 += amount;
    }
}

/// 写入侧的消费回收：推进写缓冲游标，并把被消费的字节投递给写泵。
///
/// 意图：生产者把字节写进借出的 `SegmMut` 后，段 drop 时回调这里。
/// 本回收把该段对应的原始字节复制进无界通道，由写泵异步写入 iroh
/// 发送流——这保证了“最后一段”的字节也会在段 drop 时被投递，不会
/// 残留在缓冲里等下一次 `write_async`（那样客户端会永远等不到回复）。
///
/// # Safety
///
/// `data_ptr` 指向 `IrohSendHalf::buf_` 的起始地址，与借出的段指向
/// 同一块内存：段存活期间 `buf_` 被段独占借用（不会重新分配），
/// 而本回收只在段的 drop（即借用行将结束）时读取该指针，因此
/// 指针始终有效且不与活动借用冲突。
pub struct IrohWriteReclaim<'a> {
    offset_: &'a mut usize,
    data_ptr_: *mut u8,
    chan_: mpsc::UnboundedSender<Vec<u8>>,
}

impl TrReclaim for IrohWriteReclaim<'_> {
    #[inline]
    fn reclaim(&mut self, amount: usize) {
        *self.offset_ += amount;
        if amount == 0 {
            return;
        }
        // SAFETY: 见类型文档；amount 即该段被完整填写的字节数。
        let bytes = unsafe { slice::from_raw_parts(self.data_ptr_, amount) };
        let _ = self.chan_.send(bytes.to_vec());
    }
}

/// 读取侧借出的段类型：`&'f [u8]` 视图 + 消费回收。
pub type IrohReadSegm<'f> = SegmRef<&'f [u8], IrohReadReclaim<'f>>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读写半通道
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读泵投递给接收半通道的事件。
pub enum ReadChunk {
    /// 一段从 iroh 接收流读到的数据。
    Data(Vec<u8>),

    /// 读泵遇到的流错误（此后不再投递数据）。
    Err(IrohConnError),
}

/// 发送半通道：本地写缓冲 + 通向写泵的无界通道。
pub struct IrohSendHalf {
    chan_: mpsc::UnboundedSender<Vec<u8>>,
    buf_: Vec<u8>,
    offset_: usize,
}

/// 接收半通道：本地读缓冲 + 来自读泵的无界通道。
pub struct IrohRecvHalf {
    chan_: mpsc::UnboundedReceiver<ReadChunk>,
    buf_: Vec<u8>,
    offset_: usize,
    eof_: bool,
    err_: Option<IrohConnError>,
}

impl IrohSendHalf {
    /// 压缩写缓冲并借出一段“恰好等于 demand 长度”的可写段。
    ///
    /// 意图：把段的大小与消费方的 demand 精确对齐，保证 rpc_core 的
    /// 编码器（`buff_segm_mut_write_cloned`）会把段完整填满，从而使
    /// 回收回调收到的 `amount` 就是实际写入的字节数。
    fn build_write_segm<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SegmMut<&'a mut [MaybeUninit<u8>], IrohWriteReclaim<'a>> {
        // 上一段被消费的字节已由回收回调投递给写泵，这里只需压缩游标
        if self.offset_ > 0 {
            if self.offset_ == self.buf_.len() {
                self.buf_.clear();
            } else {
                self.buf_.drain(..self.offset_);
            }
            self.offset_ = 0;
        }
        // 本次借出长度：与 demand 对齐，并封顶
        let need = demand.len().min(MAX_WRITE_CHUNK);
        if self.buf_.len() < need {
            self.buf_.resize(need, 0u8);
        }
        // SAFETY: MaybeUninit<u8> 与 u8 布局一致；段存活期间 buf_ 被独占借用
        let uninit: &'a mut [MaybeUninit<u8>] = unsafe {
            slice::from_raw_parts_mut(
                self.buf_.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                self.buf_.len(),
            )
        };
        // 回收指针从段本身派生，保证与段共享同一内存来源
        let data_ptr: *mut u8 = uninit.as_mut_ptr().cast::<u8>();
        let reclaim = IrohWriteReclaim {
            offset_: &mut self.offset_,
            data_ptr_: data_ptr,
            chan_: self.chan_.clone(),
        };
        SegmMut::new(&mut uninit[..need], Option::Some(reclaim))
    }
}

impl IrohRecvHalf {
    /// 压缩读缓冲：丢弃已被消费的字节。
    fn compact(&mut self) {
        if self.offset_ > 0 {
            if self.offset_ == self.buf_.len() {
                self.buf_.clear();
            } else {
                self.buf_.drain(..self.offset_);
            }
            self.offset_ = 0;
        }
    }

    /// 借出一段“恰好等于 demand 与可用量交集长度”的只读段。
    ///
    /// 意图：与写侧同理——把段大小精确对齐到消费方的 demand，
    /// 保证消费者（`buff_segm_ref_read`）会把段完整取走，从而
    /// 回收回调的 `amount` 恰好等于真实消费的字节数，不会丢数据。
    fn build_read_segm<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SegmRef<&'a [u8], IrohReadReclaim<'a>> {
        let available = self.buf_.len() - self.offset_;
        let need = if available > 0 {
            demand
                .compromise(&Demand::less_than(available))
                .map(|d| d.len())
                .unwrap_or(0)
        } else {
            0
        };
        let slice = &self.buf_[self.offset_..self.offset_ + need];
        let reclaim = IrohReadReclaim(&mut self.offset_);
        SegmRef::new(slice, Option::Some(reclaim))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// IrohChannel
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 封装 iroh 双向 QUIC stream 的通道。
///
/// 创建时把 iroh 的 `SendStream` / `RecvStream` 分别交给后台写泵 / 读泵
/// 任务，本地只保留无界通道与读写缓冲；`split` 出的两个半通道再借出
/// 由 `segm_buff` 提供的分段视图，实现 abs_buff 的同步读写语义。
pub struct IrohChannel {
    send_half_: IrohSendHalf,
    recv_half_: IrohRecvHalf,
}

impl IrohChannel {
    /// 由一对 iroh 双向流的收发端构造通道，并启动读泵与写泵。
    pub(crate) fn new(mut send: SendStream, mut recv: RecvStream) -> Self {
        let (write_tx, mut write_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (read_tx, read_rx) = mpsc::unbounded_channel::<ReadChunk>();

        // 写泵：把通道里的字节块依次写入 iroh 发送流。
        // 通道关闭（发送半通道被丢弃）后泵退出，SendStream 随之 drop，
        // noq 的 Drop 实现会自动 finish 流（半关闭），对端读到 EOF。
        tokio::spawn(async move {
            while let Option::Some(chunk) = write_rx.recv().await {
                if send.write_all(&chunk).await.is_err() {
                    break;
                }
            }
        });

        // 读泵：持续从 iroh 接收流读入字节块并投递给接收半通道。
        // Ok(None) 表示对端 finish（流结束）；错误则投递错误事件后退出。
        tokio::spawn(async move {
            let mut buf = vec![0u8; READ_CHUNK];
            loop {
                match recv.read(&mut buf).await {
                    Result::Ok(Option::Some(n)) => {
                        if read_tx.send(ReadChunk::Data(buf[..n].to_vec())).is_err() {
                            break;
                        }
                    }
                    Result::Ok(Option::None) => break,
                    Result::Err(e) => {
                        let _ = read_tx.send(ReadChunk::Err(IrohConnError::StreamIo(e.to_string())));
                        break;
                    }
                }
            }
        });

        IrohChannel {
            send_half_: IrohSendHalf {
                chan_: write_tx,
                buf_: Vec::new(),
                offset_: 0,
            },
            recv_half_: IrohRecvHalf {
                chan_: read_rx,
                buf_: Vec::new(),
                offset_: 0,
                eof_: false,
                err_: Option::None,
            },
        }
    }
}

impl TrChannel for IrohChannel {
    type Tx<'f> = IrohSend<'f> where Self: 'f;
    type Rx<'f> = IrohRecv<'f> where Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        (IrohSend(&mut self.send_half_), IrohRecv(&mut self.recv_half_))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送半通道适配器（TrBuffWrite / TrBuffTryWrite）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 发送半通道的 abs_buff 适配器：对 [`IrohSendHalf`] 的可变借用。
pub struct IrohSend<'f>(&'f mut IrohSendHalf);

impl<'h> TrBuffWrite for IrohSend<'h> {
    type SegmMut<'a> = SegmMut<&'a mut [MaybeUninit<u8>], IrohWriteReclaim<'a>> where Self: 'a;
    type Err = IrohConnError;

    /// 无界通道 + 按需增长的本地缓冲 → 永远不会阻塞写入。
    #[inline]
    fn is_blocked(&self) -> bool {
        false
    }

    fn write_async<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = SomeOf<Self::SegmMut<'a>, Self::Err>> {
        // 压缩与借出都是同步的（真正的异步写入由写泵完成），
        // 因此用 make_ready 包装成立即就绪的 TrMayCancel 未来
        let segm = self.0.build_write_segm(demand);
        abs_cancel::futures_util::make_ready(SomeOf::new_left(segm))
    }
}

impl<'h> TrBuffTryWrite for IrohSend<'h> {
    #[inline]
    fn try_write<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'a>, Self::Err> {
        let segm = self.0.build_write_segm(demand);
        SomeOf::new_left(segm)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 接收半通道适配器（TrBuffRead / TrBuffTryRead）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 接收半通道的 abs_buff 适配器：对 [`IrohRecvHalf`] 的可变借用。
pub struct IrohRecv<'f>(&'f mut IrohRecvHalf);

/// `TrBuffRead::read_async` 返回的异步读未来（手工实现）。
///
/// 意图：`gen_may_cancel_future` 宏无法表达“输出类型携带借用生命周期
/// `'f`”的签名（其生成的 `may_cancel_with` 生命周期推导会失败），这里
/// 用 BoxFuture 手工实现。`may_cancel_with` 忽略取消令牌——数据到达由
/// tokio 调度，令牌只决定调用方是否继续等待。
pub struct IrohRecvReadAsync<'f> {
    inner_: Pin<Box<dyn Future<Output = SomeOf<IrohReadSegm<'f>, IrohConnError>> + Send + 'f>>,
}

impl<'f> IrohRecvReadAsync<'f> {
    fn new(recv: &'f mut IrohRecvHalf, demand: &Demand<usize>) -> Self {
        // demand 与 &'f mut self 的借用生命周期相互独立（见 trait 签名），
        // 这里按值克隆一份，让 BoxFuture 只依赖 'f 一个生命周期
        let demand = demand.clone();
        let inner = async move {
            // 压缩已消费字节
            recv.compact();
            // 缓冲为空 → 异步等待读泵投递
            if recv.buf_.is_empty() {
                loop {
                    match recv.chan_.recv().await {
                        Option::Some(ReadChunk::Data(bytes)) => {
                            recv.buf_.extend_from_slice(&bytes);
                            break;
                        }
                        Option::Some(ReadChunk::Err(e)) => {
                            recv.err_ = Option::Some(e);
                            break;
                        }
                        Option::None => {
                            recv.eof_ = true;
                            break;
                        }
                    }
                }
            }
            if let Option::Some(e) = recv.err_.take() {
                return SomeOf::new_right(e);
            }
            SomeOf::new_left(recv.build_read_segm(&demand))
        };
        IrohRecvReadAsync { inner_: Box::pin(inner) }
    }
}

impl<'f> Future for IrohRecvReadAsync<'f> {
    type Output = SomeOf<IrohReadSegm<'f>, IrohConnError>;

    #[inline]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner_.as_mut().poll(cx)
    }
}

// 注意：`IntoFuture` 由 core 对 `Future` 的 blanket impl 提供，无需手动实现。

impl<'f> TrMayCancel<'f> for IrohRecvReadAsync<'f> {
    type MayCancelOutput = SomeOf<IrohReadSegm<'f>, IrohConnError>;

    #[inline]
    fn may_cancel_with<'c, C: TrCancellationToken>(
        self,
        _cancel: &'c mut C,
    ) -> impl IntoFuture<Output = Self::MayCancelOutput>
    where
        Self: 'c,
    {
        self
    }
}

impl<'h> TrBuffRead for IrohRecv<'h> {
    type SegmRef<'a> = IrohReadSegm<'a> where Self: 'a;
    type Err = IrohConnError;

    /// 读泵已结束（流 EOF / 错误）且本地缓冲已全部消费 → 流枯竭。
    #[inline]
    fn is_drained(&self) -> bool {
        self.0.eof_ && self.0.buf_.len() == self.0.offset_
    }

    fn read_async<'f>(
        &'f mut self,
        demand: &Demand<usize>,
    ) -> impl TrMayCancel<'f, MayCancelOutput = SomeOf<Self::SegmRef<'f>, Self::Err>> {
        // 手工实现的 BoxFuture 包装：真正 await 读泵通道
        IrohRecvReadAsync::new(&mut self.0, demand)
    }
}

impl<'h> TrBuffTryRead for IrohRecv<'h> {
    fn try_read<'a>(
        &'a mut self,
        demand: &Demand<usize>,
    ) -> SomeOf<IrohReadSegm<'a>, IrohConnError> {
        let half = &mut self.0;
        // 压缩已消费字节
        half.compact();
        // 尽量把通道里已有的数据块取进本地缓冲
        while let Result::Ok(chunk) = half.chan_.try_recv() {
            match chunk {
                ReadChunk::Data(bytes) => half.buf_.extend_from_slice(&bytes),
                ReadChunk::Err(e) => half.err_ = Option::Some(e),
            }
        }
        // 缓冲为空且尚未结束 → 阻塞等待读泵投递。
        // 注意：`blocking_recv` 不能在异步 worker 线程上直接调用（会 panic），
        // 这里用 `block_in_place` 把当前任务让出 worker、到阻塞池等待；
        // 读泵任务在其它 worker 上运行，因此要求多线程运行时
        // （rpc_iroh 的 tokio 即 rt-multi-thread；current_thread 运行时会 panic）。
        if half.buf_.is_empty() && half.err_.is_none() && !half.eof_ {
            loop {
                match tokio::task::block_in_place(|| half.chan_.blocking_recv()) {
                    Option::Some(ReadChunk::Data(bytes)) => {
                        half.buf_.extend_from_slice(&bytes);
                        break;
                    }
                    Option::Some(ReadChunk::Err(e)) => {
                        half.err_ = Option::Some(e);
                        break;
                    }
                    Option::None => {
                        half.eof_ = true;
                        break;
                    }
                }
            }
        }
        if let Option::Some(e) = half.err_.take() {
            return SomeOf::new_right(e);
        }
        SomeOf::new_left(half.build_read_segm(demand))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// IrohConnection
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 已建立的 iroh 连接（连同其 `Endpoint`），实现 [`TrMuxConn`]。
///
/// 两种互联方式（relay 直连/直连 IP）共用同一套底层：
/// 同一个 `Endpoint`、同一个已建立的 `Connection`、同一个 `open_bi`；
/// 差异只在于建立连接时传入的对端地址规格。
pub struct IrohConnection {
    endpoint_: Endpoint,
    conn_: Connection,
    local_id_: EndpointId,
    remote_id_: EndpointId,
}

impl IrohConnection {
    /// 方式一（借助 relay / 地址发现）：仅凭服务端 `EndpointId` 连接任意节点。
    ///
    /// `endpoint` 需要用 `iroh::endpoint::presets::N0` 等默认配置绑定
    /// （包含 relay 与地址发现能力），服务端需在相同 ALPN 上监听。
    pub async fn connect_by_id(
        endpoint: Endpoint,
        server_id: EndpointId,
        alpn: &[u8],
    ) -> Result<Self, IrohConnError> {
        let conn = endpoint
            .connect(server_id, alpn)
            .await
            .map_err(|e| IrohConnError::Connect(e.to_string()))?;
        Result::Ok(Self::from_parts(endpoint, conn))
    }

    /// 方式二（不借助任何 relay）：直接连接指定 IP 地址上的节点。
    ///
    /// `server_addr` 用 `EndpointAddr::from_parts(id, [TransportAddr::Ip(addr)])`
    /// 构造；对端 endpoint 需用 `clear_relay_transports()` 等配置只监听直连。
    pub async fn connect_by_addr(
        endpoint: Endpoint,
        server_addr: EndpointAddr,
        alpn: &[u8],
    ) -> Result<Self, IrohConnError> {
        let conn = endpoint
            .connect(server_addr, alpn)
            .await
            .map_err(|e| IrohConnError::Connect(e.to_string()))?;
        Result::Ok(Self::from_parts(endpoint, conn))
    }

    /// 服务端侧：接受一条入站连接（等价于 demo 里的 `server` / `local-server`）。
    pub async fn accept(endpoint: Endpoint) -> Result<Self, IrohConnError> {
        let incoming = endpoint
            .accept()
            .await
            .ok_or(IrohConnError::EndpointClosed)?;
        let conn = incoming
            .await
            .map_err(|e| IrohConnError::Accept(e.to_string()))?;
        Result::Ok(Self::from_parts(endpoint, conn))
    }

    /// 返回底层 `Endpoint` 的引用。
    ///
    /// 意图：`IrohConnection` 持有 `Endpoint` 是为了保证底层套接字/
    /// relay/地址发现资源在连接存续期间不被回收。
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint_
    }

    /// 两种连接方式共用的装配逻辑：包装 Endpoint 与已建立的 Connection。
    fn from_parts(endpoint: Endpoint, conn: Connection) -> Self {
        let local_id_ = endpoint.id();
        let remote_id_ = conn.remote_id();
        IrohConnection {
            endpoint_: endpoint,
            conn_: conn,
            local_id_,
            remote_id_,
        }
    }
}

/// 打开一条双向流（生成宏包装的可取消未来）。
#[gen_may_cancel_future(IrohOpenChannel)]
async fn iroh_open_channel_async_<'f, C>(
    conn: &'f IrohConnection,
    _cancel: &'f mut C,
) -> Result<IrohChannel, IrohConnError>
where
    C: TrCancellationToken,
{
    let (send, recv) = conn.conn_.open_bi().await
        .map_err(|e| IrohConnError::OpenStream(e.to_string()))?;
    Result::Ok(IrohChannel::new(send, recv))
}

impl TrMuxConn for IrohConnection {
    type Channel = IrohChannel;
    type Id = EndpointId;
    type Err = IrohConnError;

    #[inline]
    fn local_id(&self) -> Option<&Self::Id> {
        Option::Some(&self.local_id_)
    }

    #[inline]
    fn remote_id(&self) -> Option<&Self::Id> {
        Option::Some(&self.remote_id_)
    }

    fn open_channel_async<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<Self::Channel, Self::Err>> {
        // 构造宏生成的异步结构体；取消令牌在 may_cancel_with 时才注入
        IrohOpenChannelAsync(self)
    }
}

#[cfg(test)]
mod tests_ {
    use std::net::{Ipv4Addr, SocketAddrV4};

    use abs_buff::BuffWriteAsOutput;
    use abs_cancel::NonCancellableToken;
    use iroh::endpoint::presets::N0;
    use iroh::{Endpoint, EndpointAddr, TransportAddr};

    use super::*;

    const ALPN: &[u8] = b"msgpk-rpc-iroh/test/1";

    /// 服务端逻辑：接受连接 → 接受双向流 → 用分段读取语义把数据读回。
    async fn server_accept_and_read(
        server_endpoint: Endpoint,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = IrohConnection::accept(server_endpoint).await?;
        let (send, recv) = conn.conn_.accept_bi().await?;
        let mut channel = IrohChannel::new(send, recv);
        let (_tx, mut rx) = channel.split();
        // 直接按分段语义读取（绕开 abs_buff 上游 AsStdRead 的已知 bug）
        let mut out = Vec::new();
        loop {
            if rx.is_drained() {
                break;
            }
            let demand = Demand::less_than(64 * 1024);
            let res = rx.try_read(&demand);
            if let Option::Some(segm) = res.as_ref().pick_left() {
                if let Option::Some(slice) = segm.iter_slices() {
                    out.extend_from_slice(slice);
                }
                // segm 在此处 drop → 消费回收推进游标
            }
            if let Option::Some(e) = res.pick_right() {
                return Result::Err(Box::new(e));
            }
        }
        Result::Ok(out)
    }

    /// 端到端直连测试（方式二：不借助 relay，对应 demo 的 local-server/local-client）。
    ///
    /// 客户端通过 `IrohConnection::connect_by_addr` 连接服务端，打开 `IrohChannel`，
    /// 用 abs_buff 的分段写入语义把一段数据写进双向流；服务端接受连接与双向流，
    /// 用分段读取语义把数据读回并校验。这覆盖了读泵/写泵、段借出/回收、
    /// 流结束（finish）的全链路。
    #[tokio::test(flavor = "multi_thread")]
    async fn direct_connect_roundtrip() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 找一个空闲端口，服务端只监听 localhost
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

        // 服务端任务：接受连接 → 接受双向流 → 用分段读取语义读回数据
        let server_task = tokio::spawn(server_accept_and_read(server_endpoint));

        // 客户端：直连（方式二）
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

        // 打开通道，写入一段确定性的数据
        let mut channel = conn
            .open_channel_async()
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await?;
        let (mut tx, _rx) = channel.split();
        let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
        let mut output = BuffWriteAsOutput::<&mut IrohSend<'_>, IrohSend<'_>, u8>::new(&mut tx);
        let n = output
            .write_cloned_async(&payload)
            .may_cancel_with(NonCancellableToken::shared_mut())
            .await
            .as_ref()
            .pick_left()
            .copied()
            .expect("write should succeed");
        assert_eq!(n, payload.len());

        // 释放借用后丢弃通道：发送半通道 drop → 写泵通道关闭 → 泵退出 →
        // SendStream drop 自动 finish，服务端的 read_to_end 得以返回
        drop(tx);
        drop(_rx);
        drop(channel);

        // 等待服务端读回并校验
        let got = server_task.await??;
        assert_eq!(got, payload);
        Ok(())
    }
}
