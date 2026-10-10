//! 分块传输：把一段「长度事先不知道」的内容切成一个个体自带长度的块。
//!
//! 这是 [`TrBuffRead`] / [`TrBuffWrite`] 之上的帧化层：写侧把**一段内存**封装成
//! 「2 字节大端长度 + 内容」，读侧反过来按块交出字节，并且让借出的段**不跨越块边界**。
//!
//! # 帧格式
//!
//! ```text
//! chunked-body = *chunk last-chunk
//!
//! chunk      = chunk-size(2 字节大端 u16) chunk-data
//! last-chunk = 0x0000
//! ```
//!
//! - 长度**定长 2 字节**：写侧可以精确预留、读侧可以一次读满，两端都不必跑一遍
//!   `serde` 才能知道块有多长；
//! - 单块最大 65535 字节（`u16` 的上限）；超过这个数的一段内容会被自动切成多块；
//! - 结束是**显式的**：长度为 `0` 的块就是「体到此为止」。没有它，接收方无法知道
//!   分块体何时结束（总长没有写在报文头里）。
//!
//! # 与报文头的关系
//!
//! 「这段内容是分块给出的」由报文标准头 `Body_Transfer: Chunked` 声明；接收方据此
//! 选择用本模块的读视图解读，而不是靠猜（见 `messaging::body`）。
//!
//! # Examples
//!
//! ```
//! use mptp_core::messaging::chunked::ChunkedWrite;
//!
//! # async fn demo() {
//! let mut storage = [0u8; 64];
//! let mut sink: &mut [u8] = &mut storage;
//! let mut writer = ChunkedWrite::new(&mut sink);
//! // 写一块 "abc"：线上是 00 03 61 62 63。
//! # }
//! ```

use core::error::Error;
use std::io::Write;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffWrite, gen_may_cancel_future,
    error::{ReadErrTag, TrTaggedError},
    x_deps::{abs_cancel, anylr},
};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

use super::{
    io_::{MessageIoError, WaitingTx, race_cancel_},
    limit::{
        LimitedRefSegm, narrow_demand_, read_exact_into_async_, split_some_of_, total_of_,
    },
};

/// 单个块能承载的最大字节数：`u16` 的上限。
///
/// 长度前缀是 2 字节，因此单块内容不可能超过这个数；写侧遇到更长的内容会自动切块。
pub const K_MAX_CHUNK_PAYLOAD: usize = u16::MAX as usize;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 写侧
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 分块写的入口：把一个缓冲里的内容按块写出去。
///
/// 它**不缓冲任何字节**：每一块都是「长度前缀 + 内容」两次直写，长度前缀取的就是这一
/// 块内容的实际长度。因此它天然支持「内容边产生边发」——只要内容已经落在某块内存里。
///
/// # Examples
///
/// ```
/// use mptp_core::messaging::chunked::ChunkedWrite;
///
/// # async fn demo() {
/// let mut storage = [0u8; 64];
/// let mut sink: &mut [u8] = &mut storage;
/// let mut writer = ChunkedWrite::new(&mut sink);
/// # }
/// ```
pub struct ChunkedWrite<'a, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    inner_: &'a mut TyTx,
}

impl<'a, TyTx> ChunkedWrite<'a, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    /// 包住一个写缓冲。
    pub const fn new(inner: &'a mut TyTx) -> Self {
        ChunkedWrite { inner_: inner }
    }

    /// 写出一块内容，返回其中数据的字节数（不含 2 字节长度前缀）。
    ///
    /// - 空切片写出的就是**终止块**（长度 0）；
    /// - 超过 [`K_MAX_CHUNK_PAYLOAD`] 的内容会被自动切成多块，返回值仍是内容的总字节数；
    /// - 返回的 future 按本 crate 的惯例**不含**取消令牌，由调用方决定
    ///   `may_cancel_with(token)` 之后再 `await`。
    pub fn write_chunk_async<'f>(&'f mut self, chunk: &'f [u8]) -> ChunkedWriteAsync<'f, 'f, TyTx> {
        ChunkedWriteAsync::new(&mut *self.inner_, chunk)
    }

    /// 写出终止块（长度 0），宣告分块体到此结束。
    pub fn finish_async<'f>(&'f mut self) -> ChunkedWriteAsync<'f, 'f, TyTx> {
        ChunkedWriteAsync::new(&mut *self.inner_, &[])
    }
}

/// [`ChunkedWrite::write_chunk_async`] 的 step 函数。
///
/// 写路径与报文前缀走同一套适配：`AsStdWrite` 把每一段字节直接落进环的可用段，环满时
/// 由 `WaitingTx` 让它等对端腾出空间，中间没有第二块内存。
#[gen_may_cancel_future(ChunkedWrite, pub, new(pub(crate)))]
async fn chunked_write_async_<'f, TyTx, TyTok>(
    inner: &'f mut TyTx,
    chunk: &'f [u8],
    cancel: TyTok,
) -> Result<usize, MessageIoError>
where
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    let mut waiting = WaitingTx::new_(inner);
    let mut write = AsStdWrite::new(&mut waiting, cancel);

    if chunk.is_empty() {
        // 空内容就是终止块：只有长度前缀，没有数据。
        write
            .write_all(0u16.to_be_bytes().as_slice())
            .map_err(|err| MessageIoError::Io(err.to_string()))?;
        write
            .flush()
            .map_err(|err| MessageIoError::Io(err.to_string()))?;
        return Ok(0usize);
    }

    let mut written = 0usize;
    for part in chunk.chunks(K_MAX_CHUNK_PAYLOAD) {
        let len = u16::try_from(part.len())
            .map_err(|err| MessageIoError::Encode(format!("块长度超出 2 字节表达：{err}")))?;
        write
            .write_all(len.to_be_bytes().as_slice())
            .map_err(|err| MessageIoError::Io(err.to_string()))?;
        write
            .write_all(part)
            .map_err(|err| MessageIoError::Io(err.to_string()))?;
        written += part.len();
    }
    write
        .flush()
        .map_err(|err| MessageIoError::Io(err.to_string()))?;
    Ok(written)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读侧
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 分块读的错误。
#[derive(Clone, Debug)]
pub enum ChunkedReadError<TyErr> {
    /// 已经读到终止块（长度为 0 的块），此后不再有体字节。
    ///
    /// 错误标签是 `Closing`：对 `AsStdRead` 而言这就是 EOF。
    Ended,

    /// 分块帧不完整或格式非法（例如长度前缀只到了一半，对端就结束了）。
    Malformed(String),

    /// 当前块装不下调用方给出的需求。
    Unsatisfied,

    /// 同步借段正好卡在块边界上：跨块需要等待数据，请走 `read_async`。
    WouldBlock,

    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    Cancelled,

    /// 底层读缓冲自身报出的错误。
    Inner(TyErr),
}

impl<TyErr> core::fmt::Display for ChunkedReadError<TyErr>
where
    TyErr: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ChunkedReadError::Ended => f.write_str("分块体：已读到终止块"),
            ChunkedReadError::Malformed(detail) => write!(f, "分块体：帧不完整或非法（{detail}）"),
            ChunkedReadError::Unsatisfied => f.write_str("分块体：当前块装不下所需数量"),
            ChunkedReadError::WouldBlock => f.write_str("分块体：跨块读取必须走异步路径"),
            ChunkedReadError::Cancelled => f.write_str("分块体：等待期间被取消"),
            ChunkedReadError::Inner(err) => write!(f, "分块体：底层缓冲报错（{err}）"),
        }
    }
}

// 不实现 `source()`：理由同 `LimitReadError`——避免 `'static` 约束沿 trait 传染。
impl<TyErr> Error for ChunkedReadError<TyErr> where TyErr: Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for ChunkedReadError<TyErr>
where
    TyErr: Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            // 「读完终止块」就是这条体的 EOF。
            ChunkedReadError::Ended => ReadErrTag::Closing,
            ChunkedReadError::Malformed(_) => ReadErrTag::Unknown,
            ChunkedReadError::Unsatisfied => ReadErrTag::Unsatisfied,
            ChunkedReadError::WouldBlock => ReadErrTag::Drained,
            ChunkedReadError::Cancelled => ReadErrTag::Cancelled,
            ChunkedReadError::Inner(err) => err.err_tag(),
        }
    }
}

/// 分块读的帧化视图：借出的段**不会跨越块边界**。
///
/// 对上层来说它就是一个普通的 [`TrBuffRead`]：`AsStdRead` / `rmp_serde` 都能直接吃它，
/// 读到终止块时表现为 EOF。块边界对解码器完全透明。
///
/// # 同步 `try_read` 的边界
///
/// 读长度前缀需要等待数据，因此同步的 [`TrBuffTryRead::try_read`] 只在**当前块尚未读
/// 完**时借得出段；正好卡在块边界时返回 [`ChunkedReadError::WouldBlock`]，请改用
/// [`TrBuffRead::read_async`]。
pub struct ChunkedRead<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    inner_: &'a mut TyRx,

    /// 当前块里还剩多少字节没被借出。
    chunk_left_: usize,

    /// 是否已经读到终止块。
    ended_: bool,

    /// 本次借段交给底层的需求；理由见 `limit::LimitedRead` 的文档。
    demand_: Demand<usize>,
}

impl<'a, TyRx> ChunkedRead<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    /// 用一个分块读视图包住底层缓冲。
    pub const fn new(inner: &'a mut TyRx) -> Self {
        ChunkedRead {
            inner_: inner,
            chunk_left_: 0usize,
            ended_: false,
            demand_: Demand::exactly(0usize),
        }
    }

    /// 当前块还剩多少字节可以借出。
    pub const fn chunk_left(&self) -> usize {
        self.chunk_left_
    }

    /// 是否已经读到终止块。
    pub const fn is_ended(&self) -> bool {
        self.ended_
    }

    /// 取回底层缓冲的引用。
    pub const fn inner(&self) -> &TyRx {
        self.inner_
    }
}

/// 分块读的同步借段步骤；被 [`ChunkedRead`] 与报文体视图共用。
///
/// 同步路径只在**当前块尚未读完**时借得出段：跨块要先读长度前缀，那是一个等待点。
pub(super) fn chunked_try_read_<'f, TyRx>(
    inner: &'f mut TyRx,
    chunk_left: &'f mut usize,
    ended: bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, ChunkedReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
{
    if ended {
        return SomeOf::new_right(ChunkedReadError::Ended);
    }
    if *chunk_left == 0usize {
        return SomeOf::new_right(ChunkedReadError::WouldBlock);
    }
    let narrowed = match narrow_demand_(demand, *chunk_left) {
        Option::Some(narrowed) => narrowed,
        Option::None => return SomeOf::new_right(ChunkedReadError::Unsatisfied),
    };
    *demand_slot = narrowed;
    match split_some_of_(inner.try_read(&*demand_slot)) {
        Result::Ok(segm) => {
            let total = total_of_(&segm);
            SomeOf::new_left(LimitedRefSegm::new_(segm, chunk_left, total))
        }
        Result::Err(err) => SomeOf::new_right(ChunkedReadError::Inner(err)),
    }
}

impl<TyRx> TrBuffTryRead<u8> for ChunkedRead<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type SegmRef<'f>
        = LimitedRefSegm<'f, TyRx::SegmRef<'f>>
    where
        Self: 'f;

    type Err = ChunkedReadError<TyRx::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let ChunkedRead {
            inner_,
            chunk_left_,
            ended_,
            demand_,
        } = self;
        chunked_try_read_(&mut **inner_, chunk_left_, *ended_, demand_, demand)
    }
}

impl<TyRx> TrBuffRead<u8> for ChunkedRead<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = ChunkedReadAsync<'f, 'f, TyRx>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        ChunkedReadAsync::new(
            &mut *self.inner_,
            &mut self.chunk_left_,
            &mut self.ended_,
            &mut self.demand_,
            demand,
        )
    }
}

/// 分块读的核心步骤：必要时先读下一个块的长度前缀，再按「块内剩余」借出段。
///
/// 它被 [`ChunkedRead`] 与报文体视图（`BodyReader`）共用。
pub(super) async fn chunked_read_step_<'f, TyRx, TyTok>(
    inner: &'f mut TyRx,
    chunk_left: &'f mut usize,
    ended: &'f mut bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, ChunkedReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    loop {
        if *ended {
            return SomeOf::new_right(ChunkedReadError::Ended);
        }
        if *chunk_left > 0usize {
            let narrowed = match narrow_demand_(demand, *chunk_left) {
                Option::Some(narrowed) => narrowed,
                Option::None => return SomeOf::new_right(ChunkedReadError::Unsatisfied),
            };
            *demand_slot = narrowed;
            let read_fut = inner.read_async(&*demand_slot).into_future();
            let Option::Some(res) = race_cancel_(cancel.child_token(), read_fut).await else {
                return SomeOf::new_right(ChunkedReadError::Cancelled);
            };
            return match split_some_of_(res) {
                Result::Ok(segm) => {
                    let total = total_of_(&segm);
                    SomeOf::new_left(LimitedRefSegm::new_(segm, chunk_left, total))
                }
                Result::Err(err) => SomeOf::new_right(ChunkedReadError::Inner(err)),
            };
        }

        // 当前块已经读完：读下一个块的长度前缀（2 字节大端）。
        let mut head = [0u8; 2usize];
        let head_fut = read_exact_into_async_(inner, &mut head, cancel.child_token());
        match race_cancel_(cancel.child_token(), head_fut).await {
            Option::None => return SomeOf::new_right(ChunkedReadError::Cancelled),
            Option::Some(Result::Err(err)) => {
                return SomeOf::new_right(ChunkedReadError::Malformed(err.to_string()));
            }
            Option::Some(Result::Ok(())) => {}
        }
        let len = usize::from(u16::from_be_bytes(head));
        if len == 0usize {
            *ended = true;
            return SomeOf::new_right(ChunkedReadError::Ended);
        }
        *chunk_left = len;
    }
}

/// [`ChunkedRead::read_async`] 的 step 函数。
#[gen_may_cancel_future(ChunkedRead, pub, new(pub(crate)))]
async fn chunked_read_async_<'f, TyRx, TyTok>(
    inner: &'f mut TyRx,
    chunk_left: &'f mut usize,
    ended: &'f mut bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, ChunkedReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    chunked_read_step_(inner, chunk_left, ended, demand_slot, demand, cancel).await
}
