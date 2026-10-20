//! 分块传输：把一段「长度事先不知道」的内容切成一个个体自带长度的块。
//!
//! 这是 [`TrBuffRead`] / [`TrBuffWrite`] 之上的帧化层：
//!
//! - 写侧 [`ChunkedEncode`] 把**读半边**的内容按块搬进**写半边**：每一块的块长在动笔之前
//!   就定下（源现在让得出的量、目标现在装得下的量、单块上限三者取小），于是 2 字节长度
//!   前缀可以先于内容写出去；
//! - 读侧 [`ChunkedReader`] 反过来按块交出字节，并且让借出的段**不跨越块边界**。
//!
//! # 块长为什么必须「动笔之前」定下
//!
//! 段接口的消费量只在段被回收时结算（`TrReclaim`）：「调用方到底写了多少」是内容落进段的
//! 缓冲**之后**才知道的。而分块帧要求长度前缀**先于**内容——等知道实际用量再回填，前缀区
//! 早就被内层段的 offset 越过了，取不回来。所以本层不采用「一次借段 = 一块、回收时补前缀」
//! 那种依赖回填的形状，而是**先算出一块能有多大**，再一次性把前缀与内容写进同一个段。
//!
//! 这也是 [`ChunkedEncode`] 的两端都要报告状态的原因：`TrConsumerState` 说源现在让得出多少、
//! 到没到头，`TrProducerState` 说目标现在装得下多少——「这一块能有多大」只有这两个来源。
//! 它对使用者公开的约束因此也是明确的：搬运器只处理
//! `R: TrBuffRead<u8> + TrConsumerState` 到 `W: TrBuffWrite<u8> + TrProducerState` 的搬运。
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
//! use abs_mptp::messaging::chunked::ChunkedEncode;
//!
//! # async fn demo() {
//! let mut src: &[u8] = b"abc";
//! let mut storage = [0u8; 64];
//! let mut sink: &mut [u8] = &mut storage;
//! let mut encode = ChunkedEncode::new(&mut src, &mut sink);
//! // 搬完之后线上是 00 03 61 62 63 00 00：一块内容，加一个终止块。
//! let wire = encode.write_all_async().await.expect("搬运不该失败");
//! assert_eq!(wire, 7usize);
//! # }
//! ```

use core::error::Error;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrConsumerState, TrProducerState},
    error::{ReadErrTag, TrTaggedError},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, ok_or::XtOkOr};
use anylr::SomeOf;

use super::limited::{
    LimitedRefSegm, narrow_demand_, read_exact_into_async_, split_some_of_, total_of_,
};

/// 单个块能承载的最大字节数：`u16` 的上限。
///
/// 长度前缀是 2 字节，因此单块内容不可能超过这个数；写侧遇到更长的内容会自动切块。
pub const K_MAX_CHUNK_PAYLOAD: usize = u16::MAX as usize;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 写侧：把源的内容按块搬进目标
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 分块搬运的错误。
///
/// 搬运同时碰两端，因此错误也分两端：源与目标各自的错误原样带出，调用方可以向里取它们
/// 自己的标签（`ReadErrTag` / `WriteErrTag`）来决定这条连接还能不能继续用。
#[derive(Clone, Debug)]
pub enum ChunkedEncodeError<TySrcErr, TyDstErr> {
    /// 从源读取失败。
    Source(TySrcErr),

    /// 往目标写入失败。
    ///
    /// 目标是限长写口时，「体超出声明长度」也落在这一支——那是目标自己报出来的错误。
    Target(TyDstErr),

    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    ///
    /// 此时线上通常已经写出了若干块、却没有终止块：这条体的流已经不可用，调用方应当按
    /// 「这次发送失败」处理，而不是把它续成一条完整的体。
    Cancelled,

    /// 源报「还有数据」，却让出了一个 0 字节的段：搬运无法推进，就此中止。
    EmptySegment,

    /// 借到的段与这一块所需的量不符（源让出的长度对不上，或目标装不下整块）。
    Unsatisfied {
        /// 本应当搬走的字节数。
        expected: usize,

        /// 实际搬走的字节数。
        got: usize,
    },
}

impl<TySrcErr, TyDstErr> core::fmt::Display for ChunkedEncodeError<TySrcErr, TyDstErr>
where
    TySrcErr: core::fmt::Display,
    TyDstErr: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ChunkedEncodeError::Source(err) => write!(f, "分块搬运：源报错（{err}）"),
            ChunkedEncodeError::Target(err) => write!(f, "分块搬运：目标报错（{err}）"),
            ChunkedEncodeError::Cancelled => f.write_str("分块搬运：等待期间被取消"),
            ChunkedEncodeError::EmptySegment => {
                f.write_str("分块搬运：源报有数据，却让出 0 字节的段")
            }
            ChunkedEncodeError::Unsatisfied { expected, got } => write!(
                f,
                "分块搬运：段让出的量不符（预期 {expected} 字节，实际 {got} 字节）"
            ),
        }
    }
}

// 不实现 `source()`：理由同 `ChunkedReadError`——避免 `'static` 约束沿类型参数传染。
impl<TySrcErr, TyDstErr> Error for ChunkedEncodeError<TySrcErr, TyDstErr>
where
    TySrcErr: Error,
    TyDstErr: Error,
{}

/// 分块搬运器：把一个**读半边**的内容按块搬进一个**写半边**。
///
/// # 两端都必须报告状态
///
/// 块长必须在**动笔之前**定下（见模块文档），因此每一块之前本类型问两件事：
///
/// - [`TrConsumerState`]（源）：现在让得出多少字节？到没到头？
/// - [`TrProducerState`]（目标）：现在装得下多少字节？
///
/// 两者与单块上限 [`K_MAX_CHUNK_PAYLOAD`] 取小，就是这一块的块长。任何一端报不出状态
/// （返回 `None`）时，本类型退回「不替它设上限」：到头与否由读端报出的 `Closing` 判断，
/// 装不装得下由目标自己的 `write_async` 决定何时就绪。
///
/// # 一次调用搬完一整条分块体
///
/// [`ChunkedEncode::write_all_async`] 会一直搬到源结束（源报「已关闭且无剩余」，或读端报出
/// 标签为 `Closing` 的错误），再补上长度 `0` 的终止块；返回值是这一次写出的**线上**字节数
/// （含各块前缀与终止块）。
///
/// 源是 [`LimitedReader`](super::limited::LimitedReader) 时，「额度用尽」就是「体到此为止」：
/// 同一条流上跟在体后面的字节不会被误当成体的一部分（见 `dev-notes` 里 suffix stream 一节）。
///
/// # Examples
///
/// ```
/// use abs_mptp::messaging::chunked::ChunkedEncode;
///
/// # async fn demo() {
/// let mut src: &[u8] = b"abc";
/// let mut storage = [0u8; 64];
/// let mut sink: &mut [u8] = &mut storage;
/// let mut writer = ChunkedEncode::new(&mut src, &mut sink);
/// // 线上是 00 03 61 62 63 00 00：一块内容，加一个终止块。
/// let wire = writer.write_all_async().await.expect("搬运不该失败");
/// assert_eq!(wire, 7usize);
/// # }
/// ```
#[derive(Debug)]
pub struct ChunkedEncode<'a, R, W>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
{
    src_: &'a mut R,
    dst_: &'a mut W,
}

impl<'a, R, W> ChunkedEncode<'a, R, W>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
{
    /// 把一个读半边与一个写半边拼成一个分块搬运器。
    pub const fn new(src: &'a mut R, dst: &'a mut W) -> Self {
        ChunkedEncode {
            src_: src,
            dst_: dst,
        }
    }

    /// 把源的剩余内容全部搬完，并补上终止块；返回写出的**线上**字节数。
    ///
    /// 返回的 future 按本 crate 的惯例**不含**取消令牌，由调用方决定
    /// `may_cancel_with(token)` 之后再 `await`。
    pub fn write_all_async<'f>(&'f mut self) -> ChunkedEncodeAsync<'f, 'f, R, W> {
        ChunkedEncodeAsync::new(&mut *self.src_, &mut *self.dst_)
    }
}

/// [`ChunkedEncode::write_all_async`] 的 step 函数。
///
/// 每一步的形状固定为「先定块长 → 借源段 → 借目标段 → 前缀与内容一次写进同一个段」：
/// 段的消费量只在回收时结算，块长因此绝不能等到写完之后再补。
#[gen_may_cancel_future(ChunkedEncode, pub, new(pub(crate)))]
async fn chunked_write_all_async_<'f, R, W, K>(
    src: &'f mut R,
    dst: &'f mut W,
    cancel: K,
) -> Result<usize, ChunkedEncodeError<R::Err, W::Err>>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
    K: TrCancellationToken,
{
    let mut wire = 0usize;
    loop {
        // ---- 1. 源到头了吗 ----
        let src_state = src.consumer_state();
        if matches!(src_state, Option::Some((0usize, true))) {
            break;
        }

        // ---- 2. 这一块能有多大：目标现成空间、源现有量、单块上限取小 ----
        let room = match dst.producer_state() {
            // 目标报得出空间：这一块的内容不超过它现在装得下的量。
            // 报 0（或不足 3 字节）时按 1 字节试：`write_async` 会等它腾出空间，
            // 已经关闭的目标则会直接报出自己那条错误。
            Option::Some((free, _)) => core::cmp::max(free.saturating_sub(2usize), 1usize),
            // 目标不报状态：不替它设上限。
            Option::None => K_MAX_CHUNK_PAYLOAD,
        };
        let cap = match src_state {
            // 源现在就有数据：这一块不超过它现在让得出的量。
            Option::Some((avail, _)) if avail > 0usize => core::cmp::min(avail, room),
            // 源暂时空着（但还开着）：读这一下会等它，块长只由目标决定。
            _ => room,
        };
        let cap = core::cmp::min(cap, K_MAX_CHUNK_PAYLOAD);

        // ---- 3. 从源借出这一段 ----
        let read_demand = Demand::no_more_than(cap);
        let read_fut = src.read_async(&read_demand).into_future();
        let Result::Ok(res) = read_fut.ok_or(cancel.child_token().cancellation()).await else {
            return Result::Err(ChunkedEncodeError::Cancelled);
        };
        let mut src_segm = match split_some_of_(res) {
            Result::Ok(segm) => segm,
            // 读端报「结束」就是这条体到头了；终止块在循环之后补。
            Result::Err(err) if err.err_tag() == ReadErrTag::Closing => break,
            Result::Err(err) => return Result::Err(ChunkedEncodeError::Source(err)),
        };
        let len = total_of_(&src_segm);
        if len == 0usize {
            return Result::Err(ChunkedEncodeError::EmptySegment);
        }
        if len > cap {
            // `no_more_than(cap)` 没被源遵守：这一段超过这一块的上限。前缀是定长 2 字节，
            // 再往下写就会得到长度不符的帧——就此中止，别把坏帧放出去。
            return Result::Err(ChunkedEncodeError::Unsatisfied {
                expected: cap,
                got: len,
            });
        }

        // ---- 4. 写成一块：块长已定，前缀与内容一次写进同一个段 ----
        let frame_demand = Demand::exactly(2usize + len);
        let write_fut = dst.write_async(&frame_demand).into_future();
        let Result::Ok(res) = write_fut.ok_or(cancel.child_token().cancellation()).await else {
            return Result::Err(ChunkedEncodeError::Cancelled);
        };
        let mut dst_segm = match split_some_of_(res) {
            Result::Ok(segm) => segm,
            Result::Err(err) => return Result::Err(ChunkedEncodeError::Target(err)),
        };
        let moved = {
            // 段内的顺序就是线上顺序：先 2 字节大端长度，再内容。
            // `len <= cap <= K_MAX_CHUNK_PAYLOAD`，所以这里不会截断。
            let prefix = (len as u16).to_be_bytes();
            let mut dst_child = dst_segm.as_segm_mut();
            let head = dst_child.clone_items_from_buff(&prefix);
            if head != 2usize {
                return Result::Err(ChunkedEncodeError::Unsatisfied {
                    expected: 2usize + len,
                    got: head,
                });
            }
            let mut src_child = src_segm.as_segm_ref();
            src_child.move_items_to_segm(&mut dst_child)
        };
        // 两端都在这里回收：目标结算 2 + len，源结算 len。
        drop(dst_segm);
        drop(src_segm);
        if moved != len {
            return Result::Err(ChunkedEncodeError::Unsatisfied {
                expected: len,
                got: moved,
            });
        }
        wire += 2usize + len;
    }

    // ---- 5. 终止块：长度为 0 的块只有前缀 ----
    let term_demand = Demand::exactly(2usize);
    let write_fut = dst.write_async(&term_demand).into_future();
    let Result::Ok(res) = write_fut.ok_or(cancel.child_token().cancellation()).await else {
        return Result::Err(ChunkedEncodeError::Cancelled);
    };
    let mut dst_segm = match split_some_of_(res) {
        Result::Ok(segm) => segm,
        Result::Err(err) => return Result::Err(ChunkedEncodeError::Target(err)),
    };
    let head = {
        let mut dst_child = dst_segm.as_segm_mut();
        dst_child.clone_items_from_buff(&0u16.to_be_bytes())
    };
    drop(dst_segm);
    if head != 2usize {
        return Result::Err(ChunkedEncodeError::Unsatisfied {
            expected: 2usize,
            got: head,
        });
    }
    Result::Ok(wire + 2usize)
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

// 不实现 `source()`：理由同 `LimitedReaderError`——避免 `'static` 约束沿 trait 传染。
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
pub struct ChunkedReader<'a, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    chunked_src_: &'a mut R,

    /// 当前块里还剩多少字节没被借出。
    chunk_left_: usize,

    /// 是否已经读到终止块。
    ended_: bool,

    /// 本次借段交给底层的需求；理由见 [`LimitedReader`](super::limited::LimitedReader) 的文档。
    demand_: Demand<usize>,
}

impl<'a, R> ChunkedReader<'a, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    /// 用一个分块读视图包住底层缓冲。底层缓冲的数据必须符合格式
    pub const fn new(chunked_src: &'a mut R) -> Self {
        ChunkedReader {
            chunked_src_: chunked_src,
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
    pub const fn inner(&self) -> &R {
        self.chunked_src_
    }
}

/// 分块读的同步借段步骤；被 [`ChunkedReader`] 与报文体视图共用。
///
/// 同步路径只在**当前块尚未读完**时借得出段：跨块要先读长度前缀，那是一个等待点。
pub(super) fn chunked_try_read_<'f, R>(
    chunked_src: &'f mut R,
    chunk_left: &'f mut usize,
    ended: bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
) -> SomeOf<LimitedRefSegm<'f, R::SegmRef<'f>>, ChunkedReadError<R::Err>>
where
    R: TrBuffRead<u8>,
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
    match split_some_of_(chunked_src.try_read(&*demand_slot)) {
        Result::Ok(segm) => {
            let total = total_of_(&segm);
            SomeOf::new_left(LimitedRefSegm::new_(segm, chunk_left, total))
        }
        Result::Err(err) => SomeOf::new_right(ChunkedReadError::Inner(err)),
    }
}

impl<R> TrBuffTryRead<u8> for ChunkedReader<'_, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    type SegmRef<'f> = LimitedRefSegm<'f, R::SegmRef<'f>> where Self: 'f;
    type Err = ChunkedReadError<R::Err>;

    fn try_read<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let ChunkedReader {
            chunked_src_: inner_,
            chunk_left_,
            ended_,
            demand_,
        } = self;
        chunked_try_read_(&mut **inner_, chunk_left_, *ended_, demand_, demand)
    }
}

impl<R> TrBuffRead<u8> for ChunkedReader<'_, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    type ReadAsync<'f> = ChunkedReadAsync<'f, 'f, R> where Self: 'f;

    fn read_async<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> Self::ReadAsync<'f> {
        ChunkedReadAsync::new(
            &mut *self.chunked_src_,
            &mut self.chunk_left_,
            &mut self.ended_,
            &mut self.demand_,
            demand,
        )
    }
}

impl<R> TrConsumerState for ChunkedReader<'_, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        Option::Some((self.chunk_left_, true))
    }
}

/// 分块读的核心步骤：必要时先读下一个块的长度前缀，再按「块内剩余」借出段。
///
/// 它被 [`ChunkedReader`] 与报文体视图（`BodyReader`）共用。
pub(super) async fn chunked_read_step_<'f, R, K>(
    inner: &'f mut R,
    chunk_left: &'f mut usize,
    ended: &'f mut bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<LimitedRefSegm<'f, R::SegmRef<'f>>, ChunkedReadError<R::Err>>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
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
            // 与取消令牌竞速：令牌先就绪就落到 `Err`，底层 future 随语句结束被丢弃。
            let Result::Ok(res) = read_fut.ok_or(cancel.child_token().cancellation()).await else {
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
        match head_fut.ok_or(cancel.child_token().cancellation()).await {
            Result::Err(_) => return SomeOf::new_right(ChunkedReadError::Cancelled),
            Result::Ok(Result::Err(err)) => {
                return SomeOf::new_right(ChunkedReadError::Malformed(err.to_string()));
            }
            Result::Ok(Result::Ok(())) => {}
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
async fn chunked_read_async_<'f, R, K>(
    inner: &'f mut R,
    chunk_left: &'f mut usize,
    ended: &'f mut bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: K,
) -> SomeOf<LimitedRefSegm<'f, R::SegmRef<'f>>, ChunkedReadError<R::Err>>
where
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    chunked_read_step_(inner, chunk_left, ended, demand_slot, demand, cancel).await
}
