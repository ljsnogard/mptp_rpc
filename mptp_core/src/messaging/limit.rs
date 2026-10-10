//! 限长读写：把「一次能借出多少」钉死在调用方**预先设定的字节数**上。
//!
//! 这两个类型是 [`TrBuffRead`] / [`TrBuffWrite`] 的衍生包装：对外仍然是同一个段接口，
//! 但内部把每一次 `try_read` / `try_write` / `read_async` / `write_async` 的 `Demand`
//! 与「剩余额度」求交，因此底层缓冲**永远不会**借出超过额度的段。
//!
//! # 额度是精确结算的
//!
//! 段接口的消费量只在段被回收（`Drop`）时才记账，因此包装层不能靠「借出多少就扣多少」
//! 来估算——调用方完全可能只消费一半就放掉段。这里让包装段在 `Drop` 时比较
//! 「借出时的剩余量」与「归还时的剩余量」（把 `iter_slices()` 的各片长度求和，不分配），
//! 差值就是真正被消费/写入的字节数，再据此扣减额度。多片段的段同样正确。
//!
//! # 额度用尽时是什么
//!
//! 返回 [`LimitReadError::Exhausted`] / [`LimitWriteError::Exhausted`]，其错误标签是
//! `Closing`：对 [`AsStdRead`](abs_buff_stdio_adapt::AsStdRead) 而言就是「读到 EOF」，
//! 对写侧则是「不能再写了」。也就是说，一个限长读在读完 N 字节之后**表现为流结束**，
//! 调用方不必自己数字节数。
//!
//! # Examples
//!
//! ```
//! use mptp_core::messaging::limit::LimitedRead;
//!
//! # async fn demo() {
//! let mut src: &[u8] = b"hello world";
//! // 这次读取只允许看到前 5 个字节，之后表现为流结束。
//! let mut limited = LimitedRead::new(&mut src, 5usize);
//! assert_eq!(limited.remaining(), 5usize);
//! # }
//! ```

use core::{error::Error, fmt};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite, gen_may_cancel_future,
    buffer::{SegmMut, SegmRef, TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrProducerState},
    error::{ReadErrTag, TrTaggedError, WriteErrTag},
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;

use super::io_::{MessageIoError, race_cancel_};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 错误类型
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 限长读的错误。
#[derive(Clone, Debug)]
pub enum LimitReadError<TyErr> {
    /// 预设额度已经用完：此后不会再借出任何字节。
    Exhausted,

    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    Cancelled,

    /// 底层缓冲自身报出的错误。
    Inner(TyErr),
}

impl<TyErr> fmt::Display for LimitReadError<TyErr>
where
    TyErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitReadError::Exhausted => f.write_str("限长读：预设额度已用完"),
            LimitReadError::Cancelled => f.write_str("限长读：等待期间被取消"),
            LimitReadError::Inner(err) => write!(f, "限长读：底层缓冲报错（{err}）"),
        }
    }
}

// 不实现 `source()`：它会把 `'static` 约束沿 `TrBuffRead::Err` 一路传染到每一个
// 使用者身上。底层错误的文案已经由 `Display` 带出，错误链的损失远小于这份约束的代价。
impl<TyErr> Error for LimitReadError<TyErr> where TyErr: Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for LimitReadError<TyErr>
where
    TyErr: Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            // 「额度用完」对读者就是「这条流到此为止」，与对端关闭同一语义。
            LimitReadError::Exhausted => ReadErrTag::Closing,
            LimitReadError::Cancelled => ReadErrTag::Cancelled,
            LimitReadError::Inner(err) => err.err_tag(),
        }
    }
}

/// 限长写的错误。
#[derive(Clone, Debug)]
pub enum LimitWriteError<TyErr> {
    /// 预设额度已经用完：此后不会再借出任何空间。
    Exhausted,

    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    Cancelled,

    /// 底层缓冲自身报出的错误。
    Inner(TyErr),
}

impl<TyErr> fmt::Display for LimitWriteError<TyErr>
where
    TyErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LimitWriteError::Exhausted => f.write_str("限长写：预设额度已用完"),
            LimitWriteError::Cancelled => f.write_str("限长写：等待期间被取消"),
            LimitWriteError::Inner(err) => write!(f, "限长写：底层缓冲报错（{err}）"),
        }
    }
}

// 理由同 [`LimitReadError`]：不实现 `source()`，避免 `'static` 约束传染。
impl<TyErr> Error for LimitWriteError<TyErr> where TyErr: Error {}

impl<TyErr> TrTaggedError<WriteErrTag> for LimitWriteError<TyErr>
where
    TyErr: Error + TrTaggedError<WriteErrTag>,
{
    fn err_tag(&self) -> WriteErrTag {
        match self {
            LimitWriteError::Exhausted => WriteErrTag::Closing,
            LimitWriteError::Cancelled => WriteErrTag::Cancelled,
            LimitWriteError::Inner(err) => err.err_tag(),
        }
    }
}

/// 把 `SomeOf` 拆成「左值优先，否则右值」。
///
/// `SomeOf` 不是枚举：它只提供 `pick_left()` / `pick_right()` / `contains_left()`。
/// 底层缓冲的契约是「要么借出段、要么报错」，因此这里按左值优先拆分。
pub(super) fn split_some_of_<TyLeft, TyRight>(value: SomeOf<TyLeft, TyRight>) -> Result<TyLeft, TyRight> {
    if value.contains_left() {
        return Result::Ok(value.pick_left().expect("刚刚确认过含左值"));
    }
    Result::Err(value.pick_right().expect("既不含左值，则必含右值"))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 段包装：把消费量结算回额度
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 段视图里各片长度的总和。
///
/// 对单片段就是「未消费量」；对多片段（环尾拆开）是各片之和。只做求和，不分配。
pub(super) fn total_of_<TySegm>(segm: &TySegm) -> usize
where
    TySegm: TrBuffSegmView,
{
    segm.iter_slices().into_iter().map(<[TySegm::Item]>::len).sum()
}

/// 读段的透明包装：被回收时把「消费掉的字节数」结算回额度。
///
/// 它自身不改变段的内容与顺序，只是把 `S` 原样转发出去；`take_segm_ref` /
/// `as_segm_ref` 也都直接委托给 `S`，因此子段的消费会照常推进 `S` 的位置，最终在
/// **本包装**被回收时一次性结算——父子段不会重复计数。
pub struct LimitedRefSegm<'f, TySegm>
where
    TySegm: TrBuffSegmView,
{
    segm_: TySegm,
    left_: &'f mut usize,
    total_: usize,
}

impl<'f, TySegm> LimitedRefSegm<'f, TySegm>
where
    TySegm: TrBuffSegmView,
{
    /// 由底层段、额度计数器与借出时的剩余总量构造。
    pub(super) const fn new_(segm: TySegm, left: &'f mut usize, total: usize) -> Self {
        LimitedRefSegm {
            segm_: segm,
            left_: left,
            total_: total,
        }
    }
}

impl<TySegm> Drop for LimitedRefSegm<'_, TySegm>
where
    TySegm: TrBuffSegmView,
{
    fn drop(&mut self) {
        let consumed = self.total_.saturating_sub(total_of_(&self.segm_));
        *self.left_ = self.left_.saturating_sub(consumed);
    }
}

impl<TySegm> TrBuffSegmView for LimitedRefSegm<'_, TySegm>
where
    TySegm: TrBuffSegmView,
{
    type SlicesIter<'f>
        = TySegm::SlicesIter<'f>
    where
        Self: 'f,
        TySegm: 'f;

    type Item = TySegm::Item;

    #[inline]
    fn is_empty(&self) -> bool {
        self.segm_.is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.segm_.least_count()
    }

    #[inline]
    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        self.segm_.iter_slices()
    }
}

impl<'a, TySegm> TrBuffSegmRef<'a, u8> for LimitedRefSegm<'a, TySegm>
where
    TySegm: TrBuffSegmRef<'a, u8>,
{
    type Reclaimer<'f>
        = TySegm::Reclaimer<'f>
    where
        Self: 'f,
        TySegm: 'f;

    type TakeSegmRef<'f>
        = TySegm::TakeSegmRef<'f>
    where
        Self: 'f,
        TySegm: 'f;

    #[inline]
    fn take_segm_ref<'f>(&'f mut self, demand: &Demand<usize>) -> Self::TakeSegmRef<'f> {
        self.segm_.take_segm_ref(demand)
    }

    #[inline]
    fn as_segm_ref<'f>(&'f mut self) -> SegmRef<'f, u8, Self::Reclaimer<'f>> {
        self.segm_.as_segm_ref()
    }
}

/// 写段的透明包装：被回收时把「写入的字节数」结算回额度。
///
/// 语义与 [`LimitedRefSegm`] 对称：结算发生在**本包装**被回收时，子段只照常推进
/// `S` 的写位置。
pub struct LimitedMutSegm<'f, TySegm>
where
    TySegm: TrBuffSegmView,
{
    segm_: TySegm,
    left_: &'f mut usize,
    total_: usize,
}

impl<'f, TySegm> LimitedMutSegm<'f, TySegm>
where
    TySegm: TrBuffSegmView,
{
    /// 由底层段、额度计数器与借出时的剩余空间总量构造。
    const fn new_(segm: TySegm, left: &'f mut usize, total: usize) -> Self {
        LimitedMutSegm {
            segm_: segm,
            left_: left,
            total_: total,
        }
    }
}

impl<TySegm> Drop for LimitedMutSegm<'_, TySegm>
where
    TySegm: TrBuffSegmView,
{
    fn drop(&mut self) {
        let written = self.total_.saturating_sub(total_of_(&self.segm_));
        *self.left_ = self.left_.saturating_sub(written);
    }
}

impl<TySegm> TrBuffSegmView for LimitedMutSegm<'_, TySegm>
where
    TySegm: TrBuffSegmView,
{
    type SlicesIter<'f>
        = TySegm::SlicesIter<'f>
    where
        Self: 'f,
        TySegm: 'f;

    type Item = TySegm::Item;

    #[inline]
    fn is_empty(&self) -> bool {
        self.segm_.is_empty()
    }

    #[inline]
    fn least_count(&self) -> usize {
        self.segm_.least_count()
    }

    #[inline]
    fn iter_slices(&self) -> Self::SlicesIter<'_> {
        self.segm_.iter_slices()
    }
}

impl<'a, TySegm> TrBuffSegmMut<'a, u8> for LimitedMutSegm<'a, TySegm>
where
    TySegm: TrBuffSegmMut<'a, u8>,
{
    type Reclaimer<'f>
        = TySegm::Reclaimer<'f>
    where
        Self: 'f,
        TySegm: 'f;

    type TakeSegmMut<'f>
        = TySegm::TakeSegmMut<'f>
    where
        Self: 'f,
        TySegm: 'f;

    #[inline]
    fn take_segm_mut<'f>(&'f mut self, demand: &Demand<usize>) -> Self::TakeSegmMut<'f> {
        self.segm_.take_segm_mut(demand)
    }

    #[inline]
    fn as_segm_mut<'f>(&'f mut self) -> SegmMut<'f, u8, Self::Reclaimer<'f>> {
        self.segm_.as_segm_mut()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 额度收窄
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把需求收窄到「最多 `remaining` 个」；无法满足（或额度为 0）时返回 `None`。
pub(super) fn narrow_demand_(demand: &Demand<usize>, remaining: usize) -> Option<Demand<usize>> {
    if remaining == 0usize {
        return Option::None;
    }
    let narrowed = demand.compromise(&Demand::no_more_than(remaining))?;
    if narrowed.is_empty() {
        return Option::None;
    }
    Option::Some(narrowed)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// LimitedRead
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 只借出**预先设定额度**的读包装。
///
/// 额度在构造时给定；每读到一段，就按该段真正被消费的字节数扣减。额度用尽之后
/// 表现为「流结束」（错误标签 `Closing`），不会再去碰底层缓冲。
///
/// # 为什么自带一个 `Demand` 字段
///
/// `TrBuffRead::read_async` 要求「被借的缓冲」与「`Demand`」**同寿**——段返回之后
/// 依然可以引用那个 `Demand`。收窄后的需求是本次调用的局部量，活不到段被回收的
/// 那一刻，因此它必须与缓冲引用一样，从 `self` 里借出：调用时先写进 [`LimitedRead`]
/// 自己的字段，再把两个字段的引用一起交给底层。
///
/// # Examples
///
/// ```
/// use mptp_core::messaging::limit::LimitedRead;
///
/// # async fn demo() {
/// let mut src: &[u8] = b"hello world";
/// let mut limited = LimitedRead::new(&mut src, 5usize);
/// assert_eq!(limited.remaining(), 5usize);
/// # }
/// ```
pub struct LimitedRead<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    inner_: &'a mut TyRx,

    /// 还没被消费掉的额度。
    remaining_: usize,

    /// 本次借段交给底层的需求；见类型文档。
    demand_: Demand<usize>,
}

impl<'a, TyRx> LimitedRead<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    /// 用给定额度包住一个读缓冲。
    pub const fn new(inner: &'a mut TyRx, limit: usize) -> Self {
        LimitedRead {
            inner_: inner,
            remaining_: limit,
            demand_: Demand::exactly(0usize),
        }
    }

    /// 还剩多少额度。
    pub const fn remaining(&self) -> usize {
        self.remaining_
    }

    /// 额度是否已经用完。
    pub const fn is_exhausted(&self) -> bool {
        self.remaining_ == 0usize
    }

    /// 取回底层缓冲的引用。
    pub const fn inner(&self) -> &TyRx {
        self.inner_
    }
}

/// 限长读的同步借段步骤；被 [`LimitedRead`] 与报文体视图共用。
pub(super) fn limited_try_read_<'f, TyRx>(
    inner: &'f mut TyRx,
    remaining: &'f mut usize,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, LimitReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
{
    let narrowed = match narrow_demand_(demand, *remaining) {
        Option::Some(narrowed) => narrowed,
        // 借不到任何字节：交给调用方一个明确的「额度用完」。
        Option::None => return SomeOf::new_right(LimitReadError::Exhausted),
    };
    *demand_slot = narrowed;
    match split_some_of_(inner.try_read(&*demand_slot)) {
        Result::Ok(segm) => {
            let total = total_of_(&segm);
            SomeOf::new_left(LimitedRefSegm::new_(segm, remaining, total))
        }
        Result::Err(err) => SomeOf::new_right(LimitReadError::Inner(err)),
    }
}

impl<TyRx> TrBuffTryRead<u8> for LimitedRead<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type SegmRef<'f>
        = LimitedRefSegm<'f, TyRx::SegmRef<'f>>
    where
        Self: 'f;

    type Err = LimitReadError<TyRx::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let LimitedRead {
            inner_,
            remaining_,
            demand_,
        } = self;
        limited_try_read_(&mut **inner_, remaining_, demand_, demand)
    }
}

impl<TyRx> TrBuffRead<u8> for LimitedRead<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = LimitedReadAsync<'f, 'f, TyRx>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        LimitedReadAsync::new(
            &mut *self.inner_,
            &mut self.remaining_,
            &mut self.demand_,
            demand,
        )
    }
}

/// 限长读的核心步骤：收窄需求、借段、把段包成会结算额度的包装。
///
/// 它被 [`LimitedRead`] 与报文体视图（`BodyReader`）共用——两者的差别只在「额度从哪
/// 来」，借段这一步是同一件事。
///
/// `demand_slot` 是**调用方结构体里的字段**，不是局部量：`read_async` 要求被借的缓冲与
/// `Demand` 同寿，而段要活到返回类型所声明的那个生命周期（详见 [`LimitedRead`] 的文档）。
pub(super) async fn limited_read_step_<'f, TyRx, TyTok>(
    inner: &'f mut TyRx,
    remaining: &'f mut usize,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, LimitReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let narrowed = narrow_demand_(demand, *remaining);
    let narrowed = match narrowed {
        Option::Some(narrowed) => narrowed,
        Option::None => return SomeOf::new_right(LimitReadError::Exhausted),
    };
    *demand_slot = narrowed;
    let read_fut = inner.read_async(&*demand_slot).into_future();
    let Option::Some(res) = race_cancel_(cancel, read_fut).await else {
        return SomeOf::new_right(LimitReadError::Cancelled);
    };
    match split_some_of_(res) {
        Result::Ok(segm) => {
            let total = total_of_(&segm);
            SomeOf::new_left(LimitedRefSegm::new_(segm, remaining, total))
        }
        Result::Err(err) => SomeOf::new_right(LimitReadError::Inner(err)),
    }
}

/// [`LimitedRead::read_async`] 的 step 函数。
#[gen_may_cancel_future(LimitedRead, pub, new(pub(crate)))]
async fn limited_read_async_<'f, TyRx, TyTok>(
    inner: &'f mut TyRx,
    remaining: &'f mut usize,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, LimitReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    limited_read_step_(inner, remaining, demand_slot, demand, cancel).await
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// LimitedWrite
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 只借出**预先设定额度**的写包装，语义与 [`LimitedRead`] 对称。
///
/// 它同时转发底层的 [`TrProducerState`]，并把报告的「可用空间」收窄到额度以内——
/// 于是 [`AsStdWrite`](abs_buff_stdio_adapt::AsStdWrite) 这类适配器在额度用完时会
/// 认为「暂时写不进去」，而不是把字节写到底层去。
///
/// # Examples
///
/// ```
/// use mptp_core::messaging::limit::LimitedWrite;
///
/// # async fn demo() {
/// let mut storage = [0u8; 8];
/// let mut sink: &mut [u8] = &mut storage;
/// let limited = LimitedWrite::new(&mut sink, 3usize);
/// assert_eq!(limited.remaining(), 3usize);
/// # }
/// ```
pub struct LimitedWrite<'a, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    inner_: &'a mut TyTx,

    /// 还没被写掉的额度。
    remaining_: usize,

    /// 本次借段交给底层的需求；理由同 [`LimitedRead`]。
    demand_: Demand<usize>,
}

impl<'a, TyTx> LimitedWrite<'a, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    /// 用给定额度包住一个写缓冲。
    pub const fn new(inner: &'a mut TyTx, limit: usize) -> Self {
        LimitedWrite {
            inner_: inner,
            remaining_: limit,
            demand_: Demand::exactly(0usize),
        }
    }

    /// 还剩多少额度。
    pub const fn remaining(&self) -> usize {
        self.remaining_
    }

    /// 额度是否已经用完。
    pub const fn is_exhausted(&self) -> bool {
        self.remaining_ == 0usize
    }

    /// 取回底层缓冲的引用。
    pub const fn inner(&self) -> &TyTx {
        self.inner_
    }
}

impl<TyTx> TrBuffTryWrite<u8> for LimitedWrite<'_, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    type SegmMut<'f>
        = LimitedMutSegm<'f, TyTx::SegmMut<'f>>
    where
        Self: 'f;

    type Err = LimitWriteError<TyTx::Err>;

    fn try_write<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        let narrowed = narrow_demand_(demand, self.remaining_);
        let narrowed = match narrowed {
            Option::Some(narrowed) => narrowed,
            Option::None => return SomeOf::new_right(LimitWriteError::Exhausted),
        };
        let LimitedWrite {
            inner_,
            remaining_,
            demand_,
        } = self;
        *demand_ = narrowed;
        match split_some_of_(inner_.try_write(demand_)) {
            Result::Ok(segm) => {
                let total = total_of_(&segm);
                SomeOf::new_left(LimitedMutSegm::new_(segm, remaining_, total))
            }
            Result::Err(err) => SomeOf::new_right(LimitWriteError::Inner(err)),
        }
    }
}

impl<TyTx> TrBuffWrite<u8> for LimitedWrite<'_, TyTx>
where
    TyTx: TrBuffWrite<u8>,
{
    type WriteAsync<'f>
        = LimitedWriteAsync<'f, 'f, TyTx>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.demand_ = narrow_demand_(demand, self.remaining_).unwrap_or_else(|| Demand::less_than(0));
        LimitedWriteAsync::new(&mut *self.inner_, &mut self.remaining_, &self.demand_)
    }
}

impl<TyTx> TrProducerState for LimitedWrite<'_, TyTx>
where
    TyTx: TrBuffWrite<u8> + TrProducerState,
{
    fn producer_state(&self) -> Option<(usize, bool)> {
        let (free, closed) = self.inner_.producer_state()?;
        // 额度用尽等价于「没有可用空间」，底层关闭也照样如实上报。
        Option::Some((free.min(self.remaining_), closed || self.remaining_ == 0usize))
    }
}

/// [`LimitedWrite::write_async`] 的 step 函数。
#[gen_may_cancel_future(LimitedWrite, pub, new(pub(crate)))]
async fn limited_write_async_<'f, TyTx, TyTok>(
    inner: &'f mut TyTx,
    remaining: &'f mut usize,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedMutSegm<'f, TyTx::SegmMut<'f>>, LimitWriteError<TyTx::Err>>
where
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    if demand.is_empty() || *remaining == 0usize {
        return SomeOf::new_right(LimitWriteError::Exhausted);
    }
    let write_fut = inner.write_async(demand).into_future();
    let Option::Some(res) = race_cancel_(cancel, write_fut).await else {
        return SomeOf::new_right(LimitWriteError::Cancelled);
    };
    match split_some_of_(res) {
        Result::Ok(segm) => {
            let total = total_of_(&segm);
            SomeOf::new_left(LimitedMutSegm::new_(segm, remaining, total))
        }
        Result::Err(err) => SomeOf::new_right(LimitWriteError::Inner(err)),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读满定长字节
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 从读缓冲精确读满 `dst.len()` 字节。
///
/// 每一次借段只搬走**这次借到的那些**字节，段回收时按搬走的量记账；因此多出来的部分
/// 仍留在底层缓冲里等下一次读——这正是分块协议读 2 字节前缀所需要的语义。
///
/// # Errors
///
/// 在读满之前对端关闭、底层报错，或让出的段一个字节都搬不动（防死循环）时返回
/// [`MessageIoError::Truncated`]。
pub(crate) async fn read_exact_into_async_<TyRx, TyTok>(
    rx: &mut TyRx,
    dst: &mut [u8],
    tok: TyTok,
) -> Result<(), MessageIoError>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let total = dst.len();
    let mut filled = 0usize;
    while filled < total {
        let demand = Demand::no_more_than(total - filled);
        let read_fut = rx.read_async(&demand).into_future();
        let Option::Some(mut res) = race_cancel_(tok.child_token(), read_fut).await else {
            return Result::Err(MessageIoError::Truncated(format!(
                "读满 {total} 字节期间被取消（已读 {filled} 字节）"
            )));
        };
        let Option::Some(segm) = res.as_mut().pick_left() else {
            let detail = res
                .pick_right()
                .map_or_else(|| "底层没有给出错误详情".to_string(), |err| err.to_string());
            return Result::Err(MessageIoError::Truncated(format!(
                "读满 {total} 字节之前读端已经结束（已读 {filled} 字节）：{detail}"
            )));
        };

        // 与 `AsStdRead` 同一条路径：段的缓冲就是源自己的内存，`u8` 没有 drop 需求，
        // 搬移只推进段自身的偏移。
        let rest = &mut dst[filled..];
        let target = unsafe {
            core::slice::from_raw_parts_mut(
                rest.as_mut_ptr().cast::<core::mem::MaybeUninit<u8>>(),
                rest.len(),
            )
        };
        let mut child = segm.as_segm_ref();
        // SAFETY: 被搬的是 `u8`（无需 drop），`target` 在整个搬移期间独占借用。
        let moved = unsafe { child.move_items_to_buff(target) };
        debug_assert!(moved <= total - filled);
        filled += moved;
        if moved == 0usize {
            return Result::Err(MessageIoError::Truncated(format!(
                "读满 {total} 字节的过程中段让不出任何字节（已读 {filled} 字节）"
            )));
        }
    }
    Result::Ok(())
}
