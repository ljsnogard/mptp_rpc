//! 报文体的**协议无关视图**：体边界判定、体读视图与体进度。
//!
//! 客户端与服务端共享这一层：
//!
//! - 客户端用它读**回复体**（`RespBodyRead`）；
//! - 服务端用它读**请求体**（`ReqBodyRead`）。
//!
//! 两者交给上层的都是同一个 [`BodyRead`]，只是接收半边 `Rx` 不同。体边界由前缀唯一
//! 决定：没有体、定长体或分块体；视图被取走 / Drop 时把进度写回会话，续流阶段据此
//! 决定还要不要代读丢弃。
//!
//! # `body_`: transport-independent body views
//!
//! The client and the server share this layer: the client reads response bodies, the
//! server reads request bodies. Both hand out the same [`BodyRead`] over different
//! receive halves. The boundary is decided by the prefix alone, and the progress is
//! written back to the session when the view is taken or dropped.

use core::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead,
    buffer::TrConsumerState,
    error::{ReadErrTag, TrTaggedError},
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::{messaging::BodySize, spec::NumOrStr};
#[cfg(feature = "client")]
use abs_mptp::spec::access_method::AccessMethod;
use anylr::{SomeOf, some_of::SomeLR};
use mptp_wire::{
    chunked::{ChunkedReadAsync, ChunkedReadError, ChunkedReadFuture, ChunkedReader},
    limited::{LimitedReader, LimitedReaderAsync, LimitedReaderError, LimitedReaderFuture, LimitedRefSegm},
};

use crate::{
    error_::ProtocolError,
    x_deps::{abs_buff, abs_cancel, anylr},
};

/// 本条报文体的边界。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyPlan {
    /// 没有体：一个字节都不读。
    Absent,

    /// 定长体：恰好这么多字节。
    Sized {
        /// 声明的字节数。
        size_: usize,
    },

    /// 分块体：总长未知，读到终止块为止。
    Chunked,
}

impl BodyPlan {
    /// 只按前缀里的体声明定边界，不掺任何方法语义。
    ///
    /// 请求方向用它：能发多少体完全由**已经写出去的前缀**决定。声明的长度不是合法
    /// 十进制数时当场失败——把一个解析不出来的长度当成 `0` 会让体一个字节都不发，
    /// 而头里已经声明了长度，两端从此错位。
    pub(crate) fn from_info_(info: BodySize<'_>) -> Result<Self, ProtocolError> {
        match info {
            BodySize::NilBody => Result::Ok(BodyPlan::Absent),
            BodySize::FixedLength(size) => {
                let size_ = match size {
                    NumOrStr::Num(size) => size,
                    NumOrStr::Str(text) => text.trim().parse::<usize>().map_err(|_| {
                        ProtocolError::InvalidBodySize {
                            text: text.into(),
                        }
                    })?,
                };
                Result::Ok(BodyPlan::Sized { size_ })
            }
            BodySize::ChunkedData => Result::Ok(BodyPlan::Chunked),
        }
    }

    /// 由回复前缀给出的体声明（以及本端请求的方法）定下体边界。
    ///
    /// `Head` / `Drop` 的回复按协议不带体；前缀却声明了体，说明两端对协议的理解已经
    /// 不一致——宁可在读第一个字节之前失败，也不要带着错位的流继续走。
    #[cfg(feature = "client")]
    pub(crate) fn for_response_(
        info: BodySize<'_>,
        method: Option<AccessMethod>,
    ) -> Result<Self, ProtocolError> {
        let has_body = !matches!(info, BodySize::NilBody);
        if has_body
            && matches!(
                method,
                Option::Some(AccessMethod::Head) | Option::Some(AccessMethod::Drop)
            )
        {
            let method = method.ok_or(ProtocolError::MissingRequestMethod)?;
            return Result::Err(ProtocolError::ResponseBodyOnHeadOrDrop { method });
        }
        Self::from_info_(info)
    }
}

/// 体的读取进度。
///
/// 它只在**体读视图被取走或放回**时才更新，因此它回答的是「按当前视图的进度，体的
/// 剩余部分还有多少」——续流阶段要靠它决定是否需要代读丢弃。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BodyState {
    /// 还没有体的进度可言（前缀未到，或体还没开始读）。
    Fresh,

    /// 定长体：还剩 `left_` 字节没被读走。
    Sized {
        /// 剩余字节数。
        left_: usize,
    },

    /// 分块体：`ended_` 表示是否已经读到终止块。
    Chunked {
        /// 是否已经读到终止块。
        ended_: bool,
    },
}

/// 回复 / 请求体的字节视图：按前缀声明的传输方式二选一。
///
/// 两个档位共用同一套对外形状（[`TrBuffRead`] 的段接口），因此调用者不必关心这条
/// 报文是定长还是分块：任何解码器都能直接吃它。
///
/// **档位是实现细节**：对外只有一个类型，内部才分「定长 / 分块」。需要取出「前缀
/// 已经决定的那一种读口」时，用 [`TrMptpBuffRead`](abs_mptp::messaging::read::TrMptpBuffRead)
/// 的两个 `try_into_*`——取错分支会把另一个读口原样还回来。
pub struct BodyRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 实际读口。
    view_: BodyView<'f, Rx>,
}

/// [`BodyRead`] 的两个档位。
enum BodyView<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 定长档（含「没有体」：额度为 `0`）。
    Sized {
        /// 上游的限长读视图。
        reader_: LimitedReader<'f, Rx>,
        /// 进度槽：取走 / 放回时写入剩余额度。
        state_: &'f mut BodyState,
    },

    /// 分块档。
    Chunked {
        /// 上游的分块读视图。
        reader_: ChunkedReader<'f, Rx>,
        /// 进度槽：取走 / 放回时写入「是否读到终止块」。
        state_: &'f mut BodyState,
    },

    /// 视图已经被 `try_into_*` 取走；Drop 不再写回。
    Empty,
}

/// 把一个档位「还剩多少」写回它挂着的进度槽。
fn write_back_<Rx>(view: BodyView<'_, Rx>)
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    match view {
        BodyView::Sized { reader_, state_ } => {
            *state_ = BodyState::Sized {
                left_: reader_.remaining(),
            };
        }
        BodyView::Chunked { reader_, state_ } => {
            *state_ = BodyState::Chunked {
                ended_: reader_.is_ended(),
            };
        }
        BodyView::Empty => {}
    }
}

impl<'f, Rx> BodyRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 按已经定下的体边界构造视图。
    pub(crate) fn from_plan_(
        plan: BodyPlan,
        rx: &'f mut Rx,
        state: &'f mut BodyState,
    ) -> Self {
        let view_ = match plan {
            // 没有体：额度为 0 的限长视图。读起来立刻表现为 EOF，且**一个字节都不会碰**
            // 接收半边——这正是「没有体就不预读」那条纪律的落地方式。
            BodyPlan::Absent => BodyView::Sized {
                reader_: LimitedReader::new(rx, 0usize),
                state_: state,
            },
            BodyPlan::Sized { size_ } => BodyView::Sized {
                reader_: LimitedReader::new(rx, size_),
                state_: state,
            },
            BodyPlan::Chunked => BodyView::Chunked {
                reader_: ChunkedReader::new(rx),
                state_: state,
            },
        };
        BodyRead { view_ }
    }

    /// 体是否已经读完（定长档是额度用尽，分块档是读到终止块）。
    pub fn is_finished(&self) -> bool {
        match &self.view_ {
            BodyView::Sized { reader_, .. } => reader_.is_exhausted(),
            BodyView::Chunked { reader_, .. } => reader_.is_ended(),
            BodyView::Empty => true,
        }
    }

    /// 定长档还剩多少字节；分块档没有确定的总长，故为 `None`。
    pub fn remaining(&self) -> Option<usize> {
        match &self.view_ {
            BodyView::Sized { reader_, .. } => Option::Some(reader_.remaining()),
            BodyView::Chunked { .. } => Option::None,
            BodyView::Empty => Option::Some(0usize),
        }
    }
}

impl<Rx> Drop for BodyRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    fn drop(&mut self) {
        // 视图的内部状态只在这里回到会话手里；两个档位记的东西不同，但语义都是
        // 「体的剩余部分还有多少」。
        let view = core::mem::replace(&mut self.view_, BodyView::Empty);
        write_back_(view);
    }
}

impl<'f, Rx> abs_mptp::messaging::read::TrMptpBuffRead for BodyRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    type ChunkedRead = ChunkedReader<'f, Rx>;

    type LimitedRead = LimitedReader<'f, Rx>;

    /// 取出**分块**读口；前缀决定的是定长时，把定长读口原样还回来。
    fn try_into_chunked(mut self) -> Result<Self::ChunkedRead, Self::LimitedRead> {
        match core::mem::replace(&mut self.view_, BodyView::Empty) {
            BodyView::Chunked { reader_, state_ } => {
                *state_ = BodyState::Chunked {
                    ended_: reader_.is_ended(),
                };
                Result::Ok(reader_)
            }
            BodyView::Sized { reader_, state_ } => {
                *state_ = BodyState::Sized {
                    left_: reader_.remaining(),
                };
                Result::Err(reader_)
            }
            // `try_into_*` 只可能被调用一次（它消费 `self`），因此这里不可达。
            BodyView::Empty => unreachable!("BodyRead 的视图只能被取走一次"),
        }
    }

    /// 取出**定长**读口；前缀决定的是分块时，把分块读口原样还回来。
    fn try_into_limited(mut self) -> Result<Self::LimitedRead, Self::ChunkedRead> {
        match core::mem::replace(&mut self.view_, BodyView::Empty) {
            BodyView::Sized { reader_, state_ } => {
                *state_ = BodyState::Sized {
                    left_: reader_.remaining(),
                };
                Result::Ok(reader_)
            }
            BodyView::Chunked { reader_, state_ } => {
                *state_ = BodyState::Chunked {
                    ended_: reader_.is_ended(),
                };
                Result::Err(reader_)
            }
            // 同上：`self` 已被消费，视图不可能在此之前被取走。
            BodyView::Empty => unreachable!("BodyRead 的视图只能被取走一次"),
        }
    }
}

/// 报文体的读视图错误：两个档位各自的错误原样带出。
#[derive(Clone, Debug)]
pub enum BodyError<TyErr> {
    /// 定长档报出的错误。
    Sized(LimitedReaderError<TyErr>),

    /// 分块档报出的错误。
    Chunked(ChunkedReadError<TyErr>),
}

impl<TyErr> core::fmt::Display for BodyError<TyErr>
where
    TyErr: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            BodyError::Sized(err) => write!(f, "定长报文体：{err}"),
            BodyError::Chunked(err) => write!(f, "分块报文体：{err}"),
        }
    }
}

impl<TyErr> core::error::Error for BodyError<TyErr> where TyErr: core::error::Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for BodyError<TyErr>
where
    TyErr: core::error::Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            BodyError::Sized(err) => err.err_tag(),
            BodyError::Chunked(err) => err.err_tag(),
        }
    }
}

/// 把「左值优先」的 `SomeOf` 的右值（错误）换一种包装。
///
/// 用 [`SomeLR`] 的穷举分支代替 `expect`：`SomeOf` 保证左、右、两者恰有一个，
/// 因此这个转换不会 panic。
fn remap_right_<TyLeft, TyRight, TyNew>(
    value: SomeOf<TyLeft, TyRight>,
    wrap: impl FnOnce(TyRight) -> TyNew,
) -> SomeOf<TyLeft, TyNew> {
    match value.into_inner() {
        SomeLR::Left(left) | SomeLR::Both(left, _) => SomeOf::new_left(left),
        SomeLR::Right(right) => SomeOf::new_right(wrap(right)),
    }
}

impl<Rx> TrBuffTryRead<u8> for BodyRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    // 两个档位的段类型**本来就是同一个**：上游两种视图都把借出的段包成 `LimitedRefSegm`。
    type SegmRef<'f>
        = LimitedRefSegm<'f, <Rx as TrBuffTryRead<u8>>::SegmRef<'f>>
    where
        Self: 'f;

    type Err = BodyError<<Rx as TrBuffTryRead<u8>>::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        match &mut self.view_ {
            BodyView::Sized { reader_, .. } => {
                remap_right_(reader_.try_read(demand), BodyError::Sized)
            }
            BodyView::Chunked { reader_, .. } => {
                remap_right_(reader_.try_read(demand), BodyError::Chunked)
            }
            BodyView::Empty => unreachable!("BodyRead 的视图已经被取走"),
        }
    }
}

impl<Rx> TrBuffRead<u8> for BodyRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    type ReadAsync<'f>
        = BodyReadAsync<'f, Rx>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        match &mut self.view_ {
            BodyView::Sized { reader_, .. } => BodyReadAsync::Sized(reader_.read_async(demand)),
            BodyView::Chunked { reader_, .. } => BodyReadAsync::Chunked(reader_.read_async(demand)),
            BodyView::Empty => unreachable!("BodyRead 的视图已经被取走"),
        }
    }
}

impl<Rx> TrConsumerState for BodyRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        match &self.view_ {
            BodyView::Sized { reader_, .. } => reader_.consumer_state(),
            BodyView::Chunked { reader_, .. } => reader_.consumer_state(),
            BodyView::Empty => Option::None,
        }
    }
}

/// [`BodyRead::read_async`] 的产物：两个档位各自的异步读口。
pub enum BodyReadAsync<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 定长档。
    Sized(LimitedReaderAsync<'f, 'f, Rx>),

    /// 分块档。
    Chunked(ChunkedReadAsync<'f, 'f, Rx>),
}

impl<'f, Rx> TrMayCancel<'f> for BodyReadAsync<'f, Rx>
where
    Rx: 'f + TrBuffRead<u8> + TrConsumerState,
{
    type MayCancelFuture<'mf, TyTok>
        = BodyReadFuture<'mf, Rx, TyTok>
    where
        'mf: 'f,
        Self: 'mf,
        TyTok: 'mf + TrCancellationToken;

    type MayCancelOutput = SomeOf<
        LimitedRefSegm<'f, <Rx as TrBuffTryRead<u8>>::SegmRef<'f>>,
        BodyError<<Rx as TrBuffTryRead<u8>>::Err>,
    >;

    fn may_cancel_with<TyTok>(self, cancel: TyTok) -> Self::MayCancelFuture<'f, TyTok>
    where
        TyTok: 'f + TrCancellationToken,
    {
        match self {
            BodyReadAsync::Sized(async_) => {
                BodyReadFuture::Sized(async_.may_cancel_with(cancel))
            }
            BodyReadAsync::Chunked(async_) => {
                BodyReadFuture::Chunked(async_.may_cancel_with(cancel))
            }
        }
    }
}

impl<'f, Rx> IntoFuture for BodyReadAsync<'f, Rx>
where
    Rx: 'f + TrBuffRead<u8> + TrConsumerState,
{
    type IntoFuture = BodyReadFuture<'f, Rx, NonCancellableToken>;

    type Output = <Self as TrMayCancel<'f>>::MayCancelOutput;

    fn into_future(self) -> Self::IntoFuture {
        self.may_cancel_with(NonCancellableToken::new())
    }
}

/// [`BodyReadAsync::may_cancel_with`] 的产物：两个档位 future 的二选一。
///
/// 它实现 [`Future`] 的方式是**把 poll 原样转发给内部那个 future**；转发需要把
/// `Pin<&mut Self>` 降级成 `&mut Self`，因此下面那处 `unsafe` 是本文件唯一的一处。
pub enum BodyReadFuture<'f, Rx, TyTok>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    /// 定长档。
    Sized(LimitedReaderFuture<'f, 'f, Rx, TyTok>),

    /// 分块档。
    Chunked(ChunkedReadFuture<'f, 'f, Rx, TyTok>),
}

impl<'f, Rx, TyTok> Future for BodyReadFuture<'f, Rx, TyTok>
where
    Rx: 'f + TrBuffRead<u8> + TrConsumerState,
    TyTok: 'f + TrCancellationToken,
{
    type Output = SomeOf<
        LimitedRefSegm<'f, <Rx as TrBuffTryRead<u8>>::SegmRef<'f>>,
        BodyError<<Rx as TrBuffTryRead<u8>>::Err>,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 本枚举是「两个档位 future 的二选一」，没有自引用字段，也不实现 `Drop`；
        // 这里取出 `&mut self` 之后只做两件事——按变体取出内部 future、把它**原地**重新 pin
        // 住交给它的 `poll`。全程不移动、不替换、不落下任何字段，因此内部 future 在构造时得到
        // 的固定保证一直成立（这正是 `pin-project` 之类的库为枚举生成的那段代码）。
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            BodyReadFuture::Sized(fut) => {
                let fut = unsafe { Pin::new_unchecked(fut) };
                fut.poll(cx)
                    .map(|res| remap_right_(res, BodyError::Sized))
            }
            BodyReadFuture::Chunked(fut) => {
                let fut = unsafe { Pin::new_unchecked(fut) };
                fut.poll(cx)
                    .map(|res| remap_right_(res, BodyError::Chunked))
            }
        }
    }
}

/// 后缀流的读视图。
///
/// 后缀流是**裸字节**：没有长度声明、没有块帧，读到对端半关闭为止。因此它内部始终
/// 是一个「额度用不完」的限长视图，
/// [`TrMptpBuffRead`](abs_mptp::messaging::read::TrMptpBuffRead) 的两个
/// `try_into_*` 都交出同一个读口——后缀流本来就不存在「定长还是分块」这个分岔。
pub struct SuffixRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 额度为 `usize::MAX` 的限长视图。
    reader_: LimitedReader<'f, Rx>,
}

impl<'f, Rx> SuffixRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    /// 用 `usize::MAX` 的额度包住接收半边。
    pub(crate) fn new_(rx: &'f mut Rx) -> Self {
        SuffixRead {
            reader_: LimitedReader::new(rx, usize::MAX),
        }
    }
}

impl<'f, Rx> abs_mptp::messaging::read::TrMptpBuffRead for SuffixRead<'f, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    type ChunkedRead = LimitedReader<'f, Rx>;

    type LimitedRead = LimitedReader<'f, Rx>;

    /// 续流没有块帧，交出的就是裸字节读口。
    fn try_into_chunked(self) -> Result<Self::ChunkedRead, Self::LimitedRead> {
        Result::Err(self.reader_)
    }

    /// 同上。
    fn try_into_limited(self) -> Result<Self::LimitedRead, Self::ChunkedRead> {
        Result::Ok(self.reader_)
    }
}

impl<Rx> TrBuffTryRead<u8> for SuffixRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    type SegmRef<'f>
        = LimitedRefSegm<'f, <Rx as TrBuffTryRead<u8>>::SegmRef<'f>>
    where
        Self: 'f;

    type Err = LimitedReaderError<<Rx as TrBuffTryRead<u8>>::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.reader_.try_read(demand)
    }
}

impl<Rx> TrBuffRead<u8> for SuffixRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    type ReadAsync<'f>
        = LimitedReaderAsync<'f, 'f, Rx>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.reader_.read_async(demand)
    }
}

impl<Rx> TrConsumerState for SuffixRead<'_, Rx>
where
    Rx: TrBuffRead<u8> + TrConsumerState,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        self.reader_.consumer_state()
    }
}
