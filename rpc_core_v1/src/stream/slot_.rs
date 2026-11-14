//! 写方向：`Slot<T>` 守卫、[`TypedSlotFlux`] 与 [`TrSuffixWrite`]。
//!
//! `next_async` 只允许在上一槽已 set（或已丢弃）后推进；`set_async` 完成「编码 → 写
//! chunk → 归还 writer」后才允许下一槽。有限 Push 的结束由
//! [`SlotIter::finish_async`] 表达：半关闭写端并交给底层 writer 自己完成收尾
//! （客户端读回复、服务端 `SuffixStream::finish_async`）。
//!
//! # `slot_`: the write-side `Slot<T>` guard and [`TypedSlotFlux`]
//!
//! `next_async` advances only after the previous slot was set or dropped. `set_async`
//! encodes, writes one chunk and only then returns the writer. Finite Push ends through
//! [`SlotIter::finish_async`].

use alloc::{rc::Rc, string::ToString};
use core::{
    cell::RefCell,
    future::{Ready, ready},
    marker::PhantomData,
};

use abs_async_iter::{TrAsyncIterator, TrFlux};
use abs_buff::{TrBuffFlush, TrBuffWrite};
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_denco::ser::TrEncodable;
use mp_denco::MpEncoder;
use mptp_smux::{
    client::{
        RequestSuffixStream, RequestSuffixWriter, Response,
        config::{ChannelTx as ClientChannelTx, TrConfig as ClientConfig},
    },
    serving::{
        ResponseSuffixWriter, SuffixStream,
        config::{ChannelTx as ServingChannelTx, TrConfig},
    },
};

use crate::{
    stream::{StreamError, StreamFuture},
    x_deps::{abs_async_iter, abs_buff, abs_cancel, abs_denco, mp_denco, mptp_smux},
};

/// 一个能借出标准 [`TrBuffWrite`] 的后缀写端。
///
/// 实现者不拥有字节缓冲：底层会话 / ring 本来就是 `TrBuffWrite`，这里只把它的一个
/// 借用视图交出来。具体实现（`RequestSuffixWriter` / `ResponseSuffixWriter`）还实现了
/// `TrBuffTryFlush` / `TrBuffFlush`；其 `close` 只半关闭写侧，读回复 / 收尾仍由
/// [`TrSuffixFinish`] 负责。`rpc_core_v1` 因此可以**直接**把 denco 编码进这条 suffix，而不必
/// 先编码到调用者或本层准备的 `Vec`。
///
/// 已实现：
///
/// - [`SuffixStream`]（服务端 response suffix）→ `ResponseSuffixWriter`；
/// - 客户端 `RequestSuffixStream` → `RequestSuffixWriter`；
/// - 测试 sink 可在测试模块里实现。
pub trait TrSuffixWrite {
    /// 借出的标准写视图；生命周期绑定到 `self`。
    ///
    /// 要求 [`TrBuffFlush`]：后缀写端都有明确的“关闭写侧”语义，且它本来就建立在
    /// [`TrBuffWrite`] 之上；读回复 / 收尾仍由 [`TrSuffixFinish`] 负责。
    type Writer<'a>: TrBuffFlush<u8>
    where
        Self: 'a;

    /// 借出写视图。返回错误时流已经不可写（例如会话半边被取走）。
    fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError>;
}

/// 一个能显式结束的后缀写端。
///
/// `finish_async(self)` 消费写端：实现者负责半关闭 / 读回复 / 收尾。
pub trait TrSuffixFinish<'f>: TrSuffixWrite + 'f {
    /// 结束后的产出（客户端 Push 是 `Response<C>`，服务端 Pull 是 `()`）。
    type FinishOutput;

    /// 半关闭并完成收尾。
    fn finish_async(self) -> StreamFuture<'f, Self::FinishOutput>;
}

impl<'a, C> TrSuffixWrite for SuffixStream<'a, C>
where
    C: TrConfig + 'a,
    ServingChannelTx<C>: TrBuffWrite<u8>,
{
    type Writer<'b>
        = ResponseSuffixWriter<'b, C>
    where
        Self: 'b;

    fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError> {
        SuffixStream::writer(self).map_err(StreamError::from)
    }
}

impl<'a, C> TrSuffixFinish<'a> for SuffixStream<'a, C>
where
    C: TrConfig + 'a,
{
    type FinishOutput = ();

    fn finish_async(self) -> StreamFuture<'a, ()> {
        StreamFuture::new(async move {
            SuffixStream::finish_async(self)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(StreamError::from)
        })
    }
}

impl<C> TrSuffixWrite for RequestSuffixStream<C>
where
    C: ClientConfig,
    ClientChannelTx<C>: TrBuffWrite<u8>,
{
    type Writer<'b>
        = RequestSuffixWriter<'b, C>
    where
        Self: 'b;

    fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError> {
        Result::Ok(RequestSuffixStream::writer(self))
    }
}

impl<C> TrSuffixFinish<'static> for RequestSuffixStream<C>
where
    C: ClientConfig + 'static,
    mptp_smux::client::config::ChannelRx<C>:
        abs_buff::TrBuffRead<u8> + abs_buff::buffer::TrConsumerState,
{
    type FinishOutput = Response<C>;

    fn finish_async(self) -> StreamFuture<'static, Response<C>> {
        StreamFuture::new(async move {
            RequestSuffixStream::finish_async(self)
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(StreamError::from)
        })
    }
}

/// 一条写流的共享守卫状态。
#[derive(Debug, Default)]
struct GuardState_ {
    /// 是否已经交出一个未 set 的守卫。
    guard_: bool,

    /// 是否被未 set 的守卫 / 被丢弃的 future 中止。
    aborted_: bool,
}

/// 共享守卫状态句柄。
type SharedState_ = Rc<RefCell<GuardState_>>;

/// 写方向的一次性守卫。
///
/// `set_async` 消费守卫，因此重复 set 在类型层被禁止；未 set 就 drop 会把 writer 留给
/// 迭代器并标记中止，`finish_async` 仍能半关闭写端（有限 Push 的正常收尾）。
pub struct Slot<'f, T, W> {
    /// 与迭代器共享的 writer。
    writer_: Rc<RefCell<Option<W>>>,

    /// 与迭代器共享的守卫状态。
    state_: SharedState_,

    /// 生命周期与 `T` 的动量标记。
    _marker_: super::Marker_<'f, T>,
}

impl<'f, T, W> Slot<'f, T, W>
where
    T: TrEncodable + 'f,
    W: TrSuffixWrite + 'f,
{
    /// 把 `value` 直接编码进底层 suffix 的写 buff。
    ///
    /// 不经过调用者或本层准备的 `Vec` / 暂存区：writer 借出标准 [`TrBuffWrite`] 视图，
    /// denco 边编码边写入底层 ring。完成前 writer 被本操作独占，因此下一槽不会被提前
    /// 产出；写入失败时 writer 被 drop（半关闭），整条流进入中止状态。
    ///
    /// # Errors
    ///
    /// 返回 [`StreamError::Encode`] / [`StreamError::Transport`] /
    /// [`StreamError::Cancelled`]；取消或中途 drop 会让 writer 关闭并标记中止。
    pub fn set_async(self, value: T) -> StreamFuture<'f, ()> {
        let writer_cell = Rc::clone(&self.writer_);
        let state = Rc::clone(&self.state_);
        // `Slot` 有 Drop；这里主动消费它，不能让 Drop 再把状态改成 aborted。
        core::mem::forget(self);

        StreamFuture::new(async move {
            let mut abort = SetAbortOnDrop_ {
                state_: Rc::clone(&state),
                armed_: true,
            };
            let Some(mut writer) = writer_cell.borrow_mut().take() else {
                abort.armed_ = false;
                state.borrow_mut().guard_ = false;
                return Result::Err(StreamError::Transport(
                    "写流已不在 flux 中（可能已被 finish 取走）".to_string(),
                ));
            };

            let encoded = {
                let mut view = writer.writer_mut()?;
                value
                    .encode_async(MpEncoder::new(&mut view))
                    .may_cancel_with(NonCancellableToken::new())
                    .await
                    .map_err(|err| StreamError::Encode(err.to_string()))
            };
            match encoded {
                Result::Ok(()) => {
                    *writer_cell.borrow_mut() = Option::Some(writer);
                    abort.armed_ = false;
                    state.borrow_mut().guard_ = false;
                    Result::Ok(())
                }
                Result::Err(err) => {
                    state.borrow_mut().aborted_ = true;
                    abort.armed_ = false;
                    Result::Err(err)
                }
            }
        })
    }
}

impl<T, W> core::fmt::Debug for Slot<'_, T, W> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Slot").finish_non_exhaustive()
    }
}

impl<T, W> Drop for Slot<'_, T, W> {
    fn drop(&mut self) {
        let mut state = self.state_.borrow_mut();
        // 未 set 就丢弃：writer 仍在共享单元里，由 `finish_async` 半关闭；
        // 这里只标记中止，避免静默跳过。
        state.aborted_ = true;
        state.guard_ = false;
    }
}

/// `set_async` future 被中途丢弃时把流标记为中止。
struct SetAbortOnDrop_ {
    /// 共享守卫状态。
    state_: SharedState_,

    /// `false` 表示正常完成。
    armed_: bool,
}

impl Drop for SetAbortOnDrop_ {
    fn drop(&mut self) {
        if self.armed_ {
            let mut state = self.state_.borrow_mut();
            state.aborted_ = true;
            state.guard_ = false;
        }
    }
}

/// 在任意 [`TrSuffixWrite`] 之上逐项编码的写 flux。
///
/// # Examples
///
/// ```ignore
/// use rpc_core_v1::stream::TypedSlotFlux;
///
/// async fn produce<W>(writer: W) -> Result<(), rpc_core_v1::stream::StreamError>
/// where
///     W: rpc_core_v1::stream::TrSuffixWrite
///         + rpc_core_v1::stream::TrSuffixFinish<'static, FinishOutput = ()>,
/// {
///     let mut flux = TypedSlotFlux::<W, String>::new(writer);
///     let mut iter = flux.pull_async().await.may_cancel_with(
///         rpc_core_v1::x_deps::abs_cancel::NonCancellableToken::new(),
///     );
///     let _ = &mut iter;
///     Ok(())
/// }
/// ```
pub struct TypedSlotFlux<'f, W, T> {
    /// 底层写端；每个槽 set 后放回。
    writer_: Rc<RefCell<Option<W>>>,

    /// 共享守卫状态。
    state_: SharedState_,

    /// 生命周期与 `T` 的动量标记。
    _marker_: super::Marker_<'f, T>,
}

impl<'f, W, T> TypedSlotFlux<'f, W, T>
where
    W: 'f,
    T: 'f,
{
    /// 在给定写端上构造 flux。
    pub fn new(writer: W) -> Self {
        TypedSlotFlux {
            writer_: Rc::new(RefCell::new(Option::Some(writer))),
            state_: Rc::new(RefCell::new(GuardState_::default())),
            _marker_: PhantomData,
        }
    }

    /// 流是否已经中止。
    pub fn is_aborted(&self) -> bool {
        self.state_.borrow().aborted_
    }
}

impl<'f, W, T> TrFlux<Slot<'f, T, W>> for TypedSlotFlux<'f, W, T>
where
    W: TrSuffixWrite + 'f,
    T: TrEncodable + 'f,
{
    type Iter<'x>
        = SlotIter<'f, W, T>
    where
        Self: 'x;

    type PullAsync<'x>
        = Ready<SlotIter<'f, W, T>>
    where
        Self: 'x;

    fn pull_async(&mut self) -> Self::PullAsync<'_> {
        ready(SlotIter {
            writer_: Rc::clone(&self.writer_),
            state_: Rc::clone(&self.state_),
            _marker_: PhantomData,
        })
    }
}

/// [`TypedSlotFlux`] 的迭代器：每条 `next_async` 交出一个 [`Slot`]。
pub struct SlotIter<'f, W, T> {
    /// 与 flux 共享的写端。
    writer_: Rc<RefCell<Option<W>>>,

    /// 与 flux 共享的守卫状态。
    state_: SharedState_,

    /// 生命周期与 `T` 的动量标记。
    _marker_: super::Marker_<'f, T>,
}

impl<'f, W, T> SlotIter<'f, W, T>
where
    W: TrSuffixWrite + 'f,
    T: TrEncodable + 'f,
{
    /// 结束写流：半关闭 writer 并完成收尾（客户端读回复 / 服务端 `finish`）。
    ///
    /// 允许在「上一槽被丢弃导致 abort」之后调用：这正是有限 Push 提前结束的路径。
    /// 若仍有未 set 的 [`Slot`] 存活，返回 [`StreamError::GuardPending`]。
    pub fn finish_async(self) -> StreamFuture<'f, <W as TrSuffixFinish<'f>>::FinishOutput>
    where
        W: TrSuffixFinish<'f>,
    {
        if self.state_.borrow().guard_ {
            return StreamFuture::new(async { Result::Err(StreamError::GuardPending) });
        }
        let writer = self.writer_.borrow_mut().take();
        match writer {
            Option::Some(writer) => writer.finish_async(),
            Option::None => StreamFuture::new(async {
                Result::Err(StreamError::Transport(
                    "写流已被取走，无法 finish".to_string(),
                ))
            }),
        }
    }
}

impl<'f, W, T> TrAsyncIterator for SlotIter<'f, W, T>
where
    W: TrSuffixWrite + 'f,
    T: TrEncodable + 'f,
{
    type Item = Slot<'f, T, W>;

    type Err = StreamError;

    type NextAsync<'x>
        = StreamFuture<'x, Slot<'f, T, W>>
    where
        Self: 'x;

    fn next_async(&mut self) -> Self::NextAsync<'_> {
        {
            let mut state = self.state_.borrow_mut();
            if state.aborted_ {
                return StreamFuture::new(async { Result::Err(StreamError::Aborted) });
            }
            if state.guard_ {
                return StreamFuture::new(async { Result::Err(StreamError::GuardPending) });
            }
            state.guard_ = true;
        }
        let slot = Slot {
            writer_: Rc::clone(&self.writer_),
            state_: Rc::clone(&self.state_),
            _marker_: PhantomData,
        };
        StreamFuture::new(async move { Result::Ok(slot) })
    }
}

#[cfg(test)]
mod tests_ {
    use alloc::string::String;

    use abs_buff::{Demand, TrBuffTryFlush, TrBuffTryWrite, TrBuffWrite, x_deps::anylr::SomeOf};
    use abs_cancel::NonCancellableToken;
    use abs_denco::de::TrDecodable;
    use futures::executor::block_on;
    use mp_denco::MpDecoder;

    use super::*;

    /// 测试用写端：借用一段固定切片，记录已经写入的长度。
    struct SliceSink<'a> {
        /// 目标切片；写入时 `&mut [u8]` 自身会向前推进。
        buf_: &'a mut [u8],

        /// 初始长度，用来算已写入多少。
        init_: usize,
    }

    impl<'a> SliceSink<'a> {
        /// 在给定切片上构造。
        fn new(buf: &'a mut [u8]) -> Self {
            let init_ = buf.len();
            SliceSink { buf_: buf, init_ }
        }

        /// 已写入的字节数。
        fn used(&self) -> usize {
            self.init_ - self.buf_.len()
        }
    }

    /// [`SliceSink`] 的写视图：持有 `&mut &mut [u8]`，写入会推进外层切片。
    struct SliceSinkView<'b, 'a> {
        /// 指向切片变量的可变引用。
        buf_: &'b mut &'a mut [u8],
    }

    impl<'b, 'a> TrBuffTryWrite<u8> for SliceSinkView<'b, 'a> {
        type SegmMut<'f>
            = <&'a mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
        where
            Self: 'f;

        type Err = <&'a mut [u8] as TrBuffTryWrite<u8>>::Err;

        fn try_write<'f>(
            &'f mut self,
            demand: &'f Demand<usize>,
        ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
            <&'a mut [u8] as TrBuffTryWrite<u8>>::try_write(self.buf_, demand)
        }
    }

    impl<'b, 'a> TrBuffWrite<u8> for SliceSinkView<'b, 'a> {
        type WriteAsync<'f>
            = <&'a mut [u8] as TrBuffWrite<u8>>::WriteAsync<'f>
        where
            Self: 'f;

        fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
            <&'a mut [u8] as TrBuffWrite<u8>>::write_async(self.buf_, demand)
        }
    }

    impl<'b, 'a> TrBuffTryFlush<u8> for SliceSinkView<'b, 'a> {
        fn try_flush(&mut self) -> Result<(), Self::Err> {
            <&'a mut [u8] as TrBuffTryFlush<u8>>::try_flush(self.buf_)
        }

        fn close(&mut self) -> Result<(), Self::Err> {
            <&'a mut [u8] as TrBuffTryFlush<u8>>::close(self.buf_)
        }
    }

    impl<'b, 'a> TrBuffFlush<u8> for SliceSinkView<'b, 'a> {
        type FlushAsync<'f>
            = <&'a mut [u8] as TrBuffFlush<u8>>::FlushAsync<'f>
        where
            Self: 'f;

        type CloseAsync<'f>
            = <&'a mut [u8] as TrBuffFlush<u8>>::CloseAsync<'f>
        where
            Self: 'f;

        fn flush_async(&mut self) -> Self::FlushAsync<'_> {
            <&'a mut [u8] as TrBuffFlush<u8>>::flush_async(self.buf_)
        }

        fn close_async(&mut self) -> Self::CloseAsync<'_> {
            <&'a mut [u8] as TrBuffFlush<u8>>::close_async(self.buf_)
        }
    }

    impl<'a> TrSuffixWrite for SliceSink<'a> {
        type Writer<'b>
            = SliceSinkView<'b, 'a>
        where
            Self: 'b;

        fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError> {
            Result::Ok(SliceSinkView {
                buf_: &mut self.buf_,
            })
        }
    }

    impl<'a> TrSuffixFinish<'a> for SliceSink<'a> {
        type FinishOutput = usize;

        fn finish_async(self) -> StreamFuture<'a, usize> {
            let used = self.used();
            StreamFuture::new(async move { Result::Ok(used) })
        }
    }

    /// 从一段 MessagePack 字节里依次解出两个 `String`。
    async fn decode_two_(bytes: &[u8]) -> (String, String) {
        let mut src: &[u8] = bytes;
        let first = String::decode_async(MpDecoder::new(&mut src))
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("解第一个值");
        let second = String::decode_async(MpDecoder::new(&mut src))
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("解第二个值");
        (first, second)
    }

    /// 测试 `Slot::set_async` 直接编码进底层 buff，`finish_async` 交出已写长度。
    /// Test that `Slot::set_async` encodes into the underlying buff and `finish_async`
    /// reports the written length.
    ///
    /// - 手段：用固定切片作底层 buff，依次 set 两个字符串，finish 后用长度解码。
    /// - 判断：字节可依次解出 `alpha` / `beta`，且 finish 返回的长度与内容一致。
    ///
    /// - Means: use a fixed slice as the underlying buff, set `alpha` then `beta`, and
    ///   decode using the length returned by finish.
    /// - Verdict: the bytes decode to `alpha` and `beta` in order and the length matches.
    #[test]
    fn set_encodes_in_order_() {
        let mut storage = [0u8; 4096usize];
        let sink = SliceSink::new(&mut storage);
        let mut flux = TypedSlotFlux::<_, String>::new(sink);
        let mut iter = block_on(flux.pull_async());
        block_on(
            block_on(iter.next_async())
                .expect("第一个槽")
                .set_async("alpha".to_string()),
        )
        .expect("写第一个值");
        block_on(
            block_on(iter.next_async())
                .expect("第二个槽")
                .set_async("beta".to_string()),
        )
        .expect("写第二个值");
        let used = block_on(iter.finish_async()).expect("finish");
        drop(flux);
        let (first, second) = block_on(decode_two_(&storage[..used]));
        assert_eq!(first, "alpha");
        assert_eq!(second, "beta");
    }

    /// 测试未 set 就再次 `next_async` 返回 `GuardPending`。
    /// Test that `next_async` before `set` returns `GuardPending`.
    ///
    /// - 手段：取出第一个槽后不 set，直接再取下一个。
    /// - 判断：第二次 `next_async` 返回 `Err(GuardPending)`。
    ///
    /// - Means: take the first slot without setting it, then request another.
    /// - Verdict: the second `next_async` returns `Err(GuardPending)`.
    #[test]
    fn slot_must_be_consumed_() {
        let mut storage = [0u8; 4096usize];
        let sink = SliceSink::new(&mut storage);
        let mut flux = TypedSlotFlux::<_, String>::new(sink);
        let mut iter = block_on(flux.pull_async());
        let first = block_on(iter.next_async()).expect("第一个槽");
        assert_eq!(
            block_on(iter.next_async()).unwrap_err(),
            StreamError::GuardPending
        );
        drop(first);
        let _ = block_on(iter.finish_async());
    }

    /// 测试未 set 的槽 drop 会中止流，但 `finish_async` 仍能半关闭写端。
    /// Test that dropping an unset slot aborts the stream, while `finish_async` still
    /// closes the writer.
    ///
    /// - 手段：取出槽后 drop，再 `next_async`，最后 `finish_async`。
    /// - 判断：`next_async` 返回 `Err(Aborted)`；finish 返回 0（没有写入）。
    ///
    /// - Means: take and drop a slot, call `next_async`, then `finish_async`.
    /// - Verdict: `next_async` returns `Err(Aborted)`; finish reports 0 written bytes.
    #[test]
    fn dropping_slot_aborts_but_finishes_() {
        let mut storage = [0u8; 4096usize];
        let sink = SliceSink::new(&mut storage);
        let mut flux = TypedSlotFlux::<_, String>::new(sink);
        let mut iter = block_on(flux.pull_async());
        drop(block_on(iter.next_async()).expect("槽"));
        assert_eq!(
            block_on(iter.next_async()).unwrap_err(),
            StreamError::Aborted
        );
        let used = block_on(iter.finish_async()).expect("finish");
        assert_eq!(used, 0usize);
    }
}
