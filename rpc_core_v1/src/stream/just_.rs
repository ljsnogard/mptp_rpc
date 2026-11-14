//! 读方向：`Just<T>` 守卫与 [`TypedJustFlux`]。
//!
//! `next_async` 解出**当前值**并交出一个一次性守卫；守卫被 `take` 之前不允许下一次
//! `next_async`，未 `take` 就被丢弃则把整条流标记为中止。这样既满足「不能静默跳过」，
//! 又不需要把 `TrAsyncIterator::Item` 改成 GAT。
//!
//! # `just_`: the read-side `Just<T>` guard and [`TypedJustFlux`]
//!
//! `next_async` decodes the current value and hands out a one-shot guard. The next item is
//! rejected until the guard is taken; dropping an untaken guard aborts the whole stream.

use alloc::{rc::Rc, string::ToString};
use core::{
    cell::RefCell,
    future::{Ready, ready},
    marker::PhantomData,
};

use abs_async_iter::{TrAsyncIterator, TrFlux};
use abs_buff::{TrBuffRead, buffer::TrConsumerState};
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_denco::de::TrDecodableOwned;
use mp_denco::MpDecoder;

use super::error_::ready_async_;
use crate::{
    stream::{StreamError, StreamFuture},
    x_deps::{abs_async_iter, abs_buff, abs_cancel, abs_denco, mp_denco},
};

/// 一条读流的共享守卫状态。
#[derive(Debug, Default)]
struct GuardState_ {
    /// 是否已经交出一个未消费的守卫。
    guard_: bool,

    /// 是否被未消费的守卫 / 被丢弃的 future 中止。
    aborted_: bool,

    /// 是否正常耗尽。
    finished_: bool,
}

/// 共享守卫状态句柄。
type SharedState_ = Rc<RefCell<GuardState_>>;

/// 读方向的一次性守卫。
///
/// 它绑定一条已经解出当前值的读流：`next_async` 返回它，`take` 之后才允许下一次
/// `next_async`。`Just` 带 `'f` 生命周期标记，不能存进 `'static`；它内部用共享状态
/// （不是 `&mut reader`）与底层流绑定，因此不需要自引用或 unsafe。
///
/// # Examples
///
/// ```compile_fail
/// // `Just<'f, T>` 带流生命周期，不能逃逸到 `'static`。
/// use rpc_core_v1::stream::Just;
///
/// fn require_static<T: 'static>(_: T) {}
///
/// fn escape<'f>(just: Just<'f, String>) {
///     require_static(just);
/// }
/// ```
///
/// `take` / `take_async` 的使用见 [`TypedJustFlux`] 的示例。
pub struct Just<'f, T> {
    /// 当前值；`take` 之后为 `None`。
    value_: Option<T>,

    /// 与迭代器共享的守卫状态。
    state_: SharedState_,

    /// 生命周期 / 动量标记：`Just` 不能越过 `'f`。
    _borrow_: PhantomData<&'f mut ()>,
}

impl<'f, T: 'f> Just<'f, T> {
    /// 借用当前值（不消费守卫）。
    pub fn value(&self) -> Option<&T> {
        self.value_.as_ref()
    }

    /// 取走当前值并提交本次读取。
    ///
    /// 消费守卫，因此重复 `take` 在类型层被禁止。
    pub fn take(mut self) -> T {
        let value = self
            .value_
            .take()
            .expect("Just::take 只能调用一次，且守卫构造时必有值");
        self.state_.borrow_mut().guard_ = false;
        value
    }

    /// 与 [`take`](Self::take) 相同，但返回可取消 future，方便与目标调用形状对齐。
    pub fn take_async(self) -> StreamFuture<'f, T> {
        StreamFuture::new(async move { ready_async_(self.take()).await })
    }
}

impl<T: core::fmt::Debug> core::fmt::Debug for Just<'_, T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Just")
            .field("value", &self.value_)
            .finish_non_exhaustive()
    }
}

impl<T> Drop for Just<'_, T> {
    fn drop(&mut self) {
        let mut state = self.state_.borrow_mut();
        if self.value_.is_some() {
            // 未 take 就丢弃：整条流中止，不静默跳过。
            state.aborted_ = true;
        }
        state.guard_ = false;
    }
}

/// 在任意 `TrBuffRead<u8>` 之上逐项解码的读 flux。
///
/// `R` 是底层字节读口（客户端 `ChannelRx`、服务端 `RequestSuffixRead`、测试用
/// `&[u8]`……）；`T` 是 denco 可解码的业务类型。一条 flux 的所有 item 必须是同一个
/// `T`——类型头只在前缀出现一次。
///
/// # Examples
///
/// ```ignore
/// use rpc_core_v1::stream::TypedJustFlux;
/// use rpc_core_v1::preludes::*;
///
/// async fn consume<R>(reader: R) -> Result<(), Box<dyn core::error::Error>>
/// where
///     R: abs_buff::TrBuffRead<u8> + abs_buff::buffer::TrConsumerState,
/// {
///     let mut flux = TypedJustFlux::<_, String>::new(reader);
///     let mut iter = flux.pull_async().await?;
///     while let Ok(just) = iter.next_async().await {
///         let value: String = just.take();
///         let _ = value;
///     }
///     Ok(())
/// }
/// ```
pub struct TypedJustFlux<'f, R, T> {
    /// 底层读口；守卫推进后放回。
    reader_: Rc<RefCell<Option<R>>>,

    /// 共享守卫状态。
    state_: SharedState_,

    /// 生命周期与 `T` 的动量标记。
    _marker_: super::Marker_<'f, T>,
}

impl<'f, R, T> TypedJustFlux<'f, R, T>
where
    R: 'f,
    T: 'f,
{
    /// 在给定读口上构造 flux。
    pub fn new(reader: R) -> Self {
        TypedJustFlux {
            reader_: Rc::new(RefCell::new(Option::Some(reader))),
            state_: Rc::new(RefCell::new(GuardState_::default())),
            _marker_: PhantomData,
        }
    }

    /// 流是否已经结束或中止。
    pub fn is_finished(&self) -> bool {
        let state = self.state_.borrow();
        state.finished_ || state.aborted_
    }
}

impl<'f, R, T> TrFlux<Just<'f, T>> for TypedJustFlux<'f, R, T>
where
    R: TrBuffRead<u8> + TrConsumerState + 'f,
    T: TrDecodableOwned + 'f,
{
    type Iter<'x>
        = JustIter<'f, R, T>
    where
        Self: 'x;

    type PullAsync<'x>
        = Ready<JustIter<'f, R, T>>
    where
        Self: 'x;

    fn pull_async(&mut self) -> Self::PullAsync<'_> {
        ready(JustIter {
            reader_: Rc::clone(&self.reader_),
            state_: Rc::clone(&self.state_),
            _marker_: PhantomData,
        })
    }
}

/// [`TypedJustFlux`] 的迭代器：每条 `next_async` 解出一个值并交出 [`Just`]。
pub struct JustIter<'f, R, T> {
    /// 与 flux 共享的读口。
    reader_: Rc<RefCell<Option<R>>>,

    /// 与 flux 共享的守卫状态。
    state_: SharedState_,

    /// 生命周期与 `T` 的动量标记。
    _marker_: super::Marker_<'f, T>,
}

impl<'f, R, T> TrAsyncIterator for JustIter<'f, R, T>
where
    R: TrBuffRead<u8> + TrConsumerState + 'f,
    T: TrDecodableOwned + 'f,
{
    type Item = Just<'f, T>;

    type Err = StreamError;

    type NextAsync<'x>
        = StreamFuture<'x, Just<'f, T>>
    where
        Self: 'x;

    fn next_async(&mut self) -> Self::NextAsync<'_> {
        let reader = Rc::clone(&self.reader_);
        let state = Rc::clone(&self.state_);
        StreamFuture::new(async move { just_next_async_(reader, state).await })
    }
}

/// 在共享状态上放一个「future 被丢弃则中止」的保险丝。
struct AbortOnDrop_ {
    /// 共享守卫状态。
    state_: SharedState_,

    /// `false` 表示正常完成，Drop 不再改状态。
    armed_: bool,
}

impl Drop for AbortOnDrop_ {
    fn drop(&mut self) {
        if self.armed_ {
            self.state_.borrow_mut().aborted_ = true;
        }
    }
}

/// 解出下一个值；不满足「上一个守卫已消费」时返回明确错误。
async fn just_next_async_<'f, R, T>(
    reader_: Rc<RefCell<Option<R>>>,
    state: SharedState_,
) -> Result<Just<'f, T>, StreamError>
where
    R: TrBuffRead<u8> + TrConsumerState + 'f,
    T: TrDecodableOwned + 'f,
{
    {
        let guard = state.borrow();
        if guard.aborted_ {
            return Result::Err(StreamError::Aborted);
        }
        if guard.guard_ {
            return Result::Err(StreamError::GuardPending);
        }
        if guard.finished_ {
            return Result::Err(StreamError::EndOfStream);
        }
    }

    let mut reader = reader_.borrow_mut().take().ok_or(StreamError::Transport(
        "读流已不在 flux 中（可能被丢弃）".to_string(),
    ))?;

    let mut abort = AbortOnDrop_ {
        state_: Rc::clone(&state),
        armed_: true,
    };

    let decoded = T::decode_async(MpDecoder::new(&mut reader))
        .may_cancel_with(NonCancellableToken::new())
        .await;
    let value = match decoded {
        Result::Ok(value) => value,
        Result::Err(err) => {
            // 解码失败：流已经不可继续，按结束处理；reader 随局部变量 drop 关闭。
            abort.armed_ = false;
            state.borrow_mut().finished_ = true;
            return Result::Err(StreamError::Decode(err.to_string()));
        }
    };

    // 先把 reader 放回、再立守卫：这中间没有 await，不会被打断。
    *reader_.borrow_mut() = Option::Some(reader);
    {
        let mut guard = state.borrow_mut();
        guard.guard_ = true;
    }
    abort.armed_ = false;
    Result::Ok(Just {
        value_: Option::Some(value),
        state_: Rc::clone(&state),
        _borrow_: PhantomData,
    })
}

#[cfg(test)]
mod tests_ {
    use alloc::{string::String, vec, vec::Vec};

    use abs_cancel::NonCancellableToken;
    use abs_denco::ser::TrEncodable;
    use futures::executor::block_on;
    use mp_denco::MpEncoder;

    use super::*;

    /// 把两个 `String` 依次编码进一段字节。
    async fn encode_two_(a: &String, b: &String) -> Vec<u8> {
        let mut storage = vec![0u8; 4096usize];
        let used = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            a.encode_async(MpEncoder::new(&mut sink))
                .may_cancel_with(NonCancellableToken::new())
                .await
                .expect("编码第一个值");
            b.encode_async(MpEncoder::new(&mut sink))
                .may_cancel_with(NonCancellableToken::new())
                .await
                .expect("编码第二个值");
            4096usize - sink.len()
        };
        storage.truncate(used);
        storage
    }

    /// 测试 `next_async` → `Just` → `take` 的顺序读取。
    /// Test ordered reads through `next_async` → `Just` → `take`.
    ///
    /// - 手段：把两个字符串编码成字节，构造 `TypedJustFlux<_, String>`，依次 take。
    /// - 判断：两个值按编码顺序返回；第三次 `next_async` 以解码错误结束（EOF）。
    ///
    /// - Means: encode two strings, build a `TypedJustFlux<_, String>`, and take twice.
    /// - Verdict: values arrive in order; the third `next_async` ends with a decode error.
    #[test]
    fn take_advances_in_order_() {
        let bytes = block_on(encode_two_(&"alpha".to_string(), &"beta".to_string()));
        let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
        let mut iter = block_on(flux.pull_async());

        let first = block_on(iter.next_async()).expect("第一个值");
        assert_eq!(first.take(), "alpha");
        let second = block_on(iter.next_async()).expect("第二个值");
        assert_eq!(second.take(), "beta");
        assert!(block_on(iter.next_async()).is_err(), "流应当结束");
        assert!(flux.is_finished());
    }

    /// 测试未 take 就 `next_async` 返回 `GuardPending`，且不会预取下一个值。
    /// Test that `next_async` without a `take` returns `GuardPending` and does not prefetch.
    ///
    /// - 手段：take 第一个值后保留第二个守卫，直接再 `next_async`。
    /// - 判断：返回 `Err(GuardPending)`；随后 take 第二个值仍然得到 `beta`。
    ///
    /// - Means: take the first value, keep the second guard, and call `next_async` again.
    /// - Verdict: it returns `Err(GuardPending)`, and taking the second guard still yields
    ///   `beta`.
    #[test]
    fn guard_must_be_consumed_() {
        let bytes = block_on(encode_two_(&"alpha".to_string(), &"beta".to_string()));
        let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
        let mut iter = block_on(flux.pull_async());

        block_on(iter.next_async()).expect("第一个值").take();
        let second = block_on(iter.next_async()).expect("第二个值");
        assert_eq!(
            block_on(iter.next_async()).unwrap_err(),
            StreamError::GuardPending
        );
        assert_eq!(second.take(), "beta");
    }

    /// 测试未 take 的守卫 drop 会中止整条流，而不是静默跳过。
    /// Test that dropping an untaken guard aborts the stream instead of skipping silently.
    ///
    /// - 手段：解出第一个值后直接 drop 守卫，再 `next_async`。
    /// - 判断：返回 `Err(Aborted)`，且 `is_finished` 为真。
    ///
    /// - Means: decode the first value, drop its guard, then call `next_async`.
    /// - Verdict: it returns `Err(Aborted)` and `is_finished` is true.
    #[test]
    fn dropping_guard_aborts_() {
        let bytes = block_on(encode_two_(&"alpha".to_string(), &"beta".to_string()));
        let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
        let mut iter = block_on(flux.pull_async());

        drop(block_on(iter.next_async()).expect("第一个值"));
        assert_eq!(
            block_on(iter.next_async()).unwrap_err(),
            StreamError::Aborted
        );
        assert!(flux.is_finished());
    }

    /// 测试截断字节被映射成明确的解码错误。
    /// Test that truncated bytes map to an explicit decode error.
    ///
    /// - 手段：只给编码结果的前 3 个字节。
    /// - 判断：`next_async` 返回 `Err(StreamError::Decode(_))`，不会返回空值。
    ///
    /// - Means: feed only the first 3 bytes of an encoded value.
    /// - Verdict: `next_async` returns `Err(StreamError::Decode(_))`, never an empty value.
    #[test]
    fn truncated_value_is_decode_error_() {
        let bytes = block_on(encode_two_(&"alpha".to_string(), &"beta".to_string()));
        let mut flux = TypedJustFlux::<_, String>::new(&bytes[..3]);
        let mut iter = block_on(flux.pull_async());
        assert!(matches!(
            block_on(iter.next_async()),
            Result::Err(StreamError::Decode(_))
        ));
    }
}
