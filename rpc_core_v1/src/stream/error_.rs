//! 流守卫的错误与可取消 future。
//!
//! 类型化流的每一次 `next` / `take` / `set` 都返回 [`StreamFuture`]：它既可以直接
//! `.await`（不取消），也可以先 `.may_cancel_with(token)` 再 `.await`。取消信号在
//! 每次 poll 时同时检查已触发标志与取消 future，因此不会「只能等到底层 I/O 自己醒」。
//!
//! # `error_`: stream errors and the cancellable future
//!
//! Every typed-stream step returns [`StreamFuture`], which is awaitable directly or via
//! `.may_cancel_with(token)`. Cancellation is checked on every poll through both the
//! already-cancelled flag and the token's cancellation future.

use alloc::{
    boxed::Box,
    format,
    string::{String, ToString},
};
use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use mptp_smux::ServeError;

use crate::x_deps::abs_cancel;

/// 类型化流 / 守卫的错误。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamError {
    /// 上一个 `Just` / `Slot` 还没被消费，不能推进下一个。
    ///
    /// 这是**调用顺序错误**，不是流本身的错误；它防止「未 take 就静默跳过」。
    GuardPending,

    /// 上一个守卫被丢弃而未消费，流已经进入中止状态。
    Aborted,

    /// 流正常结束（对端半关闭 / 有限流耗尽）。
    ///
    /// 当前实现里 EOF 由 denco 解码报错暴露，因此本变体保留给显式结束路径；
    /// 调用者应把 `Decode` 与 `EndOfStream` 都视为「流结束」。
    EndOfStream,

    /// 业务值无法从字节流解码。
    Decode(String),

    /// 业务值无法编码成字节。
    Encode(String),

    /// 底层字节流 / 会话的读写失败。
    Transport(String),

    /// 等待期间被取消。
    Cancelled,
}

impl core::fmt::Display for StreamError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            StreamError::GuardPending => {
                write!(f, "上一个流守卫尚未被消费，不能推进下一个")
            }
            StreamError::Aborted => write!(f, "流守卫被丢弃，流已中止"),
            StreamError::EndOfStream => write!(f, "流已正常结束"),
            StreamError::Decode(detail) => write!(f, "流值解码失败：{detail}"),
            StreamError::Encode(detail) => write!(f, "流值编码失败：{detail}"),
            StreamError::Transport(detail) => write!(f, "流传输失败：{detail}"),
            StreamError::Cancelled => write!(f, "流操作已被取消"),
        }
    }
}

impl core::error::Error for StreamError {}

impl From<ServeError> for StreamError {
    fn from(err: ServeError) -> Self {
        match err {
            ServeError::Cancelled => StreamError::Cancelled,
            other => StreamError::Transport(other.to_string()),
        }
    }
}

impl From<mptp_smux::ClientError> for StreamError {
    fn from(err: mptp_smux::ClientError) -> Self {
        match err {
            mptp_smux::ClientError::Cancelled => StreamError::Cancelled,
            other => StreamError::Transport(other.to_string()),
        }
    }
}

impl From<crate::messaging::RpcError> for StreamError {
    fn from(err: crate::messaging::RpcError) -> Self {
        match err {
            crate::messaging::RpcError::Cancelled => StreamError::Cancelled,
            crate::messaging::RpcError::Encode(detail) => StreamError::Encode(detail),
            crate::messaging::RpcError::Decode(detail) => StreamError::Decode(detail),
            crate::messaging::RpcError::Transport(detail) => StreamError::Transport(detail),
            other => StreamError::Transport(format!("{other}")),
        }
    }
}

/// 一个类型擦除、可取消的流操作 future。
///
/// 通过 [`StreamFuture::new`] 构造时它不可取消；调用
/// [`TrMayCancel::may_cancel_with`] 后，每次 poll 先检查取消信号，再推进内部 future。
/// 内部 future、令牌与取消 future 都装进 `Pin<Box<_>>`，因此 `StreamFuture` 本身永远
/// `Unpin`，不需要给调用者增加 `C: Unpin` 约束。
///
/// `CF` 是取消 future 的类型参数：默认值对应不可取消令牌，`may_cancel_with` 会把它换成
/// 具体令牌的 child token 取消 future。这样即便 `C` 不实现 `Clone` / `Unpin`，本类型也能
/// 无条件实现 `Future` 与 `TrMayCancel`。
///
/// # 取消语义
///
/// - 取消信号已触发：立即返回 [`StreamError::Cancelled`]，不再 poll 内部 future；
/// - 取消 future 先就绪：同上；
/// - 内部 future 先就绪：返回其结果，之后不再检查取消。
///
/// 若调用者在操作中途丢弃本 future，内部 future 随 Box 一起 drop；守卫的 `Drop`
/// 负责把流标记为中止（或让 writer 被丢弃从而半关闭），不会静默跳过。
pub struct StreamFuture<
    'a,
    T,
    C = NonCancellableToken,
    CF = <NonCancellableToken as TrCancellationToken>::Cancellation,
> where
    C: TrCancellationToken,
    CF: Future,
{
    /// 真正的流操作。
    inner_: Pin<Box<dyn Future<Output = Result<T, StreamError>> + 'a>>,

    /// 取消令牌；缺省不可取消。
    cancel_: Pin<Box<C>>,

    /// 取消 future；不可取消时为空。
    cancel_fut_: Option<Pin<Box<CF>>>,
}

impl<'a, T> StreamFuture<'a, T> {
    /// 用一个不可取消的内部 future 构造。
    pub fn new(fut: impl Future<Output = Result<T, StreamError>> + 'a) -> Self {
        StreamFuture {
            inner_: Box::pin(fut),
            cancel_: Box::pin(NonCancellableToken::new()),
            cancel_fut_: Option::None,
        }
    }
}

impl<'a, T, C, CF> TrMayCancel<'a> for StreamFuture<'a, T, C, CF>
where
    T: 'a,
    C: TrCancellationToken + 'a,
    CF: Future + 'a,
{
    type MayCancelFuture<'f, D>
        = StreamFuture<'a, T, D, <D::ChildToken as TrCancellationToken>::Cancellation>
    where
        'f: 'a,
        Self: 'f,
        D: 'f + TrCancellationToken;

    type MayCancelOutput = Result<T, StreamError>;

    fn may_cancel_with<D>(self, cancel: D) -> Self::MayCancelFuture<'a, D>
    where
        D: 'a + TrCancellationToken,
    {
        let StreamFuture { inner_, .. } = self;
        let cancel_fut = cancel.child_token().cancellation();
        StreamFuture {
            inner_,
            cancel_: Box::pin(cancel),
            cancel_fut_: Option::Some(Box::pin(cancel_fut)),
        }
    }
}

impl<'a, T, C, CF> Future for StreamFuture<'a, T, C, CF>
where
    T: 'a,
    C: TrCancellationToken + 'a,
    CF: Future + 'a,
{
    type Output = Result<T, StreamError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.cancel_.as_ref().get_ref().is_cancelled() {
            return Poll::Ready(Result::Err(StreamError::Cancelled));
        }
        if let Option::Some(cancel_fut) = this.cancel_fut_.as_mut()
            && cancel_fut.as_mut().poll(cx).is_ready()
        {
            return Poll::Ready(Result::Err(StreamError::Cancelled));
        }
        this.inner_.as_mut().poll(cx)
    }
}

/// 供 `StreamFuture` 内部使用：一个立即返回给定值的 future。
pub(crate) async fn ready_async_<T>(value: T) -> Result<T, StreamError> {
    Result::Ok(value)
}

#[cfg(test)]
mod tests_ {
    use futures::executor::block_on;

    use super::*;

    /// 测试不可取消的 `StreamFuture` 直接 await 得到内部结果。
    /// Test that an uncancellable `StreamFuture` yields the inner result.
    ///
    /// - 手段：用一个返回 `42u64` 的 async block 构造 `StreamFuture`。
    /// - 判断：`block_on` 结果为 `Ok(42)`。
    ///
    /// - Means: build a `StreamFuture` over an async block returning `42u64`.
    /// - Verdict: `block_on` yields `Ok(42)`.
    #[test]
    fn stream_future_uncancellable_() {
        let fut = StreamFuture::new(async { Result::Ok(42u64) });
        assert_eq!(block_on(fut), Result::Ok(42u64));
    }

    /// 测试已触发的取消令牌让 `StreamFuture` 返回 `Cancelled`。
    /// Test that an already-cancelled token makes `StreamFuture` return `Cancelled`.
    ///
    /// - 手段：用 `abs_cancel::CancelledToken`（永远已取消）作为令牌，再
    ///   `may_cancel_with` 并 await。
    /// - 判断：返回 `Err(StreamError::Cancelled)`，内部 future 未被轮询到完成。
    ///
    /// - Means: use `abs_cancel::CancelledToken` (always cancelled), then
    ///   `may_cancel_with` and await.
    /// - Verdict: it returns `Err(StreamError::Cancelled)`.
    #[test]
    fn stream_future_cancelled_() {
        let fut = StreamFuture::new(async { Result::Ok(1u64) })
            .may_cancel_with(abs_cancel::CancelledToken::new());
        assert_eq!(block_on(fut), Result::Err(StreamError::Cancelled));
    }
}
