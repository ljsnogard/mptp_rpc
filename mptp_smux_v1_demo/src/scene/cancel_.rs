//! 场景层自己的取消信号：可触发、可克隆、支持多个等待者。
//!
//! `abs_cancel` 只提供两个极端（永远已取消 / 永不取消），而慢订阅者策略需要「服务端
//! 主动取消某一条订阅的后缀写入」。这里因此给出一条最小实现：
//!
//! - [`SceneCanceller`] 是触发端，持有它的业务方调用 [`SceneCanceller::cancel_`]；
//! - [`SceneCancelToken`] 是等待端，实现 [`TrCancellationToken`]，可以交给
//!   `mptp_smux` 的 `.may_cancel_with(..)`。
//!
//! 触发端与等待端共享一个 `AtomicBool` 与一条 `oneshot` 通知。等待端自己也持有一份
//! 发送端，因此「触发端被丢弃」不会被误判成「已取消」。
//!
//! # `cancel_`: a small reusable cancellation signal
//!
//! `abs_cancel` only ships the two extremes. The slow-subscriber policy needs a real
//! triggerable token, so this module provides one in the business layer; it never touches
//! transport internals.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use abs_cancel::TrCancellationToken;
use futures::{
    channel::oneshot,
    future::{FutureExt, Shared},
};

/// 取消信号的一次性通知。
type Notify = Shared<oneshot::Receiver<()>>;

/// 触发端与等待端共享的发送槽。
type SendSlot = Arc<Mutex<Option<oneshot::Sender<()>>>>;

/// 取消触发端。
pub struct SceneCanceller {
    /// 是否已经取消。
    cancelled_: Arc<AtomicBool>,

    /// 一次性通知的发送槽。
    send_: SendSlot,
}

/// 取消等待端。
#[derive(Clone)]
pub struct SceneCancelToken {
    /// 是否已经取消。
    cancelled_: Arc<AtomicBool>,

    /// 一次性通知（可被多个等待者共享）。
    notify_: Notify,

    /// 保活发送槽：只要 token 还在，触发端被丢弃也不会让等待者提前就绪。
    _send_: SendSlot,
}

/// 造一对触发端 / 等待端。
pub fn cancel_pair_() -> (SceneCanceller, SceneCancelToken) {
    let (tx, rx) = oneshot::channel();
    let cancelled_ = Arc::new(AtomicBool::new(false));
    let send_ = Arc::new(Mutex::new(Some(tx)));
    let notify_ = rx.shared();
    (
        SceneCanceller {
            cancelled_: cancelled_.clone(),
            send_: send_.clone(),
        },
        SceneCancelToken {
            cancelled_,
            notify_,
            _send_: send_,
        },
    )
}

impl SceneCanceller {
    /// 触发取消：先置位，再唤醒全部等待者；幂等。
    pub fn cancel_(&self) {
        self.cancelled_.store(true, Ordering::SeqCst);
        let sender = self
            .send_
            .lock()
            .ok()
            .and_then(|mut slot| slot.take());
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }

    /// 是否已经触发。
    pub fn is_cancelled_(&self) -> bool {
        self.cancelled_.load(Ordering::SeqCst)
    }
}

impl TrCancellationToken for SceneCancelToken {
    type Cancellation = Notify;

    type ChildToken = SceneCancelToken;

    fn is_cancelled(&self) -> bool {
        self.cancelled_.load(Ordering::SeqCst)
    }

    fn can_be_cancelled(&self) -> bool {
        true
    }

    fn child_token(&self) -> Self::ChildToken {
        self.clone()
    }

    fn cancellation(self) -> Self::Cancellation {
        self.notify_
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use core::future::Future;
    use core::pin::pin;
    use core::task::{Context, Poll, Waker};

    /// 测试取消后所有克隆都能同步看到状态。
    /// Test that every clone observes the cancelled state synchronously.
    ///
    /// - 手段：造一对信号，克隆等待端，触发取消，再读两个 token 的 `is_cancelled`。
    /// - 判断：两个都是 `true`——状态位共享，不存在「只取消了我这一份」。
    ///
    /// - Means: create a pair, clone the token, trigger cancellation, then read
    ///   `is_cancelled` on both tokens.
    /// - Verdict: both are `true`; the flag is shared, so no clone is silently left alive.
    #[test]
    fn cancel_is_shared_across_clones_() {
        let (canceller, token) = cancel_pair_();
        let clone = token.child_token();
        assert!(!token.is_cancelled());
        canceller.cancel_();
        assert!(token.is_cancelled());
        assert!(clone.is_cancelled());
    }

    /// 测试取消通知在已经触发后仍然立刻就绪。
    /// Test that the cancellation future is immediately ready once triggered.
    ///
    /// - 手段：先触发取消，再构造 `cancellation()` 并轮询一次。
    /// - 判断：第一次 poll 就是 `Ready`——不要求等待者必须早于触发者登记。
    ///
    /// - Means: trigger cancellation first, then build `cancellation()` and poll once.
    /// - Verdict: the first poll is `Ready`; waiters do not have to register before the
    ///   trigger.
    #[test]
    fn cancellation_after_trigger_is_ready_() {
        let (canceller, token) = cancel_pair_();
        canceller.cancel_();
        let mut wait = pin!(token.cancellation());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(_)));
    }
}
