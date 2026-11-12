//! 对象安全的取消句柄 [`RequestCancel`]。
//!
//! [`TrCancellationToken`] 带 GAT（`Cancellation` / `ChildToken`），不是对象安全的。为了把
//! 取消令牌放进 [`MptpContext`]（P3 后续）而不要求处理器 / 中间件泛型于具体令牌 `K`，这里
//! 提供一个**擦除**句柄：
//!
//! - `is_cancelled` / `can_be_cancelled` / `cancellation` 擦成 `Box<dyn Future>`；
//! - `child_token` 返回**共享同一底层令牌**的子句柄——子句柄随父句柄一起取消，满足「请求
//!   作用域子任务」的常见语义（真正的独立子取消源属更精细的令牌实现，本层不复制）。
//!
//! 擦除要求 `K: Clone`：`cancellation(self)` 消费 `self`，但句柄需要能被 `child_token`
//! 复用，因此每次都 `clone` 一份底层令牌再取取消 future。叶子令牌（`NonCancellableToken` /
//! `CancelledToken`）都是 `Clone` 的，可直接用。
//!
//! # `request_cancel_`: an object-safe cancellation handle
//!
//! `TrCancellationToken` has GATs and is not object-safe. `RequestCancel` erases it so it can
//! live in `MptpContext` without generic `K`. `child_token` shares the same underlying token
//! (cancelled together), which is the common request-scoped semantics. Erasure needs
//! `K: Clone` because `cancellation(self)` consumes and the handle is reusable.

use alloc::boxed::Box;
use alloc::sync::Arc;
use core::future::Future;
use core::pin::Pin;

use abs_cancel::{CancelledToken, NonCancellableToken, TrCancellationToken};

/// 被擦除的取消令牌：只有三个只读操作 + 一个取消 future。
///
/// 用 `&self` 而不是 `self` 取取消 future，便于 [`RequestCancel`] 在 `child_token` /
/// 重复取 future 时复用底层令牌。
trait ErasedToken: Send + Sync {
    /// 是否已被取消。
    fn is_cancelled(&self) -> bool;

    /// 是否可能被取消。
    fn can_be_cancelled(&self) -> bool;

    /// 取消 future（每次返回一个新的、等同一个取消信号的 future）。
    fn cancellation(&self) -> Pin<Box<dyn Future<Output = ()> + Send>>;
}

impl<K> ErasedToken for K
where
    K: TrCancellationToken + Clone + Send + Sync + 'static,
    K::Cancellation: Send + 'static,
{
    fn is_cancelled(&self) -> bool {
        TrCancellationToken::is_cancelled(self)
    }

    fn can_be_cancelled(&self) -> bool {
        TrCancellationToken::can_be_cancelled(self)
    }

    fn cancellation(&self) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let fut = TrCancellationToken::cancellation(self.clone());
        Box::pin(async move {
            let _ = fut.await;
        })
    }
}

/// 对象安全的取消句柄。
///
/// 由任意 `K: TrCancellationToken + Clone + 'static`（且 `K::Cancellation: Send + 'static`）
/// 构造；实现 [`TrCancellationToken`]，因此可以交给所有接受取消令牌的入口。
pub struct RequestCancel {
    /// 被擦除的底层令牌。
    inner_: Arc<dyn ErasedToken>,
}

impl RequestCancel {
    /// 由具体令牌构造。
    pub fn from_token<K>(token: K) -> Self
    where
        K: TrCancellationToken + Clone + Send + Sync + 'static,
        K::Cancellation: Send + 'static,
    {
        RequestCancel {
            inner_: Arc::new(token),
        }
    }

    /// 一个永不取消的句柄。
    pub fn non_cancellable() -> Self {
        RequestCancel::from_token(NonCancellableToken::new())
    }

    /// 一个已经取消的句柄。
    pub fn cancelled() -> Self {
        RequestCancel::from_token(CancelledToken::new())
    }
}

impl TrCancellationToken for RequestCancel {
    type Cancellation = Pin<Box<dyn Future<Output = ()> + Send>>;
    type ChildToken = RequestCancel;

    fn is_cancelled(&self) -> bool {
        self.inner_.is_cancelled()
    }

    fn can_be_cancelled(&self) -> bool {
        self.inner_.can_be_cancelled()
    }

    fn child_token(&self) -> Self::ChildToken {
        RequestCancel {
            inner_: self.inner_.clone(),
        }
    }

    fn cancellation(self) -> Self::Cancellation {
        self.inner_.cancellation()
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试永不取消句柄的三个只读操作。
    /// Test the three read-only operations of a non-cancellable handle.
    ///
    /// - 手段：用 `NonCancellableToken` 构造，读 `is_cancelled` / `can_be_cancelled`，并取
    ///   `child_token`。
    /// - 判断：`is_cancelled` / `can_be_cancelled` 都为 `false`，子句柄同样为 `false`。
    ///
    /// - Means: build from `NonCancellableToken`, read the flags and take a child.
    /// - Verdict: both flags are `false`, and the child is also `false`.
    #[test]
    fn non_cancellable_handle_() {
        let handle = RequestCancel::non_cancellable();
        assert!(!handle.is_cancelled());
        assert!(!handle.can_be_cancelled());

        let child = handle.child_token();
        assert!(!child.is_cancelled());
    }

    /// 测试已取消句柄的 `is_cancelled` 为真。
    /// Test that a cancelled handle reports `is_cancelled == true`.
    ///
    /// - 手段：用 `CancelledToken` 构造，读 `is_cancelled`。
    /// - 判断：为 `true`。
    ///
    /// - Means: build from `CancelledToken` and read `is_cancelled`.
    /// - Verdict: `true`.
    #[test]
    fn cancelled_handle_() {
        let handle = RequestCancel::cancelled();
        assert!(handle.is_cancelled());
    }

    /// 编译期断言：`RequestCancel` 确实实现了 `TrCancellationToken`。
    /// Compile-time assertion that `RequestCancel` implements `TrCancellationToken`.
    #[allow(dead_code)]
    fn assert_impl_() {
        fn assert_token<T: TrCancellationToken>() {}
        assert_token::<RequestCancel>();
    }
}
