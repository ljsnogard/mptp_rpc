//! 请求级 arena future：把 future 放进会话 allocator，并在轮询时保持其位置固定。
//!
//! 直接用 `Pin<Box<dyn Future, &A>>` 在当前 nightly 上会要求 `A: StaticAllocator`；
//! 而会话 allocator（`SessionAlloc`）的析构**会**让分配块失效，因此不能虚假实现
//! `StaticAllocator`。本模块用一个自定义 [`ArenaFuture`] 绕开这个约束：
//!
//! - 内层是普通的 `Box<dyn Future, &'a A>`（不需要 `Pin`）；
//! - [`ArenaFuture`] 自己 `Unpin`，轮询时把内层 future 以 `Pin::new_unchecked` 固定在其
//!   堆地址上；盒子本身移动不会移动 future；
//! - 盒子析构时按同一个 allocator 归还，因此请求结束（或分配器释放）时内存可以回收。
//!
//! # `arena_`: request-scoped arena futures
//!
//! A custom `ArenaFuture` avoids the nightly `StaticAllocator` requirement of
//! `Pin<Box<dyn Future, &A>>`; the inner future is boxed normally and pinned at its heap
//! address when polled.

use alloc::boxed::Box;
use core::{
    alloc::Allocator,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// 分配在 `&A` 上的类型擦除 future。
pub struct ArenaFuture<'a, A, O>
where
    A: Allocator,
{
    /// 内层 future；盒子本身移动不会移动堆上的 future。
    inner_: Box<dyn Future<Output = O> + 'a, &'a A>,
}

impl<A, O> Future for ArenaFuture<'_, A, O>
where
    A: Allocator,
{
    type Output = O;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<O> {
        // `ArenaFuture` 只持有一个 `Box`，所以它是 `Unpin`；拿到 `&mut self` 后把内层
        // future 固定在它的堆地址上。
        let this = self.get_mut();
        let inner: Pin<&mut (dyn Future<Output = O> + '_)> =
            // SAFETY: 内层 future 在 Box 的堆内存里，`ArenaFuture` 移动时不会移动它；
            // 这里只交出按 `&mut` 借用的 pinned 视图。
            unsafe { Pin::new_unchecked(&mut *this.inner_) };
        inner.poll(cx)
    }
}

/// 把 `fut` 分配进 `arena`，返回一个可在原地轮询的 [`ArenaFuture`]。
pub fn arena_future_<'a, A, F, O>(arena: &'a A, fut: F) -> ArenaFuture<'a, A, O>
where
    A: Allocator,
    F: Future<Output = O> + 'a,
{
    let boxed: Box<F, &'a A> = Box::new_in(fut, arena);
    let (raw, alloc) = Box::into_raw_with_allocator(boxed);
    let raw: *mut (dyn Future<Output = O> + 'a) = raw;
    // SAFETY: `raw` 仍指向同一个 arena box，只是把 `*mut F` 拓宽成 `*mut dyn Future`；
    // vtable 由 dyn 提供。`from_raw_in` 用同一个 allocator 重建所有权。
    let inner_ = unsafe { Box::from_raw_in(raw, alloc) };
    ArenaFuture { inner_ }
}
