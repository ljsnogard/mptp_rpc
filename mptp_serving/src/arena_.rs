//! 请求级 arena future：把 future 放进请求级 allocator，并在轮询时保持其位置固定。
//!
//! 直接用 `Pin<Box<dyn Future, &A>>` 在当前 nightly 上会要求 `A: StaticAllocator`；
//! 而 [`SessionAlloc`] 的析构**会**让分配块失效，因此不能虚假实现
//! `StaticAllocator`。本模块用一个自定义 [`ArenaFuture`] 绕开这个约束：
//!
//! - 内层是普通的 `Box<dyn Future, ArenaBoxAlloc>`（不需要 `Pin`）；
//! - [`ArenaFuture`] 自己 `Unpin`，轮询时把内层 future 以 `Pin::new_unchecked` 固定在其
//!   堆地址上；盒子本身移动不会移动 future；
//! - 盒子析构时按同一个 allocator 归还，因此请求结束、句柄 drop 时内存可以回收。
//!
//! # 超大 future 的兜底
//!
//! `BumpListAlloc` 的一个块不能跨页，因此单块上限是该页的可用容量。handler / 中间件
//! 的 async 块由编译器生成，大小可能超过一页（尤其是带多个局部变量的业务链）。装箱
//! 前先向 arena **探测**一次：放得下就进 arena；放不下回退到 [`Global`]，避免整个
//! 请求因为 future 太大而 abort。回退只影响这一块内存，语义不变。
//!
//! # `arena_`: request-scoped arena futures
//!
//! A custom `ArenaFuture` avoids the nightly `StaticAllocator` requirement of
//! `Pin<Box<dyn Future, &A>>`; the inner future is boxed normally and pinned at its heap
//! address when polled. Blocks that do not fit the arena's single-page limit fall back to
//! `Global` instead of aborting the request.

use alloc::{alloc::Global, boxed::Box};
use core::{
    alloc::{AllocError, Allocator, Layout},
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use mptp_smux::SessionAlloc;

/// 请求级 arena future 的内部 allocator：正常走 arena，超大块回退 [`Global`]。
enum ArenaBoxAlloc {
    /// 分配在请求级 arena 里。
    Arena(SessionAlloc),

    /// 单块超过 arena 页容量时的兜底。
    Global,
}

// SAFETY: `allocate` / `deallocate` 都转发给同一个变体所表示的 allocator；`Box` 按值
// 保存本枚举，因此 `deallocate` 看到的变体与分配时一致。`SessionAlloc` 与 `Global` 都
// 满足 `Allocator` 契约。
unsafe impl Allocator for ArenaBoxAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        match self {
            Self::Arena(arena) => <SessionAlloc as Allocator>::allocate(arena, layout),
            Self::Global => Global.allocate(layout),
        }
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        match self {
            Self::Arena(arena) => {
                // SAFETY: 调用方保证 ptr/layout 来自本 allocator；变体与分配时相同。
                unsafe { <SessionAlloc as Allocator>::deallocate(arena, ptr, layout) }
            }
            Self::Global => {
                // SAFETY: 调用方保证 ptr/layout 来自本 allocator；变体与分配时相同。
                unsafe { Global.deallocate(ptr, layout) }
            }
        }
    }
}

/// 分配在请求级 arena（或超大块时的 [`Global`]）上的类型擦除 future。
pub struct ArenaFuture<'a, O> {
    /// 内层 future；盒子本身移动不会移动堆上的 future。
    inner_: Box<dyn Future<Output = O> + 'a, ArenaBoxAlloc>,
}

impl<O> Future for ArenaFuture<'_, O> {
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

/// 选择这次装箱使用的 allocator：先探测 arena 能否放下一个这样的块。
fn select_alloc_(arena: SessionAlloc, layout: Layout) -> ArenaBoxAlloc {
    // SAFETY: 探测得到的块立即按同一 layout 归还；没有读取或移动未初始化内存。
    let probe = <SessionAlloc as Allocator>::allocate(&arena, layout);
    match probe {
        Result::Ok(block) => {
            let ptr = block.cast::<u8>();
            // SAFETY: ptr/layout 来自上面同一次分配，且尚未交给其它人。
            unsafe { <SessionAlloc as Allocator>::deallocate(&arena, ptr, layout) };
            ArenaBoxAlloc::Arena(arena)
        }
        Result::Err(_) => ArenaBoxAlloc::Global,
    }
}

/// 把 `fut` 分配进 `arena`，返回一个可在原地轮询的 [`ArenaFuture`]。
///
/// 单块超过 arena 页容量时回退到 [`Global`]；见模块文档。
pub fn arena_future_<'a, F, O>(arena: SessionAlloc, fut: F) -> ArenaFuture<'a, O>
where
    F: Future<Output = O> + 'a,
{
    let alloc = select_alloc_(arena, Layout::new::<F>());
    let boxed: Box<F, ArenaBoxAlloc> = Box::new_in(fut, alloc);
    let (raw, alloc) = Box::into_raw_with_allocator(boxed);
    let raw: *mut (dyn Future<Output = O> + 'a) = raw;
    // SAFETY: `raw` 仍指向同一个 arena box，只是把 `*mut F` 拓宽成 `*mut dyn Future`；
    // vtable 由 dyn 提供。`from_raw_in` 用同一个 allocator 重建所有权。
    let inner_ = unsafe { Box::from_raw_in(raw, alloc) };
    ArenaFuture { inner_ }
}
