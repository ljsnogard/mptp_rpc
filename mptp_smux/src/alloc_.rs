//! 会话 / 客户端 / 服务端各自持有的 allocation source 句柄。
//!
//! [`SessionAlloc`] 是 [`BumpListAlloc`] 的 `Arc` 句柄：它实现
//! [`Allocator`](core::alloc::Allocator)、[`AllocatorClone`]、
//! [`TrArena`] 与 [`TrArenaBytes`]，因此同一个 allocator 既能给 `Vec` /
//! `abs_str::String` / 头表用，也能直接交给 `MpDecoder::new_in`。
//!
//! # 为什么是句柄，而不是 `&'a BumpListAlloc`
//!
//! 每个持有者（`MptpClient`、`MptpSession`、`MptpServlet`、`ServingSession`、
//! `MptpContext`）需要**既拥有 arena，又把由该 arena 分配的容器保存成自己的字段**
//! （请求前缀、头表、location……）。如果字段里的 allocator 是指向结构体自身字段的
//! `&'a BumpListAlloc`，那就是自引用，安全 Rust 构造不出来。
//!
//! `Arc` 句柄把「arena 的地址」与「持有者结构体的地址」解耦：句柄按值保存、可克隆、
//! 是 `'static` 的；所有克隆共享同一个 [`BumpListAlloc`]，最后一个句柄（包括被前缀 /
//! 头表持有的克隆）释放时才回收页。于是：
//!
//! - 持有者可以按值保存 `RequestPrefix<SessionAlloc>` 这类字段，不需要生命周期参数；
//! - 会话类型保持 `'static`，`abs_mptp` 的关联类型不需要改 GAT；
//! - 每个持有者可以按自己的生命周期创建**独立的** arena：client 一份、每个 session
//!   一份、servlet 一份、每个请求的 context 一份。
//!
//! # `alloc_`: the per-owner allocation source handle
//!
//! `SessionAlloc` is an `Arc<BumpListAlloc>` handle implementing `Allocator`,
//! `AllocatorClone`, `TrArena` and `TrArenaBytes`. Each owner holds its own handle, so an
//! arena can be released when that owner (and every object allocated from it) goes away,
//! while the session types stay lifetime-free.

use alloc::sync::Arc;
use core::{
    alloc::{AllocError, Allocator, AllocatorClone, Layout},
    fmt, future,
    ptr::NonNull,
};

use abs_denco::de::{TrAllocSource, TrArena, TrArenaBytes};
use bumplist_alloc::{
    BumpListAlloc, BumpListAllocError, BumpListArenaError, BumpListBox, PageSize,
};

/// 客户端 / 会话 / 服务端共享的分配器句柄。
///
/// # Examples
///
/// ```
/// use bumplist_alloc::PageSize;
/// use mptp_smux::SessionAlloc;
///
/// let alloc = SessionAlloc::new(PageSize::P4K).expect("4 KiB 页应当受支持");
/// let clone = alloc.clone();
/// assert!(std::ptr::eq(alloc.bump(), clone.bump()));
/// ```
#[derive(Clone)]
pub struct SessionAlloc {
    /// 所有克隆共享的底层分配器。
    inner_: Arc<BumpListAlloc>,
}

impl SessionAlloc {
    /// 创建一个新的会话分配器。
    ///
    /// # Errors
    ///
    /// 页大小在当前平台不受支持时返回 [`BumpListAllocError`]。
    pub fn new(page_size: PageSize) -> Result<Self, BumpListAllocError> {
        Result::Ok(SessionAlloc {
            inner_: Arc::new(BumpListAlloc::new(page_size)?),
        })
    }

    /// 用一个已有的 [`BumpListAlloc`] 构造句柄。
    pub fn from_bump(bump: BumpListAlloc) -> Self {
        SessionAlloc {
            inner_: Arc::new(bump),
        }
    }

    /// 底层分配器的共享引用。
    pub fn bump(&self) -> &BumpListAlloc {
        &self.inner_
    }

    /// 本 arena 的页大小。
    ///
    /// 持有者用它为新作用域创建同样页大小的 arena，避免一个进程里出现多种页大小。
    pub fn page_size(&self) -> PageSize {
        self.inner_.page_size()
    }
}

impl fmt::Debug for SessionAlloc {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SessionAlloc")
            .field("page_size", &self.inner_.page_size())
            .finish()
    }
}

// SAFETY: 所有方法都转发给 `&BumpListAlloc` 的 `Allocator` 实现；`SessionAlloc` 的
// `Arc` 保证底层分配器在所有克隆与分配块释放之前一直存活。
unsafe impl Allocator for SessionAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        <&BumpListAlloc as Allocator>::allocate(&self.bump(), layout)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        // SAFETY: 调用方保证 ptr/layout 来自本分配器；本实现转发给同一个底层分配器。
        unsafe { <&BumpListAlloc as Allocator>::deallocate(&self.bump(), ptr, layout) }
    }
}

// SAFETY: `SessionAlloc: Allocator + Clone`；克隆共享同一个底层分配器，符合
// `AllocatorClone` 的「克隆后仍能释放原分配块」契约。
unsafe impl AllocatorClone for SessionAlloc {}

impl<'alloc> TrArena<'alloc> for SessionAlloc {
    type Error = BumpListArenaError;

    type Ref<T: 'alloc>
        = BumpListBox<'alloc, T>
    where
        Self: 'alloc;

    type AllocValueAsync<'a, T>
        = future::Ready<Result<BumpListBox<'alloc, T>, BumpListArenaError>>
    where
        'alloc: 'a,
        T: 'alloc + 'a,
        Self: 'alloc + 'a;

    fn alloc_value_async<'a, T>(&'alloc self, value: T) -> Self::AllocValueAsync<'a, T>
    where
        'alloc: 'a,
        T: 'alloc + 'a,
        Self: 'alloc + 'a,
    {
        TrArena::alloc_value_async(self.bump(), value)
    }
}

impl<'alloc> TrArenaBytes<'alloc> for SessionAlloc {
    type AllocBytesAsync<'a>
        = future::Ready<Result<&'alloc [u8], BumpListArenaError>>
    where
        'alloc: 'a,
        Self: 'alloc + 'a;

    fn alloc_bytes_async<'a>(&'alloc self, src: &'a [u8]) -> Self::AllocBytesAsync<'a>
    where
        'alloc: 'a,
        Self: 'alloc + 'a,
    {
        TrArenaBytes::alloc_bytes_async(self.bump(), src)
    }
}

impl<'alloc> TrAllocSource<'alloc, SessionAlloc> for SessionAlloc {
    fn alloc_from(arena: &'alloc SessionAlloc) -> Self {
        arena.clone()
    }
}
