//! 会话 / 客户端 / 服务端共享的 clonable 分配器句柄。
//!
//! [`SessionAlloc`] 是 [`BumpListAlloc`] 的 `Arc` 句柄：它实现
//! [`Allocator`](core::alloc::Allocator)、[`TrArena`] 与 [`TrArenaBytes`]，因此同一个
//! allocator 既能给 `Vec` / `abs_str::String` 用，也能直接交给 `MpDecoder::new_in`。
//!
//! 为什么不是裸 `&'a BumpListAlloc`：`abs_mptp::client::TrMptpClient::Session` 是一个
//! **不带生命周期的关联类型**，session 不能借用 client 的分配器；因此每个 client /
//! session / servlet 持有一份 `Arc` 句柄的克隆，内存由所有克隆共享的同一个
//! [`BumpListAlloc`] 提供。句柄是 `Clone` 且 `'static`，所以会话类型不需要任何生命周期
//! 参数。
//!
//! # `alloc_`: the clonable session allocator handle
//!
//! `SessionAlloc` is an `Arc<BumpListAlloc>` handle implementing `Allocator`, `TrArena` and
//! `TrArenaBytes`. Every client / session / servlet holds a clone, so all allocations share
//! one arena while the session types stay lifetime-free.

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
