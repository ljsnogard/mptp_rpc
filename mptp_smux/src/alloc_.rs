//! per-request / per-session arena 的类型与构造。
//!
//! [`RequestAlloc`] 是 `BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>`：
//!
//! - 内层 `BumpListAlloc` 是本次请求自己的 arena；
//! - `Arc` 拥有这个 arena，并把控制块分配在 client / servlet 的 root arena 里；
//! - `&'a BumpListAlloc` 让句柄的生命周期绑定 root，因此请求 arena 不可能活过
//!   `MptpClient` / `MptpServlet`；
//! - `BumpListAllocRef` 把 `&*Arc` 的 `Allocator` / denco arena 实现转发到句柄上。
//!
//! 这不是「一个 client 一个 arena」的粗粒度模型：每次 `request` / `accept` 都创建新的
//! 内层 `BumpListAlloc`，会话结束时内层 arena 随最后一个句柄释放。
//!
//! # `alloc_`: per-request arena handles
//!
//! `RequestAlloc<'a>` owns one per-request `BumpListAlloc` inside an `Arc` whose control
//! block is allocated from the client/servlet root arena. The `'a` borrow keeps it from
//! outliving that root.

use alloc::sync::Arc;

use bumplist_alloc::{BumpListAlloc, BumpListAllocError, BumpListAllocRef, PageSize};

/// 一次请求 / 一条会话自己的 arena 句柄。
///
/// 内层 `BumpListAlloc` 由 `Arc` 拥有；Arc 的控制块从 `&'a BumpListAlloc` root 分配。
pub type RequestAlloc<'a> = BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>;

/// 从 root arena 分配一个新的 per-request arena。
///
/// 内层 `BumpListAlloc` 在构造时不申请 Page，第一次分配才申请；因此把它移进
/// `Arc::new_in` 是安全的。返回的句柄带 root 的借用生命周期。
///
/// # Errors
///
/// 页大小在当前平台不受支持时返回 [`BumpListAllocError::UnsupportedPageSize`]。
pub fn new_request_alloc<'a>(
    root: &'a BumpListAlloc,
    page_size: PageSize,
) -> Result<RequestAlloc<'a>, BumpListAllocError> {
    let inner = BumpListAlloc::new(page_size)?;
    let inner = Arc::new_in(inner, root);
    Result::Ok(BumpListAllocRef::new(inner))
}
