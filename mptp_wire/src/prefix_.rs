//! 报文前缀的两个具体类型：请求是 `access_method + location + headers`，回复是
//! `status + headers`。
//!
//! 这两个类型就是 MPTP 的**线格式载体**：`mptp_wire` 不再要求使用者的配置提供 codec，而是
//! 直接把这两个类型交给 [`wire_`](super::wire_) 编解码。业务类型（例如 RPC 的参数与返回值）
//! 不在这里出现——它们仍然由使用者自己的序列化格式负责。
//!
//! # allocator
//!
//! `location` 是 `abs_str::String<A>`，头表是 [`Headers<A>`]；两者的堆内存都由 `A` 分配。
//! 前缀自身保存一份 allocator 句柄，用来在 `headers_mut()` 现场创建头表、在
//! `set_location()` 现场分配新位置。类型定义只要求 `A: Allocator`；构造与改写另需
//! `A: Clone`。
//!
//! # `prefix_`: the two concrete prefix types
//!
//! Requests are `method + location + headers`; responses are `status + headers`. The
//! location string and the header map are both allocated by `A`.

use core::{alloc::AllocatorClone, fmt};

use abs_mptp::{
    messaging::{TrMsgPrefix, TrRequestPrefix, TrResponsePrefix},
    spec::{Status, access_method::AccessMethod},
};
use abs_str::str_alloc::String;

use super::headers_::Headers;

/// 请求前缀：`access_method + location + headers`。
///
/// 三个部分按这个顺序上线；`headers` 没有内容时写 MessagePack 的 `nil`，而不是空映射。
/// 没有头与「有一张空头表」在线上是两种不同的表示，本类型原样保留这个区别。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::access_method::AccessMethod;
/// use mptp_wire::RequestPrefix;
/// use std::alloc::Global;
///
/// let mut prefix = RequestPrefix::new(AccessMethod::Call, "/api/v1/customer/goods", Global);
/// prefix
///     .headers_mut()
///     .set_num(abs_mptp::spec::StdHeaderKey::Body_Size, 12);
///
/// assert_eq!(prefix.method(), AccessMethod::Call);
/// assert_eq!(prefix.location(), "/api/v1/customer/goods");
/// ```
pub struct RequestPrefix<A: AllocatorClone> {
    /// 访问方法。
    pub(crate) method_: AccessMethod,

    /// 资源位置（路径），由 `A` 分配。
    pub(crate) location_: String<A>,

    /// 头表；没有头就是 `None`（线上写 `nil`，而不是空 map）。
    pub(crate) headers_: Option<Headers<A>>,

    /// 现场创建头表 / 位置时使用的 allocator。
    pub(crate) alloc_: A,
}

impl<A: AllocatorClone> Clone for RequestPrefix<A> {
    fn clone(&self) -> Self {
        RequestPrefix {
            method_: self.method_,
            location_: self.location_.clone(),
            headers_: self.headers_.clone(),
            alloc_: self.alloc_.clone(),
        }
    }
}

impl<A: AllocatorClone> fmt::Debug for RequestPrefix<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestPrefix")
            .field("method", &self.method_)
            .field("location", &&*self.location_)
            .field("headers", &self.headers_)
            .finish()
    }
}

impl<A: AllocatorClone> PartialEq for RequestPrefix<A> {
    fn eq(&self, other: &Self) -> bool {
        self.method_ == other.method_
            && *self.location_ == *other.location_
            && self.headers_ == other.headers_
    }
}

impl<A: AllocatorClone> Eq for RequestPrefix<A> {}

impl<A: AllocatorClone> RequestPrefix<A> {
    /// 由已分配好的四部分构造；供本 crate 的线格式解码使用。
    ///
    /// Build from already allocated parts; used by this crate's wire decoder.
    pub(crate) const fn from_parts_(
        method: AccessMethod,
        location: String<A>,
        headers: Option<Headers<A>>,
        alloc: A,
    ) -> Self {
        RequestPrefix {
            method_: method,
            location_: location,
            headers_: headers,
            alloc_: alloc,
        }
    }
}

impl<A: AllocatorClone> RequestPrefix<A> {
    /// 由「方法 + 位置 + allocator」构造一条**没有头**的请求前缀。
    ///
    /// Build a header-less request prefix; the location is allocated by `alloc`.
    pub fn new(method: AccessMethod, location: &str, alloc: A) -> Self {
        let location_ = String::from_str_in(location, alloc.clone());
        RequestPrefix {
            method_: method,
            location_,
            headers_: Option::None,
            alloc_: alloc,
        }
    }

    /// 由四部分构造；`headers` 为 `None` 表示这条报文没有头表。
    ///
    /// Build from four parts; `None` means the message carries no header map.
    pub fn with_headers(
        method: AccessMethod,
        location: &str,
        headers: Option<Headers<A>>,
        alloc: A,
    ) -> Self {
        let location_ = String::from_str_in(location, alloc.clone());
        RequestPrefix {
            method_: method,
            location_,
            headers_: headers,
            alloc_: alloc,
        }
    }

    /// 取可变头表；没有头时用本前缀的 allocator 创建一张空表。
    ///
    /// Borrow the header map mutably, creating an empty one when absent.
    pub fn headers_mut(&mut self) -> &mut Headers<A> {
        let alloc = self.alloc_.clone();
        self.headers_.get_or_insert_with(|| Headers::new(alloc))
    }

    /// 替换资源位置；新文本由本前缀的 allocator 分配。
    ///
    /// Replace the location; the new text is allocated by this prefix's allocator.
    pub fn set_location(&mut self, location: &str) {
        self.location_ = String::from_str_in(location, self.alloc_.clone());
    }
}

impl<A: AllocatorClone> RequestPrefix<A> {
    /// 本请求使用的访问方法。
    pub const fn method(&self) -> AccessMethod {
        self.method_
    }

    /// 访问的资源位置。
    pub fn location(&self) -> &str {
        &self.location_
    }

    /// 取头表；没有头时返回 `None`。
    pub const fn headers(&self) -> Option<&Headers<A>> {
        self.headers_.as_ref()
    }

    /// 本前缀的 allocator 句柄。
    pub const fn allocator(&self) -> &A {
        &self.alloc_
    }

    /// 取可变头表；没有头时**不会**自动创建。
    pub fn headers_mut_if_present(&mut self) -> Option<&mut Headers<A>> {
        self.headers_.as_mut()
    }

    /// 整张替换头表；`None` 表示没有头。
    pub fn set_headers(&mut self, headers: Option<Headers<A>>) {
        self.headers_ = headers;
    }
}

impl<A: AllocatorClone> TrMsgPrefix for RequestPrefix<A> {
    type Headers<'f>
        = Headers<A>
    where
        Self: 'f;

    fn headers(&self) -> Option<&Self::Headers<'_>> {
        self.headers_.as_ref()
    }
}

impl<A: AllocatorClone> TrRequestPrefix for RequestPrefix<A> {
    fn method(&self) -> AccessMethod {
        self.method_
    }

    fn location(&self) -> &str {
        &self.location_
    }
}

/// 回复前缀：`status + headers`。
///
/// 两个部分按这个顺序上线；`headers` 没有内容时写 `nil`。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::Status;
/// use mptp_wire::ResponsePrefix;
/// use std::alloc::Global;
///
/// let mut prefix = ResponsePrefix::new(Status::Ok, Global);
/// prefix
///     .headers_mut()
///     .set_num(abs_mptp::spec::StdHeaderKey::Body_Size, 12);
///
/// assert_eq!(prefix.status(), Status::Ok);
/// ```
pub struct ResponsePrefix<A: AllocatorClone> {
    /// 状态码。
    pub(crate) status_: Status,

    /// 头表；没有头就是 `None`。
    pub(crate) headers_: Option<Headers<A>>,

    /// 现场创建头表时使用的 allocator。
    pub(crate) alloc_: A,
}

impl<A: AllocatorClone> Clone for ResponsePrefix<A> {
    fn clone(&self) -> Self {
        ResponsePrefix {
            status_: self.status_,
            headers_: self.headers_.clone(),
            alloc_: self.alloc_.clone(),
        }
    }
}

impl<A: AllocatorClone> fmt::Debug for ResponsePrefix<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponsePrefix")
            .field("status", &self.status_)
            .field("headers", &self.headers_)
            .finish()
    }
}

impl<A: AllocatorClone> PartialEq for ResponsePrefix<A> {
    fn eq(&self, other: &Self) -> bool {
        self.status_ == other.status_ && self.headers_ == other.headers_
    }
}

impl<A: AllocatorClone> Eq for ResponsePrefix<A> {}

impl<A: AllocatorClone> ResponsePrefix<A> {
    /// 由已分配好的三部分构造；供本 crate 的线格式解码使用。
    ///
    /// Build from already allocated parts; used by this crate's wire decoder.
    pub(crate) const fn from_parts_(status: Status, headers: Option<Headers<A>>, alloc: A) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: headers,
            alloc_: alloc,
        }
    }
}

impl<A: AllocatorClone> ResponsePrefix<A> {
    /// 由「状态码 + allocator」构造一条**没有头**的回复前缀。
    ///
    /// Build a header-less response prefix; `alloc` is used for a later header map.
    pub fn new(status: Status, alloc: A) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: Option::None,
            alloc_: alloc,
        }
    }

    /// 由三部分构造；`headers` 为 `None` 表示这条报文没有头表。
    ///
    /// Build from three parts; `None` means the message carries no header map.
    pub fn with_headers(status: Status, headers: Option<Headers<A>>, alloc: A) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: headers,
            alloc_: alloc,
        }
    }

    /// 取可变头表；没有头时用本前缀的 allocator 创建一张空表。
    ///
    /// Borrow the header map mutably, creating an empty one when absent.
    pub fn headers_mut(&mut self) -> &mut Headers<A> {
        let alloc = self.alloc_.clone();
        self.headers_.get_or_insert_with(|| Headers::new(alloc))
    }
}

impl<A: AllocatorClone> ResponsePrefix<A> {
    /// 本次回复的状态码。
    pub const fn status(&self) -> Status {
        self.status_
    }

    /// 取头表；没有头时返回 `None`。
    pub const fn headers(&self) -> Option<&Headers<A>> {
        self.headers_.as_ref()
    }

    /// 本前缀的 allocator 句柄。
    pub const fn allocator(&self) -> &A {
        &self.alloc_
    }

    /// 取可变头表；没有头时**不会**自动创建。
    pub fn headers_mut_if_present(&mut self) -> Option<&mut Headers<A>> {
        self.headers_.as_mut()
    }

    /// 整张替换头表；`None` 表示没有头。
    pub fn set_headers(&mut self, headers: Option<Headers<A>>) {
        self.headers_ = headers;
    }

    /// 替换状态码。
    pub fn set_status(&mut self, status: Status) {
        self.status_ = status;
    }
}

impl<A: AllocatorClone> TrMsgPrefix for ResponsePrefix<A> {
    type Headers<'f>
        = Headers<A>
    where
        Self: 'f;

    fn headers(&self) -> Option<&Self::Headers<'_>> {
        self.headers_.as_ref()
    }
}

impl<A: AllocatorClone> TrResponsePrefix for ResponsePrefix<A> {
    fn status(&self) -> Status {
        self.status_
    }
}

#[cfg(test)]
mod tests_ {
    //! 前缀的纯本地判定：体边界由两个标准头唯一决定。
    //!
    //! Local tests for the prefix types: the body boundary is decided uniquely by the two
    //! standard headers.

    use alloc::alloc::Global;

    use abs_mptp::{
        messaging::BodySize,
        spec::{NumOrStr, StdHeaderKey, StdHeaderVal},
    };

    use super::*;

    /// 测试请求前缀的体边界把三种声明各归各位。
    /// Test that the request prefix maps each body declaration to its own boundary.
    ///
    /// - 手段：分别构造没有体声明的请求、`Body_Size: 12` 的请求与
    ///   `Body_Transfer: Chunked` 的请求。
    /// - 判断：`body_info` 依次为 `NilBody` / `FixedLength(Num(12))` / `ChunkedData`。
    ///
    /// - Means: build a request with no declaration, one with `Body_Size: 12` and one with
    ///   `Body_Transfer: Chunked`.
    /// - Verdict: `body_info` yields `NilBody`, `FixedLength(Num(12))` and `ChunkedData`.
    #[test]
    fn request_body_info_maps_declarations_() {
        let plain = RequestPrefix::new(AccessMethod::View, "/rpc/echo", Global);
        assert_eq!(plain.body_info(), BodySize::NilBody);

        let mut sized = plain.clone();
        sized.headers_mut().set_num(StdHeaderKey::Body_Size, 12);
        assert_eq!(
            sized.body_info(),
            BodySize::FixedLength(NumOrStr::Num(12usize))
        );

        let mut chunked = plain;
        chunked.headers_mut().set_num(
            StdHeaderKey::Body_Transfer,
            StdHeaderVal::Body_Transfer_Chunked.into_inner(),
        );
        assert_eq!(chunked.body_info(), BodySize::ChunkedData);
    }

    /// 测试回复前缀的体边界与请求前缀走同一套默认判定。
    /// Test that response prefixes use the same default body-boundary decision.
    ///
    /// - 手段：构造 `Body_Size: 8` 的回复前缀。
    /// - 判断：`body_info` 为 `FixedLength(Num(8))`。
    ///
    /// - Means: build a response prefix carrying `Body_Size: 8`.
    /// - Verdict: `body_info` is `FixedLength(Num(8))`.
    #[test]
    fn response_body_info_uses_default_() {
        let mut prefix = ResponsePrefix::new(Status::Ok, Global);
        prefix.headers_mut().set_num(StdHeaderKey::Body_Size, 8);

        assert_eq!(
            prefix.body_info(),
            BodySize::FixedLength(NumOrStr::Num(8usize))
        );
    }
}
