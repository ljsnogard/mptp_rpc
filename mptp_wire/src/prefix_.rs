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
pub struct RequestPrefix<A>
where
    A: AllocatorClone,
{
    /// 访问方法。
    pub(crate) method_: AccessMethod,

    /// 资源位置（路径），由 `A` 分配。
    pub(crate) location_: String<A>,

    /// 头表；没有头就是 `None`（线上写 `nil`，而不是空 map）。
    pub(crate) headers_: Option<Headers<A>>,
}

impl<A> Clone for RequestPrefix<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        RequestPrefix {
            method_: self.method_,
            location_: self.location_.clone(),
            headers_: self.headers_.clone(),
        }
    }
}

impl<A> fmt::Debug for RequestPrefix<A>
where
    A: AllocatorClone,
{
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

impl<A> RequestPrefix<A>
where
    A: AllocatorClone,
{
    /// 由已分配好的四部分构造；供本 crate 的线格式解码使用。
    ///
    /// Build from already allocated parts; used by this crate's wire decoder.
    pub(crate) const fn from_parts_(
        method: AccessMethod,
        location: String<A>,
        headers: Option<Headers<A>>,
    ) -> Self {
        RequestPrefix {
            method_: method,
            location_: location,
            headers_: headers,
        }
    }
}

impl<A> RequestPrefix<A>
where
    A: AllocatorClone,
{
    /// 由「方法 + 位置 + allocator」构造一条**没有头**的请求前缀。
    ///
    /// Build a header-less request prefix; the location is allocated by `alloc`.
    pub const fn new(
        method: AccessMethod,
        location: String<A>,
    ) -> Self {
        RequestPrefix {
            method_: method,
            location_: location,
            headers_: Option::None,
        }
    }

    /// 由四部分构造；`headers` 为 `None` 表示这条报文没有头表。
    ///
    /// Build from four parts; `None` means the message carries no header map.
    pub fn with_headers(
        method: AccessMethod,
        location: String<A>,
        headers: Option<Headers<A>>,
    ) -> Self {
        RequestPrefix {
            method_: method,
            location_: location,
            headers_: headers,
        }
    }
}

impl<A> RequestPrefix<A>
where
    A: AllocatorClone,
{
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

    /// 取可变头表；没有头时**不会**自动创建。
    pub fn headers_mut_if_present(&mut self) -> Option<&mut Headers<A>> {
        self.headers_.as_mut()
    }

    /// 整张替换头表；`None` 表示没有头。
    pub fn set_headers(&mut self, headers: Option<Headers<A>>) {
        self.headers_ = headers;
    }
}

impl<A> TrMsgPrefix for RequestPrefix<A>
where
    A: AllocatorClone,
{
    type Headers<'f> = Headers<A> where Self: 'f;

    fn headers(&self) -> Option<&Self::Headers<'_>> {
        self.headers_.as_ref()
    }
}

impl<A> TrRequestPrefix for RequestPrefix<A>
where
    A: AllocatorClone,
{
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
pub struct ResponsePrefix<A>
where
    A: AllocatorClone,
{
    /// 状态码。
    pub(crate) status_: Status,

    /// 头表；没有头就是 `None`。
    pub(crate) headers_: Option<Headers<A>>,
}

impl<A> Clone for ResponsePrefix<A>
where
    A: AllocatorClone,
{
    fn clone(&self) -> Self {
        ResponsePrefix {
            status_: self.status_,
            headers_: self.headers_.clone(),
        }
    }
}

impl<A> fmt::Debug for ResponsePrefix<A>
where
    A: AllocatorClone,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResponsePrefix")
            .field("status", &self.status_)
            .field("headers", &self.headers_)
            .finish()
    }
}

impl<A> PartialEq for ResponsePrefix<A>
where
    A: AllocatorClone,
{
    fn eq(&self, other: &Self) -> bool {
        self.status_ == other.status_ && self.headers_ == other.headers_
    }
}

impl<A> Eq for ResponsePrefix<A>
where
    A: AllocatorClone,
{}

impl<A> ResponsePrefix<A>
where
    A: AllocatorClone,
{
    /// 由已分配好的三部分构造；供本 crate 的线格式解码使用。
    ///
    /// Build from already allocated parts; used by this crate's wire decoder.
    pub(crate) const fn from_parts_(
        status: Status,
        headers: Option<Headers<A>>,
    ) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: headers,
        }
    }
}

impl<A> ResponsePrefix<A>
where
    A: AllocatorClone,
{
    /// 由「状态码 + allocator」构造一条**没有头**的回复前缀。
    ///
    /// Build a header-less response prefix; `alloc` is used for a later header map.
    pub const fn new(status: Status) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: Option::None,
        }
    }

    /// 由三部分构造；`headers` 为 `None` 表示这条报文没有头表。
    ///
    /// Build from three parts; `None` means the message carries no header map.
    pub const fn with_headers(
        status: Status,
        headers: Option<Headers<A>>,
    ) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: headers,
        }
    }
}

impl<A> ResponsePrefix<A>
where
    A: AllocatorClone,
{
    /// 本次回复的状态码。
    pub const fn status(&self) -> Status {
        self.status_
    }

    /// 取头表；没有头时返回 `None`。
    pub const fn headers(&self) -> Option<&Headers<A>> {
        self.headers_.as_ref()
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

impl<A> TrMsgPrefix for ResponsePrefix<A>
where
    A: AllocatorClone,
{
    type Headers<'f> = Headers<A> where Self: 'f;

    fn headers(&self) -> Option<&Self::Headers<'_>> {
        self.headers_.as_ref()
    }
}

impl<A> TrResponsePrefix for ResponsePrefix<A>
where
    A: AllocatorClone,
{
    fn status(&self) -> Status {
        self.status_
    }
}
