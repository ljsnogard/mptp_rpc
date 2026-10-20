//! 报文前缀的两个具体类型：请求是 `access_method + location + headers`，回复是
//! `status + headers`。
//!
//! 这两个类型就是 MPTP 的**线格式载体**：`mptp_smux` 不再要求使用者的配置提供 codec，而是
//! 直接把这两个类型交给 [`wire_`](super::wire_) 编解码。业务类型（例如 RPC 的参数与返回值）
//! 不在这里出现——它们仍然由使用者自己的序列化格式负责。
//!
//! # 与 `abs_mptp` 的关系
//!
//! [`TrMsgPrefix`] / [`TrRequestPrefix`] / [`TrResponsePrefix`] 规定「前缀能回答什么」，
//! 本模块的类型实现它们。体边界仍然走 `abs_mptp` 的默认 `body_info`：由 `Body_Size` 与
//! `Body_Transfer` 两个标准头唯一决定。
//!
//! # `prefix_`: the two concrete prefix types
//!
//! Requests are `method + location + headers`; responses are `status + headers`. These are
//! the wire-format carriers owned by `mptp_smux`; business payload types stay outside.

use abs_mptp::{
    messaging::{TrMsgPrefix, TrRequestPrefix, TrResponsePrefix},
    spec::{Status, access_method::AccessMethod},
};

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
/// use mptp_smux::messaging::RequestPrefix;
///
/// let mut prefix = RequestPrefix::new(AccessMethod::Call, "/api/v1/customer/goods");
/// prefix
///     .headers_mut()
///     .set_text(abs_mptp::spec::StdHeaderKey::Body_Size, "12");
///
/// assert_eq!(prefix.method(), AccessMethod::Call);
/// assert_eq!(prefix.location(), "/api/v1/customer/goods");
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestPrefix {
    /// 访问方法。
    pub(crate) method_: AccessMethod,

    /// 资源位置（路径）。
    pub(crate) location_: String,

    /// 头表；没有头就是 `None`（线上写 `nil`，而不是空 map）。
    pub(crate) headers_: Option<Headers>,
}

impl RequestPrefix {
    /// 由「方法 + 位置」构造一条**没有头**的请求前缀。
    pub fn new(method: AccessMethod, location: impl Into<String>) -> Self {
        RequestPrefix {
            method_: method,
            location_: location.into(),
            headers_: Option::None,
        }
    }

    /// 由三部分构造；`headers` 为 `None` 表示这条报文没有头表。
    pub fn with_headers(
        method: AccessMethod,
        location: impl Into<String>,
        headers: Option<Headers>,
    ) -> Self {
        RequestPrefix {
            method_: method,
            location_: location.into(),
            headers_: headers,
        }
    }

    /// 本请求使用的访问方法。
    pub const fn method(&self) -> AccessMethod {
        self.method_
    }

    /// 访问的资源位置。
    pub fn location(&self) -> &str {
        self.location_.as_str()
    }

    /// 取头表；没有头时返回 `None`。
    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    /// 取可变头表；没有头时**不会**自动创建。
    pub fn headers_mut_if_present(&mut self) -> Option<&mut Headers> {
        self.headers_.as_mut()
    }

    /// 取可变头表；没有头时创建一张空表。
    ///
    /// 客户端代理在装配请求时最常用这个入口：先写 `Body_Size` / `Data_Type_Id` 等标准头，
    /// 再交给发送路径。创建之后再取消这些头，得到的是「空 map」而不是 `nil`。
    pub fn headers_mut(&mut self) -> &mut Headers {
        self.headers_.get_or_insert_with(Headers::new)
    }

    /// 整张替换头表；`None` 表示没有头。
    pub fn set_headers(&mut self, headers: Option<Headers>) {
        self.headers_ = headers;
    }

    /// 替换资源位置。
    pub fn set_location(&mut self, location: impl Into<String>) {
        self.location_ = location.into();
    }
}

impl TrMsgPrefix for RequestPrefix {
    type Headers = Headers;

    fn headers(&self) -> Option<&Self::Headers> {
        self.headers_.as_ref()
    }
}

impl TrRequestPrefix for RequestPrefix {
    fn method(&self) -> AccessMethod {
        self.method_
    }

    fn location(&self) -> &str {
        self.location_.as_str()
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
/// use mptp_smux::messaging::ResponsePrefix;
///
/// let mut prefix = ResponsePrefix::new(Status::Ok);
/// prefix
///     .headers_mut()
///     .set_text(abs_mptp::spec::StdHeaderKey::Body_Size, "12");
///
/// assert_eq!(prefix.status(), Status::Ok);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResponsePrefix {
    /// 状态码。
    pub(crate) status_: Status,

    /// 头表；没有头就是 `None`。
    pub(crate) headers_: Option<Headers>,
}

impl ResponsePrefix {
    /// 由状态码构造一条**没有头**的回复前缀。
    pub const fn new(status: Status) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: Option::None,
        }
    }

    /// 由两部分构造；`headers` 为 `None` 表示这条报文没有头表。
    pub const fn with_headers(status: Status, headers: Option<Headers>) -> Self {
        ResponsePrefix {
            status_: status,
            headers_: headers,
        }
    }

    /// 本次回复的状态码。
    pub const fn status(&self) -> Status {
        self.status_
    }

    /// 取头表；没有头时返回 `None`。
    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    /// 取可变头表；没有头时**不会**自动创建。
    pub fn headers_mut_if_present(&mut self) -> Option<&mut Headers> {
        self.headers_.as_mut()
    }

    /// 取可变头表；没有头时创建一张空表。
    pub fn headers_mut(&mut self) -> &mut Headers {
        self.headers_.get_or_insert_with(Headers::new)
    }

    /// 整张替换头表；`None` 表示没有头。
    pub fn set_headers(&mut self, headers: Option<Headers>) {
        self.headers_ = headers;
    }

    /// 替换状态码。
    pub fn set_status(&mut self, status: Status) {
        self.status_ = status;
    }
}

impl TrMsgPrefix for ResponsePrefix {
    type Headers = Headers;

    fn headers(&self) -> Option<&Self::Headers> {
        self.headers_.as_ref()
    }
}

impl TrResponsePrefix for ResponsePrefix {
    fn status(&self) -> Status {
        self.status_
    }
}

#[cfg(test)]
mod tests_ {
    //! 前缀的纯本地判定：体边界由两个标准头唯一决定。
    //!
    //! 这里不编码、不建连接：`body_info` 是 `abs_mptp` 的默认实现，本模块只负责让请求与
    //! 回复两种前缀都把它接上。
    //!
    //! Local tests for the prefix types: the body boundary is decided uniquely by the two
    //! standard headers. No encoding or connection is involved.

    use abs_mptp::{
        messaging::BodySize,
        spec::{NumOrStr, StdHeaderKey, StdHeaderVal},
    };

    use super::*;

    /// 测试请求前缀的体边界把三种声明各归各位。
    /// Test that the request prefix maps each body declaration to its own boundary.
    ///
    /// - 手段：分别构造没有体声明的请求、`Body_Size: "12"` 的请求与
    ///   `Body_Transfer: Chunked` 的请求。
    /// - 判断：`body_info` 依次为 `NilBody` / `FixedLength(Num(12))` / `ChunkedData`；
    ///   默认实现把文本长度解析成数字形态，调用者两种形态都应接受。
    ///
    /// - Means: build a request with no declaration, one with `Body_Size: "12"` and one with
    ///   `Body_Transfer: Chunked`.
    /// - Verdict: `body_info` yields `NilBody`, `FixedLength(Num(12))` and `ChunkedData`; the
    ///   default implementation parses textual sizes into numeric form.
    #[test]
    fn request_body_info_maps_declarations_() {
        let plain = RequestPrefix::new(AccessMethod::View, "/rpc/echo");
        assert_eq!(plain.body_info(), BodySize::NilBody);

        let mut sized = plain.clone();
        sized.headers_mut().set_text(StdHeaderKey::Body_Size, "12");
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
    /// - 手段：构造 `Body_Size: "8"` 的回复前缀。
    /// - 判断：`body_info` 为 `FixedLength(Num(8))`；前缀类型本身不额外解释方法语义，
    ///   `Head` / `Drop` 的「回复无体」核对留在会话状态机里。
    ///
    /// - Means: build a response prefix carrying `Body_Size: "8"`.
    /// - Verdict: `body_info` is `FixedLength(Num(8))`; the prefix type adds no method
    ///   semantics — the `Head` / `Drop` check stays in the session state machine.
    #[test]
    fn response_body_info_uses_default_() {
        let mut prefix = ResponsePrefix::new(Status::Ok);
        prefix.headers_mut().set_text(StdHeaderKey::Body_Size, "8");

        assert_eq!(
            prefix.body_info(),
            BodySize::FixedLength(NumOrStr::Num(8usize))
        );
    }
}
