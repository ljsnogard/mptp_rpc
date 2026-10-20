//! 回复状态码。
//!
//! 取值沿用 HTTP 的既有约定（`2xx` 成功、`3xx` 重定向、`4xx` 客户端错误、`5xx` 服务端
//! 错误），因为这些约定已经被广泛理解，重新发明一套只会增加两头对齐的成本。
//!
//! # `status_`: response status codes
//!
//! The values follow HTTP's established conventions (`2xx` success, `3xx` redirection,
//! `4xx` client error, `5xx` server error): they are already widely understood, and
//! inventing another set would only add alignment cost on both ends.

use core::fmt::Debug;


/// 一次回复的状态码。
///
/// 它是**线上契约**的一部分，因此只由数值决定相等性：两侧用同一个数就是同一个状态。
///
/// The status code of a response. It is part of the **wire contract**, so equality is
/// decided by the numeric value alone.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Status(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl Status {
    pub const fn new(inner: u16) -> Self {
        Status(inner)
    }

    /// 返回状态码数值。
    ///
    /// The numeric status code.
    pub const fn inner(self) -> u16 {
        self.0
    }

    pub const Ok     : Status = Status(200);
    pub const Created: Status = Status(201);

    /// The HTTP 301 Moved Permanently redirection response status code indicates that the requested
    /// resource has been permanently moved to the URL in the Location header.
    pub const MovedPermanently: Status = Status(301);

    /// The HTTP 302 Found redirection response status code indicates that the requested resource
    /// has been temporarily moved to the URL in the Location header.
    pub const Found: Status = Status(302);

    /// The HTTP 303 See Other redirection response status code indicates that the browser should
    /// redirect to the URL in the Location header instead of rendering the requested resource.
    ///
    /// This response code is often sent back as a result of PUT or POST methods so the client may
    /// retrieve a confirmation, or view a representation of a real-world object (see HTTP range-14).
    /// The method to retrieve the redirected resource is always GET.
    pub const SeeOther: Status = Status(303);

    pub const BadRequest  : Status = Status(400);
    pub const Unauthorized: Status = Status(401);
    pub const Forbidden   : Status = Status(403);
    pub const NotFound    : Status = Status(404);

    /// The access method is known by the server but is not supported by the target resource.
    /// For example, an API may not allow DROP a resource, or the TRACE method entirely.
    pub const MethodNotAllowed: Status = Status(405);

    /// This response is sent when the server, after performing server-driven content negotiation,
    /// doesn't find any content that conforms to the criteria given by the user agent.
    pub const NotAcceptable: Status = Status(406);

    pub const InternalServerError: Status = Status(500);
    pub const NotImplemented     : Status = Status(501);
    pub const BadGateway         : Status = Status(502);
    pub const ServiceUnavailable : Status = Status(503);
    pub const GatewayTimeout     : Status = Status(504);
}
