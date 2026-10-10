use core::{
    marker::PhantomData,
};

use crate::{
    access_method::AccessMethod,
    specs::{HeaderVal, Headers, Status, StdHeaderKey},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrRpcMessage, Request, Response
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// The common base of `Request` and `Response`
pub trait TrRpcMessage {
    fn headers(&self) -> Option<&Headers>;

    /// 获取报文体的 MIME 类型（`Body_Type` 标准头）。
    fn try_get_body_type(&self) -> Option<&HeaderVal> {
        self.headers()?
            .try_get_header(&StdHeaderKey::Body_Type.into())
    }

    fn try_get_body_size_str(&self) -> Option<&HeaderVal> {
        self.headers()?
            .try_get_header(&StdHeaderKey::Body_Size.into())
    }

    #[inline]
    fn try_get_body_size(&self) -> Option<usize> {
        let val = self.try_get_body_size_str()?;
        match val.try_as_header_val() {
            Result::Ok(n) => Option::Some(n.into_inner() as usize),
            Result::Err(s) => s.parse::<usize>().ok(),
        }
    }
}

pub trait TrRpcRequest
where
    Self: TrRpcMessage + Sized,
{
    fn method(&self) -> AccessMethod;

    fn location(&self) -> &str;
}

pub trait TrRpcResponse
where
    Self: TrRpcMessage + Sized,
{
    fn status(&self) -> Status;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type Nothing = ();

/// The content that a client will send to the server and ask for something.
///
/// A request may or may not have a body, of which content length must be
/// declared in the standard header with `StdHeaderKey::Body_Size` key.
///
/// A request does not include the stream that its content length is not
/// declared in the header. However, a client could append content after the
/// request is sent, directly in the same stream from which the client sends
/// the request.
///
/// And the server should be told by both the header and the body that any
/// suffix stream should be received and how it is suggested to handle.
#[derive(Debug)]
pub struct Request<TyBody = Nothing, TyPush = Nothing> {
    method_: AccessMethod,
    path_: String,
    headers_: Option<Headers>,
    body_: Option<TyBody>,
    _push: PhantomData<[TyPush]>,
}

impl<TyBody, TyPush> Request<TyBody, TyPush> {
    /// 创建一个请求。
    ///
    /// 服务端测试和客户端 builder 都可以使用这个基础构造器；
    /// 更完整的 body / headers 组装可以在上层继续封装。
    pub fn new(method: AccessMethod, path: impl Into<String>) -> Self {
        Request {
            method_: method,
            path_: path.into(),
            headers_: Option::None,
            body_: Option::None,
            _push: PhantomData,
        }
    }

    /// 给请求附加一组头。
    pub fn with_headers(mut self, headers: impl Into<Headers>) -> Self {
        self.headers_ = Option::Some(headers.into());
        self
    }

    pub const fn method(&self) -> AccessMethod {
        self.method_
    }

    pub const fn location(&self) -> &str {
        self.path_.as_str()
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    pub const fn headers_mut(&mut self) -> &mut Option<Headers> {
        &mut self.headers_
    }

    pub const fn body(&self) -> Option<&TyBody> {
        self.body_.as_ref()
    }

    pub const fn body_mut(&mut self) -> &mut Option<TyBody> {
        &mut self.body_
    }

    #[inline]
    pub fn try_get_body_type(&self) -> Option<&HeaderVal> {
        TrRpcMessage::try_get_body_type(self)
    }

    #[inline]
    pub fn try_get_body_size_str(&self) -> Option<&HeaderVal> {
        TrRpcMessage::try_get_body_size_str(self)
    }
}

impl<TyBody, TyPush> TrRpcMessage for Request<TyBody, TyPush>  {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Request::headers(self)
    }
}

impl<TyBody, TyPush> TrRpcRequest for Request<TyBody, TyPush> {
    #[inline]
    fn method(&self) -> AccessMethod {
        Request::method(self)
    }

    #[inline]
    fn location(&self) -> &str {
        Request::location(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// The content that the server will react to the client when being asked for
/// something.
///
/// A response may or may not have a body, of which content length must be
/// declared in the standard header with `StdHeaderKey::Body_Size` key.
///
/// A response does not include the stream that its content length is not
/// declared in the header. However, a server could append content after the
/// response is sent, directly in the same stream from which the client
/// receives the response.
///
/// And the client should be told by the both header and body that any suffix
/// stream should be received and how it is suggested to handle.
#[derive(Debug)]
pub struct Response<TyBody = Nothing, TyPull = Nothing>  {
    status_: Status,
    headers_: Option<Headers>,
    body_: Option<TyBody>,
    _pull: PhantomData<[TyPull]>,
}

impl<TyBody, TyPush> Response<TyBody, TyPush> {
    /// 创建一个只有状态码、没有额外头的回复。
    ///
    /// 服务端 handler 通常会先构造 `Response`，再通过 `headers_` 设置
    /// `Body_Type` / `Body_Size` 等标准头，然后把回复头写入输出流。
    pub const fn new(status: Status) -> Self {
        Response {
            status_: status,
            headers_: Option::None,
            body_: Option::None,
            _pull: PhantomData,
        }
    }

    pub const fn status(&self) -> Status {
        self.status_
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    pub const fn headers_mut(&mut self) -> &Option<Headers> {
        &mut self.headers_
    }

    pub const fn body(&self) -> Option<&TyBody> {
        self.body_.as_ref()
    }

    pub const fn body_mut(&mut self) -> &mut Option<TyBody> {
        &mut self.body_
    }

    #[inline]
    pub fn try_get_body_type(&self) -> Option<&HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_type(self)
    }

    #[inline]
    pub fn try_get_body_size_str(&self) -> Option<&HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_size_str(self)
    }
}

impl<TyBody, TyPush> TrRpcMessage for Response<TyBody, TyPush> {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Response::headers(self)
    }
}

impl<TyBody, TyPush> TrRpcResponse for Response<TyBody, TyPush> {
    #[inline]
    fn status(&self) -> Status {
        Response::status(self)
    }
}
