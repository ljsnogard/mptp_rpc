use core::mem::MaybeUninit;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use abs_buff::{
    TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    as_std_read::AsStdRead,
    as_std_write::AsStdWrite,
    gen_may_cancel_future,
    pipelining::{PipeJoin, PipeJoinIoResult},
};
use abs_cancel::{TrMayCancel, TrCancellationToken};
use buffex::x_deps::{abs_buff, abs_cancel};

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
    ///
    /// 意图：`Body_Type` 才是声明报文体 MIME 类型的标准头（见 specs.rs）；
    /// 原来这里误用了 `Body_Size`，会导致调用方永远读不到回复体的类型，
    /// 客户端据此决定是否读取并解析回复体的逻辑将失效。
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

    fn encode(&mut self) -> Encode<'_, Self>
    where
        Self: Sized + Serialize,
    {
        Encode::new(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

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
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    method_: AccessMethod,
    path_: String,
    headers_: Option<Headers>,
}

impl Request {
    /// 创建一个请求。
    ///
    /// 服务端测试和客户端 builder 都可以使用这个基础构造器；
    /// 更完整的 body / headers 组装可以在上层继续封装。
    pub fn new(method: AccessMethod, path: impl Into<String>) -> Self {
        Request {
            method_: method,
            path_: path.into(),
            headers_: Option::None,
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

    pub const fn headers_mut(&mut self) -> Option<&mut Headers> {
        self.headers_.as_mut()
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

impl TrRpcMessage for Request {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Request::headers(self)
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
#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    status_: Status,
    headers_: Option<Headers>,
}

impl Response {
    /// 创建一个只有状态码、没有额外头的回复。
    ///
    /// 服务端 handler 通常会先构造 `Response`，再通过 `headers_` 设置
    /// `Body_Type` / `Body_Size` 等标准头，然后把回复头写入输出流。
    pub const fn new(status: Status) -> Self {
        Response {
            status_: status,
            headers_: Option::None,
        }
    }

    /// 给回复附加一组头。
    pub fn with_headers(mut self, headers: Headers) -> Self {
        self.headers_ = Option::Some(headers);
        self
    }

    pub const fn status(&self) -> Status {
        self.status_
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
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

impl TrRpcMessage for Response {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Response::headers(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Encode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// To regulate the behaviour of sending request and response. This will
/// check the header and body to determine how to operate the TX stream.
pub struct Encode<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    message_: Option<&'a M>,
}

pub enum EncoderError<R, W>
where
    R: TrBuffRead,
    W: TrBuffWrite,
{
    ReadBodyErr(<R as TrBuffRead>::Err),
    WriteErr(<W as TrBuffWrite>::Err),
}

impl<'a, M> Encode<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    pub(crate) const fn new(message: &'a M) -> Self {
        Encode {
            message_: Option::Some(message),
        }
    }

    pub fn try_write<'f, W>(
        &'f mut self,
        buf_write: &'f mut W,
    ) -> Option<EncoderNilBodyWriteMessageAsync<'f, M, W>>
    where
        W: TrBuffTryWrite,
    {
        if let Option::Some(message) = self.message_.take() {
            let t = EncoderNilBodyWriteMessageAsync(message, buf_write);
            Option::Some(t)
        } else {
            Option::None
        }
    }
}

#[gen_may_cancel_future(EncoderNilBodyWriteMessage)]
async fn encoder_nil_body_write_message_async_<'m, 'w, 'c, M, W, C>(
    message: &'m M,
    buf_write: &'w mut W,
    cancel: &'c mut C,
) -> Result<(), <W as TrBuffWrite>::Err>
where
    'm: 'c,
    'w: 'c,
    M: TrRpcMessage + Serialize,
    W: Sized + TrBuffTryWrite,
    C: TrCancellationToken,
{
    let mut std_write = AsStdWrite::new(buf_write, cancel);
    let encode = rmp_serde::encode::write(&mut std_write, &message);
    if encode.is_err() {
        todo!("handle serializer error");
    };
    Result::Ok(())
}

#[gen_may_cancel_future(EncoderWithBodyWriteMessage)]
async fn encoder_with_body_write_message_async_<'f, M, R, W, C>(
    message: &'f M,
    body_cont: &'f mut R,
    buf_write: &'f mut W,
    cancel: &'f mut C,
) -> Result<PipeJoinIoResult<W, R, u8>, Option<EncoderError<R, W>>>
where
    M: TrRpcMessage + Serialize,
    R: TrBuffRead,
    W: TrBuffTryWrite,
    C: TrCancellationToken + Clone,
{
    let mut std_write = AsStdWrite::new(buf_write, cancel);
    let encode = rmp_serde::encode::write(&mut std_write, &message);
    if encode.is_err() {
        todo!("handle serializer error");
    };
    let mut pipe_join = PipeJoin::new(buf_write, body_cont);
    let res: PipeJoinIoResult<_, _, _> = pipe_join.pipe_async().may_cancel_with(cancel).await;
    Result::Ok(res)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Decode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

struct MessageDecode<'a, M>(&'a mut MaybeUninit<M>)
where
    M: TrRpcMessage;

pub struct RequestDecode<'a>(Option<MessageDecode<'a, Request>>);

pub struct ResponseDecode<'a>(Option<MessageDecode<'a, Response>>);

#[derive(Error, Debug)]
pub enum DecodeError<E>
where
    E: core::error::Error,
{
    #[error("Bad content {0}")]
    BadContent(String),

    #[error("Error occurs during IO")]
    StreamErr(#[from] E),
}

impl<'a> RequestDecode<'a> {
    pub const fn new(m: &'a mut MaybeUninit<Request>) -> Self {
        RequestDecode(Option::Some(MessageDecode::<Request>::new(m)))
    }

    pub fn try_decode<'f, R>(
        &'f mut self,
        rx: &'f mut R,
    ) -> Option<DecodeMessageAsync<'f, R, Request>>
    where
        R: TrBuffTryRead,
    {
        let m = self.0.as_mut()?;
        Option::Some(DecodeMessageAsync(&mut m.0, rx))
    }
}

impl<'a> ResponseDecode<'a> {
    pub const fn new(m: &'a mut MaybeUninit<Response>) -> Self {
        ResponseDecode(Option::Some(MessageDecode::<Response>::new(m)))
    }

    pub fn try_decode<'f, R>(
        &'f mut self,
        rx: &'f mut R,
    ) -> Option<DecodeMessageAsync<'f, R, Response>>
    where
        R: TrBuffTryRead,
    {
        let m = self.0.as_mut()?;
        Option::Some(DecodeMessageAsync(&mut m.0, rx))
    }
}

impl<'a, M> MessageDecode<'a, M>
where
    M: TrRpcMessage,
{
    const fn new(m: &'a mut MaybeUninit<M>) -> Self {
        MessageDecode(m)
    }
}

#[gen_may_cancel_future(DecodeMessage)]
pub(crate) async fn decode_msg_async_<'f, R, M, C>(
    _m: &'f mut MaybeUninit<M>,
    buff_r: &'f mut R,
    cancel: &'f mut C,
) -> Result<M, DecodeError<<R as TrBuffRead>::Err>>
where
    R: TrBuffTryRead,
    M: TrRpcMessage + DeserializeOwned,
    C: TrCancellationToken,
{
    let _ = cancel;
    let mut rx = abs_buff::as_std_read::AsStdRead::new(buff_r, cancel);
    rmp_serde::from_read(&mut rx).map_err(|e| DecodeError::BadContent(e.to_string()))
}

#[gen_may_cancel_future(DecodeRequest)]
pub(crate) async fn decode_request_async_<'f, R, C>(
    _m: &'f mut MaybeUninit<Request>,
    buff_r: &'f mut R,
    cancel: &'f mut C,
) -> Result<Request, DecodeError<<R as TrBuffRead>::Err>>
where
    R: TrBuffTryRead,
    C: TrCancellationToken,
{
    let r = self::decode_msg_async_(_m, buff_r, cancel).await;
    let req = match r {
        Result::Err(e) => return Result::Err(e),
        Result::Ok(req) => req,
    };
    let am = req.method();
    if matches!(am, AccessMethod::Push) {
        todo!()
    }
    Result::Ok(req)
}
