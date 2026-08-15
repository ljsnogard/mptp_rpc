use core::mem::MaybeUninit;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use thiserror::Error;

use abs_cancel::TrCancellationToken;

use abs_buff::{
    chaining::{Chain, ChainingIoResult},
    gen_may_cancel_future,
    x_deps::abs_cancel::{self, TrMayCancel},
    BuffWriteAsOutput, TrBuffRead, TrBuffTryRead, TrBuffWrite,
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
    ///
    /// 意图：`Body_Type` 才是声明报文体 MIME 类型的标准头（见 specs.rs）；
    /// 原来这里误用了 `Body_Size`，会导致调用方永远读不到回复体的类型，
    /// 客户端据此决定是否读取并解析回复体的逻辑将失效。
    fn try_get_body_type<'f>(&'f self) -> Option<&'f HeaderVal> {
        self.headers()?
            .try_get_header(&StdHeaderKey::Body_Type.into())
    }

    fn try_get_body_size_str<'f>(&'f self) -> Option<&'f HeaderVal> {
        self.headers()?
            .try_get_header(&StdHeaderKey::Body_Size.into())
    }

    fn encode(&mut self) -> Encode<'_, Self>
    where
        Self: Sized + Serialize,
    {
        if self.try_get_body_type().is_some() {
            Encode::WithBody(EncoderWithBody::new(self))
        } else {
            Encode::WithoutBody(EncoderNilBody::new(self))
        }
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
    pub const fn access_method(&self) -> AccessMethod {
        self.method_
    }

    pub const fn access_path(&self) -> &str {
        self.path_.as_str()
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    #[inline]
    pub fn try_get_content_type<'f>(&'f self) -> Option<&'f HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_type(self)
    }

    #[inline]
    pub fn try_get_content_length_str<'f>(&'f self) -> Option<&'f HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_size_str(self)
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
    pub const fn status(&self) -> Status {
        self.status_
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    #[inline]
    pub fn try_get_body_type<'f>(&'f self) -> Option<&'f HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_type(self)
    }

    #[inline]
    pub fn try_get_body_size_str<'f>(&'f self) -> Option<&'f HeaderVal> {
        <Self as TrRpcMessage>::try_get_body_size_str(self)
    }

    #[inline]
    pub fn try_get_body_size<'f>(&'f self) -> Option<usize> {
        let val = self.try_get_body_size_str()?;
        match val.try_as_header_val() {
            Result::Ok(n) => Option::Some(n.into_inner() as usize),
            Result::Err(s) => s.parse::<usize>().ok(),
        }
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
pub enum Encode<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    WithoutBody(EncoderNilBody<'a, M>),
    WithBody(EncoderWithBody<'a, M>),
}

pub struct EncoderNilBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    message_: Option<&'a M>,
}

pub struct EncoderWithBody<'a, M>
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

impl<'a, M> EncoderNilBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    pub(crate) const fn new(message: &'a M) -> Self {
        EncoderNilBody { message_: Option::Some(message) }
    }

    pub fn try_write<'f, W>(
        &'f mut self,
        buf_write: &'f mut W,
    ) -> Option<EncoderNilBodyWriteMessageAsync<'f, M, W>>
    where
        W: TrBuffWrite,
    {
        if let Option::Some(message) = self.message_.take() {
            let t = EncoderNilBodyWriteMessageAsync(message, buf_write);
            Option::Some(t)
        } else {
            Option::None
        }

    }
}

impl<'a, M> EncoderWithBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    pub(crate) const fn new(message: &'a mut M) -> Self {
        EncoderWithBody { message_: Option::Some(message) }
    }

    pub fn try_write<'f, R, W>(
        &'f mut self,
        body: &'f mut R,
        buf_write: &'f mut W,
    ) -> Option<EncoderWithBodyWriteMessageAsync<'f, M, R, W>>
    where
        R: TrBuffRead,
        W: TrBuffWrite,
    {
        if let Option::Some(message) = self.message_.take() {
            let t =EncoderWithBodyWriteMessageAsync(message, body, buf_write);
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
    cancel: &'c mut C
) -> Result<usize, <W as TrBuffWrite>::Err>
where
    'm: 'c,
    'w: 'c,
    M: TrRpcMessage + Serialize,
    W: Sized + TrBuffWrite,
    C: TrCancellationToken,
{
    let mut buf = std::vec::Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut buf);
    let Result::Ok(_) = <M as Serialize>::serialize(message, &mut ser) else {
        todo!("handle serializer error");
    };
    let mut c = 0usize;
    let mut output = BuffWriteAsOutput::<&'_ mut W, W, u8>::new(buf_write);
    let x = output
        .write_cloned_async(&buf)
        .may_cancel_with(cancel)
        .await;
    if let Option::Some(head_size) = x.as_ref().pick_left() {
        c += head_size;
    }
    if let Option::Some(err) = x.pick_right() {
        return Result::Err(err);
    }
    Result::Ok(c)
}

#[gen_may_cancel_future(EncoderWithBodyWriteMessage)]
async fn encoder_with_body_write_message_async_<'f, M, R, W, C>(
    message: &'f M,
    body_cont: &'f mut R,
    buf_write: &'f mut W,
    cancel: &'f mut C
) -> Result<ChainingIoResult<W, R, u8>, Option<EncoderError<R, W>>>
where
    M: TrRpcMessage + Serialize,
    R: TrBuffRead,
    W: TrBuffWrite,
    C: TrCancellationToken,
{
    let mut buf = std::vec::Vec::new();
    let mut ser = rmp_serde::Serializer::new(&mut buf);
    let Result::Ok(_) = <M as Serialize>::serialize(message, &mut ser) else {
        return Result::Err(Option::None);
    };
    let mut c = 0usize;
    if true {
        let mut output = BuffWriteAsOutput::<&'_ mut W, W, u8>::new(buf_write);
        let x = output
            .write_cloned_async(&buf)
            .may_cancel_with(cancel)
            .await;
        if let Option::Some(head_size) = x.as_ref().pick_left() {
            c += head_size;
        }
        if let Option::Some(err) = x.pick_right() {
            return Result::Err(Option::Some(EncoderError::WriteErr(err)));
        }
    }

    let mut chain = Chain::new(buf_write, body_cont);
    let res: ChainingIoResult<_, _, _> = chain.chain_io_async().may_cancel_with(cancel).await;
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
    rmp_serde::from_read(&mut rx)
        .map_err(|e| DecodeError::BadContent(e.to_string()))
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
    let am = req.access_method();
    if matches!(am, AccessMethod::Push) {
        todo!()
    }
    Result::Ok(req)
}
