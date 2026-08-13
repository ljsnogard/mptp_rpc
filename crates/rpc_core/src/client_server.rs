use std::mem::MaybeUninit;

use serde::{Deserialize, Serialize};

use abs_cancel::TrCancellationToken;

use abs_buff::{
    chaining::{Chain, ChainingIoResult},
    gen_may_cancel_future,
    x_deps::abs_cancel::{self, TrMayCancel},
    BuffWriteAsOutput, TrBuffRead, TrBuffWrite,
};

use crate::specs::{AccessMethod, Headers, HeaderKey, Status, StdHeaderKey};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrRpcMessage, Request, Response
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub trait TrRpcMessage {
    fn headers(&self) -> Option<&Headers>;

    fn try_get_content_type<'f>(&'f self) -> Option<&'f str> {
        self.headers()?
            .try_get_header(&HeaderKey::Std(StdHeaderKey::Content_Length))
    }

    fn try_get_content_length_str<'f>(&'f self) -> Option<&'f str> {
        self.headers()?
            .try_get_header(&HeaderKey::Std(StdHeaderKey::Content_Length))
    }

    fn encode(&mut self) -> Encode<'_, Self>
    where
        Self: Sized + Serialize,
    {
        if self.try_get_content_type().is_some() {
            Encode::WithBody(EncoderWithBody::new(self))
        } else {
            Encode::WithoutBody(EncoderNilBody::new(self))
        }
    }
}

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

    pub fn access_path(&self) -> &str {
        self.path_.as_str()
    }

    pub fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    #[inline]
    pub fn try_get_content_type<'f>(&'f self) -> Option<&'f str> {
        <Self as TrRpcMessage>::try_get_content_type(self)
    }

    #[inline]
    pub fn try_get_content_length_str<'f>(&'f self) -> Option<&'f str> {
        <Self as TrRpcMessage>::try_get_content_length_str(self)
    }
}

impl TrRpcMessage for Request {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Request::headers(self)
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Response {
    status_: Status,
    headers_: Option<Headers>,
}

impl Response {
    pub const fn status(&self) -> Status {
        self.status_
    }

    pub fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    #[inline]
    pub fn try_get_content_type<'f>(&'f self) -> Option<&'f str> {
        <Self as TrRpcMessage>::try_get_content_type(self)
    }

    #[inline]
    pub fn try_get_content_length_str<'f>(&'f self) -> Option<&'f str> {
        <Self as TrRpcMessage>::try_get_content_length_str(self)
    }
}

impl TrRpcMessage for Response {
    #[inline]
    fn headers(&self) -> Option<&Headers> {
        Response::headers(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Encode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

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
    message_: Option<&'a mut M>,
}

pub struct EncoderWithBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    message_: Option<&'a mut M>,
}

pub enum EncoderError<R, W>
where
    R: TrBuffRead,
    W: TrBuffWrite,
{
    ReadBodyErr(<R as TrBuffRead>::Err),
    WriteErr(<W as TrBuffWrite>::Err),
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Decode
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub struct Decode<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    message_: &'a mut MaybeUninit<M>,
}

impl<'a, M> EncoderNilBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    pub(crate) const fn new(message: &'a mut M) -> Self {
        EncoderNilBody { message_: Option::Some(message) }
    }

    pub fn write_async<'f, W>(
        &'f mut self,
        buf_write: &'f mut W,
    ) -> EncoderNilBodyWriteMessageAsync<'f, M, W>
    where
        W: TrBuffWrite,
    {
        let Option::Some(message) = self.message_.take() else {
            todo!()
        };
        EncoderNilBodyWriteMessageAsync(message, buf_write)
    }
}

impl<'a, M> EncoderWithBody<'a, M>
where
    M: TrRpcMessage + Serialize,
{
    pub(crate) const fn new(message: &'a mut M) -> Self {
        EncoderWithBody { message_: Option::Some(message) }
    }

    pub fn write_async<'f, R, W>(
        &'f mut self,
        body: &'f mut R,
        buf_write: &'f mut W,
    ) -> EncoderWithBodyWriteMessageAsync<'f, M, R, W>
    where
        R: TrBuffRead,
        W: TrBuffWrite,
    {
        let Option::Some(message) = self.message_.take() else {
            todo!()
        };
        EncoderWithBodyWriteMessageAsync(message, body, buf_write)
    }
}

#[gen_may_cancel_future(EncoderNilBodyWriteMessage)]
async fn encoder_nil_body_write_message_async_<'m, 'w, 'c, M, W, C>(
    message: &'m mut M,
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
    message: &'f mut M,
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
