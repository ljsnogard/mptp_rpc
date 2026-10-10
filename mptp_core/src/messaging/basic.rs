use core::marker::PhantomData;
use std::io;

use serde::Serialize;
use thiserror::Error;

use crate::{
    access_method::AccessMethod,
    specs::{HeaderVal, Headers, Status, StdHeaderKey, StdHeaderVal},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrRpcBody
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 报文体的**线上编码**。
///
/// 实现这一个 trait 同时回答两个问题：体有多少字节、以及怎么把它写出去。两者都
/// **不经过任何中转缓冲**——长度用「只数不写」的 sink 量出来，字节直接写进调用方给的
/// `sink`（通常就是 ring 的写半边）。
///
/// # 为什么不是「体的字节视图」
///
/// 换成 `fn try_body_bytes(&self) -> Option<&[u8]>` 会逼着每个体类型先把编码结果落在
/// 某块内存里（否则交不出 `&[u8]`），而那正是要杜绝的那次分配。这里反过来：体自己知道
/// 怎么把内容**流进**一个 `Write`，调用方给什么就写什么。
///
/// # 为什么这两个方法是同步的
///
/// 它们包住的正是 `serde`——本框架唯一允许暂时用同步代码的地方。同步停在这一层，
/// 不会外溢成调用方的语义：协议层的每个 IO 入口仍然是 `async`。
///
/// # Examples
///
/// ```
/// use mptp_core::messaging::{Nothing, TrRpcBody};
///
/// // 「没有体」是一个明确的类型，而不是一个恰好编成 nil 的值。
/// assert!(TrRpcBody::try_encoded_len(&Nothing).expect("不该失败").is_none());
///
/// // 任何 `Serialize` 类型都可以直接当体，长度与写出量由同一个编码器决定。
/// assert_eq!(TrRpcBody::try_encoded_len(&"hi").expect("不该失败"), Some(3usize));
/// ```
pub trait TrRpcBody {
    /// 体在线上占多少字节；`None` 表示这条报文**没有体**。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_encoded_len(&self) -> Result<Option<usize>, BodyEncodeError>;

    /// 把自己编进 `sink`，返回写出的字节数；`None` 表示没有体。
    ///
    /// 返回的数目必须与 [`TrRpcBody::try_encoded_len`] 一致——写出方会当场核对。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError>;
}

/// 体编码失败的原因。
#[derive(Clone, Debug, Error)]
pub enum BodyEncodeError {
    /// 体本身无法编码。
    #[error("体编码失败：{0}")]
    Encode(String),

    /// 往 `sink` 写出时失败。
    #[error("体写出失败：{0}")]
    Io(String),
}

/// 「什么都没有」：既表示报文**没有体**，也用作 suffix stream 的缺省标记。
///
/// 它是一个**独立类型**而不是 `()`：`()` 也实现了 `Serialize`，若拿它当「没有体」的
/// 标记，就会与 [`TrRpcBody`] 的批量实现撞在一起（编译器无法区分「`()` 表示空」与
/// 「`()` 编成 nil」）。这里刻意让它**不实现** `Serialize`，两件事于是泾渭分明。
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Nothing;

impl TrRpcBody for Nothing {
    #[inline]
    fn try_encoded_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        Ok(Option::None)
    }

    #[inline]
    fn try_encode_into(
        &self,
        _sink: &mut dyn io::Write,
    ) -> Result<Option<usize>, BodyEncodeError> {
        Ok(Option::None)
    }
}

/// 已经按 MessagePack **编好**的报文体。
///
/// 它是「体已经在内存里了」这条事实的显式载体：构造它的那一刻编码就已经完成，写出时
/// 只是把这串字节原样搬进 ring，不再二次编码。
///
/// 想省掉这次内存里的编码，就直接把业务类型本身当体（`Request<MyMsg, Nothing>`）：
/// 那样编码发生在写出时，字节直接落进 ring。
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EncodedBody {
    bytes_: Vec<u8>,
}

impl EncodedBody {
    /// 由已经编好的字节构造。
    pub const fn new(bytes: Vec<u8>) -> Self {
        EncodedBody { bytes_: bytes }
    }

    /// 已编好的字节。
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes_.as_slice()
    }

    /// 交出内部的字节。
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes_
    }
}

impl TrRpcBody for EncodedBody {
    #[inline]
    fn try_encoded_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        Ok(Option::Some(self.bytes_.len()))
    }

    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError> {
        sink.write_all(self.bytes_.as_slice())
            .map_err(|err| BodyEncodeError::Io(err.to_string()))?;
        Ok(Option::Some(self.bytes_.len()))
    }
}

/// 只累加长度、不保存任何字节的 sink。
///
/// 它是「先编码一次拿长度」这件事不落缓冲的关键：编码器照样跑一遍，产物只留一个计数。
#[derive(Clone, Copy, Debug, Default)]
struct CountSink {
    count_: usize,
}

impl io::Write for CountSink {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.count_ += buf.len();
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// 量出一个 `Serialize` 值按 MessagePack 编出来会占多少字节。
fn measure_<T>(value: &T) -> Result<usize, BodyEncodeError>
where
    T: Serialize,
{
    let mut sink = CountSink::default();
    rmp_serde::encode::write(&mut sink, value)
        .map_err(|err| BodyEncodeError::Encode(err.to_string()))?;
    Ok(sink.count_)
}

// 任何 `Serialize` 类型都可以**直接**当报文体：编成 MessagePack，字节流进 sink。
// `Nothing` 与 `EncodedBody` 都不实现 `Serialize`，因此与这条批量实现不冲突。
impl<T> TrRpcBody for T
where
    T: Serialize,
{
    #[inline]
    fn try_encoded_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        measure_(self).map(Option::Some)
    }

    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError> {
        let mut count = CountingSink::new_(sink);
        rmp_serde::encode::write(&mut count, self)
            .map_err(|err| BodyEncodeError::Encode(err.to_string()))?;
        Ok(Option::Some(count.written_()))
    }
}

/// 把 `Body_Size` 头写成 `size` 字节。
///
/// 值优先用数字形态；只有超出 `u16` 表达能力时才退化成十进制字符串——协议允许
/// `HeaderVal` 是「字符串或 u16」的联合体，两种形态接收方都认。
pub(crate) fn set_body_size_header_(headers: &mut Headers, size: usize) {
    let key = StdHeaderKey::Body_Size.into();
    let val = match u16::try_from(size) {
        Result::Ok(small) => HeaderVal::from_u16(small),
        Result::Err(_) => HeaderVal::from_string(size.to_string()),
    };
    headers.add_or_set_header(&key, &val);
}

/// 按体的实际长度攒出 `Body_Size` / `Body_Type` 两个头。
///
/// 长度用「只数不写」的方式量出来，因此这一步**不落任何缓冲**。没有体
/// （`try_encoded_len` 给出 `None`）时返回空头：此时协议上也就不该出现这两个头。
///
/// # Errors
///
/// 体编码失败时返回错误。
pub(crate) fn measured_headers_<TyBody>(body: &TyBody) -> Result<Headers, BodyEncodeError>
where
    TyBody: TrRpcBody,
{
    let mut headers = Headers::new();
    if let Option::Some(len) = body.try_encoded_len()? {
        set_body_size_header_(&mut headers, len);
        headers.add_or_set_header(
            &StdHeaderKey::Body_Type.into(),
            &HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack),
        );
    }
    Ok(headers)
}

/// 给 `&mut dyn Write` 套一层计数，好让体的批量实现能回报写出量。
struct CountingSink<'a> {
    inner_: &'a mut dyn io::Write,
    written_: usize,
}

impl<'a> CountingSink<'a> {
    const fn new_(inner: &'a mut dyn io::Write) -> Self {
        CountingSink {
            inner_: inner,
            written_: 0usize,
        }
    }

    const fn written_(&self) -> usize {
        self.written_
    }
}

impl io::Write for CountingSink<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner_.write(buf)?;
        self.written_ += written;
        Ok(written)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.inner_.flush()
    }
}

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

    /// 本条请求的体在线上占多少字节；`None` 表示没有体。
    ///
    /// 写出方据此核对 `Body_Size` 头；`None` 对应「头缺省或为 0」，两者必须一致。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_body_len(&self) -> Result<Option<usize>, BodyEncodeError>;

    /// 把本条请求的体编进 `sink`。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_write_body(&self, sink: &mut dyn io::Write)
    -> Result<Option<usize>, BodyEncodeError>;
}

pub trait TrRpcResponse
where
    Self: TrRpcMessage + Sized,
{
    fn status(&self) -> Status;

    /// 本条回复的体在线上占多少字节；`None` 表示没有体。语义同
    /// [`TrRpcRequest::try_body_len`]。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_body_len(&self) -> Result<Option<usize>, BodyEncodeError>;

    /// 把本条回复的体编进 `sink`，语义同 [`TrRpcRequest::try_write_body`]。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_write_body(&self, sink: &mut dyn io::Write)
    -> Result<Option<usize>, BodyEncodeError>;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
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

    /// 给请求附加报文体。
    ///
    /// 这里只按值存下体，**不做**序列化：体的线上编码由 [`TrRpcBody`] 决定，发生在
    /// 写出的时候，字节直接落进 ring。要顺手把 `Body_Size` / `Body_Type` 头也写好，
    /// 用 [`Request::with_measured_body`]。
    pub fn with_body(mut self, body: TyBody) -> Self {
        self.body_ = Option::Some(body);
        self
    }

    /// 由方法、路径与体造一条请求，并顺手把 `Body_Size` / `Body_Type` 两个头写好。
    ///
    /// 这是「手工构造请求」的推荐入口：头里的长度与实际体长度必然一致，而核对这些正是
    /// 写出方会做的事（不一致会被拒绝写出）。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    pub fn with_measured_body(
        method: AccessMethod,
        path: impl Into<String>,
        body: TyBody,
    ) -> Result<Self, BodyEncodeError>
    where
        TyBody: TrRpcBody,
    {
        let headers = measured_headers_(&body)?;
        Ok(Request::new(method, path)
            .with_headers(headers)
            .with_body(body))
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

impl<TyBody, TyPush> TrRpcRequest for Request<TyBody, TyPush>
where
    TyBody: TrRpcBody,
{
    #[inline]
    fn method(&self) -> AccessMethod {
        Request::method(self)
    }

    #[inline]
    fn location(&self) -> &str {
        Request::location(self)
    }

    #[inline]
    fn try_body_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_encoded_len(),
            Option::None => Ok(Option::None),
        }
    }

    #[inline]
    fn try_write_body(
        &self,
        sink: &mut dyn io::Write,
    ) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_encode_into(sink),
            Option::None => Ok(Option::None),
        }
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

    /// 给回复附加一组头。
    pub fn with_headers(mut self, headers: impl Into<Headers>) -> Self {
        self.headers_ = Option::Some(headers.into());
        self
    }

    /// 给回复附加报文体，语义同 [`Request::with_body`]。
    pub fn with_body(mut self, body: TyBody) -> Self {
        self.body_ = Option::Some(body);
        self
    }

    /// 由状态码与体造一条回复，并顺手把 `Body_Size` / `Body_Type` 两个头写好。
    ///
    /// 语义同 [`Request::with_measured_body`]：handler 回体时用这个入口，头里的长度与
    /// 实际体长度必然一致。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    pub fn with_measured_body(status: Status, body: TyBody) -> Result<Self, BodyEncodeError>
    where
        TyBody: TrRpcBody,
    {
        let headers = measured_headers_(&body)?;
        Ok(Response::new(status)
            .with_headers(headers)
            .with_body(body))
    }

    pub const fn headers(&self) -> Option<&Headers> {
        self.headers_.as_ref()
    }

    /// 取回复头部的可变引用，供 handler 设置 `Body_Size` / `Body_Type` 等标准头。
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

impl<TyBody, TyPush> TrRpcResponse for Response<TyBody, TyPush>
where
    TyBody: TrRpcBody,
{
    #[inline]
    fn status(&self) -> Status {
        Response::status(self)
    }

    #[inline]
    fn try_body_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_encoded_len(),
            Option::None => Ok(Option::None),
        }
    }

    #[inline]
    fn try_write_body(
        &self,
        sink: &mut dyn io::Write,
    ) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_encode_into(sink),
            Option::None => Ok(Option::None),
        }
    }
}
