use core::marker::PhantomData;
use std::io;

use serde::Serialize;
use thiserror::Error;

use crate::{
    access_method::AccessMethod,
    codec::Codec,
    specs::{HeaderVal, Headers, Status, StdHeaderKey, StdHeaderVal},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// TrRpcBody
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 报文体的**内容来源**。
///
/// 实现这一个 trait 回答两件事：这条报文**有没有体**、以及怎么把它**写出去**。
///
/// # 为什么编码发生在「写出去」的那一刻
///
/// 体的编码规则属于业务类型（谁产生内容谁决定），而不属于协议层。协议层不预量长度、
/// 不为体攒缓冲，只是在发送时把 `sink` 递过去：
///
/// - **定长**：`sink` 是一个限长写口，写出量必须恰好等于头里声明的长度；
/// - **分块**：`sink` 是一个分块写口，**写多少就是一块多大**——序列化器边编边写，
///   体的总长直到发完都不必知道（见 `dev-notes/body-transfer-20261010-2020.md` §1、§3.1）。
///
/// 因此这里没有「先编一遍量长度」的方法：那要求体可重放，也把「边编边发」堵死。
///
/// # 为什么体不是「任意 `Serialize` 类型」
///
/// 「用什么格式编」是调用方的选择，不是类型系统能推出来的事实。业务值要用
/// [`CodableBody`] 显式配上编码格式；已经编好的字节就是 `Vec<u8>` 自己；没有体用
/// [`Nothing`]。协议层不接受「恰好实现了 `Serialize`」就自动成为体的做法——那会把
/// 格式写死成某一种，也不是这个库该替使用者做的决定。
pub trait TrRpcBody {
    /// 本条报文是否有体。
    ///
    /// 返回 `false` 时发送路径**不会**调用 [`TrRpcBody::try_encode_into`]，接收方也
    /// 一个字节都不读。
    fn has_body(&self) -> bool {
        true
    }

    /// 把体的内容写进 `sink`，返回写出的字节数。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError>;

    /// 若长度**事先已知**，给出它；未知则 `None`。
    ///
    /// 它不是「能不能算出来」的问题，而是「现在手上有没有」：已经编好的字节有长度，
    /// 尚未编码的业务值没有。协议层只用它做装配期的核对（声明了定长、而体又自称长度
    /// 不同，就当场报错），**不**拿它去找长度。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        Ok(Option::None)
    }

    /// 本体的编码格式对应的 `Body_Type` 标准头取值；答不出来就返回 `None`。
    ///
    /// 已经编好的原始字节（`Vec<u8>`）不携带格式信息，因此默认是 `None`；配上
    /// 编码格式的 [`CodableBody`] 会给出它实际用的格式——`Body_Type` 该由**编码格式**
    /// 决定，不该让调用方手写第二遍。
    fn body_type_val(&self) -> Option<StdHeaderVal> {
        Option::None
    }
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
/// 它是一个**独立类型**而不是 `()`：`()` 也可以被序列化成 nil，若拿它当「没有体」的
/// 标记，就分不清「没有体」与「体是一个 nil」了。这里刻意让它不参与任何编码。
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Nothing;

impl TrRpcBody for Nothing {
    #[inline]
    fn has_body(&self) -> bool {
        false
    }

    #[inline]
    fn try_encode_into(&self, _sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError> {
        Ok(0usize)
    }
}

/// 字节串直接就是体：已经编好的内容不必再套一层壳。
///
/// `Vec<u8>` 是「内容已经在手上」这条事实的最朴素载体——从别处转发来的字节、调用方自己
/// 用别的工具编好的内容、乃至一段原始二进制，都是它。写出时原样交给 `sink`，不做二次
/// 编码；长度因此是**已知**的（[`TrRpcBody::try_known_len`] 会给出它），定长模式下可以
/// 直接声明 `Body_Size`。
///
/// 要发的是**业务值**时用 [`CodableBody`]；要把一段 `&[u8]` 按流的形状发出去（不构造
/// 报文对象）时，用 [`send_content_async`](super::body::send_content_async)。
impl TrRpcBody for Vec<u8> {
    #[inline]
    fn has_body(&self) -> bool {
        !self.is_empty()
    }

    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError> {
        sink.write_all(self.as_slice())
            .map_err(|err| BodyEncodeError::Io(err.to_string()))?;
        Ok(self.len())
    }

    #[inline]
    fn try_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        Ok(Option::Some(self.len()))
    }
}

/// **随发送而编码**的体：业务值与它的编码格式打包在一起。
///
/// 它是「值体」的标准载体，也是 [`RequestBuilder`](super::request::RequestBuilder) 的
/// `body` / `body_with` 入口存下来的东西。构造它**不做任何编码**——编码发生在
/// [`TrRpcBody::try_encode_into`]，也就是真正往连接里写的那一刻：
///
/// - 大 body 不会卡住构造请求的那一行；
/// - 编码器把字节逐段写进 `sink`，路径上没有中转缓冲，因此天然支持「边序列化边发送」；
/// - 总长在写出去之前谁都不知道，所以它只配合**分块**传输（`try_known_len` 恒为 `None`）。
///
/// # Examples
///
/// ```
/// use mptp_core::{
///     codec::Codec,
///     messaging::{CodableBody, TrRpcBody},
/// };
///
/// let body = CodableBody::new("hi".to_string(), Codec::MsgPack);
/// // 构造它不会做任何编码，长度自然也不知道。
/// assert!(body.try_known_len().expect("不该失败").is_none());
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodableBody<TyData> {
    data_: TyData,
    codec_: Codec,
}

impl<TyData> CodableBody<TyData> {
    /// 把业务值与它的编码格式打包。
    pub const fn new(data: TyData, codec: Codec) -> Self {
        CodableBody {
            data_: data,
            codec_: codec,
        }
    }

    /// 本体的编码格式。
    pub const fn codec(&self) -> Codec {
        self.codec_
    }

    /// 业务值本身。
    pub const fn data(&self) -> &TyData {
        &self.data_
    }

    /// 交出业务值。
    pub fn into_data(self) -> TyData {
        self.data_
    }
}

impl<TyData> TrRpcBody for CodableBody<TyData>
where
    TyData: Serialize,
{
    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError> {
        let mut count = CountingSink::new_(sink);
        match self.codec_ {
            // 编码器逐段写进 `sink`：不经过任何中转缓冲，写出多少字节只有写完才知道。
            Codec::MsgPack => rmp_serde::encode::write(&mut count, &self.data_)
                .map_err(|err| BodyEncodeError::Encode(err.to_string()))?,
            Codec::Json => {
                return Result::Err(BodyEncodeError::Encode(
                    "JSON 编码尚未实现".to_string(),
                ));
            }
        }
        Ok(count.written_())
    }

    #[inline]
    fn body_type_val(&self) -> Option<StdHeaderVal> {
        Option::Some(self.codec_.body_type_val())
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

/// 往头里写下「体是分块传输的」这条声明。
pub(crate) fn set_chunked_transfer_header_(headers: &mut Headers) {
    headers.add_or_set_header(
        &StdHeaderKey::Body_Transfer.into(),
        &HeaderVal::from(StdHeaderVal::Body_Transfer_Chunked),
    );
}

/// 按体的自述补上 `Body_Type` 头（体答不出格式时什么都不做）。
pub(crate) fn set_body_type_from_body_<TyBody>(headers: &mut Headers, body: &TyBody)
where
    TyBody: TrRpcBody,
{
    if let Option::Some(val) = body.body_type_val() {
        headers.add_or_set_header(&StdHeaderKey::Body_Type.into(), &HeaderVal::from(val));
    }
}

/// 给 `&mut dyn Write` 套一层计数：体要回报自己写出了多少字节。
///
/// 它只加一个 `usize`，不缓冲任何数据——记账与缓冲是两件事，这里刻意只做前者。
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

    /// 本条请求**有没有体**。
    fn has_body(&self) -> bool;

    /// 体的长度若**事先已知**，给出它；未知则 `None`。
    ///
    /// 它是装配期核对的依据：头里声明了定长、而体自述的长度不同，就应当在上网之前失败。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_body_known_len(&self) -> Result<Option<usize>, BodyEncodeError>;

    /// 把本条请求的体编进 `sink`，返回写出的字节数；没有体时返回 `None`。
    ///
    /// `sink` 由发送路径按头里声明的模式给出：定长是一个限长写口（写出量必须恰好等于
    /// 声明的长度），分块是一个分块写口（写多少就是一块多大）。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_write_body(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError>;
}

pub trait TrRpcResponse
where
    Self: TrRpcMessage + Sized,
{
    fn status(&self) -> Status;

    /// 本条回复**有没有体**。
    fn has_body(&self) -> bool;

    /// 体的长度若**事先已知**，给出它；未知则 `None`。语义同
    /// [`TrRpcRequest::try_body_known_len`]。
    ///
    /// # Errors
    ///
    /// 体编码失败时返回错误。
    fn try_body_known_len(&self) -> Result<Option<usize>, BodyEncodeError>;

    /// 把本条回复的体编进 `sink`，语义同 [`TrRpcRequest::try_write_body`]。
    ///
    /// # Errors
    ///
    /// 体编码失败或写 `sink` 失败时返回错误。
    fn try_write_body(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError>;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// The content that a client will send to the server and ask for something.
///
/// A request may or may not have a body. When it has one, the standard
/// headers must declare **how it is transferred**: either its exact length
/// (`Body_Size`) or that it comes in chunks (`Body_Transfer: Chunked`).
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
    /// 这里只按值存下体，**不做**任何编码：编码发生在发送的那一刻（见 [`TrRpcBody`]）。
    /// 要顺手把传输模式的头也写好，用 [`Request::with_sized_body`] 或
    /// [`Request::with_chunked_body`]——体的长度是否已知，决定了能用哪一个。
    pub fn with_body(mut self, body: TyBody) -> Self {
        self.body_ = Option::Some(body);
        self
    }

    /// 由方法、路径与体造一条请求，并声明体按**定长**传输。
    ///
    /// 前提是体自己的长度**事先已知**（例如 `Vec<u8>`）：协议层不会为了填
    /// `Body_Size` 去把体编一遍——那会要求体可重放，也把「边编边发」堵死。长度未知的体
    /// 请走 [`Request::with_chunked_body`]。
    ///
    /// # Errors
    ///
    /// 体长度未知、或体编码失败时返回错误。
    pub fn with_sized_body(
        method: AccessMethod,
        path: impl Into<String>,
        body: TyBody,
    ) -> Result<Self, BodyEncodeError>
    where
        TyBody: TrRpcBody,
    {
        let Some(len) = body.try_known_len()? else {
            return Result::Err(BodyEncodeError::Encode(
                "体的长度事先未知，无法声明 Body_Size；请改用分块传输".to_string(),
            ));
        };
        let mut headers = Headers::new();
        set_body_size_header_(&mut headers, len);
        set_body_type_from_body_(&mut headers, &body);
        Ok(Request::new(method, path)
            .with_headers(headers)
            .with_body(body))
    }

    /// 由方法、路径与体造一条请求，并声明体按**分块**传输。
    ///
    /// 这是「值体」的标准入口：总长直到发完都不必知道，编码器边编边写，写多少就是一块
    /// 多大（见 `dev-notes/body-transfer-20261010-2020.md` §1）。
    pub fn with_chunked_body(
        method: AccessMethod,
        path: impl Into<String>,
        body: TyBody,
    ) -> Self
    where
        TyBody: TrRpcBody,
    {
        let mut headers = Headers::new();
        set_chunked_transfer_header_(&mut headers);
        set_body_type_from_body_(&mut headers, &body);
        Request::new(method, path)
            .with_headers(headers)
            .with_body(body)
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
    fn has_body(&self) -> bool {
        self.body_.as_ref().is_some_and(TrRpcBody::has_body)
    }

    #[inline]
    fn try_body_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_known_len(),
            Option::None => Ok(Option::None),
        }
    }

    #[inline]
    fn try_write_body(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) if body.has_body() => body.try_encode_into(sink).map(Option::Some),
            _ => Ok(Option::None),
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// The content that the server will react to the client when being asked for
/// something.
///
/// A response may or may not have a body. When it has one, the standard
/// headers must declare **how it is transferred**, with the same two
/// mutually exclusive forms as a request.
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

    /// 由状态码与体造一条回复，并声明体按**定长**传输。语义同
    /// [`Request::with_sized_body`]。
    ///
    /// # Errors
    ///
    /// 体长度未知、或体编码失败时返回错误。
    pub fn with_sized_body(status: Status, body: TyBody) -> Result<Self, BodyEncodeError>
    where
        TyBody: TrRpcBody,
    {
        let Some(len) = body.try_known_len()? else {
            return Result::Err(BodyEncodeError::Encode(
                "体的长度事先未知，无法声明 Body_Size；请改用分块传输".to_string(),
            ));
        };
        let mut headers = Headers::new();
        set_body_size_header_(&mut headers, len);
        set_body_type_from_body_(&mut headers, &body);
        Ok(Response::new(status)
            .with_headers(headers)
            .with_body(body))
    }

    /// 由状态码与体造一条回复，并声明体按**分块**传输。语义同
    /// [`Request::with_chunked_body`]。
    pub fn with_chunked_body(status: Status, body: TyBody) -> Self
    where
        TyBody: TrRpcBody,
    {
        let mut headers = Headers::new();
        set_chunked_transfer_header_(&mut headers);
        set_body_type_from_body_(&mut headers, &body);
        Response::new(status)
            .with_headers(headers)
            .with_body(body)
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
    fn has_body(&self) -> bool {
        self.body_.as_ref().is_some_and(TrRpcBody::has_body)
    }

    #[inline]
    fn try_body_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) => body.try_known_len(),
            Option::None => Ok(Option::None),
        }
    }

    #[inline]
    fn try_write_body(&self, sink: &mut dyn io::Write) -> Result<Option<usize>, BodyEncodeError> {
        match self.body_.as_ref() {
            Option::Some(body) if body.has_body() => body.try_encode_into(sink).map(Option::Some),
            _ => Ok(Option::None),
        }
    }
}
