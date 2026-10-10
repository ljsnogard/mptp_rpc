use abs_buff::{TrBuffRead, TrBuffWrite, buffer::TrProducerState, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::{
    basic::{EncodedBody, Nothing, Request, set_body_size_header_},
    io_::{
        CountingWrite, MessageIoError, WaitingTx, check_body_size_, decode_from_async_,
        read_body_async_, try_get_body_size_,
    },
};
use crate::{
    access_method::{AccessMethod, TrAccessMethod, method_of},
    messaging,
    specs::{HeaderVal, Headers, StdHeaderKey, StdHeaderVal},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// RequestBuilder
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 请求构造失败的原因。
#[derive(Debug, Error)]
pub enum RequestBuildError {
    /// 还没指定 access method。
    #[error("请求缺少 access method")]
    MissingMethod,

    /// 还没指定 path。
    #[error("请求缺少 path")]
    MissingPath,

    /// 报文体编码失败。
    #[error("请求体编码失败：{0}")]
    BodyEncode(String),

    /// `Body_Size` 头声明的长度与实际体字节数不符。
    ///
    /// 这是装配错误：两者不一致时接收方会按头声明的长度切分字节，从而破坏流对齐。
    #[error("Body_Size 头声明 {declared} 字节，实际体有 {actual} 字节")]
    BodySizeMismatch { declared: usize, actual: usize },
}

/// [`RequestBuilder`] 的内部状态。
///
/// 放在 `Box` 里是为了让 [`RequestBuilder`] 自身保持瘦指针：builder 沿着调用链一路
/// 按值传递（每个 setter 都返回 `Self`），把状态挂在堆上可以让这些传递只搬指针。
struct ReqBuilderInner {
    method_: Option<AccessMethod>,

    path_: Option<String>,

    headers_: Option<Headers>,

    /// **已经编好**的报文体字节。
    body_: Option<Vec<u8>>,

    /// 体编码失败的原因。setter 一律返回 `Self`，没有位置报错，于是失败被记在这里，
    /// 等 [`RequestBuilder::build`] 一并交还调用方。
    body_err_: Option<String>,
}

impl ReqBuilderInner {
    const fn new() -> Self {
        ReqBuilderInner {
            method_: Option::None,
            path_: Option::None,
            headers_: Option::None,
            body_: Option::None,
            body_err_: Option::None,
        }
    }
}

/// 一次请求的构造器：`method + path + headers + body`。
///
/// # 体的两种给法
///
/// - [`RequestBuilder::body`]：给一个业务类型，按 MessagePack 编成字节，并自动写上
///   `Body_Size` 与 `Body_Type`（`Mime_Body_Type_MsgPack`）；
/// - [`RequestBuilder::body_bytes`]：直接给已经编好的字节，只写 `Body_Size`。
///
/// # 构造器会在构造期把体编掉
///
/// builder 自身不是泛型的，存不下任意业务类型，所以 [`RequestBuilder::body`] 在**构造
/// 期**完成编码，产出的体是 [`EncodedBody`]。这对「攒一条请求再发出去」的用法没有额外
/// 往返（编码总要做一次），但如果你希望编码发生在**写出时**、字节直接落进 ring，那就
/// 绕过 builder，用 [`Request::with_measured_body`] 直接挂上业务类型——那条路径连这次
/// 构造期编码也不会有。
///
/// # Examples
///
/// ```
/// use mptp_core::{
///     access_method::AccessMethod,
///     messaging::{TrRpcRequest, request::RequestBuilder},
/// };
///
/// let req = RequestBuilder::new()
///     .method(AccessMethod::Post)
///     .path("/topic/chat")
///     .body("hi")
///     .build()
///     .expect("method 与 path 都给齐了，应当构造成功");
///
/// assert_eq!(req.method(), AccessMethod::Post);
/// assert_eq!(req.location(), "/topic/chat");
/// // MessagePack 的 "hi" 是 3 个字节，`body` 入口顺手把 Body_Size 也写好了。
/// assert_eq!(req.try_body_len().expect("体可编码"), Some(3usize));
/// ```
pub struct RequestBuilder(Box<ReqBuilderInner>);

impl RequestBuilder {
    /// 创建一个空构造器。
    pub fn new() -> Self {
        RequestBuilder(Box::new(ReqBuilderInner::new()))
    }

    /// 创建一个 access method 已经定好的构造器。
    ///
    /// 泛型参数是 [`TrAccessMethod`] 的编译期标记（`View` / `Post` / …），于是
    /// 「用哪个方法」在类型里就写死了，调用点不会写错取值。
    pub fn builder<M: TrAccessMethod>() -> Self {
        Self::new().method(method_of::<M>())
    }

    /// 指定 access method。
    pub fn method(mut self, method: AccessMethod) -> Self {
        self.0.method_ = Option::Some(method);
        self
    }

    /// 指定资源路径。
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.0.path_ = Option::Some(path.into());
        self
    }

    /// 指定一组头，**替换**此前设置的全部头。
    ///
    /// 用 `TryInto` 而不是 `Into`：头的转换本身可能失败，失败时保持原有头不变。
    pub fn headers(mut self, headers: impl TryInto<Headers>) -> Self {
        let Result::Ok(headers) = headers.try_into() else {
            return self;
        };
        self.0.headers_ = Option::Some(headers);
        self
    }

    /// 按 MessagePack 编入一个业务类型作为报文体。
    ///
    /// 同时写上两个标准头：`Body_Size`（编出来的字节数）与 `Body_Type`
    /// （`Mime_Body_Type_MsgPack`）。`Data_Type_Id` **不**在这里写：泛型函数拿不到
    /// 具体类型名，需要它的调用方用
    /// [`HeadersBuilder`](crate::client::HeadersBuilder) 显式设置。
    ///
    /// 编码失败不会在这里暴露，而是在 [`RequestBuilder::build`] 时以
    /// [`RequestBuildError::BodyEncode`] 报出。
    pub fn body<T>(mut self, body: T) -> Self
    where
        T: Serialize,
    {
        match rmp_serde::to_vec(&body) {
            Result::Ok(bytes) => self.set_body_bytes_(bytes),
            Result::Err(err) => {
                self.0.body_ = Option::None;
                self.0.body_err_ = Option::Some(err.to_string());
            }
        }
        self
    }

    /// 直接给**已经编好**的报文体字节，只写 `Body_Size` 头。
    pub fn body_bytes(mut self, bytes: impl Into<Vec<u8>>) -> Self {
        self.set_body_bytes_(bytes.into());
        self
    }

    /// 收尾：产出一条可发送的请求。
    ///
    /// 体的长度会与 `Body_Size` 头对齐：头缺失就补上；头已存在但不等于实际字节数则报
    /// [`RequestBuildError::BodySizeMismatch`]——**不静默改写**，那会把装配错误藏起来。
    ///
    /// # Errors
    ///
    /// method 或 path 缺失、体编码失败、体长度与头声明不符时返回错误。
    pub fn build(self) -> Result<Request<EncodedBody, Nothing>, RequestBuildError> {
        let ReqBuilderInner {
            method_,
            path_,
            headers_,
            body_,
            body_err_,
        } = *self.0;

        if let Option::Some(err) = body_err_ {
            return Result::Err(RequestBuildError::BodyEncode(err));
        }
        let Option::Some(method) = method_ else {
            return Result::Err(RequestBuildError::MissingMethod);
        };
        let Option::Some(path) = path_ else {
            return Result::Err(RequestBuildError::MissingPath);
        };

        let actual = body_.as_ref().map_or(0usize, Vec::len);
        let declared = try_get_body_size_(headers_.as_ref())
            .map_err(|err| RequestBuildError::BodyEncode(err.to_string()))?;
        let mut headers = headers_.unwrap_or_default();
        if headers
            .try_get_header(&StdHeaderKey::Body_Size.into())
            .is_some()
        {
            if declared != actual {
                return Result::Err(RequestBuildError::BodySizeMismatch { declared, actual });
            }
        } else if actual > 0usize {
            set_body_size_header_(&mut headers, actual);
        }

        let mut req: Request<EncodedBody, Nothing> = Request::new(method, path);
        if headers.iter_headers().into_iter().next().is_some() {
            req = req.with_headers(headers);
        }
        if let Option::Some(bytes) = body_ {
            req = req.with_body(EncodedBody::new(bytes));
        }
        Result::Ok(req)
    }

    /// 记下体字节，并同步 `Body_Size` / `Body_Type` 两个头。
    fn set_body_bytes_(&mut self, bytes: Vec<u8>) {
        let size = bytes.len();
        self.0.body_err_ = Option::None;
        self.0.body_ = Option::Some(bytes);
        let headers = self.0.headers_.get_or_insert_with(Headers::new);
        set_body_size_header_(headers, size);
        headers.add_or_set_header(
            &StdHeaderKey::Body_Type.into(),
            &HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack),
        );
    }
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Request 的线上形态
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 请求前缀的线上形态：`method + path + headers`。
///
/// 它是解码过程中的中间态而不是公开类型：调用者要的是「请求」这个概念，而前缀只是
/// 它在流上的一段表示。
#[derive(Debug)]
pub(crate) struct ReqPrefix(pub AccessMethod, pub String, pub Option<Headers>);

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// io utils when sending and receiving request from IO.
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写出请求前缀（`method` / `location` / `headers`），返回写出的字节数。
///
/// 三个值**依次编进同一个写口**：线上格式就是「三个连续的 MessagePack 值」，接收端
/// 也是逐个解出来的。用一个元组一次序列化会多写一个数组头，改变线上字节。
///
/// 字节直接落进 ring 的可用段：`AsStdWrite` 把 `rmp-serde` 的每一段写进环，中间没有
/// 第二块内存。环满时由 [`WaitingTx`] 让它等对端腾出空间，而不是把字节攒在别处。
///
/// # Errors
///
/// 编码失败或写 ring 失败时返回错误。
pub(crate) async fn send_request_prefix_async<'f, TyReq, TyTx, TyTok>(
    req: &'f TyReq,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyReq: messaging::TrRpcRequest,
    TyTx: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let mut waiting = WaitingTx::new_(tx);
    let mut write = CountingWrite::new_(AsStdWrite::new(&mut waiting, tok));
    {
        let mut serializer = rmp_serde::Serializer::new(&mut write);
        req.method()
            .serialize(&mut serializer)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?;
        req.location()
            .serialize(&mut serializer)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?;
        req.headers()
            .serialize(&mut serializer)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    }
    Ok(write.written_())
}

/// 写出一条完整的请求：前缀 + 报文体。
///
/// 写出之前核对 `Body_Size` 头与体的实际长度；不一致时**宁可失败也不写出**——那样的
/// 报文会让接收方按错误的长度切分后续字节。
///
/// # Errors
///
/// 编码失败、`Body_Size` 与体长度不符，或写 ring 失败时返回错误。
pub(crate) async fn send_request_async<'f, TyReq, TyTx, TyTok>(
    req: &'f TyReq,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyReq: messaging::TrRpcRequest,
    TyTx: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let body_len = req
        .try_body_len()
        .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    check_body_size_(req.headers(), body_len)?;

    let written = send_request_prefix_async(req, tx, tok.child_token()).await?;

    // 体继续写进 ring 的发送半边：中间没有第二块内存。
    let mut waiting = WaitingTx::new_(tx);
    let mut write = CountingWrite::new_(AsStdWrite::new(&mut waiting, tok));
    let declared = req
        .try_write_body(&mut write)
        .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    let actual = write.written_();
    if let Option::Some(declared) = declared
        && declared != actual
    {
        return Err(MessageIoError::BodySizeMismatch { declared, actual });
    }
    Ok(written + actual)
}

/// 解码请求前缀（`method` / `location` / `headers`）。
///
/// 直接从 ring 的接收半边解出来：`rmp-serde` 要多少字节读多少字节，因此**不会**碰到
/// 报文体的第一个字节，也就不需要任何预读缓冲。
///
/// # Errors
///
/// 报文不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub(crate) async fn recv_request_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    tok: TyTok,
) -> Result<ReqPrefix, MessageIoError>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let method = decode_from_async_::<AccessMethod, _, _>(rx, tok.child_token()).await?;
    let location = decode_from_async_::<String, _, _>(rx, tok.child_token()).await?;
    let headers = decode_from_async_::<Option<Headers>, _, _>(rx, tok.child_token()).await?;
    Ok(ReqPrefix(method, location, headers))
}

/// 按 `Body_Size` 头把请求体解成一个业务类型。
///
/// 这是 handler 读请求体的推荐入口：
///
/// - 边界完全由协议头决定：头缺省或为 0 时**一个字节都不读**，返回 `None`；
/// - 解码**直接发生在 ring 的接收半边上**，中间没有中转缓冲；
/// - 有长度时包成 `Read::take(size)`，即使对端多写了字节也不会被这条请求读走。
///
/// # Errors
///
/// 头值不合法、体不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub async fn recv_request_body_async<T, TyRx, TyTok>(
    rx: &mut TyRx,
    headers: Option<&Headers>,
    tok: TyTok,
) -> Result<Option<T>, MessageIoError>
where
    T: DeserializeOwned,
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    read_body_async_(rx, headers, tok).await
}
