use std::io;

use abs_buff::{TrBuffRead, TrBuffWrite, buffer::TrProducerState, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::{
    basic::{
        BodyEncodeError, CodableBody, Nothing, Request, TrRpcBody,
        set_body_size_header_,
        set_body_type_from_body_,
        set_chunked_transfer_header_,
    },
    body::send_body_async,
    io_::{
        CountingWrite, MessageIoError, WaitingTx,
        decode_from_async_, read_body_async_, try_get_body_size_,
    },
};
use crate::{
    access_method::{AccessMethod, TrAccessMethod, method_of},
    codec::Codec,
    messaging,
    specs::{Headers, StdHeaderKey},
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

    /// 显式声明的定长长度与体自述的长度不符。
    ///
    /// 这是装配错误：两者不一致时接收方会按头声明的长度切分字节，从而破坏流对齐。
    #[error("Body_Size 头声明 {declared} 字节，体自述 {actual} 字节")]
    BodySizeMismatch { declared: usize, actual: usize },

    /// 体声明自相矛盾（例如既声明定长、又声明分块）。
    #[error("请求的体声明自相矛盾：{0}")]
    ConflictingTransfer(String),
}

/// [`RequestBuilder`] 的内部状态。
///
/// 放在 `Box` 里是为了让 builder 自身保持瘦指针：它沿着调用链一路按值传递（每个 setter
/// 都返回 `Self`），把状态挂在堆上可以让这些传递只搬指针。
struct ReqBuilderInner<TyBody> {
    method_: Option<AccessMethod>,

    path_: Option<String>,

    headers_: Option<Headers>,

    /// 报文体。**它还没有被编码过**——`body` 入口存下的只是「业务值 + 编码格式」的打包，
    /// 真正编码发生在发送时（见 [`TrRpcBody`]）。
    body_: Option<TyBody>,

    /// 调用方显式声明的定长长度；`None` 表示体按**分块**传输（值体的默认）。
    declared_size_: Option<usize>,
}

impl<TyBody> ReqBuilderInner<TyBody> {
    const fn new() -> Self {
        ReqBuilderInner {
            method_: Option::None,
            path_: Option::None,
            headers_: Option::None,
            body_: Option::None,
            declared_size_: Option::None,
        }
    }
}

/// 一次请求的构造器：`method + path + headers + body`。
///
/// # 体的给法
///
/// - [`RequestBuilder::body`]：给一个业务值，**默认按 MessagePack 编码**；它不会在这里
///   被编码，只是与编码格式一起打包存下；
/// - [`RequestBuilder::body_with`]：给业务值并明确指定编码格式——不指定才用默认值；
/// - [`RequestBuilder::body_bytes`]：给已经编好的字节。
///
/// 三者的共同点是：**编码都发生在发送的那一刻**，而不是构造请求的这一刻。因此大 body
/// 不会卡住构造这一行，序列化器也可以边编边写、不必先把结果攒齐。
///
/// # 传输模式
///
/// **值体默认分块**：总长事先不知道，也不该为了填 `Body_Size` 先编一遍。要用定长，显式
/// 调 [`RequestBuilder::body_size`] 声明长度（此时体自述的长度必须与之一致）。
///
/// # Examples
///
/// ```
/// use mptp_core::{
///     access_method::AccessMethod,
///     messaging::{CodableBody, TrRpcBody, request::RequestBuilder},
/// };
///
/// let req = RequestBuilder::new()
///     .method(AccessMethod::Post)
///     .path("/topic/chat")
///     .body("hi".to_string())
///     .build()
///     .expect("method 与 path 都给齐了，应当构造成功");
///
/// assert_eq!(req.method(), AccessMethod::Post);
/// assert_eq!(req.location(), "/topic/chat");
/// // 体没有被编码过，长度自然也不知道。
/// assert!(req.body().expect("有体").try_known_len().expect("不该失败").is_none());
/// ```
pub struct RequestBuilder<TyBody = Nothing> {
    inner_: Box<ReqBuilderInner<TyBody>>,
}

impl RequestBuilder<Nothing> {
    /// 创建一个空构造器。
    pub fn new() -> Self {
        RequestBuilder {
            inner_: Box::new(ReqBuilderInner::new()),
        }
    }

    /// 创建一个 access method 已经定好的构造器。
    ///
    /// 泛型参数是 [`TrAccessMethod`] 的编译期标记（`View` / `Post` / …），于是
    /// 「用哪个方法」在类型里就写死了，调用点不会写错取值。
    pub fn builder<M: TrAccessMethod>() -> Self {
        Self::new().method(method_of::<M>())
    }
}

impl<TyBody> RequestBuilder<TyBody> {
    /// 指定 access method。
    pub fn method(mut self, method: AccessMethod) -> Self {
        self.inner_.method_ = Option::Some(method);
        self
    }

    /// 指定资源路径。
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.inner_.path_ = Option::Some(path.into());
        self
    }

    /// 指定一组头，**替换**此前设置的全部头。
    ///
    /// 用 `TryInto` 而不是 `Into`：头的转换本身可能失败，失败时保持原有头不变。
    pub fn headers(mut self, headers: impl TryInto<Headers>) -> Self {
        let Result::Ok(headers) = headers.try_into() else {
            return self;
        };
        self.inner_.headers_ = Option::Some(headers);
        self
    }

    /// 声明本次体是**定长**传输，长度恰好 `size` 字节。
    ///
    /// 不调用它时体按分块传输（值体的默认）。声明之后：体若自述长度就当场核对，写出时
    /// 也会核对实际写出量，少一个字节都算截断。
    pub fn body_size(mut self, size: usize) -> Self {
        self.inner_.declared_size_ = Option::Some(size);
        self
    }

    /// 按 MessagePack 打包一个业务值作为报文体。
    ///
    /// 这里**不做编码**：存下的是「值 + `Codec::MsgPack`」，编码发生在发送时。想指定别的
    /// 格式用 [`RequestBuilder::body_with`]。
    pub fn body<TyData>(
        self,
        data: TyData,
    ) -> RequestBuilder<CodableBody<TyData>> {
        self.body_with(data, Codec::MsgPack)
    }

    /// 打包一个业务值，并**明确指定**它的编码格式。
    ///
    /// 同时记下这个格式对应的 `Body_Type`——对端据此知道该用什么解，不需要调用方再手写
    /// 一遍头。
    pub fn body_with<TyData>(
        self,
        data: TyData,
        codec: Codec,
    ) -> RequestBuilder<CodableBody<TyData>> {
        let RequestBuilder { inner_ } = self;
        let ReqBuilderInner {
            method_,
            path_,
            headers_,
            declared_size_,
            ..
        } = *inner_;
        RequestBuilder {
            inner_: Box::new(ReqBuilderInner {
                method_,
                path_,
                headers_,
                body_: Option::Some(CodableBody::new(data, codec)),
                declared_size_,
            }),
        }
    }

    /// 直接给**已经编好**的体字节。
    ///
    /// 字节串自己就是体（`Vec<u8>` 实现了 [`TrRpcBody`]），不必再套一层壳。长度因此是
    /// 已知的：想按定长发就再调一次 [`RequestBuilder::body_size`]；否则一样走分块。
    pub fn body_bytes(
        self,
        bytes: impl Into<Vec<u8>>,
    ) -> RequestBuilder<Vec<u8>> {
        let RequestBuilder { inner_ } = self;
        let ReqBuilderInner {
            method_,
            path_,
            headers_,
            declared_size_,
            ..
        } = *inner_;
        RequestBuilder {
            inner_: Box::new(ReqBuilderInner {
                method_,
                path_,
                headers_,
                body_: Option::Some(bytes.into()),
                declared_size_,
            }),
        }
    }

    /// 收尾：产出一条可发送的请求。
    ///
    /// 体**不会**在这一步被编码，这里只决定传输模式并把它写进头：
    ///
    /// 1. 头里已经显式写了 `Body_Size` 或分块声明 → 尊重调用方的写法；
    /// 2. 否则调过 [`RequestBuilder::body_size`] → 写 `Body_Size`；
    /// 3. 否则（有体）→ 写分块声明；
    /// 4. `Body_Type` 只在头里没有时才由体的编码格式补上。
    ///
    /// # Errors
    ///
    /// method / path 缺失、显式声明与体自述的长度不符、头里已有冲突的体声明时返回错误。
    pub fn build(self) -> Result<Request<TyBody, Nothing>, RequestBuildError>
    where
        TyBody: TrRpcBody,
    {
        let ReqBuilderInner {
            method_,
            path_,
            headers_,
            body_,
            declared_size_,
        } = *self.inner_;

        let Option::Some(method) = method_ else {
            return Result::Err(RequestBuildError::MissingMethod);
        };
        let Option::Some(path) = path_ else {
            return Result::Err(RequestBuildError::MissingPath);
        };

        let mut headers = headers_.unwrap_or_default();
        let has_body = body_.as_ref().is_some_and(TrRpcBody::has_body);

        if has_body {
            // 头里已有的体声明优先：调用方显式写下的东西，builder 不去改写它。
            let declared_in_header =
                try_get_body_size_(Option::Some(&headers)).map_err(|err| {
                    RequestBuildError::BodyEncode(err.to_string())
                })?;
            let has_size_header = headers
                .try_get_header(&StdHeaderKey::Body_Size.into())
                .is_some();
            let has_transfer_header = headers
                .try_get_header(&StdHeaderKey::Body_Transfer.into())
                .is_some();

            if has_size_header && has_transfer_header {
                return Result::Err(RequestBuildError::ConflictingTransfer(
                    "Body_Size 与 Body_Transfer 同时在场，体边界无从判定".to_string(),
                ));
            }

            match declared_size_ {
                // 调用方在 builder 上声明了定长。
                Option::Some(size) => {
                    if has_transfer_header {
                        return Result::Err(RequestBuildError::ConflictingTransfer(
                            "builder 声明了定长，头里却写了分块传输".to_string(),
                        ));
                    }
                    if has_size_header && declared_in_header != size {
                        return Result::Err(RequestBuildError::BodySizeMismatch {
                            declared: declared_in_header,
                            actual: size,
                        });
                    }
                    if let Option::Some(body) = body_.as_ref()
                        && let Option::Some(known) = body
                            .try_known_len()
                            .map_err(|err| RequestBuildError::BodyEncode(err.to_string()))?
                        && known != size
                    {
                        return Result::Err(RequestBuildError::BodySizeMismatch {
                            declared: size,
                            actual: known,
                        });
                    }
                    if !has_size_header {
                        set_body_size_header_(&mut headers, size);
                    }
                }
                // 没有声明长度：走分块（值体的默认）。
                Option::None => {
                    if has_size_header {
                        // 调用方自己写了长度，就按定长走，builder 不插手。
                    } else if !has_transfer_header {
                        set_chunked_transfer_header_(&mut headers);
                    }
                }
            }

            // `Body_Type` 由编码格式决定，只在头里没有时才补。
            if headers
                .try_get_header(&StdHeaderKey::Body_Type.into())
                .is_none()
                && let Option::Some(body) = body_.as_ref()
            {
                set_body_type_from_body_(&mut headers, body);
            }
        }

        let mut req: Request<TyBody, Nothing> = Request::new(method, path);
        if headers.iter_headers().into_iter().next().is_some() {
            req = req.with_headers(headers);
        }
        if let Option::Some(body) = body_ {
            req = req.with_body(body);
        }
        Result::Ok(req)
    }
}

impl Default for RequestBuilder<Nothing> {
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

/// 写出一条完整的请求：**先写前缀，再按头里声明的模式搬体**。
///
/// 两步是分开的：前缀的内容（method / path / headers）由协议实现自己决定，构造时就完全
/// 确定，所以可以先写出去、先把 ring 的空间腾出来；体则按 `Body_Size` 或分块声明搬运，
/// 长度不必事先知道（见 `dev-notes/body-transfer`）。
///
/// 体的字节必须**现成可借出**（[`TrRpcRequest::try_body_bytes`]）：协议层不为它临时
/// 分配缓存。需要发送「边编码边产生」的体时，先把它写进一块环，再从环的读半边取出来
/// 作为源。
///
/// # Errors
///
/// 头部体声明违规、体的字节借不出、体不足声明长度、编码失败，或写 ring 失败时返回错误。
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
    let written = send_request_prefix_async(req, tx, tok.child_token()).await?;

    // 体按头里声明的模式写出去：编码就发生在这一步，而不是构造请求的那一刻。
    let body_view = ReqBodyView { req_: req };
    let body = send_body_async(&body_view, tx, req.headers(), tok).await?;
    Result::Ok(written + body)
}

/// 把「一条请求的体」适配成 [`TrRpcBody`]，好交给统一的搬运入口。
///
/// 请求类型只承诺「能把体写出去」（[`messaging::TrRpcRequest::try_write_body`]），
/// 而搬运入口要的是一个体；这一层薄适配把两者接上，顺便把「没有体」翻译成
/// [`TrRpcBody::has_body`] 的 `false`。
struct ReqBodyView<'a, TyReq> {
    req_: &'a TyReq,
}

impl<TyReq> TrRpcBody for ReqBodyView<'_, TyReq>
where
    TyReq: messaging::TrRpcRequest,
{
    #[inline]
    fn has_body(&self) -> bool {
        self.req_.has_body()
    }

    #[inline]
    fn try_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        self.req_.try_body_known_len()
    }

    #[inline]
    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError> {
        Ok(self.req_.try_write_body(sink)?.unwrap_or(0usize))
    }
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
