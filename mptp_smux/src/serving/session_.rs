//! 服务端会话：一条入向子流上的「读请求 → 写回复 → 收尾」。
//!
//! [`ServingSession`] 与客户端 `MptpSession` 共享 [`SessionCore`](crate::session_core_::SessionCore)：
//! 两个半边、请求前缀、体边界与体进度只有一份实现；方向性的方法分别放在两个类型上。
//! 一条连接上可以并行持有多个 `ServingSession`，由调用者决定如何并发。
//!
//! # `session_`: the server-side session
//!
//! The server session shares the session core with the client session. Directional
//! methods live here: read the request body / suffix, build the response, then finish.
//! Many sessions can be held concurrently on one connection.

use alloc::string::{String, ToString};

use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    messaging::{BodySize, TrMsgPrefix},
    spec::{Status, StdHeaderKey, StdHeaderVal, access_method::AccessMethod},
};
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::{
    HeaderKey, HeaderVal, Headers, RequestPrefix, ResponsePrefix,
    chunked::ChunkedEncode, write_response_prefix_async,
};

use crate::{
    body_::{BodyPlan, BodyRead, BodyState, SuffixRead},
    copy_::{CopyError, copy_all_async_, copy_sized_async_, drain_to_eof_async_},
    error_::{ProtocolError, ServeError, StateError},
    session_core_::SessionCore,
    session_state_::ServingState,
    x_deps::{abs_buff, abs_cancel, gen_mcf2},
};
use super::config::{ChannelRx, ChannelTx, TrConfig};

/// 服务端请求体的读视图。
pub type RequestBodyRead<'f, C> = BodyRead<'f, ChannelRx<C>>;

/// 服务端请求后缀流的读视图。
pub type RequestSuffixRead<'f, C> = SuffixRead<'f, ChannelRx<C>>;

/// 一条入向子流上的服务端会话。
pub struct ServingSession<C>
where
    C: TrConfig,
{
    /// 共享核心：两个半边、请求前缀、体边界与体进度。
    core_: SessionCore<ChannelTx<C>, ChannelRx<C>>,

    /// 本次请求的方法。
    method_: AccessMethod,

    /// 本次请求的位置。
    location_: String,

    /// 请求头表的可变副本；handler 的修改不写回前缀。
    headers_: Headers,

    /// 已经写出的回复前缀。
    resp_prefix_: Option<ResponsePrefix>,

    /// 服务端状态机。
    state_: ServingState,

    /// 是否已经写过回复前缀。
    responded_: bool,
}

impl<C> ServingSession<C>
where
    C: TrConfig,
{
    /// 由「已解码的请求前缀 + 两个半边」构造。
    pub(crate) fn new_(
        prefix: RequestPrefix,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> Result<Self, ServeError> {
        let plan = BodyPlan::from_info_(prefix.body_info()).map_err(ServeError::Protocol)?;
        let method_ = prefix.method();
        let location_ = String::from(prefix.location());
        let headers_ = prefix.headers().cloned().unwrap_or_default();
        Result::Ok(ServingSession {
            core_: SessionCore::new_(prefix, plan, tx, rx),
            method_,
            location_,
            headers_,
            resp_prefix_: Option::None,
            state_: ServingState::ReqPrefixReady,
            responded_: false,
        })
    }

    /// 本次请求的方法。
    pub fn method(&self) -> AccessMethod {
        self.method_
    }

    /// 本次请求的位置。
    pub fn location(&self) -> &str {
        &self.location_
    }

    /// 请求头表的可变副本。
    pub fn headers(&self) -> &Headers {
        &self.headers_
    }

    /// 可变请求头表。
    pub fn headers_mut(&mut self) -> &mut Headers {
        &mut self.headers_
    }

    /// 已经写出的回复前缀。
    pub fn sent_response_prefix(&self) -> Option<&ResponsePrefix> {
        self.resp_prefix_.as_ref()
    }

    /// 当前状态。
    pub fn state(&self) -> ServingState {
        self.state_
    }

    /// 按请求前缀声明的体边界取读视图。
    pub fn req_body_async<'f>(&'f mut self) -> RequestBodyAsync<'f, 'f, C>
    where
        C: 'f,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        RequestBodyAsync::new(self)
    }

    /// 丢弃未读完的请求体，然后把接收半边当裸后缀流交出去。
    pub fn req_suffix_async<'f>(&'f mut self) -> RequestSuffixAsync<'f, 'f, C>
    where
        C: 'f,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        RequestSuffixAsync::new(self)
    }

    /// 构造回复 helper；终结时才写前缀。
    pub fn response(&mut self, status: Status) -> ResponseBuilder<'_, C> {
        self.response_prefix(ResponsePrefix::new(status))
    }

    /// 用给定的回复前缀构造 helper。
    pub fn response_prefix(&mut self, prefix: ResponsePrefix) -> ResponseBuilder<'_, C> {
        ResponseBuilder {
            session_: self,
            prefix_: prefix,
        }
    }

    /// 收尾：丢弃剩余请求体 / 后缀、半关闭写端、读尽对端。
    ///
    /// 返回丢弃的请求字节数。若两个半边已经被 `take_parts` 取走，返回 `Ok(0)`。
    pub fn finish_async(&mut self) -> FinishSessionAsync<'_, '_, C> {
        FinishSessionAsync::new(self)
    }

    /// 拿走两个半边与头表，自己驱动；之后框架不再兜底收尾。
    pub fn take_parts(&mut self) -> Option<(ChannelTx<C>, ChannelRx<C>, Headers, String)> {
        let (tx, rx) = self.core_.take_channel_()?;
        let headers = core::mem::take(&mut self.headers_);
        let location = core::mem::take(&mut self.location_);
        Option::Some((tx, rx, headers, location))
    }

    /// 两个半边是否还在。
    pub(crate) fn has_channel_(&self) -> bool {
        self.core_.has_tx_() && self.core_.has_rx_()
    }

    /// 是否已经写过回复前缀。
    pub(crate) fn responded_(&self) -> bool {
        self.responded_
    }

}

/// 把回复体 / 后缀的搬运错误映射成服务端错误。
fn resp_copy_error_(err: CopyError) -> ServeError {
    match err {
        CopyError::Cancelled => ServeError::Cancelled,
        CopyError::Truncated { declared, read } => {
            ServeError::Protocol(ProtocolError::ResponseBodyTruncated { declared, read })
        }
        CopyError::Read { detail } => {
            ServeError::Protocol(ProtocolError::ResponseBodyRead { detail })
        }
        CopyError::Write { detail } => {
            ServeError::Protocol(ProtocolError::ResponseBodyWrite { detail })
        }
        CopyError::EmptySegment => ServeError::Protocol(ProtocolError::ResponseBodyRead {
            detail: String::from("源或目标让出的段是空的，无法推进"),
        }),
    }
}

/// 请求体读视图的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RequestBody, pub, new(pub(crate)))]
async fn serving_req_body_async_<'f, C, K>(
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<RequestBodyRead<'f, C>, ServeError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let _ = cancel;
    if !matches!(
        session.state_,
        ServingState::ReqPrefixReady | ServingState::ReadingReqBody
    ) {
        return Result::Err(ServeError::State(StateError::Serving {
            actual: session.state_,
        }));
    }
    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;
    session.state_ = ServingState::ReadingReqBody;
    let (rx, body_state) = session.core_.rx_and_body_mut_()?;
    Result::Ok(BodyRead::from_plan_(plan, rx, body_state))
}

/// 请求后缀流的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RequestSuffix, pub, new(pub(crate)))]
async fn serving_req_suffix_async_<'f, C, K>(
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<RequestSuffixRead<'f, C>, ServeError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    if !matches!(
        session.state_,
        ServingState::ReqPrefixReady | ServingState::ReadingReqBody
    ) {
        return Result::Err(ServeError::State(StateError::Serving {
            actual: session.state_,
        }));
    }
    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;

    {
        let (rx, body_state) = session.core_.rx_and_body_mut_()?;
        crate::session_core_::drain_body_async_(plan, body_state, rx, cancel.child_token())
            .await
            .map_err(ServeError::Protocol)?;
    }
    session.core_.set_body_state_(BodyState::Fresh);
    session.state_ = ServingState::ReadingReqSuffix;

    let (rx, _) = session.core_.rx_and_body_mut_()?;
    Result::Ok(SuffixRead::new_(rx))
}

/// 收尾的 step 函数。
#[gen_may_cancel_future(FinishSession, pub, new(pub(crate)))]
async fn finish_session_async_<'f, C, K>(
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<usize, ServeError>
where
    C: TrConfig + 'f,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    if session.state_ == ServingState::Terminated {
        return Result::Ok(0usize);
    }
    if !session.has_channel_() {
        session.state_ = ServingState::Terminated;
        return Result::Ok(0usize);
    }

    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;

    let mut dropped = 0usize;
    {
        let (rx, body_state) = session.core_.rx_and_body_mut_()?;
        dropped += crate::session_core_::drain_body_async_(
            plan,
            body_state,
            rx,
            cancel.child_token(),
        )
        .await
        .map_err(ServeError::Protocol)?;
    }

    // 先半关闭写端，再读尽对端；顺序不能反。
    let _ = session.core_.take_tx_();
    if let Result::Ok(rx) = session.core_.rx_mut_() {
        dropped += crate::copy_::drain_to_eof_async_(rx, cancel.child_token()).await;
    }
    let _ = session.core_.take_rx_();
    session.state_ = ServingState::Terminated;
    Result::Ok(dropped)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 回复 helper
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回复 helper：状态码 + 头 + 无体 / 定长体 / 分块体 / 后缀流。
pub struct ResponseBuilder<'a, C>
where
    C: TrConfig,
{
    /// 所属会话。
    session_: &'a mut ServingSession<C>,

    /// 待写出的回复前缀。
    prefix_: ResponsePrefix,
}

impl<'a, C> ResponseBuilder<'a, C>
where
    C: TrConfig,
{
    /// 追加 / 覆盖一个回复头。
    pub fn header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal) -> &mut Self {
        self.prefix_.headers_mut().set(key, val);
        self
    }

    /// 无回复体。
    pub fn no_body_async(self) -> ResponseNoBodyAsync<'a, 'a, C>
    where
        C: 'a,
    {
        ResponseNoBodyAsync::new(self)
    }

    /// 定长回复体：自动写 `Body_Size`。
    pub fn sized_async<R>(self, body: &'a mut R, size: usize) -> ResponseSizedAsync<'a, 'a, C, R>
    where
        C: 'a,
        R: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseSizedAsync::new(self, body, size)
    }

    /// 分块回复体：自动写 `Body_Transfer: Chunked`。
    pub fn chunked_async<R>(self, body: &'a mut R) -> ResponseChunkedAsync<'a, 'a, C, R>
    where
        C: 'a,
        R: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseChunkedAsync::new(self, body)
    }

    /// 回复前缀之后的裸后缀流。
    pub fn suffix_async<R>(self, suffix: &'a mut R) -> ResponseSuffixAsync<'a, 'a, C, R>
    where
        C: 'a,
        R: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseSuffixAsync::new(self, suffix)
    }

    /// 写回复前缀（无回复体），返回**可无限追加 chunk** 的后缀流句柄。
    ///
    /// 与 [`ResponseBuilder::suffix_async`] 的区别只在源：这里没有「源」，调用者拿到
    /// [`SuffixStream`] 之后用 [`SuffixStream::write_chunk_async`] 一次一次地追加字节。
    /// 写端在句柄存活期间**不会**被半关闭，因此同一个流可以持续数小时。
    ///
    /// 返回的 future 可经 [`TrMayCancel::may_cancel_with`] 施加取消令牌；`await` 成功后
    /// 得到 [`SuffixStream`]。
    ///
    /// # Errors
    ///
    /// - `Head` / `Drop`：这两个方法按协议没有回复体，也没有后缀流，返回
    ///   [`ProtocolError::ResponseBodyOnHeadOrDrop`]；
    /// - 前缀已声明了体：返回 [`ProtocolError::BodyDeclaredButNotWritten`]；
    /// - 写回复前缀失败：返回 [`ServeError::Protocol`]。
    pub fn suffix_stream_async(self) -> ResponseSuffixStreamAsync<'a, 'a, C>
    where
        C: 'a,
    {
        ResponseSuffixStreamAsync::new(self)
    }
}

/// 已经写完回复体的会话句柄；可以在它之后追加回复后缀流。
///
/// [`ResponseBuilder::no_body_async`] / [`ResponseBuilder::sized_async`] /
/// [`ResponseBuilder::chunked_async`] 返回本类型，因此 `Pull` / `Call` 可以先写回复体
/// 再写回复后缀流。
pub struct ResponseBodyWriter<'a, C>
where
    C: TrConfig,
{
    /// 所属会话。
    session_: &'a mut ServingSession<C>,
}

impl<'a, C> ResponseBodyWriter<'a, C>
where
    C: TrConfig,
{
    /// 在已经写出的回复体之后追加裸后缀流。
    ///
    /// `Head` / `Drop` 会被拒绝：这两个方法按协议没有后缀流。
    pub fn suffix_async<R>(self, suffix: &'a mut R) -> ResponseWriterSuffixAsync<'a, 'a, C, R>
    where
        C: 'a,
        R: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseWriterSuffixAsync::new(self, suffix)
    }
}

/// [`ResponseBodyWriter::suffix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseWriterSuffix, pub, new(pub(crate)))]
async fn response_writer_suffix_async_<'f, C, R, K>(
    writer: ResponseBodyWriter<'f, C>,
    suffix: &'f mut R,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'f,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let session_ = writer.session_;
    ensure_body_allowed_(session_.method_)?;
    {
        let tx = session_.core_.tx_mut_()?;
        copy_all_async_(suffix, tx, cancel.child_token())
            .await
            .map_err(resp_copy_error_)?;
    }
    Result::Ok(())
}

/// `Head` / `Drop` 的回复不允许写体；其余方法允许。
fn ensure_body_allowed_(method: AccessMethod) -> Result<(), ProtocolError> {
    if matches!(method, AccessMethod::Head | AccessMethod::Drop) {
        return Result::Err(ProtocolError::ResponseBodyOnHeadOrDrop { method });
    }
    Result::Ok(())
}

/// `no_body` 要求前缀本身没有声明任何体。
fn ensure_no_body_(prefix: &ResponsePrefix) -> Result<(), ProtocolError> {
    if !matches!(prefix.body_info(), BodySize::NilBody) {
        return Result::Err(ProtocolError::BodyDeclaredButNotWritten);
    }
    Result::Ok(())
}

/// 写回复前缀，并标记为已回复。
async fn write_response_prefix_inner_<C, K>(
    session: &mut ServingSession<C>,
    prefix: ResponsePrefix,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
    K: TrCancellationToken,
{
    {
        let tx = session.core_.tx_mut_()?;
        write_response_prefix_async(&prefix, tx)
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ServeError::Protocol(ProtocolError::ResponsePrefixEncode(err)))?;
    }
    session.resp_prefix_ = Option::Some(prefix);
    session.responded_ = true;
    session.state_ = ServingState::Responded;
    Result::Ok(())
}

/// 无回复体的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseNoBody, pub, new(pub(crate)))]
async fn response_no_body_async_<'f, C, K>(
    builder: ResponseBuilder<'f, C>,
    cancel: K,
) -> Result<ResponseBodyWriter<'f, C>, ServeError>
where
    C: TrConfig + 'f,
    K: TrCancellationToken,
{
    let ResponseBuilder {
        session_,
        prefix_,
    } = builder;

    // `no_body` 对 `Head` / `Drop` 也是合法的：这两个方法本来就不写回复体。
    // 只有前缀已经声明了体时才拒绝。
    ensure_no_body_(&prefix_)?;

    write_response_prefix_inner_(session_, prefix_, cancel).await?;
    Result::Ok(ResponseBodyWriter { session_ })
}

/// 定长回复体的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseSized, pub, new(pub(crate)))]
async fn response_sized_async_<'f, C, R, K>(
    builder: ResponseBuilder<'f, C>,
    body: &'f mut R,
    size: usize,
    cancel: K,
) -> Result<ResponseBodyWriter<'f, C>, ServeError>
where
    C: TrConfig + 'f,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let ResponseBuilder {
        session_,
        mut prefix_,
    } = builder;

    ensure_body_allowed_(session_.method_)?;

    prefix_
        .headers_mut()
        .set_text(StdHeaderKey::Body_Size, size.to_string());
    prefix_.headers_mut().remove(StdHeaderKey::Body_Transfer);

    write_response_prefix_inner_(session_, prefix_, cancel.child_token()).await?;

    {
        let tx = session_.core_.tx_mut_()?;
        copy_sized_async_(body, tx, size, cancel.child_token())
            .await
            .map_err(resp_copy_error_)?;
    }
    Result::Ok(ResponseBodyWriter { session_ })
}

/// 分块回复体的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseChunked, pub, new(pub(crate)))]
async fn response_chunked_async_<'f, C, R, K>(
    builder: ResponseBuilder<'f, C>,
    body: &'f mut R,
    cancel: K,
) -> Result<ResponseBodyWriter<'f, C>, ServeError>
where
    C: TrConfig + 'f,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let ResponseBuilder {
        session_,
        mut prefix_,
    } = builder;

    ensure_body_allowed_(session_.method_)?;

    prefix_.headers_mut().remove(StdHeaderKey::Body_Size);
    prefix_.headers_mut().set_num(
        StdHeaderKey::Body_Transfer,
        StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );

    write_response_prefix_inner_(session_, prefix_, cancel.child_token()).await?;

    {
        let tx = session_.core_.tx_mut_()?;
        let mut encode = ChunkedEncode::new(body, tx);
        encode
            .write_all_async()
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| {
                ServeError::Protocol(ProtocolError::ResponseBodyWrite {
                    detail: err.to_string(),
                })
            })?;
    }
    Result::Ok(ResponseBodyWriter { session_ })
}

/// 回复后缀流的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseSuffix, pub, new(pub(crate)))]
async fn response_suffix_async_<'f, C, R, K>(
    builder: ResponseBuilder<'f, C>,
    suffix: &'f mut R,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let ResponseBuilder {
        session_,
        prefix_,
    } = builder;

    ensure_body_allowed_(session_.method_)?;
    ensure_no_body_(&prefix_)?;

    write_response_prefix_inner_(session_, prefix_, cancel.child_token()).await?;

    {
        let tx = session_.core_.tx_mut_()?;
        copy_all_async_(suffix, tx, cancel.child_token())
            .await
            .map_err(resp_copy_error_)?;
    }
    Result::Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 无限后缀流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// **可无限追加 chunk** 的回复后缀流句柄。
///
/// 它由 [`ResponseBuilder::suffix_stream_async`] 打开：打开时写出「没有回复体」的回复
/// 前缀，之后写端一直保持打开，直到调用者显式 [`finish_async`](Self::finish_async)
/// 或会话被丢弃。因此同一个句柄既可以用三五条事件，也可以持续数小时。
///
/// # 这里没有帧格式
///
/// chunk 只是 `mptp_smux` 的**写入单位**：本类型不定义对象边界、不写长度前缀、不做
/// 序列化，也不改 `mptp_wire` 的线上格式。后缀流在线上仍然是裸字节，接收端要按自己
/// 的应用协议（例如 denco 的自描述值序列）逐值解码。一个对象可以拆成任意多个 chunk
/// 写入，接收端看到的字节序列与一次写完完全相同。
///
/// # 取消与收尾
///
/// [`write_chunk_async`](Self::write_chunk_async) /
/// [`write_chunks_async`](Self::write_chunks_async) /
/// [`finish_async`](Self::finish_async) 返回的 future 都实现
/// [`TrMayCancel`]，调用方用 `.may_cancel_with(token)` 施加取消令牌。`finish_async`
/// 复用标准收尾顺序：先半关闭写端，再读尽对端，最后把会话置为终止。
pub struct SuffixStream<'a, C>
where
    C: TrConfig,
{
    /// 所属会话；句柄存活期间独占它。
    session_: &'a mut ServingSession<C>,
}

impl<'a, C> SuffixStream<'a, C>
where
    C: TrConfig,
{
    /// 追加一个 chunk；**不**关闭写端。
    ///
    /// `chunk` 的字节被原样写进子流，既不封帧也不加长度。空 chunk 是合法的空操作。
    ///
    /// # Errors
    ///
    /// - `Head` / `Drop`：按协议没有后缀流，返回
    ///   [`ProtocolError::ResponseBodyOnHeadOrDrop`]；
    /// - 写端已被取走（已经 `finish`）：返回 [`ServeError::State`]；
    /// - 写失败或等待期间被取消：返回 [`ServeError::Protocol`] / [`ServeError::Cancelled`]。
    pub fn write_chunk_async<'f>(
        &'f mut self,
        chunk: &'f [u8],
    ) -> SuffixWriteChunkAsync<'f, 'a, 'f, C>
    where
        C: 'a,
        'a: 'f,
    {
        SuffixWriteChunkAsync::new(self, chunk)
    }

    /// 把一个有限读源的内容连续写成 chunks，直到源结束；**不**关闭写端。
    ///
    /// 这是 [`Self::write_chunk_async`] 的便捷包装：源里有多少字节就追加多少字节，
    /// 不改变「后缀流是裸字节」的语义。调用方仍然可以在它返回后继续
    /// [`write_chunk_async`](Self::write_chunk_async)。
    ///
    /// # Errors
    ///
    /// 与 [`Self::write_chunk_async`] 相同；源读取失败映射成
    /// [`ProtocolError::SuffixRead`]。
    pub fn write_chunks_async<'f, R>(
        &'f mut self,
        src: &'f mut R,
    ) -> SuffixWriteChunksAsync<'f, 'a, 'f, C, R>
    where
        C: 'a,
        'a: 'f,
        R: TrBuffRead<u8> + TrConsumerState,
    {
        SuffixWriteChunksAsync::new(self, src)
    }

    /// 半关闭写端、读尽对端并结束会话。
    ///
    /// 返回后这个句柄不能再写；框架也不会再替它兜底回复或收尾。
    ///
    /// # Errors
    ///
    /// 收尾阶段的读错误**不**上报（它已经不影响写出的响应）；本方法目前只在底层
    /// 状态不成立（两个半边都已被取走）时返回 [`ServeError::State`]。
    pub fn finish_async(self) -> SuffixFinishAsync<'a, 'a, C>
    where
        C: 'a,
    {
        SuffixFinishAsync::new(self)
    }
}

/// 把后缀写失败映射成服务端错误。
///
/// 与 [`resp_copy_error_`] 分开：后缀流没有「声明长度」，因此截断不会被报告成
/// `ResponseBodyTruncated`，而是回落到 `SuffixWrite`。
fn suffix_copy_error_(err: CopyError) -> ServeError {
    match err {
        CopyError::Cancelled => ServeError::Cancelled,
        CopyError::Truncated { declared, read } => {
            ServeError::Protocol(ProtocolError::SuffixTruncated { declared, read })
        }
        CopyError::Read { detail } => ServeError::Protocol(ProtocolError::SuffixRead { detail }),
        CopyError::Write { detail } => ServeError::Protocol(ProtocolError::SuffixWrite { detail }),
        CopyError::EmptySegment => ServeError::Protocol(ProtocolError::SuffixWrite {
            detail: String::from("源或目标让出的段是空的，无法推进"),
        }),
    }
}

/// [`ResponseBuilder::suffix_stream_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseSuffixStream, pub, new(pub(crate)))]
async fn response_suffix_stream_async_<'f, C, K>(
    builder: ResponseBuilder<'f, C>,
    cancel: K,
) -> Result<SuffixStream<'f, C>, ServeError>
where
    C: TrConfig + 'f,
    K: TrCancellationToken,
{
    let ResponseBuilder {
        session_,
        prefix_,
    } = builder;

    ensure_body_allowed_(session_.method_)?;
    ensure_no_body_(&prefix_)?;

    write_response_prefix_inner_(session_, prefix_, cancel).await?;
    Result::Ok(SuffixStream { session_ })
}

/// [`SuffixStream::write_chunk_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SuffixWriteChunk, pub, new(pub(crate)))]
async fn suffix_write_chunk_async_<'f, 'a, C, K>(
    stream: &'f mut SuffixStream<'a, C>,
    chunk: &'f [u8],
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'a,
    'a: 'f,
    K: TrCancellationToken,
{
    let session_ = &mut *stream.session_;
    ensure_body_allowed_(session_.method_)?;
    let size = chunk.len();
    if size == 0usize {
        return Result::Ok(());
    }
    let mut src = chunk;
    let tx = session_.core_.tx_mut_()?;
    copy_sized_async_(&mut src, tx, size, cancel)
        .await
        .map_err(suffix_copy_error_)?;
    Result::Ok(())
}

/// [`SuffixStream::write_chunks_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SuffixWriteChunks, pub, new(pub(crate)))]
async fn suffix_write_chunks_async_<'f, 'a, C, R, K>(
    stream: &'f mut SuffixStream<'a, C>,
    src: &'f mut R,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'a,
    'a: 'f,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let session_ = &mut *stream.session_;
    ensure_body_allowed_(session_.method_)?;
    let tx = session_.core_.tx_mut_()?;
    copy_all_async_(src, tx, cancel)
        .await
        .map_err(suffix_copy_error_)?;
    Result::Ok(())
}

/// [`SuffixStream::finish_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SuffixFinish, pub, new(pub(crate)))]
async fn suffix_finish_async_<'a, C, K>(
    stream: SuffixStream<'a, C>,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'a,
    K: TrCancellationToken,
{
    let session_ = stream.session_;
    // 先半关闭写端（丢弃 tx），再读尽对端；顺序不能反。
    let _ = session_.core_.take_tx_();
    if let Result::Ok(rx) = session_.core_.rx_mut_() {
        let _ = drain_to_eof_async_(rx, cancel).await;
    }
    let _ = session_.core_.take_rx_();
    session_.state_ = ServingState::Terminated;
    Result::Ok(())
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 `Head` 与 `Drop` 的回复体在 helper 层被拒绝。
    /// Test that `Head` and `Drop` response bodies are rejected at the helper layer.
    ///
    /// - 手段：分别对 `Head`、`Drop` 调用 `ensure_body_allowed_`。
    /// - 判断：都返回 `ResponseBodyOnHeadOrDrop`。
    ///
    /// - Means: call `ensure_body_allowed_` with `Head` and `Drop`.
    /// - Verdict: both return `ResponseBodyOnHeadOrDrop`.
    #[test]
    fn head_and_drop_reject_response_body_() {
        for method in [AccessMethod::Head, AccessMethod::Drop] {
            let res = ensure_body_allowed_(method);
            assert!(
                matches!(
                    res,
                    Result::Err(ProtocolError::ResponseBodyOnHeadOrDrop { .. })
                ),
                "{method:?} 应当拒绝回复体，实际是 {res:?}"
            );
        }
    }

    /// 测试其余方法允许写回复体。
    /// Test that the other methods allow a response body.
    ///
    /// - 手段：对 `View`、`Post`、`Push`、`Pull`、`Call` 调用 `ensure_body_allowed_`。
    /// - 判断：全部返回 `Ok`。
    ///
    /// - Means: call `ensure_body_allowed_` for `View`, `Post`, `Push`, `Pull` and `Call`.
    /// - Verdict: all return `Ok`.
    #[test]
    fn other_methods_allow_response_body_() {
        for method in [
            AccessMethod::View,
            AccessMethod::Post,
            AccessMethod::Push,
            AccessMethod::Pull,
            AccessMethod::Call,
        ] {
            assert!(ensure_body_allowed_(method).is_ok(), "{method:?}");
        }
    }

    /// 测试 `no_body` 收到声明了体的前缀时拒绝。
    /// Test that `no_body` rejects a prefix that already declares a body.
    ///
    /// - 手段：构造带 `Body_Size: 8` 的回复前缀，调用 `ensure_no_body_`。
    /// - 判断：返回 `BodyDeclaredButNotWritten`。
    ///
    /// - Means: build a response prefix with `Body_Size: 8` and call `ensure_no_body_`.
    /// - Verdict: it returns `BodyDeclaredButNotWritten`.
    #[test]
    fn no_body_rejects_declared_body_() {
        let mut prefix = ResponsePrefix::new(Status::Ok);
        prefix
            .headers_mut()
            .set_text(StdHeaderKey::Body_Size, 8usize.to_string());
        let res = ensure_no_body_(&prefix);
        assert!(matches!(
            res,
            Result::Err(ProtocolError::BodyDeclaredButNotWritten)
        ));
    }

    /// 测试没有体声明的前缀可以通过 `no_body` 校验。
    /// Test that a prefix without a body declaration passes the `no_body` check.
    ///
    /// - 手段：构造没有头的回复前缀。
    /// - 判断：`ensure_no_body_` 返回 `Ok`。
    ///
    /// - Means: build a header-less response prefix.
    /// - Verdict: `ensure_no_body_` returns `Ok`.
    #[test]
    fn no_body_accepts_plain_prefix_() {
        let prefix = ResponsePrefix::new(Status::Ok);
        assert!(ensure_no_body_(&prefix).is_ok());
    }

    /// 测试无限后缀流与有限后缀流共用同一道方法校验：`Head` / `Drop` 一律拒绝。
    /// Test that the infinite suffix stream shares the method gate: `Head` / `Drop` are
    /// rejected.
    ///
    /// - 手段：对七种方法逐个调用 `ensure_body_allowed_`，模拟打开后缀流前的校验。
    /// - 判断：`Head` / `Drop` 返回 `ResponseBodyOnHeadOrDrop`，其余五种返回 `Ok`。
    ///
    /// - Means: call `ensure_body_allowed_` for all seven methods, mirroring the gate that
    ///   runs before a suffix stream is opened.
    /// - Verdict: `Head` / `Drop` return `ResponseBodyOnHeadOrDrop`; the other five return
    ///   `Ok`.
    #[test]
    fn suffix_stream_rejects_head_and_drop_() {
        for method in [AccessMethod::Head, AccessMethod::Drop] {
            assert!(matches!(
                ensure_body_allowed_(method),
                Result::Err(ProtocolError::ResponseBodyOnHeadOrDrop { .. })
            ));
        }
        for method in [
            AccessMethod::View,
            AccessMethod::Post,
            AccessMethod::Push,
            AccessMethod::Pull,
            AccessMethod::Call,
        ] {
            assert!(ensure_body_allowed_(method).is_ok(), "{method:?}");
        }
    }

    /// 测试后缀写错误被映射成后缀专用的协议错误，而不是回复体错误。
    /// Test that suffix write failures map to suffix-specific protocol errors.
    ///
    /// - 手段：构造 `CopyError::Write` 与 `CopyError::EmptySegment` 两种失败，调用
    ///   `suffix_copy_error_`。
    /// - 判断：两者都落到 `ProtocolError::SuffixWrite`，不会冒充 `ResponseBodyWrite`。
    ///
    /// - Means: feed `CopyError::Write` and `CopyError::EmptySegment` to
    ///   `suffix_copy_error_`.
    /// - Verdict: both become `ProtocolError::SuffixWrite`, never `ResponseBodyWrite`.
    #[test]
    fn suffix_errors_are_classified_as_suffix_() {
        for err in [
            CopyError::Write {
                detail: String::from("x"),
            },
            CopyError::EmptySegment,
        ] {
            assert!(matches!(
                suffix_copy_error_(err),
                ServeError::Protocol(ProtocolError::SuffixWrite { .. })
            ));
        }
    }
}
