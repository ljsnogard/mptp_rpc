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
    copy_::{CopyError, copy_all_async_, copy_sized_async_},
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
) -> Result<(), ServeError>
where
    C: TrConfig,
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
    Result::Ok(())
}

/// 定长回复体的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseSized, pub, new(pub(crate)))]
async fn response_sized_async_<'f, C, R, K>(
    builder: ResponseBuilder<'f, C>,
    body: &'f mut R,
    size: usize,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
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
    Result::Ok(())
}

/// 分块回复体的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseChunked, pub, new(pub(crate)))]
async fn response_chunked_async_<'f, C, R, K>(
    builder: ResponseBuilder<'f, C>,
    body: &'f mut R,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
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
    Result::Ok(())
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
}
