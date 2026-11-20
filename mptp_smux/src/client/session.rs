//! 客户端的会话对象：一条已建成子流上的「发请求 → 收回复」全过程。
//!
//! 一个 [`MptpSession`] 围绕 `abs_smux` 概念里的一条 channel 进行：它持有该子流的收发
//! 半边（`accept_async` 的产物）与本次请求的前缀，因此调用者可以在同一条子流上把请求体
//! 发完、把回复读完。
//!
//! # 请求方向的三步与它们的状态
//!
//! | 入口 | 写什么 | 状态迁移 |
//! | --- | --- | --- |
//! | [`TrMptpClient::request_async`]（客户端实现） | 请求前缀 | → `SendingBody`（有体）/ `WaitingResp` |
//! | [`TrMptpSession::send_req_body_async`] | 请求体（按前缀的体声明搬运） | `SendingBody` → `WaitingResp` |
//! | [`TrMptpSession::send_req_suffix_async`] | 请求后缀流（裸字节） | `WaitingResp` → `SendingSuffix` → `WaitingResp` |
//! | [`MptpSession::close_req_async`] | 半关闭写端 | `WaitingResp`（保持） |
//!
//! 「要不要发体」与「体按定长还是分块发」全由**已写出去的前缀**决定
//! （[`TrMsgPrefix::body_info`]），会话自己不猜、调用者也不必再声明一次。请求体与后缀流
//! 收的都是**已经序列化好的字节流**（[`TrBuffRead`]）：业务值怎么变成字节是上层的事，
//! 本层只按协议搬运。
//!
//! # 响应方向的三个读入口
//!
//! | 入口 | 读什么 | 读完之后 |
//! | --- | --- | --- |
//! | [`TrMptpSession::recv_resp_prefix_async`] | 回复前缀（状态码 + 头） | 体边界这才确定 |
//! | [`TrMptpSession::recv_resp_body_async`] | 回复体的字节视图 | 体读视图归还后才知道读了多少 |
//! | [`TrMptpSession::recv_resp_suffix_async`] | 报文之后的裸字节续流 | 会话可以持续到对端半关闭 |
//!
//! # `session`: one channel's request and response
//!
//! The request is sent in protocol order: prefix, body, optional raw suffix, then the
//! write half is closed. The response body view is a single type over the two upstream
//! readers; `TrMptpBuffRead` extracts the one the prefix already decided.

use core::alloc::Allocator;

use alloc::string::ToString;

use abs_buff::{
    Demand, TrBuffFlush, TrBuffRead, TrBuffTryFlush, TrBuffTryWrite, TrBuffWrite,
    buffer::TrConsumerState, x_deps::anylr::SomeOf,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    client::{SessionState, TrMptpSession},
    messaging::TrMsgPrefix,
};
use bumplist_alloc::BumpListAlloc;
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::{RequestPrefix, chunked::ChunkedEncode, read_response_prefix_with_async};

use super::{
    config::{ChannelRx, ChannelTx, RespPrefix, TrConfig},
    method_::Response,
};
use crate::{
    RequestAlloc,
    body_::{BodyError, BodyPlan, BodyRead, BodyState, SuffixRead},
    copy_::{CopyError, copy_all_async_, copy_sized_async_},
    error_::{ClientError, ProtocolError, StateError},
    session_core_::SessionCore,
    x_deps::{abs_buff, abs_cancel, gen_mcf2},
};

/// 回复体的读视图。
pub type RespBodyRead<'f, C> = BodyRead<'f, ChannelRx<C>>;

/// 响应续流的读视图。
pub type RespSuffixRead<'f, C> = SuffixRead<'f, ChannelRx<C>>;

/// 报文体读视图的错误；保留旧名字供下游使用。
pub type RespBodyError<TyErr> = BodyError<TyErr>;

/// 一次请求 / 回复过程的会话。
///
/// 字段不对外公开：调用者只能通过 [`TrMptpSession`] 的契约驱动它。
pub struct MptpSession<'a, C, A>
where
    C: TrConfig,
    A: Allocator + Send,
{
    /// 共享的会话核心：两个半边、请求前缀、体边界与体进度。
    core_: SessionCore<ChannelTx<C>, ChannelRx<C>, BumpListAlloc<A>>,

    /// 客户端侧的状态机。
    state_: SessionState,
}

impl<'a, C, A> MptpSession<'a, C, A>
where
    C: TrConfig,
    A: Allocator + Send,
{
    /// 由「请求前缀 + 该子流的收发半边」构造。
    ///
    /// 只允许客户端实现（[`super::client_`]）在最终裁决成功、且请求前缀已经写出之后调用。
    ///
    /// 初始状态由**前缀里的体声明**决定：带体请求处于 `SendingBody`（体还没发，不能直接读
    /// 响应前缀），没有体的请求处于 `WaitingResp`（请求已经整体发出）。
    pub(crate) fn new_(
        request: RequestPrefix<RequestAlloc<'a>>,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
        alloc: RequestAlloc<'a>,
    ) -> Result<Self, ClientError> {
        let plan = BodyPlan::from_info_(request.body_info())?;
        let state_ = if matches!(plan, BodyPlan::Absent) {
            SessionState::WaitingResp
        } else {
            SessionState::SendingBody
        };
        Result::Ok(MptpSession {
            core_: SessionCore::new_(request, plan, tx, rx),
            state_,
            alloc_: alloc,
        })
    }

    /// 取回本会话对应的请求前缀。
    ///
    /// 它是调用者判断「下一步该发什么」的唯一依据：前缀里的 `body_info` 已经说明体是定长
    /// 还是分块、有没有体。本层不替调用者解释成业务语义。
    pub fn req_prefix(&self) -> Option<&RequestPrefix<RequestAlloc<'a>>> {
        self.core_.request_()
    }

    /// 半关闭写端：丢弃发送半边，保留接收半边继续读回复。
    ///
    /// 对应协议 §12.1 的「写完请求后关闭写方向」。在 `SendingBody` 状态下调用会失败——
    /// 体还没发完就关写端会把请求截断。
    pub fn close_req_async<'f>(&'f mut self) -> CloseReqAsync<'a, 'f, 'f, C>
    where
        C: 'f,
    {
        CloseReqAsync::new(self)
    }

    /// 把整个会话消费掉，排出未读完的回复体，然后交出**拥有接收半边**的后缀读口。
    ///
    /// 客户端 typed Pull 需要拥有这个读口（而不是借 `Response`），否则「flux 拥有
    /// reader」会变成自引用。返回的 [`ChannelRx`](super::config::ChannelRx) 已经实现
    /// `TrBuffRead<u8>` / `TrConsumerState`，可以直接交给 `rpc_core_v1` 的类型化 flux。
    ///
    /// # Errors
    ///
    /// 会话状态不是 `PrefixReady` / `BodyReady`（还没读回复前缀）时返回
    /// [`ClientError::State`]；排空回复体失败时返回协议 / 传输错误。
    pub fn into_resp_suffix_async<'f>(self) -> IntoRespSuffixAsync<'a, 'f, 'f, C>
    where
        C: 'f,
    {
        IntoRespSuffixAsync::new(self)
    }

    /// 为有限 Push 取走发送半边：此后由 [`RequestSuffixStream`] 逐 chunk 写请求后缀。
    ///
    /// 只允许在请求前缀已写出、且请求没有 body（处于 `WaitingResp`）时调用。
    pub(crate) fn take_tx_for_suffix_(mut self) -> Result<(Self, ChannelTx<C>), ClientError> {
        if self.state_ != SessionState::WaitingResp {
            let actual = self.state_;
            return Result::Err(ClientError::State(StateError::Client { actual }));
        }
        let tx = self.core_.take_tx_().ok_or(StateError::ChannelTaken)?;
        self.state_ = SessionState::SendingSuffix;
        Result::Ok((self, tx))
    }
}

/// 服务端 / 客户端都需要的「拥有写端」的请求后缀流。
///
/// 它由 [`RequestSuffixStream::new`] 从已发出请求前缀的 [`MptpSession`] 构造：拥有会话与发送半边，
/// 逐 chunk 写请求后缀；[`finish_async`](Self::finish_async) 丢弃发送半边（半关闭），
/// 再读回复前缀，交出 [`Response`]。
///
/// 本类型只做字节搬运，不依赖 denco / 类型头；typed Push 由 `rpc_core_v1` 在其上实现。
pub struct RequestSuffixStream<'a, C>
where
    C: TrConfig,
{
    /// 拥有接收半边与会话状态的会话；发送半边已在本类型里。
    session_: MptpSession<'a, C>,

    /// 请求后缀发送半边；`finish` / drop 时丢弃即半关闭。
    tx_: ChannelTx<C>,
}

impl<'a, C> RequestSuffixStream<'a, C>
where
    C: TrConfig,
{
    /// 由已发出请求前缀的会话构造。
    ///
    /// # Errors
    ///
    /// 会话不在 `WaitingResp`（例如请求还有 body 未发）时返回 [`ClientError::State`]。
    pub fn new(session: MptpSession<'a, C>) -> Result<Self, ClientError> {
        let (session_, tx_) = session.take_tx_for_suffix_()?;
        Result::Ok(RequestSuffixStream { session_, tx_ })
    }

    /// 借出底层发送半边，作为标准 [`TrBuffWrite`] / [`TrBuffFlush`] 使用。
    ///
    /// 调用者不需要自己准备缓冲：这里的 view 直接借用会话自带的 ring / buff。
    /// [`TrBuffFlush::close_async`] 只**半关闭请求后缀写侧**，不会读回复；
    /// 读回复仍由 [`finish_async`](Self::finish_async) 完成。
    /// 返回的 view 存活期间不能再调用 `finish_async`；先 drop view。
    pub fn writer(&mut self) -> RequestSuffixWriter<'_, C>
    where
        ChannelTx<C>: TrBuffWrite<u8>,
    {
        RequestSuffixWriter { tx_: &mut self.tx_ }
    }

    /// 追加一个请求后缀 chunk；**不**关闭写端。
    ///
    /// # Errors
    ///
    /// 写端已被 `finish` 取走时返回 [`ClientError::State`]；写失败或取消返回传输 /
    /// 协议错误。
    pub fn write_chunk_async<'f>(
        &'f mut self,
        chunk: &'f [u8],
    ) -> RequestSuffixWriteAsync<'a, 'f, 'f, C>
    where
        C: 'f,
    {
        RequestSuffixWriteAsync::new(self, chunk)
    }

    /// 半关闭请求后缀并读回复前缀，交出 [`Response`]。
    pub fn finish_async<'f>(self) -> RequestSuffixFinishAsync<'a, 'f, 'f, C>
    where
        C: 'f,
    {
        RequestSuffixFinishAsync::new(self)
    }
}

/// 借用 [`RequestSuffixStream`] 发送半边的标准 [`TrBuffWrite`] 视图。
///
/// 它不拥有会话，也不定义关闭语义：drop 视图后仍由
/// [`RequestSuffixStream::finish_async`] 半关闭并读回复。这样调用者可以直接把
/// `&mut writer` 交给 denco 编码，不必自备 `Vec` / 暂存区。
pub struct RequestSuffixWriter<'a, C>
where
    C: TrConfig,
{
    /// 会话自带的发送半边。
    tx_: &'a mut ChannelTx<C>,
}

impl<'a, C> TrBuffTryWrite<u8> for RequestSuffixWriter<'a, C>
where
    C: TrConfig,
    ChannelTx<C>: TrBuffWrite<u8>,
{
    type SegmMut<'f> = <ChannelTx<C> as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <ChannelTx<C> as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <ChannelTx<C> as TrBuffTryWrite<u8>>::try_write(self.tx_, demand)
    }
}

impl<C> TrBuffWrite<u8> for RequestSuffixWriter<'_, C>
where
    C: TrConfig,
    ChannelTx<C>: TrBuffWrite<u8>,
{
    type WriteAsync<'f> = <ChannelTx<C> as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        <ChannelTx<C> as TrBuffWrite<u8>>::write_async(self.tx_, demand)
    }
}

impl<C> TrBuffTryFlush<u8> for RequestSuffixWriter<'_, C>
where
    C: TrConfig,
    ChannelTx<C>: TrBuffFlush<u8>,
{
    fn try_flush(&mut self) -> Result<(), Self::Err> {
        <ChannelTx<C> as TrBuffTryFlush<u8>>::try_flush(self.tx_)
    }

    fn close(&mut self) -> Result<(), Self::Err> {
        <ChannelTx<C> as TrBuffTryFlush<u8>>::close(self.tx_)
    }
}

impl<C> TrBuffFlush<u8> for RequestSuffixWriter<'_, C>
where
    C: TrConfig,
    ChannelTx<C>: TrBuffFlush<u8>,
{
    type FlushAsync<'f> = <ChannelTx<C> as TrBuffFlush<u8>>::FlushAsync<'f>
    where
        Self: 'f;

    type CloseAsync<'f> = <ChannelTx<C> as TrBuffFlush<u8>>::CloseAsync<'f>
    where
        Self: 'f;

    fn flush_async(&mut self) -> Self::FlushAsync<'_> {
        <ChannelTx<C> as TrBuffFlush<u8>>::flush_async(self.tx_)
    }

    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        <ChannelTx<C> as TrBuffFlush<u8>>::close_async(self.tx_)
    }
}

/// [`RequestSuffixStream::write_chunk_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RequestSuffixWrite, pub, new(pub(crate)))]
async fn request_suffix_write_async_<'a, 'f, C, K>(
    stream: &'f mut RequestSuffixStream<'a, C>,
    chunk: &'f [u8],
    cancel: K,
) -> Result<(), ClientError>
where
    C: TrConfig + 'f,
    K: TrCancellationToken,
{
    let size = chunk.len();
    let mut src = chunk;
    let _ = copy_sized_async_(&mut src, &mut stream.tx_, size, cancel)
        .await
        .map_err(req_copy_error_)?;
    Result::Ok(())
}

/// [`RequestSuffixStream::finish_async`] 的 step 函数。
#[gen_may_cancel_future(RequestSuffixFinish, pub, new(pub(crate)))]
async fn request_suffix_finish_async_<'a, 'f, C, K>(
    stream: RequestSuffixStream<'a, C>,
    cancel: K,
) -> Result<Response<'a, C>, ClientError>
where
    C: TrConfig + 'f,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let RequestSuffixStream { mut session_, tx_ } = stream;
    // 丢弃发送半边即半关闭请求后缀；服务端据此结束请求并回复。
    drop(tx_);
    let prefix = session_
        .recv_resp_prefix_async()
        .may_cancel_with(cancel.child_token())
        .await?;
    Result::Ok(Response::from_parts_(session_, prefix))
}

/// [`MptpSession::into_resp_suffix_async`] 的 step 函数。
#[gen_may_cancel_future(IntoRespSuffix, pub, new(pub(crate)))]
async fn into_resp_suffix_async_<'a, 'f, C, K>(
    mut session: MptpSession<'a, C>,
    cancel: K,
) -> Result<ChannelRx<C>, ClientError>
where
    C: TrConfig + 'f,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    if !matches!(
        session.state_,
        SessionState::PrefixReady | SessionState::BodyReady
    ) {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }
    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;
    {
        let (rx, body_state) = session.core_.rx_and_body_mut_()?;
        crate::session_core_::drain_body_async_(plan, body_state, rx, cancel.child_token())
            .await
            .map_err(ClientError::Protocol)?;
    }
    session.core_.set_body_state_(BodyState::Fresh);
    session.state_ = SessionState::SuffixReady;
    let rx = session.core_.take_rx_().ok_or(StateError::ChannelTaken)?;
    Result::Ok(rx)
}

impl<'a, C> TrMptpSession<'a, C> for MptpSession<'a, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    /// 回复体的读视图：一个类型，两种档位（由前缀决定）。
    type RespBodyRead<'f>
        = RespBodyRead<'f, C>
    where
        Self: 'f;

    /// 响应续流的读视图：裸字节，两种 `try_into_*` 都交出同一个额度用不完的限长视图。
    type RespSuffixRead<'f>
        = RespSuffixRead<'f, C>
    where
        Self: 'f;

    type Err = ClientError;

    fn state(&self) -> SessionState {
        self.state_
    }

    type SendReqBodyAsync<'f, R>
        = SendRequestBodyAsync<'a, 'f, 'f, C, R>
    where
        R: 'f + TrBuffRead<u8> + TrConsumerState,
        Self: 'f;

    fn send_req_body_async<'f, R>(&'f mut self, body: &'f mut R) -> Self::SendReqBodyAsync<'f, R>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        SendRequestBodyAsync::new(self, body)
    }

    type SendReqSuffixAsync<'f, R>
        = SendRequestSuffixAsync<'a, 'f, 'f, C, R>
    where
        R: 'f + TrBuffRead<u8> + TrConsumerState,
        Self: 'f;

    fn send_req_suffix_async<'f, R>(
        &'f mut self,
        suffix: &'f mut R,
    ) -> Self::SendReqSuffixAsync<'f, R>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        SendRequestSuffixAsync::new(self, suffix)
    }

    type RecvRespPrefixAsync<'f>
        = SessionRecvRespPrefixAsync<'f, 'a, 'f, C>
    where
        Self: 'f,
        'a: 'f;

    fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f>
    where
        'a: 'f,
    {
        SessionRecvRespPrefixAsync::new(self)
    }

    type RecvRespBodyAsync<'f>
        = SessionRecvRespBodyAsync<'a, 'f, 'f, C>
    where
        Self: 'f;

    fn recv_resp_body_async<'f>(&'f mut self) -> Self::RecvRespBodyAsync<'f> {
        SessionRecvRespBodyAsync::new(self)
    }

    type RecvSuffixAsync<'f>
        = SessionRecvSuffixAsync<'a, 'f, 'f, C>
    where
        Self: 'f;

    fn recv_resp_suffix_async<'f>(&'f mut self) -> Self::RecvSuffixAsync<'f> {
        SessionRecvSuffixAsync::new(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 关闭请求写端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 半关闭请求写端的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(CloseReq, pub, new(pub(crate)))]
async fn session_close_req_async_<'a, 'f, C, K>(
    session: &'f mut MptpSession<'a, C>,
    cancel: K,
) -> Result<SessionState, ClientError>
where
    C: TrConfig,
    K: TrCancellationToken,
{
    let _ = cancel;
    if !matches!(
        session.state_,
        SessionState::WaitingResp | SessionState::SendingSuffix
    ) {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }
    // 发送半边被丢弃即半关闭；接收半边继续留着读回复。
    let _ = session.core_.take_tx_();
    session.state_ = SessionState::WaitingResp;
    Result::Ok(session.state_)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送请求体
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求体按**已写出去的前缀**里声明的传输方式搬进子流的发送半边。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendRequestBody, pub, new(pub(crate)))]
async fn session_send_req_body_async_<'a, 'f, C, R, TyTok>(
    session: &'f mut MptpSession<'a, C>,
    body: &'f mut R,
    cancel: TyTok,
) -> Result<SessionState, ClientError>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    if session.state_ != SessionState::SendingBody {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }

    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;

    match plan {
        BodyPlan::Absent => Result::Err(ClientError::State(StateError::BodyNotDeclared)),
        BodyPlan::Sized { size_ } => {
            let tx = session.core_.tx_mut_()?;
            copy_sized_async_(body, tx, size_, cancel.child_token())
                .await
                .map_err(req_copy_error_)?;
            session.state_ = SessionState::WaitingResp;
            Result::Ok(session.state_)
        }
        BodyPlan::Chunked => {
            let tx = session.core_.tx_mut_()?;
            let mut encode = ChunkedEncode::new(body, tx);
            encode
                .write_all_async()
                .may_cancel_with(cancel.child_token())
                .await
                .map_err(|err| {
                    ClientError::Protocol(ProtocolError::RequestBodyWrite {
                        detail: err.to_string(),
                    })
                })?;
            session.state_ = SessionState::WaitingResp;
            Result::Ok(session.state_)
        }
    }
}

/// 把请求体搬运错误映射成客户端错误。
fn req_copy_error_(err: CopyError) -> ClientError {
    match err {
        CopyError::Cancelled => ClientError::Cancelled,
        CopyError::Truncated { declared, read } => {
            ClientError::Protocol(ProtocolError::RequestBodyTruncated { declared, read })
        }
        CopyError::Read { detail } => {
            ClientError::Protocol(ProtocolError::RequestBodyRead { detail })
        }
        CopyError::Write { detail } => {
            ClientError::Protocol(ProtocolError::RequestBodyWrite { detail })
        }
        CopyError::EmptySegment => ClientError::Protocol(ProtocolError::RequestBodyRead {
            detail: alloc::string::String::from("源或目标让出的段是空的，无法推进"),
        }),
    }
}

/// 把请求后缀流的搬运错误映射成客户端错误。
fn suffix_copy_error_(err: CopyError) -> ClientError {
    match err {
        CopyError::Cancelled => ClientError::Cancelled,
        CopyError::Truncated { declared, read } => {
            ClientError::Protocol(ProtocolError::SuffixTruncated { declared, read })
        }
        CopyError::Read { detail } => ClientError::Protocol(ProtocolError::SuffixRead { detail }),
        CopyError::Write { detail } => ClientError::Protocol(ProtocolError::SuffixWrite { detail }),
        CopyError::EmptySegment => ClientError::Protocol(ProtocolError::SuffixRead {
            detail: alloc::string::String::from("源或目标让出的段是空的，无法推进"),
        }),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送请求后缀流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求后缀流原样搬进子流的发送半边。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendRequestSuffix, pub, new(pub(crate)))]
async fn session_send_req_suffix_async_<'a, 'f, C, R, TyTok>(
    session: &'f mut MptpSession<'a, C>,
    suffix: &'f mut R,
    cancel: TyTok,
) -> Result<SessionState, ClientError>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    if !matches!(
        session.state_,
        SessionState::WaitingResp | SessionState::SendingSuffix
    ) {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }
    session.state_ = SessionState::SendingSuffix;
    let tx = session.core_.tx_mut_()?;
    copy_all_async_(suffix, tx, cancel.child_token())
        .await
        .map_err(suffix_copy_error_)?;
    session.state_ = SessionState::WaitingResp;
    Result::Ok(session.state_)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 接收响应前缀 / 体 / 续流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读取响应前缀并**当场定下体边界**。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvRespPrefix, pub, new(pub(crate)))]
async fn session_recv_resp_prefix_async_<'f, 'a, C, K>(
    session: &'f mut MptpSession<'a, C>,
    cancel: K,
) -> Result<RespPrefix<'a>, ClientError>
where
    C: TrConfig,
    K: TrCancellationToken,
{
    if !matches!(
        session.state_,
        SessionState::WaitingResp | SessionState::SendingSuffix
    ) {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }

    let prefix = {
        let rx = session.core_.rx_mut_()?;
        read_response_prefix_with_async(rx, &session.alloc_)
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| ClientError::Protocol(ProtocolError::ResponsePrefixDecode(err)))?
    };

    let method = session.core_.request_().map(RequestPrefix::method);
    let plan =
        BodyPlan::for_response_(prefix.body_info(), method).map_err(ClientError::Protocol)?;
    session.core_.set_body_plan_(plan);
    session.core_.set_body_state_(BodyState::Fresh);
    session.state_ = SessionState::PrefixReady;
    Result::Ok(prefix)
}

/// 取回复体的读视图。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvRespBody, pub, new(pub(crate)))]
async fn session_recv_resp_body_async_<'a, 'f, C, TyTok>(
    session: &'f mut MptpSession<'a, C>,
    cancel: TyTok,
) -> Result<RespBodyRead<'f, C>, ClientError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    if session.state_ != SessionState::PrefixReady {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }
    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;

    session.state_ = SessionState::BodyReady;
    let (rx, body_state) = session.core_.rx_and_body_mut_()?;
    Result::Ok(BodyRead::from_plan_(plan, rx, body_state))
}

/// 进入续流阶段：先把**尚未读完的体**代读丢弃，再把接收半边当作裸字节续流交出去。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvSuffix, pub, new(pub(crate)))]
async fn session_recv_suffix_async_<'a, 'f, C, TyTok>(
    session: &'f mut MptpSession<'a, C>,
    cancel: TyTok,
) -> Result<RespSuffixRead<'f, C>, ClientError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    if !matches!(
        session.state_,
        SessionState::PrefixReady | SessionState::BodyReady
    ) {
        let actual = session.state_;
        return Result::Err(ClientError::State(StateError::Client { actual }));
    }

    let plan = session
        .core_
        .body_plan_()
        .ok_or(StateError::NoRequestPrefix)?;

    {
        let (rx, body_state) = session.core_.rx_and_body_mut_()?;
        crate::session_core_::drain_body_async_(plan, body_state, rx, cancel.child_token())
            .await
            .map_err(ClientError::Protocol)?;
    }
    session.core_.set_body_state_(BodyState::Fresh);
    session.state_ = SessionState::SuffixReady;

    let (rx, _) = session.core_.rx_and_body_mut_()?;
    Result::Ok(SuffixRead::new_(rx))
}

#[cfg(test)]
mod tests_ {
    //! 会话状态机中**纯本地**的那部分判定。
    //!
    //! 这里刻意不建连接：体边界的判定只依赖「前缀给出的传输声明」与「本端请求的方法」，
    //! 两者都是纯数据，不需要真实子流。
    //!
    //! Unit tests for the purely local half of the session state machine. The boundary
    //! decision depends only on the transport declaration and the request method, so no
    //! real channel is involved here.

    use abs_mptp::{
        messaging::BodySize,
        spec::{NumOrStr, access_method::AccessMethod},
    };

    use super::*;

    /// 测试体边界判定把三种传输声明各归各位。
    /// Test that the body-boundary decision maps each transport declaration to its own plan.
    ///
    /// - 手段：分别以「没有体」「定长 12 字节」「分块」三种声明调用 `BodyPlan::from_info_`。
    /// - 判断：三次判定依次为 `Absent` / `Sized { size_: 12 }` / `Chunked`。
    ///
    /// - Means: call `BodyPlan::from_info_` with the absent / sized-12 / chunked declarations.
    /// - Verdict: the plans are `Absent`, `Sized { size_: 12 }` and `Chunked`.
    #[test]
    fn body_plan_maps_declarations_() {
        assert_eq!(
            BodyPlan::from_info_(BodySize::NilBody).expect("没有体应当判成 Absent"),
            BodyPlan::Absent
        );
        assert_eq!(
            BodyPlan::from_info_(BodySize::FixedLength(NumOrStr::Str("12")))
                .expect("文本形态的定长 12 应当判成 Sized"),
            BodyPlan::Sized { size_: 12usize }
        );
        assert_eq!(
            BodyPlan::from_info_(BodySize::FixedLength(NumOrStr::Num(12)))
                .expect("数字形态的定长 12 应当判成 Sized"),
            BodyPlan::Sized { size_: 12usize }
        );
        assert_eq!(
            BodyPlan::from_info_(BodySize::ChunkedData).expect("分块应当判成 Chunked"),
            BodyPlan::Chunked
        );
    }

    /// 测试 `Head` / `Drop` 的回复声明了体时按协议违规当场失败。
    /// Test that a declared body on a `Head` / `Drop` response fails as a protocol violation.
    ///
    /// - 手段：对 `Head` 与 `Drop` 两个方法分别给出「定长 8」与「分块」两种带体声明。
    /// - 判断：六次调用全部返回 `Err(ProtocolError::ResponseBodyOnHeadOrDrop)`。
    ///
    /// - Means: with the `Head` and `Drop` methods, call `BodyPlan::for_response_` on both
    ///   sized forms and a chunked declaration.
    /// - Verdict: all six calls return `Err(ProtocolError::ResponseBodyOnHeadOrDrop)`.
    #[test]
    fn body_plan_rejects_body_on_head_and_drop_() {
        for method in [AccessMethod::Head, AccessMethod::Drop] {
            for info in [
                BodySize::FixedLength(NumOrStr::Str("8")),
                BodySize::FixedLength(NumOrStr::Num(8)),
                BodySize::ChunkedData,
            ] {
                let res = BodyPlan::for_response_(info, Option::Some(method));
                assert!(
                    matches!(
                        res,
                        Result::Err(ProtocolError::ResponseBodyOnHeadOrDrop { .. })
                    ),
                    "{method:?} 的回复带体（{info:?}）应当报协议违规，实际是 {res:?}"
                );
            }
        }
    }

    /// 测试定长声明不是合法长度时当场失败，而不是把非法值当成长度 0。
    /// Test that a malformed `Body_Size` fails instead of silently becoming zero bytes.
    ///
    /// - 手段：给出 `FixedLength(Str("12x"))` 与 `FixedLength(Str(""))` 两个非法声明。
    /// - 判断：两次都返回 `Err(ProtocolError::InvalidBodySize)`。
    ///
    /// - Means: feed the malformed declarations `FixedLength(Str("12x"))` and
    ///   `FixedLength(Str(""))`.
    /// - Verdict: both return `Err(ProtocolError::InvalidBodySize)`.
    #[test]
    fn body_plan_rejects_malformed_size_() {
        for text in ["12x", ""] {
            let res = BodyPlan::for_response_(
                BodySize::FixedLength(NumOrStr::Str(text)),
                Option::Some(AccessMethod::View),
            );
            assert!(
                matches!(res, Result::Err(ProtocolError::InvalidBodySize { .. })),
                "非法长度声明 {text:?} 应当报错，实际是 {res:?}"
            );
        }
    }

    /// 测试「没有体」在状态机里也是一个**明确的**边界，而不是「不知道」。
    /// Test that an absent body is an explicit boundary rather than an unknown one.
    ///
    /// - 手段：以 `NilBody` 判定一次，并与 `Chunked` 的判定做比较。
    /// - 判断：`Absent` 与 `Chunked` 不相等，且 `Absent` 不是 `Sized`。
    ///
    /// - Means: decide once with `NilBody` and compare it against the chunked decision.
    /// - Verdict: `Absent` differs from `Chunked` and is not a `Sized`.
    #[test]
    fn absent_body_is_its_own_boundary_() {
        let absent = BodyPlan::from_info_(BodySize::NilBody).expect("没有体应当判成 Absent");
        let chunked = BodyPlan::from_info_(BodySize::ChunkedData).expect("分块应当判成 Chunked");
        assert_ne!(absent, chunked);
        assert!(!matches!(absent, BodyPlan::Sized { .. }));
    }
}
