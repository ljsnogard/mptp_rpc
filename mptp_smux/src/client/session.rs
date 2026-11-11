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

use alloc::string::ToString;

use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    client::{SessionState, TrMptpSession},
    messaging::TrMsgPrefix,
};
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::{RequestPrefix, chunked::ChunkedEncode};

use crate::{
    body_::{BodyError, BodyPlan, BodyRead, BodyState, SuffixRead},
    copy_::{CopyError, copy_all_async_, copy_sized_async_},
    error_::{ClientError, ProtocolError, StateError},
    messaging,
    session_core_::SessionCore,
    x_deps::{abs_buff, abs_cancel, gen_mcf2},
};
use super::config::{ChannelRx, ChannelTx, RespPrefix, TrConfig};

/// 回复体的读视图。
pub type RespBodyRead<'f, C> = BodyRead<'f, ChannelRx<C>>;

/// 响应续流的读视图。
pub type RespSuffixRead<'f, C> = SuffixRead<'f, ChannelRx<C>>;

/// 报文体读视图的错误；保留旧名字供下游使用。
pub type RespBodyError<TyErr> = BodyError<TyErr>;

/// 一次请求 / 回复过程的会话。
///
/// 字段不对外公开：调用者只能通过 [`TrMptpSession`] 的契约驱动它。
pub struct MptpSession<C>
where
    C: TrConfig,
{
    /// 共享的会话核心：两个半边、请求前缀、体边界与体进度。
    core_: SessionCore<ChannelTx<C>, ChannelRx<C>>,

    /// 客户端侧的状态机。
    state_: SessionState,
}

impl<C> MptpSession<C>
where
    C: TrConfig,
{
    /// 由「请求前缀 + 该子流的收发半边」构造。
    ///
    /// 只允许客户端实现（[`super::client_`]）在最终裁决成功、且请求前缀已经写出之后调用。
    ///
    /// 初始状态由**前缀里的体声明**决定：带体请求处于 `SendingBody`（体还没发，不能直接读
    /// 响应前缀），没有体的请求处于 `WaitingResp`（请求已经整体发出）。
    pub(crate) fn new_(
        request: RequestPrefix,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
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
        })
    }

    /// 取回本会话对应的请求前缀。
    ///
    /// 它是调用者判断「下一步该发什么」的唯一依据：前缀里的 `body_info` 已经说明体是定长
    /// 还是分块、有没有体。本层不替调用者解释成业务语义。
    pub fn req_prefix(&self) -> Option<&RequestPrefix> {
        self.core_.request_()
    }

    /// 半关闭写端：丢弃发送半边，保留接收半边继续读回复。
    ///
    /// 对应协议 §12.1 的「写完请求后关闭写方向」。在 `SendingBody` 状态下调用会失败——
    /// 体还没发完就关写端会把请求截断。
    pub fn close_req_async<'f>(&'f mut self) -> CloseReqAsync<'f, 'f, C>
    where
        C: 'f,
    {
        CloseReqAsync::new(self)
    }
}

impl<C> TrMptpSession<C> for MptpSession<C>
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
        = SendRequestBodyAsync<'f, 'f, C, R>
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
        = SendRequestSuffixAsync<'f, 'f, C, R>
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
        = SessionRecvRespPrefixAsync<'f, 'f, C>
    where
        Self: 'f;

    fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f> {
        SessionRecvRespPrefixAsync::new(self)
    }

    type RecvRespBodyAsync<'f>
        = SessionRecvRespBodyAsync<'f, 'f, C>
    where
        Self: 'f;

    fn recv_resp_body_async<'f>(&'f mut self) -> Self::RecvRespBodyAsync<'f> {
        SessionRecvRespBodyAsync::new(self)
    }

    type RecvSuffixAsync<'f>
        = SessionRecvSuffixAsync<'f, 'f, C>
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
async fn session_close_req_async_<'f, C, K>(
    session: &'f mut MptpSession<C>,
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
async fn session_send_req_body_async_<'f, C, R, TyTok>(
    session: &'f mut MptpSession<C>,
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
        CopyError::EmptySegment => {
            ClientError::Protocol(ProtocolError::RequestBodyRead {
                detail: alloc::string::String::from("源或目标让出的段是空的，无法推进"),
            })
        }
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
        CopyError::EmptySegment => {
            ClientError::Protocol(ProtocolError::SuffixRead {
                detail: alloc::string::String::from("源或目标让出的段是空的，无法推进"),
            })
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送请求后缀流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求后缀流原样搬进子流的发送半边。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendRequestSuffix, pub, new(pub(crate)))]
async fn session_send_req_suffix_async_<'f, C, R, TyTok>(
    session: &'f mut MptpSession<C>,
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
async fn session_recv_resp_prefix_async_<'f, C, K>(
    session: &'f mut MptpSession<C>,
    cancel: K,
) -> Result<RespPrefix<C>, ClientError>
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
        messaging::read_response_prefix_async(rx)
            .may_cancel_with(cancel.child_token())
            .await
            .map_err(|err| {
                ClientError::Protocol(ProtocolError::ResponsePrefixDecode(err))
            })?
    };

    let method = session.core_.request_().map(RequestPrefix::method);
    let plan = BodyPlan::for_response_(prefix.body_info(), method).map_err(ClientError::Protocol)?;
    session.core_.set_body_plan_(plan);
    session.core_.set_body_state_(BodyState::Fresh);
    session.state_ = SessionState::PrefixReady;
    Result::Ok(prefix)
}

/// 取回复体的读视图。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SessionRecvRespBody, pub, new(pub(crate)))]
async fn session_recv_resp_body_async_<'f, C, TyTok>(
    session: &'f mut MptpSession<C>,
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
async fn session_recv_suffix_async_<'f, C, TyTok>(
    session: &'f mut MptpSession<C>,
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

    use super::*;
    use abs_mptp::{
        messaging::BodySize,
        spec::{NumOrStr, access_method::AccessMethod},
    };

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
