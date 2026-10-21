//! 客户端的会话对象：一条已建成子流上的「发请求 → 收回复」全过程。
//!
//! 一个 [`MptpSession`] 围绕 `abs_smux` 概念里的一条 channel 进行：它持有该子流的收发半边
//! （`accept_async` 的产物）与本次请求的前缀，因此调用者可以在同一条子流上把请求体发完、
//! 把回复读完。
//!
//! # 请求方向的三步与它们的状态
//!
//! | 入口 | 写什么 | 状态迁移 |
//! | --- | --- | --- |
//! | [`TrMptpClient::request_async`]（客户端实现） | 请求前缀 | → `SendingBody`（有体）/ `WaitingResp` |
//! | [`TrMptpSession::send_req_body_async`] | 请求体（按前缀的体声明搬运） | `SendingBody` → `WaitingResp` |
//! | [`TrMptpSession::send_req_suffix_async`] | 请求后缀流（裸字节） | `WaitingResp` → `SendingSuffix` → `WaitingResp` |
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
//! 体边界**完全由前缀决定**（[`TrMsgPrefix::body_info`]）：没有体就一个字节都不读，定长体
//! 恰好读那么多，分块体读到终止块。绝不用猜出来的长度去切分后续字节——那会破坏这条子流上
//! 报文之间的对齐。
//!
//! # 体读视图为什么同时是「一个类型」和「两种档位」
//!
//! 上游的两个现成视图（限长 [`LimitedReader`]、分块 [`ChunkedReader`]）是不同的类型，而
//! [`TrMptpSession::RespBodyRead`] 是一个关联类型，只能是一个类型。因此 [`RespBodyRead`] 把
//! 二者装进同一个枚举，并实现 [`TrMptpBuffRead`]：**调用者拿到视图之后，用
//! `try_into_chunked` / `try_into_limited` 取出前缀已经决定的那一种读口**。取错分支会得到
//! 「另一个读口」原样还回来（类型上二选一），而不是静默改变读法。
//!
//! 「体读到哪了」在视图被 `try_into_*` 取走或 Drop 时写回会话——**视图一旦交出去，会话就再也
//! 看不到它的内部状态**（自引用的状态无法用借用表达），而续流的起点恰恰取决于体有没有被
//! 读完。响应续流是裸字节，没有分块帧也没有长度声明；因此 [`RespSuffixRead`] 的两个
//! `try_into_*` 都交出同一个「额度用不完」的限长视图。
//!
//! # `session`: one channel's worth of request sending and response reading
//!
//! The request is sent in protocol order: the prefix first, then — according to what the prefix
//! announced — the body, then an optional raw suffix stream. Both body and suffix are handed in
//! as already-serialised byte streams. On the response side the body view is a single type over
//! the two upstream readers, and `TrMptpBuffRead` is how the caller extracts the one the prefix
//! already decided.

use core::{
    future::{Future, IntoFuture},
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrConsumerState, TrProducerState},
    error::{ReadErrTag, TrTaggedError},
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::{
    client::{SessionState, TrMptpSession},
    messaging::{
        BodySize, TrMsgPrefix,
        chunked::{
            ChunkedEncode, ChunkedReadAsync, ChunkedReadError, ChunkedReadFuture, ChunkedReader,
        },
        limited::{
            LimitedReader, LimitedReaderAsync, LimitedReaderError, LimitedReaderFuture,
            LimitedRefSegm, LimitedWriter,
        },
        read::TrMptpBuffRead,
    },
    spec::{NumOrStr, access_method::AccessMethod},
};
use anylr::SomeOf;
use gen_mcf2::gen_may_cancel_future;

use crate::{
    messaging,
    x_deps::{abs_cancel, anylr, gen_mcf2},
};
use super::{
    client_::ClientError,
    config::{ChannelRx, ChannelTx, ReqPrefix, RespPrefix, TrConfig},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 会话
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一次请求 / 回复过程的会话。
///
/// 字段不对外公开：调用者只能通过 [`TrMptpSession`] 的契约驱动它。
pub struct MptpSession<C>
where
    C: TrConfig,
{
    /// 本会话对应的请求前缀。按值持有：发送请求体时要回到它，看头里声明的体传输方式；
    /// 判定回复体的边界时也要回到它，看请求用的是哪个 access method（`Head` / `Drop` 的
    /// 回复按协议不带本体内容）。
    request_: Option<ReqPrefix<C>>,

    /// 本条回复的体边界。读响应前缀时**一次定下**，之后不再回头看前缀——体边界是一个
    /// 「读多少字节」的问题，与前缀的表示形态无关。
    body_plan_: Option<BodyPlan>,

    /// 子流的发送半边。
    ///
    /// 请求体与 `Push` 的续流都要经它写出；请求方向的收尾由会话被放掉时完成——丢弃发送半边
    /// 就是半关闭。
    tx_: ChannelTx<C>,

    /// 子流的接收半边。读回复时直接接段接口，中间不留缓冲。
    rx_: ChannelRx<C>,

    /// 会话状态机。
    state_: SessionState,

    /// 体的读取进度；续流阶段据此跳过尚未读完的体。
    body_: BodyState,
}

/// 本条回复的体边界。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyPlan {
    /// 没有体：一个字节都不读。
    Absent,

    /// 定长体：恰好这么多字节。
    Sized { size_: usize },

    /// 分块体：总长未知，读到终止块为止。
    Chunked,
}

impl BodyPlan {
    /// 只按前缀里的体声明定边界，不掺任何方法语义。
    ///
    /// 请求方向用它：能发多少体完全由**已经写出去的前缀**决定。声明的长度不是合法十进制数
    /// 时当场失败——把一个解析不出来的长度当成 `0` 会让体一个字节都不发，而头里已经声明了
    /// 长度，两端从此错位。
    fn from_info_(info: BodySize<'_>) -> Result<Self, ClientError> {
        match info {
            BodySize::NilBody => Result::Ok(BodyPlan::Absent),
            BodySize::FixedLength(size) => {
                let size_ = match size {
                    NumOrStr::Num(size) => size,
                    NumOrStr::Str(text) => text.trim().parse::<usize>().map_err(|err| {
                        ClientError::ReqErr(format!("Body_Size 头不是合法的长度（{text}）：{err}"))
                    })?,
                };
                Result::Ok(BodyPlan::Sized { size_ })
            }
            BodySize::ChunkedData => Result::Ok(BodyPlan::Chunked),
        }
    }

    /// 由回复前缀给出的体声明（以及本端请求的方法）定下体边界。
    ///
    /// `Head` / `Drop` 的回复按协议不带本体内容，前缀却声明了体，说明两端对协议的理解已经
    /// 不一致——宁可在读第一个字节之前失败，也不要带着错位的流继续走。
    fn of_(info: BodySize<'_>, method: Option<AccessMethod>) -> Result<Self, ClientError> {
        let has_body = !matches!(info, BodySize::NilBody);
        if has_body
            && matches!(
                method,
                Option::Some(AccessMethod::Head) | Option::Some(AccessMethod::Drop)
            )
        {
            let text = format!(
                "{:?} 的回复按协议不带报文体，前缀却声明了体",
                method.expect("刚刚确认过是 Some")
            );
            return Result::Err(ClientError::RespErr(format!("协议违规：{text}")));
        }
        Self::from_info_(info).map_err(|err| match err {
            ClientError::ReqErr(text) => ClientError::RespErr(text),
            other => other,
        })
    }
}

/// 体的读取进度。
///
/// 它只在**体读视图被取走或放回**时才更新，因此它回答的是「按当前视图的进度，体的剩余部分
/// 还有多少」——续流阶段要靠它决定是否需要代读丢弃。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum BodyState {
    /// 还没有体的进度可言（前缀未到，或体还没开始读）。
    Fresh,

    /// 定长体：还剩 `left_` 字节没被读走。
    Sized { left_: usize },

    /// 分块体：`ended_` 表示是否已经读到终止块。
    Chunked { ended_: bool },
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
        request: ReqPrefix<C>,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> Self {
        let has_body = !matches!(request.body_info(), BodySize::NilBody);
        let state_ = if has_body {
            SessionState::SendingBody
        } else {
            SessionState::WaitingResp
        };
        MptpSession {
            request_: Option::Some(request),
            body_plan_: Option::None,
            tx_: tx,
            rx_: rx,
            state_,
            body_: BodyState::Fresh,
        }
    }

    /// 取回本会话对应的请求前缀。
    ///
    /// 它是调用者判断「下一步该发什么」的唯一依据：前缀里的 `body_info` 已经说明体是定长
    /// 还是分块、有没有体。本层不替调用者解释成业务语义。
    pub const fn req_prefix(&self) -> Option<&ReqPrefix<C>> {
        self.request_.as_ref()
    }
}

impl<C> TrMptpSession<C> for MptpSession<C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    /// 回复体的读视图：一个类型，两种档位（由前缀决定）。
    type RespBodyRead<'f> = RespBodyRead<'f, C> where Self: 'f;

    /// 响应续流的读视图：裸字节，两种 `try_into_*` 都交出同一个额度用不完的限长视图。
    type RespSuffixRead<'f> = RespSuffixRead<'f, C> where Self: 'f;

    type Err = ClientError;

    fn state(&self) -> SessionState {
        self.state_
    }

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // send_req_body_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type SendReqBodyAsync<'f, R> = SendRequestBodyAsync<'f, 'f, C, R>
    where
        R: 'f + TrBuffRead<u8> + TrConsumerState,
        Self: 'f;

    fn send_req_body_async<'f, R>(
        &'f mut self,
        body: &'f mut R,
    ) -> Self::SendReqBodyAsync<'f, R>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        SendRequestBodyAsync::new(self, body)
    }

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // send_req_suffix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type SendReqSuffixAsync<'f, R> = SendRequestSuffixAsync<'f, 'f, C, R>
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

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_prefix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RecvRespPrefixAsync<'f> = SessionRecvRespPrefixAsync<'f, 'f, C> where Self: 'f;

    fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f> {
        SessionRecvRespPrefixAsync::new(self)
    }

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_body_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RecvRespBodyAsync<'f> = SessionRecvRespBodyAsync<'f, 'f, C> where Self: 'f;

    fn recv_resp_body_async<'f>(&'f mut self) -> Self::RecvRespBodyAsync<'f> {
        SessionRecvRespBodyAsync::new(self)
    }

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_suffix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RecvSuffixAsync<'f> = SessionRecvSuffixAsync<'f, 'f, C> where Self: 'f;

    fn recv_resp_suffix_async<'f>(&'f mut self) -> Self::RecvSuffixAsync<'f> {
        SessionRecvSuffixAsync::new(self)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送请求体
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 按**已写出去的前缀**里声明的体传输方式，把请求体搬进子流的发送半边。
///
/// 体源是一个**已经序列化好的字节流**（[`TrBuffRead`]）：业务值怎么变成字节由上层决定，
/// 本层只按协议搬运。定长按 `Body_Size` 只搬那么多字节；分块逐段封块并补终止块。两条路都
/// 是段到段的直接拷贝，中间不留中转缓冲。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的 `'f`
// 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
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
        let info = format!("会话处于 {:?}，此时不接受发送请求体", session.state_);
        return Result::Err(ClientError::State(info));
    }

    // 体边界只由**已经写出去的前缀**决定。先把借用缩到前缀上，算出 owned 的 plan，再拿
    // 发送半边的可变借用。
    let plan = match session.request_.as_ref() {
        Option::Some(request) => BodyPlan::from_info_(request.body_info())?,
        Option::None => {
            return Result::Err(ClientError::State(
                "会话没有请求前缀，无法判定请求体的传输方式".to_string(),
            ));
        }
    };

    match plan {
        BodyPlan::Absent => Result::Err(ClientError::State(
            "请求前缀没有声明报文体，不接受发送请求体".to_string(),
        )),
        BodyPlan::Sized { size_ } => {
            let MptpSession { tx_, state_, .. } = session;
            copy_sized_async_(body, tx_, size_, cancel.child_token()).await?;
            *state_ = SessionState::WaitingResp;
            Result::Ok(*state_)
        }
        BodyPlan::Chunked => {
            let MptpSession { tx_, state_, .. } = session;
            let mut encode = ChunkedEncode::new(body, tx_);
            encode
                .write_all_async()
                .may_cancel_with(cancel.child_token())
                .await
                .map_err(|err| ClientError::ReqErr(format!("分块发送请求体失败：{err}")))?;
            *state_ = SessionState::WaitingResp;
            Result::Ok(*state_)
        }
    }
}

/// 定长搬运：只搬 `size` 字节。
///
/// 目标被套成 [`LimitedWriter`]：额度一旦用尽，多写的字节会被挡下，而不是溢到后续报文的
/// 位置上。源在搬完之前结束是**截断**——头里声明的长度必须被如实满足。
async fn copy_sized_async_<R, W, K>(
    src: &mut R,
    dst: &mut W,
    size: usize,
    cancel: K,
) -> Result<usize, ClientError>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
    K: TrCancellationToken,
{
    let mut limited = LimitedWriter::new(dst, size);
    let mut total = 0usize;
    while total < size {
        if cancel.is_cancelled() {
            return Result::Err(ClientError::Cancelled);
        }
        let demand = Demand::no_more_than(size - total);
        let res = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if !res.contains_left() {
            let err = res.pick_right().expect("既不含左值，则必含右值");
            if matches!(err.err_tag(), ReadErrTag::Closing) {
                return Result::Err(ClientError::ReqErr(format!(
                    "请求体声明了 {size} 字节，源在搬完之前就结束了（已搬 {total} 字节）"
                )));
            }
            return Result::Err(ClientError::ReqErr(format!("读取请求体失败：{err}")));
        }
        let mut segm = res.pick_left().expect("刚刚确认过含左值");
        let want = segm
            .iter_slices()
            .into_iter()
            .map(<[u8]>::len)
            .sum::<usize>();
        if want == 0usize {
            return Result::Err(ClientError::ReqErr(
                "源让出的段是空的，无法推进".to_string(),
            ));
        }

        let write_demand = Demand::no_more_than(want);
        let wres = limited
            .write_async(&write_demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if !wres.contains_left() {
            let err = wres.pick_right().expect("既不含左值，则必含右值");
            return Result::Err(ClientError::ReqErr(format!("写入请求体失败：{err}")));
        }
        let mut target = wres.pick_left().expect("刚刚确认过含左值");
        let moved = {
            let mut source = segm.as_segm_ref();
            let mut target_child = target.as_segm_mut();
            source.move_items_to_segm(&mut target_child)
        };
        if moved == 0usize {
            return Result::Err(ClientError::ReqErr(
                "目标让出的段是空的，无法推进".to_string(),
            ));
        }
        // 目标段在这里回收：写入量随之结算到 `LimitedWriter` 的额度上。
        drop(target);
        total += moved;
    }
    Result::Ok(total)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送请求后缀流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求后缀流原样搬进子流的发送半边。
///
/// 后缀流在协议上是**裸字节**：没有长度声明、没有块帧，读到源结束为止。因此这里不做任何
/// 包装——包装是前缀与请求体之间的事，后缀流已经是「体之后」的原始续流。
///
/// 发送完这段后缀之后，会话状态回到 `WaitingResp`；请求方向的最终收尾在会话被放掉时完成。
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
        let info = format!("会话处于 {:?}，此时不接受发送请求后缀流", session.state_);
        return Result::Err(ClientError::State(info));
    }
    session.state_ = SessionState::SendingSuffix;
    let MptpSession { tx_, state_, .. } = session;
    copy_all_async_(suffix, tx_, cancel.child_token()).await?;
    *state_ = SessionState::WaitingResp;
    Result::Ok(*state_)
}

/// 把源里的字节原样搬到目标，直到源结束（`Closing`）；返回搬走的字节数。
async fn copy_all_async_<R, W, K>(
    src: &mut R,
    dst: &mut W,
    cancel: K,
) -> Result<usize, ClientError>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
    K: TrCancellationToken,
{
    // 额度用不完：这条流没有长度声明，读到源结束为止。
    let mut limited = LimitedWriter::new(dst, usize::MAX);
    let mut total = 0usize;
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ClientError::Cancelled);
        }
        let demand = Demand::at_least(1usize);
        let res = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if !res.contains_left() {
            let err = res.pick_right().expect("既不含左值，则必含右值");
            if matches!(err.err_tag(), ReadErrTag::Closing) {
                return Result::Ok(total);
            }
            return Result::Err(ClientError::ReqErr(format!(
                "读取请求后缀流失败：{err}"
            )));
        }
        let mut segm = res.pick_left().expect("刚刚确认过含左值");
        let want = segm
            .iter_slices()
            .into_iter()
            .map(<[u8]>::len)
            .sum::<usize>();
        if want == 0usize {
            continue;
        }

        let write_demand = Demand::no_more_than(want);
        let wres = limited
            .write_async(&write_demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if !wres.contains_left() {
            let err = wres.pick_right().expect("既不含左值，则必含右值");
            return Result::Err(ClientError::ReqErr(format!(
                "写入请求后缀流失败：{err}"
            )));
        }
        let mut target = wres.pick_left().expect("刚刚确认过含左值");
        let moved = {
            let mut source = segm.as_segm_ref();
            let mut target_child = target.as_segm_mut();
            source.move_items_to_segm(&mut target_child)
        };
        if moved == 0usize {
            return Result::Err(ClientError::ReqErr(
                "目标让出的段是空的，无法推进".to_string(),
            ));
        }
        drop(target);
        total += moved;
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 接收响应前缀 / 体 / 续流
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读取响应前缀（状态码 + 头）并**当场定下体边界**。
///
/// 解码完全由 [`crate::messaging`] 负责：本层只保证「在正确的状态下、借正确的半边、调用
/// 正确的入口」，不猜线上格式。
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
    // 请求方向到此为止：带体请求必须先把体发完（状态在发体成功后转 `WaitingResp`），
    // 否则体还留在流里，接着读响应只会读到错位的字节。
    if !matches!(
        session.state_,
        SessionState::WaitingResp | SessionState::SendingSuffix
    ) {
        let info = format!("会话处于 {:?}，不接受读取响应前缀", session.state_);
        return Result::Err(ClientError::State(info));
    }
    let prefix = messaging::read_response_prefix_async(&mut session.rx_)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ClientError::RespErr(format!("解码响应前缀失败：{err}")))?;

    // 体边界到此才确定，因此「定下边界」与「推进状态」必须同时发生：调用者拿到前缀之后
    // 立刻就能读体，中间没有第二个判定点。
    let method = session.request_.as_ref().map(|req| req.method());
    let plan = BodyPlan::of_(prefix.body_info(), method)?;
    session.body_plan_ = Option::Some(plan);
    session.body_ = BodyState::Fresh;
    session.state_ = SessionState::PrefixReady;
    Result::Ok(prefix)
}

/// 取回复体的读视图。
///
/// 宽度由**前缀**唯一决定（见 [`BodyPlan`]），`Head` / `Drop` 的协议核对在读到前缀时就做过
/// 了，因此这里只做装配。
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
        let info = format!("会话处于 {:?}，不接受读取回复体", session.state_);
        return Result::Err(ClientError::State(info));
    }
    let Option::Some(plan) = session.body_plan_ else {
        return Result::Err(ClientError::State(
            "会话还没有读到响应前缀，无法判定回复体边界".to_string(),
        ));
    };

    // 借出两个**不相交**的字段：接收半边给视图，进度槽留给视图在被取走 / 放回时写。
    let MptpSession {
        rx_, body_, state_, ..
    } = session;
    *state_ = SessionState::BodyReady;

    let view = match plan {
        // 没有体：额度为 0 的限长视图。读起来立刻表现为 EOF，且**一个字节都不会碰**接收
        // 半边——这正是「没有体就不预读」那条纪律的落地方式。
        BodyPlan::Absent => BodyView::Sized {
            reader_: LimitedReader::new(rx_, 0usize),
            state_: body_,
        },
        BodyPlan::Sized { size_ } => BodyView::Sized {
            reader_: LimitedReader::new(rx_, size_),
            state_: body_,
        },
        BodyPlan::Chunked => BodyView::Chunked {
            reader_: ChunkedReader::new(rx_),
            state_: body_,
        },
    };
    Result::Ok(RespBodyRead {
        view_: Option::Some(view),
    })
}

/// 进入续流阶段：先把**尚未读完的体**代读丢弃，再把接收半边当作裸字节续流交出去。
///
/// 「代读」这一步不能省：调用者完全可能在读到一半时放下体视图（协议只要求它别把体读成
/// 别人的字节），而续流的起点是体的终点——不丢完，续流就会从体中间开始。
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
        let info = format!("会话处于 {:?}，不接受读取续流", session.state_);
        return Result::Err(ClientError::State(info));
    }

    // 1. 把没读完的体丢完——按当前进度决定要丢多少。
    match session.body_ {
        BodyState::Fresh => {}
        BodyState::Sized { left_: 0usize } | BodyState::Chunked { ended_: true } => {}
        BodyState::Sized { left_ } => {
            let mut reader = LimitedReader::new(&mut session.rx_, left_);
            drain_to_end_async_(&mut reader, cancel.child_token()).await?;
        }
        BodyState::Chunked { ended_: false } => {
            let mut reader = ChunkedReader::new(&mut session.rx_);
            drain_to_end_async_(&mut reader, cancel.child_token()).await?;
        }
    }
    session.body_ = BodyState::Fresh;
    session.state_ = SessionState::SuffixReady;
    // 续流没有长度声明：额度取「用不完」，于是读到对端半关闭即为结束。
    Result::Ok(RespSuffixRead {
        reader_: LimitedReader::new(&mut session.rx_, usize::MAX),
    })
}

/// 读完一个读口的剩余内容并丢弃，返回丢弃的字节数。
///
/// 这里不走 `std::io` 适配层：只需要「把段借出来、不消费就放回」——放回即视为已消费（段的
/// 消费量在回收时结算），因此直接借段即可，既不必中转缓冲，也不引入 io 依赖。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DrainToEnd, pub(crate), new(pub(crate)))]
async fn drain_to_end_async_<'f, TyRx, TyTok>(
    reader: &'f mut TyRx,
    cancel: TyTok,
) -> Result<usize, ClientError>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let mut dropped = 0usize;
    let demand = Demand::exactly(4096usize);
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ClientError::Cancelled);
        }
        let some_of = reader
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if some_of.contains_left() {
            let segm = some_of.pick_left().expect("刚刚确认过含左值");
            // 借出即消费：段在这里被放回，消费量随之结算。
            dropped += segm.iter_slices().into_iter().map(<[u8]>::len).sum::<usize>();
            continue;
        }
        // 「读到边界」在两个视图里都表现为 `Closing` 标签的错误（限长视图是额度用尽，
        // 分块视图是终止块）；其余错误才是真的失败。
        let err = some_of.pick_right().expect("既不含左值，则必含右值");
        if matches!(err.err_tag(), ReadErrTag::Closing) {
            return Result::Ok(dropped);
        }
        let info = format!("丢弃未读完的报文体失败：{err}");
        return Result::Err(ClientError::RespErr(info));
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 报文体读视图
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回复体的字节视图：按前缀声明的传输方式二选一。
///
/// 两个档位共用同一套对外形状（[`TrBuffRead`] 的段接口），因此调用者不必关心这条回复是定长
/// 还是分块：任何解码器都能直接吃它。
///
/// **档位是实现细节**：对外只有一个类型，内部才分「定长 / 分块」，因此档位不会泄漏到任何
/// 公开签名里（enum 的变体字段没有独立的可见性，把档位写成公开 enum 就等于把内部状态机摆上
/// 台面）。需要取出「前缀已经决定的那一种读口」时，用 [`TrMptpBuffRead`] 的两个
/// `try_into_*`——取错分支会把另一个读口原样还回来。
///
/// # 取走或放回时会发生什么
///
/// 视图的读取进度只在两个时刻回到 [`MptpSession`]：`try_into_*` 把它取走时，以及它被
/// Drop 时。这是**必要**的：视图一旦交出去，会话就看不见它的内部状态，而进入续流阶段时
/// 要知道体还剩多少。
pub struct RespBodyRead<'f, C>
where
    C: TrConfig,
{
    /// 实际读口，二选一。`None` 表示它已经被 `try_into_*` 取走（此时 Drop 不再写回）。
    view_: Option<BodyView<'f, C>>,
}

/// [`RespBodyRead`] 的两个档位。
enum BodyView<'f, C>
where
    C: TrConfig,
{
    /// 定长档（含「没有体」：额度为 `0`）。
    Sized {
        /// 上游的限长读视图。
        reader_: LimitedReader<'f, ChannelRx<C>>,
        /// 进度槽：取走 / 放回时写入剩余额度。
        state_: &'f mut BodyState,
    },

    /// 分块档。
    Chunked {
        /// 上游的分块读视图。
        reader_: ChunkedReader<'f, ChannelRx<C>>,
        /// 进度槽：取走 / 放回时写入「是否读到终止块」。
        state_: &'f mut BodyState,
    },
}

/// 把一个档位「还剩多少」写回它挂着的进度槽。
fn write_back_<C>(view: BodyView<'_, C>)
where
    C: TrConfig,
{
    match view {
        BodyView::Sized { reader_, state_ } => {
            *state_ = BodyState::Sized {
                left_: reader_.remaining(),
            };
        }
        BodyView::Chunked { reader_, state_ } => {
            *state_ = BodyState::Chunked {
                ended_: reader_.is_ended(),
            };
        }
    }
}

impl<C> RespBodyRead<'_, C>
where
    C: TrConfig,
{
    /// 体是否已经读完（定长档是额度用尽，分块档是读到终止块）。
    pub fn is_finished(&self) -> bool {
        match self.view_.as_ref().expect("视图尚未被取走") {
            BodyView::Sized { reader_, .. } => reader_.is_exhausted(),
            BodyView::Chunked { reader_, .. } => reader_.is_ended(),
        }
    }

    /// 定长档还剩多少字节；分块档没有确定的总长，故为 `None`。
    pub fn remaining(&self) -> Option<usize> {
        match self.view_.as_ref().expect("视图尚未被取走") {
            BodyView::Sized { reader_, .. } => Option::Some(reader_.remaining()),
            BodyView::Chunked { .. } => Option::None,
        }
    }
}

impl<C> Drop for RespBodyRead<'_, C>
where
    C: TrConfig,
{
    fn drop(&mut self) {
        // 视图的内部状态只在这里回到会话手里；两个档位记的东西不同，但语义都是
        // 「体的剩余部分还有多少」。
        if let Option::Some(view) = self.view_.take() {
            write_back_(view);
        }
    }
}

impl<'f, C> TrMptpBuffRead for RespBodyRead<'f, C>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    type ChunkedRead = ChunkedReader<'f, ChannelRx<C>>;

    type LimitedRead = LimitedReader<'f, ChannelRx<C>>;

    /// 取出**分块**读口；前缀决定的是定长时，把定长读口原样还回来。
    fn try_into_chunked(mut self) -> Result<Self::ChunkedRead, Self::LimitedRead> {
        match self.view_.take().expect("视图只能被取走一次") {
            BodyView::Chunked { reader_, state_ } => {
                *state_ = BodyState::Chunked {
                    ended_: reader_.is_ended(),
                };
                Result::Ok(reader_)
            }
            BodyView::Sized { reader_, state_ } => {
                *state_ = BodyState::Sized {
                    left_: reader_.remaining(),
                };
                Result::Err(reader_)
            }
        }
    }

    /// 取出**定长**读口；前缀决定的是分块时，把分块读口原样还回来。
    fn try_into_limited(mut self) -> Result<Self::LimitedRead, Self::ChunkedRead> {
        match self.view_.take().expect("视图只能被取走一次") {
            BodyView::Sized { reader_, state_ } => {
                *state_ = BodyState::Sized {
                    left_: reader_.remaining(),
                };
                Result::Ok(reader_)
            }
            BodyView::Chunked { reader_, state_ } => {
                *state_ = BodyState::Chunked {
                    ended_: reader_.is_ended(),
                };
                Result::Err(reader_)
            }
        }
    }
}

/// 报文体读视图的错误：两个档位各自的错误原样带出。
#[derive(Clone, Debug)]
pub enum RespBodyError<TyErr> {
    /// 定长档报出的错误。
    Sized(LimitedReaderError<TyErr>),

    /// 分块档报出的错误。
    Chunked(ChunkedReadError<TyErr>),
}

impl<TyErr> core::fmt::Display for RespBodyError<TyErr>
where
    TyErr: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            RespBodyError::Sized(err) => write!(f, "定长报文体：{err}"),
            RespBodyError::Chunked(err) => write!(f, "分块报文体：{err}"),
        }
    }
}

impl<TyErr> core::error::Error for RespBodyError<TyErr> where TyErr: core::error::Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for RespBodyError<TyErr>
where
    TyErr: core::error::Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            RespBodyError::Sized(err) => err.err_tag(),
            RespBodyError::Chunked(err) => err.err_tag(),
        }
    }
}

/// 把「左值优先」的 `SomeOf` 的右值（错误）换一种包装。
///
/// `SomeOf` 不是枚举，只提供 `contains_left` / `pick_left` / `pick_right`，因此这里按它自己的
/// 查询接口拆分；`Expect` 的两处都紧接着一次确认，不会在真的取不到时 panic。
fn remap_right_<TyLeft, TyRight, TyNew>(
    value: SomeOf<TyLeft, TyRight>,
    wrap: impl FnOnce(TyRight) -> TyNew,
) -> SomeOf<TyLeft, TyNew> {
    if value.contains_left() {
        return SomeOf::new_left(value.pick_left().expect("刚刚确认过含左值"));
    }
    SomeOf::new_right(wrap(
        value.pick_right().expect("既不含左值，则必含右值"),
    ))
}

impl<C> TrBuffTryRead<u8> for RespBodyRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8>,
{
    // 两个档位的段类型**本来就是同一个**：上游两种视图都把借出的段包成 `LimitedRefSegm`，
    // 因此这里不需要再为段做一层分派。
    type SegmRef<'f>
        = LimitedRefSegm<'f, <ChannelRx<C> as TrBuffTryRead<u8>>::SegmRef<'f>>
    where
        Self: 'f;

    type Err = RespBodyError<<ChannelRx<C> as TrBuffTryRead<u8>>::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        match self.view_.as_mut().expect("视图已经被取走") {
            BodyView::Sized { reader_, .. } => {
                remap_right_(reader_.try_read(demand), RespBodyError::Sized)
            }
            BodyView::Chunked { reader_, .. } => {
                remap_right_(reader_.try_read(demand), RespBodyError::Chunked)
            }
        }
    }
}

impl<C> TrBuffRead<u8> for RespBodyRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = RespBodyReadAsync<'f, C>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        match self.view_.as_mut().expect("视图已经被取走") {
            BodyView::Sized { reader_, .. } => {
                RespBodyReadAsync::Sized(reader_.read_async(demand))
            }
            BodyView::Chunked { reader_, .. } => {
                RespBodyReadAsync::Chunked(reader_.read_async(demand))
            }
        }
    }
}

impl<C> TrConsumerState for RespBodyRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        match self.view_.as_ref().expect("视图已经被取走") {
            BodyView::Sized { reader_, .. } => reader_.consumer_state(),
            BodyView::Chunked { reader_, .. } => reader_.consumer_state(),
        }
    }
}

/// [`RespBodyRead::read_async`] 的产物：两个档位各自的异步读口。
pub enum RespBodyReadAsync<'f, C>
where
    C: TrConfig,
{
    /// 定长档。
    Sized(LimitedReaderAsync<'f, 'f, ChannelRx<C>>),

    /// 分块档。
    Chunked(ChunkedReadAsync<'f, 'f, ChannelRx<C>>),
}

impl<'f, C> TrMayCancel<'f> for RespBodyReadAsync<'f, C>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8>,
{
    type MayCancelFuture<'mf, TyTok>
        = RespBodyReadFuture<'mf, C, TyTok>
    where
        'mf: 'f,
        Self: 'mf,
        TyTok: 'mf + TrCancellationToken;

    type MayCancelOutput = SomeOf<
        LimitedRefSegm<'f, <ChannelRx<C> as TrBuffTryRead<u8>>::SegmRef<'f>>,
        RespBodyError<<ChannelRx<C> as TrBuffTryRead<u8>>::Err>,
    >;

    fn may_cancel_with<TyTok>(self, cancel: TyTok) -> Self::MayCancelFuture<'f, TyTok>
    where
        TyTok: 'f + TrCancellationToken,
    {
        match self {
            RespBodyReadAsync::Sized(async_) => {
                RespBodyReadFuture::Sized(async_.may_cancel_with(cancel))
            }
            RespBodyReadAsync::Chunked(async_) => {
                RespBodyReadFuture::Chunked(async_.may_cancel_with(cancel))
            }
        }
    }
}

impl<'f, C> IntoFuture for RespBodyReadAsync<'f, C>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8>,
{
    type IntoFuture = RespBodyReadFuture<'f, C, NonCancellableToken>;

    type Output = <Self as TrMayCancel<'f>>::MayCancelOutput;

    fn into_future(self) -> Self::IntoFuture {
        self.may_cancel_with(NonCancellableToken::new())
    }
}

/// [`RespBodyReadAsync::may_cancel_with`] 的产物：两个档位 future 的二选一。
///
/// 它实现 [`Future`] 的方式是**把 poll 原样转发给内部那个 future**；转发需要把
/// `Pin<&mut Self>` 降级成 `&mut Self`，因此下面那处 `unsafe` 是本文件唯一的一处。
pub enum RespBodyReadFuture<'f, C, TyTok>
where
    C: TrConfig,
    TyTok: TrCancellationToken,
{
    /// 定长档。
    Sized(LimitedReaderFuture<'f, 'f, ChannelRx<C>, TyTok>),

    /// 分块档。
    Chunked(ChunkedReadFuture<'f, 'f, ChannelRx<C>, TyTok>),
}

impl<'f, C, TyTok> Future for RespBodyReadFuture<'f, C, TyTok>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8>,
    TyTok: 'f + TrCancellationToken,
{
    type Output = SomeOf<
        LimitedRefSegm<'f, <ChannelRx<C> as TrBuffTryRead<u8>>::SegmRef<'f>>,
        RespBodyError<<ChannelRx<C> as TrBuffTryRead<u8>>::Err>,
    >;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 本枚举是「两个档位 future 的二选一」，没有自引用字段，也不实现 `Drop`；
        // 这里取出 `&mut self` 之后只做两件事——按变体取出内部 future、把它**原地**重新 pin
        // 住交给它的 `poll`。全程不移动、不替换、不落下任何字段，因此内部 future 在构造时得到
        // 的固定保证一直成立（这正是 `pin-project` 之类的库为枚举生成的那段代码）。
        let this = unsafe { self.get_unchecked_mut() };
        match this {
            RespBodyReadFuture::Sized(fut) => {
                let fut = unsafe { Pin::new_unchecked(fut) };
                fut.poll(cx)
                    .map(|res| remap_right_(res, RespBodyError::Sized))
            }
            RespBodyReadFuture::Chunked(fut) => {
                let fut = unsafe { Pin::new_unchecked(fut) };
                fut.poll(cx)
                    .map(|res| remap_right_(res, RespBodyError::Chunked))
            }
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 响应续流视图
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 响应续流的读视图。
///
/// 续流是**裸字节**：没有长度声明、没有块帧，读到对端半关闭为止。因此它内部始终是一个
/// 「额度用不完」的限长视图，[`TrMptpBuffRead`] 的两个 `try_into_*` 都交出同一个读口
/// ——续流本来就不存在「定长还是分块」这个分岔，调用者不必先猜一个再试。
pub struct RespSuffixRead<'f, C>
where
    C: TrConfig,
{
    /// 额度为 `usize::MAX` 的限长视图。
    reader_: LimitedReader<'f, ChannelRx<C>>,
}

impl<'f, C> TrMptpBuffRead for RespSuffixRead<'f, C>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    type ChunkedRead = LimitedReader<'f, ChannelRx<C>>;

    type LimitedRead = LimitedReader<'f, ChannelRx<C>>;

    /// 续流没有块帧，交出的就是裸字节读口。
    fn try_into_chunked(self) -> Result<Self::ChunkedRead, Self::LimitedRead> {
        Result::Err(self.reader_)
    }

    /// 同上。
    fn try_into_limited(self) -> Result<Self::LimitedRead, Self::ChunkedRead> {
        Result::Ok(self.reader_)
    }
}

impl<C> TrBuffTryRead<u8> for RespSuffixRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    type SegmRef<'f>
        = LimitedRefSegm<'f, <ChannelRx<C> as TrBuffTryRead<u8>>::SegmRef<'f>>
    where
        Self: 'f;

    type Err = LimitedReaderError<<ChannelRx<C> as TrBuffTryRead<u8>>::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        self.reader_.try_read(demand)
    }
}

impl<C> TrBuffRead<u8> for RespSuffixRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    type ReadAsync<'f>
        = LimitedReaderAsync<'f, 'f, ChannelRx<C>>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        self.reader_.read_async(demand)
    }
}

impl<C> TrConsumerState for RespSuffixRead<'_, C>
where
    C: TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    fn consumer_state(&self) -> Option<(usize, bool)> {
        self.reader_.consumer_state()
    }
}

#[cfg(test)]
mod tests_ {
    //! 会话状态机中**纯本地**的那部分判定。
    //!
    //! 这里刻意不建连接：体边界的判定只依赖「前缀给出的传输声明」与「本端请求的方法」，
    //! 两者都是纯数据，不需要真实子流。真正需要连接的那几条（读前缀 / 读体 / 丢体）留给
    //! 带 `abs_smux` 实现的集成测试。
    //!
    //! Unit tests for the purely local half of the session state machine. The boundary
    //! decision depends only on the transport declaration and the request method, so no real
    //! channel is involved here.

    use super::*;

    /// 测试体边界判定把三种传输声明各归各位。
    /// Test that the body-boundary decision maps each transport declaration to its own plan.
    ///
    /// - 手段：分别以「没有体」「定长 12 字节」「分块」三种声明调用 `BodyPlan::of_`，方法都取
    ///   `View`（一个允许带体的方法）。
    /// - 判断：三次判定依次为 `Absent` / `Sized { size_: 12 }` / `Chunked`——声明与边界一一
    ///   对应，没有互相串味。
    ///
    /// - Means: call `BodyPlan::of_` with the absent / sized-12 / chunked declarations under
    ///   the `View` method, which may carry a body.
    /// - Verdict: the plans are `Absent`, `Sized { size_: 12 }` and `Chunked`, one to one with
    ///   the declarations.
    #[test]
    fn body_plan_maps_declarations_() {
        let method = Option::Some(AccessMethod::View);

        assert_eq!(
            BodyPlan::of_(BodySize::NilBody, method).expect("没有体应当判成 Absent"),
            BodyPlan::Absent
        );
        assert_eq!(
            BodyPlan::of_(BodySize::FixedLength(NumOrStr::Str("12")), method)
                .expect("文本形态的定长 12 应当判成 Sized"),
            BodyPlan::Sized { size_: 12usize }
        );
        assert_eq!(
            BodyPlan::of_(BodySize::FixedLength(NumOrStr::Num(12)), method)
                .expect("数字形态的定长 12 应当判成 Sized"),
            BodyPlan::Sized { size_: 12usize }
        );
        assert_eq!(
            BodyPlan::of_(BodySize::ChunkedData, method).expect("分块应当判成 Chunked"),
            BodyPlan::Chunked
        );
    }

    /// 测试 `Head` / `Drop` 的回复声明了体时按协议违规当场失败。
    /// Test that a declared body on a `Head` / `Drop` response fails as a protocol violation.
    ///
    /// - 手段：对 `Head` 与 `Drop` 两个方法分别给出「定长 8」与「分块」两种带体声明，调用
    ///   `BodyPlan::of_`。
    /// - 判断：六次调用全部返回 `Err`，且错误是 `ClientError::RespErr`（协议违规），**不是**
    ///   任何一种 `Ok` 的边界——宁可当场失败，也不带着错位的流继续读。
    ///
    /// - Means: with the `Head` and `Drop` methods, call `BodyPlan::of_` on both sized forms
    ///   and a chunked declaration.
    /// - Verdict: all six calls return `Err(ClientError::RespErr(..))`, never an `Ok` plan —
    ///   failing early beats reading a stream that is already out of step.
    #[test]
    fn body_plan_rejects_body_on_head_and_drop_() {
        for method in [AccessMethod::Head, AccessMethod::Drop] {
            for info in [
                BodySize::FixedLength(NumOrStr::Str("8")),
                BodySize::FixedLength(NumOrStr::Num(8)),
                BodySize::ChunkedData,
            ] {
                let res = BodyPlan::of_(info, Option::Some(method));
                assert!(
                    matches!(res, Result::Err(ClientError::RespErr(_))),
                    "{method:?} 的回复带体（{info:?}）应当报协议违规，实际是 {res:?}"
                );
            }
        }
    }

    /// 测试定长声明不是合法长度时当场失败，而不是把非法值当成长度 0。
    /// Test that a malformed `Body_Size` fails instead of silently becoming zero bytes.
    ///
    /// - 手段：给出 `FixedLength(Str("12x"))` 与 `FixedLength(Str(""))` 两个非法声明，方法是 `View`。
    /// - 判断：两次都返回 `Err(ClientError::RespErr)`；若实现把解析失败吞成 `0`，体就会一个
    ///   字节都不读，这里因此专门断言错误而不是边界值。
    ///
    /// - Means: feed the malformed declarations `FixedLength(Str("12x"))` and
    ///   `FixedLength(Str(""))` under the `View` method.
    /// - Verdict: both return `Err(ClientError::RespErr)`; a swallowed parse error would mean
    ///   "read zero bytes", which is exactly what this test rules out.
    #[test]
    fn body_plan_rejects_malformed_size_() {
        for text in ["12x", ""] {
            let res = BodyPlan::of_(
                BodySize::FixedLength(NumOrStr::Str(text)),
                Option::Some(AccessMethod::View),
            );
            assert!(
                matches!(res, Result::Err(ClientError::RespErr(_))),
                "非法长度声明 {text:?} 应当报错，实际是 {res:?}"
            );
        }
    }

    /// 测试「没有体」在状态机里也是一个**明确的**边界，而不是「不知道」。
    /// Test that an absent body is an explicit boundary rather than an unknown one.
    ///
    /// - 手段：以 `NilBody` 判定一次，并与 `Chunked` 的判定做比较。
    /// - 判断：`Absent` 与 `Chunked` 不相等，且 `Absent` 不是 `Sized`——三种边界互不混淆，
    ///   续流阶段的「要不要代读丢弃」因此不会把「没有体」误当成「体还没读」。
    ///
    /// - Means: decide once with `NilBody` and compare it against the chunked decision.
    /// - Verdict: `Absent` differs from `Chunked` and is not a `Sized`, so the suffix stage can
    ///   never mistake "no body" for "body not read yet".
    #[test]
    fn absent_body_is_its_own_boundary_() {
        let method = Option::Some(AccessMethod::View);
        let absent = BodyPlan::of_(BodySize::NilBody, method).expect("没有体应当判成 Absent");
        let chunked = BodyPlan::of_(BodySize::ChunkedData, method).expect("分块应当判成 Chunked");

        assert_ne!(absent, chunked);
        assert!(!matches!(absent, BodyPlan::Sized { .. }));
    }
}
