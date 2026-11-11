//! 七种 AccessMethod 的标准客户端入口：方法糖 + 统一 `RequestBuilder` / `Response`。
//!
//! 方法糖只提供**可发现的入口**与默认语义；实际有无体仍由 [`Body_Size`] /
//! [`Body_Transfer`] 两个标准头唯一决定。需要完全自定义前缀时用
//! [`MptpClient::request_prefix`](super::MptpClient::request_prefix)，或退回
//! [`MptpClient::request_async`](super::MptpClient::request_async)。
//!
//! # `method_`: the standard client entries for the seven methods
//!
//! The sugar only provides discoverable entry points and defaults; the actual body
//! still follows the `Body_Size` / `Body_Transfer` headers. `request_prefix` and the
//! original `request_async` remain the escape hatches.

use alloc::string::ToString;

use core::{
    future::{Future, Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{
    client::{SessionState, TrMptpSession},
    spec::StdHeaderKey,
};
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::{
    HeaderKey, HeaderVal, RequestPrefix, ResponsePrefix,
};

use crate::{
    error_::ClientError,
    x_deps::{abs_buff, abs_cancel, gen_mcf2},
};
use super::{
    client_::MptpClient,
    config::{ChannelRx, Dock, TrConfig},
    session::{
        MptpSession, RespBodyRead, RespSuffixRead, SendRequestBodyAsync,
        SendRequestSuffixAsync,
    },
};

/// 没有请求体。
pub struct NoBody;

/// 已经序列化好的定长请求体。
pub struct BytesBody<'a> {
    /// 体字节。
    bytes_: &'a [u8],
}

/// 定长请求体源。
pub struct SizedBody<'a, R> {
    /// 体源。
    src_: &'a mut R,

    /// 声明长度。
    size_: usize,
}

/// 分块请求体源。
pub struct ChunkedBody<'a, R> {
    /// 体源。
    src_: &'a mut R,
}

/// 没有请求后缀流。
pub struct NoSuffix;

/// 请求后缀流源。
pub struct Suffix<'a, R> {
    /// 后缀源。
    src_: &'a mut R,
}

/// 请求体源如何交给会话。
pub trait TrBodySend<C>
where
    C: TrConfig,
{
    /// 发送 future 的类型。
    type SendAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<SessionState, ClientError>>
    where
        Self: 'f,
        C: 'f;

    /// 把体写进会话。
    fn send_body_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f>;
}

/// 请求后缀源如何交给会话。
pub trait TrSuffixSend<C>
where
    C: TrConfig,
{
    /// 发送 future 的类型。
    type SendAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<SessionState, ClientError>>
    where
        Self: 'f,
        C: 'f;

    /// 把后缀写进会话。
    fn send_suffix_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f>;
}

/// 一个立即就绪、且忽略取消令牌的可取消 future。
pub struct ReadyMayCancel<T> {
    inner_: Ready<T>,
}

impl<T> ReadyMayCancel<T> {
    /// 用 `value` 造一个立即就绪的 future。
    pub fn new(value: T) -> Self {
        ReadyMayCancel { inner_: ready(value) }
    }
}

impl<T> Future for ReadyMayCancel<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner_).poll(cx)
    }
}

impl<'a, T: 'a> TrMayCancel<'a> for ReadyMayCancel<T> {
    type MayCancelFuture<'f, C>
        = Self
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = T;

    fn may_cancel_with<C>(self, _cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        self
    }
}

impl<C> TrBodySend<C> for NoBody
where
    C: TrConfig,
{
    type SendAsync<'f>
        = ReadyMayCancel<Result<SessionState, ClientError>>
    where
        Self: 'f,
        C: 'f;

    fn send_body_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f> {
        ReadyMayCancel::new(Result::Ok(session.state()))
    }
}

impl<'a, C> TrBodySend<C> for BytesBody<'a>
where
    C: TrConfig,
    &'a [u8]: TrBuffRead<u8> + TrConsumerState,
{
    type SendAsync<'f>
        = SendRequestBodyAsync<'f, 'f, C, &'a [u8]>
    where
        Self: 'f,
        C: 'f;

    fn send_body_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f> {
        session.send_req_body_async(&mut self.bytes_)
    }
}

impl<C, R> TrBodySend<C> for SizedBody<'_, R>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
{
    type SendAsync<'f>
        = SendRequestBodyAsync<'f, 'f, C, R>
    where
        Self: 'f,
        C: 'f;

    fn send_body_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f> {
        let _ = self.size_;
        session.send_req_body_async(&mut *self.src_)
    }
}

impl<C, R> TrBodySend<C> for ChunkedBody<'_, R>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
{
    type SendAsync<'f>
        = SendRequestBodyAsync<'f, 'f, C, R>
    where
        Self: 'f,
        C: 'f;

    fn send_body_async<'f>(&'f mut self, session: &'f mut MptpSession<C>) -> Self::SendAsync<'f> {
        session.send_req_body_async(&mut *self.src_)
    }
}

impl<C> TrSuffixSend<C> for NoSuffix
where
    C: TrConfig,
{
    type SendAsync<'f>
        = ReadyMayCancel<Result<SessionState, ClientError>>
    where
        Self: 'f,
        C: 'f;

    fn send_suffix_async<'f>(
        &'f mut self,
        session: &'f mut MptpSession<C>,
    ) -> Self::SendAsync<'f> {
        ReadyMayCancel::new(Result::Ok(session.state()))
    }
}

impl<C, R> TrSuffixSend<C> for Suffix<'_, R>
where
    C: TrConfig,
    R: TrBuffRead<u8> + TrConsumerState,
{
    type SendAsync<'f>
        = SendRequestSuffixAsync<'f, 'f, C, R>
    where
        Self: 'f,
        C: 'f;

    fn send_suffix_async<'f>(
        &'f mut self,
        session: &'f mut MptpSession<C>,
    ) -> Self::SendAsync<'f> {
        session.send_req_suffix_async(&mut *self.src_)
    }
}

/// 客户端请求 builder。
pub struct RequestBuilder<'a, C, B = NoBody, S = NoSuffix>
where
    C: TrConfig,
{
    /// 发起请求的客户端。
    client_: &'a MptpClient<C>,

    /// 请求前缀。
    prefix_: RequestPrefix,

    /// 目标垛口；缺省取配置。
    dock_: Option<Dock<C>>,

    /// 请求体源。
    body_: B,

    /// 请求后缀源。
    suffix_: S,
}

/// 把前缀写成「定长体」声明；`size == 0` 时清掉两个体头（等价于没有体）。
fn set_sized_header_(prefix: &mut RequestPrefix, size: usize) {
    prefix.headers_mut().remove(StdHeaderKey::Body_Transfer);
    if size == 0usize {
        prefix.headers_mut().remove(StdHeaderKey::Body_Size);
    } else {
        prefix
            .headers_mut()
            .set_text(StdHeaderKey::Body_Size, size.to_string());
    }
}

/// 把前缀写成分块体声明。
fn set_chunked_header_(prefix: &mut RequestPrefix) {
    prefix.headers_mut().remove(StdHeaderKey::Body_Size);
    prefix.headers_mut().set_num(
        StdHeaderKey::Body_Transfer,
        abs_mptp::spec::StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );
}

impl<'a, C> RequestBuilder<'a, C, NoBody, NoSuffix>
where
    C: TrConfig,
{
    /// 由「客户端 + 前缀」构造。
    pub(crate) fn new_(client: &'a MptpClient<C>, prefix: RequestPrefix) -> Self {
        RequestBuilder {
            client_: client,
            prefix_: prefix,
            dock_: Option::None,
            body_: NoBody,
            suffix_: NoSuffix,
        }
    }
}

impl<'a, C, B, S> RequestBuilder<'a, C, B, S>
where
    C: TrConfig,
{
    /// 覆盖目标垛口。
    pub fn dock(mut self, dock: Dock<C>) -> Self {
        self.dock_ = Option::Some(dock);
        self
    }

    /// 追加 / 覆盖一个请求头。
    pub fn header(mut self, key: impl Into<HeaderKey>, val: HeaderVal) -> Self {
        self.prefix_.headers_mut().set(key, val);
        self
    }

    /// 定长请求体：自动写 `Body_Size`。
    pub fn body_bytes(self, bytes: &'a [u8]) -> RequestBuilder<'a, C, BytesBody<'a>, S> {
        let size = bytes.len();
        let mut next = RequestBuilder {
            client_: self.client_,
            prefix_: self.prefix_,
            dock_: self.dock_,
            body_: BytesBody { bytes_: bytes },
            suffix_: self.suffix_,
        };
        set_sized_header_(&mut next.prefix_, size);
        next
    }

    /// 定长请求体：自动写 `Body_Size`。
    pub fn body_sized<R>(
        self,
        src: &'a mut R,
        size: usize,
    ) -> RequestBuilder<'a, C, SizedBody<'a, R>, S>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        let mut next = RequestBuilder {
            client_: self.client_,
            prefix_: self.prefix_,
            dock_: self.dock_,
            body_: SizedBody { src_: src, size_: size },
            suffix_: self.suffix_,
        };
        set_sized_header_(&mut next.prefix_, size);
        next
    }

    /// 分块请求体：自动写 `Body_Transfer: Chunked`。
    pub fn body_chunked<R>(
        self,
        src: &'a mut R,
    ) -> RequestBuilder<'a, C, ChunkedBody<'a, R>, S>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        let mut next = RequestBuilder {
            client_: self.client_,
            prefix_: self.prefix_,
            dock_: self.dock_,
            body_: ChunkedBody { src_: src },
            suffix_: self.suffix_,
        };
        set_chunked_header_(&mut next.prefix_);
        next
    }

    /// 请求后缀流（`Push` / `Call`）。
    pub fn suffix<R>(self, src: &'a mut R) -> RequestBuilder<'a, C, B, Suffix<'a, R>>
    where
        R: TrBuffRead<u8> + TrConsumerState,
    {
        RequestBuilder {
            client_: self.client_,
            prefix_: self.prefix_,
            dock_: self.dock_,
            body_: self.body_,
            suffix_: Suffix { src_: src },
        }
    }

    /// 发送请求并读回复前缀。
    pub fn send_async(self) -> SendRequestAsync<'a, 'a, C, B, S>
    where
        C: 'a,
        B: TrBodySend<C> + 'a,
        S: TrSuffixSend<C> + 'a,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        SendRequestAsync::new(self)
    }
}

/// [`RequestBuilder::send_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendRequest, pub, new(pub(crate)))]
async fn send_request_async_<'f, C, B, S, K>(
    builder: RequestBuilder<'f, C, B, S>,
    cancel: K,
) -> Result<Response<C>, ClientError>
where
    C: TrConfig,
    B: TrBodySend<C> + 'f,
    S: TrSuffixSend<C> + 'f,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let RequestBuilder {
        client_,
        prefix_,
        dock_,
        mut body_,
        mut suffix_,
    } = builder;

    let dock = dock_.unwrap_or_else(C::default_serving_dock);
    let mut session = client_
        .request_to_dock_async(dock, prefix_)
        .may_cancel_with(cancel.child_token())
        .await?;

    // 前缀没有声明体时（例如 `.body_bytes(&[])` 把两个体头都清掉），状态已经是
    // `WaitingResp`；此时不能再调用 `send_req_body_async`，否则会撞上状态检查。
    if session.state() == SessionState::SendingBody {
        let _ = body_
            .send_body_async(&mut session)
            .may_cancel_with(cancel.child_token())
            .await?;
    }
    let _ = suffix_
        .send_suffix_async(&mut session)
        .may_cancel_with(cancel.child_token())
        .await?;
    session
        .close_req_async()
        .may_cancel_with(cancel.child_token())
        .await?;

    let prefix = session
        .recv_resp_prefix_async()
        .may_cancel_with(cancel.child_token())
        .await?;

    Result::Ok(Response {
        session_: session,
        prefix_: prefix,
    })
}

/// 一次请求的回复。
pub struct Response<C>
where
    C: TrConfig,
{
    /// 会话；回复体 / 后缀流从它读。
    session_: MptpSession<C>,

    /// 回复前缀。
    prefix_: ResponsePrefix,
}

impl<C> Response<C>
where
    C: TrConfig,
{
    /// 状态码。
    pub fn status(&self) -> abs_mptp::spec::Status {
        self.prefix_.status()
    }

    /// 回复头表。
    pub fn headers(&self) -> Option<&mptp_wire::Headers> {
        self.prefix_.headers()
    }

    /// 回复前缀。
    pub fn prefix(&self) -> &ResponsePrefix {
        &self.prefix_
    }

    /// 取回复体的读视图。
    pub fn body_async<'f>(&'f mut self) -> ResponseBodyAsync<'f, 'f, C>
    where
        C: 'f,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseBodyAsync::new(self)
    }

    /// 取回复后缀流的读视图。
    pub fn suffix_async<'f>(&'f mut self) -> ResponseSuffixAsync<'f, 'f, C>
    where
        C: 'f,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        ResponseSuffixAsync::new(self)
    }

    /// 拿回会话，自己按旧原语驱动。
    pub fn into_session(self) -> MptpSession<C> {
        self.session_
    }
}

/// [`Response::body_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseBody, pub, new(pub(crate)))]
async fn response_body_async_<'f, C, K>(
    response: &'f mut Response<C>,
    cancel: K,
) -> Result<RespBodyRead<'f, C>, ClientError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    response
        .session_
        .recv_resp_body_async()
        .may_cancel_with(cancel.child_token())
        .await
}

/// [`Response::suffix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ResponseSuffix, pub, new(pub(crate)))]
async fn response_suffix_async_<'f, C, K>(
    response: &'f mut Response<C>,
    cancel: K,
) -> Result<RespSuffixRead<'f, C>, ClientError>
where
    C: 'f + TrConfig,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    response
        .session_
        .recv_resp_suffix_async()
        .may_cancel_with(cancel.child_token())
        .await
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use abs_mptp::{
        messaging::{BodySize, TrMsgPrefix},
        spec::{NumOrStr, access_method::AccessMethod},
    };

    /// 测试七种方法默认都不声明请求体。
    /// Test that all seven methods start with no declared request body.
    ///
    /// - 手段：对七种方法各构造一个 `RequestPrefix`，直接查 `body_info`。
    /// - 判断：全部为 `NilBody`——方法糖只给入口，不偷偷写体头。
    ///
    /// - Means: build one `RequestPrefix` per method and inspect `body_info`.
    /// - Verdict: all are `NilBody`; the sugar adds no body header by itself.
    #[test]
    fn all_methods_default_to_no_body_() {
        for method in [
            AccessMethod::Head,
            AccessMethod::View,
            AccessMethod::Post,
            AccessMethod::Drop,
            AccessMethod::Push,
            AccessMethod::Pull,
            AccessMethod::Call,
        ] {
            let prefix = RequestPrefix::new(method, "/x");
            assert_eq!(prefix.body_info(), BodySize::NilBody, "{method:?}");
        }
    }

    /// 测试定长体 helper 写入 `Body_Size` 并清掉分块头。
    /// Test that the sized-body helper writes `Body_Size` and clears chunked.
    ///
    /// - 手段：先给前缀写分块头，再调用 `set_sized_header_(12)`。
    /// - 判断：`body_info` 为 `FixedLength(Num(12))`，不是分块。
    ///
    /// - Means: write the chunked header first, then call `set_sized_header_(12)`.
    /// - Verdict: `body_info` is `FixedLength(Num(12))`, not chunked.
    #[test]
    fn sized_body_helper_overrides_chunked_() {
        let mut prefix = RequestPrefix::new(AccessMethod::Post, "/x");
        set_chunked_header_(&mut prefix);
        set_sized_header_(&mut prefix, 12usize);
        assert_eq!(
            prefix.body_info(),
            BodySize::FixedLength(NumOrStr::Num(12usize))
        );
    }

    /// 测试分块体 helper 写入 `Body_Transfer` 并清掉定长头。
    /// Test that the chunked helper writes `Body_Transfer` and clears the sized header.
    ///
    /// - 手段：先给前缀写 `Body_Size`，再调用 `set_chunked_header_`。
    /// - 判断：`body_info` 为 `ChunkedData`。
    ///
    /// - Means: write `Body_Size` first, then call `set_chunked_header_`.
    /// - Verdict: `body_info` is `ChunkedData`.
    #[test]
    fn chunked_body_helper_overrides_sized_() {
        let mut prefix = RequestPrefix::new(AccessMethod::Call, "/x");
        set_sized_header_(&mut prefix, 8usize);
        set_chunked_header_(&mut prefix);
        assert_eq!(prefix.body_info(), BodySize::ChunkedData);
    }

    /// 测试零长度定长体归一成「没有体」。
    /// Test that a zero-sized body normalises to "no body".
    ///
    /// - 手段：对前缀调用 `set_sized_header_(0)`。
    /// - 判断：`body_info` 为 `NilBody`，两个体头都不在场。
    ///
    /// - Means: call `set_sized_header_(0)` on a prefix.
    /// - Verdict: `body_info` is `NilBody`; neither body header is present.
    #[test]
    fn zero_sized_body_is_no_body_() {
        let mut prefix = RequestPrefix::new(AccessMethod::Post, "/x");
        set_sized_header_(&mut prefix, 0usize);
        assert_eq!(prefix.body_info(), BodySize::NilBody);
    }
}
