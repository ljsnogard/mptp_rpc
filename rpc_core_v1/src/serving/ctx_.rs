//! 服务端一次调用的上下文：解码请求、写回复、收尾。
//!
//! [`RpcCallCtx`] 按值持有一次 RPC job 的收发半边与请求前缀信息。它只做**与业务无关**
//! 的事：
//!
//! - 按请求头里的 `Body_Size` / `Body_Transfer` 流式解码请求体；
//! - 把业务返回值编码成回复体，写出 `200 Ok` 回复；
//! - 把业务错误按状态码写成错误回复；
//! - 在 worker 处理完后统一收尾。
//!
//! `#[rpc(service = "...")]` 生成的服务端 dispatch 只负责 `match` body enum 的方法
//! 变体并调用用户实现；解码、编码、收尾都在这里。
//!
//! # `ctx_`: the per-call serving context
//!
//! [`RpcCallCtx`] owns the two channel halves of one RPC job. It decodes the request body,
//! writes success / error replies, and performs the final close. Macro-generated dispatch
//! only matches the body enum and calls the user's service implementation.

use core::mem::MaybeUninit;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mptp::{
    messaging::chunked::ChunkedEncode,
    spec::{Status, StdHeaderKey, StdHeaderVal},
};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    messaging::{Headers, ResponsePrefix, write_response_prefix_async},
    serving::{
        close_channel_async,
        config::{ChannelRx, ChannelTx},
    },
};

use crate::{
    messaging::body_::{
        BodyRing_, K_BODY_RING_CAP_,
        decode_body_from_async_,
        encode_into_async_,
        write_body_async_,
    },
    messaging::{RpcError, RpcResult},
    serving::TrServingConfig,
    x_deps::{abs_cancel, abs_mptp, gen_mcf2},
};

/// 一次 RPC 调用的服务端上下文。
///
/// 它按值持有本端子流的发送 / 接收半边，因此 worker 在 dispatch 结束后可以调用
/// [`RpcCallCtx::close_async`] 完成「写回复 → 半关闭 tx → 读完 rx → 释放」这条收尾链。
///
/// `'a` 是请求前缀信息（`location`、`headers`）的借用期；两个 channel 半边由本上下文
/// 拥有，不借用 worker 的栈。
pub struct RpcCallCtx<'a, C: TrServingConfig> {
    /// 请求路径，仅用于错误信息。
    location_: &'a str,

    /// 请求前缀里的头表。
    headers_: &'a Headers,

    /// 本子流的发送半边。
    tx_: ChannelTx<C>,

    /// 本子流的接收半边。
    rx_: ChannelRx<C>,
}

impl<'a, C: TrServingConfig> RpcCallCtx<'a, C> {
    /// 由请求信息与两个 channel 半边构造上下文。
    pub const fn new(
        location: &'a str,
        headers: &'a Headers,
        tx: ChannelTx<C>,
        rx: ChannelRx<C>,
    ) -> Self {
        RpcCallCtx {
            location_: location,
            headers_: headers,
            tx_: tx,
            rx_: rx,
        }
    }

    /// 请求路径。
    pub const fn location(&self) -> &'a str {
        self.location_
    }

    /// 请求前缀里的头表。
    pub const fn headers(&self) -> &'a Headers {
        self.headers_
    }

    /// 解码本次请求的 body enum。
    ///
    /// 支持 `Body_Size` 定长帧与 `Body_Transfer` 分块帧：解码器直接从限长 / 分块读视图
    /// 读取，不把整个请求体攒进 `Vec`。调用方拿到 future 后通过
    /// `.may_cancel_with(token)` 把取消令牌传进来。
    ///
    /// # Errors
    ///
    /// - [`RpcError::Transport`]：读 body 失败；
    /// - [`RpcError::Decode`]：MessagePack 解码失败。
    pub fn decode_call_async<'f, T>(
        &'f mut self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>> + 'f
    where
        T: TrDecodableOwned + 'f,
    {
        DecodeCallAsync::<'a, 'f, 'f, C, T>::new(self)
    }

    /// 写出一条 `200 Ok` 回复。
    ///
    /// `value` 按 MessagePack 直接编进 ring，回复按 `Body_Transfer: Chunked` 写出；
    /// 编码与分块搬运在同一个 task 内 `join` 推进，不预编码整个业务体。
    ///
    /// # Errors
    ///
    /// - [`RpcError::Encode`]：业务值编码失败；
    /// - [`RpcError::Transport`]：写回复前缀或 body 失败。
    pub fn reply_ok_async<'f, T>(
        &'f mut self,
        value: T,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
    where
        T: TrEncodable + 'f,
    {
        ReplyOkAsync::<'a, 'f, 'f, C, T>::new(self, value)
    }

    /// 写出一条非成功状态回复，`message` 原样作为 UTF-8 错误体。
    ///
    /// 错误体不声明 `Body_Type`：客户端只把它当作展示用的原始字节，不参与业务解码。
    ///
    /// # Errors
    ///
    /// - [`RpcError::Transport`]：写回复前缀或 body 失败。
    pub fn reply_status_async<'f>(
        &'f mut self,
        status: Status,
        message: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f {
        ReplyStatusAsync::<'a, 'f, 'f, C>::new(self, status, message)
    }

    /// 收尾一条已经处理完的子流：半关闭发送半边，再读到对端关闭。
    ///
    /// 返回被丢弃的字节数。与 `mptp_smux::serving::close_channel_async` 一样，收尾阶段
    /// 的读错误被静默忽略；这里直接复用那个普通 `async fn`，因为它是纯等待、不需要再
    /// 传播取消令牌。
    pub async fn close_async(self) -> usize {
        let RpcCallCtx {
            tx_: tx,
            rx_: mut rx,
            ..
        } = self;
        close_channel_async(tx, &mut rx).await
    }
}

/// 解码请求 body 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DecodeCall, pub(crate), new(pub(crate)))]
async fn decode_call_async_<'a, 'f, C, T, TyTok>(
    ctx: &'f mut RpcCallCtx<'a, C>,
    cancel: TyTok,
) -> RpcResult<T>
where
    'a: 'f,
    C: TrServingConfig,
    T: TrDecodableOwned + 'f,
    TyTok: TrCancellationToken,
{
    let size = ctx.headers_.body_size().unwrap_or(0usize);
    decode_body_from_async_(
        &mut ctx.rx_,
        ctx.headers_.is_chunked(),
        size,
        cancel.child_token(),
    )
    .await
}

/// 写成功回复的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReplyOk, pub(crate), new(pub(crate)))]
async fn reply_ok_async_<'a, 'f, C, T, TyTok>(
    ctx: &'f mut RpcCallCtx<'a, C>,
    value: T,
    cancel: TyTok,
) -> RpcResult<()>
where
    'a: 'f,
    C: TrServingConfig,
    T: TrEncodable + 'f,
    TyTok: TrCancellationToken,
{
    let location = ctx.location_;
    let mut headers = Headers::new();
    headers.set_num(
        StdHeaderKey::Body_Transfer,
        StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    let prefix = ResponsePrefix::with_headers(Status::Ok, Option::Some(headers));

    write_response_prefix_async(&prefix, &mut ctx.tx_)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("写 {location} 回复前缀失败：{err}")))?;

    // 与客户端请求体同一条策略：编码器写 ring，`ChunkedEncode` 从 ring 读半边搬进
    // 子流发送半边；ring 只做并发窗口，不限制业务体大小。
    let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
    let (mut writer, mut reader) = BodyRing_::split(&mut ring);
    let encode_cancel = cancel.child_token();
    let encode_fut = async move {
        let result = encode_into_async_(&value, &mut writer, encode_cancel).await;
        writer.close();
        result
    };
    let mut chunked = ChunkedEncode::new(&mut reader, &mut ctx.tx_);
    let send_fut = chunked
        .write_all_async()
        .may_cancel_with(cancel.child_token());
    let (encode_res, send_res) = futures::future::join(encode_fut, send_fut).await;
    encode_res?;
    send_res.map_err(|err| RpcError::Transport(format!("写 {location} 回复体失败：{err}")))?;
    Result::Ok(())
}

/// 写错误回复的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReplyStatus, pub(crate), new(pub(crate)))]
async fn reply_status_async_<'a, 'f, C, TyTok>(
    ctx: &'f mut RpcCallCtx<'a, C>,
    status: Status,
    message: String,
    cancel: TyTok,
) -> RpcResult<()>
where
    'a: 'f,
    C: TrServingConfig,
    TyTok: TrCancellationToken,
{
    let body = message.into_bytes();
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    let prefix = ResponsePrefix::with_headers(status, Option::Some(headers));

    write_response_prefix_async(&prefix, &mut ctx.tx_)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| {
            RpcError::Transport(format!("写 {} 错误回复前缀失败：{err}", ctx.location_))
        })?;
    write_body_async_(&mut ctx.tx_, &body, cancel.child_token()).await?;
    Result::Ok(())
}
