use abs_buff::{TrBuffRead, TrBuffWrite, buffer::TrProducerState, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::io_::{
    CountingWrite, MessageIoError, WaitingTx, check_body_size_, decode_from_async_,
    read_body_async_,
};
use crate::{
    access_method::AccessMethod,
    messaging,
    specs::{HeaderVal, Headers, Status, StdHeaderKey},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// RespPrefix
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回复前缀的线上形态：`status + headers`。
///
/// 与请求前缀一样，它是解码过程中的中间态：一条回复的边界是「前缀 + 由 `Body_Size`
/// 声明的体」，前缀自己说明不了全部。
#[derive(Clone, Debug)]
pub struct RespPrefix(pub Status, pub Option<Headers>);

impl RespPrefix {
    /// 回复状态码。
    pub const fn status(&self) -> Status {
        self.0
    }

    /// 回复头。
    pub const fn headers(&self) -> Option<&Headers> {
        self.1.as_ref()
    }

    /// 回复头里 `Body_Size` 声明的长度（字节）；没有该头时为 `None`。
    pub fn try_get_body_size(&self) -> Option<usize> {
        let headers = self.1.as_ref()?;
        let val = headers.try_get_header(&StdHeaderKey::Body_Size.into())?;
        match val.try_as_header_val() {
            Result::Ok(num) => Option::Some(num.into_inner() as usize),
            Result::Err(text) => text.parse::<usize>().ok(),
        }
    }

    /// 回复头里 `Body_Type` 声明的类型；没有该头时为 `None`。
    pub fn try_get_body_type(&self) -> Option<&HeaderVal> {
        self.1
            .as_ref()?
            .try_get_header(&StdHeaderKey::Body_Type.into())
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 回复体的读取决策
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回复头自相矛盾、或请求方法与回复头冲突时的协议违规。
#[derive(Debug, Error)]
pub enum ProtocolViolation {
    /// `Head` / `Drop` 按协议不带本体内容，服务端却在回复头里声明了报文体。
    #[error("协议违规：{method:?} 的回复不应带报文体，但回复头声明了 {declared} 字节")]
    BodyNotAllowed {
        method: AccessMethod,
        declared: usize,
    },

    /// 回复头声明了 `Body_Type` 却没有 `Body_Size`：无法确定回复体的边界。
    #[error("协议违规：回复头声明了 Body_Type 却没有 Body_Size，无法确定回复体边界")]
    MissingBodySize,
}

/// 客户端在读完回复前缀之后，该拿报文体怎么办。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseBodyDecision {
    /// 没有报文体：**一个字节都不要读**。
    Absent,

    /// 有报文体：恰好这么多字节。
    Present(usize),
}

impl ResponseBodyDecision {
    /// 依据请求方法与回复前缀做出决策。
    ///
    /// 这是 MPTP 里「回复体边界」的唯一判据，取代了历史上那个只回答「要不要读」的
    /// `should_read_response_body`：边界一旦确定，读多少字节也就定了，调用方不必再
    /// 自己去看 `Body_Size`。
    ///
    /// 判定顺序：
    ///
    /// 1. 两个头都没有 → [`ResponseBodyDecision::Absent`]；
    /// 2. 有 `Body_Type` 但没有 `Body_Size` → 无法确定边界，
    ///    [`ProtocolViolation::MissingBodySize`]；
    /// 3. 请求方法是 `Head` / `Drop` 却声明了体 →
    ///    [`ProtocolViolation::BodyNotAllowed`]；
    /// 4. 其余情况 → `Present(size)`（`size` 可以是 0，那就等价于没有体）。
    ///
    /// # Errors
    ///
    /// 命中上述第 2、3 条时返回 [`ProtocolViolation`]。
    pub fn decide(method: AccessMethod, prefix: &RespPrefix) -> Result<Self, ProtocolViolation> {
        let size = prefix.try_get_body_size();
        let has_type = prefix.try_get_body_type().is_some();
        match (size, has_type) {
            // 什么都没声明：这条回复就到前缀为止。
            (Option::None, false) => Result::Ok(ResponseBodyDecision::Absent),
            // 只有类型没有长度：边界无从谈起。
            (Option::None, true) => Result::Err(ProtocolViolation::MissingBodySize),
            (Option::Some(0usize), _) => Result::Ok(ResponseBodyDecision::Absent),
            (Option::Some(size), _) => match method {
                // 这两个方法按协议不带本体内容；服务端仍然声明了体，说明两端对协议的
                // 理解已经不一致——继续按自己的理解读下去只会越错越远。
                AccessMethod::Head | AccessMethod::Drop => {
                    Result::Err(ProtocolViolation::BodyNotAllowed {
                        method,
                        declared: size,
                    })
                }
                AccessMethod::View
                | AccessMethod::Post
                | AccessMethod::Push
                | AccessMethod::Pull
                | AccessMethod::Call => Result::Ok(ResponseBodyDecision::Present(size)),
            },
        }
    }

    /// 这次回复需要读多少字节；没有体就是 0。
    pub const fn body_size(&self) -> usize {
        match self {
            ResponseBodyDecision::Absent => 0usize,
            ResponseBodyDecision::Present(size) => *size,
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// io utils when sending and receiving response from IO.
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写出回复前缀（`status` / `headers`），返回写出的字节数。
///
/// 两个值**依次编进同一个写口**：线上格式就是「两个连续的 MessagePack 值」。字节直接
/// 落进 ring 的可用段，中间没有中转缓冲。
///
/// # Errors
///
/// 编码失败或写 ring 失败时返回错误。
pub(crate) async fn send_response_prefix_async<'f, TyResp, TyTx, TyTok>(
    resp: &'f TyResp,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyResp: messaging::TrRpcResponse,
    TyTx: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let mut waiting = WaitingTx::new_(tx);
    let mut write = CountingWrite::new_(AsStdWrite::new(&mut waiting, tok));
    {
        let mut serializer = rmp_serde::Serializer::new(&mut write);
        resp.status()
            .serialize(&mut serializer)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?;
        resp.headers()
            .serialize(&mut serializer)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    }
    Ok(write.written_())
}

/// 写出一条完整的回复：前缀 + 报文体。
///
/// 与请求方向同一条纪律：写出之前核对 `Body_Size` 头与体的实际长度，不一致就失败——
/// 那样的报文会让客户端按错误的长度切分后续字节。
///
/// # Errors
///
/// 编码失败、`Body_Size` 与体长度不符，或写 ring 失败时返回错误。
pub(crate) async fn send_response_async<'f, TyResp, TyTx, TyTok>(
    resp: &'f TyResp,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyResp: messaging::TrRpcResponse,
    TyTx: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let body_len = resp
        .try_body_len()
        .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    check_body_size_(resp.headers(), body_len)?;

    let written = send_response_prefix_async(resp, tx, tok.child_token()).await?;

    let mut waiting = WaitingTx::new_(tx);
    let mut write = CountingWrite::new_(AsStdWrite::new(&mut waiting, tok));
    let declared = resp
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

/// 解码回复前缀（`status` / `headers`）。
///
/// 直接从 ring 的接收半边解出来：`rmp-serde` 要多少字节读多少字节，因此**不会**碰到
/// 回复体的第一个字节。
///
/// # Errors
///
/// 报文不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub(crate) async fn recv_response_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    tok: TyTok,
) -> Result<RespPrefix, MessageIoError>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let status = decode_from_async_::<Status, _, _>(rx, tok.child_token()).await?;
    let headers = decode_from_async_::<Option<Headers>, _, _>(rx, tok.child_token()).await?;
    Ok(RespPrefix(status, headers))
}

/// 按 `Body_Size` 头把回复体解成一个业务类型。语义同
/// [`recv_request_body_async`](super::request::recv_request_body_async)：边界由协议头
/// 决定，没有体时一个字节都不读，解码直接发生在 ring 的接收半边上。
///
/// # Errors
///
/// 头值不合法、体不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub async fn recv_response_body_async<T, TyRx, TyTok>(
    rx: &mut TyRx,
    prefix: &RespPrefix,
    tok: TyTok,
) -> Result<Option<T>, MessageIoError>
where
    T: DeserializeOwned,
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    read_body_async_(rx, prefix.headers(), tok).await
}
