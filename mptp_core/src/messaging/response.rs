use std::io;

use abs_buff::{TrBuffRead, TrBuffWrite, buffer::TrProducerState, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::{
    basic::{BodyEncodeError, TrRpcBody},
    body::{BodyTransfer, body_transfer_of, send_body_async},
    io_::{CountingWrite, MessageIoError, WaitingTx, decode_from_async_, read_body_async_},
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
///
/// 请求方向的同类判定由 [`body_transfer_of`](super::body::body_transfer_of) 给出，
/// 这里多出来的是**回复侧才有**的那条：某些请求方法的回复按协议不带本体内容。
#[derive(Clone, Debug, Error)]
pub enum ProtocolViolation {
    /// `Head` / `Drop` 按协议不带本体内容，服务端却在回复头里声明了报文体。
    ///
    /// `declared` 是 `Body_Size` 声明的长度；分块体没有总长，因此是 `None`。
    #[error("协议违规：{method:?} 的回复不应带报文体，但回复头声明了体（长度 {declared:?}）")]
    BodyNotAllowed {
        method: AccessMethod,
        declared: Option<usize>,
    },

    /// 回复头声明了 `Body_Type`，却既没有 `Body_Size` 也没有分块声明：无法确定回复体的边界。
    #[error("协议违规：回复头声明了 Body_Type 却没有给出体边界（既无 Body_Size 也无分块声明）")]
    MissingBodySize,

    /// `Body_Size` 与 `Body_Transfer: Chunked` 同时在场：两种边界声明互斥，无法判定该按哪种读。
    #[error("协议违规：Body_Size（{declared} 字节）与分块声明同时在场，体边界无从判定")]
    ConflictingTransfer { declared: usize },

    /// `Body_Transfer` 的取值不是已知的传输方式。
    #[error("协议违规：无法解读的体传输方式（{0}）")]
    UnknownBodyTransfer(String),

    /// `Body_Size` 头不是合法的长度。
    #[error("协议违规：Body_Size 头不是合法的长度（{0}）")]
    MalformedBodySize(String),
}

/// 客户端在读完回复前缀之后，该拿报文体怎么办。
///
/// 这是「回复体边界」的唯一判据：边界一旦确定，读多少字节、按哪种方式读也就定了，
/// 调用方不必自己去看头。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseBodyDecision {
    /// 没有报文体：**一个字节都不要读**。
    Absent,

    /// 定长报文体：恰好这么多字节。
    Present(usize),

    /// 分块报文体：总长未知，读到终止块为止。
    Chunked,
}

impl ResponseBodyDecision {
    /// 依据请求方法与回复前缀做出决策。
    ///
    /// 这是 MPTP 里「回复体边界」的唯一判据，取代了历史上那个只回答「要不要读」的
    /// `should_read_response_body`。
    ///
    /// 判定顺序（与 HTTP 的成熟约定同构）：
    ///
    /// 1. 请求方法是 `Head` / `Drop`——方法本身就否决体，无论头里怎么声明都属违规；
    /// 2. 否则按 [`body_transfer_of`](super::body::body_transfer_of) 判定：两个体声明头
    ///    都不在场（且没有 `Body_Type`）是没有体，`Body_Size` 为 0 也是没有体，
    ///    有长度是定长，只有分块声明是分块；
    /// 3. 头里的两种边界声明同时在场、或只有 `Body_Type` 而没有任何边界信息，都是协议
    ///    违规——宁可不读，也不能按猜出来的长度切分后续字节。
    ///
    /// # Errors
    ///
    /// 命中上述第 1、3 条时返回 [`ProtocolViolation`]。
    pub fn decide(method: AccessMethod, prefix: &RespPrefix) -> Result<Self, ProtocolViolation> {
        let transfer = body_transfer_of(prefix.headers())?;
        match transfer {
            BodyTransfer::Absent => Result::Ok(ResponseBodyDecision::Absent),
            BodyTransfer::Sized(size) => match method {
                // 这两个方法按协议不带本体内容；服务端仍然声明了体，说明两端对协议的
                // 理解已经不一致——继续按自己的理解读下去只会越错越远。
                AccessMethod::Head | AccessMethod::Drop => {
                    Result::Err(ProtocolViolation::BodyNotAllowed {
                        method,
                        declared: Option::Some(size),
                    })
                }
                AccessMethod::View
                | AccessMethod::Post
                | AccessMethod::Push
                | AccessMethod::Pull
                | AccessMethod::Call => Result::Ok(ResponseBodyDecision::Present(size)),
            },
            BodyTransfer::Chunked => match method {
                AccessMethod::Head | AccessMethod::Drop => {
                    Result::Err(ProtocolViolation::BodyNotAllowed {
                        method,
                        declared: Option::None,
                    })
                }
                AccessMethod::View
                | AccessMethod::Post
                | AccessMethod::Push
                | AccessMethod::Pull
                | AccessMethod::Call => Result::Ok(ResponseBodyDecision::Chunked),
            },
        }
    }

    /// 这次回复需要读多少字节；没有体是 `Some(0)`，分块体没有确定的总长故为 `None`。
    pub const fn body_size(&self) -> Option<usize> {
        match self {
            ResponseBodyDecision::Absent => Option::Some(0usize),
            ResponseBodyDecision::Present(size) => Option::Some(*size),
            ResponseBodyDecision::Chunked => Option::None,
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

/// 写出一条完整的回复：**先写前缀，再按头里声明的模式搬体**。
///
/// 与请求方向同一条纪律：前缀先走（腾出 ring 的空间），体按 `Body_Size` 或分块声明搬运，
/// 长度不必事先知道。
///
/// # Errors
///
/// 头部体声明违规、体的字节借不出、体不足声明长度、编码失败，或写 ring 失败时返回错误。
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
    let written = send_response_prefix_async(resp, tx, tok.child_token()).await?;

    // 体按头里声明的模式写出去：编码就发生在这一步，而不是构造回复的那一刻。
    let body_view = RespBodyView { resp_: resp };
    let body = send_body_async(&body_view, tx, resp.headers(), tok).await?;
    Result::Ok(written + body)
}

/// 把「一条回复的体」适配成 [`TrRpcBody`]，理由同
/// [`ReqBodyView`](super::request::send_request_async)。
struct RespBodyView<'a, TyResp> {
    resp_: &'a TyResp,
}

impl<TyResp> TrRpcBody for RespBodyView<'_, TyResp>
where
    TyResp: messaging::TrRpcResponse,
{
    #[inline]
    fn has_body(&self) -> bool {
        self.resp_.has_body()
    }

    #[inline]
    fn try_known_len(&self) -> Result<Option<usize>, BodyEncodeError> {
        self.resp_.try_body_known_len()
    }

    #[inline]
    fn try_encode_into(&self, sink: &mut dyn io::Write) -> Result<usize, BodyEncodeError> {
        Ok(self.resp_.try_write_body(sink)?.unwrap_or(0usize))
    }
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

/// 按回复头把回复体解成一个业务类型。语义同
/// [`recv_request_body_async`](super::request::recv_request_body_async)：边界由协议头
/// 决定（定长按 `Body_Size`、分块按块自带的长度），没有体时一个字节都不读，解码直接
/// 发生在 ring 的接收半边上。
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
