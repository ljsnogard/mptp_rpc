//! 报文体的传输：**模式判定**、**发送搬运**与**接收视图**。
//!
//! # 发送体有两条路，取决于内容是什么形状
//!
//! | 内容是什么 | 走哪个入口 | 怎么发 |
//! | --- | --- | --- |
//! | **字节流**（`&[u8]`、环的读半边、文件、上游转发来的字节） | [`send_body_from_reader_async`]（便利包装 [`send_content_async`]） | 从源借段、往目标借段、段到段搬；**不预量长度、不分配缓存** |
//! | **业务值 + 编码格式** | [`send_body_async`] | 编码器把字节逐段**直接编进目标半边**（外面按模式套写口），边编边发 |
//!
//! 两条路都遵守同一件事：**协议层不事先知道体有多长**。区别只在字节是谁产生的——
//! 已经存在（pull：去源里借）还是正在产生（push：往目标里写）。协议层不替调用方做格式
//! 选择，也不替它把值编成字节：那是「谁产生内容谁负责」的事。
//!
//! 常见的那条是第一条：`&[u8]` 本身就实现了 [`TrBuffRead<u8>`]，不需要任何中间类型。
//!
//! # 两种模式
//!
//! | 模式 | 头部声明 | 发送 | 接收 |
//! | --- | --- | --- | --- |
//! | 定长 | `Body_Size` | 只搬声明的那么多字节；源不足报截断，源有多余**不碰** | 限长视图：读到该长度即为流末 |
//! | 分块 | `Body_Transfer: Chunked` | 每让出一段就封一块（前缀 = 该段实际长度），末尾写长度 0 的块 | 分块视图：块边界对解码器透明，终止块即流末 |
//!
//! 判别顺序与 HTTP 的成熟约定同构：**先看方法/状态能否否决体，再看传输模式标记，最后
//! 才是长度**（回复侧的否决见
//! [`ResponseBodyDecision`](super::response::ResponseBodyDecision)）。
//!
//! # Examples
//!
//! ```
//! use mptp_core::{client::HeadersBuilder, messaging::body::{BodyTransfer, body_transfer_of}};
//!
//! // 没有体。
//! assert_eq!(
//!     body_transfer_of(Option::None).expect("不应当违规"),
//!     BodyTransfer::Absent
//! );
//!
//! // 声明了长度就是定长。
//! let headers = HeadersBuilder::new().set_body_size(1024usize).build();
//! assert_eq!(
//!     body_transfer_of(Option::Some(&headers)).expect("不应当违规"),
//!     BodyTransfer::Sized(1024usize)
//! );
//! ```

use core::error::Error;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryRead, TrBuffWrite, gen_may_cancel_future,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrProducerState},
    error::{ReadErrTag, TrTaggedError},
    x_deps::{abs_cancel, anylr},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::SomeOf;

use abs_buff_stdio_adapt::AsStdWrite;

use super::{
    basic::TrRpcBody,
    chunked::{ChunkedReadError, ChunkedSink, chunked_read_step_, chunked_try_read_},
    io_::{MessageIoError, WaitingTx, race_cancel_},
    limit::{
        LimitReadError, LimitedRead, LimitedRefSegm, LimitedWrite, limited_read_step_,
        limited_try_read_, split_some_of_,
    },
    response::ProtocolViolation,
};
use crate::specs::{HeaderVal, Headers, StdHeaderKey, StdHeaderVal};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 模式判定
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一条报文的体按哪种方式传输。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyTransfer {
    /// 没有体：一个字节都不收发。
    Absent,

    /// 定长：体的总长由 `Body_Size` 声明，恰好这么多字节。
    Sized(usize),

    /// 分块：总长不写在头里，由「2 字节长度前缀 + 内容」的块序列给出，以长度 `0` 的块结束。
    Chunked,
}

impl BodyTransfer {
    /// 按声明的长度构造：`0` 与「没有体」在协议上是同一件事。
    pub const fn from_size(size: usize) -> Self {
        if size == 0usize {
            return BodyTransfer::Absent;
        }
        BodyTransfer::Sized(size)
    }

    /// 本条报文是否有体（定长或分块都算有体）。
    pub const fn has_body(&self) -> bool {
        !matches!(self, BodyTransfer::Absent)
    }

    /// 定长模式下声明的长度；其余模式返回 `None`。
    pub const fn declared_size(&self) -> Option<usize> {
        match self {
            BodyTransfer::Sized(size) => Option::Some(*size),
            BodyTransfer::Absent | BodyTransfer::Chunked => Option::None,
        }
    }
}

/// 从头里读出 `Body_Size`（只认在场的那一个头）。
///
/// # Errors
///
/// 头值既不是数字也不是合法的十进制字符串时返回协议违规。
fn try_get_optional_size_(headers: Option<&Headers>) -> Result<Option<usize>, ProtocolViolation> {
    let Option::Some(headers) = headers else {
        return Ok(Option::None);
    };
    let Option::Some(val) = headers.try_get_header(&StdHeaderKey::Body_Size.into()) else {
        return Ok(Option::None);
    };
    match val.try_as_header_val() {
        Result::Ok(num) => Ok(Option::Some(usize::from(num.into_inner()))),
        Result::Err(text) => text.parse::<usize>().map(Option::Some).map_err(|err| {
            ProtocolViolation::MalformedBodySize(format!("Body_Size 头不是合法的长度（{text:?}）：{err}"))
        }),
    }
}

/// 头里是否声明了「分块传输」。
///
/// 两种形态都认：数字形态的标准值，以及等价的文本形态（`Chunked`，忽略大小写）。
/// 认不出的取值按协议违规报出，而不是当成「没有声明」——那会让接收方按错误的边界
/// 切分后续字节。
///
/// # Errors
///
/// `Body_Transfer` 的取值不是已知的传输方式时返回协议违规。
fn try_get_chunked_(headers: Option<&Headers>) -> Result<bool, ProtocolViolation> {
    let Option::Some(headers) = headers else {
        return Ok(false);
    };
    let Option::Some(val) = headers.try_get_header(&StdHeaderKey::Body_Transfer.into()) else {
        return Ok(false);
    };
    match val.try_as_header_val() {
        Result::Ok(num) if num == StdHeaderVal::Body_Transfer_Chunked => Ok(true),
        Result::Ok(num) => Err(ProtocolViolation::UnknownBodyTransfer(format!(
            "Body_Transfer 的取值 {} 不是已知的传输方式",
            num.into_inner()
        ))),
        Result::Err(text) if text.eq_ignore_ascii_case("chunked") => Ok(true),
        Result::Err(text) => Err(ProtocolViolation::UnknownBodyTransfer(format!(
            "Body_Transfer 的取值 {text:?} 不是已知的传输方式"
        ))),
    }
}

/// 一条报文的体按哪种方式传输；这是「体边界」的唯一判据。
///
/// 判定规则：
///
/// 1. `Body_Transfer: Chunked` 在场且 `Body_Size` 也在场 → [`ProtocolViolation::ConflictingTransfer`]；
/// 2. 只有 `Body_Transfer: Chunked` → [`BodyTransfer::Chunked`]；
/// 3. 有 `Body_Size`：为 `0` 等价于没有体，否则 → [`BodyTransfer::Sized`]；
/// 4. 两个头都不在场：声明了 `Body_Type` 说明对端以为自己发了体，却给不出边界 →
///    [`ProtocolViolation::MissingBodySize`]；否则 → [`BodyTransfer::Absent`]。
///
/// # Errors
///
/// 命中上述第 1、4 条，或某个头值本身不合法时返回 [`ProtocolViolation`]。
pub fn body_transfer_of(headers: Option<&Headers>) -> Result<BodyTransfer, ProtocolViolation> {
    let content_length = try_get_optional_size_(headers)?;
    let chunked = try_get_chunked_(headers)?;
    let has_type = headers
        .and_then(|headers| headers.try_get_header(&StdHeaderKey::Body_Type.into()))
        .is_some();

    match (chunked, content_length) {
        // 两种声明同时在场：边界无从判定，宁可失败也不猜。
        (true, Option::Some(declared)) => {
            Err(ProtocolViolation::ConflictingTransfer { declared })
        }
        (true, Option::None) => Ok(BodyTransfer::Chunked),
        (false, Option::Some(size)) => Ok(BodyTransfer::from_size(size)),
        (false, Option::None) => {
            if has_type {
                // 对端声明了类型却没有给出任何边界信息。
                return Err(ProtocolViolation::MissingBodySize);
            }
            Ok(BodyTransfer::Absent)
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 接收视图
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 报文体读取出错。
#[derive(Clone, Debug)]
pub enum BodyReadError<TyErr> {
    /// 本条报文没有体：读端到此为止（错误标签是 `Closing`，即 `AsStdRead` 眼里的 EOF）。
    Absent,

    /// 定长体读取错误。
    Sized(LimitReadError<TyErr>),

    /// 分块体读取错误。
    Chunked(ChunkedReadError<TyErr>),
}

impl<TyErr> core::fmt::Display for BodyReadError<TyErr>
where
    TyErr: core::fmt::Display,
{
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            BodyReadError::Absent => f.write_str("报文体：本条报文没有体"),
            BodyReadError::Sized(err) => err.fmt(f),
            BodyReadError::Chunked(err) => err.fmt(f),
        }
    }
}

// 不实现 `source()`：理由同 `LimitReadError`——避免 `'static` 约束沿 trait 传染。
impl<TyErr> Error for BodyReadError<TyErr> where TyErr: Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for BodyReadError<TyErr>
where
    TyErr: Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            BodyReadError::Absent => ReadErrTag::Closing,
            BodyReadError::Sized(err) => err.err_tag(),
            BodyReadError::Chunked(err) => err.err_tag(),
        }
    }
}

/// 一条报文的**体读视图**：按头里声明的模式解读底下的字节流。
///
/// 它自身就是一个 [`TrBuffRead`]，因此上层可以直接把它交给
/// `AsStdRead` / `rmp_serde` 解码，也可以再搬到别处去：
///
/// - 没有体时，第一次读就报「流结束」，**一个字节都不会碰**；
/// - 定长时，读到声明的长度即为流末，多写的字节留给同一条 channel 上的后续数据；
/// - 分块时，块边界对解码器完全透明，终止块即为流末。
///
/// # Examples
///
/// ```
/// use mptp_core::messaging::body::{BodyTransfer, BodyReader, body_reader};
///
/// # async fn demo() {
/// let mut wire: &[u8] = b"";
/// // 头里什么都没声明：视图是「没有体」。
/// let reader = body_reader(&mut wire, Option::None).expect("不应当违规");
/// assert_eq!(reader.transfer(), BodyTransfer::Absent);
/// # }
/// ```
pub struct BodyReader<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    inner_: &'a mut TyRx,

    /// 本条报文的传输模式。
    transfer_: BodyTransfer,

    /// 定长模式下还没读到的字节数；分块模式下是**当前块内**还没读到的字节数。
    left_: usize,

    /// 分块模式下是否已经读到终止块。
    ended_: bool,

    /// 本次借段交给底层的需求；理由见 `limit::LimitedRead` 的文档。
    demand_: Demand<usize>,
}

impl<'a, TyRx> BodyReader<'a, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    /// 用给定的传输模式包住一个读缓冲。
    pub const fn new(inner: &'a mut TyRx, transfer: BodyTransfer) -> Self {
        let (left, ended) = match transfer {
            BodyTransfer::Sized(size) => (size, false),
            BodyTransfer::Absent => (0usize, true),
            BodyTransfer::Chunked => (0usize, false),
        };
        BodyReader {
            inner_: inner,
            transfer_: transfer,
            left_: left,
            ended_: ended,
            demand_: Demand::exactly(0usize),
        }
    }

    /// 本条报文的传输模式。
    pub const fn transfer(&self) -> BodyTransfer {
        self.transfer_
    }

    /// 取回底层缓冲的引用。
    pub const fn inner(&self) -> &TyRx {
        self.inner_
    }
}

impl<TyRx> TrBuffTryRead<u8> for BodyReader<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type SegmRef<'f>
        = LimitedRefSegm<'f, TyRx::SegmRef<'f>>
    where
        Self: 'f;

    type Err = BodyReadError<TyRx::Err>;

    fn try_read<'f>(&'f mut self, demand: &'f Demand<usize>) -> SomeOf<Self::SegmRef<'f>, Self::Err> {
        let BodyReader {
            inner_,
            transfer_,
            left_,
            ended_,
            demand_,
        } = self;
        match transfer_ {
            BodyTransfer::Absent => SomeOf::new_right(BodyReadError::Absent),
            BodyTransfer::Sized(_) => limited_try_read_(&mut **inner_, left_, demand_, demand)
                .map_right(BodyReadError::Sized),
            BodyTransfer::Chunked => chunked_try_read_(
                &mut **inner_,
                left_,
                *ended_,
                demand_,
                demand,
            )
            .map_right(BodyReadError::Chunked),
        }
    }
}

impl<TyRx> TrBuffRead<u8> for BodyReader<'_, TyRx>
where
    TyRx: TrBuffRead<u8>,
{
    type ReadAsync<'f>
        = BodyReaderAsync<'f, 'f, TyRx>
    where
        Self: 'f;

    fn read_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::ReadAsync<'f> {
        BodyReaderAsync::new(
            &mut *self.inner_,
            self.transfer_,
            &mut self.left_,
            &mut self.ended_,
            &mut self.demand_,
            demand,
        )
    }
}

/// [`BodyReader::read_async`] 的 step 函数：按模式把借段交给限长/分块两条路径之一。
#[gen_may_cancel_future(BodyReader, pub, new(pub(crate)))]
async fn body_reader_async_<'f, TyRx, TyTok>(
    inner: &'f mut TyRx,
    transfer: BodyTransfer,
    left: &'f mut usize,
    ended: &'f mut bool,
    demand_slot: &'f mut Demand<usize>,
    demand: &'f Demand<usize>,
    cancel: TyTok,
) -> SomeOf<LimitedRefSegm<'f, TyRx::SegmRef<'f>>, BodyReadError<TyRx::Err>>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    match transfer {
        BodyTransfer::Absent => SomeOf::new_right(BodyReadError::Absent),
        BodyTransfer::Sized(_) => {
            limited_read_step_(inner, left, demand_slot, demand, cancel)
                .await
                .map_right(BodyReadError::Sized)
        }
        BodyTransfer::Chunked => {
            chunked_read_step_(inner, left, ended, demand_slot, demand, cancel)
                .await
                .map_right(BodyReadError::Chunked)
        }
    }
}

/// 按报文头给出一条报文的体读视图。
///
/// **没有体时也返回视图**（模式是 [`BodyTransfer::Absent`]）：调用方不必先自己判一遍，
/// 读它只会立刻得到 EOF，底层缓冲一个字节都不会被碰。
///
/// # Errors
///
/// 头里的体声明自相矛盾或无法解读时返回 [`ProtocolViolation`]。
pub fn body_reader<'a, TyRx>(
    rx: &'a mut TyRx,
    headers: Option<&Headers>,
) -> Result<BodyReader<'a, TyRx>, ProtocolViolation>
where
    TyRx: TrBuffRead<u8>,
{
    let transfer = body_transfer_of(headers)?;
    Ok(BodyReader::new(rx, transfer))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 发送搬运
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 按报文头声明的模式，把体写进 `dst`。
///
/// **怎么写出头决定，写什么由体决定**：
///
/// - 没有体：一个字节都不写；
/// - 定长：`dst` 外面套一个限长写口，写出量必须**恰好**等于 `Body_Size` 声明的长度
///   ——少了报截断，多了会被限长写口挡下；
/// - 分块：`dst` 外面套一个分块写口，体每写一次就封一块，**块长就是这一次写出的字节数**，
///   最后补一个长度 `0` 的终止块。体的总长直到发完都不必知道。
///
/// 编码发生在这一刻：体（例如 [`CodableBody`](super::basic::CodableBody)）在此之前只是
/// 业务值与编码格式的打包，没有被编过，因此大 body 不会卡住构造请求的那一行。
///
/// # Errors
///
/// 头部声明违规、体自称的长度与声明不符、定长体写出量对不上、编码失败，或写 `dst` 失败时
/// 返回 [`MessageIoError`]。
#[gen_may_cancel_future(SendBody, pub, new(pub(crate)))]
async fn send_body_async_<'f, TyBody, TyDst, TyTok>(
    body: &'f TyBody,
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    cancel: TyTok,
) -> Result<usize, MessageIoError>
where
    TyBody: TrRpcBody,
    TyDst: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let transfer = body_transfer_of(headers)
        .map_err(|err| MessageIoError::Protocol(err.to_string()))?;
    if !transfer.has_body() || !body.has_body() {
        // 「头里没有体」与「体自己说没有内容」都意味着一个字节都不写。
        return Result::Ok(0usize);
    }
    match transfer {
        BodyTransfer::Absent => Result::Ok(0usize),
        BodyTransfer::Sized(size) => write_sized_async_(body, dst, size, cancel).await,
        BodyTransfer::Chunked => write_chunked_async_(body, dst, cancel).await,
    }
}

/// 定长：把体写进一个**恰好 `size` 字节**的限长写口。
async fn write_sized_async_<'f, TyBody, TyDst, TyTok>(
    body: &'f TyBody,
    dst: &'f mut TyDst,
    size: usize,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyBody: TrRpcBody,
    TyDst: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    // 装配期核对：体若自称长度已知，就必须与头里声明的一致——不一致说明两端对这条报文的
    // 理解已经不同，宁可当场失败，也不要写出一条会让接收方错位的报文。
    if let Option::Some(known) = body
        .try_known_len()
        .map_err(|err| MessageIoError::Encode(err.to_string()))?
        && known != size
    {
        return Result::Err(MessageIoError::BodySizeMismatch {
            declared: size,
            actual: known,
        });
    }

    let written = {
        let mut limited = LimitedWrite::new(dst, size);
        // `WaitingTx` 让 `AsStdWrite` 走「等对端腾空间」那条路：环满时它等，而不是把
        // 字节攒在别处。额度耗尽时限长写口会报「不能再写」，写超出的体因此立刻失败。
        let mut waiting = WaitingTx::new_(&mut limited);
        let mut write = AsStdWrite::new(&mut waiting, tok.child_token());
        body.try_encode_into(&mut write)
            .map_err(|err| MessageIoError::Encode(err.to_string()))?
    };
    if written != size {
        return Result::Err(MessageIoError::Truncated(format!(
            "体声明为 {size} 字节，实际只写出 {written} 字节"
        )));
    }
    Result::Ok(written)
}

/// 分块：把体写进一个**写多少就封多大块**的分块写口，末尾补终止块。
async fn write_chunked_async_<'f, TyBody, TyDst, TyTok>(
    body: &'f TyBody,
    dst: &'f mut TyDst,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TyBody: TrRpcBody,
    TyDst: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken,
{
    let mut chunked = ChunkedSink::new(dst, tok.child_token());
    body.try_encode_into(&mut chunked)
        .map_err(|err| MessageIoError::Encode(err.to_string()))?;
    chunked
        .finish()
        .map_err(|err| MessageIoError::Io(err.to_string()))?;
    // 回报的是线上字节数（含块前缀与终止块），调用方据此知道这条体占用了多少字节。
    Result::Ok(chunked.wire_written())
}

/// 把报文体按报文头声明的模式写进 `dst`。语义与参数见 [`send_body_async_`] 的文档。
///
/// 返回实际写出的**线上**字节数：定长时就是体的长度，分块时含各块的 2 字节前缀与终止块。
pub fn send_body_async<'f, TyBody, TyDst, TyTok>(
    body: &'f TyBody,
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    tok: TyTok,
) -> SendBodyFuture<'f, 'f, TyBody, TyDst, TyTok>
where
    TyBody: TrRpcBody,
    TyDst: TrBuffWrite<u8> + TrProducerState,
    TyTok: TrCancellationToken + 'f,
{
    SendBodyAsync::new(body, dst, headers).may_cancel_with(tok)
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 从字节流搬运
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把一个 `TrBuffRead<u8>` **源**按报文头声明的模式搬进 `dst`。
///
/// 这是发送体的**底层真实入口**：体在别处已经是一股字节流时（`&[u8]`、环的读半边、
/// 文件、上游报文转发的字节……）都走这里。协议层只负责搬运，不预量长度、不为体分配缓存：
///
/// - 没有体：一个字节都不搬；
/// - 定长：源被套上限长读，只搬 `Body_Size` 声明的那么多；源提前结束报截断，源有多余
///   的字节**不碰**；
/// - 分块：源每让出一段就封一块，**块长就是那一段实际的字节数**（超 65535 自动切开），
///   源耗尽后补一个长度 `0` 的块收尾。
///
/// 搬运全程是**段到段**的直接拷贝：从源借一段、往目标借一段、把前者搬进后者。这既免掉了
/// 中转缓冲，也保证了源**真的被消费**——只读一眼段的字节是不会推进源的（段的消费量只在
/// 段被回收时结算）。
///
/// [`send_content_async`] 是它在「内容就在内存里」时的便利包装。
///
/// # Errors
///
/// 头部声明违规、源提前结束（定长不足）、读写任一侧失败，或期间被取消时返回
/// [`MessageIoError`]。
#[gen_may_cancel_future(SendBodyFromReader, pub, new(pub(crate)))]
async fn send_body_from_reader_async_<'f, TySrc, TyDst, TyTok>(
    src: &'f mut TySrc,
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    cancel: TyTok,
) -> Result<usize, MessageIoError>
where
    TySrc: TrBuffRead<u8>,
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    let transfer = body_transfer_of(headers)
        .map_err(|err| MessageIoError::Protocol(err.to_string()))?;
    match transfer {
        // 没有体：一个字节都不搬。
        BodyTransfer::Absent => Result::Ok(0usize),
        BodyTransfer::Sized(size) => copy_sized_async_(src, dst, size, cancel).await,
        BodyTransfer::Chunked => copy_chunked_async_(src, dst, cancel).await,
    }
}

/// 定长搬运：只搬 `size` 字节。
async fn copy_sized_async_<'f, TySrc, TyDst, TyTok>(
    src: &'f mut TySrc,
    dst: &'f mut TyDst,
    size: usize,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TySrc: TrBuffRead<u8>,
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    // 源被套上限长读：即使上游写得比声明的多，也不会被这条报文搬走。
    let mut limited = LimitedRead::new(src, size);
    let mut total = 0usize;
    while total < size {
        let demand = Demand::no_more_than(size - total);
        let read_fut = limited.read_async(&demand).into_future();
        let Option::Some(res) = race_cancel_(tok.child_token(), read_fut).await else {
            return Result::Err(MessageIoError::Io(
                "发送定长报文体期间被取消".to_string(),
            ));
        };
        let mut segm = match split_some_of_(res) {
            Result::Ok(segm) => segm,
            Result::Err(LimitReadError::Inner(err)) if err.err_tag() == ReadErrTag::Closing => {
                return Result::Err(MessageIoError::Truncated(format!(
                    "体声明了 {size} 字节，源在搬完之前就结束了（已搬 {total} 字节）"
                )));
            }
            Result::Err(err) => {
                return Result::Err(MessageIoError::Io(err.to_string()));
            }
        };

        // 只借「源这一段首片」那么多空间，搬完就换下一段。
        let want = segm.least_count();
        if want == 0usize {
            return Result::Err(MessageIoError::Io(
                "源让出的段是空的，无法推进".to_string(),
            ));
        }
        let write_demand = Demand::no_more_than(want);
        let write_fut = dst.write_async(&write_demand).into_future();
        let Option::Some(res) = race_cancel_(tok.child_token(), write_fut).await else {
            return Result::Err(MessageIoError::Io(
                "发送定长报文体期间被取消".to_string(),
            ));
        };
        let mut target = match split_some_of_(res) {
            Result::Ok(segm) => segm,
            Result::Err(err) => return Result::Err(MessageIoError::Io(err.to_string())),
        };
        let moved = {
            let mut source = segm.as_segm_ref();
            let mut target = target.as_segm_mut();
            source.move_items_to_segm(&mut target)
        };
        if moved == 0usize {
            return Result::Err(MessageIoError::Io(
                "目标让出的段是空的，无法推进".to_string(),
            ));
        }
        total += moved;
    }
    Result::Ok(total)
}

/// 分块搬运：源每让出一段就封一块，末尾补终止块。
///
/// 每个块的长度前缀取的是**这一块实际搬走的字节数**：块边界就是源让出的那一段（首片）
/// 的边界，最多 [`K_MAX_CHUNK_PAYLOAD`](super::chunked::K_MAX_CHUNK_PAYLOAD) 字节；更大的
/// 段会被切成多个块，剩下的部分留在源里等下一轮借出。
async fn copy_chunked_async_<'f, TySrc, TyDst, TyTok>(
    src: &'f mut TySrc,
    dst: &'f mut TyDst,
    tok: TyTok,
) -> Result<usize, MessageIoError>
where
    TySrc: TrBuffRead<u8>,
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    let mut total = 0usize;
    loop {
        let demand = Demand::at_least(1usize);
        let read_fut = src.read_async(&demand).into_future();
        let Option::Some(res) = race_cancel_(tok.child_token(), read_fut).await else {
            return Result::Err(MessageIoError::Io(
                "发送分块报文体期间被取消".to_string(),
            ));
        };
        let mut segm = match split_some_of_(res) {
            Result::Ok(segm) => segm,
            // 源到此为止：分块体的内容已经给完，接着补终止块。
            Result::Err(err) if err.err_tag() == ReadErrTag::Closing => break,
            Result::Err(err) => return Result::Err(MessageIoError::Io(err.to_string())),
        };

        let mut child = segm.as_segm_ref();
        let piece_len = child.iter_slices().map_or(0usize, <[u8]>::len);
        let mut offset = 0usize;
        while offset < piece_len {
            let take = core::cmp::min(piece_len - offset, super::chunked::K_MAX_CHUNK_PAYLOAD);

            // 目标段**恰好**留出「2 字节前缀 + 本块内容」的空间：多一点都不借，
            // 免得把源段里本属于下一块的字节一起搬走。
            let target_demand = Demand::exactly(take + 2usize);
            let write_fut = dst.write_async(&target_demand).into_future();
            let Option::Some(res) = race_cancel_(tok.child_token(), write_fut).await else {
                return Result::Err(MessageIoError::Io(
                    "发送分块报文体期间被取消".to_string(),
                ));
            };
            let mut target = match split_some_of_(res) {
                Result::Ok(segm) => segm,
                Result::Err(err) => return Result::Err(MessageIoError::Io(err.to_string())),
            };

            let prefix = [
                core::mem::MaybeUninit::new(((take >> 8) & 0xffusize) as u8),
                core::mem::MaybeUninit::new((take & 0xffusize) as u8),
            ];
            let wrote_prefix = target.move_items_from_buff(prefix.as_slice());
            if wrote_prefix != 2usize {
                return Result::Err(MessageIoError::Io(format!(
                    "目标只让出了 {wrote_prefix} 字节，放不下块长度前缀"
                )));
            }

            let Option::Some(mut part) = child.take_segm_ref(&Demand::exactly(take)) else {
                return Result::Err(MessageIoError::Io(
                    "源段让不出本块所需的字节".to_string(),
                ));
            };
            // 搬移同时完成两件事：目标收到内容、源段被消费（这正是源得以推进的原因）。
            let moved = target.move_items_from_segm(&mut part);
            if moved != take {
                return Result::Err(MessageIoError::Io(format!(
                    "目标只接收了 {moved} 字节，写不下声明为 {take} 字节的整块内容"
                )));
            }
            total += take;
            offset += take;
        }
    }

    // 终止块：长度 0。
    let target_demand = Demand::exactly(2usize);
    let write_fut = dst.write_async(&target_demand).into_future();
    let Option::Some(res) = race_cancel_(tok.child_token(), write_fut).await else {
        return Result::Err(MessageIoError::Io(
            "发送分块报文体终止块期间被取消".to_string(),
        ));
    };
    let mut target = match split_some_of_(res) {
        Result::Ok(segm) => segm,
        Result::Err(err) => return Result::Err(MessageIoError::Io(err.to_string())),
    };
    let end_mark = [
        core::mem::MaybeUninit::new(0u8),
        core::mem::MaybeUninit::new(0u8),
    ];
    if target.move_items_from_buff(end_mark.as_slice()) != 2usize {
        return Result::Err(MessageIoError::Io(
            "目标空间放不下分块体的终止块".to_string(),
        ));
    }
    Result::Ok(total)
}

/// 把一个 `TrBuffRead<u8>` 源按头声明的模式搬进 `dst`。语义与参数见
/// [`send_body_from_reader_async_`] 的文档。
///
/// 返回实际搬运的**体**字节数（分块模式下不含块前缀与终止块）。
pub fn send_body_from_reader_async<'f, TySrc, TyDst, TyTok>(
    src: &'f mut TySrc,
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    tok: TyTok,
) -> SendBodyFromReaderFuture<'f, 'f, TySrc, TyDst, TyTok>
where
    TySrc: TrBuffRead<u8>,
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken + 'f,
{
    SendBodyFromReaderAsync::new(src, dst, headers).may_cancel_with(tok)
}

/// 把一段**已经在内存里**的内容按头声明的模式发出去。
///
/// 它是 [`send_body_from_reader_async`] 的便利包装：`&[u8]` 本身就实现了
/// [`TrBuffRead<u8>`]，因此不需要任何中间类型——调用方少写一次 `let mut src = content;`
/// 而已。
///
/// 这是「直接塞一串字节发出去」的推荐入口；要发的是**业务值**时用 [`send_body_async`]
/// （编码器边编边写）。
///
/// # Errors
///
/// 同 [`send_body_from_reader_async`]。
#[gen_may_cancel_future(SendContent, pub, new(pub(crate)))]
async fn send_content_async_<'f, TyDst, TyTok>(
    content: &'f [u8],
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    cancel: TyTok,
) -> Result<usize, MessageIoError>
where
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    // `&[u8]` 自己就是源；这里只是给它一个可推进的落脚点。
    let mut src: &[u8] = content;
    send_body_from_reader_async_(&mut src, dst, headers, cancel).await
}

/// [`send_content_async_`] 的公开入口。
pub fn send_content_async<'f, TyDst, TyTok>(
    content: &'f [u8],
    dst: &'f mut TyDst,
    headers: Option<&'f Headers>,
    tok: TyTok,
) -> SendContentFuture<'f, 'f, TyDst, TyTok>
where
    TyDst: TrBuffWrite<u8>,
    TyTok: TrCancellationToken + 'f,
{
    SendContentAsync::new(content, dst, headers).may_cancel_with(tok)
}

/// 头里是否把报文体声明成「分块传输」。
///
/// 这是给不关心其余判定细节、只想知道「该不该用分块读」的调用方准备的便捷入口；
/// 语义与 [`body_transfer_of`] 一致，违规同样报错。
///
/// # Errors
///
/// 头里的体声明自相矛盾或无法解读时返回 [`ProtocolViolation`]。
pub fn is_chunked_body(headers: Option<&Headers>) -> Result<bool, ProtocolViolation> {
    Ok(matches!(body_transfer_of(headers)?, BodyTransfer::Chunked))
}

/// `Body_Transfer: Chunked` 的头值：写在请求 / 回复头里，声明体是分块给出的。
pub fn chunked_transfer_header_val() -> HeaderVal {
    HeaderVal::from(StdHeaderVal::Body_Transfer_Chunked)
}
