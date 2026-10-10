//! 报文前缀与报文体的 IO：**直接读写 ring，路径上不留任何缓冲**。
//!
//! # 为什么是 `AsStdRead` / `AsStdWrite`
//!
//! 这两个适配器把 ring 的两个半边直接暴露成 `std::io::Read` / `Write`：`rmp-serde`
//! 编出来的字节被**逐段落进 ring 的可用段**，解出来的字节**直接从 ring 的段里取**，
//! 中间没有第二块内存。这正是 ring 存在的意义——字节从产生到上网只经过那块共享内存。
//! 换成「先序列化到 `Vec` 再写出去」「先读进 `Vec` 再解」会立刻丢掉这个性质。
//!
//! # 为什么没有预读缓冲
//!
//! 不需要。`AsStdRead::read` 只搬**调用方要的那么多**字节，而 `rmp-serde` 要多少读
//! 多少——解析前缀时它不会碰报文体的第一个字节。缓冲只是把「多读的字节」挪个地方存，
//! 而这里根本不会多读。
//!
//! # 两个必须正面处理的困难
//!
//! **其一：`AsStdWrite` 在环满时不等。** 它在目标**报告**生产者状态时退化成「尽力写」：
//! 写不进去就返回 `Ok(0)`，而 `write_all` 会把 `Ok(0)` 当成 `WriteZero` 错误。
//! `buffex::RingWriter` 正是会报告状态的那一类。这里不去绕过它（绕过就意味着自己攒
//! 缓冲），而是用一个**不报告状态**的包装 [`WaitingTx`]，让它走回自己那条为「等待对端
//! 腾出空间」准备的异步等待分支。
//!
//! **其二：适配器要求调用点处于后端运行时上下文内。** 它们的同步等待最终落到
//! `TrLocalScope::block_on_local`，而后者要问门面取当前后端的运行时值。这是这条路线的
//! 固有限制：**调用点必须满足它**（连接交给宿主线程驱动、测试进入后端上下文）。
//! 本模块提供的每个 IO 入口都是 `async`：同步只发生在 `serde` 那一层，不外溢成调用方
//! 的语义。

use core::{
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};
use std::io::{self, Write};

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryWrite, TrBuffWrite,
    buffer::TrProducerState,
    x_deps::{abs_cancel, anylr},
};
use abs_buff_stdio_adapt::AsStdRead;
use abs_cancel::TrCancellationToken;
use anylr::SomeOf;
use serde::de::DeserializeOwned;
use thiserror::Error;

use super::body::{BodyReader, body_transfer_of};
use crate::specs::{Headers, StdHeaderKey};

/// 报文级 IO 的错误。
///
/// 它对使用者可见：`recv_request_body_async` / `recv_response_body_async` 这两个公开
/// 入口把它交回给 handler 与客户端。
#[derive(Debug, Error)]
pub enum MessageIoError {
    /// 报文编码失败。
    #[error("encode failed: {0}")]
    Encode(String),

    /// 报文解码失败。
    #[error("decode failed: {0}")]
    Decode(String),

    /// 读写底层缓冲失败（例如把字节写进 ring 时出错）。
    #[error("io failed: {0}")]
    Io(String),

    /// 报文头里的体声明自相矛盾或无法解读。
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// 对端在报文读完之前关闭了通道。
    ///
    /// 报文头已经声明了长度，流却在长度满足之前结束——这是**协议违规**，不能当成功。
    #[error("channel closed early: {0}")]
    Truncated(String),

    /// `Body_Size` 头声明的长度与实际体字节数不符。
    ///
    /// 协议路径**不再**预量体长，因此当前没有产生点：定长体在搬运时源给不出声明的那么多
    /// 字节，如实报成 [`MessageIoError::Truncated`]。装配期的同类校验由
    /// [`RequestBuildError::BodySizeMismatch`](super::request::RequestBuildError::BodySizeMismatch)
    /// 承担；这个变体保留给「显式核对」的场合。
    #[error("Body_Size 头声明 {declared} 字节，实际体有 {actual} 字节")]
    BodySizeMismatch { declared: usize, actual: usize },
}

/// 判断一个 `rmp-serde` 解码错误是否只是「流提前结束」。
fn is_truncated_(err: &rmp_serde::decode::Error) -> bool {
    use rmp_serde::decode::Error as DecodeError;
    match err {
        DecodeError::InvalidMarkerRead(io) | DecodeError::InvalidDataRead(io) => {
            io.kind() == io::ErrorKind::UnexpectedEof
        }
        _ => false,
    }
}

/// 把 `rmp-serde` 的解码错误映射成本层的错误。
fn map_decode_err_(err: rmp_serde::decode::Error) -> MessageIoError {
    if is_truncated_(&err) {
        MessageIoError::Truncated(err.to_string())
    } else {
        MessageIoError::Decode(err.to_string())
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 竞速取消
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 让一个**不可取消**的 future 也能响应取消令牌。
///
/// # 为什么不用 `may_cancel_with`
///
/// 交付给 `gen_may_cancel_future` 的 step 函数里，底层缓冲的 `ReadAsync<'f>` /
/// `WriteAsync<'f>` 借用了 step 的缓冲参数，而 `may_cancel_with` 要求令牌类型
/// `C: 'f`。这条约束只能写在 where 子句里，但宏会把 where 中**提到取消令牌类型**的
/// 谓词整条去掉（它自己只补 `TyTok__: TrCancellationToken`），于是那条约束无处可写。
///
/// 这里改用竞速：令牌先响应就返回 `None`，底层 future 随本函数返回而被丢弃，它的等待
/// 注册随之注销。`TrCancellationToken` 只保证 `Send + Sync`，因此不假设它 `Unpin`。
pub(super) async fn race_cancel_<TyFut, TyTok>(tok: TyTok, fut: TyFut) -> Option<TyFut::Output>
where
    TyFut: Future,
    TyTok: TrCancellationToken,
{
    if !tok.can_be_cancelled() {
        return Option::Some(fut.await);
    }
    let mut fut = pin!(fut);
    let mut cancelled = pin!(tok.cancellation());
    poll_fn(|cx| {
        if cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Option::None);
        }
        if let Poll::Ready(output) = fut.as_mut().poll(cx) {
            return Poll::Ready(Option::Some(output));
        }
        Poll::Pending
    })
    .await
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// WaitingTx：让 AsStdWrite 等空间，而不是「尽力写」
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个**不报告**生产者状态的发送半边包装。
///
/// `AsStdWrite` 的分支是这么写的：目标报了状态且「无空间 / 对端已关」就直接返回
/// `Ok(0)`；**不报**状态才落进 `write_async` 的等待。于是这里的全部工作就是把状态报告
/// 挡掉（`TrProducerState` 用默认实现）——`write_all` 因此能正常等到空间、把报文写完，
/// 而不需要我们在外面攒一块缓冲。
///
/// 由调用方**按值持有**：`AsStdWrite` 要的是 `&mut W`，所以它必须活得比那个借用长。
/// 这是一次编译期就能确定代价的转发：没有新分配，也没有额外的数据移动。
pub(super) struct WaitingTx<'a, W> {
    inner_: &'a mut W,
}

impl<'a, W> WaitingTx<'a, W> {
    pub(super) const fn new_(inner: &'a mut W) -> Self {
        WaitingTx { inner_: inner }
    }
}

impl<W> TrBuffTryWrite<u8> for WaitingTx<'_, W>
where
    W: TrBuffTryWrite<u8>,
{
    type SegmMut<'f>
        = W::SegmMut<'f>
    where
        Self: 'f;

    type Err = W::Err;

    #[inline]
    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        self.inner_.try_write(demand)
    }
}

impl<W> TrBuffWrite<u8> for WaitingTx<'_, W>
where
    W: TrBuffWrite<u8>,
{
    type WriteAsync<'f>
        = W::WriteAsync<'f>
    where
        Self: 'f;

    #[inline]
    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.inner_.write_async(demand)
    }
}

// 刻意**不实现** `producer_state`：trait 的默认实现返回 `None`，而「不报状态」正是
// `AsStdWrite` 进入异步等待分支的条件。
impl<W> TrProducerState for WaitingTx<'_, W> {}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// CountingWrite：只数写了多少字节
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 给一个 `std::io::Write` 套一层写计数：调用方要回报「报文写出去多少字节」。
///
/// 它只加一个 `usize`，不缓冲任何数据——记账与缓冲是两件事，这里刻意只做前者。
pub(super) struct CountingWrite<W> {
    inner_: W,
    written_: usize,
}

impl<W> CountingWrite<W> {
    pub(super) const fn new_(inner: W) -> Self {
        CountingWrite {
            inner_: inner,
            written_: 0usize,
        }
    }

    pub(super) const fn written_(&self) -> usize {
        self.written_
    }
}

impl<W> Write for CountingWrite<W>
where
    W: Write,
{
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner_.write(buf)?;
        self.written_ += written;
        Ok(written)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.inner_.flush()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 读入
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 从接收半边**直接解出**一个 MessagePack 值。
///
/// `rmp-serde` 要多少字节就读多少字节，不会越过这个值的边界——因此解析完前缀之后，
/// 报文体的第一个字节仍然留在 ring 里等下一次读取。
///
/// # Errors
///
/// 报文不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub(crate) async fn decode_from_async_<T, TyRx, TyTok>(
    rx: &mut TyRx,
    tok: TyTok,
) -> Result<T, MessageIoError>
where
    T: DeserializeOwned,
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let mut read = AsStdRead::new(rx, tok);
    rmp_serde::from_read::<_, T>(&mut read).map_err(map_decode_err_)
}

/// 按报文头声明的传输方式，从接收半边**直接解出**一个业务类型的报文体。
///
/// 边界完全交给 [`body_reader`](super::body::body_reader) 给出的体视图：
///
/// - 没有体时**一个字节都不读**，返回 `None`；
/// - 定长体读到声明的长度即为流末，对端多写的字节不会被这条报文读走（那属于同一条
///   channel 上的后续数据）；
/// - 分块体按块自带的长度前缀解，读到终止块为止。
///
/// 解码直接发生在 ring 的接收半边上，中间没有中转缓冲。
///
/// # Errors
///
/// 头部声明违规、报文不是合法的 MessagePack、对端提前关闭，或读 ring 失败时返回错误。
pub(crate) async fn read_body_async_<T, TyRx, TyTok>(
    rx: &mut TyRx,
    headers: Option<&Headers>,
    tok: TyTok,
) -> Result<Option<T>, MessageIoError>
where
    T: DeserializeOwned,
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let transfer = body_transfer_of(headers).map_err(|err| MessageIoError::Protocol(err.to_string()))?;
    if !transfer.has_body() {
        return Ok(Option::None);
    }
    let mut reader = BodyReader::new(rx, transfer);
    let mut read = AsStdRead::new(&mut reader, tok);
    let value = rmp_serde::from_read::<_, T>(&mut read).map_err(map_decode_err_)?;
    // 解出一个值并不等于「这条体读完了」：解码器只知道值在哪里结束，不知道体的边界在哪里。
    // 剩下的部分必须一并读掉——分块体的终止块就在其中，不消费它，同一条 channel 上紧随
    // 其后的字节（suffix stream）就会被下一条报文当成自己的开头。读干净的终点由体视图给
    // 出：定长到声明长度为止，分块到终止块为止。
    std::io::copy(&mut read, &mut std::io::sink())
        .map_err(|err| MessageIoError::Io(err.to_string()))?;
    Ok(Option::Some(value))
}

/// 从头里取 `Body_Size` 声明的长度。
///
/// `None`（没有该头）按 0 处理：没有长度声明就是没有体。
///
/// # Errors
///
/// 头值既不是数字也不是合法的十进制字符串时返回错误。
pub(crate) fn try_get_body_size_(headers: Option<&Headers>) -> Result<usize, MessageIoError> {
    let Option::Some(headers) = headers else {
        return Ok(0usize);
    };
    let Option::Some(val) = headers.try_get_header(&StdHeaderKey::Body_Size.into()) else {
        return Ok(0usize);
    };
    match val.try_as_header_val() {
        Result::Ok(num) => Ok(num.into_inner() as usize),
        Result::Err(text) => text.parse::<usize>().map_err(|err| {
            MessageIoError::Decode(format!("Body_Size 头不是合法的长度（{text:?}）：{err}"))
        }),
    }
}
