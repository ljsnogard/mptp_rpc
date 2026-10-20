//! demo 的**业务体编解码**：前缀线格式不在这里。
//!
//! 请求 / 回复前缀的线上格式已经由 [`mptp_smux::messaging`] 拥有；本模块只负责那些仍然属于
//! 用户应用的部分：
//!
//! - 业务类型 → MessagePack 字节（`MpEncoder`，直接写进 ring）；
//! - MessagePack 字节 → 业务类型（`MpDecoder`，按需从 ring 借段）；
//! - 按 `Body_Size` 从 ring 读一段体、向 ring 写一段已经编好的体。
//!
//! 体之后的**后缀流**是裸字节，既不封块也不解业务类型，本模块同样不参与。
//!
//! # 编码暂存区
//!
//! 这里仍保留一块固定暂存区（[`K_BODY_CAP_`]），因为业务体的长度通常已知、量级也小；需要
//! 更大的体时应改用分块体，而不是悄悄截断。
//!
//! # `body_`: the demo's body codec
//!
//! The prefix wire format is owned by [`mptp_smux::messaging`]. This module keeps only the
//! user-application half: business values ⇄ MessagePack bytes, and raw body bytes in and out
//! of the ring. The suffix stream after the body is raw bytes and never passes through here.

use std::io::Write;

use abs_buff::{
    Demand, TrBuffRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
    gen_may_cancel_future,
    x_deps::{abs_cancel, anylr},
};
use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{TrDecodable, TrDecodableOwned},
    ser::TrEncodable,
};
use anylr::SomeOf;
use mp_denco::{MpDecoder, MpEncoder};
use thiserror::Error;

/// 业务体编解码的错误。
///
/// 前缀的解码错误属于 [`mptp_smux::messaging::MessagingError`]，不在本类型里。
#[derive(Debug, Error)]
pub enum BodyError {
    /// 编码业务体失败。
    #[error("编码业务体失败：{0}")]
    Encode(String),

    /// 解码业务体失败（含对端提前关闭）。
    #[error("解码业务体失败：{0}")]
    Decode(String),

    /// 读写 ring 失败。
    #[error("读写业务体失败：{0}")]
    Io(String),
}

/// 业务体编码暂存区的容量（字节）。
///
/// 它只负责在超出时**明确失败**，不负责承载大报文。需要更大的体时应换成分块体或自备更大的
/// 暂存，而不是悄悄截断。
const K_BODY_CAP_: usize = 8usize * 1024usize;

/// 把 `value` 编进 `sink` 的剩余空间，返回写出的字节数；`sink` 会前进到尚未使用的后缀。
async fn encode_into_async_<TyValue, TyTok>(
    value: &TyValue,
    sink: &mut &mut [u8],
    cancel: TyTok,
) -> Result<usize, BodyError>
where
    TyValue: ?Sized + TrEncodable,
    TyTok: TrCancellationToken,
{
    let before = sink.len();
    match value
        .encode_async(MpEncoder::new(sink))
        .may_cancel_with(cancel)
        .await
    {
        Result::Ok(()) => Result::Ok(before - sink.len()),
        // 编码器本身没有「空间不足」这个错误：写段借不出来时它报的是底层写错误。
        // 暂存区恰好被写空是这里唯一能判定的容量信号。
        Result::Err(err) if sink.is_empty() => Result::Err(BodyError::Encode(format!(
            "编码暂存区 {K_BODY_CAP_} 字节不足：{err}"
        ))),
        Result::Err(err) => Result::Err(BodyError::Encode(err.to_string())),
    }
}

/// 把 `value` 编成一段独立字节（请求体 / 回复体）。
///
/// 编码是纯内存工作，没有可取消的等待点，因此这里用不可取消令牌。
pub(crate) async fn encode_to_vec_async_<TyValue>(value: &TyValue) -> Result<Vec<u8>, BodyError>
where
    TyValue: ?Sized + TrEncodable,
{
    let mut storage = vec![0u8; K_BODY_CAP_];
    let written = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        encode_into_async_(value, &mut sink, NonCancellableToken::new()).await?
    };
    storage.truncate(written);
    Result::Ok(storage)
}

/// 从接收半边**直接解出**一个 MessagePack 值——要多少借多少，不多读一个字节。
pub(crate) async fn decode_from_async_<'de, T, TyRx, TyTok>(
    rx: &mut TyRx,
    cancel: TyTok,
) -> Result<T, BodyError>
where
    T: TrDecodable<'de>,
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    <T as TrDecodable<'de>>::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| BodyError::Decode(err.to_string()))
}

/// 从**已经读出的整段字节**解出一个值。
///
/// 服务端先按 `Body_Size` 把体读出来、校验 `Data_Type_Id`，确认类型对得上才走到这里。
pub(crate) async fn decode_slice_async_<T>(bytes: &[u8]) -> Result<T, BodyError>
where
    T: TrDecodableOwned,
{
    let mut slice: &[u8] = bytes;
    decode_from_async_::<T, _, _>(&mut slice, NonCancellableToken::new()).await
}

/// 一个**不报告**生产者状态的发送半边包装。
///
/// `AsStdWrite` 遇到「目标报了状态且没有空间」会退化成尽力写（返回 `Ok(0)`），而
/// `write_all` 把 `Ok(0)` 当 `WriteZero` 错误。把状态报告挡掉，它就落回自己那条「等待对端
/// 腾出空间」的异步等待分支——于是 `write_all` 会一直等到这段字节**真正写完**，不会提前
/// 返回。这与 `mptp_core` 的做法相同。
struct WaitingTx<'a, W> {
    inner_: &'a mut W,
}

impl<'a, W> WaitingTx<'a, W> {
    const fn new_(inner: &'a mut W) -> Self {
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

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        self.inner_.write_async(demand)
    }
}

// 刻意**不实现** `producer_state`：默认实现返回 `None`，「不报状态」正是 `AsStdWrite`
// 进入异步等待分支的条件。
impl<W> TrProducerState for WaitingTx<'_, W> {}

/// 把 `bytes` 原样写进发送半边（等空间，不缓冲）。
async fn write_bytes_async_<TyTx, TyTok>(
    tx: &mut TyTx,
    bytes: &[u8],
    cancel: TyTok,
) -> Result<usize, BodyError>
where
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    let mut waiting = WaitingTx::new_(tx);
    let mut write = AsStdWrite::new(&mut waiting, cancel);
    write
        .write_all(bytes)
        .map_err(|err| BodyError::Io(err.to_string()))?;
    Result::Ok(bytes.len())
}

/// 把一段**已经在内存里**的字节写进发送半边（体、续流都走它）。
///
/// 它只写字节，不封帧：定长体直接写这么多，分块体由调用者自己按块写。本 demo 的体一律
/// 定长（`Body_Size` 头），因此这里就是「写体」的全部。
///
/// # Errors
///
/// 写 ring 失败（含等待期间被取消）时返回 [`BodyError::Io`]。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteBody, pub, new(pub(crate)))]
pub async fn write_body_async_<'f, TyTx, TyTok>(
    tx: &'f mut TyTx,
    bytes: &'f [u8],
    cancel: TyTok,
) -> Result<usize, BodyError>
where
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    write_bytes_async_(tx, bytes, cancel).await
}

/// 把一段**已经在内存里**的字节读出来（按 `Body_Size` 声明的长度）。
///
/// # Errors
///
/// 读 ring 失败、对端提前关闭或解出的值不合法时返回错误。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadBody, pub, new(pub(crate)))]
pub async fn read_body_async_<'f, TyRx, TyTok>(
    rx: &'f mut TyRx,
    size: usize,
    cancel: TyTok,
) -> Result<Vec<u8>, BodyError>
where
    TyRx: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    use abs_mptp::messaging::limited::LimitedReader;

    let mut limited = LimitedReader::new(rx, size);
    let mut read = AsStdRead::new(&mut limited, cancel);
    let mut bytes = vec![0u8; size];
    std::io::Read::read_exact(&mut read, &mut bytes)
        .map_err(|err| BodyError::Io(err.to_string()))?;
    Result::Ok(bytes)
}

#[cfg(test)]
mod tests_ {
    //! 业务体的纯内存往返：编码 → 解码。
    //!
    //! 这里不建连接、不碰 socket：`encode_to_vec_async_` 与 `decode_slice_async_` 都只操作
    //! 内存中的字节。真实 socket 往返由 `scripts/e2e_socket.py` 覆盖。
    //!
    //! In-memory round trips for the body codec. No socket is involved; the real round trip is
    //! covered by `scripts/e2e_socket.py`.

    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use super::*;

    /// 立即完成的测试执行器：编解码只操作内存，会一直 `Ready`。
    fn block_on_<TyFut>(future: TyFut) -> TyFut::Output
    where
        TyFut: Future,
    {
        let mut cx = Context::from_waker(Waker::noop());
        let mut future = pin!(future);
        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => core::hint::spin_loop(),
            }
        }
    }

    /// 测试业务体编码后再解码能还原原值。
    /// Test that a body value survives an encode-then-decode round trip.
    ///
    /// - 手段：用一个 `String` 作为业务体，经 `encode_to_vec_async_` 编成字节，再经
    ///   `decode_slice_async_` 解回来。
    /// - 判断：解出的字符串与原值完全相等；编码失败或解码吃掉多余字节都会让这条断言失败。
    ///
    /// - Means: encode a `String` body through `encode_to_vec_async_`, then decode it back
    ///   through `decode_slice_async_`.
    /// - Verdict: the decoded string equals the original; an encode failure or an over-read
    ///   on decode would fail this.
    #[test]
    fn body_roundtrip_preserves_value_() {
        let original = "hello from mptp_smux_v1_demo".to_string();
        let bytes = block_on_(encode_to_vec_async_(&original)).expect("业务体编码应当成功");
        let decoded: String = block_on_(decode_slice_async_(&bytes)).expect("业务体解码应当成功");

        assert_eq!(decoded, original);
    }
}
