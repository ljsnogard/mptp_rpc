//! 业务体编解码：MessagePack ⇄ Rust 类型，以及按 `Body_Size` 读写 ring。
//!
//! 前缀线格式由 `mptp_smux::messaging` 拥有；本模块只处理业务体。

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

/// 业务体编解码错误。
#[derive(Debug, Error)]
pub(crate) enum BodyError {
    /// 编码失败。
    #[error("编码业务体失败：{0}")]
    Encode(String),

    /// 解码失败。
    #[error("解码业务体失败：{0}")]
    Decode(String),

    /// 读写 ring 失败。
    #[error("读写业务体失败：{0}")]
    Io(String),
}

/// 编码暂存区容量（字节）。
const K_BODY_CAP_: usize = 64usize * 1024usize;

/// 把值编进 sink 的剩余空间，返回写出的字节数。
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
        Result::Err(err) if sink.is_empty() => Result::Err(BodyError::Encode(format!(
            "编码暂存区 {K_BODY_CAP_} 字节不足：{err}"
        ))),
        Result::Err(err) => Result::Err(BodyError::Encode(err.to_string())),
    }
}

/// 把值编成一段独立字节。
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

/// 从接收半边直接解出一个值。
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

/// 从已经读出的整段字节解出一个值。
pub(crate) async fn decode_slice_async_<T>(bytes: &[u8]) -> Result<T, BodyError>
where
    T: TrDecodableOwned,
{
    let mut slice: &[u8] = bytes;
    decode_from_async_::<T, _, _>(&mut slice, NonCancellableToken::new()).await
}

/// 不报告生产者状态的发送半边包装。
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

// 刻意不实现 producer_state：让 AsStdWrite 进入异步等待分支。
impl<W> TrProducerState for WaitingTx<'_, W> {}

/// 把字节原样写进发送半边。
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

/// 把一段已经在内存里的字节写进发送半边。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteBody, pub(crate), new(pub(crate)))]
pub(crate) async fn write_body_async_<'f, TyTx, TyTok>(
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

/// 按 `Body_Size` 声明的长度从接收半边读出一段字节。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadBody, pub(crate), new(pub(crate)))]
pub(crate) async fn read_body_async_<'f, TyRx, TyTok>(
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
    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use super::*;

    /// 用立即完成的执行器驱动纯内存 future。
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
    /// Test that a body value survives an encode/decode round trip.
    ///
    /// - 手段：用 `String` 走 `encode_to_vec_async_` 与 `decode_slice_async_`。
    /// - 判断：解出的字符串与原值完全相等。
    ///
    /// - Means: round-trip a `String` through the body codec.
    /// - Verdict: the decoded value equals the original.
    #[test]
    fn body_roundtrip_preserves_value_() {
        let original = "workspace-alpha".to_string();
        let bytes = block_on_(encode_to_vec_async_(&original)).expect("编码应当成功");
        let decoded: String = block_on_(decode_slice_async_(&bytes)).expect("解码应当成功");

        assert_eq!(decoded, original);
    }
}
