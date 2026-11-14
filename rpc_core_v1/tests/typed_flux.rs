//! 类型化 flux 的集成测试（A-01 / A-02 / A-03 / A-05 的语义层证据）。
//!
//! 这些用例只走 `rpc_core_v1` 的公开 API 与内存读写口，不需要真实 TCP；传输层的
//! `SuffixStream` / `RequestSuffixStream` 适配在各自的 crate 测试里覆盖。
//!
//! # `typed_flux`: integration tests for the typed guards and fluxes
//!
//! The cases use only the public `rpc_core_v1` API over in-memory readers/writers; no
//! real TCP is involved. Transport adapters are covered by their own crate tests.

use abs_denco::{de::TrDecodable, ser::TrEncodable};
use futures::executor::block_on;
use mp_denco::MpEncoder;
use rpc_core_v1::stream::{
    Just, StreamError, StreamFuture, TrSuffixFinish, TrSuffixWrite, TypedJustFlux, TypedSlotFlux,
};
use rpc_core_v1::x_deps::{
    abs_async_iter::{TrAsyncIterator, TrFlux},
    abs_buff::{
        Demand, TrBuffFlush, TrBuffTryFlush, TrBuffTryWrite, TrBuffWrite, x_deps::anylr::SomeOf,
    },
    abs_cancel::{NonCancellableToken, TrMayCancel},
    abs_denco, mp_denco,
};

/// 把两个 `String` 依次编码进一段字节。
async fn encode_two_(a: &str, b: &str) -> Vec<u8> {
    let mut storage = vec![0u8; 4096usize];
    let used = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        a.to_string()
            .encode_async(MpEncoder::new(&mut sink))
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("编码第一个值");
        b.to_string()
            .encode_async(MpEncoder::new(&mut sink))
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("编码第二个值");
        4096usize - sink.len()
    };
    storage.truncate(used);
    storage
}

/// 依次解出两个 `String`。
async fn decode_two_(bytes: &[u8]) -> (String, String) {
    let mut src: &[u8] = bytes;
    let first = String::decode_async(MpDecoder::new(&mut src))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .expect("解第一个值");
    let second = String::decode_async(MpDecoder::new(&mut src))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .expect("解第二个值");
    (first, second)
}

use mp_denco::MpDecoder;

/// 测试借用型 reader 上的 `TypedJustFlux` 顺序读取与 take 推进。
/// Test ordered reads and take-driven progress of `TypedJustFlux` over a borrowed reader.
///
/// - 手段：把两个字符串编码进 `Vec<u8>`，用 `&[u8]` 构造 flux，依次 `next_async` + `take`。
/// - 判断：两个值按顺序返回；第三次读取以错误结束（流结束）。
///
/// - Means: encode two strings, build the flux over `&[u8]`, and take two values.
/// - Verdict: values arrive in order; the third read ends with an error.
#[test]
fn just_take_progresses_() {
    let bytes = block_on(encode_two_("alpha", "beta"));
    let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
    let mut iter = block_on(flux.pull_async());

    assert_eq!(
        block_on(block_on(iter.next_async()).expect("第一个值").take_async()),
        Ok("alpha".to_string())
    );
    assert_eq!(
        block_on(block_on(iter.next_async()).expect("第二个值").take_async()),
        Ok("beta".to_string())
    );
    assert!(block_on(iter.next_async()).is_err());
}

/// 测试 `Just` 未 take 时不能推进下一个值。
/// Test that `Just` blocks the next item until it is taken.
///
/// - 手段：解出第一个 `Just` 后保留，直接再调 `next_async`。
/// - 判断：第二次 `next_async` 返回 `GuardPending`；take 第一个值仍正常。
///
/// - Means: keep the first `Just` and call `next_async` again.
/// - Verdict: the second call returns `GuardPending`; taking the first value still works.
#[test]
fn just_guard_blocks_next_() {
    let bytes = block_on(encode_two_("alpha", "beta"));
    let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
    let mut iter = block_on(flux.pull_async());

    let first: Just<'_, String> = block_on(iter.next_async()).expect("第一个值");
    assert_eq!(
        block_on(iter.next_async()).unwrap_err(),
        StreamError::GuardPending
    );
    assert_eq!(first.take(), "alpha".to_string());
}

/// 测试未 take 的 `Just` drop 会中止流，而不是静默跳过。
/// Test that dropping an untaken `Just` aborts the stream instead of skipping.
///
/// - 手段：解出第一个值后 drop 守卫，再调 `next_async`。
/// - 判断：返回 `Aborted`。
///
/// - Means: drop the first guard, then call `next_async`.
/// - Verdict: it returns `Aborted`.
#[test]
fn just_drop_aborts_() {
    let bytes = block_on(encode_two_("alpha", "beta"));
    let mut flux = TypedJustFlux::<_, String>::new(bytes.as_slice());
    let mut iter = block_on(flux.pull_async());

    drop(block_on(iter.next_async()).expect("第一个值"));
    assert_eq!(
        block_on(iter.next_async()).unwrap_err(),
        StreamError::Aborted
    );
}

/// 测试用写端：借用一段固定切片，`finish` 返回写入长度作为「服务端 ack」。
struct SliceSink<'a> {
    /// 目标切片；写入时自身向前推进。
    buf_: &'a mut [u8],

    /// 初始长度。
    init_: usize,
}

impl<'a> SliceSink<'a> {
    /// 在给定切片上构造。
    fn new(buf: &'a mut [u8]) -> Self {
        let init_ = buf.len();
        SliceSink { buf_: buf, init_ }
    }

    /// 已写入字节数。
    fn used(&self) -> usize {
        self.init_ - self.buf_.len()
    }
}

/// [`SliceSink`] 的标准写视图。
struct SliceSinkView<'b, 'a> {
    /// 指向切片变量的可变引用。
    buf_: &'b mut &'a mut [u8],
}

impl<'b, 'a> TrBuffTryWrite<u8> for SliceSinkView<'b, 'a> {
    type SegmMut<'f>
        = <&'a mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <&'a mut [u8] as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <&'a mut [u8] as TrBuffTryWrite<u8>>::try_write(self.buf_, demand)
    }
}

impl<'b, 'a> TrBuffWrite<u8> for SliceSinkView<'b, 'a> {
    type WriteAsync<'f>
        = <&'a mut [u8] as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        <&'a mut [u8] as TrBuffWrite<u8>>::write_async(self.buf_, demand)
    }
}

impl<'b, 'a> TrBuffTryFlush<u8> for SliceSinkView<'b, 'a> {
    fn try_flush(&mut self) -> Result<(), Self::Err> {
        <&'a mut [u8] as TrBuffTryFlush<u8>>::try_flush(self.buf_)
    }

    fn close(&mut self) -> Result<(), Self::Err> {
        <&'a mut [u8] as TrBuffTryFlush<u8>>::close(self.buf_)
    }
}

impl<'b, 'a> TrBuffFlush<u8> for SliceSinkView<'b, 'a> {
    type FlushAsync<'f>
        = <&'a mut [u8] as TrBuffFlush<u8>>::FlushAsync<'f>
    where
        Self: 'f;

    type CloseAsync<'f>
        = <&'a mut [u8] as TrBuffFlush<u8>>::CloseAsync<'f>
    where
        Self: 'f;

    fn flush_async(&mut self) -> Self::FlushAsync<'_> {
        <&'a mut [u8] as TrBuffFlush<u8>>::flush_async(self.buf_)
    }

    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        <&'a mut [u8] as TrBuffFlush<u8>>::close_async(self.buf_)
    }
}

impl<'a> TrSuffixWrite for SliceSink<'a> {
    type Writer<'b>
        = SliceSinkView<'b, 'a>
    where
        Self: 'b;

    fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError> {
        Ok(SliceSinkView {
            buf_: &mut self.buf_,
        })
    }
}

impl<'a> TrSuffixFinish<'a> for SliceSink<'a> {
    type FinishOutput = usize;

    fn finish_async(self) -> StreamFuture<'a, usize> {
        let used = self.used();
        StreamFuture::new(async move { Ok(used) })
    }
}

/// 测试有限 Push：逐槽写入 → 半关闭 → 拿到服务端 ack。
/// Test finite Push: write slot by slot, half-close, then receive the server ack.
///
/// - 手段：用固定切片作底层 buff，set 两个字符串后 `finish_async`。
/// - 判断：返回的 ack 等于写入长度，且字节能解出两个原值。
///
/// - Means: use a fixed slice as the underlying buff, set two strings, then
///   `finish_async`.
/// - Verdict: the ack equals the written length and the bytes decode to both values.
#[test]
fn finite_push_writes_then_finishes_() {
    let mut storage = [0u8; 4096usize];
    let sink = SliceSink::new(&mut storage);
    let mut flux = TypedSlotFlux::<_, String>::new(sink);
    let mut iter = block_on(flux.pull_async());

    block_on(
        block_on(iter.next_async())
            .expect("槽 1")
            .set_async("alpha".to_string()),
    )
    .expect("写 alpha");
    block_on(
        block_on(iter.next_async())
            .expect("槽 2")
            .set_async("beta".to_string()),
    )
    .expect("写 beta");

    let ack = block_on(iter.finish_async()).expect("finish");
    drop(flux);
    assert_eq!(ack, 11usize, "两个字符串的编码长度");
    let (first, second) = block_on(decode_two_(&storage[..ack]));
    assert_eq!(first, "alpha");
    assert_eq!(second, "beta");
}

/// 测试未 set 的 `Slot` drop 会中止流；随后 `finish_async` 仍能半关闭。
/// Test that dropping an unset `Slot` aborts the stream, and `finish_async` still closes.
///
/// - 手段：取一个槽直接 drop，再 `next_async`，最后 finish。
/// - 判断：`next_async` 返回 `Aborted`；finish 返回 0 长度。
///
/// - Means: drop an unset slot, call `next_async`, then finish.
/// - Verdict: `next_async` returns `Aborted`; finish reports zero written bytes.
#[test]
fn slot_drop_aborts_then_finish_() {
    let mut storage = [0u8; 4096usize];
    let sink = SliceSink::new(&mut storage);
    let mut flux = TypedSlotFlux::<_, String>::new(sink);
    let mut iter = block_on(flux.pull_async());

    drop(block_on(iter.next_async()).expect("槽"));
    assert_eq!(
        block_on(iter.next_async()).unwrap_err(),
        StreamError::Aborted
    );
    let ack = block_on(iter.finish_async()).expect("finish");
    assert_eq!(ack, 0usize);
}
