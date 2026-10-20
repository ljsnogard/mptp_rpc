//! RPC body 的 MessagePack 编解码与流式 body IO。
//!
//! 业务体**不预编码**：编码器直接把字节写进一个 **64 字节的栈上 SPSC ring**，
//! `send_req_body_async` 从另一半读走并按请求头声明的定长 / 分块方式搬运。ring 只是
//! 并发窗口，不是业务体大小的上限；编码与发送在同一个 task 内 `join` 推进，不占堆、
//! 也不替使用者选运行时。
//!
//! 接收侧同样直接从限长 / 分块读视图解码，不把整个请求体攒进 `Vec`。
//!
//! # `body_`: streaming MessagePack body IO
//!
//! The body is not pre-encoded. The encoder writes into a 64-byte stack SPSC ring while
//! `send_req_body_async` drains the other half; the ring is only a bounded concurrency
//! window, not a size limit. Decoding reads directly through a limited / chunked view.

use core::mem::MaybeUninit;

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrConsumerState},
    error::{ReadErrTag, TrTaggedError},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{TrDecodable, TrDecodableOwned},
    ser::TrEncodable,
};
use abs_mptp::messaging::{chunked::ChunkedReader, limited::LimitedReader};
use buffex::ring::Ring;
use mp_denco::{MpDecoder, MpEncoder};

use crate::{
    error_::{RpcError, RpcResult},
    x_deps::{abs_buff, abs_cancel, abs_mptp},
};

/// 编码 / 发送之间的 ring 窗口（字节）。
///
/// 它不是业务体上限：ring 满时编码端等待，发送端搬走多少、编码端就能再写多少。
/// 64 字节足够容纳 MessagePack 的最大单次写入（128 位整数头+负载约 18 字节）；
/// 更大的字符串 / 二进制体由 `write_all_async_` 按 `Demand::no_more_than` 分段写。
pub(crate) const K_BODY_RING_CAP_: usize = 64usize;

/// 栈上 ring 本体：固定 64 字节缓冲区，整个对象随调用栈存活。
pub(crate) type BodyRing_ = Ring<[MaybeUninit<u8>; K_BODY_RING_CAP_]>;

/// 把业务值直接编进一个 `TrBuffWrite` 写口。
///
/// 编码器的每次写入都直接落到写口上；ring 满时由写口的 `write_async` 等待，不在本层
/// 攒整段字节。
///
/// # Errors
///
/// 业务值无法编码，或写口报错时返回 [`RpcError::Encode`]。
pub(crate) async fn encode_into_async_<V, T, K>(
    value: &V,
    tx: &mut T,
    cancel: K,
) -> RpcResult<()>
where
    V: ?Sized + TrEncodable,
    T: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    value
        .encode_async(MpEncoder::new(tx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| RpcError::Encode(err.to_string()))
}

/// 从接收半边直接解出一个值。
pub(crate) async fn decode_from_async_<'de, V, R, K>(
    rx: &mut R,
    cancel: K,
) -> RpcResult<V>
where
    V: TrDecodable<'de>,
    R: TrBuffRead<u8>,
    K: TrCancellationToken,
{
    <V as TrDecodable<'de>>::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| RpcError::Decode(err.to_string()))
}

/// 按请求头的体声明（定长 / 分块）直接从接收半边解码，不经过整段 `Vec`。
pub(crate) async fn decode_body_from_async_<V, R, K>(
    rx: &mut R,
    chunked: bool,
    size: usize,
    cancel: K,
) -> RpcResult<V>
where
    V: TrDecodableOwned,
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    if chunked {
        let mut reader = ChunkedReader::new(rx);
        decode_from_async_(&mut reader, cancel).await
    } else {
        let mut reader = LimitedReader::new(rx, size);
        decode_from_async_(&mut reader, cancel).await
    }
}

/// 把一段已经在内存里的字节直接搬进发送半边。
///
/// 不经过 `std::io` 适配层：借写段、把源字节搬进去、段回收时结算进度。
pub(crate) async fn write_body_async_<T, K>(
    tx: &mut T,
    bytes: &[u8],
    cancel: K,
) -> RpcResult<usize>
where
    T: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    let mut written = 0usize;
    while written < bytes.len() {
        if cancel.is_cancelled() {
            return Result::Err(RpcError::Cancelled);
        }
        let demand = Demand::no_more_than(bytes.len() - written);
        let mut res = tx
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let Some(segm) = res.as_mut().pick_left() else {
            let err = res.pick_right().expect("既不含左值，则必含右值");
            return Result::Err(RpcError::Transport(format!("写 RPC body 失败：{err}")));
        };
        let moved = segm.move_items_from_as_buff(&bytes[written..]);
        written += moved;
        if moved == 0usize {
            return Result::Err(RpcError::Transport(
                "写 RPC body 无进展：目标段为空".to_string(),
            ));
        }
    }
    Result::Ok(written)
}

/// 按 `Body_Size` 声明的长度从接收半边读出一段字节。
///
/// 只用于**已知长度的错误体**；业务体走 [`decode_body_from_async_`] 的流式解码。
pub(crate) async fn read_body_async_<R, K>(
    rx: &mut R,
    size: usize,
    cancel: K,
) -> RpcResult<Vec<u8>>
where
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let mut limited = LimitedReader::new(rx, size);
    let mut out = Vec::with_capacity(size);
    while out.len() < size {
        if cancel.is_cancelled() {
            return Result::Err(RpcError::Cancelled);
        }
        let demand = Demand::no_more_than(size - out.len());
        let mut res = limited
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let Some(segm) = res.as_mut().pick_left() else {
            let err = res.pick_right().expect("既不含左值，则必含右值");
            if err.err_tag() == ReadErrTag::Closing {
                return Result::Err(RpcError::Transport(format!(
                    "RPC 错误体声明了 {size} 字节，源提前结束（已读 {} 字节）",
                    out.len()
                )));
            }
            return Result::Err(RpcError::Transport(format!("读 RPC body 失败：{err}")));
        };
        let take = segm.least_count();
        let mut chunk = vec![0u8; take];
        let moved = segm.move_items_to_as_buff(chunk.as_mut_slice());
        out.extend_from_slice(&chunk[..moved]);
        if moved == 0usize {
            return Result::Err(RpcError::Transport(
                "读 RPC body 无进展：源段为空".to_string(),
            ));
        }
    }
    Result::Ok(out)
}

#[cfg(test)]
pub(crate) async fn decode_slice_async_<T>(bytes: &[u8]) -> RpcResult<T>
where
    T: TrDecodableOwned,
{
    let mut slice: &[u8] = bytes;
    decode_from_async_::<T, _, _>(
        &mut slice,
        abs_cancel::NonCancellableToken::new(),
    ).await
}

#[cfg(test)]
mod tests_ {
    use abs_cancel::NonCancellableToken;
    use futures::{executor::block_on, future::join};

    use super::*;

    /// 把 ring 读端的剩余内容读成一段字节，直到写端关闭。
    async fn read_ring_to_vec_<R>(reader: &mut R) -> Vec<u8>
    where
        R: TrBuffRead<u8>,
    {
        let mut out = Vec::new();
        loop {
            let demand = Demand::no_more_than(4096usize);
            let mut res = reader.read_async(&demand).await;
            if let Option::Some(segm) = res.as_mut().pick_left() {
                let take = segm.least_count();
                let mut chunk = vec![0u8; take];
                let moved = segm.move_items_to_as_buff(chunk.as_mut_slice());
                out.extend_from_slice(&chunk[..moved]);
                continue;
            }
            let err = res.pick_right().expect("既不含左值，则必含右值");
            if err.err_tag() == ReadErrTag::Closing {
                return out;
            }
            panic!("读 ring 失败：{err}");
        }
    }

    /// 把一个值经栈上 ring 边编码边读出。
    fn encode_via_ring_<T>(value: &T) -> Vec<u8>
    where
        T: ?Sized + TrEncodable,
    {
        let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
        let (mut writer, mut reader) = BodyRing_::split(&mut ring);
        let encode = async {
            let result = encode_into_async_(value, &mut writer, NonCancellableToken::new()).await;
            writer.close();
            result
        };
        let read = async { read_ring_to_vec_(&mut reader).await };
        let (encoded, bytes) = block_on(join(encode, read));
        encoded.expect("编码应当成功");
        bytes
    }

    /// 测试 ring 的写 / 读两半在同一个 task 内能并发推进。
    /// Test that the ring writer / reader halves make progress in one task.
    ///
    /// - 手段：写端借 3 字节、填入 `abc`、关闭；读端循环读到 EOF。
    /// - 判断：读端拿到 `abc`，join 能结束。
    ///
    /// - Means: write `abc` into the ring, close it, and read until EOF.
    /// - Verdict: the reader receives `abc` and the join completes.
    #[test]
    fn ring_join_smoke_() {
        let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
        let (mut writer, mut reader) = BodyRing_::split(&mut ring);
        let write = async {
            let demand = Demand::exactly(3usize);
            let res = writer.write_async(&demand).await;
            let mut segm = res.pick_left().expect("写端应能借出写段");
            assert_eq!(segm.move_items_from_as_buff(&b"abc"[..]), 3usize);
            drop(segm);
            writer.close();
        };
        let read = async { read_ring_to_vec_(&mut reader).await };
        let ((), bytes) = block_on(join(write, read));
        assert_eq!(bytes, b"abc");
    }

    /// 测试业务体经 ring 编码后能解码还原原值。
    /// Test that a body encoded through the ring decodes back to the original value.
    ///
    /// - 手段：用 `String` 走 ring 编码，再走 `decode_slice_async_`。
    /// - 判断：解出的字符串与原值完全相等。
    ///
    /// - Means: encode a `String` through the ring and decode the bytes.
    /// - Verdict: the decoded value equals the original.
    #[test]
    fn body_roundtrip_preserves_value_() {
        let original = "workspace-alpha".to_string();
        let bytes = encode_via_ring_(&original);
        let decoded: String = block_on(decode_slice_async_(&bytes)).expect("解码应当成功");

        assert_eq!(decoded, original);
    }

    /// 测试远大于原先 64 KiB 暂存区的业务体能流式往返。
    /// Test that a body far larger than the old 64 KiB staging buffer round-trips.
    ///
    /// - 手段：构造 200 KiB 字符串，经 ring 编码 / 读出，再解码。
    /// - 判断：字节数超过 64 KiB，解出的内容与原值完全一致。
    ///
    /// - Means: stream a 200 KiB string through the ring and decode it.
    /// - Verdict: the wire bytes exceed 64 KiB and the value is preserved.
    #[test]
    fn large_body_streams_through_ring_() {
        let original = "x".repeat(200usize * 1024usize);
        let bytes = encode_via_ring_(&original);
        assert!(bytes.len() > 64usize * 1024usize, "编码结果应超过 64 KiB");
        let decoded: String = block_on(decode_slice_async_(&bytes)).expect("解码应当成功");
        assert_eq!(decoded, original);
    }

    /// 测试错误体写入不经过 std IO 适配层也能完整落到目标。
    /// Test that error-body writes reach the sink without the std IO adapters.
    ///
    /// - 手段：把 `error-body` 写进栈上 ring，读端读到 EOF。
    /// - 判断：写入字节数与内容都与源一致。
    ///
    /// - Means: write `error-body` into a stack ring and read until EOF.
    /// - Verdict: the byte count and contents match the source.
    #[test]
    fn writes_error_body_without_std_adapter_() {
        let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
        let (mut writer, mut reader) = BodyRing_::split(&mut ring);
        let write = async {
            let result =
                write_body_async_(&mut writer, b"error-body", NonCancellableToken::new()).await;
            writer.close();
            result
        };
        let read = async { read_ring_to_vec_(&mut reader).await };
        let (written, bytes) = block_on(join(write, read));

        assert_eq!(written.expect("写错误体应当成功"), b"error-body".len());
        assert_eq!(bytes, b"error-body");
    }

    /// 测试错误体读取不经过 std IO 适配层也能按声明长度取出。
    /// Test that error-body reads use the declared length without std IO adapters.
    ///
    /// - 手段：把 `error-body` 包成 `&[u8]` 读视图，按精确长度读取。
    /// - 判断：读出的字节与源完全一致；声明长度大于源时返回 `Transport`。
    ///
    /// - Means: read `error-body` through a `&[u8]` view with its exact length, then with
    ///   an oversized length.
    /// - Verdict: the exact read matches the source; the oversized read returns
    ///   `Transport`.
    #[test]
    fn reads_error_body_without_std_adapter_() {
        let data = b"error-body";
        let mut src: &[u8] = data;
        let got = block_on(read_body_async_(
            &mut src,
            data.len(),
            NonCancellableToken::new(),
        ))
        .expect("按精确长度读错误体应当成功");
        assert_eq!(got, data);

        let mut src: &[u8] = data;
        let err = block_on(read_body_async_(
            &mut src,
            data.len() + 4usize,
            NonCancellableToken::new(),
        ))
        .expect_err("声明长度超过源时应当失败");
        assert!(matches!(err, RpcError::Transport(_)));
    }

    /// 测试定长 / 分块两种读视图都能直接解码出 owned 值。
    /// Test that both fixed-size and chunked read views decode directly.
    ///
    /// - 手段：先把 `String` 用 ring 编码；定长视图直接解，分块视图手工加块前缀后再解。
    /// - 判断：两种视图都还原出原字符串。
    ///
    /// - Means: encode a string through the ring, then decode it through a limited view
    ///   and through a manually chunked view.
    /// - Verdict: both views recover the original string.
    #[test]
    fn decodes_fixed_and_chunked_views_() {
        let original = "streamed-body".to_string();
        let bytes = encode_via_ring_(&original);

        let decoded: String = block_on(decode_body_from_async_(
            &mut bytes.as_slice(),
            false,
            bytes.len(),
            NonCancellableToken::new(),
        ))
        .expect("定长视图解码应当成功");
        assert_eq!(decoded, original);

        let mut chunked = Vec::new();
        chunked.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        chunked.extend_from_slice(&bytes);
        chunked.extend_from_slice(&0u16.to_be_bytes());

        let decoded: String = block_on(decode_body_from_async_(
            &mut chunked.as_slice(),
            true,
            0usize,
            NonCancellableToken::new(),
        ))
        .expect("分块视图解码应当成功");
        assert_eq!(decoded, original);
    }
}
