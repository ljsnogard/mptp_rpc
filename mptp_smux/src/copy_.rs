//! 段到段的搬运原语：定长体、后缀流、丢弃体、读到 EOF。
//!
//! 这些函数只依赖 [`abs_buff`] 的段接口，不依赖 smux 的具体类型；客户端与服务端
//! 共用。错误用 [`CopyError`] 表达，由调用方映射成「请求体 / 回复体 / 后缀」的具体
//! 协议错误。
//!
//! # `copy_`: segment-to-segment body copy primitives
//!
//! These helpers depend only on the `abs_buff` segment interfaces. The client and the
//! server share them; callers map [`CopyError`] into request / response / suffix
//! protocol errors.

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrBuffSegmRef, TrBuffSegmView, TrConsumerState, TrProducerState},
    error::{ReadErrTag, TrTaggedError},
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use anylr::{SomeOf, some_of::SomeLR};
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::limited::LimitedWriter;

use alloc::string::ToString;

use crate::{
    error_::ProtocolError,
    x_deps::{abs_buff, abs_cancel, anylr, gen_mcf2},
};

/// 搬运过程中的局部错误；调用方负责映射到方向性的协议错误。
#[derive(Debug)]
pub(crate) enum CopyError {
    /// 调用方取消。
    Cancelled,

    /// 源在搬够声明长度之前结束。
    Truncated {
        /// 声明的长度。
        declared: usize,
        /// 已搬走的字节数。
        read: usize,
    },

    /// 读源失败。
    Read {
        /// 底层错误的文本描述。
        detail: alloc::string::String,
    },

    /// 写目标失败。
    Write {
        /// 底层错误的文本描述。
        detail: alloc::string::String,
    },

    /// 源或目标让出的段是空的，无法推进。
    EmptySegment,
}

/// 把 `SomeOf` 的左值优先语义转成 `Result`，避免在库代码里使用 `expect`。
fn pick_left_<L, R>(value: SomeOf<L, R>) -> Result<L, R> {
    match value.into_inner() {
        SomeLR::Left(left) | SomeLR::Both(left, _) => Result::Ok(left),
        SomeLR::Right(right) => Result::Err(right),
    }
}

/// 定长搬运：只搬 `size` 字节。
///
/// 目标被套成 [`LimitedWriter`]：额度一旦用尽，多写的字节会被挡下，而不是溢到后续
/// 报文的位置上。源在搬完之前结束是**截断**——头里声明的长度必须被如实满足。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(CopySized, pub(crate), new(pub(crate)))]
pub(crate) async fn copy_sized_async_<'f, R, W, K>(
    src: &'f mut R,
    dst: &'f mut W,
    size: usize,
    cancel: K,
) -> Result<usize, CopyError>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
    K: TrCancellationToken,
{
    let mut limited = LimitedWriter::new(dst, size);
    let mut total = 0usize;
    while total < size {
        if cancel.is_cancelled() {
            return Result::Err(CopyError::Cancelled);
        }
        let demand = Demand::no_more_than(size - total);
        let res = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let mut segm = match pick_left_(res) {
            Result::Ok(segm) => segm,
            Result::Err(err) => {
                if matches!(err.err_tag(), ReadErrTag::Closing) {
                    return Result::Err(CopyError::Truncated {
                        declared: size,
                        read: total,
                    });
                }
                return Result::Err(CopyError::Read {
                    detail: err.to_string(),
                });
            }
        };
        let want = segm
            .iter_slices()
            .into_iter()
            .map(<[u8]>::len)
            .sum::<usize>();
        if want == 0usize {
            return Result::Err(CopyError::EmptySegment);
        }

        let write_demand = Demand::no_more_than(want);
        let wres = limited
            .write_async(&write_demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let mut target = match pick_left_(wres) {
            Result::Ok(target) => target,
            Result::Err(err) => {
                return Result::Err(CopyError::Write {
                    detail: err.to_string(),
                });
            }
        };
        let moved = {
            let mut source = segm.as_segm_ref();
            let mut target_child = target.as_segm_mut();
            source.move_items_to_segm(&mut target_child)
        };
        if moved == 0usize {
            return Result::Err(CopyError::EmptySegment);
        }
        // 目标段在这里回收：写入量随之结算到 `LimitedWriter` 的额度上。
        drop(target);
        total += moved;
    }
    Result::Ok(total)
}

/// 把源里的字节原样搬到目标，直到源结束（`Closing`）；返回搬走的字节数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(CopyAll, pub(crate), new(pub(crate)))]
pub(crate) async fn copy_all_async_<'f, R, W, K>(
    src: &'f mut R,
    dst: &'f mut W,
    cancel: K,
) -> Result<usize, CopyError>
where
    R: TrBuffRead<u8> + TrConsumerState,
    W: TrBuffWrite<u8> + TrProducerState,
    K: TrCancellationToken,
{
    // 额度用不完：这条流没有长度声明，读到源结束为止。
    let mut limited = LimitedWriter::new(dst, usize::MAX);
    let mut total = 0usize;
    loop {
        if cancel.is_cancelled() {
            return Result::Err(CopyError::Cancelled);
        }
        let demand = Demand::at_least(1usize);
        let res = src
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let mut segm = match pick_left_(res) {
            Result::Ok(segm) => segm,
            Result::Err(err) => {
                if matches!(err.err_tag(), ReadErrTag::Closing) {
                    return Result::Ok(total);
                }
                return Result::Err(CopyError::Read {
                    detail: err.to_string(),
                });
            }
        };
        let want = segm
            .iter_slices()
            .into_iter()
            .map(<[u8]>::len)
            .sum::<usize>();
        if want == 0usize {
            continue;
        }

        let write_demand = Demand::no_more_than(want);
        let wres = limited
            .write_async(&write_demand)
            .may_cancel_with(cancel.child_token())
            .await;
        let mut target = match pick_left_(wres) {
            Result::Ok(target) => target,
            Result::Err(err) => {
                return Result::Err(CopyError::Write {
                    detail: err.to_string(),
                });
            }
        };
        let moved = {
            let mut source = segm.as_segm_ref();
            let mut target_child = target.as_segm_mut();
            source.move_items_to_segm(&mut target_child)
        };
        if moved == 0usize {
            return Result::Err(CopyError::EmptySegment);
        }
        drop(target);
        total += moved;
    }
}

/// 读完一个读口的剩余内容并丢弃，返回丢弃的字节数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DrainToEnd, pub(crate), new(pub(crate)))]
pub(crate) async fn drain_to_end_async_<'f, TyRx, TyTok>(
    reader: &'f mut TyRx,
    cancel: TyTok,
) -> Result<usize, ProtocolError>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let mut dropped = 0usize;
    let demand = Demand::exactly(4096usize);
    loop {
        if cancel.is_cancelled() {
            return Result::Err(ProtocolError::DrainBody {
                detail: "调用方取消".into(),
            });
        }
        let some_of = reader
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        match some_of.into_inner() {
            SomeLR::Left(segm) | SomeLR::Both(segm, _) => {
                dropped += segm.iter_slices().into_iter().map(<[u8]>::len).sum::<usize>();
            }
            SomeLR::Right(err) => {
                if matches!(err.err_tag(), ReadErrTag::Closing) {
                    return Result::Ok(dropped);
                }
                return Result::Err(ProtocolError::DrainBody {
                    detail: err.to_string(),
                });
            }
        }
    }
}

/// 读到对端关闭为止，顺带丢弃途中收到的多余字节。
#[cfg(feature = "server")]
///
/// 收尾阶段的任何读错误都**不**上报：这一步的职责是「把这条子流读干净」，而它失败
/// 与否已经不影响本端要发的响应。取消时也直接返回已丢弃的字节数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DrainToEof, pub(crate), new(pub(crate)))]
pub(crate) async fn drain_to_eof_async_<'f, TyRx, TyTok>(
    rx: &'f mut TyRx,
    cancel: TyTok,
) -> usize
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    use core::mem::MaybeUninit;

    let mut dropped = 0usize;
    let demand = Demand::exactly(4096usize);
    // 借出的段必须**真正搬走**才会推进消费量；只 `iter_slices()` 看一眼就 drop
    // 会把同一段永远读回来（空转）。收尾时数据已经没人要，搬进栈上暂存再丢弃。
    let mut sink = [MaybeUninit::<u8>::uninit(); 4096usize];
    loop {
        if cancel.is_cancelled() {
            return dropped;
        }
        let some_of = rx
            .read_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        match some_of.into_inner() {
            SomeLR::Left(mut segm) | SomeLR::Both(mut segm, _) => {
                while segm.least_count() > 0usize {
                    let moved = segm.move_items_to_buff(&mut sink);
                    if moved == 0usize {
                        break;
                    }
                    dropped += moved;
                }
            }
            SomeLR::Right(_) => return dropped,
        }
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use abs_cancel::NonCancellableToken;

    /// 测试定长搬运恰好搬走声明字节数。
    /// Test that sized copy moves exactly the declared number of bytes.
    ///
    /// - 手段：源 4 字节、目标 8 字节，调用 `copy_sized_async_(4)`。
    /// - 判断：返回 4，目标前 4 字节与源一致。
    ///
    /// - Means: a 4-byte source and an 8-byte destination; call `copy_sized_async_(4)`.
    /// - Verdict: it returns 4 and the first four destination bytes equal the source.
    #[tokio::test]
    async fn copy_sized_moves_exact_bytes_() {
        let src_bytes = [1u8, 2, 3, 4];
        let mut src: &[u8] = &src_bytes;
        let mut dst_buf = [0u8; 8];
        let mut dst: &mut [u8] = &mut dst_buf;

        let moved = copy_sized_async_(&mut src, &mut dst, 4usize, NonCancellableToken::new())
            .await
            .expect("定长搬运应当成功");
        assert_eq!(moved, 4usize);
        assert_eq!(&dst_buf[..4], &src_bytes);
    }

    /// 测试源提前结束时定长搬运报截断。
    /// Test that a source ending early makes sized copy report truncation.
    ///
    /// - 手段：源只有 2 字节，声明 4 字节。
    /// - 判断：返回 `CopyError::Truncated { declared: 4, read: 2 }`。
    ///
    /// - Means: a 2-byte source with a declared size of 4.
    /// - Verdict: it returns `CopyError::Truncated { declared: 4, read: 2 }`.
    #[tokio::test]
    async fn copy_sized_reports_truncation_() {
        let src_bytes = [1u8, 2];
        let mut src: &[u8] = &src_bytes;
        let mut dst_buf = [0u8; 8];
        let mut dst: &mut [u8] = &mut dst_buf;

        let err = copy_sized_async_(&mut src, &mut dst, 4usize, NonCancellableToken::new())
            .await
            .expect_err("源不足应当报截断");
        assert!(matches!(
            err,
            CopyError::Truncated {
                declared: 4usize,
                read: 2usize
            }
        ));
    }

    /// 测试后缀搬运读到源结束为止。
    /// Test that suffix copy moves everything until the source ends.
    ///
    /// - 手段：源 3 字节、目标 8 字节，调用 `copy_all_async_`。
    /// - 判断：返回 3，目标前 3 字节与源一致。
    ///
    /// - Means: a 3-byte source and an 8-byte destination; call `copy_all_async_`.
    /// - Verdict: it returns 3 and the first three destination bytes equal the source.
    #[tokio::test]
    async fn copy_all_moves_until_source_end_() {
        let src_bytes = [9u8, 8, 7];
        let mut src: &[u8] = &src_bytes;
        let mut dst_buf = [0u8; 8];
        let mut dst: &mut [u8] = &mut dst_buf;

        let moved = copy_all_async_(&mut src, &mut dst, NonCancellableToken::new())
            .await
            .expect("后缀搬运应当成功");
        assert_eq!(moved, 3usize);
        assert_eq!(&dst_buf[..3], &src_bytes);
    }
}
