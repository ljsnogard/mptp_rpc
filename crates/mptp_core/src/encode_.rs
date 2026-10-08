//! 把一段字节**完整**写进 `abs_buff` 的写半边。
//!
//! # 为什么要循环，而不是一次 `write_async`
//!
//! 旧版 `abs_buff` 的 `Demand::less_than(n)` 是「至多 n 个」，照它写就能一次拿到
//! 足够空间；新版把它收紧成**严格小于 n**（`no_more_than` 才是「至多 n 个」）。
//! 照旧写法申请空间会**少拿一个字节**，于是「一次写完」变成「悄悄少写一个字节」——
//! 帧的最后一个字节留在应用手里，对端永远等不到完整消息。
//!
//! 本原语按「至多剩多少」逐段申请、写多少算多少，直到写完为止，因此不依赖
//! 「一次能借到全部空间」这个假设。

use std::io;

use abs_buff::{Demand, TrBuffWrite, buffer::TrBuffSegmMut, x_deps::abs_cancel};
use abs_cancel::{TrCancellationToken, TrMayCancel};

/// 把 `bytes` 全部写进 `target`，写不动就等（等待可被 `cancel` 打断）。
///
/// # Errors
///
/// 目标拒绝给出任何空间、报告错误，或流已关闭时返回 [`io::Error`]。
pub(crate) async fn write_all_async_<W, C>(
    target: &mut W,
    bytes: &[u8],
    cancel: &C,
) -> Result<(), io::Error>
where
    W: TrBuffWrite<u8>,
    C: TrCancellationToken,
{
    let mut off = 0usize;
    while off < bytes.len() {
        let demand = Demand::no_more_than(bytes.len() - off);
        let mut outcome = target
            .write_async(&demand)
            .may_cancel_with(cancel.child_token())
            .await;
        if let Option::Some(segm) = outcome.as_mut().pick_left() {
            let mut child = segm.as_segm_mut();
            let take = core::cmp::min(child.least_count(), bytes.len() - off);
            let moved = child.clone_items_from_buff(&bytes[off..off + take]);
            if moved == 0usize {
                return Result::Err(io::Error::other("write half accepted no byte"));
            }
            off += moved;
            continue;
        }
        if let Option::Some(err) = outcome.pick_right() {
            return Result::Err(io::Error::other(err.to_string()));
        }
        return Result::Err(io::Error::other("write half is closed"));
    }
    Result::Ok(())
}
