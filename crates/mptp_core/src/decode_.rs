//! 从 `abs_buff` 的读半边里**异步**读出一条 MessagePack 值。
//!
//! # 为什么不能用 `std::io::Read` 那条路
//!
//! `abs_buff_stdio_adapt::AsStdRead` 把读半边包成 `std::io::Read`，而 `io::Read`
//! 是同步接口——它内部只能 `block_on` 去等数据。这在 `smux_v1` 的用法下是**死锁**：
//! 连接的读 / 写循环是经 `abs_art::TrLocalScope` **投递到本线程**的（tokio 下即
//! `LocalSet`），而 `block_on` 会把当前线程占住，本地队列因此再也不会被驱动，
//! 于是「等环里有数据」等的是一个永远没人喂的环。
//!
//! MessagePack 是自描述格式，逐字节读、每读一个就试着解析一次，既保持全异步，
//! 又不需要预先知道前缀有多长。
//!
//! # 代价
//!
//! 每条前缀值要「读 1 字节 + 重新解析一次」，因此是 O(n²) 的。MPTP 的前缀只有
//! 几十字节，这个量级可以接受。真要优化时，方向是用 `abs_buff` 的 peek 能力
//! （`TrBuffPeek`）先窥视整段、解析出长度后再一次性消费——那需要先确认 peek 在
//! 各实现上的一致语义。

use core::mem::MaybeUninit;
use std::io;

use abs_buff::{Demand, TrBuffRead, buffer::TrBuffSegmRef, x_deps::abs_cancel};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use serde::de::DeserializeOwned;

/// 单条前缀值的字节上限：超过它就不再继续读，直接报错。
///
/// 它防的是「对端发来的根本不是 MessagePack」这类情形——没有上限就会一直读到
/// 连接被拆掉为止。
pub(crate) const K_MAX_PREFIX_VALUE_LEN: usize = 64usize * 1024usize;

/// 从 `rx` 读出恰好一条 MessagePack 值。
///
/// `buf` / `consumed` 是调用方持有的**跨值累积状态**：连续读多个值时复用同一份缓冲，
/// `consumed` 指向「已经被前面那些值消费掉的字节数」，因此新值从 `buf[consumed..]`
/// 开始解析。返回值是该值占用的字节数（已累加进 `consumed`）。
///
/// # Errors
///
/// 读半边报错、流被关闭，或单条值超过 [`K_MAX_PREFIX_VALUE_LEN`] 时返回错误。
pub(crate) async fn read_value_async_<T, R, C>(
    rx: &mut R,
    buf: &mut Vec<u8>,
    consumed: &mut usize,
    cancel: &C,
) -> Result<T, io::Error>
where
    T: DeserializeOwned,
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    loop {
        // 先看「已经读到、但还没被更早的值消费掉」的字节够不够解析。
        if let Option::Some((used, value)) = try_parse_::<T>(buf, *consumed) {
            *consumed += used;
            return Result::Ok(value);
        }
        if buf.len() - *consumed >= K_MAX_PREFIX_VALUE_LEN {
            return Result::Err(io::Error::other(
                "message prefix value exceeds the configured limit",
            ));
        }
        // 再多读一个字节。段的余量留在环里：`move_items_to_buff` 只推进被搬走的那部分。
        let byte = read_one_async_(rx, cancel).await?;
        buf.push(byte);
    }
}

/// 试着从 `buf[consumed..]` 解析一条值。
///
/// 返回 `Some((占用字节数, 值))`；字节还不够（或不是合法编码）时返回 `None`，
/// 由调用方继续多读一个字节再试。
fn try_parse_<T>(buf: &[u8], consumed: usize) -> Option<(usize, T)>
where
    T: DeserializeOwned,
{
    if buf.len() == consumed {
        return Option::None;
    }
    let rest = &buf[consumed..];
    let mut cursor: &[u8] = rest;
    let mut deserializer = rmp_serde::Deserializer::new(&mut cursor);
    let value = T::deserialize(&mut deserializer).ok()?;
    let used = rest.len() - cursor.len();
    Option::Some((used, value))
}

/// 从 `rx` 读走**恰好一个**字节。
async fn read_one_async_<R, C>(rx: &mut R, cancel: &C) -> Result<u8, io::Error>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    let demand = Demand::at_least(1usize);
    let mut outcome = rx
        .read_async(&demand)
        .may_cancel_with(cancel.child_token())
        .await;
    if let Option::Some(segm) = outcome.as_mut().pick_left() {
        let mut child = segm.as_segm_ref();
        let mut one = [MaybeUninit::<u8>::uninit(); 1];
        // SAFETY: `one` 是本地独占的一格可写内存；`move_items_to_buff` 至多写入其中
        // 已初始化的前缀并返回写入长度，因此下面的 `assume_init` 只在 `moved == 1`
        // 时执行，读到的必定是已初始化的字节。
        let moved = unsafe { child.move_items_to_buff(&mut one) };
        if moved == 0usize {
            return Result::Err(io::Error::other("read half yielded no byte"));
        }
        return Result::Ok(unsafe { one[0].assume_init() });
    }
    if let Option::Some(err) = outcome.pick_right() {
        return Result::Err(io::Error::other(err.to_string()));
    }
    Result::Err(io::Error::other("read half is closed"))
}
