//! 内置的 body 编解码器，以及编解码错误与异步产物类型。
//!
//! 这里只放「与具体业务类型无关」的三样东西：
//!
//! - [`CodecError`]：编解码失败的错误；
//! - [`CodecAsync`]：编解码过程的异步产物（借用了调用方的数据与缓冲，因此带生命周期）；
//! - [`Codec`]：内置的编码格式（MessagePack / JSON）。它对任意
//!   `Serialize` / `DeserializeOwned` 的类型都可用，因此可以直接作为一个业务类型
//!   的编解码器注册进 [`CodecRegistry`](super::CodecRegistry)。
//!
//! # 为什么不复用 `AsStdWrite` 写完整个 body
//!
//! `rmp_serde` 的编码入口需要一个 `std::io::Write` 视图，而 `abs_buff` 的段接口是
//! 异步的。本模块的做法是：先用 `rmp_serde::to_vec` 在自己的内存里算出完整字节
//! （这一步顺带得到 `Body_Size` 需要的长度），再用 [`write_all_async_`] 分块异步落进
//! 目标环。这样目标侧只要求 [`TrBuffWrite`]，不必额外要求「能报告生产者状态」，
//! 写入等待也是真正异步的（而不是在 `AsStdWrite` 里 `block_on`）。
//!
//! 解码一侧无法这样做：`rmp_serde` 的自描述格式决定了「读到哪里算一条完整值」只有
//! 解析器自己知道，因此解码走 [`AsStdRead`]，并由 [`CountingReader`] 记账，好让解码器
//! 回报消耗的字节数。

use core::{any::Any, pin::Pin};
use std::{future::Future, io};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrConsumerState},
    x_deps::abs_cancel,
};
use abs_buff_stdio_adapt::{AsStdRead, x_deps::abs_buff};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::registry_::{TrDecodeAsync, TrDecodeFn, TrEncodeAsync, TrEncodeFn};

/// Body 编解码错误。
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("encode body failed: {0}")]
    Encode(String),

    #[error("decode body failed: {0}")]
    Decode(String),
}

/// 编解码过程返回的异步产物。
///
/// 它借用了调用方的数据 / 目标缓冲，因此带一个生命周期参数：调用点必须在这些借用
/// 仍然有效的同一个作用域里把它 `await` 掉，不能把它存进结构体或跨任务发送。
pub type CodecAsync<'a, T> = Pin<Box<dyn Future<Output = Result<T, CodecError>> + 'a>>;

/// 支持的 body 编解码器。
///
/// 使用枚举而不是 trait object，是为了让「选哪种格式」保持成一个轻量的 `Copy` 值：
/// 注册表里存的是**类型到编解码器**的映射，而格式的选择本身是配置项。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Codec {
    /// MessagePack，对应 `StdHeaderVal::Mime_Body_Type_MsgPack`。
    MsgPack,
    /// JSON，对应 `StdHeaderVal::Mime_Body_Type_Json` 或字符串 `application/json`。
    Json,
}

impl TrEncodeAsync for Codec {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TrDecodeAsync for Codec {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T, W> TrEncodeFn<T, W> for Codec
where
    T: Serialize + 'static,
    W: TrBuffWrite<u8> + 'static,
{
    fn encode_async<'a>(&'a self, data: &'a T, target: &'a mut W) -> CodecAsync<'a, usize>
    where
        Self: 'a,
        T: 'a,
        W: 'a,
    {
        Box::pin(async move {
            let bytes = match self {
                Codec::MsgPack => {
                    rmp_serde::to_vec(data).map_err(|e| CodecError::Encode(e.to_string()))?
                }
                Codec::Json => todo!("Support JSON codec at the moment."),
            };
            let size = bytes.len();
            write_all_async_(target, &bytes).await?;
            Result::Ok(size)
        })
    }
}

impl<T, R> TrDecodeFn<T, R> for Codec
where
    T: DeserializeOwned + 'static,
    R: TrBuffRead<u8> + TrConsumerState + 'static,
{
    fn decode_async<'a>(&'a self, source: &'a mut R) -> CodecAsync<'a, (T, usize)>
    where
        Self: 'a,
        T: 'a,
        R: 'a,
    {
        Box::pin(async move {
            let mut read = CountingReader::from_source_(source, NonCancellableToken::new());
            let value = match self {
                Codec::MsgPack => rmp_serde::from_read::<_, T>(&mut read)
                    .map_err(|e| CodecError::Decode(e.to_string()))?,
                Codec::Json => todo!("Support JSON codec at the moment."),
            };
            let consumed = read.read_count_();
            Result::Ok((value, consumed))
        })
    }
}

/// 把 `bytes` 全部写进 `target`，写不动就等（等待过程可被取消，这里用不可取消令牌）。
///
/// 每轮只向目标索要「剩余字节数」以内的空间，写多少算多少，直到写完。目标拒绝给出
/// 任何空间或报告错误时，如实返回错误——上层据此把这次会话判为失败。
async fn write_all_async_<W>(target: &mut W, bytes: &[u8]) -> Result<(), CodecError>
where
    W: TrBuffWrite<u8>,
{
    let mut off = 0usize;
    while off < bytes.len() {
        // `no_more_than` 是「至多 n 个」；`less_than` 在新版 `abs_buff` 里是**严格
        // 小于 n**，剩余 1 个字节时会构造出借不到任何空间的请求。
        let demand = Demand::no_more_than(bytes.len() - off);
        let mut outcome = target
            .write_async(&demand)
            .may_cancel_with(NonCancellableToken::new())
            .await;
        if let Option::Some(segm) = outcome.as_mut().pick_left() {
            let mut child = segm.as_segm_mut();
            let take = core::cmp::min(child.least_count(), bytes.len() - off);
            let moved = child.clone_items_from_buff(&bytes[off..off + take]);
            if moved == 0usize {
                return Result::Err(CodecError::Encode(
                    "write target accepted no byte".to_string(),
                ));
            }
            off += moved;
            continue;
        }
        if let Option::Some(err) = outcome.pick_right() {
            return Result::Err(CodecError::Encode(err.to_string()));
        }
        return Result::Err(CodecError::Encode("write target is closed".to_string()));
    }
    Result::Ok(())
}

/// 给 [`AsStdRead`] 套一层读计数：解码器要回报「消耗了多少字节」。
///
/// `rmp_serde` 解析完一条完整值就停下，不会预先多读，因此记账值就是该值的编码长度。
struct CountingReader<'a, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    inner_: AsStdRead<'a, R, C>,
    read_: usize,
}

impl<'a, R, C> CountingReader<'a, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    /// 由读缓冲与取消令牌构造，计数从零开始。
    fn from_source_(source: &'a mut R, cancel: C) -> Self {
        CountingReader {
            inner_: AsStdRead::new(source, cancel),
            read_: 0usize,
        }
    }

    /// 到目前为止被上层读走的字节数。
    const fn read_count_(&self) -> usize {
        self.read_
    }
}

impl<R, C> io::Read for CountingReader<'_, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = AsStdRead::read(&mut self.inner_, buf)?;
        self.read_ += read;
        Result::Ok(read)
    }
}
