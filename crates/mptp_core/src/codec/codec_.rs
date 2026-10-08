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
//! # 为什么两边都不走 `std::io` 适配器
//!
//! `rmp_serde` 的编解码入口要的是 `std::io::{Read, Write}`，而 `abs_buff` 的段接口是
//! 异步的。`abs_buff_stdio_adapt` 的 `AsStdWrite` / `AsStdRead` 能把两者接起来，但它们是
//! **同步**的——内部靠 `block_on` 等数据。那在 `smux_v1` 的用法下会死锁：连接的读 / 写
//! 循环被投递到**本线程**（tokio 下即 `LocalSet`），而 `block_on` 把当前线程占住之后，
//! 本地队列再也不会被驱动。
//!
//! 因此两侧都用真正的异步路径：
//!
//! - 编码：`rmp_serde::to_vec` 先在自己内存里算出完整字节（顺带得到 `Body_Size` 需要的
//!   长度），再用 [`write_all_async_`] 分块异步落进目标环；
//! - 解码：走 [`read_value_async_`](crate::decode_::read_value_async_)，它逐字节异步读出
//!   恰好一条 MessagePack 值，并回报消耗的字节数。

use core::{any::Any, pin::Pin};
use std::{future::Future, io};

use abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::TrProducerState,
    x_deps::abs_cancel,
};
use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use abs_cancel::{NonCancellableToken, TrCancellationToken};
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
    W: TrBuffWrite<u8> + TrProducerState + 'static,
{
    fn encode_async<'a>(&'a self, data: &'a T, target: &'a mut W) -> CodecAsync<'a, usize>
    where
        Self: 'a,
        T: 'a,
        W: 'a,
    {
        Box::pin(async move {
            // `AsStdWrite` 把这条写半边暴露成 `std::io::Write`，`rmp_serde` 于是可以直接
            // 往环里编码；外面再套一层计数，好把 `Body_Size` 需要的长度报回去。
            let mut write = CountingWriter::new_(AsStdWrite::new(
                target,
                NonCancellableToken::new(),
            ));
            match self {
                Codec::MsgPack => rmp_serde::encode::write(&mut write, data)
                    .map_err(|err| CodecError::Encode(err.to_string()))?,
                Codec::Json => todo!("Support JSON codec at the moment."),
            }
            Result::Ok(write.written_())
        })
    }
}

impl<T, R> TrDecodeFn<T, R> for Codec
where
    T: DeserializeOwned + 'static,
    R: TrBuffRead<u8> + 'static,
{
    fn decode_async<'a>(&'a self, source: &'a mut R) -> CodecAsync<'a, (T, usize)>
    where
        Self: 'a,
        T: 'a,
        R: 'a,
    {
        Box::pin(async move {
            // 同编码侧：`AsStdRead` 提供 `std::io::Read`，计数层回报消耗了多少字节。
            let mut read = CountingReader::new_(AsStdRead::new(source, NonCancellableToken::new()));
            let value = match self {
                Codec::MsgPack => rmp_serde::from_read::<_, T>(&mut read)
                    .map_err(|err| CodecError::Decode(err.to_string()))?,
                Codec::Json => todo!("Support JSON codec at the moment."),
            };
            Result::Ok((value, read.read_count_()))
        })
    }
}

/// 给 [`AsStdWrite`] 套一层写计数：编码器要回报写了多少字节。
struct CountingWriter<'a, W, C>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    inner_: AsStdWrite<'a, W, C>,
    written_: usize,
}

impl<'a, W, C> CountingWriter<'a, W, C>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    fn new_(inner: AsStdWrite<'a, W, C>) -> Self {
        CountingWriter {
            inner_: inner,
            written_: 0usize,
        }
    }

    const fn written_(&self) -> usize {
        self.written_
    }
}

impl<W, C> io::Write for CountingWriter<'_, W, C>
where
    W: TrBuffWrite + TrProducerState,
    C: TrCancellationToken,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let written = self.inner_.write(buf)?;
        self.written_ += written;
        Result::Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner_.flush()
    }
}

/// 给 [`AsStdRead`] 套一层读计数：解码器要回报消耗了多少字节。
struct CountingReader<'a, R, C>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    inner_: AsStdRead<'a, R, C>,
    read_: usize,
}

impl<'a, R, C> CountingReader<'a, R, C>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    fn new_(inner: AsStdRead<'a, R, C>) -> Self {
        CountingReader {
            inner_: inner,
            read_: 0usize,
        }
    }

    const fn read_count_(&self) -> usize {
        self.read_
    }
}

impl<R, C> io::Read for CountingReader<'_, R, C>
where
    R: TrBuffRead<u8>,
    C: TrCancellationToken,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = io::Read::read(&mut self.inner_, buf)?;
        self.read_ += read;
        Result::Ok(read)
    }
}
