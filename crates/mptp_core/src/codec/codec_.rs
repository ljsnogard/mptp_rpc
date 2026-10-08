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
use std::future::Future;

use abs_buff::{TrBuffRead, TrBuffWrite, x_deps::abs_cancel};
use abs_cancel::NonCancellableToken;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use super::registry_::{TrDecodeAsync, TrDecodeFn, TrEncodeAsync, TrEncodeFn};
use crate::{decode_::read_value_async_, encode_::write_all_async_};

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
            write_all_async_(target, &bytes, &NonCancellableToken::new())
                .await
                .map_err(|err| CodecError::Encode(err.to_string()))?;
            Result::Ok(size)
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
            match self {
                Codec::MsgPack => {
                    // 与解码请求 / 响应前缀走同一条异步读取路径：没有 `block_on`，
                    // 因此不会在 `LocalSet` 驱动的连接上死锁。
                    let mut buf: Vec<u8> = Vec::new();
                    let mut consumed = 0usize;
                    let value: T = read_value_async_(
                        source,
                        &mut buf,
                        &mut consumed,
                        &NonCancellableToken::new(),
                    )
                    .await
                    .map_err(|err| CodecError::Decode(err.to_string()))?;
                    Result::Ok((value, consumed))
                }
                Codec::Json => todo!("Support JSON codec at the moment."),
            }
        })
    }
}
