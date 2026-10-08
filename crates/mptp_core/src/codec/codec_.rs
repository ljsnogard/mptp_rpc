//! 根据消息头（`Data_Type_Id`）选择 body 编解码器。
//!
//! MPTP 的报文头是二进制友好的 `HeaderKey` / `HeaderVal`，其中 `HeaderVal`
//! 既可以是数字（标准头值，如 `StdHeaderVal::Mime_Body_Type_MsgPack`），
//! 也可以是字符串（如 `"application/json"`）。本模块把这些头值映射到
//! 具体的 `BodyCodec`，供上层在读取/写入 body 前确定如何序列化。
//!
//! # 当前实现
//!
//! - `BodyCodec::MsgPack`：使用 `rmp-serde`（MessagePack），适合 MPTP 默认二进制场景；
//! - `BodyCodec::Json`：使用 `serde_json`，便于调试和与外部 JSON 系统互操作。
//!
//! 后续可以继续增加 `Raw`、`CBOR` 等 codec，只要在 `CodecRegistry::lookup`
//! 中补充分支即可。

use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use buffex::x_deps::{abs_buff, abs_cancel::NonCancellableToken};

/// Body 编解码错误。
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("encode body failed: {0}")]
    Encode(String),

    #[error("decode body failed: {0}")]
    Decode(String),
}

/// 支持的 body 编解码器。
///
/// 使用枚举而不是 trait object，是为了让 `lookup` 返回一个轻量的 `Copy`
/// 值；同时 `encode` / `decode` 仍然保持泛型，方便上层直接对任意
/// `Serialize + DeserializeOwned` 类型操作。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Codec {
    /// MessagePack，对应 `StdHeaderVal::Mime_Body_Type_MsgPack`。
    MsgPack,
    /// JSON，对应 `StdHeaderVal::Mime_Body_Type_Json` 或字符串 `application/json`。
    Json,
}

impl Codec {
    /// 将 `value` 编码为 body 字节，并写入到输出流。
    ///
    /// `W` 只要实现 `abs_buff::TrBuffTryWrite` 即可。`RpcChannel` 的写半通道
    /// 和 `circular_buff` 的被动生产端都满足该 trait，因此 body codec 不依赖
    /// 底层具体是 `ring_buffer` 还是 `circular_buff`。
    pub fn encode_async<T, W>(
        &self,
        value: &T,
        write: &mut W,
    ) -> Result<(), CodecError>
    where
        T: Serialize,
        W: abs_buff::TrBuffTryWrite,
    {
        let Codec::MsgPack = &self else {
            todo!("Support msgpack only at the moment.")
        };
        let mut write = AsStdWrite::new(write, NonCancellableToken::shared_mut());
        rmp_serde::encode::write(&mut write, value)
            .map_err(|e| CodecError::Encode(e.to_string()))
    }

    /// 从输入流中解码出 `T`。
    ///
    /// `R` 只要实现 `abs_buff::TrBuffTryRead` 即可。与 [`Codec::encode_async`]
    /// 一样，底层缓冲实现被 `abs_buff` trait 屏蔽，`ring_buffer` → `circular_buff`
    /// 的重构不需要改动这里。
    pub fn decode_async<T, R>(&self, read: &mut R) -> Result<T, CodecError>
    where
        T: DeserializeOwned,
        R: abs_buff::TrBuffTryRead,
    {
        let Codec::MsgPack = &self else {
            todo!("Support msgpack only at the moment.")
        };
        let mut read = AsStdRead::new(read, NonCancellableToken::shared_mut());
        rmp_serde::decode::from_read(&mut read)
            .map_err(|e| CodecError::Decode(e.to_string()))
    }
}

pub type CodecAsync<T> = Box<dyn Future<Output = Result<T, CodecError>>>;
