//! 根据消息头（`Body_Type` / `Content-Type`）选择 body 编解码器。
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

use crate::specs::{HeaderVal, StdHeaderVal};

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
pub enum BodyCodec {
    /// MessagePack，对应 `StdHeaderVal::Mime_Body_Type_MsgPack`。
    MsgPack,
    /// JSON，对应 `StdHeaderVal::Mime_Body_Type_Json` 或字符串 `application/json`。
    Json,
}

impl BodyCodec {
    /// 将 `value` 编码为 body 字节。
    pub fn encode<T>(&self, value: &T) -> Result<Vec<u8>, CodecError>
    where
        T: Serialize,
    {
        match self {
            BodyCodec::MsgPack => {
                rmp_serde::encode::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
            }
            BodyCodec::Json => {
                serde_json::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
            }
        }
    }

    /// 从 body 字节解码出 `T`。
    pub fn decode<T>(&self, data: &[u8]) -> Result<T, CodecError>
    where
        T: DeserializeOwned,
    {
        match self {
            BodyCodec::MsgPack => {
                rmp_serde::decode::from_slice(data).map_err(|e| CodecError::Decode(e.to_string()))
            }
            BodyCodec::Json => {
                serde_json::from_slice(data).map_err(|e| CodecError::Decode(e.to_string()))
            }
        }
    }
}

/// 根据 `HeaderVal` 查找 `BodyCodec` 的注册表。
///
/// 这是一个很小的“策略查找”抽象：调用方只需要持有 `HeaderVal`，不必在业务
/// 代码里到处写 `if header == MsgPack { ... }`。
#[derive(Clone, Copy, Debug, Default)]
pub struct CodecRegistry;

impl CodecRegistry {
    /// 创建一个默认注册表。
    ///
    /// 目前是零大小结构体；后续如果需要注册自定义 codec，可以改为持有表项。
    pub const fn new() -> Self {
        CodecRegistry
    }

    /// 根据头值返回对应的 `BodyCodec`。
    ///
    /// 支持：
    /// - 数字标准头值：`StdHeaderVal::Mime_Body_Type_MsgPack` / `Json`；
    /// - 字符串头值：`"application/msgpack"` / `"application/json"`（大小写不敏感）。
    pub fn lookup(&self, header: &HeaderVal) -> Option<BodyCodec> {
        // 数字形态：标准 MIME 类型值。
        if let Ok(val) = header.try_as_header_val() {
            return match val {
                StdHeaderVal::Mime_Body_Type_MsgPack => Some(BodyCodec::MsgPack),
                StdHeaderVal::Mime_Body_Type_Json => Some(BodyCodec::Json),
                _ => None,
            };
        }

        // 字符串形态：常见的 MIME 字符串。
        if let Ok(s) = header.try_as_str() {
            let lower = s.to_ascii_lowercase();
            if lower.contains("msgpack") || lower == "application/octet-stream" {
                return Some(BodyCodec::MsgPack);
            }
            if lower.contains("json") {
                return Some(BodyCodec::Json);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests_ {
    use serde::Deserialize;

    use super::*;
    use crate::specs::{HeaderVal, StdHeaderVal};

    #[test]
    fn lookup_standard_values() {
        let registry = CodecRegistry::new();
        assert_eq!(
            registry.lookup(&HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack)),
            Some(BodyCodec::MsgPack)
        );
        assert_eq!(
            registry.lookup(&HeaderVal::from(StdHeaderVal::Mime_Body_Type_Json)),
            Some(BodyCodec::Json)
        );
    }

    #[test]
    fn msgpack_roundtrip() {
        #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
        struct Item {
            id: u32,
            name: String,
        }

        let item = Item {
            id: 7,
            name: "mptp".to_string(),
        };
        let bytes = BodyCodec::MsgPack.encode(&item).unwrap();
        let decoded: Item = BodyCodec::MsgPack.decode(&bytes).unwrap();
        assert_eq!(decoded, item);
    }

    #[test]
    fn json_roundtrip() {
        let value = serde_json::json!({ "ok": true, "n": 42 });
        let bytes = BodyCodec::Json.encode(&value).unwrap();
        let decoded: serde_json::Value = BodyCodec::Json.decode(&bytes).unwrap();
        assert_eq!(decoded, value);
    }
}
