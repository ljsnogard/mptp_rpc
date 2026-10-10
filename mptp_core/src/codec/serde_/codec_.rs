//! 内置的 body 编码格式。

use crate::specs::StdHeaderVal;

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

impl Codec {
    /// 本格式对应的 `Body_Type` 标准头取值。
    ///
    /// 报文头里的 `Body_Type` 由**编码格式**决定，不该让调用方再手写一遍：选了哪个
    /// 编解码器，对端就该按哪个格式解。
    pub const fn body_type_val(&self) -> StdHeaderVal {
        match self {
            Codec::MsgPack => StdHeaderVal::Mime_Body_Type_MsgPack,
            Codec::Json => StdHeaderVal::Mime_Body_Type_Json,
        }
    }

    /// 编解码器的名字，用于错误文案。
    pub const fn name(&self) -> &'static str {
        match self {
            Codec::MsgPack => "MessagePack",
            Codec::Json => "JSON",
        }
    }
}

impl Default for Codec {
    /// 缺省的编码格式：MessagePack。
    ///
    /// 只有调用方**没有明确指定**格式时才用它——指定了就用指定的那个。
    fn default() -> Self {
        Codec::MsgPack
    }
}
