//! 内置的 body 编码格式。

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
