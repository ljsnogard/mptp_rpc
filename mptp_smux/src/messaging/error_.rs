//! MPTP 前缀线格式的错误类型。
//!
//! 本模块的错误只覆盖**前缀**的编码、解码与线上取值识别；报文体的解码失败属于使用者
//! 自己的序列化格式，不会出现在这里。
//!
//! # `error_`: errors of the prefix wire format
//!
//! These errors cover **prefix** encoding, decoding and wire-value recognition only. Body
//! decoding belongs to the user's own serialization format and never appears here.

use thiserror::Error;

/// 编码或解码 MPTP 报文前缀时出现的错误。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::MessagingError;
///
/// let err = MessagingError::Unknown("不认识的方法位 0x7f".to_string());
/// assert!(err.to_string().contains("线上取值不可识别"));
/// ```
#[derive(Debug, Error)]
pub enum MessagingError {
    /// 把前缀写进发送半边时失败（含等待期间被取消）。
    ///
    /// # 英文
    ///
    /// Failed to encode the prefix into the write half (including cancellation while
    /// waiting).
    #[error("编码报文前缀失败：{0}")]
    Encode(String),

    /// 从接收半边解出前缀时失败：不是合法的 MessagePack、对端提前关闭，或读 ring 失败。
    ///
    /// # 英文
    ///
    /// Failed to decode the prefix from the read half: malformed MessagePack, the peer
    /// closing early, or a ring read error.
    #[error("解码报文前缀失败：{0}")]
    Decode(String),

    /// 线上取值不在本端认识的集合里。
    ///
    /// 访问方法是**线上契约**：不认识的方法位就是两端不一致，不能猜一个近似的顶上。状态码
    /// 则是开放数值（[`Status`](abs_mptp::spec::Status) 可以表示任意 `u16`），因此不会走到
    /// 这个变体。
    ///
    /// # 英文
    ///
    /// A wire value is outside the set this side recognises. Access methods are a wire
    /// contract and an unknown bit is never guessed; status codes are open numeric values
    /// and therefore never fail here.
    #[error("线上取值不可识别：{0}")]
    Unknown(String),
}
