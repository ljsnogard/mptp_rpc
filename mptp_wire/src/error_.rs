//! MPTP 前缀线格式的具体错误类型。
//!
//! 本模块不依赖任何传输层类型；编解码错误直接保留 `mp_denco` 的 [`MpError`]，
//! 未知方法位保留原始数值，不再把信息拼成字符串后丢弃结构。

use mp_denco::MpError;
use thiserror::Error;

/// 编码或解码 MPTP 报文前缀时出现的错误。
///
/// # Examples
///
/// ```
/// use mptp_wire::MessagingError;
///
/// let err = MessagingError::UnknownMethod { code: 0x7f };
/// assert!(err.to_string().contains("0x7f"));
/// ```
#[derive(Debug, Error)]
pub enum MessagingError {
    /// 把前缀写进发送半边时失败（含等待期间被取消）。
    ///
    /// 具体格式 / 缓冲错误保留在 [`MpError`] 中。
    #[error("编码报文前缀失败：{0}")]
    Encode(MpError),

    /// 从接收半边解出前缀时失败：不是合法的 MessagePack、对端提前关闭，或读 ring 失败。
    #[error("解码报文前缀失败：{0}")]
    Decode(MpError),

    /// 线上方法位不在本端认识的集合里。
    ///
    /// 访问方法是**线上契约**：不认识的方法位就是两端不一致，不能猜一个近似的顶上。
    /// 状态码则是开放数值，因此不会走到这个变体。
    #[error("不认识的访问方法位：{code:#04x}")]
    UnknownMethod {
        /// 从线上读到的原始方法位。
        code: u8,
    },
}
