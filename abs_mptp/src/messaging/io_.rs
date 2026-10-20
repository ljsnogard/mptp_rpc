//! 报文 IO 的公共地基：报文级错误类型，以及等待点统一的取消竞速约定。
//!
//! 这一层刻意不含任何缓冲：限长与分块两个视图都建立在
//! [`TrBuffRead`](abs_buff::TrBuffRead) / [`TrBuffWrite`](abs_buff::TrBuffWrite) 的段接口
//! 之上，本模块只提供它们共用的小工具。

use thiserror::Error;

/// 报文级 IO 的错误。
///
/// 它同时被限长视图（`limited::read_exact_into_async_`）与上层的报文体装配使用：
/// 凡是「协议层的读写在字节这一级失败」都归到这里。
#[derive(Debug, Error)]
pub enum MessageIoError {
    /// 报文编码失败。
    #[error("encode failed: {0}")]
    Encode(String),

    /// 报文解码失败。
    #[error("decode failed: {0}")]
    Decode(String),

    /// 读写底层缓冲失败（例如把字节写进 ring 时出错）。
    #[error("io failed: {0}")]
    Io(String),

    /// 报文头里的声明自相矛盾或无法解读。
    #[error("protocol violation: {0}")]
    Protocol(String),

    /// 对端在报文读完之前关闭了通道。
    ///
    /// 报文头已经声明了长度，流却在长度满足之前结束——这是**协议违规**，不能当成功。
    #[error("channel closed early: {0}")]
    Truncated(String),
}
