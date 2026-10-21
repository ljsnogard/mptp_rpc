//! RPC 层统一错误与结果别名。
//!
//! 传输、编解码、远端状态与业务错误最终都收敛到 [`RpcError`]：客户端据此决定
//! 重试 / 上报，服务端据此选择线上状态码。业务错误通过 [`RpcError::Service`] 携带
//! 一个明确的 [`Status`]，而不是靠字符串猜。
//!
//! # `error_`: the unified RPC error
//!
//! Transport, codec, remote-status and business failures all collapse into [`RpcError`].
//! Business failures carry an explicit [`Status`] through [`RpcError::Service`].
//!
//! [`Status`]: crate::x_deps::abs_mptp::spec::Status

use core::fmt;

use crate::x_deps::abs_mptp::spec::Status;

/// 一次 RPC 调用的结果。
///
/// 这是协议端 service trait 方法的统一返回类型；宏据此把业务成功类型 `T`
/// 从 `RpcResult<T>` 中取出并生成客户端 / 服务端代码。
pub type RpcResult<T> = core::result::Result<T, RpcError>;

/// RPC 层统一错误。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::messaging::RpcError;
/// use rpc_core_v1::x_deps::abs_mptp::spec::Status;
///
/// let err = RpcError::Service {
///     status: Status::NotFound,
///     message: "goods 不存在".to_string(),
/// };
/// assert_eq!(err.status(), Status::NotFound);
/// ```
#[derive(Debug)]
pub enum RpcError {
    /// 传输层失败：建流、收发前缀或收发 body 失败。
    Transport(String),

    /// 请求 / 回复 body 编码失败。
    Encode(String),

    /// 请求 / 回复 body 解码失败。
    Decode(String),

    /// 远端返回了非成功状态；`body` 是远端给出的原始错误体。
    Remote {
        /// 远端状态码。
        status: Status,

        /// 远端错误体的原始字节。
        body: Vec<u8>,
    },

    /// 服务端业务实现主动返回的错误。
    ///
    /// 与 [`RpcError::Remote`] 的区别：`Service` 由**本端业务**产生，`status` 会被
    /// 直接写到回复前缀；`Remote` 是**对端**回复的错误。
    Service {
        /// 要写回调用方的状态码。
        status: Status,

        /// 面向调用方的错误说明。
        message: String,
    },

    /// 当前实现尚不支持的能力（例如分块 body）。
    Unsupported(String),

    /// 调用被取消令牌中止。
    Cancelled,
}

impl RpcError {
    /// 把错误映射成线上状态码。
    ///
    /// 服务端 dispatch 用它决定 `reply_status_async` 的状态；客户端也可以据此做
    /// 统一的上报。映射规则：
    ///
    /// | 错误 | 状态码 |
    /// |---|---|
    /// | [`RpcError::Decode`] | `400 BadRequest` |
    /// | [`RpcError::Unsupported`] | `501 NotImplemented` |
    /// | [`RpcError::Cancelled`] | `504 GatewayTimeout` |
    /// | [`RpcError::Transport`] / [`RpcError::Encode`] | `500 InternalServerError` |
    /// | [`RpcError::Remote`] / [`RpcError::Service`] | 自带的 `status` |
    pub const fn status(&self) -> Status {
        match self {
            RpcError::Transport(_) | RpcError::Encode(_) => Status::InternalServerError,
            RpcError::Decode(_) => Status::BadRequest,
            RpcError::Remote { status, .. } | RpcError::Service { status, .. } => *status,
            RpcError::Unsupported(_) => Status::NotImplemented,
            RpcError::Cancelled => Status::GatewayTimeout,
        }
    }

    /// 是否由取消令牌中止。
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, RpcError::Cancelled)
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Transport(message) => write!(f, "RPC 传输失败：{message}"),
            RpcError::Encode(message) => write!(f, "RPC body 编码失败：{message}"),
            RpcError::Decode(message) => write!(f, "RPC body 解码失败：{message}"),
            RpcError::Remote { status, body } => write!(
                f,
                "RPC 远端返回 {}（{}）：{}",
                status.inner(),
                status.debug_name_(),
                String::from_utf8_lossy(body)
            ),
            RpcError::Service { status, message } => write!(
                f,
                "RPC 业务错误 {}（{}）：{message}",
                status.inner(),
                status.debug_name_()
            ),
            RpcError::Unsupported(message) => write!(f, "RPC 暂不支持：{message}"),
            RpcError::Cancelled => f.write_str("RPC 调用已取消"),
        }
    }
}

impl std::error::Error for RpcError {}

/// 给状态码补一个便于阅读的名字。
///
/// `Status` 本身只有数值与 `Debug`；这里只为错误展示服务，不改变线上契约。
trait StatusDebugName_ {
    /// 人类可读的状态名。
    fn debug_name_(&self) -> &'static str;
}

impl StatusDebugName_ for Status {
    fn debug_name_(&self) -> &'static str {
        match self.inner() {
            200 => "Ok",
            201 => "Created",
            400 => "BadRequest",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "NotFound",
            405 => "MethodNotAllowed",
            406 => "NotAcceptable",
            500 => "InternalServerError",
            501 => "NotImplemented",
            502 => "BadGateway",
            503 => "ServiceUnavailable",
            504 => "GatewayTimeout",
            _ => "Unknown",
        }
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试错误到状态码的映射覆盖主要分支。
    /// Test that the error-to-status mapping covers the main branches.
    ///
    /// - 手段：分别构造 Decode / Unsupported / Cancelled / Service 四种错误。
    /// - 判断：状态码依次是 400 / 501 / 504 / 业务自带的状态。
    ///
    /// - Means: build four representative errors.
    /// - Verdict: they map to 400 / 501 / 504 / the business status respectively.
    #[test]
    fn maps_error_status_() {
        assert_eq!(
            RpcError::Decode("bad".to_string()).status(),
            Status::BadRequest
        );
        assert_eq!(
            RpcError::Unsupported("chunked".to_string()).status(),
            Status::NotImplemented
        );
        assert_eq!(RpcError::Cancelled.status(), Status::GatewayTimeout);
        assert_eq!(
            RpcError::Service {
                status: Status::NotFound,
                message: "missing".to_string(),
            }
            .status(),
            Status::NotFound
        );
    }

    /// 测试远端错误体的展示不会因为非 UTF-8 而 panic。
    /// Test that displaying a non-UTF-8 remote body never panics.
    ///
    /// - 手段：构造一个包含非法 UTF-8 字节的 `Remote` 错误并格式化。
    /// - 判断：格式化成功，且输出包含状态码数值。
    ///
    /// - Means: build a `Remote` error with invalid UTF-8 bytes and format it.
    /// - Verdict: formatting succeeds and the output contains the numeric status.
    #[test]
    fn displays_lossy_remote_body_() {
        let err = RpcError::Remote {
            status: Status::BadGateway,
            body: vec![0xffu8, 0xfeu8],
        };
        let text = err.to_string();
        assert!(text.contains("502"), "状态码应出现在错误文本里：{text}");
    }
}
