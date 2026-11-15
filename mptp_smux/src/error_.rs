//! 公开错误类型：取消、传输、协议、状态机四类。
//!
//! 它们都是 `no_std` 可用的具体类型：`Display` 用 `core::fmt`，`Error` 实现
//! `core::error::Error`。传输层的具体错误类型由配置的关联类型给出（泛型），非泛型的
//! 公开错误无法在变体里命名它，因此 `TransportError` 用 `detail: String` 保留底层
//! 描述；协议错误能保留具体类型的地方（例如前缀编解码）直接保留。
//!
//! # `error_`: public error types
//!
//! Concrete, `no_std`-friendly errors classified as cancelled / transport / protocol /
//! state. Transport errors keep a textual detail because their concrete types are
//! generic over the user's configuration; protocol errors keep concrete wire errors
//! where the type is nameable.

use alloc::string::String;
use core::fmt::{self, Display, Formatter};

use abs_mptp::{client::SessionState, spec::access_method::AccessMethod};

use crate::{method_set_::MethodSet, session_state_::ServingState};

/// 客户端错误。
#[derive(Debug)]
pub enum ClientError {
    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    Cancelled,

    /// 连接 / 子流 / ring 工厂的传输失败。
    Transport(TransportError),

    /// 前缀解码、体声明、截断、`Head` / `Drop` 回复体等协议违规。
    Protocol(ProtocolError),

    /// 会话状态机不接受当前操作。
    State(StateError),
}

impl Display for ClientError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Cancelled => write!(f, "异步操作已被调用方取消"),
            ClientError::Transport(err) => write!(f, "传输失败：{err}"),
            ClientError::Protocol(err) => write!(f, "协议错误：{err}"),
            ClientError::State(err) => write!(f, "会话状态不允许该操作：{err}"),
        }
    }
}

impl core::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            ClientError::Cancelled => Option::None,
            ClientError::Transport(err) => Option::Some(err),
            ClientError::Protocol(err) => Option::Some(err),
            ClientError::State(err) => Option::Some(err),
        }
    }
}

impl From<TransportError> for ClientError {
    fn from(err: TransportError) -> Self {
        ClientError::Transport(err)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(err: ProtocolError) -> Self {
        ClientError::Protocol(err)
    }
}

impl From<StateError> for ClientError {
    fn from(err: StateError) -> Self {
        ClientError::State(err)
    }
}

/// 传输层错误：按失败步骤分类，底层描述保留在 `detail`。
#[derive(Debug)]
pub enum TransportError {
    /// 进入监听状态（`listen_async`）失败。
    Listen {
        /// 底层传输错误的文本描述。
        detail: String,
    },

    /// 在本端派生会话（`bind_async`）失败。
    Bind {
        /// 底层传输错误的文本描述。
        detail: String,
    },

    /// 登记子流身份（`open_channel_async`）失败。
    OpenChannel {
        /// 底层传输错误的文本描述。
        detail: String,
    },

    /// 最终裁决 / 接受子流（`accept_async`）失败。
    Accept {
        /// 底层传输错误的文本描述。
        detail: String,
    },

    /// 准备 ring 内存失败（工厂报错）。
    RingPrepare {
        /// 工厂错误的文本描述。
        detail: String,
    },

    /// 写子流失败。
    Write {
        /// 底层传输错误的文本描述。
        detail: String,
    },

    /// 读子流失败。
    Read {
        /// 底层传输错误的文本描述。
        detail: String,
    },
}

impl Display for TransportError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Listen { detail } => write!(f, "进入监听失败：{detail}"),
            TransportError::Bind { detail } => write!(f, "在本端派生会话失败：{detail}"),
            TransportError::OpenChannel { detail } => write!(f, "登记子流身份失败：{detail}"),
            TransportError::Accept { detail } => write!(f, "接受子流失败：{detail}"),
            TransportError::RingPrepare { detail } => write!(f, "准备 ring 内存失败：{detail}"),
            TransportError::Write { detail } => write!(f, "写子流失败：{detail}"),
            TransportError::Read { detail } => write!(f, "读子流失败：{detail}"),
        }
    }
}

impl core::error::Error for TransportError {}

/// 协议错误：前缀、体声明、体截断、后缀流等。
#[derive(Debug)]
pub enum ProtocolError {
    /// 请求前缀编码失败。
    RequestPrefixEncode(mptp_wire::MessagingError),

    /// 请求前缀解码失败。
    RequestPrefixDecode(mptp_wire::MessagingError),

    /// 回复前缀编码失败。
    ResponsePrefixEncode(mptp_wire::MessagingError),

    /// 回复前缀解码失败。
    ResponsePrefixDecode(mptp_wire::MessagingError),

    /// `Body_Size` 头不是合法的长度。
    InvalidBodySize {
        /// 头里的原始文本。
        text: String,
    },

    /// 回复要按方法语义校验，但会话没有请求前缀可供判断。
    MissingRequestMethod,

    /// `Head` / `Drop` 的回复声明了体。
    ResponseBodyOnHeadOrDrop {
        /// 本端请求使用的方法。
        method: AccessMethod,
    },

    /// 请求体在源结束前没有搬够声明长度。
    RequestBodyTruncated {
        /// 前缀声明的长度。
        declared: usize,
        /// 实际搬走的字节数。
        read: usize,
    },

    /// 读取请求体失败。
    RequestBodyRead {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 写入请求体失败。
    RequestBodyWrite {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 回复体在源结束前没有搬够声明长度。
    ResponseBodyTruncated {
        /// 声明的长度。
        declared: usize,
        /// 实际搬走的字节数。
        read: usize,
    },

    /// 读取回复体失败。
    ResponseBodyRead {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 写回复体失败。
    ResponseBodyWrite {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 读取请求后缀流失败。
    SuffixRead {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 写入请求后缀流失败。
    SuffixWrite {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 请求后缀流在源结束前没有搬够声明长度。
    SuffixTruncated {
        /// 声明的长度。
        declared: usize,
        /// 实际搬走的字节数。
        read: usize,
    },

    /// 丢弃未读完的报文体失败。
    DrainBody {
        /// 底层错误的文本描述。
        detail: String,
    },

    /// 回复后缀流之前的前缀声明了体；helper 只写后缀，不能替调用者写体。
    SuffixWithBody,

    /// `no_body` helper 收到一个声明了体的前缀。
    BodyDeclaredButNotWritten,
}

impl Display for ProtocolError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolError::RequestPrefixEncode(err) => write!(f, "编码请求前缀失败：{err}"),
            ProtocolError::RequestPrefixDecode(err) => write!(f, "解码请求前缀失败：{err}"),
            ProtocolError::ResponsePrefixEncode(err) => write!(f, "编码回复前缀失败：{err}"),
            ProtocolError::ResponsePrefixDecode(err) => write!(f, "解码回复前缀失败：{err}"),
            ProtocolError::InvalidBodySize { text } => {
                write!(f, "Body_Size 头不是合法的长度：{text:?}")
            }
            ProtocolError::MissingRequestMethod => {
                write!(f, "会话没有请求前缀，无法按方法校验回复体")
            }
            ProtocolError::ResponseBodyOnHeadOrDrop { method } => {
                write!(f, "{method:?} 的回复按协议不带报文体，前缀却声明了体")
            }
            ProtocolError::RequestBodyTruncated { declared, read } => {
                write!(f, "请求体声明了 {declared} 字节，源只给出 {read} 字节")
            }
            ProtocolError::RequestBodyRead { detail } => write!(f, "读取请求体失败：{detail}"),
            ProtocolError::RequestBodyWrite { detail } => write!(f, "写入请求体失败：{detail}"),
            ProtocolError::ResponseBodyTruncated { declared, read } => {
                write!(f, "回复体被截断：声明 {declared} 字节，实际 {read} 字节")
            }
            ProtocolError::ResponseBodyRead { detail } => write!(f, "读取回复体失败：{detail}"),
            ProtocolError::ResponseBodyWrite { detail } => write!(f, "写回复体失败：{detail}"),
            ProtocolError::SuffixRead { detail } => write!(f, "读取请求后缀流失败：{detail}"),
            ProtocolError::SuffixWrite { detail } => write!(f, "写入请求后缀流失败：{detail}"),
            ProtocolError::SuffixTruncated { declared, read } => {
                write!(f, "请求后缀流被截断：声明 {declared} 字节，实际 {read} 字节")
            }
            ProtocolError::DrainBody { detail } => write!(f, "丢弃未读完的报文体失败：{detail}"),
            ProtocolError::SuffixWithBody => {
                write!(f, "回复前缀声明了体，后缀 helper 不能跳过回复体")
            }
            ProtocolError::BodyDeclaredButNotWritten => {
                write!(f, "回复前缀声明了体，但 no_body helper 不会写体")
            }
        }
    }
}

impl core::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            ProtocolError::RequestPrefixEncode(err)
            | ProtocolError::RequestPrefixDecode(err)
            | ProtocolError::ResponsePrefixEncode(err)
            | ProtocolError::ResponsePrefixDecode(err) => Option::Some(err),
            _ => Option::None,
        }
    }
}

/// 会话状态机错误。
#[derive(Debug)]
pub enum StateError {
    /// 客户端会话处于意外的状态。
    Client {
        /// 当前状态。
        actual: SessionState,
    },

    /// 服务端会话处于意外的状态。
    Serving {
        /// 当前状态。
        actual: ServingState,
    },

    /// 会话没有请求前缀，无法判定请求体的传输方式。
    NoRequestPrefix,

    /// 请求前缀没有声明报文体。
    BodyNotDeclared,

    /// 已经写过回复前缀。
    AlreadyResponded,

    /// 子流两半已经被 `take_parts` / `take_channel` 取走。
    ChannelTaken,

    /// 当前不是在读取体的状态。
    NotReadingBody,

    /// 当前不是在读取后缀流的状态。
    NotReadingSuffix,
}

impl Display for StateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            StateError::Client { actual } => {
                write!(f, "客户端会话处于 {actual:?}，此时不接受该操作")
            }
            StateError::Serving { actual } => {
                write!(f, "服务端会话处于 {actual:?}，此时不接受该操作")
            }
            StateError::NoRequestPrefix => write!(f, "会话没有请求前缀，无法判定请求体的传输方式"),
            StateError::BodyNotDeclared => write!(f, "请求前缀没有声明报文体，不接受发送请求体"),
            StateError::AlreadyResponded => write!(f, "已经写过回复前缀，不能再次写回复"),
            StateError::ChannelTaken => write!(f, "子流的收发半边已经被取走"),
            StateError::NotReadingBody => write!(f, "当前不是在读取体的状态"),
            StateError::NotReadingSuffix => write!(f, "当前不是在读取后缀流的状态"),
        }
    }
}

impl core::error::Error for StateError {}

/// 服务端错误。
#[derive(Debug)]
pub enum ServeError {
    /// 等待期间调用方取消。
    Cancelled,

    /// 连接 / 子流 / ring 工厂的传输失败。
    Transport(TransportError),

    /// 协议违规。
    Protocol(ProtocolError),

    /// 状态机错误。
    State(StateError),

    /// handler 报出的错误。
    Target {
        /// handler 错误的文本描述。
        detail: String,
    },

    /// 访问方法不在 handler 声明的集合内。
    NotAllowed {
        /// 本次请求的方法。
        method: AccessMethod,
        /// handler 声明允许的方法集合。
        allow: MethodSet,
    },
}

impl Display for ServeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ServeError::Cancelled => write!(f, "异步操作已被调用方取消"),
            ServeError::Transport(err) => write!(f, "传输失败：{err}"),
            ServeError::Protocol(err) => write!(f, "协议错误：{err}"),
            ServeError::State(err) => write!(f, "会话状态不允许该操作：{err}"),
            ServeError::Target { detail } => write!(f, "handler 处理失败：{detail}"),
            ServeError::NotAllowed { method, allow } => {
                write!(f, "{method:?} 不在允许的方法集合 {allow:?} 内")
            }
        }
    }
}

impl core::error::Error for ServeError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            ServeError::Cancelled | ServeError::Target { .. } | ServeError::NotAllowed { .. } => {
                Option::None
            }
            ServeError::Transport(err) => Option::Some(err),
            ServeError::Protocol(err) => Option::Some(err),
            ServeError::State(err) => Option::Some(err),
        }
    }
}

impl From<TransportError> for ServeError {
    fn from(err: TransportError) -> Self {
        ServeError::Transport(err)
    }
}

impl From<ProtocolError> for ServeError {
    fn from(err: ProtocolError) -> Self {
        ServeError::Protocol(err)
    }
}

impl From<StateError> for ServeError {
    fn from(err: StateError) -> Self {
        ServeError::State(err)
    }
}

#[cfg(test)]
mod tests_ {
    use alloc::string::ToString;

    use super::*;

    /// 测试客户端错误能区分取消 / 传输 / 协议 / 状态四类。
    /// Test that client errors distinguish cancelled / transport / protocol / state.
    ///
    /// - 手段：分别构造四个变体。
    /// - 判断：`matches!` 各自命中对应类别，互不混淆。
    ///
    /// - Means: construct one value of each variant.
    /// - Verdict: `matches!` identifies each category without overlap.
    #[test]
    fn client_error_has_four_categories_() {
        let cancelled = ClientError::Cancelled;
        let transport = ClientError::Transport(TransportError::Bind {
            detail: String::from("x"),
        });
        let protocol = ClientError::Protocol(ProtocolError::MissingRequestMethod);
        let state = ClientError::State(StateError::ChannelTaken);

        assert!(matches!(cancelled, ClientError::Cancelled));
        assert!(matches!(transport, ClientError::Transport(_)));
        assert!(matches!(protocol, ClientError::Protocol(_)));
        assert!(matches!(state, ClientError::State(_)));
    }

    /// 测试 `405` 错误携带方法集合，`Display` 能读出方法名。
    /// Test that the `405` error carries the method set and its name in `Display`.
    ///
    /// - 手段：构造 `ServeError::NotAllowed`，再格式化。
    /// - 判断：错误是 `NotAllowed` 变体，文本包含方法名。
    ///
    /// - Means: build `ServeError::NotAllowed` and format it.
    /// - Verdict: it is the `NotAllowed` variant and the text names the method.
    #[test]
    fn not_allowed_reports_method_() {
        let err = ServeError::NotAllowed {
            method: AccessMethod::Push,
            allow: MethodSet::HEAD.union(MethodSet::VIEW),
        };
        assert!(matches!(err, ServeError::NotAllowed { .. }));
        assert!(err.to_string().contains("Push"));
    }

    /// 测试状态错误保留实际状态。
    /// Test that the state error keeps the actual state.
    ///
    /// - 手段：构造客户端状态错误并匹配。
    /// - 判断：`actual` 与传入的 `SessionState` 一致。
    ///
    /// - Means: build a client state error and match it.
    /// - Verdict: `actual` equals the supplied `SessionState`.
    #[test]
    fn state_error_keeps_actual_() {
        let err = StateError::Client {
            actual: SessionState::SendingBody,
        };
        match err {
            StateError::Client { actual } => assert_eq!(actual, SessionState::SendingBody),
            _ => panic!("应当命中 Client 变体"),
        }
    }
}
