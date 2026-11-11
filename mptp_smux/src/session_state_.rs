//! 服务端会话状态。
//!
//! 客户端状态机使用 [`abs_mptp::client::SessionState`]；服务端是它的镜像，但方向
//! 相反（读请求、写回复），因此单独定义。两者共享体边界与收尾原语，不共享方向性
//! 方法。
//!
//! # `session_state_`: server-side session state
//!
//! The client state machine is `abs_mptp::client::SessionState`. The server is its
//! mirror (read the request, write the response), so it gets its own enum while sharing
//! the body-boundary and teardown primitives.

/// 服务端一次会话的状态。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServingState {
    /// 请求前缀已经读出，等待 handler 读取体或直接回复。
    ReqPrefixReady,

    /// 正在读取请求体。
    ReadingReqBody,

    /// 正在读取请求后缀流。
    ReadingReqSuffix,

    /// 已经写过回复前缀。
    Responded,

    /// 会话已经收尾。
    Terminated,
}
