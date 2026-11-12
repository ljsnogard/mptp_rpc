//! 服务端侧：单连接入口、会话、单一 handler 与回复 helper。
//!
//! 这里**没有 router**：路由、鉴权、方法分派都由单一 handler 自己决定。本模块只提供
//! 一条连接上的 `MptpServlet`、一次请求的 `ServingSession`，以及写回复的 helper。
//!
//! # `serving`: the server side
//!
//! There is no router here. Routing and dispatch live inside the single handler. This
//! module provides `MptpServlet` for one connection, `ServingSession` for one request,
//! and the response helpers.

mod connection_;
mod handler_;
mod prep_ctx_;
mod servlet_;
mod session_;

pub mod config;

pub use connection_::MptpConnection;
pub use handler_::{LegacyHandler, TrServeHandler};
pub use prep_ctx_::ServingPrepCtx;
pub use servlet_::{
    K_TMP_ALLOW_HEADER_KEY, K_TMP_ALLOW_HEADER_NAME, MptpServlet, ServeOutcome, close_channel_async,
};
pub use session_::{
    RequestBodyAsync, RequestBodyRead, RequestSuffixAsync, RequestSuffixRead, ResponseBodyWriter,
    ResponseBuilder, ServingSession, SuffixStream,
};
