//! 服务端侧：配置契约与请求处理者。
//!
//! 这一侧的配置（[`config`]）**只属于服务端**：监听从哪来、内存从哪来、本端收到的请求与
//! 发回去的回复长什么样。客户端那份契约在 [`crate::client`]，两者刻意不共享——它们可能由
//! 两份代码各自编译、各自升级。
//!
//! # `serving`: the server side
//!
//! The configuration ([`config`]) belongs to the server alone: where the listener comes
//! from, where memory comes from, and the shape of the requests this side receives and the
//! responses it sends back. The client's contract lives in [`crate::client`]; the two are
//! deliberately separate, since they may be compiled and upgraded independently.

pub mod config;
mod handler_;

pub use handler_::TrReqHandler;
