//! 客户端侧：配置契约、MPTP 客户端，以及一次请求对应的会话。
//!
//! 这一侧的配置（[`config`]）**只属于客户端**：它规定连接从哪来、内存从哪来、本端发出去
//! 的请求与期待收到的回复长什么样。服务端那份契约在 [`crate::serving`]，两者刻意不共享
//! ——它们可以跑在两个进程里、各自独立升级。
//!
//! # `client`: the client side
//!
//! The configuration ([`config`]) belongs to the client alone: where the connection comes
//! from, where memory comes from, and the shape of the requests this side sends and the
//! responses it expects. The server's contract lives in [`crate::serving`]; the two are
//! deliberately separate, since they may run in different processes and be upgraded
//! independently.

mod client_;
pub mod config;

pub use client_::{SessionState, TrMptpClient, TrMptpSession};
