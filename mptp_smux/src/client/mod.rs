//! 客户端侧：配置契约、MPTP 客户端，以及一次请求对应的会话。
//!
//! 这一侧的配置（[`config`]）**只属于客户端**：它规定连接从哪来、ring 内存从哪来。
//! 报文前缀的线上格式由 [`crate::messaging`] 统一拥有，不在这一侧重复实现。服务端那份契约
//! 在 [`crate::serving`]，两者刻意不共享——它们可以跑在两个进程里、各自独立升级。
//!
//! # `client`: the client side
//!
//! The configuration here belongs to the client alone: where the connection comes from and
//! where the per-channel ring memory comes from. The prefix wire format is owned once by
//! [`crate::messaging`]. The server's contract lives in [`crate::serving`] and is
//! deliberately separate.

mod client_;
mod prep_ctx_;
mod session;

pub mod config;

pub use client_::{ClientError, MptpClient};
pub use prep_ctx_::ClientPrepCtx;
pub use session::{MptpSession, RespBodyError, RespBodyRead};
