//! 服务端侧：配置契约、准备环境、路由，以及负责「解码 → 路由 → 处理」的服务器。
//!
//! 这一侧的配置（[`config`]）**只属于服务端**：它规定监听从哪来、每条入向子流的 ring 内存
//! 从哪来。报文前缀的线上格式由 [`crate::messaging`] 统一拥有。客户端那份契约在
//! [`crate::client`]，两者刻意不共享——它们可能由两份代码各自编译、各自升级。
//!
//! # `serving`: the server side
//!
//! The configuration here belongs to the server alone: where the listener comes from and
//! where the ring memory of every incoming channel comes from. The prefix wire format is
//! owned once by [`crate::messaging`]. The client's contract lives in [`crate::client`] and
//! is deliberately separate.

mod prep_ctx_;
mod router_;
mod server_;

pub mod config;

pub use prep_ctx_::ServingPrepCtx;
pub use router_::TrPathRouter;
pub use server_::{MptpServer, ServeError, close_channel_async};
