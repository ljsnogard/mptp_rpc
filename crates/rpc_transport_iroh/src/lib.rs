#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(async_fn_traits)]
//! MPTP over Iroh / QUIC 的传输层实现。
//!
//! 这个 crate 对应 README 中的 L1（通道抽象层）：
//!
//! - [`IrohConnection`]：实现 `TrMuxConn`，表示一条可开多条独立 stream 的 iroh 连接；
//! - [`IrohChannel`]：实现 `TrChannel`，表示一条双向 QUIC stream 对；
//! - [`IrohSend`] / [`IrohRecv`]：`split()` 后得到的读写半通道，分别实现
//!   `TrBuffTryWrite` / `TrBuffTryRead`，供上层 RPC 编解码器使用。
//!
//! # 典型用法
//!
//! 客户端：
//!
//! ```ignore
//! let conn = IrohConnection::connect_by_id(endpoint, server_id, ALPN).await?;
//! let mut channel = conn.open_channel_async().may_cancel_with(cancel).await?;
//! let (mut tx, mut rx) = channel.split();
//! tx.write_all(b"hello").await?;
//! ```
//!
//! 服务端：
//!
//! ```ignore
//! let conn = IrohConnection::accept(endpoint).await?;
//! let mut channel = conn.accept_channel_async().await?;
//! let (mut tx, mut rx) = channel.split();
//! ```
//!
//! 更上层的 MPTP 消息解析（Request / Response / body）应该只依赖
//! `TrChannel` / `TrBuffTryRead` / `TrBuffTryWrite`，不依赖本 crate 的具体类型。

pub mod channel;
pub mod conn;

pub use channel::{IrohChannel, IrohRecv, IrohSend};
pub use conn::{IrohConnError, IrohConnection};
