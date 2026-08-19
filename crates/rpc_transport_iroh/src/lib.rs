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

//! # 外部库限制测试
//!
//! 下面的 doctest 用于“测出”当前 `abs_buff` 的 segment 异步搬移 future
//! 不是 `Send` 的问题：`SegmMut::move_items_from_input_async` /
//! `SegmRef::move_items_to_output_async` 生成的 future 无法放进
//! `tokio::spawn`，因此不能直接作为 IrohChannel 的后台 pump 使用。
//!
//! ```compile_fail
//! use std::{mem::MaybeUninit, pin::Pin};
//!
//! use abs_buff::{
//!     Demand,
//!     buffer::{SegmMut, SegmReclaim},
//!     x_deps::abs_cancel::{NonCancellableToken, TrMayCancel},
//! };
//! use abs_buff_tokio_adapt::ReadAsInput;
//!
//! fn main() {
//!     let (mut a, _b) = tokio::io::duplex(64);
//!     let a: &'static mut tokio::io::DuplexStream = Box::leak(Box::new(a));
//!     let mut input = ReadAsInput::new(a);
//!     let storage: &'static mut [MaybeUninit<u8>; 8] =
//!         Box::leak(Box::new([MaybeUninit::uninit(); 8]));
//!     let consumed: &'static mut usize = Box::leak(Box::new(0usize));
//!     let mut segm = SegmMut::new(
//!         &mut storage[..],
//!         SegmReclaim::new(Pin::new(consumed)),
//!     );
//!
//!     tokio::spawn(async move {
//!         let _ = segm
//!             .move_items_from_input_async(&mut input, &Demand::less_than(8))
//!             .may_cancel_with(NonCancellableToken::shared_mut())
//!             .await;
//!     });
//! }
//! ```
//!
pub mod channel;
pub mod conn;

pub use channel::{IrohChannel, IrohRecv, IrohSend};
pub use conn::{IrohConnError, IrohConnection};

pub mod x_deps {
    pub use mptp_rpc_core;
}
