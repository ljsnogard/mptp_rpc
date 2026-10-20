//! `abs_mptp`：MPTP（Multiplexed P2P Transportation Protocol）的协议基础 trait 与基础实现。
//!
//! 本 crate 只定义**协议层**的形状与不变量，不掺任何 RPC 业务：
//!
//! - [`spec`]：协议规范里的公共词汇——访问方法、状态码、标准头键值，以及头表本身；
//! - [`messaging`]：报文前缀（`cs_`）与报文体（[`messaging::limited`] 定长、
//!   [`messaging::chunked`] 分块）的收发机制，以及两者共用的报文 IO 地基；
//! - [`client`] / [`serving`]：两侧各自的配置契约与会话 / 处理者 trait。
//!
//! 客户端与服务端的配置**刻意不共享**：它们可能跑在两个进程里，由两份代码各自编译、
//! 各自升级，因此连接与内存策略互不相干。
//!
//! # `abs_mptp`: MPTP protocol traits and baseline implementations
//!
//! This crate defines the shape and invariants of the protocol layer only, with no RPC
//! business mixed in:
//!
//! - [`spec`]: the shared vocabulary — access methods, status codes, standard header
//!   keys/values, and the header map itself;
//! - [`messaging`]: the wire mechanics for the message prefix (`cs_`) and the body
//!   (sized via [`messaging::limited`], chunked via [`messaging::chunked`]), plus the
//!   message IO footing they share;
//! - [`client`] / [`serving`]: each side's own configuration contract and its session
//!   and handler traits.
//!
//! The client and server configurations are deliberately **not** shared: the two sides
//! may live in different processes, compiled from different code and upgraded
//! independently, so neither connection nor memory policy is tied together.

#![feature(impl_restriction)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod client;
pub mod messaging;
pub mod serving;
pub mod spec;

pub mod x_deps {
    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr, funty, gen_mcf2};
    pub use abs_mm;
    pub use abs_smux;
}
