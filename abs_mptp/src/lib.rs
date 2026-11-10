//! `abs_mptp`：MPTP（Multiplexed P2P Transportation Protocol）的协议基础 trait 与基础实现。
//!
//! 本 crate 只定义**协议层**的形状与不变量，不掺任何 RPC 业务：
//!
//! - [`spec`]：协议规范里的公共词汇——访问方法、状态码、标准头键值，以及头表本身；
//! - [`messaging`]：报文前缀的形状（方法 / 位置 / 状态 / 头表）与体声明；
//!   具体定长 / 分块体格式在 `mptp_wire`；
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
//! - [`messaging`]: the shape of request/response prefixes (method, location, status,
//!   headers) and body declarations; the concrete sized/chunked body formats live in
//!   `mptp_wire`;
//! - [`client`] / [`serving`]: each side's own configuration contract and its session
//!   and handler traits.
//!
//! The client and server configurations are deliberately **not** shared: the two sides
//! may live in different processes, compiled from different code and upgraded
//! independently, so neither connection nor memory policy is tied together.

#![feature(impl_restriction)]
#![feature(unboxed_closures)]
#![no_std]

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
