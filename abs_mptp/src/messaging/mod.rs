//! MPTP 报文前缀的**形状**：方法 / 位置 / 状态 / 头表能回答什么。
//!
//! 报文体的线上传输格式（定长、分块、精确读满错误）已经从本 crate 移到
//! `mptp_wire`：它们不是协议形状，而是具体编解码实现。本模块只保留类型与 trait，
//! 因此 `abs_mptp` 可以保持 `no_std` 且不依赖任何缓冲 / 分配设施。
//!
//! # messaging: the shape of MPTP message prefixes
//!
//! This module only keeps the request/response prefix traits and body-declaration
//! vocabulary. The concrete sized/chunked body framing moved to `mptp_wire`; it is an
//! implementation detail, not a protocol shape, and keeping it here would force `no_std`
//! protocol users to pull in the byte-buffer machinery.

mod cs_;
pub mod read;

pub use cs_::{BodySize, TrMsgPrefix, TrRequestPrefix, TrResponsePrefix};
pub use read::TrMptpBuffRead;
