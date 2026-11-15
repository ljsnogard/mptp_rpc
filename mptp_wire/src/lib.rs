//! MPTP 的**传输无关线上格式**：前缀、头表、限长体与分块体。
//!
//! 本 crate 是从 `mptp_smux::messaging` 与 `abs_mptp::messaging` 抽出的共享层：
//!
//! - 具体前缀类型 [`RequestPrefix`] / [`ResponsePrefix`]；
//! - 头表 [`Headers`] / [`HeaderKey`] / [`HeaderVal`] / [`U16OrStr`]；
//! - 四个前缀读写入口与线格式错误 [`MessagingError`]；
//! - 报文体传输格式：模块 [`limited`]（定长）与 [`chunked`]（2 字节大端块长）；
//! - 报文 IO 的精确读取错误 [`ReadExactError`]。
//!
//! 它**不依赖** `smux_v1`、dock、ring 或任何客户端 / 服务端实现，因此 `mptp_smux`
//! 与未来的 `mptp_quic` 可以共用同一份线格式。
//!
//! # 编码模型
//!
//! MPTP 的请求前缀是三个**连续的顶层 MessagePack 值**：`method → location → headers`；
//! 回复前缀是两个：`status → headers`。它们不是一个数组或结构体，因此不能对
//! `RequestPrefix` / `ResponsePrefix` 整体派生 denco 的 `TrEncodable` / `TrDecodable`；
//! 四个入口按字段顺序逐个编码 / 解码，避免多写容器头。
//!
//! 编解码直接架在 [`abs_buff`] 的 `TrBuffRead` / `TrBuffWrite` 上：
//! [`mp_denco::MpDecoder`] / [`mp_denco::MpEncoder`] 只借需要的段，不预读体的第一个字节，
//! 也不把整个前缀预攒进 `Vec`。每个 await 点都接受调用方取消令牌。
//!
//! # `mptp_wire`: transport-independent MPTP prefix wire format
//!
//! This crate owns the concrete request/response prefix types, the header map, the
//! MessagePack prefix codec, and the sized/chunked body framing. It is deliberately
//! independent of `smux_v1` and of any client or server binding, so `mptp_smux` and a
//! future `mptp_quic` can share exactly one wire implementation.

#![no_std]
#![feature(impl_trait_in_assoc_type)]

extern crate alloc;

mod error_;
mod headers_;
mod io_;
mod num_or_str_;
mod prefix_;
mod wire_;

pub mod chunked;
pub mod limited;
pub mod read;

pub use error_::MessagingError;
pub use headers_::{HeaderEntry, HeaderKey, HeaderVal, Headers};
pub use io_::ReadExactError;
pub use num_or_str_::U16OrStr;
pub use prefix_::{RequestPrefix, ResponsePrefix};
pub use wire_::{
    ReadRequestPrefixAsync, ReadResponsePrefixAsync, WriteRequestPrefixAsync,
    WriteResponsePrefixAsync, read_request_prefix_async, read_response_prefix_async,
    write_request_prefix_async, write_response_prefix_async,
};

/// 本 crate 依赖的外部 crate 转出，供下游与宏生成代码不必逐条声明版本。
///
/// 这不是稳定的公共 API 面；下游需要时优先使用具体类型。
pub mod x_deps {
    pub use abs_buff;
    pub use abs_cancel;
    pub use abs_denco;
    pub use abs_mptp;
    pub use denco_macro;
    pub use gen_mcf2;
    pub use mp_denco;
}
