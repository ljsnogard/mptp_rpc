//! MPTP 的报文收发机制：限长、分块，以及两者共用的 IO 地基。
//!
//! 本层只依赖 [`abs_buff`] 的段接口：报文头与报文体的字节既不预先攒进 `Vec`，也不多读
//! 一个字节。需要 `std::io::Read` / `Write` 的调用点在**应用侧**自行套适配——本 crate
//! 不依赖任何 io 适配 crate，也就不替使用者决定后端运行时（见模块 `io_` 的说明）。
//!
//! - [`limited`]：把「一次能借出多少」钉死在预先设定的额度上（定长体的读写口）；
//! - [`chunked`]：把一段长度未知的内容切成「2 字节大端长度 + 内容」的块（分块体）。
//!
//! # messaging: the MPTP wire mechanics
//!
//! This layer builds only on `abs_buff`'s segment interface: neither the prefix nor the body
//! is ever gathered into a `Vec`, and not a single byte beyond the message is read. Callers
//! that need `std::io::Read` / `Write` apply the adapter on the **application** side: this
//! crate does not depend on any io adapter crate, and therefore never picks the backend
//! runtime for its users.
//!
//! - [`limited`] pins "how much may be lent out at once" to a preset quota (the sized
//!   body's read/write port);
//! - [`chunked`] frames content of unknown length as `2-byte big-endian length + payload`
//!   chunks (streaming bodies).

mod cs_;
mod io_;

pub mod chunked;
pub mod limited;
pub mod read;

pub use cs_::{BodySize, TrRequestPrefix, TrResponsePrefix, TrMsgPrefix};
pub use io_::MessageIoError;
