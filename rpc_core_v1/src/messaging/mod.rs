//! RPC 消息层的公共类型：统一错误、结果别名与 interface 路径契约。
//!
//! body 的编解码实现留在本模块内部；对外只转出 [`RpcError`] / [`RpcResult`] /
//! [`TrRpcInterface`] 与路径拼接函数 [`concat_location`]。
//!
//! # `messaging`: shared RPC message types
//!
//! The body codec stays internal; this module exposes the unified error / result aliases,
//! the interface path contract and the path composition helper.
pub(crate) mod body_;
mod error_;
mod interface_;

pub use error_::{RpcError, RpcResult};
pub use interface_::{TrRpcInterface, concat_location};
