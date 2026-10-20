//! 客户端侧：配置、分配约定、极简代理与通用 unary 调用。
//!
//! 运行期只保留一个代理 [`ClientProxy`]：它持有 `&MptpClient<C>`，interface 由零大小的
//! 类型参数携带。配置契约与分配约定仍在本模块，body 编解码是 crate 内部实现。
//!
//! # `client`: configuration, allocation policy, proxy and unary calls
//!
//! The runtime keeps exactly one proxy: [`ClientProxy`] holds `&MptpClient<C>` while the
//! interface is a zero-sized type parameter. Configuration and allocation policy stay here;
//! body codec is crate-private.

mod alloc_config_;
mod proxy_;

pub mod config;

pub use alloc_config_::TrRpcClienAllocConfig;
pub use config::{DefaultRpcAllocConfig, Dock, TrRpcClientConfig};
pub use proxy_::{ClientProxy, ServiceSelector, TrRpcClientExt, call_unary_async};
