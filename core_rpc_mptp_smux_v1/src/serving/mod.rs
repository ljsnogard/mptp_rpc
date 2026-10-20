//! 服务端侧：配置、分配约定、一次调用的上下文与两级路由器。
//!
//! 这一层提供三块与业务无关的底座：
//!
//! - [`RpcCallCtx`]：一次调用的解码、回复与收尾；
//! - [`RpcJob`] / [`ServiceRoute`]：交给 service worker 的请求，以及 service 的注册项；
//! - [`RpcRouter`]：interface → service 两级运行期 Trie 路由器，本身实现
//!   [`TrReqHandler`](crate::x_deps::abs_mptp::serving::TrReqHandler)。
//!
//! `#[rpc(service = "...")]` 生成的 worker 只负责「从队列取 [`RpcJob`] → 调用生成的
//! dispatch → 收尾」；`#[rpc(path = "...", services = [...])]` 生成的 worker 组负责把多个
//! service 的 [`ServiceRoute`] 收集起来。两者都不替使用者 spawn 线程或选择运行时。
//!
//! # `serving`: configuration, per-call context and the two-level router
//!
//! This module provides the business-agnostic serving base: the per-call context, the job /
//! route types, and the interface → service trie router. Macro-generated workers receive a
//! job, call the generated dispatch and close the channel; they never choose a runtime.

mod alloc_config_;
mod ctx_;
mod router_;
mod trie_;

pub mod config;

pub use alloc_config_::TrServingAllocConfig;
pub use config::{DefaultServingAllocConfig, TrServingConfig};
pub use ctx_::RpcCallCtx;
pub use router_::{K_DEFAULT_SERVICE_QUEUE_CAP, RpcJob, RpcRouter, RpcRouterError, ServiceRoute};
