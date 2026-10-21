//! 服务端侧：配置、分配约定、一次调用的上下文，以及 router 契约。
//!
//! 这一层提供与业务无关的底座：
//!
//! - [`RpcCallCtx`]：一次调用的解码、回复与收尾；
//! - [`RpcJob`] / [`ServiceRoute`]：交给 service worker 的请求，以及缺省的 service 注册项；
//! - [`TrRpcServiceRoute`]：把 job 交给 service 的契约，允许使用者替换队列；
//! - [`TrRpcRouter`]：按 interface / service 匹配 location 的契约；
//! - [`RpcRouterHandler`]：把任意 [`TrRpcRouter`] 接入
//!   [`TrReqHandler`](crate::x_deps::abs_mptp::serving::TrReqHandler) 的适配器。
//!
//! 具体的前缀树 router 实现不在本 crate，而在 `rpc_router_trie`。`#[rpc(service = "...")]`
//! 生成的 worker 只负责「从队列取 [`RpcJob`] → 调用生成的 dispatch → 收尾」；
//! `#[rpc(path = "...", services = [...])]` 生成的 worker 组负责把多个 service 的
//! [`ServiceRoute`] 收集起来。两者都不替使用者 spawn 线程或选择运行时。
//!
//! # `serving`: configuration, per-call context and the router contracts
//!
//! This module provides the business-agnostic serving base: the per-call context, the job /
//! route types, the delivery contract, and the router contract plus its MPTP handler adapter.
//! The concrete trie router lives in `rpc_router_trie`.

mod alloc_config_;
mod ctx_;
mod route_;
mod router_;
mod service_;

pub mod config;

pub use alloc_config_::TrServingAllocConfig;
pub use config::{DefaultServingAllocConfig, TrServingConfig};
pub use ctx_::RpcCallCtx;
pub use route_::{
    K_DEFAULT_SERVICE_QUEUE_CAP,
    RpcJob, RpcRouteError, ServiceRoute, TrRpcServiceRoute,
};
pub use router_::{RpcDispatchError, RpcRouterHandler, TrRpcRouter};
pub use service_::TrRpcServiceRouter;
