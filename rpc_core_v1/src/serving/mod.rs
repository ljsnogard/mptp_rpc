//! 服务端侧：配置、一次调用的上下文，以及 interface 级 RPC router。
//!
//! 这一层提供与业务无关的底座：
//!
//! - [`TrServingConfig`] / [`TrServingAllocConfig`]：服务端配置契约（定义在
//!   `abs_rpc_serv`，本模块转出并给出缺省实现）；
//! - [`RpcCallCtx`]：一次调用的解码、回复与收尾，并实现
//!   `abs_rpc_serv::service::TrRpcCallCtx`，因此宏生成的 dispatch 不绑定具体上下文；
//! - [`RpcRouter`]：interface 级 RPC facade。用户用
//!   `RpcRouter::<ApiV1>::new(ctx).add_service::<Impl>(...)` 自己组织 service；service
//!   路径匹配统一交给 [`matchit_route`]，命中后直接解码、调用 service、写回复、收尾。
//!
//! 顶层 interface 分派由本模块的 `Router` 完成，它同样使用
//! [`matchit_route`]。项目里不再保留队列版 router 契约与 `rpc_router_trie`：需要背压时，
//! 在 handler / target 内部实现自己的队列，而不是再引入第二套路径匹配算法。
//!
//! # `serving`: configuration, per-call context and the interface-level RPC facade
//!
//! [`RpcRouter`] owns concrete service implementations and dispatches through the single
//! [`matchit_route`] path matcher. The queued router contracts and the separate
//! `rpc_router_trie` crate were removed; backpressure belongs inside a handler or target,
//! not in a second routing algorithm.

mod body_;
mod cancel_future_;
mod ctx_;
mod rpc_router_;
mod router_;

pub mod config;

pub use config::{DefaultServingAllocConfig, TrServingAllocConfig, TrServingConfig};
pub use ctx_::RpcCallCtx;
pub use router_::{Router, RouterCtx, RouterError, TrPathHandler, TrPathRouter, TrPathRouterMut};
pub use rpc_router_::{RpcRouter, RpcServiceCtx};
pub use body_::{decode_body_async, write_ok_async, write_status_async};
#[doc(hidden)]
pub use cancel_future_::{EitherMayCancel, MapErrMayCancel, ReadyMayCancel};
#[doc(hidden)]
pub use router_::{RouteValues, TrPathRouterList};
#[doc(hidden)]
pub use rpc_router_::{ServiceValues, TrRpcServiceList, TrRpcServiceValues};
