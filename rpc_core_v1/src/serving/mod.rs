//! 服务端侧：配置、一次调用的上下文，以及两代 router 契约。
//!
//! 这一层提供与业务无关的底座：
//!
//! - [`TrServingConfig`] / [`TrServingAllocConfig`]：服务端配置契约（定义在
//!   `abs_rpc_serv`，本模块转出并给出缺省实现）；
//! - [`RpcCallCtx`]：一次调用的解码、回复与收尾，并实现
//!   `abs_rpc_serv::service::TrRpcCallCtx`，因此宏生成的 dispatch 不绑定具体上下文；
//! - [`RpcRouter`]：**内联**的 interface 级 router。用户用
//!   `RpcRouter::<ApiV1>::new(ctx).add_service::<Impl>(...)` 自己组织 service；每个请求
//!   在 router 内直接解码、调用 service、写回复、收尾；
//! - [`RpcJob`] / [`ServiceRoute`] / [`TrRpcServiceRoute`] / [`TrRpcRouter`] /
//!   [`RpcRouterHandler`]：**队列版**的旧装配链，保留给需要背压或自定义 worker 的使用者；
//!   宏不再生成它们。
//!
//! 顶层 interface 分派由 `mptp_smux::serving::Router` 完成：`RpcRouter` 实现
//! `TrPathHandler`，用 `ApiV1::PATH` 作为前缀；`Router::call` 把它登记到顶层。
//!
//! # `serving`: configuration, per-call context and two router generations
//!
//! The inline [`RpcRouter`] owns concrete service implementations and dispatches without a
//! queue. The older [`TrRpcRouter`] / [`RpcRouterHandler`] chain remains for users who need
//! backpressure or custom workers; the macro no longer generates it. Interface prefixes are
//! registered on `mptp_smux::serving::Router` through `TrPathHandler`.

mod ctx_;
mod route_;
mod router_;
mod rpc_router_;

pub mod config;

pub use config::{DefaultServingAllocConfig, TrServingAllocConfig, TrServingConfig};
pub use ctx_::RpcCallCtx;
pub use route_::{
    K_DEFAULT_SERVICE_QUEUE_CAP, RpcJob, RpcRouteError, ServiceRoute, TrRpcServiceRoute,
};
pub use router_::{RpcDispatchError, RpcRouterHandler, TrRpcRouter};
pub use rpc_router_::{RpcRouter, RpcServiceCtx};
#[doc(hidden)]
pub use rpc_router_::{ServiceValues, TrRpcServiceList, TrRpcServiceValues};
