//! 服务端处理框架。
//!
//! 这个模块提供：
//!
//! - [`config::TrServingConfig`]：服务端自己的配置契约（**与客户端那份完全独立**，
//!   因为两端可能跑在不同进程里）；
//! - [`config::TrServingAllocConfig`]：服务端的 ring 内存来源（accept 时必须交出）；
//! - [`handler::TrReqHandler`]：单个 handler 的抽象；
//! - [`handler::HandlerChain`]：把多个 handler 串成链，让同一个请求有机会
//!   按顺序被感兴趣的 handler 处理；
//! - [`server::Server`]：负责解码请求、路由、调用链、写回回复，并可在一个 binding
//!   上监听、循环接受子流。
//!
//! # 与 Salvo 的对应关系
//!
//! - `TrReqHandler` 类似于 Salvo 的 `Handler`；
//! - `HandlerChain` 类似于 Salvo 的 handler 链 / 中间件链；
//! - `Server` 类似于 Salvo 的 `Service`，负责把请求交给匹配的链处理。
//!
//! # 子流从哪来
//!
//! 本模块**不重复发明 channel**：一条子流的收发半边由 `abs_smux` 的
//! `ChannelHandle::accept_async` 交出，[`server::Server::serve_listener_async`]
//! 只负责「监听 → 接受 → 处理」这条循环。

pub mod config;
pub mod handler;
pub mod server;

mod alloc_config_;

pub use alloc_config_::{ServingRingPrepare, TrServingAllocConfig};
pub use config::TrServingConfig;
pub use handler::{BoxedFuture, FlowCtrl, HandlerChain, HandlerError, TrReqHandler};
pub use server::{ServeError, Server, SessionContext};
