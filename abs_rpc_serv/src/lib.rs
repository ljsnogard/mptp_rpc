#![feature(allocator_ext)]
#![feature(unboxed_closures)]
#![no_std]

//! RPC 服务端侧的**抽象契约**：供宏生成代码与核心实现共同依赖，不绑定具体运行时。
//!
//! 本 crate 只放「宏需要知道的最小形状」：
//!
//! - [`config::TrServingAllocConfig`] / [`config::TrServingConfig`]：服务端配置；
//! - [`service::TrRpcCallCtx`]：一次调用的上下文；
//! - [`service::TrRpcService`]：一个 service 实现如何被服务端调度。
//!
//! 具体实现（`RpcCallCtx`、`RpcRouter` 等）在 `rpc_core_v1`；本 crate 不反向依赖它，
//! 因此宏展开与核心库可以各自演进。
//!
//! # `abs_rpc_serv`: abstract serving contracts
//!
//! This crate owns only the minimal shapes the macro-generated code needs: the serving
//! configuration, the per-call context and the schedulable service contract. Concrete
//! implementations live in `rpc_core_v1`; the dependency direction never points back.

pub mod config;
pub mod service;
