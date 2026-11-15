#![feature(allocator_ext)]
#![no_std]

//! RPC 服务端侧的**抽象配置契约**：供宏生成代码与核心实现共同依赖，不绑定具体运行时。
//!
//! 本 crate 只放「宏需要知道的最小形状」：
//!
//! - [`config::TrServingAllocConfig`] / [`config::TrServingConfig`]：服务端配置。
//!
//! 具体配置类型（如 `ServingConfig` / `DefaultServingAllocConfig`）在 `rpc_core_v1`；
//! 本 crate 不反向依赖它，因此宏展开与核心库可以各自演进。
//!
//! # `abs_rpc_serv`: abstract serving configuration contracts
//!
//! This crate owns only the serving configuration shapes the macro-generated code needs.
//! Concrete configuration types live in `rpc_core_v1`; the dependency direction never points back.

pub mod config;
