//! `rpc_core_v1`：`smux_v1` 之上的 MPTP RPC 核心库。
//!
//! # 当前阶段：controller 形态的 RPC 运行时
//!
//! 本 crate 为 `#[rpc(interface = "...")]` controller 形态提供运行时底座：
//!
//! - 客户端 / 服务端配置（连接、垛口、ring 工厂、分配约定）；
//! - [`crate::messaging::RpcError`] / [`crate::messaging::RpcResult`] 与业务状态码映射；
//! - 客户端通用调用 [`crate::client::call_endpoint_async`]（controller 形态代理的方法糖）；
//! - 服务端 body 编解码辅助 [`crate::serving`]：请求体解码、回复体 / 后缀流写出、
//!   业务错误 → 状态映射（[`crate::serving::TrReplyError`]）；
//! - 常用类型与宏集中在 [`preludes`]：`use rpc_core_v1::preludes::*;` 即可覆盖协议、
//!   客户端与服务端的大部分入口。
//!
//! 服务端的路由与处理器由 `mptp_serving` 的 [`RadixRouter`](mptp_serving::RadixRouter) 承担；
//! `#[rpc]` 宏（`rpc_svc_macro`）为每个端点生成 [`TrEndpointHandler`](mptp_serving::TrEndpointHandler)
//! 并登记进路由表。线上格式由 `mptp_wire` 拥有，本 crate 不再重复实现。
//!
//! # 为什么两端在同一个库里
//!
//! MPTP 是**双向互相提供服务**的协议：一条连接的两端都可以发请求、也都可以提供服务。因此
//! 本 crate 必须同时给出客户端与服务端的形状：客户端有通用 unary / endpoint 调用，服务端
//! 有 body 编解码辅助。
//!
//! # `rpc_core_v1`: the RPC core on top of `smux_v1`
//!
//! The crate provides the runtime behind `#[rpc]`: the unified error type, the client
//! endpoint call, and the serving body helpers. Routing and endpoint handling live in
//! `mptp_serving`; the wire format belongs to [`mptp_smux`], and body framing belongs to
//! `abs_mptp`.

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod client;
pub mod messaging;
pub mod preludes;
pub mod serving;

mod config_;
pub mod prep;

pub use config_::{ClientConfig, ServingConfig, TrDefaultDock, TrUserConn};

/// 本 crate 依赖的外部 crate 转出，供下游与宏生成代码不必逐条声明版本。
///
/// 这不是稳定的公共 API 面；下游需要时优先使用 `rpc_core_v1::preludes` 或具体模块。
pub mod x_deps {
    pub use abs_art_facade;
    pub use abs_denco;
    pub use abs_rpc_serv;
    pub use buffex;
    pub use buffex::x_deps::{abs_async_iter, abs_buff, abs_cancel, anylr, funty, gen_mcf2};
    pub use denco_macro;
    pub use futures;
    pub use mm_ptr;
    pub use mp_denco;
    pub use mptp_smux;
    pub use mptp_smux::x_deps::{abs_mm, abs_mptp, abs_smux};
    pub use mptp_wire;
}
