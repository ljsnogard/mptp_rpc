//! `core_rpc_mptp_smux_v1`：`smux_v1` 之上的 MPTP RPC 核心库。
//!
//! # 当前阶段：Phase 1（宏所需的最小运行时）
//!
//! 本 crate 正在从「MPTP 的 rmp-serde 实现 + 七种 access method 的双端形状」重构为
//! **RPC 语义层**。当前已经落地 `#[rpc(service = "...")]` 宏所依赖的最小运行时：
//!
//! - 客户端 / 服务端配置（连接、垛口、ring 工厂、分配约定）；
//! - [`RpcError`] / [`RpcResult`] 与业务状态码映射；
//! - [`TrRpcInterface`] 与 `/{interface}/{service}` 路径拼装；
//! - 客户端极简代理 [`ClientProxy`]、[`TrRpcClientExt`] 与通用 unary 调用；
//! - 服务端一次调用的上下文 [`RpcCallCtx`]：解码请求、写回复、收尾；
//! - 服务端两级 Trie 路由器 [`RpcRouter`] 与 [`RpcJob`] / [`ServiceRoute`]：按
//!   interface → service 分派，每个 service 一个队列；
//! - body 的 MessagePack 流式编解码：ring 边编码边发送，定长 / 分块帧都可解码。
//!
//! `#[rpc(service = "...")]` 的 worker 与 `#[rpc(path = "...", services = [...])]` 的
//! worker 组都建立在上面这些运行时之上；宏本身不替使用者 spawn 线程或选择运行时。
//! 线上格式已由 [`mptp_smux`] 拥有，body 的定长 / 分块帧由 [`abs_mptp`] 拥有，本 crate
//! 都不再重复实现。
//!
//! # 已实现的契约
//!
//! - 路径只由 **interface + service** 组成，方法名**不出现在 path 上**；
//! - 一个 service 的多个关联函数组织进一个 **body enum**；变体名直接用函数名，字段直接用
//!   参数名，不再额外生成参数结构体；
//! - service trait 的 `SERVICE_PATH` 由宏注入；客户端方法由宏对统一的 [`ClientProxy`] 生成；
//! - 服务端 dispatch 由宏生成 `match`，一次调用的解码 / 回复 / 收尾由 [`RpcCallCtx`] 承担。
//!
//! # 分层位置
//!
//! ```text
//! ┌───────────────────────────────────────────────────────────────┐
//! │ 本 crate（Phase 1）：RPC 语义 + 配置 + 分配约定               │
//! ├───────────────────────────────────────────────────────────────┤
//! │ mptp_smux：开流 → 交出 ring → 收发前缀；MptpServer            │
//! ├───────────────────────────────────────────────────────────────┤
//! │ abs_mptp：协议形状（前缀 trait、访问方法、标准头、状态码、体）│
//! ├───────────────────────────────────────────────────────────────┤
//! │ abs_smux / smux_v1：多流连接契约与它的现成实现                │
//! └───────────────────────────────────────────────────────────────┘
//! ```
//!
//! # 为什么两端在同一个库里
//!
//! MPTP 是**双向互相提供服务**的协议：一条连接的两端都可以发请求、也都可以提供服务。因此
//! 本 crate 必须同时给出客户端与服务端的形状。Phase 1 之后，这条理由仍然成立：客户端有
//! 通用 unary 调用，服务端有一次调用的解码 / 回复 / 收尾上下文。
//!
//! # `core_rpc_mptp_smux_v1`: the RPC core on top of `smux_v1`
//!
//! Phase 1 provides the minimal runtime needed by `#[rpc(service = "...")]`: the unified
//! error type, interface path composition, the minimal client proxy with unary calls, the
//! per-call serving context, and MessagePack body IO. The server router and the interface
//! macro are the next phase. The wire format belongs to [`mptp_smux`], and body framing
//! belongs to [`abs_mptp`].

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod client;
pub mod serving;

mod body_;
mod config_;
mod error_;
mod interface_;
mod prep_;

pub use client::{
    ClientProxy, DefaultRpcAllocConfig, Dock, ServiceSelector, TrRpcClienAllocConfig,
    TrRpcClientConfig, TrRpcClientExt,
};
pub use config_::{ClientConfig, ServingConfig, TrDefaultDock, TrUserConn};
pub use error_::{RpcError, RpcResult};
pub use interface_::{TrRpcInterface, location};
// 使用者自己搭服务循环时，需要在每条入向子流上当场交出一份准备器；`RingBuffs` 与
// 「怎么造一块 ring 内存」由此上移到 crate 根部，省得使用者去 `mptp_smux` 里找。
pub use mptp_smux::{
    RingBuffs,
    x_deps::{abs_mm::CoreAlloc, mm_ptr::Owned},
};
pub use prep_::{
    ClientPrepFactory, K_CHANNEL_CAP, RingBuff, ServingPrepFactory, new_ring_buff_, new_ring_buffs_,
};
pub use serving::{
    DefaultServingAllocConfig, K_DEFAULT_SERVICE_QUEUE_CAP, RpcCallCtx, RpcJob, RpcRouter,
    RpcRouterError, ServiceRoute, TrServingAllocConfig, TrServingConfig,
};

pub mod x_deps {
    pub use abs_art_facade;
    pub use abs_denco;

    pub use buffex;
    pub use buffex::x_deps::{
        abs_async_iter, abs_buff, abs_cancel,
        anylr, funty, gen_mcf2,
    };
    pub use denco_macro;
    pub use futures;
    pub use mm_ptr;
    pub use mp_denco;
    pub use mptp_smux;
    pub use mptp_smux::x_deps::{abs_mm, abs_mptp, abs_smux};
}
