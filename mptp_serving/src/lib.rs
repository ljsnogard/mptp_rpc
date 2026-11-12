//! `mptp_serving`：MPTP 服务端框架——端点、元数据、中间件与分层路由。
//!
//! 本 crate 把「一次请求该交给哪个端点、端点携带什么元数据、中间件按元数据执行什么策略、
//! 终端处理器只写业务」这四件事分开。它**不**重做会话驱动：一次请求的前缀 / 体 / 后缀顺序、
//! 半关闭与收尾仍由 [`mptp_smux`] 承担；本 crate 只负责“在会话之上如何路由与分派”。
//!
//! 目标运行环境是 `#![no_std]` + `alloc`。所有堆上对象要么带分配器参数、要么进
//! [`matchit_route`] 的 `bumpalo` arena；`String` 一律用 [`abs_str::String<A>`]，
//! 不引入 `std::sync::OnceLock` / `RwLock` / `Mutex`。
//!
//! # 分层位置
//!
//! ```text
//! 应用 / RPC：service 实现、资源 handler（只写业务）
//!       ▲
//! mptp_serving：Endpoint / 元数据 / 中间件 / 分层 router（本 crate）
//!       ▲
//! mptp_smux：一次请求的会话驱动 + smux 绑定
//!       ▲
//! mptp_wire / abs_mptp / smux_v1
//! ```
//!
//! # 当前状态（round 1 / P0）
//!
//! 已落地**端点骨架 + 单层 router**：`Endpoint`、`TrEndpointHandler`、`RouteParams`、
//! `TrRouteMatcher` 与默认实现 `RadixRouter`（复用 `matchit_route`）。元数据与中间件链
//! 属 P2，尚未来到这里。

#![no_std]

extern crate alloc;

mod endpoint_;
mod handler_;
mod route_params_;
mod router_;

pub use endpoint_::Endpoint;
pub use handler_::TrEndpointHandler;
pub use route_params_::RouteParams;
pub use router_::{MatchOutcome, RadixRouter, TrRouteMatcher};

/// 访问方法集合：来自 [`mptp_smux`]，本 crate 不重复定义。
pub use mptp_smux::MethodSet;

/// 本 crate 依赖的外部 crate 转出，供下游不必逐条声明版本。
///
/// 这不是稳定的公共 API 面；下游需要时优先使用具体模块。
pub mod x_deps {
    pub use abs_mptp;
    pub use matchit_route;
    pub use mptp_smux;
}
