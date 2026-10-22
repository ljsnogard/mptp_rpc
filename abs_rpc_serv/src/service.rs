//! 宏生成代码与服务端运行时之间的**最小契约**。
//!
//! 这一层刻意不依赖 `rpc_core_v1`：宏展开在协议 crate 里，服务端实现可以在任意
//! 下游 crate 里；两者只需要认识这里的两条 trait，具体上下文与路由器由核心库实现。
//!
//! - [`TrRpcCallCtx`]：一次调用的上下文。宏生成的 dispatch 只通过它解码请求、写回复，
//!   因此不绑定 `rpc_core_v1::serving::RpcCallCtx` 的具体形状；
//! - [`TrRpcService`]：一个 service 实现如何被服务端调度。它把「service 属于哪个
//!   interface」「相对路径是什么」「如何 dispatch」三件事放在一起，供
//!   `#[rpc(service_of(...))]` 为用户的 impl 块生成实现。
//!
//! 具体类型（`RpcCallCtx`、`RpcRouter` 等）由 `rpc_core_v1` 提供；本 crate 只回答
//! 「宏生成代码至少需要什么」。
//!
//! # `service`: the minimal contracts shared by generated code and the core
//!
//! This module deliberately does not depend on `rpc_core_v1`. Generated dispatch only needs
//! a per-call context abstraction, and server-side routing only needs to know the interface,
//! the relative service path and a dispatch entry. Concrete types live in the core crate.

extern crate alloc;

use alloc::string::String;
use core::error::Error;

use abs_cancel::TrMayCancel;
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mptp::spec::Status;
use mptp_smux::x_deps::{abs_cancel, abs_denco, abs_mptp};

/// 一次服务端 RPC 调用的最小上下文契约。
///
/// `#[rpc(service = "...")]` 生成的 dispatch 是泛型于上下文的：它只要求实现者能解码
/// 请求体、写成功回复与写状态回复。这样同一个 dispatch 既可由核心库的
/// `RpcCallCtx` 驱动，也可由使用者自己的上下文实现驱动。
///
/// `C` 是服务端配置类型；本 trait 不约束它，具体实现自行决定是否使用它。
///
/// # Errors
///
/// 三个方法的错误类型统一由 [`Self::Err`] 表达；核心实现使用
/// `rpc_core_v1::messaging::RpcError`。
///
/// # `TrRpcCallCtx`: the minimal per-call context
///
/// Generated dispatch is generic over this trait, so it does not depend on the concrete
/// `RpcCallCtx` shape. An implementor only has to answer decode / reply / status-reply.
pub trait TrRpcCallCtx<C> {
    /// 本上下文的错误类型。
    type Err: Error;

    /// 解码本次请求的 body。
    ///
    /// # Errors
    ///
    /// 读 body 或解码失败时返回 [`Self::Err`]。
    fn decode_call_async<'f, T>(
        &'f mut self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<T, Self::Err>> + 'f
    where
        T: TrDecodableOwned + 'f,
        Self: 'f;

    /// 把一个成功的业务值写成 `200 Ok` 回复。
    ///
    /// # Errors
    ///
    /// 编码或写入失败时返回 [`Self::Err`]。
    fn reply_ok_async<'f, T>(
        &'f mut self,
        value: T,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>> + 'f
    where
        T: TrEncodable + 'f,
        Self: 'f;

    /// 写一条非成功状态回复；`message` 原样作为 UTF-8 错误体。
    ///
    /// # Errors
    ///
    /// 写入失败时返回 [`Self::Err`]。
    fn reply_status_async<'f>(
        &'f mut self,
        status: Status,
        message: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>> + 'f
    where
        Self: 'f;
}

/// 一个可以被 RPC 服务端调度的 service 实现。
///
/// `I` 是 interface marker（例如 `ApiV1`），`C` 是服务端配置类型。宏
/// `#[rpc(service_of(ApiV1))]` 为用户的 `impl GoodsCustomerService for Impl` 生成本
/// trait 的实现；用户不需要手写。
///
/// `SERVICE_PATH` 是相对于 interface 前缀的 service 路径，例如 `customer/goods`；
/// 完整路径由核心路由在装配时把 `I::PATH` 与它拼起来（或在匹配时剥掉 interface
/// 前缀）。
///
/// # Errors
///
/// [`dispatch_async`](Self::dispatch_async) 的错误类型由 [`Self::Err`] 表达；核心实现
/// 使用 `rpc_core_v1::messaging::RpcError`。
///
/// # 对象安全
///
/// `dispatch_async` 是泛型方法且返回 `impl TrMayCancel`，本 trait 不是对象安全的。
/// 服务端路由按具体类型静态分发，不需要 `dyn`。
///
/// # `TrRpcService`: how a service implementation is scheduled
///
/// `I` is the interface marker and `C` the serving configuration. The
/// `#[rpc(service_of(...))]` macro implements this trait for the user's impl block; the
/// service path is relative to the interface. The trait is intentionally not object safe:
/// the router dispatches statically over concrete service types.
pub trait TrRpcService<I, C>: Sized {
    /// service 相对于 interface 前缀的路径。
    const SERVICE_PATH: &'static str;

    /// dispatch 的错误类型。
    type Err: Error;

    /// 调度一次调用。
    ///
    /// `ctx` 由核心路由构造；宏生成的实现会从它解码 body、调用用户方法并写回复。
    ///
    /// # Errors
    ///
    /// 解码、业务实现或写回复失败时返回 [`Self::Err`]。
    fn dispatch_async<'f, X>(
        &'f self,
        ctx: &'f mut X,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>> + 'f
    where
        C: 'f,
        X: TrRpcCallCtx<C, Err = Self::Err> + 'f;
}
