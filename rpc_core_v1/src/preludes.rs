//! 常用类型与宏的 prelude。
//!
//! 业务 crate 通常需要同时引用 RPC 配置、客户端代理、服务端 body 辅助、取消令牌与
//! `denco` / `gen_mcf2` 宏。把这几组入口集中到一处后，协议、客户端与服务端代码可以统一写：
//!
//! ```
//! use rpc_core_v1::preludes::*;
//!
//! fn _assert_result(_: RpcResult<()>) {}
//! ```
//!
//! 这里只做**转出**，不改变任何类型的可见性、trait 约束或宏行为。
//!
//! # 包含哪些东西
//!
//! - 配置：`ClientConfig` / `ServingConfig` / `TrRpcClientConfig` / `TrServingConfig` /
//!   缺省分配约定；
//! - 客户端：`MptpClient` / `ClientProxy` / `ServiceSelector` / `TrRpcClientExt` /
//!   `call_endpoint_async`（controller 形态代理用）；
//! - 服务端：`MptpServlet` + 新框架 body 辅助（`decode_body_async` / `write_ok_async` /
//!   `write_status_async` / `write_stream_item_async` / `TrReplyError`）；
//! - 协议与错误：`RpcError` / `RpcResult` / `TrRpcInterface` / `Status`；
//! - 类型化流：`Just` / `Slot` / `TypedJustFlux` / `TypedSlotFlux` / `open_pull_async` /
//!   `open_push_async`，以及类型头契约 `DataTypeId` / `TrDataTypeId` / `AcceptType` /
//!   `RpcDataTypeInfo` / `TypePolicy`；
//! - 取消：`TrMayCancel` / `TrCancellationToken` / `NonCancellableToken`；
//! - 宏与派生：`gen_may_cancel_future` / `TrEncodable` / `TrDecodable`；
//! - 常用底座：`Shared` / `CoreAlloc` / `TrConnection`。
//!
//! # `preludes`: the common RPC imports
//!
//! A single glob gives protocol, client and serving code the configuration types, client
//! proxies, serving body helpers, cancellation traits, and the `denco` / `gen_mcf2` macros.
//! This module only re-exports; it does not change visibility or behavior.

/// denco 类型指纹的 facade（`type-id` feature）。
///
/// 与 `stream` 的转出一致：不转出 derive，使用 derive 的 crate 仍需直接依赖
/// `abs_denco`（生成的代码硬编码 `abs_denco::...`）。
#[cfg(feature = "type-id")]
pub use crate::stream::{TrHasTypeId, TypeIdEntry, check_unique, denco_type_ids};
pub use crate::{
    ClientConfig, ServingConfig, TrDefaultDock, TrUserConn,
    client::{
        ClientProxy, DefaultRpcAllocConfig, ServiceSelector, TrRpcClientConfig, TrRpcClientExt,
        call_endpoint_async, call_endpoint_typed_async, open_pull_async, open_push_async,
    },
    messaging::{RpcError, RpcResult, TrRpcInterface},
    serving::{
        DefaultServingAllocConfig, RpcDataTypeInfo, RpcEndpointInfo, RpcMethodInfo, RpcServiceInfo,
        TrReplyError,
        TrServingAllocConfig, TrServingConfig, decode_body_async, typed_response,
        validate_request_types, write_ok_async, write_status_async, write_stream_item_async,
    },
    stream::{
        AcceptType, BodyCodec, DataTypeId, Just, Slot, TrDataTypeId, TypePolicy, TypedJustFlux,
        TypedSlotFlux,
    },
    x_deps::{
        abs_async_iter::{TrAsyncIterator, TrFlux},
        abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel},
        abs_mm::CoreAlloc,
        abs_mptp::{serving::TrReqHandler, spec::Status},
        abs_smux::conn::TrConnection,
        denco_macro::{TrDecodable, TrEncodable},
        gen_mcf2::gen_may_cancel_future,
        mm_ptr::Shared,
        mptp_smux::{client::MptpClient, serving::MptpServlet},
    },
};
