//! 类型化 Pull / Push 流与类型头协议。
//!
//! 本模块是 `rpc_core_v1` 对「suffix 是裸字节」这一传输事实的**类型化封装**：
//! 业务流对象仍然留在 `mptp_smux`，这里只提供
//!
//! - [`Just`]：读方向的一次性守卫，`take` 才提交本次读取；
//! - [`Slot`]：写方向的一次性守卫，`set_async` 完成编码 + 写 chunk；
//! - [`TypedJustFlux`] / [`TypedSlotFlux`]：在任意 `TrBuffRead` / [`TrSuffixWrite`]
//!   之上逐项推进的类型化 flux；
//! - [`DataTypeId`] / [`TrDataTypeId`] / [`AcceptType`] / [`RpcDataTypeInfo`]：
//!   把端点元信息与 MPTP 标准头连起来。
//!
//! 设计决策、方向表与版本规则见
//! `mptp_rpc/dev-notes/rpc-typed-push-pull-20261114-1300.md`。
//!
//! # `stream`: typed Pull / Push and the type-header protocol
//!
//! This module wraps the byte-level suffix stream with one-shot guards and typed fluxes,
//! and connects endpoint metadata to the MPTP standard headers `Body_Type`,
//! `Data_Type_Id` and `Accept`. Business payloads stay in `rpc_core_v1`; the transport
//! crates remain byte-only.

use core::marker::PhantomData;

mod error_;
mod just_;
mod slot_;
mod types_;

pub use error_::{StreamError, StreamFuture};
pub use just_::{Just, JustIter, TypedJustFlux};
pub use slot_::{Slot, SlotIter, TrSuffixFinish, TrSuffixWrite, TypedSlotFlux};
pub use types_::{
    AcceptType, BodyCodec, DataTypeId, RpcDataTypeInfo, RpcEndpointInfo, TrDataTypeId,
    TypeHeaderError, TypePolicy, TypeRejection, apply_request_type_headers,
    apply_response_type_headers, read_accept, read_body_codec, read_data_type_id,
    validate_request_types, validate_response_types, write_accept, write_body_codec,
    write_data_type_id,
};

/// 各处 flux / guard 共用的「生命周期 + 动量」标记。
///
/// `&'f mut ()` 把 guard 绑在流生命周期上（不能 `'static` 逃逸），`fn() -> T` 只为把
/// 业务类型参数带进 `PhantomData`，不表达所有权。
pub(crate) type Marker_<'f, T> = PhantomData<(&'f mut (), fn() -> T)>;
