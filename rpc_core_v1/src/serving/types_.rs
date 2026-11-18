//! 服务端的类型元信息与回复头 helper。
//!
//! 端点元信息（[`RpcDataTypeInfo`] / [`RpcEndpointInfo`]）与纯函数校验
//! [`validate_request_types`] 定义在 `rpc_core_v1::stream`；本模块只做两件事：
//!
//! 1. 把这两个标记类型转出到 `serving` 路径，方便 handler / 中间件使用；
//! 2. 提供 [`typed_response`]，把 Pull / Push / Call 的响应头一次性写全。
//!
//! 中间件实现仍应放在上层（`rpc_serv_ext` / 应用）：本 crate 不依赖 `mptp_serving`。
//!
//! # `serving::types_`: endpoint type metadata and typed replies
//!
//! The marker types and the pure validator live in `rpc_core_v1::stream`; this module
//! re-exports them under the serving path and adds [`typed_response`] to write the reply
//! headers in one place.

use abs_mptp::spec::{Status, access_method::AccessMethod};
use mptp_smux::{
    SessionAlloc,
    serving::{ResponseBuilder, ServingSession, config::TrConfig},
};
use mptp_wire::{Headers, ResponsePrefix};

pub use crate::stream::{
    AcceptType, BodyCodec, DataTypeId, RpcDataTypeInfo, RpcEndpointInfo, RpcMethodInfo,
    RpcServiceInfo, TrDataTypeId, TypeHeaderError, TypePolicy, TypeRejection,
    apply_request_type_headers, apply_response_type_headers, read_accept, read_body_codec,
    read_data_type_id, validate_request_types, validate_response_types, write_accept,
    write_body_codec, write_data_type_id,
};
use crate::x_deps::{abs_mptp, mptp_smux, mptp_wire};

/// 用端点元信息构造回复 helper，写全本方向的 `Body_Type` / `Data_Type_Id`。
///
/// Pull 响应写 suffix codec + item 类型；Call / Push 响应写 body codec + body 类型。
/// 返回的 [`ResponseBuilder`] 仍可继续加自定义头，最后按需 `.no_body_async()` /
/// `.sized_async(...)` / `.suffix_stream_async()`。
///
/// # Examples
///
/// ```ignore
/// use rpc_core_v1::serving::typed_response;
/// use rpc_core_v1::x_deps::abs_mptp::spec::{Status, access_method::AccessMethod};
///
/// let mut builder = typed_response(
///     ctx.session_mut(),
///     Status::Ok,
///     AccessMethod::Pull,
///     &info,
/// );
/// let mut stream = builder.suffix_stream_async().await?;
/// ```
pub fn typed_response<'a, C>(
    session: &'a mut ServingSession<C>,
    status: Status,
    method: AccessMethod,
    info: &RpcDataTypeInfo,
) -> ResponseBuilder<'a, C>
where
    C: TrConfig + 'a,
{
    let alloc = session.allocator().clone();
    let mut headers = Headers::<SessionAlloc>::new_in(alloc.clone());
    apply_response_type_headers(&mut headers, method, info);
    session.response_prefix(ResponsePrefix::with_headers(status, Option::Some(headers)))
}
