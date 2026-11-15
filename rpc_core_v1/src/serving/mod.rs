//! 服务端侧：配置与面向 `ServingSession` 的 body 编解码辅助。
//!
//! 这一层提供新 controller 形态 handler 所需的底座：
//!
//! - [`TrServingConfig`] / [`TrServingAllocConfig`]：服务端配置契约（定义在
//!   `abs_rpc_serv`，本模块转出并给出缺省实现）；
//! - [`TrReplyError`]：业务错误 → 状态回复的契约；
//! - [`decode_body_async`] / [`write_ok_async`] / [`write_status_async`] /
//!   [`write_stream_item_async`] / [`write_typed_value_async`]：请求体解码与回复体 /
//!   后缀流写出（后者按方法级 [`RpcDataTypeInfo`] 写类型头）。
//!
//! # `serving`: configuration and body codec helpers
//!
//! The new controller handlers reach `&mut ServingSession` through `MptpContext` and use
//! these helpers to decode the request body and write replies.

mod body_;
mod types_;

pub mod config;

pub use body_::{
    TrReplyError, decode_body_async, write_ok_async, write_status_async, write_stream_item_async,
    write_typed_value_async,
};
pub use config::{DefaultServingAllocConfig, TrServingAllocConfig, TrServingConfig};
pub use types_::{
    AcceptType, BodyCodec, DataTypeId, RpcDataTypeInfo, RpcEndpointInfo, RpcMethodInfo,
    RpcServiceInfo, TrDataTypeId, TypeHeaderError, TypePolicy, TypeRejection,
    apply_request_type_headers, apply_response_type_headers, read_accept, read_body_codec,
    read_data_type_id, typed_response, validate_request_types, validate_response_types,
    write_accept, write_body_codec, write_data_type_id,
};
