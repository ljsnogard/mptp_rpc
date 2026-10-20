//! 协议规范里的公共词汇：访问方法、状态码、标准头键值，以及头表本身的契约。
//!
//! 这些类型是**线上契约**的一部分，与具体业务无关；`client` 与 `serving` 两侧都按它们
//! 描述报文，因此它们不能被打上任何一侧的烙印。
//!
//! # `spec`: the protocol's shared vocabulary
//!
//! Access methods, status codes, standard header keys/values, and the contract of the
//! header map itself. All of these are part of the **wire contract** and belong to
//! neither the client nor the server side.

mod status_;
mod std_header_;

pub mod access_method;

pub use status_::Status;
pub use std_header_::{
    NumOrStr, StdHeaderKey, StdHeaderVal,
    TrHeaders, TrHeadersMut, TrNumOrStr,
};
