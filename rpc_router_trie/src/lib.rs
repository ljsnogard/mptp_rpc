//! `rpc_router_trie`：`core_rpc_mptp_smux_v1` 的前缀树 router 实现。
//!
//! 本 crate 只回答「一个 location 命中哪个 service」：
//!
//! - interface 前缀在内部 `PathTrie` 中做最长段匹配；
//! - 命中的 interface 内部，再用剩余路径匹配 service 前缀；
//! - 命中的 [`ServiceRoute`](core_rpc_mptp_smux_v1::serving::ServiceRoute) 交给
//!   [`TrRpcRouter`](core_rpc_mptp_smux_v1::serving::TrRpcRouter) 的使用者（通常是
//!   [`RpcRouterHandler`](core_rpc_mptp_smux_v1::serving::RpcRouterHandler)）。
//!
//! 拒绝回复、`RpcJob` 构造与入队都不在这里：它们由 core 的
//! [`RpcRouterHandler`](core_rpc_mptp_smux_v1::serving::RpcRouterHandler) 统一完成。
//! 这样本 crate 只依赖 core，不直接依赖 `mptp_smux` / `abs_mptp`。
//!
//! # `rpc_router_trie`: the trie-based router implementation
//!
//! This crate only answers "which service does a location hit": an interface trie followed
//! by a service trie inside the matched interface. Rejection replies, job construction and
//! delivery stay in `core_rpc_mptp_smux_v1::serving::RpcRouterHandler`.

#![forbid(unsafe_code)]

mod router_;
mod trie_;

pub use router_::{RpcRouter, RpcRouterError};
