//! `mptp_smux_v1_demo` 的场景层：目录 + 每行星后缀流 + Bevy 客户端。
//!
//! 本 crate 的第一目标不是太阳系模拟器，而是用真实业务压测 `mptp_smux` 的公开 API：
//! 目录发现（`View` / `Head`）、选择性订阅（每颗行星一个 `Pull`）、无限后缀流
//! （`SuffixStream::write_chunk_async`）、退订与重新订阅（丢弃 / 新建 `Pull`）。
//!
//! # 分层
//!
//! | 模块 | 职责 |
//! | --- | --- |
//! | [`scene::protocol_`] | 业务线上契约：denco 类型、URI、头键、编解码 |
//! | [`scene::state_`] | 行星目录与 per-planet 订阅中心（业务状态） |
//! | [`scene::handler_`] | 单一 `TrServeHandler`：路由 + join / update / View / Head / Pull |
//! | [`scene::cfg_`] | compio 两端的 `mptp_smux` 配置与 ring 准备工厂 |
//! | [`scene::conn_`] | compio TCP ↔ smux 握手装配 |
//!
//! 传输层（建流、前缀、体边界、后缀半关闭、收尾）全部走 `mptp_smux` 公开 API；
//! 本 crate 只写业务，不复制会话逻辑。
//!
//! # `mptp_smux_v1_demo`: the scene layer
//!
//! The primary goal is to stress the public `mptp_smux` API with a real workload:
//! directory discovery, per-planet selective subscriptions, an infinite suffix stream,
//! unsubscribe / resubscribe. Everything transport-related stays inside `mptp_smux`.

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
// denco 的 GAT future 与多层 helper 让单条 `async fn` 的类型嵌套很深，默认上限不够。
#![recursion_limit = "256"]

pub mod scene;
