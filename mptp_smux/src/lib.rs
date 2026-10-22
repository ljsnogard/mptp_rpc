//! `mptp_smux`：MPTP 协议在 [`abs_smux`](crate::x_deps::abs_smux) 之上的**通用实现**。
//!
//! 本 crate 是 `mptp_core` 拆分出来的第二块：协议本身的抽象与形状在 [`abs_mptp`]，而「怎么在
//! 一个多流连接上把它跑起来」在这里。它与**用户业务类型无关**：报文前缀的线上格式由
//! [`messaging`] 自己拥有（基于 `mp_denco`），业务体与后缀流的序列化则仍由使用者决定。
//!
//! # 四块内容
//!
//! - [`client`]：建流 → 交出 ring → 发前缀 → 收回复
//!   （[`MptpClient`](client::MptpClient) / [`MptpSession`](client::MptpSession)）；
//! - [`serving`]：监听 → 收建流 → 交出 ring → 解码 → 路由 → 交给 handler
//!   （[`MptpServer`](serving::MptpServer)）；
//! - [`messaging`]：请求 / 回复前缀的**自有线格式**；
//! - [`TrPrepFactory`] 一族：**ring 内存怎么准备**的注入契约，两端各一个环境类型；
//! - [`x_deps`]：本 crate 依赖的对外重导出，供下游不必逐条声明版本。
//!
//! # ring 内存为什么必须由调用者注入
//!
//! 上游 `abs_smux` 把「这条子流用哪种智能指针、多大、从哪来」放在 `accept_async` 的**实参**
//! 上（见 [`TrPrepareRing`](crate::x_deps::abs_smux::chan::TrPrepareRing)），而不是连接的静态配置。本 crate
//! 顺着这条契约给出一个**工厂**：
//!
//! - 工厂由**环境**现场构造（[`TrPrepFactory::with_ctx`]），环境里有两个 dock 与角色，所以
//!   「按连接 / 按本条子流选内存」在类型层面就表达得出来；
//! - 默认策略以**类型**的形式声明在配置里（`DefaultPrepFactory`），因此没有实例需要提前注入，
//!   `Client::new` / `Server::new` 的签名保持干净；
//! - 客户端另有一条「调用者当场交出准备器」的覆盖路径；服务端没有——一次 `serve_listener`
//!   要接受 N 条子流，而一个准备器交出即被消费。
//!
//! # `mptp_smux`: the generic MPTP implementation on top of `abs_smux`
//!
//! This crate is the second half of the `mptp_core` split: `abs_mptp` holds the protocol
//! shapes, and this crate runs them over a multiplexed connection. It knows nothing about the
//! user's business types: the prefix wire format is owned here by [`messaging`] (on top of
//! `mp_denco`), while body and suffix serialization stay with the application.
//!
//! Ring memory is injected the way upstream demands it: a *factory* built from a per-channel
//! *environment*, declared as a **type** in the configuration so that no instance ever has to
//! be handed to a constructor.

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod client;
pub mod messaging;
pub mod serving;

mod ring_prepare_;

pub use messaging::{
    HeaderEntry, HeaderKey, HeaderVal, Headers, RequestPrefix, ResponsePrefix, U16OrStr,
};
pub use ring_prepare_::{PeerRole, RingBuffs, TrPrepFactory, TrPrepareCtx};

/// 协议约定的**默认服务垛口**：`0x80`。
///
/// 7 种 access method 的请求在没有显式指定目标时都发往这里，与 HTTP 默认端口相似。它是**协议
/// 常量**；而「它在本端垛口类型（`Dock<u8>` / `Dock<u16>` / `Dock<u32>`……）上分别长什么样」
/// 是本端的事实，因此由配置回答
/// （[`client::config::TrConfig::default_serving_dock`]），本 crate 只为它定一个数值。
pub const K_DEFAULT_SERVING_DOCK: u8 = 0x80;

pub mod x_deps {
    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr, funty, gen_mcf2};
    pub use abs_denco;
    pub use abs_mptp;
    pub use abs_mptp::x_deps::abs_smux;
    pub use abs_smux::x_deps::abs_mm;
    pub use denco_macro;
    pub use mm_ptr;
    pub use mp_denco;
    pub use tlist;
}
