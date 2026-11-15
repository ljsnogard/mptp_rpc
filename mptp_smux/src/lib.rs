//! `mptp_smux`：MPTP 协议在 [`abs_smux`](crate::x_deps::abs_smux) 之上的**通用实现**。
//!
//! 本 crate 是 `mptp_core` 拆分出来的第二块：协议本身的抽象与形状在 [`abs_mptp`]，而「怎么在
//! 一个多流连接上把它跑起来」在这里。它与**用户业务类型无关**：报文前缀的线上格式由
//! [`mptp_wire`] 拥有，业务体与后缀流的序列化则仍由使用者决定。
//!
//! 换句话说，本 crate 剩下的是**传输绑定 + 一次请求的会话驱动 + 两端角色包装**：
//! 打开 / 接受 smux 子流、注入 ring 内存、按 MPTP 顺序驱动一次请求、传播取消并把传输
//! 错误映射成公开错误；具体的字节格式不属于这里。
//!
//! # 四块内容
//!
//! - [`client`]：建流 → 交出 ring → 发前缀 → 收回复
//!   （[`MptpClient`](client::MptpClient) / [`MptpSession`](client::MptpSession)）；
//! - [`serving`]：监听 → 收建流 → 交出 ring → 解码 → 交给单一 handler
//!   （[`MptpServlet`](serving::MptpServlet) / [`ServingSession`](serving::ServingSession)）；
//! - [`messaging`]：指向 [`mptp_wire`] 的兼容别名；具体线格式由该 crate 拥有；
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
//! user's business types: the prefix wire format is owned by [`mptp_wire`], while body and
//! suffix serialization stay with the application. What remains here is the transport
//! binding, the per-request session driver and the client/server role wrappers.
//!
//! Ring memory is injected the way upstream demands it: a *factory* built from a per-channel
//! *environment*, declared as a **type** in the configuration so that no instance ever has to
//! be handed to a constructor.

#![no_std]
#![feature(allocator_ext)]
#![cfg_attr(any(feature = "client", feature = "server"), feature(impl_trait_in_assoc_type))]
#![feature(unboxed_closures)]

extern crate alloc;

#[cfg(feature = "client")]
pub mod client;
#[cfg(feature = "server")]
pub mod serving;

#[cfg(any(feature = "client", feature = "server"))]
mod body_;
#[cfg(any(feature = "client", feature = "server"))]
mod copy_;
mod error_;
mod method_set_;
mod ring_prepare_;
#[cfg(any(feature = "client", feature = "server"))]
mod session_core_;
mod session_state_;

// 兼容旧路径：`mptp_smux::messaging::*` 仍然可用，但类型由 `mptp_wire` 拥有。
pub use mptp_wire as messaging;
pub use mptp_wire::{
    HeaderEntry, HeaderKey, HeaderVal, Headers, RequestPrefix, ResponsePrefix, U16OrStr,
};
pub use error_::{ClientError, ProtocolError, ServeError, StateError, TransportError};
pub use method_set_::MethodSet;
pub use ring_prepare_::{PeerRole, RingBuffs, TrPrepFactory, TrPrepareCtx};
pub use session_state_::ServingState;

/// 协议约定的**默认服务垛口**：`0x80`。
///
/// 7 种 access method 的请求在没有显式指定目标时都发往这里，与 HTTP 默认端口相似。它是**协议
/// 常量**；而「它在本端垛口类型（`Dock<u8>` / `Dock<u16>` / `Dock<u32>`……）上分别长什么样」
/// 是本端的事实，因此由配置回答
/// （[`client::config::TrConfig::default_serving_dock`]），本 crate 只为它定一个数值。
pub const K_DEFAULT_SERVING_DOCK: u8 = 0x80;

/// 本 crate 依赖的外部 crate 转出，供下游不必逐条声明版本。
///
/// 这不是稳定的公共 API 面；下游需要时优先使用具体模块。
pub mod x_deps {
    pub use abs_buff;
    pub use abs_buff::x_deps::{abs_cancel, anylr, funty, gen_mcf2};
    pub use abs_mptp;
    pub use abs_mptp::x_deps::abs_smux;
    pub use abs_smux::x_deps::abs_mm;
    pub use mm_ptr;
    pub use mptp_wire;
    // 兼容旧的 `mptp_smux::x_deps::{abs_denco, denco_macro, mp_denco}` 路径；
    // 实际依赖由 `mptp_wire` 持有，本 crate 不直接依赖 denco。
    pub use mptp_wire::x_deps::{abs_denco, denco_macro, mp_denco};
}

#[cfg(test)]
mod tests_ {
    use abs_mptp::spec::access_method::AccessMethod;

    /// 测试 `mptp_smux::messaging` 兼容别名仍导出 `mptp_wire` 的同一批类型。
    /// Test that the `mptp_smux::messaging` compatibility alias still exposes the same
    /// `mptp_wire` types.
    ///
    /// - 手段：用别名路径构造 `RequestPrefix`，再交给要求 `mptp_wire::RequestPrefix`
    ///   的函数。
    /// - 判断：编译通过，说明两条路径指向同一个类型；运行期只检查构造成功。
    ///
    /// - Means: build a `RequestPrefix` through the alias path and pass it to a function
    ///   requiring `mptp_wire::RequestPrefix`.
    /// - Verdict: it compiles, proving both paths name the same type; at runtime it only
    ///   checks construction.
    #[test]
    fn messaging_alias_exposes_wire_types_() {
        fn assert_same(_: mptp_wire::RequestPrefix) {}

        let prefix: crate::messaging::RequestPrefix =
            crate::messaging::RequestPrefix::new(AccessMethod::Call, "/alias");
        assert_same(prefix);

        let root_prefix: crate::RequestPrefix =
            mptp_wire::RequestPrefix::new(AccessMethod::Call, "/root");
        assert_same(root_prefix);
    }
}
