//! 服务端侧的公开契约。
//!
//! # 为什么不复用 [`TrClientConfig`](crate::client::config::TrClientConfig)
//!
//! 服务端与客户端**可能跑在两个不同的进程里**，由两份不同的代码编译、可以各自
//! 独立升级。它们的请求 / 响应类型不必是同一个 Rust 类型（甚至不必来自同一个
//! crate），连接与内存策略也各自独立。把两者合成一个配置，就等于假定「客户端与
//! 服务端永远是同一份二进制」，而这恰恰是 RPC 框架不该假定的东西。
//!
//! 因此本模块的 [`TrServingConfig`] 自带一整套关联类型，与客户端那份**没有任何
//! 共享**。需要同时扮演两种角色的进程，可以写一个同时实现两者的类型（甚至让两边
//! 的关联类型取同一个具体类型）——但那是使用者的选择，不是本库的默认。

use abs_smux::{
    chan::TrChannelHandle,
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};

use super::alloc_config_::TrServingAllocConfig;

/// 一个服务端实现所需的全部配置。
pub trait TrServingConfig {
    /// 复用连接类型（`abs_smux` 的连接根对象）。
    ///
    /// `Data` 收紧为 `u8`：MPTP 的报文前缀与报文体的线上编码都以字节为单位。
    type MuxConn: TrConnection<Config: TrMuxConfig<Data = u8>>;

    /// 服务端侧的资源分配约定（共享连接对象 + 每条子流的 ring 内存）。
    type AllocCfg: TrServingAllocConfig;

    /// 本服务端**接收**的请求类型。
    type Request: crate::messaging::TrRpcRequest;

    /// 本服务端**发出**的响应类型。
    type Response: crate::messaging::TrRpcResponse;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 从配置派生出来的类型别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type MuxConn<TySvcCfg> = <TySvcCfg as TrServingConfig>::MuxConn;

pub type MuxCfg<TySvcCfg> = <MuxConn<TySvcCfg> as TrConnection>::Config;

pub type Dock<TySvcCfg> = <MuxCfg<TySvcCfg> as TrMuxConfig>::Dock;

pub type DockBinding<TySvcCfg> = <MuxConn<TySvcCfg> as TrConnection>::DockBinding;

pub type Listener<TySvcCfg> = <DockBinding<TySvcCfg> as TrDockBinding<MuxCfg<TySvcCfg>>>::Listener;

/// 本端待裁决的子流句柄。
///
/// 注意它取自 [`Listener`] 的 `income_async`，**不是** `DockBinding` 上那个
/// `ChannelHandle`：`abs_smux` 里这是两条彼此独立的关联类型，服务端只走
/// 「监听 → 收请求」这条路径，因此以 listener 侧为准。客户端走的是
/// `open_channel_async`，它那边以 binding 侧为准——两者在实际实现里通常是同一个
/// 具体类型，但契约上不必相同。
pub type ChannelHandle<TySvcCfg> =
    <Listener<TySvcCfg> as TrChannelListener<MuxCfg<TySvcCfg>>>::ChannelHandle;

/// 本端子流的发送半边。
///
/// 它取自 [`ChannelHandle`] 的最终裁决（`accept_async`），而不是 `DockBinding` 上的
/// `Tx`：只有 `TrChannelHandle::Tx` 才是「这条已建成子流的写半部」。
pub type ChannelTx<TySvcCfg> = <ChannelHandle<TySvcCfg> as TrChannelHandle<MuxCfg<TySvcCfg>>>::Tx;

/// 本端子流的接收半边，来源同 [`ChannelTx`]。
pub type ChannelRx<TySvcCfg> = <ChannelHandle<TySvcCfg> as TrChannelHandle<MuxCfg<TySvcCfg>>>::Rx;

pub type Request<C> = <C as TrServingConfig>::Request;

pub type Response<C> = <C as TrServingConfig>::Response;
