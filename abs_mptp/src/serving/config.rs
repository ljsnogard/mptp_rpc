//! 服务端侧的公开契约。
//!
//! # 为什么不复用 [`TrConfig`](crate::client::config::TrConfig)
//!
//! 服务端与客户端**可能跑在两个不同的进程里**，由两份不同的代码编译、可以各自
//! 独立升级。它们的请求 / 响应类型不必是同一个 Rust 类型（甚至不必来自同一个
//! crate），连接与内存策略也各自独立。把两者合成一个配置，就等于假定「客户端与
//! 服务端永远是同一份二进制」，而这恰恰是 RPC 框架不该假定的东西。
//!
//! 因此本模块的 [`TrConfig`] 自带一整套关联类型，与客户端那份**没有任何
//! 共享**。需要同时扮演两种角色的进程，可以写一个同时实现两者的类型（甚至让两边
//! 的关联类型取同一个具体类型）——但那是使用者的选择，不是本库的默认。

use abs_smux::{
    chan::TrChannelHandle,
    conf::TrMuxConfig,
    conn::{TrChannelListener, TrConnection, TrDockBinding},
};

use crate::spec::{TrHeaders, TrHeadersMut};

/// 一个服务端实现所需的全部配置。
pub trait TrConfig {
    /// 复用连接类型（`abs_smux` 的连接根对象）。
    ///
    /// `Data` 收紧为 `u8`：MPTP 的报文前缀与报文体的线上编码都以字节为单位。
    type MuxConn: TrConnection<Config: TrMuxConfig<Data = u8>>;

    type ReqHeaders: TrHeaders;

    type RespHeaders: TrHeadersMut;

    /// 本服务端**接收**的请求类型。
    type ReqPrefix: crate::messaging::TrRequestPrefix;

    /// 本服务端**发出**的响应类型。
    type RespPrefix: crate::messaging::TrResponsePrefix;
}


//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 从配置派生出来的类型别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type MuxConn<TySrvCfg> = <TySrvCfg as TrConfig>::MuxConn;

pub type MuxCfg<TySrvCfg> = <MuxConn<TySrvCfg> as TrConnection>::Config;

pub type Dock<TySrvCfg> = <MuxCfg<TySrvCfg> as TrMuxConfig>::Dock;

pub type DockBinding<TySrvCfg> = <MuxConn<TySrvCfg> as TrConnection>::DockBinding;

pub type Listener<TySrvCfg> = <DockBinding<TySrvCfg> as TrDockBinding<MuxCfg<TySrvCfg>>>::Listener;

/// 本端待裁决的子流句柄。
///
/// 注意它取自 [`Listener`] 的 `income_async`，**不是** `DockBinding` 上那个
/// `ChannelHandle`：`abs_smux` 里这是两条彼此独立的关联类型，服务端只走
/// 「监听 → 收请求」这条路径，因此以 listener 侧为准。客户端走的是
/// `open_channel_async`，它那边以 binding 侧为准——两者在实际实现里通常是同一个
/// 具体类型，但契约上不必相同。
pub type ChannelHandle<TySrvCfg> =
    <Listener<TySrvCfg> as TrChannelListener<MuxCfg<TySrvCfg>>>::ChannelHandle;

/// 本端子流的发送半边。
///
/// 它取自 [`ChannelHandle`] 的最终裁决（`accept_async`），而不是 `DockBinding` 上的
/// `Tx`：只有 `TrChannelHandle::Tx` 才是「这条已建成子流的写半部」。
pub type ChannelTx<TySrvCfg> = <ChannelHandle<TySrvCfg> as TrChannelHandle<MuxCfg<TySrvCfg>>>::Tx;

/// 本端子流的接收半边，来源同 [`ChannelTx`]。
pub type ChannelRx<TySrvCfg> = <ChannelHandle<TySrvCfg> as TrChannelHandle<MuxCfg<TySrvCfg>>>::Rx;

pub type ReqHeaders<TySrvCfg> = <TySrvCfg as self::TrConfig>::ReqHeaders;

pub type RespHeaders<TySrvCfg> = <TySrvCfg as self::TrConfig>::RespHeaders;

pub type ReqPrefix<TySrvCfg> = <TySrvCfg as TrConfig>::ReqPrefix;

pub type RespPrefix<TySrvCfg> = <TySrvCfg as TrConfig>::RespPrefix;
