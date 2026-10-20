use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrDockBinding, TrConnection},
    conf::TrMuxConfig,
};
use abs_mm::res_man::TrStrongShared;

/// 一个 MPTP 客户端实现所需的全部配置。
///
/// 复用连接的形状由 `abs_smux` 的 [`TrConnection`] 给出；本 trait 再补上客户端自己的
/// 请求 / 响应类型、错误类型，以及「内存从哪来」的约定。
pub trait TrConfig {
    /// 复用连接类型（`abs_smux` 的连接根对象）。
    ///
    /// `Data` 收紧为 `u8`：MPTP 的报文前缀与报文体的线上编码都以字节为单位，
    /// 而 `abs_smux` 的 `Data` 本身是开放的。
    type MuxConn: TrConnection<Config: TrMuxConfig<Data = u8>>;

    type SharedConn: TrStrongShared<Item = Self::MuxConn>;

    /// 本客户端发送的请求类型。
    type ReqPrefix: crate::messaging::TrRequestPrefix;

    type RespPrefix: crate::messaging::TrResponsePrefix;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 从配置派生出来的类型别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type MuxConn<TyClientCfg> = <TyClientCfg as self::TrConfig>::MuxConn;

pub type MuxCfg<TyClientCfg> = <MuxConn<TyClientCfg> as TrConnection>::Config;

pub type Dock<TyClientCfg> = <MuxCfg<TyClientCfg> as TrMuxConfig>::Dock;

pub type DockBinding<TyClientCfg> = <MuxConn<TyClientCfg> as TrConnection>::DockBinding;

pub type ChannelHandle<TyClientCfg> =
    <DockBinding<TyClientCfg> as TrDockBinding<MuxCfg<TyClientCfg>>>::ChannelHandle;

/// 本端子流的发送半边。
///
/// 它取自 [`ChannelHandle`] 的最终裁决（`accept_async`）——**不是** `DockBinding` 上的
/// `Tx`。二者在实现里通常是同一个具体类型，但在 `abs_smux` 的契约上是两条独立的
/// 关联类型，只有 `TrChannelHandle::Tx` 才是「这条已建成子流的写半部」。
pub type ChannelTx<TyClientCfg> =
    <ChannelHandle<TyClientCfg> as TrChannelHandle<MuxCfg<TyClientCfg>>>::Tx;

/// 本端子流的接收半边，来源同 [`ChannelTx`]。
pub type ChannelRx<TyClientCfg> =
    <ChannelHandle<TyClientCfg> as TrChannelHandle<MuxCfg<TyClientCfg>>>::Rx;

pub type ChannelListener<TyClientCfg> =
    <DockBinding<TyClientCfg> as TrDockBinding<MuxCfg<TyClientCfg>>>::Listener;

pub type ReqPrefix<C> = <C as self::TrConfig>::ReqPrefix;
pub type RespPrefix<C> = <C as self::TrConfig>::RespPrefix;
