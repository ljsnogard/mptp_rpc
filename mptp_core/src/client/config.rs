//! 客户端侧的公开契约。
//!
//! 本模块只描述「使用者要提供什么、客户端能做什么」，不含实现：具体实现见
//! [`super::client_`] 与 [`super::session`]。

use abs_buff::x_deps::abs_cancel;
use abs_cancel::TrMayCancel;
use serde::de::DeserializeOwned;
use abs_smux::{
    chan::TrChannelHandle,
    conf::TrMuxConfig,
    conn::{TrConnection, TrDockBinding},
};
use mm_ptr::x_deps::abs_mm::res_man::TrStrongShared;

use super::{ClientError, alloc_config_::TrClienAllocConfig};

/// 一个客户端实现所需的全部配置。
///
/// 复用连接的形状由 `abs_smux` 的 [`TrConnection`] 给出；本 trait 再补上客户端自己的
/// 请求 / 响应类型、错误类型，以及「内存从哪来」的约定。
pub trait TrClientConfig {
    /// 复用连接类型（`abs_smux` 的连接根对象）。
    ///
    /// `Data` 收紧为 `u8`：MPTP 的报文前缀与报文体的线上编码都以字节为单位，
    /// 而 `abs_smux` 的 `Data` 本身是开放的。
    type MuxConn: TrConnection<Config: TrMuxConfig<Data = u8>>;

    /// 客户端侧的资源分配约定。
    type AllocCfg: TrClienAllocConfig;

    type SharedConn: TrStrongShared<Item = Self::MuxConn>;

    /// 本客户端发送的请求类型。
    type Request: crate::messaging::TrRpcRequest;

    // TODO(重构): 当前 client 侧只解析「响应前缀」，本关联类型还没有使用者。
    // 待响应体的读取路径落地后（Session 需要按 `Body_Size` 读取 body），
    // 再决定它是保留为「完整响应类型」还是换成「前缀 + body」的组合类型。
    type Response: crate::messaging::TrRpcResponse;

    // TODO(重构): `TrSession` 的错误类型已经改成具体的 `ClientError`，本关联类型
    // 暂时没有使用者；保留它是为了后续把「会话层错误」交还给配置者决定。
    type Err: core::error::Error;
}

/// 客户端的行为契约。
pub trait TrClient {
    type Config: TrClientConfig;

    type Session: TrSession<Self::Config>;

    type ReqErr: core::error::Error;

    type RequestAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Session, Self::ReqErr>>
    where
        Self: 'f;

    /// 开一个 channel 然后向对端发送一个请求。如果发送成功，返回一个关于这个请求
    /// 的会话对象。
    fn request_async<'f>(&'f self, request: Request<Self::Config>) -> Self::RequestAsync<'f>;
}

/// 一个与对端之间的会话对象。通常一个会话就是围绕一个 abs_smux 概念里的
/// channel 进行的通信过程。
///
/// 简单的会话只包括一问一答。针对推送或者拉取类请求的会话，则会维持到推流结束。
///
/// # 读取始终是 async 的
///
/// 报文的读写经 `AsStdRead` / `AsStdWrite` 直接落在 ring 上，而那两个适配器内部是
/// 同步等待。这份同步是**本框架对 `serde` 的暂时妥协**，不外溢成调用方的语义：这里
/// 交出去的每个读取入口都是可取消的异步产物。
pub trait TrSession<C>
where
    C: TrClientConfig,
{
    type RecvRespAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<RespPrefix, ClientError>>
    where
        Self: 'f;

    /// 接收对端就本次请求给出的响应前缀（状态码 + 头）。
    ///
    /// 前缀本身说明不了回复的全部：体是可选的，长度写在 `Body_Size` 头里。要不要读、
    /// 读多少，由 [`TrSession::recv_response_body_async`] 按协议判定。
    fn recv_response_async<'f>(&'f mut self) -> Self::RecvRespAsync<'f>;

    type RecvRespBodyAsync<'f, T>: TrMayCancel<'f, MayCancelOutput = Result<Option<T>, ClientError>>
    where
        Self: 'f,
        T: 'f + DeserializeOwned + 'static;

    /// 读取本次回复的报文体，并解成一个业务类型。
    ///
    /// 决策完全由协议给出（见 [`ResponseBodyDecision`](crate::messaging::ResponseBodyDecision)）：
    /// 没有声明体的回复**一个字节都不会读**，因此也不会破坏这条 channel 上后续数据的
    /// 对齐；声明了体却与请求方法冲突（`Head` / `Drop`），或只有 `Body_Type` 没有
    /// `Body_Size`，都会按协议违规报错，而不是猜一个长度读下去。
    ///
    /// 解码**直接从 ring 的接收半边进行**，中间没有中转缓冲。
    fn recv_response_body_async<'f, T>(
        &'f mut self,
        prefix: &'f RespPrefix,
    ) -> Self::RecvRespBodyAsync<'f, T>
    where
        T: DeserializeOwned + 'static;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 从配置派生出来的类型别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub type MuxConn<TyClientCfg> = <TyClientCfg as TrClientConfig>::MuxConn;

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

pub type Request<C> = <C as TrClientConfig>::Request;

/// 响应前缀：状态码 + 头。
///
/// 原来的 `Response<C>` 别名随「Session 只产出前缀」的决策一并替换成这个具体类型：
/// 会话被要求读取响应时，读到的是前缀而不是完整响应体。
pub type RespPrefix = crate::messaging::response::RespPrefix;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// used internally by the client
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

pub(super) type ClietnAllocCfg<C> = <C as TrClientConfig>::AllocCfg;

pub(super) type SharedConnAlloc<C> = <ClietnAllocCfg<C> as TrClienAllocConfig>::SharedConnAlloc;
