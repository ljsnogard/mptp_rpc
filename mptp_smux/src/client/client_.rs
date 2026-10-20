//! 客户端的默认实现：在 `abs_smux` 的连接上完成「建流 → 交出 ring → 发请求」。
//!
//! 一次请求要走的五步（与 `abs_smux` 的建流契约一一对应）：
//!
//! 1. `bind_async(unspecified)`：由连接自行安排一个空闲的本端 dock（`bind(2)` 传 `0` 的
//!    语义）；
//! 2. `open_channel_async(remote_dock, message)`：登记子流身份；此时**不发任何帧**；
//! 3. **准备 ring 内存**：按本条子流的环境（两个 dock + 角色）现场构造工厂，让它生产一份
//!    准备器——这是本模块与上游契约的接缝，也是「内存从哪来」唯一的决策点；
//! 4. `accept_async(welcome, prepare)`：交出准备器，换回该子流的收发半边——建流唯一的提交点；
//! 5. 写出请求前缀（[`write_request_prefix_async`](crate::messaging::write_request_prefix_async)），
//!    把两个半边包成 [`MptpSession`](super::MptpSession) 交给调用者。
//!
//! # ring 内存的两条来源
//!
//! - [`TrMptpClient::request_async`](abs_mptp::client::TrMptpClient::request_async) /
//!   [`MptpClient::request_to_dock_async`](super::MptpClient::request_to_dock_async)：走配置里声明的默认准备
//!   **工厂**——由本 crate 用本条子流的环境现场构造工厂，再让它生产一份准备器；
//! - [`MptpClient::request_to_dock_with_prep_async`](super::MptpClient::request_to_dock_with_prep_async)：
//!   调用者**当场交出**准备器（容量、来源都由自己定），
//!   这条路径连环境都不需要构造。
//!
//! # 垛口从哪来
//!
//! `abs_mptp` 的协议形状里没有 dock（那是 `abs_smux` 的概念），因此 dock 只能落在本 crate：
//! 缺省走配置声明的 [`default_serving_dock`](super::config::TrConfig::default_serving_dock)
//! （协议约定的 [`K_DEFAULT_SERVING_DOCK`](crate::K_DEFAULT_SERVING_DOCK)），要去别处就用
//! [`MptpClient::request_to_dock_async`](super::MptpClient::request_to_dock_async)。`MptpClient`
//! **自己不持有任何垛口**：本端每次由连接安排，远端每次由调用决定。

use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
    ops::Deref,
};

use thiserror::Error;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mm::res_man::TrUnique;
use abs_mptp::client::TrMptpClient;
use abs_smux::{
    chan::{TrChannelHandle, TrPrepareRing},
    conn::{TrConnection, TrDockBinding},
    dock::TrDock,
};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    messaging,
    ring_prepare_::TrPrepFactory,
    x_deps::{abs_cancel, abs_mm, abs_smux, gen_mcf2},
};
use super::{
    config::{self, ChannelHandle, Dock, ReqPrefix, TrConfig},
    prep_ctx_::ClientPrepCtx,
    session::MptpSession,
};

/// 客户端错误。
#[derive(Debug, Error)]
pub enum ClientError {
    /// 等待期间调用方通过取消令牌发出了主动取消信号。
    #[error("异步操作已被调用方取消")]
    Cancelled,

    /// 连接不可用（已断开，或建流失败）。
    #[error("连接不可用：{0}")]
    ConnectionLost(String),

    /// 发送请求失败。
    #[error("发送请求失败：{0}")]
    ReqErr(String),

    /// 接收回复失败。
    #[error("接收回复失败：{0}")]
    RespErr(String),

    /// 准备 ring 内存失败（工厂报错）。
    #[error("准备 ring 内存失败：{0}")]
    RingPrepare(String),

    /// 会话状态机不接受当前操作。
    ///
    /// 「还没收到响应前缀就去读体」属于这一条：按协议此时根本不知道体该怎么切分，任何猜出来
    /// 的长度都会破坏这条子流上后续数据的对齐。
    #[error("会话状态不允许该操作：{0}")]
    State(String),
}

/// 客户端持有的连接智能指针。
type SharedConn<C> = <C as abs_mptp::client::config::TrConfig>::SharedConn;

/// MPTP 客户端：持有共享连接，按需在其上开子流发请求。
///
/// 它只持有连接：**没有**本端垛口（每次由连接安排），**也没有**远端垛口（每次调用的实参
/// 或配置声明的缺省值）。于是同一个 `Client` 可以向任意垛口发起请求。
pub struct MptpClient<C>(SharedConn<C>)
where
    C: TrConfig;

impl<C> MptpClient<C>
where
    C: TrConfig,
{
    /// 由共享连接构造客户端。
    ///
    /// 没有策略参数：默认的 ring 准备工厂是一个**类型**
    /// （[`TrConfig::DefaultPrepFactory`]），由本 crate 在每次裁决前用环境现场构造，没有实例
    /// 需要提前注入；目标垛口也是每次调用的实参（缺省时读配置）。
    pub const fn new(conn: SharedConn<C>) -> Self {
        MptpClient(conn)
    }

    /// 开一条通往**指定垛口**的子流并发出一条请求——ring 内存走配置声明的默认工厂。
    ///
    /// 与 [`TrMptpClient::request_async`](abs_mptp::client::TrMptpClient::request_async) 的唯一差别是目标垛口由调用者当场给出。
    pub fn request_to_dock_async<'f>(
        &'f self,
        dock: Dock<C>,
        request: ReqPrefix<C>,
    ) -> ClientRequestToDockAsync<'f, 'f, C>
    where
        C: 'f,
    {
        ClientRequestToDockAsync::<'f, 'f, C>::new(self, dock, request)
    }

    /// 同 [`MptpClient::request_to_dock_async`](super::MptpClient::request_to_dock_async)，但本条子流的 ring 内存由**调用者当场交出的准备器**
    /// 生产，而不是走配置里声明的默认工厂。
    ///
    /// 这条路径不构造环境、也不碰工厂：内存的来源调用者自己最清楚。想「按本条子流的环境决定
    /// 内存来源」时用 [`MptpClient::request_to_dock_async`](super::MptpClient::request_to_dock_async)。
    pub fn request_to_dock_with_prep_async<'f, B, P>(
        &'f self,
        dock: Dock<C>,
        request: ReqPrefix<C>,
        prepare: P,
    ) -> ClientRequestToDockWithPrepAsync<'f, 'f, C, B, P>
    where
        C: 'f,
        B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
        P: 'static + TrPrepareRing<B, u8>,
    {
        ClientRequestToDockWithPrepAsync::new(self, dock, request, prepare)
    }
}

impl<C> TrMptpClient for MptpClient<C>
where
    C: TrConfig,
{
    type Config = C;
    type Session = MptpSession<C>;
    type ReqErr = ClientError;
    type RequestAsync<'f> = SendClientRequestAsync<'f, 'f, C> where Self: 'f;

    /// 开一条通往**配置声明的缺省垛口**的子流并发出一条请求。
    fn request_async<'f>(
        &'f self,
        request: ReqPrefix<C>,
    ) -> Self::RequestAsync<'f> {
        SendClientRequestAsync::new(self, request)
    }
}

/// 建流的前两步：安排本端 dock、登记子流身份。
///
/// 返回 `(句柄, 本端实际 dock)`。dock 取**实际绑到的**那个：`bind_async` 传 `unspecified`
/// 时由连接安排，只有从 binding 才读得回来；默认路径要用它构造环境。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的 `'f`
// 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(OpenChannelDock, pub(crate), new(pub(crate)))]
async fn open_channel_dock_async_<'f, C, TyTok>(
    client: &'f MptpClient<C>,
    remote: Dock<C>,
    cancel: TyTok,
) -> Result<(ChannelHandle<C>, Dock<C>), ClientError>
where
    C: TrConfig,
    TyTok: TrCancellationToken,
{
    let conn = client.0.deref();

    // 1. 由连接自行安排一个空闲的本端 dock（`Client` 不持有本端垛口）。
    let unspecified = <Dock<C> as TrDock>::unspecified();
    let mut binding = match conn
        .bind_async(unspecified)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(binding) => binding,
        Result::Err(err) => {
            let info = format!("在本端派生会话失败：{err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };
    let actual_local_dock = binding.local_dock().clone();

    // 2. 登记子流身份。开场消息当前不由 MPTP 使用（`smux_v1` 的响应方读循环还没有把
    //    `OPEN` 载荷交给调用方），因此请求前缀只能等到裁决之后才进环。
    let mut open_message: &[u8] = &[];
    let handle = match binding
        .open_channel_async(remote, &mut open_message)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(handle) => handle,
        Result::Err(err) => {
            let info = format!("登记子流身份失败：{err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };

    Result::Ok((handle, actual_local_dock))
}

/// 建流的后两步：最终裁决（交出准备器）→ 写出请求前缀 → 包成 [`Session`]。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(AcceptSend, pub(crate), new(pub(crate)))]
async fn accept_send_async_<'f, C, B, P, K>(
    handle: &'f mut ChannelHandle<C>,
    request: ReqPrefix<C>,
    prepare: P,
    cancel: K,
) -> Result<MptpSession<C>, ClientError>
where
    C: 'f + TrConfig,
    B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
    P: 'static + TrPrepareRing<B, u8>,
    K: TrCancellationToken,
{
    // 欢迎信息当前不由 MPTP 使用（`smux_v1` 按空载荷发出，见其 channel_handle 文档）。
    let mut welcome: &mut [u8] = &mut [];
    let (mut tx, rx) = match handle
        .accept_async(&mut welcome, prepare)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(halves) => halves,
        Result::Err(err) => {
            let info = format!("最终裁决失败：{err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };

    // 前缀必须**一次写完**：顺序由 `messaging` 负责，调用者不背顺序。
    if let Result::Err(err) = messaging::write_request_prefix_async(&request, &mut tx)
        .may_cancel_with(cancel.child_token())
        .await
    {
        let info = format!("写出请求前缀失败：{err}");
        return Result::Err(ClientError::ReqErr(info));
    }

    Result::Ok(MptpSession::new_(request, tx, rx))
}

/// 缺省路径的 step 函数：目标垛口取配置声明的默认服务垛口。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendClientRequest, pub, new(pub(crate)))]
async fn client_request_async_<'f, C, K>(
    client: &'f MptpClient<C>,
    request: ReqPrefix<C>,
    cancel: K,
) -> Result<MptpSession<C>, ClientError>
where
    C: TrConfig,
    K: TrCancellationToken,
{
    let dock = C::default_serving_dock();
    client_request_to_dock_async_(client, dock, request, cancel).await
}

/// 指定垛口路径的 step 函数：ring 内存由配置声明的默认工厂按环境生产。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ClientRequestToDock, pub, new(pub(crate)))]
async fn client_request_to_dock_async_<'f, C, K>(
    client: &'f MptpClient<C>,
    dock: Dock<C>,
    request: ReqPrefix<C>,
    cancel: K,
) -> Result<MptpSession<C>, ClientError>
where
    C: TrConfig,
    K: TrCancellationToken,
{
    let (mut handle, local_dock) =
        open_channel_dock_async_(client, dock.clone(), cancel.child_token()).await?;

    // 环境类型由本 crate 给出（不是使用者定义的），工厂只是它的读者；工厂实例短命
    // （每条子流一个），所以可以放心用 `&mut`。
    let ctx = ClientPrepCtx::new(&local_dock, &dock);
    let mut factory = config::DefaultPrepFactory::<C>::with_ctx(&ctx);
    let prepare = factory
        .get_prepared_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ClientError::RingPrepare(err.to_string()))?;

    accept_send_async_(&mut handle, request, prepare, cancel.child_token()).await
}

/// 覆盖路径的 step 函数：ring 内存由调用者当场交出，因此这里不构造环境、也不碰工厂。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ClientRequestToDockWithPrep, pub, new(pub(crate)))]
async fn client_request_to_dock_with_prep_async_<'f, C, B, P, K>(
    client: &'f MptpClient<C>,
    dock: Dock<C>,
    request: ReqPrefix<C>,
    prepare: P,
    cancel: K,
) -> Result<MptpSession<C>, ClientError>
where
    C: TrConfig,
    B: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>,
    P: 'static + TrPrepareRing<B, u8>,
    K: TrCancellationToken,
{
    let (mut handle, _local_dock) =
        open_channel_dock_async_(client, dock, cancel.child_token()).await?;
    accept_send_async_(&mut handle, request, prepare, cancel.child_token()).await
}
