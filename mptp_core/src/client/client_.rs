//! 客户端的默认实现。
//!
//! 一次请求要走的四步（与 `smux_v1` 的建流契约一一对应）：
//!
//! 1. `bind_async(unspecified)`：由连接自行安排一个空闲的本端 dock（`bind(2)` 传 0
//!    的语义）；
//! 2. `open_channel_async(remote_dock, message)`：登记身份；此时**不发任何帧**；
//! 3. `accept_async(welcome, prepare)`：交出本端的两块 ring 内存，换回该子流的收发
//!    半边 —— 这是建流唯一的提交点；
//! 4. 把请求前缀写进发送环。
//!
//! 第 2 步的开场消息当前不由 MPTP 使用：`smux_v1` 的响应方读循环还没有把 `OPEN`
//! 载荷交给调用方，因此请求前缀只能等到第 4 步才进环。

use abs_buff::{
    buffer::TrProducerState,
    gen_may_cancel_future,
};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_smux::{
    chan::TrChannelHandle,
    conn::{TrConnection, TrDockBinding},
    dock::TrDock,
};
use thiserror::Error;

use crate::{
    messaging,
    x_deps::{abs_buff, abs_cancel},
};
use super::{
    alloc_config_::{ClientRingPrepare, TrClienAllocConfig},
    config::{self, Dock, TrClient, TrClientConfig},
    session::Session,
};

/// 单次会话内的操作错误。
///
/// TODO(重构): 目前没有使用者，保留是为了给「会话中途的操作失败」留一个与
/// [`ClientError`] 区分的层次。
#[derive(Debug, Error)]
pub enum OperationError {
    #[error("IO error: {0}")]
    IoErr(String),

    #[error("Rpc error: {0}")]
    RpcErr(String),
}

/// 客户端错误。
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Async operation cancelled by user")]
    Cancelled,

    #[error("no connection available for client")]
    ConnectionLost(String),

    #[error("Error occurs during sending request: {0}")]
    ReqErr(String),

    #[error("Error occurs during recving response: {0}")]
    RespErr(String),
}

/// 客户端持有的连接智能指针。
type SharedMuxConn<C> = <C as TrClientConfig>::SharedConn;

/// 面向**某一个**对端 dock 的 RPC 客户端。
///
/// `remote_dock` 在构造时固定：每次请求都会在连接上自行安排一个新的本端 dock，
/// 与它组成这条子流的身份（`(local, remote)` 即身份，见 `smux_v1` 的连接文档 §4.1）。
pub struct Client<C>
where
    C: TrClientConfig,
{
    /// 共享的连接对象。取 `Option` 是为了将来支持主动断开（断开后置空）。
    conn_: Option<SharedMuxConn<C>>,

    /// 目标（对端）dock。
    remote_dock_: Dock<C>,
}

impl<C> Client<C>
where
    C: TrClientConfig,
{
    /// 由「共享连接 + 目标 dock」构造客户端。
    pub const fn new(conn: SharedMuxConn<C>, remote_dock: Dock<C>) -> Self {
        Client {
            conn_: Option::Some(conn),
            remote_dock_: remote_dock,
        }
    }

    /// 本客户端的目标 dock。
    pub const fn remote_dock(&self) -> &Dock<C> {
        &self.remote_dock_
    }

    /// 是否还持有连接。
    pub const fn has_conn(&self) -> bool {
        matches!(self.conn_, Option::Some(_))
    }
}

impl<C> TrClient for Client<C>
where
    C: TrClientConfig,
    // 写请求前缀要经过 `AsStdWrite`，它要求写半边能报告「环是否已满」（`smux_v1` 的
    // 实现沿用 trait 默认：不报告，于是适配器会真正等待空间）。
    config::ChannelTx<C>: TrProducerState,
{
    type Config = C;

    type Session = Session<C>;

    type ReqErr = ClientError;

    type RequestAsync<'f>
        = SendClientRequestAsync<'f, 'f, C>
    where
        Self: 'f;

    fn request_async<'f>(&'f self, request: config::Request<C>) -> Self::RequestAsync<'f> {
        SendClientRequestAsync::new(self, request)
    }
}

/// 发起一次请求：建流、写入请求前缀，返回会话。
// `gen_may_cancel_future` 要求被包装的 `async fn` 显式声明所需生命周期，因此这里的
// `'f` 虽然可以被编译器省略，也必须照写——这正是 clippy 提示需要豁免的原因。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SendClientRequest, pub, new(pub(crate)))]
async fn client_request_async_<'f, C, TyTok>(
    client: &'f Client<C>,
    request: config::Request<C>,
    cancel: TyTok,
) -> Result<Session<C>, ClientError>
where
    C: TrClientConfig,
    // 见 `impl TrClient`：写请求前缀要经过 `AsStdWrite`。
    config::ChannelTx<C>: TrProducerState,
    TyTok: TrCancellationToken,
{
    let Option::Some(conn) = client.conn_.as_ref() else {
        return Result::Err(ClientError::ConnectionLost(
            "client has no connection".to_string(),
        ));
    };

    // 1. 由连接自行安排一个空闲的本端 dock。
    let local_dock = <Dock<C> as TrDock>::unspecified();
    let mut binding = match conn
        .bind_async(local_dock)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(binding) => binding,
        Result::Err(err) => {
            let info = format!("bind local dock failed: {err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };

    // 2. 登记子流身份。开场消息暂时为空（见模块文档）。
    let mut open_message: &[u8] = &[];
    let mut handle = match binding
        .open_channel_async(client.remote_dock_.clone(), &mut open_message)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(handle) => handle,
        Result::Err(err) => {
            let info = format!("open channel failed: {err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };

    // 3. 最终裁决：交出两块 ring 内存，换回本子流的收发半边。
    let (tx_buff, rx_buff) = <config::ClietnAllocCfg<C> as TrClienAllocConfig>::make_ring_buffs();
    let prepare = ClientRingPrepare::new(tx_buff, rx_buff);
    // 欢迎信息当前不由 MPTP 使用（`smux_v1` 按空载荷发出，见其 channel_handle 文档）。
    let mut welcome: &mut [u8] = &mut [];
    let (mut tx, rx) = match handle
        .accept_async(&mut welcome, prepare)
        .may_cancel_with(cancel.child_token())
        .await
    {
        Result::Ok(halves) => halves,
        Result::Err(err) => {
            let info = format!("accept channel failed: {err}");
            return Result::Err(ClientError::ConnectionLost(info));
        }
    };

    // 4. 写入请求前缀（method / location / headers）。
    let send_res = messaging::request
        ::send_request_prefix_async(&request, &mut tx, cancel.child_token())
        .await;
    if let Result::Err(err) = send_res {
        let info = format!("send request prefix failed: {err}");
        return Result::Err(ClientError::ReqErr(info));
    }

    Result::Ok(Session::new(request, tx, rx))
}
