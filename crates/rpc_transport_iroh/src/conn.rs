//! Iroh 连接层：实现 `mptp_rpc_core::transport::TrMuxConn`。
//!
//! [`IrohConnection`] 包装一个已建立的 iroh `Connection`（同时保留 `Endpoint`
//! 以维持底层资源生命周期），并负责：
//!
//! - 客户端侧：通过 `open_channel_async` 打开新的双向 QUIC stream；
//! - 服务端侧：通过 `accept` 接受入站连接，再通过 `accept_channel_async`
//!   接受该连接上的入站双向 stream。
//!
//! # 构造方式
//!
//! - [`IrohConnection::connect_by_id`]：通过对端 `EndpointId` 连接（依赖 relay / 地址发现）；
//! - [`IrohConnection::connect_by_addr`]：通过具体地址直连，不依赖 relay；
//! - [`IrohConnection::accept`]：服务端接受一条入站连接。

use buffex::x_deps::abs_buff::gen_may_cancel_future;
use buffex::x_deps::abs_cancel;
use buffex::x_deps::abs_cancel::{TrCancellationToken, TrMayCancel};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use mptp_rpc_core::transport::TrMuxConn;
use thiserror::Error;

use crate::channel::IrohChannel;

/// 传输层错误类型。
#[derive(Debug, Error)]
pub enum IrohConnError {
    #[error("connect to remote failed: {0}")]
    Connect(String),

    #[error("accept incoming connection failed: {0}")]
    Accept(String),

    #[error("endpoint closed")]
    EndpointClosed,

    #[error("open bidirectional stream failed: {0}")]
    OpenStream(String),

    #[error("accept bidirectional stream failed: {0}")]
    AcceptStream(String),

    #[error("stream io failed: {0}")]
    StreamIo(String),
}

/// 已建立的 iroh 连接（连同其 `Endpoint`），实现 `TrMuxConn`。
pub struct IrohConnection {
    endpoint_: Endpoint,
    conn_: Connection,
    local_id_: EndpointId,
    remote_id_: EndpointId,
}

impl IrohConnection {
    /// 方式一（借助 relay / 地址发现）：仅凭服务端 `EndpointId` 连接任意节点。
    ///
    /// `endpoint` 需要用 `iroh::endpoint::presets::N0` 等默认配置绑定
    /// （包含 relay 与地址发现能力），服务端需在相同 ALPN 上监听。
    pub async fn connect_by_id(
        endpoint: Endpoint,
        server_id: EndpointId,
        alpn: &[u8],
    ) -> Result<Self, IrohConnError> {
        let conn = endpoint
            .connect(server_id, alpn)
            .await
            .map_err(|e| IrohConnError::Connect(e.to_string()))?;
        Ok(Self::from_parts(endpoint, conn))
    }

    /// 方式二（不借助任何 relay）：直接连接指定 IP 地址上的节点。
    ///
    /// `server_addr` 用 `EndpointAddr::from_parts(id, [TransportAddr::Ip(addr)])`
    /// 构造；对端 endpoint 需用 `clear_relay_transports()` 等配置只监听直连。
    pub async fn connect_by_addr(
        endpoint: Endpoint,
        server_addr: EndpointAddr,
        alpn: &[u8],
    ) -> Result<Self, IrohConnError> {
        let conn = endpoint
            .connect(server_addr, alpn)
            .await
            .map_err(|e| IrohConnError::Connect(e.to_string()))?;
        Ok(Self::from_parts(endpoint, conn))
    }

    /// 服务端侧：接受一条入站连接。
    pub async fn accept(endpoint: Endpoint) -> Result<Self, IrohConnError> {
        let incoming = endpoint
            .accept()
            .await
            .ok_or(IrohConnError::EndpointClosed)?;
        let conn = incoming
            .await
            .map_err(|e| IrohConnError::Accept(e.to_string()))?;
        Ok(Self::from_parts(endpoint, conn))
    }

    /// 服务端侧：在当前连接上接受一条入站双向 stream，封装成 `IrohChannel`。
    ///
    /// 一个 `IrohConnection` 可以多次调用本方法，分别得到独立的 channel，
    /// 对应 MPTP 中并发的 stream / session。
    pub async fn accept_channel_async(&self) -> Result<IrohChannel, IrohConnError> {
        let (send, recv) = self
            .conn_
            .accept_bi()
            .await
            .map_err(|e| IrohConnError::AcceptStream(e.to_string()))?;
        Ok(IrohChannel::new(send, recv))
    }

    /// 返回底层 `Endpoint` 的引用。
    ///
    /// 持有 `Endpoint` 是为了保证底层套接字 / relay / 地址发现资源在连接存续期间不被回收。
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint_
    }

    /// 两种连接方式共用的装配逻辑：包装 Endpoint 与已建立的 Connection。
    fn from_parts(endpoint: Endpoint, conn: Connection) -> Self {
        let local_id_ = endpoint.id();
        let remote_id_ = conn.remote_id();
        IrohConnection {
            endpoint_: endpoint,
            conn_: conn,
            local_id_,
            remote_id_,
        }
    }
}

/// 打开一条双向流（生成宏包装的可取消未来）。
#[gen_may_cancel_future(IrohOpenChannel)]
async fn iroh_open_channel_async_<'f, C>(
    conn: &'f IrohConnection,
    _cancel: &'f mut C,
) -> Result<IrohChannel, IrohConnError>
where
    C: TrCancellationToken,
{
    let (send, recv) = conn
        .conn_
        .open_bi()
        .await
        .map_err(|e| IrohConnError::OpenStream(e.to_string()))?;
    Ok(IrohChannel::new(send, recv))
}

impl TrMuxConn for IrohConnection {
    type Channel = IrohChannel;
    type Id = EndpointId;
    type Err = IrohConnError;

    fn local_id(&self) -> Option<&Self::Id> {
        Some(&self.local_id_)
    }

    fn remote_id(&self) -> Option<&Self::Id> {
        Some(&self.remote_id_)
    }

    fn open_channel_async<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<Self::Channel, Self::Err>> {
        IrohOpenChannelAsync(self)
    }
}
