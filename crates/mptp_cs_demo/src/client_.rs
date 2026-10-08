//! 基于 `mptp_core::client` 的客户端侧。
//!
//! 这里唯一的实质内容是**两套配置的具体实现**：一整套分配约定、一份客户端配置。
//! 它们跑在服务端的同一个进程里，但**类型上彼此独立**——这正是 `mptp_core` 把
//! 客户端与服务端配置分开的用意。

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow};
use mm_ptr::{Shared, x_deps::abs_mm};
use mptp_core::{
    access_method::AccessMethod,
    client::{Client, TrClienAllocConfig, TrClient, TrSession, config::TrClientConfig},
    messaging::{Request, Response},
    specs::Status,
};
use smux_v1::x_deps::{abs_buff, mm_ptr};

use crate::mux_::{DemoConn, K_CHANNEL_CAP, make_channel_buffs_};

/// 配置层错误类型。
///
/// `mptp_core` 目前还没有「从配置错误里构造错误」的路径（`TrClientConfig::Err`
/// 暂无使用者），这里给出一个最小实现即可。
#[derive(Debug)]
pub struct DemoCfgError;

impl core::fmt::Display for DemoCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("demo config error")
    }
}

impl core::error::Error for DemoCfgError {}

/// 本 demo 的资源分配约定（客户端侧）。
pub struct DemoClientAllocCfg;

impl TrClienAllocConfig for DemoClientAllocCfg {
    type SharedConnAlloc = CoreAlloc;

    type RingAlloc = CoreAlloc;

    type RingBuff = crate::mux_::DemoRingBuff;

    const RING_CAPACITY: usize = K_CHANNEL_CAP;

    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff) {
        make_channel_buffs_()
    }
}

/// 本 demo 的客户端配置。
pub struct DemoClientCfg;

impl TrClientConfig for DemoClientCfg {
    type MuxConn = DemoConn;

    type AllocCfg = DemoClientAllocCfg;

    type Request = Request<(), ()>;

    type Response = Response<(), ()>;

    type Err = DemoCfgError;
}

/// 向 `remote_dock` 发一次 `View /hello` 请求，返回对端给出的状态码。
///
/// 取消令牌用 [`NonCancellableToken`]：本 demo 只验证一次成功往返，不涉及取消。
pub async fn run_client_(conn: Shared<DemoConn, CoreAlloc>, remote_dock: u32) -> Result<Status> {
    let client = Client::<DemoClientCfg>::new(conn, smux_v1::connection::Dock::new(remote_dock));
    let request: Request<(), ()> = Request::new(AccessMethod::View, "/hello");

    eprintln!("[dbg] 客户端发起请求");
    let mut session = client
        .request_async(request)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("发起请求失败: {err}"))?;

    eprintln!("[dbg] 客户端拿到会话");
    let prefix = session
        .recv_response_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("接收响应失败: {err}"))?;

    Result::Ok(prefix.0)
}
