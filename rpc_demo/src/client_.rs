//! 客户端侧：配置实现，以及一次带请求体的 `Call`。
//!
//! 与 [`crate::server_`] 对称：请求体用 [`Request::with_measured_body`] 直接挂上业务
//! 类型（编码发生在写出时，字节直接落进 ring），回复体则先拿前缀、再按协议决策读回来
//! ——没有体的回复一个字节都不会读。

use core::mem::MaybeUninit;

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow, bail};
use mm_ptr::{Owned, Shared, x_deps::abs_mm};
use mptp_core::{
    access_method::AccessMethod,
    client::{Client, TrClienAllocConfig, TrClient, TrSession, config::TrClientConfig},
    messaging::{Nothing, Request, Response},
    specs::Status,
};
use smux_v1::{
    connection::Dock,
    x_deps::{abs_buff, mm_ptr},
};

use crate::{conn_::Conn, server_::K_ECHO_PATH};

/// 一条子流向一个方向的 ring 容量（字节）。
const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 子流 ring 存储的拥有者类型。
type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 配置层错误类型。
///
/// `mptp_core` 目前没有「从配置错误里构造错误」的路径（`TrClientConfig::Err` 暂无
/// 使用者），这里给出一个最小实现即可。
#[derive(Debug)]
pub struct DemoCfgError;

impl core::fmt::Display for DemoCfgError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("demo config error")
    }
}

impl core::error::Error for DemoCfgError {}

/// 客户端侧的资源分配约定。
pub struct DemoClientAllocCfg;

impl TrClienAllocConfig for DemoClientAllocCfg {
    type SharedConnAlloc = CoreAlloc;

    type RingAlloc = CoreAlloc;

    type RingBuff = RingBuff;

    const RING_CAPACITY: usize = K_CHANNEL_CAP;

    fn make_ring_buffs() -> (Self::RingBuff, Self::RingBuff) {
        (
            Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
            Owned::new_uninit_slice(K_CHANNEL_CAP, CoreAlloc),
        )
    }
}

/// 本 demo 的客户端配置。
///
/// 与 [`crate::server_::DemoServingCfg`] **没有任何类型上的关系**：两端可能跑在不同
/// 进程里、由两份代码编译。它们在这里恰好同源，但契约上并不绑在一起。
pub struct DemoClientCfg;

impl TrClientConfig for DemoClientCfg {
    type MuxConn = Conn;

    type AllocCfg = DemoClientAllocCfg;

    type SharedConn = Shared<Conn, CoreAlloc>;

    type Request = Request<Vec<u8>, Nothing>;

    type Response = Response<Vec<u8>, Nothing>;

    type Err = DemoCfgError;
}

/// 向 `remote_dock` 发一次 `Call /rpc/echo`，把 `payload` 作为请求体发出去，
/// 返回服务端回声回来的那个字符串。
///
/// 取消令牌用 [`NonCancellableToken`]：本 demo 只验证一次成功往返，不涉及取消。
///
/// # Errors
///
/// 建流、发送请求、读回复前缀或读回复体任一步失败都会带上下文返回 `Err`；服务端回的
/// 状态码不是 `200`、或回声内容与请求不一致时也算失败。
pub async fn run_client_(
    conn: Shared<Conn, CoreAlloc>,
    remote_dock: u32,
    payload: &str,
) -> Result<String> {
    let client = Client::<DemoClientCfg>::new(conn, Dock::new(remote_dock));

    // 这里演示「内容已经在内存里、按定长发出去」这条最常见的路：业务值先编成字节，
    // 字节串自己就是体（`Vec<u8>` 实现了 `TrRpcBody`），`with_sized_body` 顺手写下
    // `Body_Size`。要发业务值本身，用 `body` / `body_with`（编码推迟到发送时）。
    let encoded = rmp_serde::to_vec(payload).map_err(|err| anyhow!("编码请求体失败：{err}"))?;
    let request = Request::<Vec<u8>, Nothing>::with_sized_body(
        AccessMethod::Call,
        K_ECHO_PATH,
        encoded,
    )
    .map_err(|err| anyhow!("攒请求失败：{err}"))?;

    let mut session = client
        .request_async(request)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("发起请求失败：{err}"))?;

    let prefix = session
        .recv_resp_header_async()
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("接收响应前缀失败：{err}"))?;
    if prefix.status() != Status::Ok {
        bail!("服务端回了 {}，期望 200", prefix.status().inner());
    }

    // 读回复体：`Head` / `Drop` 的回复一个字节都不会读；这里用的是 `Call`，
    // 服务端声明了体，于是按 `Body_Size` 直接从 ring 上解出来。
    let echoed: Option<String> = session
        .recv_resp_body_async::<String>(&prefix)
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("接收响应体失败：{err}"))?;

    match echoed {
        Option::Some(text) if text == payload => Result::Ok(text),
        Option::Some(text) => bail!("回声与请求不一致：发出 {payload:?}，收到 {text:?}"),
        Option::None => bail!("服务端没有回体"),
    }
}
