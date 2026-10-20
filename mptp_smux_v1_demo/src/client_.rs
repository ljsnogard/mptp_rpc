//! 客户端侧：三次调用，演示**类型靠类型自己给出的标识来确定**。
//!
//! 客户端这边承担三件事，都不需要手写任何标识常量：
//!
//! 1. 建流：`MptpClient::request_to_dock_async(dock, prefix)` —— ring 内存走配置声明的
//!    默认工厂，目标垛口由调用点给出；
//! 2. 写请求体：`MptpSession::send_req_body_async` 借出子流的发送半边（`abs_mptp` 的会话
//!    契约只规定「怎么收回复」，写入口是 `mptp_smux` 的固有方法）；
//! 3. 收回复：先读前缀（状态码 + 头），校验 `Data_Type_Id` 确实是本端期待的类型，**然后**
//!    才解码体。

use abs_buff::x_deps::abs_cancel;
use abs_cancel::NonCancellableToken;
use abs_denco::de::TrDecodableOwned;
use abs_mm::CoreAlloc;
use abs_mptp::{
    client::TrMptpSession,
    spec::{Status, access_method::AccessMethod},
};
use anyhow::{Result, anyhow, bail};
use mm_ptr::Shared;
use mptp_smux::{client::MptpClient, messaging::RequestPrefix};

use crate::{
    body_::decode_from_async_,
    cfg_::{ClientCfg, Dock},
    conn_::Conn,
    protocol_::{
        EchoReply, EchoRequest, K_ECHO_PATH, K_SUM_PATH, SumReply, SumRequest, TypeId64,
        expect_declared_, request_with_, type_id_of_,
    },
};

/// 客户端这一次要发出的回声内容。
const K_TEXT: &str = "hello from mptp_smux_v1_demo";

/// 依次跑完三次调用。
///
/// 1. `/rpc/echo` + `EchoRequest`：正常调用，标识由类型给出；
/// 2. `/rpc/sum` + `SumRequest`：换一个类型、换一条路径，同一套机制；
/// 3. `/rpc/sum` + `EchoRequest`：**故意把类型发错路径**——服务端期待 `SumRequest`，收到的
///    却是 `EchoRequest` 的标识，于是明确拒绝（406），而不是按默契硬解。
///
/// 三次调用各用一个 dock：`smux_v1` 会把拆掉的 dock 对放进拆流宽限期，各用各的既绕开它，
/// 也顺带演示「一条连接上多 dock、多子流」。
///
/// # Errors
///
/// 任一次调用失败、回复标识与期待不符，或错配没有被拒绝时返回错误。
pub async fn run_(conn: Shared<Conn, CoreAlloc>, base_dock: u32) -> Result<()> {
    let client = MptpClient::<ClientCfg>::new(conn);

    let (prefix, body) = request_with_(
        AccessMethod::Call,
        K_ECHO_PATH,
        &EchoRequest {
            text: K_TEXT.to_string(),
        },
    )
    .await?;
    let reply: EchoReply = call_ok_(&client, Dock::new(base_dock), prefix, body, "echo").await?;
    println!(
        "客户端：/rpc/echo 回来 {:?}（回复声明 {:#x}）",
        reply.text,
        type_id_of_::<EchoReply>()
    );

    let (prefix, body) = request_with_(
        AccessMethod::Call,
        K_SUM_PATH,
        &SumRequest { a: 17i64, b: 25i64 },
    )
    .await?;
    let reply: SumReply =
        call_ok_(&client, Dock::new(base_dock + 1u32), prefix, body, "sum").await?;
    println!(
        "客户端：/rpc/sum 回来 {}（回复声明 {:#x}）",
        reply.total,
        type_id_of_::<SumReply>()
    );

    let (prefix, body) = request_with_(
        AccessMethod::Call,
        K_SUM_PATH,
        &EchoRequest {
            text: "should never be decoded".to_string(),
        },
    )
    .await?;
    let status = call_status_(&client, Dock::new(base_dock + 2u32), prefix, body, "错配").await?;
    println!(
        "客户端：把 EchoRequest 发到 /rpc/sum（类型与路径不匹配），服务端回 {}",
        status.inner()
    );
    if status != Status::NotAcceptable {
        bail!(
            "类型与路径不匹配本应被拒绝（406），实际回了 {}",
            status.inner()
        );
    }

    Result::Ok(())
}

/// 发一条请求并**按类型接收**回复。
///
/// # Errors
///
/// 建流、写体、读前缀、校验标识或解码回复体任一步失败时返回错误。
async fn call_ok_<TyReply>(
    client: &MptpClient<ClientCfg>,
    dock: Dock,
    prefix: RequestPrefix,
    body: Vec<u8>,
    what: &str,
) -> Result<TyReply>
where
    TyReply: TrDecodableOwned + TypeId64,
{
    let mut session = client
        .request_to_dock_async(dock, prefix)
        .await
        .map_err(|err| anyhow!("发起 {what} 请求失败：{err}"))?;

    // 请求体在会话中途写出去：MPTP 层按**前缀里已经声明的传输方式**搬运这段字节流。
    // 体源是**已经序列化好的字节**；业务值怎么变成字节是上层的事。
    let mut body_src: &[u8] = body.as_slice();
    session
        .send_req_body_async(&mut body_src)
        .await
        .map_err(|err| anyhow!("写 {what} 请求体失败：{err}"))?;

    let resp = session
        .recv_resp_prefix_async()
        .await
        .map_err(|err| anyhow!("接收 {what} 回复前缀失败：{err}"))?;
    if resp.status() != Status::Ok {
        bail!("{what} 回了 {}，期望 200", resp.status().inner());
    }
    expect_declared_::<TyReply>(resp.headers(), what)?;

    let mut body_read = session
        .recv_resp_body_async()
        .await
        .map_err(|err| anyhow!("取 {what} 回复体失败：{err}"))?;
    let value = decode_from_async_::<TyReply, _, _>(&mut body_read, NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("解码 {what} 回复体失败：{err}"))?;
    Result::Ok(value)
}

/// 发一条请求，**只看状态码**（用于「故意错配」那一次：那种回复不带体）。
///
/// # Errors
///
/// 建流、写体或读前缀任一步失败时返回错误。
async fn call_status_(
    client: &MptpClient<ClientCfg>,
    dock: Dock,
    prefix: RequestPrefix,
    body: Vec<u8>,
    what: &str,
) -> Result<Status> {
    let mut session = client
        .request_to_dock_async(dock, prefix)
        .await
        .map_err(|err| anyhow!("发起 {what} 请求失败：{err}"))?;

    let mut body_src: &[u8] = body.as_slice();
    session
        .send_req_body_async(&mut body_src)
        .await
        .map_err(|err| anyhow!("写 {what} 请求体失败：{err}"))?;

    let resp = session
        .recv_resp_prefix_async()
        .await
        .map_err(|err| anyhow!("接收 {what} 回复前缀失败：{err}"))?;
    Result::Ok(resp.status())
}
