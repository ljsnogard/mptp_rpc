#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(async_fn_traits)]

//! MPTP 客户端/服务端 Demo。
//!
//! 这个 Demo 把 `rpc_core::serving` 中的内存测试通信搬到了真实网络上：
//!
//! - `local-server <port>` / `local-client <server-id> <ip:port>`：
//!   使用 iroh 直连本机回环地址，不经过 relay；
//! - `relay-server` / `relay-client <server-id>`：
//!   使用 iroh 默认 relay，让客户端和服务端借助外部 relay 转发通信。
//!
//! 由于当前 `Server` 面向内存 `ServiceChannel`，Demo 中通过一个简单的桥接函数：
//! 从 iroh channel 读入请求字节 → 写入内存 channel → 交给 `Server` 处理 →
//! 读出回复字节 → 写回 iroh channel。这样既复用了 core 的 handler 框架，
//! 又完成了真实网络收发。

use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use anyhow::{Context, Result, anyhow};
use iroh::{Endpoint, EndpointAddr, EndpointId, RelayMode, TransportAddr, endpoint::presets::N0};

use abs_buff::gen_may_cancel_future;
use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff, abs_cancel};

use mptp_rpc_core::{
    access_method::AccessMethod,
    messaging::{Request, Response},
    routing::prefix_router::Router,
    serving::{
        channel::ServiceChannel,
        handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
        server::{Server, SessionContext},
    },
    specs::Status,
    transport::{TrChannel, TrMuxConn},
    x_deps::{buffex, abs_buff_stdio_adapt},
};
use mptp_rpc_transport_iroh::{IrohChannel, IrohConnection};

const ALPN: &[u8] = b"mptp-rpc-demo/1";

// ---------------------------------------------------------------------------
// Demo handler
// ---------------------------------------------------------------------------

/// 一个最简单的 handler：无论请求什么，都返回 `200 Ok`。
struct HelloHandler;

#[gen_may_cancel_future(HandleHello)]
async fn handle_hello_async_<'f, C>(
    _handler: &'f HelloHandler,
    _method: AccessMethod,
    _location: &'f str,
    _headers: &'f mut mptp_rpc_core::specs::Headers,
    _channel: &'f mut ServiceChannel,
    _context: &'f mut SessionContext,
    _cancel: &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken,
{
    Ok(FlowCtrl::Ceased(Some(Response::new(Status::Ok))))
}

impl TrReqHandler for HelloHandler {
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut mptp_rpc_core::specs::Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        HandleHelloAsync(self, method, location, headers, channel, context)
    }
}

/// 构造 Demo 使用的路由和 Server。
fn build_server() -> Server {
    let mut router = Router::new();
    let mut chain = HandlerChain::new();
    chain.add_handler(HelloHandler);
    router.add_target("/hello", chain);
    Server::new(router)
}

// ---------------------------------------------------------------------------
// 网络 <-> 内存 channel 桥接
// ---------------------------------------------------------------------------

/// 在一条 iroh channel 上完成一次请求/回复。
///
/// 桥接流程：
/// 1. 从 iroh 读半通道读取客户端发来的完整请求字节；
/// 2. 把请求字节写入内存 `ClientChannel`；
/// 3. 调用 core `Server` 在内存 `ServiceChannel` 上处理；
/// 4. 从内存 `ClientChannel` 读出回复字节；
/// 5. 把回复字节写回 iroh 写半通道并关闭，让客户端读到 EOF。
async fn serve_iroh_channel(server: &Server, mut channel: IrohChannel) -> Result<()> {
    // 1. 读取请求字节。
    let request_bytes = {
        let (_tx, mut rx) = channel.split();
        let mut buf = Vec::new();
        rx.read_to_end(&mut buf).await?;
        buf
    };

    // 2. 把请求交给内存 server。
    let (mut service_channel, mut client_channel) = ServiceChannel::new_pair();
    {
        let mut client_tx = client_channel.split_tx();
        let mut writer = AsStdWrite::new(&mut client_tx, NonCancellableToken::shared_mut());
        writer.write_all(&request_bytes)?;
    }

    server
        .serve_channel_async(&mut service_channel, NonCancellableToken::shared_mut())
        .await?;

    // 3. 读取内存回复。
    let response_bytes = {
        let mut client_rx = client_channel.split_rx();
        let mut reader = AsStdRead::new(&mut client_rx, NonCancellableToken::shared_mut());
        // 内存 channel 没有 EOF 概念，这里按“单次读取”处理；Demo 的回复很小。
        let mut buf = [0u8; 4096];
        let n = reader.read(&mut buf)?;
        buf[..n].to_vec()
    };

    // 4. 写回网络并关闭发送端。
    let (mut tx, _rx) = channel.split();
    tx.write_all(&response_bytes).await?;
    tx.close();
    Ok(())
}

/// 客户端通过 iroh channel 发送一个请求，并读取回复。
async fn client_roundtrip(conn: IrohConnection, request: Request) -> Result<Response> {
    let mut channel = conn
        .open_channel_async()
        .may_cancel_with(NonCancellableToken::shared_mut())
        .await?;

    // 发送请求并关闭发送端，让服务端读到 EOF。
    let request_bytes = rmp_serde::to_vec(&request)?;
    {
        let (mut tx, _rx) = channel.split();
        tx.write_all(&request_bytes).await?;
        tx.close();
    }

    // 读取回复。
    let mut response_bytes = Vec::new();
    {
        let (_tx, mut rx) = channel.split();
        rx.read_to_end(&mut response_bytes).await?;
    }

    let response = rmp_serde::decode::from_slice(&response_bytes)?;
    Ok(response)
}

// ---------------------------------------------------------------------------
// 本地回环（无 relay）
// ---------------------------------------------------------------------------

async fn run_local_server(port: u16) -> Result<()> {
    let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let endpoint = Endpoint::builder(N0)
        .alpns(vec![ALPN.to_vec()])
        .clear_relay_transports()
        .bind_addr(bind_addr)?
        .bind()
        .await?;

    println!("local server id: {}", endpoint.id());
    println!("local server listening on: {bind_addr}");
    println!("waiting for one client...");

    let conn = IrohConnection::accept(endpoint).await?;
    let channel = conn.accept_channel_async().await?;
    let server = build_server();
    serve_iroh_channel(&server, channel).await?;
    // 给后台 send pump 一点时间把回复 flush 到网络上，然后再退出进程。
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("local server handled one request");
    Ok(())
}

async fn run_local_client(server_id: EndpointId, addr: SocketAddr) -> Result<()> {
    let endpoint = Endpoint::builder(N0)
        .alpns(vec![ALPN.to_vec()])
        .clear_relay_transports()
        .bind()
        .await?;

    let server_addr = EndpointAddr::from_parts(server_id, vec![TransportAddr::Ip(addr)]);
    let conn = IrohConnection::connect_by_addr(endpoint, server_addr, ALPN).await?;
    println!("local client connected to {addr}");

    let request = Request::new(AccessMethod::View, "/hello");
    let response = client_roundtrip(conn, request).await?;
    println!(
        "local client got response status: {}",
        response.status().inner()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 借助外部 relay
// ---------------------------------------------------------------------------

async fn run_relay_server() -> Result<()> {
    let endpoint = Endpoint::builder(N0)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await?;
    endpoint.online().await;

    println!("relay server id: {}", endpoint.id());
    println!("relay server is online; waiting for one client...");

    let conn = IrohConnection::accept(endpoint).await?;
    let channel = conn.accept_channel_async().await?;
    let server = build_server();
    serve_iroh_channel(&server, channel).await?;
    // 给后台 send pump 一点时间把回复 flush 到网络上，然后再退出进程。
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    println!("relay server handled one request");
    Ok(())
}

async fn run_relay_client(server_id: EndpointId) -> Result<()> {
    let endpoint = Endpoint::builder(N0)
        .alpns(vec![ALPN.to_vec()])
        .relay_mode(RelayMode::Default)
        .bind()
        .await?;
    endpoint.online().await;

    println!("relay client online, connecting to {server_id} ...");
    let conn = IrohConnection::connect_by_id(endpoint, server_id, ALPN).await?;
    println!("relay client connected");

    let request = Request::new(AccessMethod::View, "/hello");
    let response = client_roundtrip(conn, request).await?;
    println!(
        "relay client got response status: {}",
        response.status().inner()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    match args[1].as_str() {
        "local-server" => {
            let port = args
                .get(2)
                .map(|s| s.parse::<u16>())
                .transpose()
                .context("invalid port")?
                .unwrap_or(0);
            run_local_server(port).await
        }
        "local-client" => {
            if args.len() < 4 {
                return Err(anyhow!("usage: local-client <server-id> <ip:port>"));
            }
            let server_id = EndpointId::from_str(&args[2])?;
            let addr = SocketAddr::from_str(&args[3])?;
            run_local_client(server_id, addr).await
        }
        "relay-server" => run_relay_server().await,
        "relay-client" => {
            if args.len() < 3 {
                return Err(anyhow!("usage: relay-client <server-id>"));
            }
            let server_id = EndpointId::from_str(&args[2])?;
            run_relay_client(server_id).await
        }
        _ => {
            print_usage();
            Err(anyhow!("unknown command: {}", args[1]))
        }
    }
}

fn print_usage() {
    println!(
        "usage:
  mptp_rpc_cs_demo local-server [port]
  mptp_rpc_cs_demo local-client <server-id> <ip:port>
  mptp_rpc_cs_demo relay-server
  mptp_rpc_cs_demo relay-client <server-id>"
    );
}
