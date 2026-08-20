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
//! 本地回环、relay 和两者共用逻辑分别放在 `local`、`relay`、`common` 模块中。

mod common;
mod local;
mod relay;

use std::{net::SocketAddr, str::FromStr};

use anyhow::{Context, Result, anyhow};
use iroh::EndpointId;

use crate::local::{run_local_client, run_local_server};
use crate::relay::{run_relay_client, run_relay_server};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    // `IrohChannel` 的后台 pump 用 `spawn_local` 启动，必须运行在 `LocalSet`
    // 里（详见 `mptp_rpc_transport_iroh` 的文档），所以整个 main 体包一层。
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
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
        })
        .await
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
