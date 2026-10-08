//! 借助外部 relay 的 Demo。

use std::time::Duration;

use anyhow::Result;
use iroh::{Endpoint, EndpointId, RelayMode, endpoint::presets::N0};
use mptp_rpc_core::{access_method::AccessMethod, messaging::Request};
use mptp_rpc_transport_iroh::IrohConnection;

use crate::common::{self, ALPN};

pub(crate) async fn run_relay_server() -> Result<()> {
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
    let server = common::build_server();
    common::serve_iroh_channel(&server, channel).await?;
    // 给后台 send pump 一点时间把回复 flush 到网络上，然后再退出进程。
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("relay server handled one request");
    Ok(())
}

pub(crate) async fn run_relay_client(server_id: EndpointId) -> Result<()> {
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
    let response = common::client_roundtrip(conn, request).await?;
    println!(
        "relay client got response status: {}",
        response.status().inner()
    );
    Ok(())
}
