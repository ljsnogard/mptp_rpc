//! 本地回环（无 relay）Demo。

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::Result;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets::N0};
use mptp_rpc_core::{access_method::AccessMethod, messaging::Request};
use mptp_rpc_transport_iroh::IrohConnection;

use crate::common::{self, ALPN};

pub(crate) async fn run_local_server(port: u16) -> Result<()> {
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
    let server = common::build_server();
    common::serve_iroh_channel(&server, channel).await?;
    // 给后台 send pump 一点时间把回复 flush 到网络上，然后再退出进程。
    tokio::time::sleep(Duration::from_millis(200)).await;
    println!("local server handled one request");
    Ok(())
}

pub(crate) async fn run_local_client(server_id: EndpointId, addr: SocketAddr) -> Result<()> {
    let endpoint = Endpoint::builder(N0)
        .alpns(vec![ALPN.to_vec()])
        .clear_relay_transports()
        .bind()
        .await?;

    let server_addr = EndpointAddr::from_parts(server_id, vec![TransportAddr::Ip(addr)]);
    let conn = IrohConnection::connect_by_addr(endpoint, server_addr, ALPN).await?;
    println!("local client connected to {addr}");

    let request = Request::new(AccessMethod::View, "/hello");
    let response = common::client_roundtrip(conn, request).await?;
    println!(
        "local client got response status: {}",
        response.status().inner()
    );
    Ok(())
}
