//! 本地回环与 relay 两种 Demo 共用功能。

use std::io::Write;

use abs_buff::gen_may_cancel_future;
use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use anyhow::{Result, anyhow};
use buffex::x_deps::{abs_buff, abs_cancel};
use mptp_rpc_core::{
    access_method::AccessMethod,
    client::Client,
    codec::channel::RpcChannel,
    messaging::{Request, Response},
    routing::prefix_router::Router,
    serving::{
        handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
        server::{Server, SessionContext},
    },
    specs::Status,
    transport::TrChannel,
    x_deps::{abs_buff_stdio_adapt, buffex},
};
use mptp_rpc_transport_iroh::{IrohChannel, IrohConnection};

pub(crate) const ALPN: &[u8] = b"mptp-rpc-demo/1";

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
    _channel: &'f mut RpcChannel,
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
        channel: &'f mut RpcChannel,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        HandleHelloAsync(self, method, location, headers, channel, context)
    }
}

/// 构造 Demo 使用的路由和 Server。
pub(crate) fn build_server() -> Server {
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
/// 2. 把请求字节写入内存 RpcChannel；
/// 3. 调用 core `Server` 在同一内存 RpcChannel 上处理；
/// 4. 从同一内存 RpcChannel 读出回复字节；
/// 5. 把回复字节写回 iroh 写半通道并关闭，让客户端读到 EOF。
pub(crate) async fn serve_iroh_channel(server: &Server, mut channel: IrohChannel) -> Result<()> {
    // 1. 读取请求字节。
    let request_bytes = {
        let (_tx, mut rx) = channel.split();
        let mut buf = Vec::new();
        rx.read_to_end(&mut buf).await?;
        buf
    };

    // 2. 把请求交给内存 server。
    let mut memory_channel = RpcChannel::new_pair();
    {
        let (mut client_tx, _client_rx) = memory_channel.split();
        let mut writer = AsStdWrite::new(&mut client_tx, NonCancellableToken::shared_mut());
        writer.write_all(&request_bytes)?;
    }
    server
        .serve_channel_async(&mut memory_channel, NonCancellableToken::shared_mut())
        .await?;

    // 3. 读取内存回复。
    let response_bytes = {
        let (_client_tx, mut client_rx) = memory_channel.split();
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
pub(crate) async fn client_roundtrip(
    conn: IrohConnection,
    request: Request,
) -> Result<Response> {
    let client = Client::new(&conn);
    let mut session = client
        .request_async(&request)
        .await
        .map_err(|e| anyhow!("send request failed: {e}"))?;

    let prefix = session
        .recv_response_async()
        .await
        .map_err(|e| anyhow!("receive response failed: {e}"))?;
    let response = Response::new(prefix.0);
    Ok(response)
}
