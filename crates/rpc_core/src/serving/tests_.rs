use std::io::Write;

use abs_buff::{
    as_std_read::AsStdRead,
    as_std_write::AsStdWrite,
    gen_may_cancel_future,
    x_deps::{
        abs_cancel,
        abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel},
    },
};

use super::{
    channel::ServiceChannel,
    server::{Server, ServiceContext, write_response_head},
};
use crate::{
    access_method::AccessMethod,
    messaging::{Request, Response},
    serving::handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
    specs::{Headers, Status},
};

type Router = crate::routing::prefix_router::Router<HandlerChain>;

/// 一个简单的中间件式 handler：只放行，不生成最终回复。
struct AnyHandler;

#[gen_may_cancel_future(HandleAnyRequest)]
async fn handle_any_request_async_<'f, C>(
    _handler: &'f AnyHandler,
    _method: AccessMethod,
    _location: &'f str,
    _headers: &'f mut Headers,
    _channel: &'f mut ServiceChannel,
    _context: &'f mut ServiceContext,
    _cancel: &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken + Clone,
{
    // 中间件只放行，让后续 handler 有机会处理。
    Ok(FlowCtrl::CallNext)
}

impl TrReqHandler for AnyHandler {
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        HandleAnyRequestAsync(self, method, location, headers, channel, context)
    }
}

/// 一个 View 专用 handler：直接生成 201 回复并终止链。
struct ViewHandler;

#[gen_may_cancel_future(ViewHandlerHandle)]
async fn view_handler_handle_async_<'f, C>(
    _handler: &'f ViewHandler,
    _method: AccessMethod,
    _location: &'f str,
    _headers: &'f mut Headers,
    _channel: &'f mut ServiceChannel,
    _context: &'f mut ServiceContext,
    _cancel: &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken,
{
    let resp = Response::new(Status::Created);
    Ok(FlowCtrl::Ceased(Some(resp)))
}

impl TrReqHandler for ViewHandler {
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        channel: &'f mut ServiceChannel,
        context: &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        ViewHandlerHandleAsync(self, method, location, headers, channel, context)
    }
}

#[test]
fn handler_chain_runs_in_order() {
    let mut chain = HandlerChain::new();
    chain.add_handler(AnyHandler);
    chain.add_handler(ViewHandler);
    assert_eq!(chain.len(), 2);
    assert!(!chain.is_empty());
}

#[tokio::test]
async fn end_to_end_in_memory_request_response() -> Result<(), Box<dyn std::error::Error>> {
    let mut router = Router::new();
    router.add_target("/a", {
        let mut chain = HandlerChain::new();
        chain.add_handler(AnyHandler);
        chain.add_handler(ViewHandler);
        chain
    });
    let server = Server::new(router);

    let (mut server_channel, mut client_channel) = ServiceChannel::new_pair();
    let cancel = NonCancellableToken::shared_mut();

    // 客户端把请求写入内存 channel。
    let request = Request::new(AccessMethod::View, "/a");
    let request_bytes = rmp_serde::to_vec(&request).expect("encode request");
    {
        let mut client_tx = client_channel.split_tx();
        let mut writer = AsStdWrite::new(&mut client_tx, cancel);
        writer.write_all(&request_bytes).expect("write request");
    }

    // 服务端处理请求。
    server
        .serve_channel_async(&mut server_channel, cancel)
        .await?;

    // 客户端读取回复。这里只读一次，因为内存 channel 没有 EOF 概念；
    // 更完整的流式读取应由上层根据 Body_Size / 消息边界处理。
    let mut response_bytes = [0u8; 4096];
    let n = {
        let mut client_rx = client_channel.split_rx();
        let mut reader = AsStdRead::new(&mut client_rx, cancel);
        reader.read(&mut response_bytes).expect("read response")
    };

    let response: Response =
        rmp_serde::decode::from_slice(&response_bytes[..n]).expect("decode response");
    assert_eq!(response.status(), Status::Created);

    Ok(())
}

#[test]
fn write_response_head_is_stream_based() {
    let resp = Response::new(Status::Ok);
    let mut bytes = Vec::new();
    write_response_head(&resp, &mut bytes).unwrap();
    let mut read: &[u8] = bytes.as_ref();
    let decoded: Response = rmp_serde::decode::from_read(&mut read).unwrap();
    assert_eq!(decoded.status(), Status::Ok);
}
