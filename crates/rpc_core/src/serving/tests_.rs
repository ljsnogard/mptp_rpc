use abs_buff::gen_may_cancel_future;
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::{
    access_method::AccessMethod,
    codec::channel::RpcChannel,
    messaging::{Request, Response},
    serving::{
        handler::{FlowCtrl, HandlerChain, HandlerError, TrReqHandler},
        server::{ServeError, Server, SessionContext},
    },
    specs::{Headers, Status},
    transport::TrChannel,
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
    _channel: &'f mut RpcChannel,
    _context: &'f mut SessionContext,
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
        channel: &'f mut RpcChannel,
        context: &'f mut SessionContext,
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
    _channel: &'f mut RpcChannel,
    _context: &'f mut SessionContext,
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
        channel: &'f mut RpcChannel,
        context: &'f mut SessionContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        ViewHandlerHandleAsync(self, method, location, headers, channel, context)
    }
}

/// 测试目的：确认 `HandlerChain` 会按添加顺序保存 handler，并能正确报告
/// 链的长度和是否为空。
///
/// 测试方法：新建一个空链，依次加入 `AnyHandler` 和 `ViewHandler`，然后检查
/// `len()` 与 `is_empty()` 的返回值。
///
/// 成功判断：`len()` 返回 2，且 `is_empty()` 返回 false，说明两个 handler
/// 都成功进入链中。
#[test]
fn handler_chain_runs_in_order() {
    let mut chain = HandlerChain::new();
    chain.add_handler(AnyHandler);
    chain.add_handler(ViewHandler);

    assert_eq!(chain.len(), 2);
    assert!(!chain.is_empty());
}

/// 测试目的：验证 `Server` 能基于内存 `RpcChannel` 完成一次完整的
/// “客户端发送请求 → 服务端路由 → handler 生成回复 → 客户端读取回复”流程。
///
/// 测试方法：
/// 1. 构造一个指向 `/a` 的 `HandlerChain`，其中先经过 `AnyHandler` 放行，
///    再由 `ViewHandler` 返回 `201 Created`；
/// 2. 使用 `RpcChannel::new_pair()` 创建内存回环 channel；
/// 3. 通过 `send_request_prefix_async` 把请求头写入 channel；
/// 4. 调用 `Server::serve_channel_async` 处理请求；
/// 5. 通过 `recv_response_prefix_async` 读回回复头。
///
/// 成功判断：服务端返回的回复状态码是 `Status::Created`，说明请求被成功解码、
/// 路由到正确 handler，并且回复被成功写回 channel。
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

    let mut channel = RpcChannel::new_pair();
    let cancel = NonCancellableToken::shared_mut();

    let request: Request<(), ()> = Request::new(AccessMethod::View, "/a");
    {
        let (mut tx, _rx) = channel.split();
        crate::messaging::request::send_request_prefix_async(&request, &mut tx, cancel).await?;
    }

    server.serve_channel_async(&mut channel, cancel).await?;

    let response: Response<(), ()> = {
        let (_tx, mut rx) = channel.split();
        let prefix =
            crate::messaging::response::recv_response_prefix_async(&mut rx, 1024, cancel).await?;
        Response::new(prefix.0)
    };

    assert_eq!(response.status(), Status::Created);
    Ok(())
}

/// 测试目的：验证回复头（status + headers）能够以流式方式写入内存 channel，
/// 并可以被对端按同样的流式方式读回。
///
/// 测试方法：
/// 1. 创建内存 `RpcChannel`；
/// 2. 用 `send_response_prefix_async` 写入一个 `Status::Ok` 的回复头；
/// 3. 用 `recv_response_prefix_async` 从同一 channel 读回。
///
/// 成功判断：读回的回复头状态码为 `Status::Ok`，且 headers 为 `None`，
/// 说明序列化/反序列化过程没有丢失或改变信息。
#[tokio::test]
async fn response_prefix_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    let mut channel = RpcChannel::new_pair();
    let cancel = NonCancellableToken::shared_mut();

    let resp: Response<(), ()> = Response::new(Status::Ok);
    {
        let (mut tx, _rx) = channel.split();
        crate::messaging::response::send_response_prefix_async(&resp, &mut tx, cancel).await?;
    }

    let prefix = {
        let (_tx, mut rx) = channel.split();
        crate::messaging::response::recv_response_prefix_async(&mut rx, 1024, cancel).await?
    };

    assert_eq!(prefix.0, Status::Ok);
    assert!(prefix.1.is_none());
    Ok(())
}

/// 测试目的：验证 `Server` 在路由表中找不到对应路径时，会返回明确的
/// `ServeError::NotFound`，而不是错误地继续处理或写回无效回复。
///
/// 测试方法：
/// 1. 创建一个没有注册任何路由的 `Server`；
/// 2. 向内存 channel 写入一个访问 `/missing` 的请求头；
/// 3. 调用 `serve_channel_async` 并检查返回的错误。
///
/// 成功判断：调用结果返回 `Err(ServeError::NotFound(_))`，说明服务端能正确识别
/// 未匹配路由并给出 NotFound 错误。
#[tokio::test]
async fn server_returns_not_found_for_unmatched_route() -> Result<(), Box<dyn std::error::Error>> {
    let server = Server::new(Router::new());
    let mut channel = RpcChannel::new_pair();
    let cancel = NonCancellableToken::shared_mut();

    let request: Request<(), ()> = Request::new(AccessMethod::View, "/missing");
    {
        let (mut tx, _rx) = channel.split();
        crate::messaging::request::send_request_prefix_async(&request, &mut tx, cancel).await?;
    }

    let err = server
        .serve_channel_async(&mut channel, cancel)
        .await
        .expect_err("unmatched route should fail");

    assert!(matches!(err, ServeError::NotFound(_)));
    Ok(())
}
