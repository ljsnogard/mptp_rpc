use abs_buff::{
    gen_may_cancel_future,
    x_deps::abs_cancel,
};
use abs_cancel::{TrMayCancel, TrCancellationToken};
use buffex::x_deps::abs_buff;

use crate::{
    access_method::AccessMethod,
    messaging::Response,
    serving::handler::HandlerChain,
    specs::{HeaderVal, Headers, Status, StdHeaderKey, StdHeaderVal},
};
use super::{
    channel::ServiceChannel,
    handler::{FlowCtrl, HandlerError, TrReqHandler},
    server::ServiceContext,
};

type Router = crate::routing::prefix_router::Router<HandlerChain>;

/// 一个简单的路径级 handler：无论什么方法都返回 200 + 空 body。
struct AnyHandler;

#[gen_may_cancel_future(HandleAnyRequest)]
async fn handle_any_request_async_<'f, H, C>(
    handler : &'f H,
    method  : AccessMethod,
    req_path: &'f str,
    headers : &'f Headers,
    channel : &'f mut ServiceChannel,
    context : &'f mut ServiceContext,
    cancel  : &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    H: TrReqHandler,
    C: TrCancellationToken + Clone,
{
    // TODO: add any logic for testing at the moment
    Result::Ok(FlowCtrl::Review)
}

impl TrReqHandler for AnyHandler {
    fn handle_async<'f>(
        &'f self,
        method  : AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        HandleAnyRequestAsync(self, method, location, headers, channel, context)
    }
}

/// 一个 View 专用 handler：返回 201，证明方法级路由优先。
struct ViewHandler;

impl TrReqHandler for ViewHandler {
    fn handle_async<'f>(
        &'f self,
        method  : AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        ViewHandlerHandleAsync(self, method, location, headers, channel, context)
    }
}

#[gen_may_cancel_future(ViewHandlerHandle)]
async fn view_handler_handle_async_<'f, C>(
    handler : &'f ViewHandler,
    method  : AccessMethod,
    req_path: &'f str,
    headers : &'f mut Headers,
    channel : &'f mut ServiceChannel,
    context : &'f mut ServiceContext,
    cancel  : &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken,
{
    let resp = Response::new(Status::Created).with_headers({
        let mut headers = Headers::new();
        headers.add_or_set_header(
            &StdHeaderKey::Body_Type.into(),
            &HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack),
        );
        headers
    });
    Result::Ok(FlowCtrl::Ceased(Option::Some(resp)))
}

#[test]
fn router_prefers_access_handler() {
    const PATH: &str = "/a";

    let mut router = Router::new();
    router.add_target(PATH, {
        let mut ch = HandlerChain::new();
        ch.add_handler(AnyHandler);
        ch.add_handler(ViewHandler);
        ch
    });

    // View 应该命中 ViewHandler（方法级优先）。
    assert!(router.try_match(PATH).is_some());

    // Head 应该回退到路径级 AnyHandler。
    let head_req = Request::new_for_test(AccessMethod::Head, "/a");
    assert!(router.find(&head_req).is_some());
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
