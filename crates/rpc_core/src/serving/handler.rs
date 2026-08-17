use abs_buff::{
    x_deps::abs_cancel,
};
use abs_cancel::{TrMayCancel, TrCancellationToken};
use buffex::x_deps::abs_buff::{self, gen_may_cancel_future};

use crate::{
    access_method::AccessMethod,
    messaging::Response,
    specs::Headers,
};
use super::{
    server::ServiceContext,
    channel::ServiceChannel,
};

/// 给处理链条发送信号，告知 HandlerChain 打算如何处理 request 本身在链条内的流动。
pub enum FlowCtrl {
    /// 只进行后处理
    Review,
    /// 停止向后传递 request，但会话仍有可能被此前的 handler 检视，
    /// 尤其是那些检查 Response 的 handler
    SkipRest(Option<Response>),

    /// 跳出处理链条，与停止向后传递不同的是，不会再有任何 handler 处理
    /// 这个会话。
    Ceased(Option<Response>),
    CallNext,
}

pub enum HandlerError {
    IoError,
}

/// 路径级 handler：在一个 stream 上响应某个具体路径的任意方法请求。
///
/// 实现者应当：
///
/// 1. 根据 `ctx.request.access_method()` 决定如何处理；
/// 2. 从 `ctx.reader` 读取请求体 / 持续流；
/// 3. 向 `ctx.writer` 写入 `Response` 头、body 或持续流。
pub trait TrReqHandler {
    /// 处理一次已经完成请求头解码的 MPTP 请求。
    fn handle_async<'f>(
        &'f self,
        method  : AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>>;
}

/// HandlerChain 保存一个 handler 链条，被路由器匹配到的请求会进入这个 handler
/// 链条，被一个或者多个 handler 依次处理。每个 handler 都有可能修改进入链条
/// 的请求，例如修改 header，使用 channel 发送数据等等。参考 `TrReqHandler::handle_async`
/// 的函数签名。
pub struct HandlerChain
{}

impl HandlerChain {
    pub const fn new() -> Self {
        HandlerChain {}
    }

    pub fn add_handler<H>(&mut self, _handler: H) {
        todo!()
    }

    pub const fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut ServiceContext,
    ) -> ChainHandleRequestAsync<'f> {
        ChainHandleRequestAsync(self, method, location, headers, channel, context)
    }
}

impl TrReqHandler for HandlerChain {
    #[inline]
    fn handle_async<'f>(
        &'f self,
        method  : AccessMethod,
        location: &'f str,
        headers : &'f mut Headers,
        channel : &'f mut ServiceChannel,
        context : &'f mut ServiceContext,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<FlowCtrl, HandlerError>> {
        HandlerChain::handle_async(self, method, location, headers, channel, context)
    }
}

#[gen_may_cancel_future(ChainHandleRequest)]
async fn chain_handle_request_async_<'f, C>(
    chain: &'f HandlerChain,
    method: AccessMethod,
    location: &'f str,
    headers : &'f mut Headers,
    channel : &'f mut ServiceChannel,
    context : &'f mut ServiceContext,
    cancel  : &'f mut C,
) -> Result<FlowCtrl, HandlerError>
where
    C: TrCancellationToken + Clone,
{
    Result::Ok(FlowCtrl::CallNext)
}
