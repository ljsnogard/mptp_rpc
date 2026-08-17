use std::{mem::MaybeUninit, io};

use thiserror::Error;

use abs_cancel::{TrMayCancel, TrCancellationToken};
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::{
    access_method::{AccessMethod, Head},
    messaging::{self, Request},
    specs::Headers,
    transport::TrChannel,
};
use super::{
    handler::HandlerChain,
    channel::ServiceChannel,
};

type Router = crate::routing::prefix_router::Router<HandlerChain>;

pub struct ServiceContext;

/// 服务端处理过程中可能出现的错误。
#[derive(Debug, Error)]
pub enum ServeError {
    #[error("decode request failed: {0}")]
    Decode(String),

    #[error("resource not found: {0}")]
    NotFound(String),

    #[error("handler error: {0}")]
    Handler(String),

    #[error("io error: {0}")]
    Io(String),
}

impl From<io::Error> for ServeError {
    fn from(value: io::Error) -> Self {
        ServeError::Io(value.to_string())
    }
}

/// 基础服务器组件：负责“从 channel 解码请求 → 路由 → 调用 handler”。
///
/// 它不关心具体传输层是 iroh / QUIC 还是其它实现，只依赖 [`TrChannel`]。
/// 更高级的功能（连接管理、多路复用循环、鉴权、中间件等）可以在它之上继续构建。
pub struct Server {
    router_: Router,
}

impl Server {
    /// 使用指定路由表创建服务器。
    pub const fn new(router: Router) -> Self {
        Server { router_: router }
    }

    /// 返回路由表引用，方便继续注册或检查。
    pub const fn router(&self) -> &Router {
        &self.router_
    }

    /// 在一条已建立的 channel 上处理一个请求。
    ///
    /// 流程：
    /// 1. `split()` 得到读写半通道；
    /// 2. 从读半通道解码 `Request` 头；
    /// 3. 用 `Router` 找到匹配 handler；
    /// 4. 构造 `ReqCtx`（流式读写）并调用 handler。
    pub async fn serve_channel_async<Ch, C>(
        &self,
        channel: &mut Ch,
        cancel: &mut C,
    ) -> Result<(), ServeError>
    where
        Ch: TrChannel,
        C: TrCancellationToken + Clone,
    {
        let (mut tx, mut rx) = channel.split();

        // 1. 解码请求头。请求体 / suffix stream 留给 handler 从 ctx.reader 读取。
        let mut m = MaybeUninit::<Request>::uninit();
        let request: Request = messaging::decode_request_async_(&mut m, &mut rx, cancel)
            .await
            .map_err(|e| ServeError::Decode(e.to_string()))?;

        let method = request.method().clone();
        let location = request.location().to_string();
        // 2. 路由。
        let handler = self
            .router_
            .try_match(location.as_str())
            .ok_or_else(|| ServeError::NotFound(location))?;

        // 3. 把底层 TrBuff stream 包装成 std::io::Read/Write，交给 handler。
        //    两个方向各自持有一个取消令牌 clone，避免同时可变借用同一个 cancel。
        let mut reader_cancel = cancel.clone();
        let mut writer_cancel = cancel.clone();

        let mut channel = ServiceChannel;
        let mut context = ServiceContext;
        let mut headers = request.headers().cloned().get_or_insert_with(Headers::new);
        handler
            .handle_async(method, &location, &mut headers, &mut channel, &mut context)
            .may_cancel_with(cancel)
            .await;
        Result::Ok(())
    }
}
