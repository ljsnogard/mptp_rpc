//! 基础服务器组件。
//!
//! [`Server`] 负责：
//!
//! 1. 从 [`ServiceChannel`] 解码 `Request`；
//! 2. 使用路由表找到对应的 [`HandlerChain`]；
//! 3. 调用 `HandlerChain` 让请求按顺序经过感兴趣的 handler；
//! 4. 如果 handler 通过 `FlowCtrl` 返回了一个 `Response`，则把回复头写回 channel。
//!
//! 当前版本面向“代码内直接模拟客户端/服务端收发”的测试场景，
//! 因此直接操作内存 [`ServiceChannel`]，不依赖具体网络传输。

use std::{io, mem::MaybeUninit};

use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::abs_cancel;
use thiserror::Error;

use super::{channel::ServiceChannel, handler::HandlerChain};
use crate::{
    messaging::{self, Request, Response},
    specs::Headers,
    transport::TrChannel,
};

type Router = crate::routing::prefix_router::Router<HandlerChain>;

/// 会话上下文，后续可以存放连接信息、鉴权结果、日志等。
/// 在客户端连接到服务器时被创建，判定断线后被回收。
pub struct SessionContext;

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

/// 把 `Response` 头写入输出流。
///
/// 这是流式回复的基础：handler 可以先构造 `Response`，调用本函数写头，
/// 然后继续用 `BodyCodec` 或其它流式 writer 写 body。
pub fn write_response_head(resp: &Response, writer: &mut dyn io::Write) -> io::Result<()> {
    rmp_serde::encode::write(writer, resp).map_err(|e| io::Error::other(e.to_string()))
}

/// 基础服务器组件：负责“从 channel 解码请求 → 路由 → 调用 handler”。
///
/// 它不关心具体网络传输，只依赖内存 [`ServiceChannel`]。
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

    /// 在一条内存 channel 上处理一个请求。
    ///
    /// 流程：
    /// 1. 从 `ServiceChannel` 解码 `Request` 头；
    /// 2. 用路由表找到匹配的 `HandlerChain`；
    /// 3. 调用 `HandlerChain` 让请求依次经过 handler；
    /// 4. 若 handler 返回 `SkipRest(Some(resp))` 或 `Ceased(Some(resp))`，
    ///    则把该 `Response` 头写回 channel。
    pub async fn serve_channel_async<C>(
        &self,
        channel: &mut ServiceChannel,
        cancel: &mut C,
    ) -> Result<(), ServeError>
    where
        C: TrCancellationToken + Clone,
    {
        // 1. 解码请求头。请求体 / suffix stream 由 handler 从 channel 中读取。
        let request = {
            let (_tx, mut rx) = channel.split();
            let mut m = MaybeUninit::<Request>::uninit();
            let request = messaging::decode_request_async_(&mut m, &mut rx, cancel)
                .await
                .map_err(|e| ServeError::Decode(e.to_string()))?;
            // 丢弃临时读写半通道，避免占用 channel 的可变借用。
            drop(_tx);
            drop(rx);
            request
        };

        let method = request.method();
        let location = request.location().to_string();

        // 2. 路由。
        let handler = self
            .router_
            .try_match(location.as_str())
            .ok_or_else(|| ServeError::NotFound(location.clone()))?;

        // 3. 调用 HandlerChain。
        let mut headers = request.headers().cloned().unwrap_or_else(Headers::new);
        let mut context = SessionContext; // a dummy context currently
        let ctrl = handler
            .handle_async(method, &location, &mut headers, channel, &mut context)
            .may_cancel_with(cancel)
            .await
            .map_err(|_| ServeError::Handler("handler error".to_string()))?;

        // 4. 如果 handler 通过 FlowCtrl 返回了 Response，则写回客户端。
        if let Some(resp) = ctrl.response() {
            let (mut tx, mut _rx) = channel.split();
            let mut writer = AsStdWrite::new(&mut tx, cancel);
            write_response_head(resp, &mut writer)?;
        }

        Ok(())
    }
}
