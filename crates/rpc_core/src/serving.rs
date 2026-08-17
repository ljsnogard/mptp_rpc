//! 服务端处理框架：路径路由 + 请求解码 + Handler 分发。
//!
//! # 设计目标
//!
//! 这个模块提供一个“足够简单但可扩展”的 MPTP 服务端基础：
//!
//! - [`TrReqHandler`] 是路径级 handler：它针对某个具体路径上的资源，
//!   处理来自客户端的**任意** `AccessMethod` 请求；
//! - [`TrAccessHandler<M>`] 是方法级细化 handler：它表示“路径 + 特定访问方法”
//!   都匹配时才处理的 handler；
//! - [`Router`] 负责保存路径级和方法级 handler，并在请求到达时选择最具体的那个；
//! - [`Server`] 是基础服务组件：它从 `TrChannel` 的读半通道解码 `Request`，
//!   然后调用匹配的 handler。
//!
//! # 流式设计
//!
//! 序列化和反序列化都在流上进行。`ReqCtx` 向 handler 暴露：
//!
//! - `reader`：请求头之后的请求体 / suffix stream，类型为 `&mut dyn std::io::Read`；
//! - `writer`：回复输出流，类型为 `&mut dyn std::io::Write`。
//!
//! 这样 handler 可以直接使用 [`crate::codec::BodyCodec`] 或 `rmp-serde` /
//! `serde_json` 的流式 API，而不需要先把整个 body 读进内存。
//!
//! # 与 Salvo 的对应关系
//!
//! - `TrReqHandler` 类似于 Salvo 中处理某个路由的 `Handler`；
//! - `TrAccessHandler<M>` 类似于按 HTTP Method 细分的 handler；
//! - `Router` 类似于 Salvo 的 `Router`，但当前只做最基础的精确路径匹配。

use core::{future::Future, mem::MaybeUninit, pin::Pin};
use std::{
    collections::BTreeMap,
    io::{self, Read, Write},
};

use abs_buff::{
    as_std_read::AsStdRead, as_std_write::AsStdWrite, x_deps::abs_cancel::TrCancellationToken,
};
use thiserror::Error;

use crate::{
    access_method::{self, AccessMethod, TrAccessMethod},
    messaging::{self, Request, Response},
    transport::TrChannel,
};

/// Handler 返回的异步 future 类型。
///
/// 使用 `BoxFuture` 而不是 `async fn in trait`，是为了让 `TrReqHandler` 可以
/// 作为 `Box<dyn TrReqHandler>` 存储和分发，保持对象安全。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

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

/// 一次请求的上下文。
///
/// `request` 是已经解码出来的请求头（method / path / headers）。
/// `reader` 和 `writer` 是同一 channel 上请求/回复两个方向的流式句柄。
pub struct ReqCtx<'req, 'io> {
    /// 已解码的请求头。
    pub request: &'req Request,
    /// 请求头之后的输入流：包括请求体以及可能的 suffix stream。
    pub reader: &'io mut dyn Read,
    /// 回复输出流：handler 把 `Response` 头和 body / suffix stream 写到这里。
    pub writer: &'io mut dyn Write,
}

impl<'req, 'io> ReqCtx<'req, 'io> {
    /// 创建新的请求上下文。
    pub const fn new(
        request: &'req Request,
        reader: &'io mut dyn Read,
        writer: &'io mut dyn Write,
    ) -> Self {
        ReqCtx {
            request,
            reader,
            writer,
        }
    }

    /// 请求头引用。
    pub const fn request(&self) -> &'req Request {
        self.request
    }

    /// 请求体 / suffix stream 读取器。
    pub fn reader(&mut self) -> &mut dyn Read {
        self.reader
    }

    /// 回复写入器。
    pub fn writer(&mut self) -> &mut dyn Write {
        self.writer
    }
}

/// 路径级 handler：在一个 stream 上响应某个具体路径的任意方法请求。
///
/// 实现者应当：
///
/// 1. 根据 `ctx.request.access_method()` 决定如何处理；
/// 2. 从 `ctx.reader` 读取请求体 / 持续流；
/// 3. 向 `ctx.writer` 写入 `Response` 头、body 或持续流。
pub trait TrReqHandler: Send + Sync {
    /// 处理一次已经完成请求头解码的 MPTP 请求。
    fn handle<'a>(&'a self, ctx: ReqCtx<'a, 'a>) -> BoxFuture<'a, Result<(), ServeError>>;
}

/// 方法级细化 handler：仅当路径和 `M` 指定的 `AccessMethod` 都匹配时才会被调用。
///
/// 它继承 [`TrReqHandler`] 的全部能力，只是一个编译期标记，让注册 API 可以
/// 把 handler 绑定到具体访问方法上。
pub trait TrAccessHandler<M: TrAccessMethod>: TrReqHandler {}

/// 把回复头写入输出流。
///
/// 这是流式回复的基础：handler 可以先构造 `Response`，调用本函数写头，
/// 然后继续用 `BodyCodec` 或其它流式 writer 写 body。
pub fn write_response_head(resp: &Response, writer: &mut dyn Write) -> io::Result<()> {
    rmp_serde::encode::write(writer, resp).map_err(|e| io::Error::other(e.to_string()))
}

/// 路由表：保存路径级与“路径 + 方法”级 handler。
///
/// 查找顺序：
/// 1. 先查 `(path, method)` 精确匹配的 `TrAccessHandler`；
/// 2. 没有再查该 path 的 `TrReqHandler`。
#[derive(Default)]
pub struct Router {
    path_handlers_: BTreeMap<String, Box<dyn TrReqHandler>>,
    access_handlers_: BTreeMap<(String, AccessMethod), Box<dyn TrReqHandler>>,
}

impl Router {
    /// 创建空路由表。
    pub const fn new() -> Self {
        Router {
            path_handlers_: BTreeMap::new(),
            access_handlers_: BTreeMap::new(),
        }
    }

    /// 注册一个路径级 handler，处理该路径上的任意访问方法。
    pub fn add_path_handler<H>(&mut self, path: impl Into<String>, handler: H)
    where
        H: TrReqHandler + 'static,
    {
        self.path_handlers_.insert(path.into(), Box::new(handler));
    }

    /// 注册一个“路径 + 访问方法”级 handler。
    ///
    /// `M` 是编译期方法标记，例如 `crate::access_method::View`。
    pub fn add_access_handler<M, H>(&mut self, path: impl Into<String>, handler: H)
    where
        M: TrAccessMethod,
        H: TrAccessHandler<M> + 'static,
    {
        let key = (path.into(), access_method::method_of::<M>());
        self.access_handlers_.insert(key, Box::new(handler));
    }

    /// 根据请求查找最匹配的 handler。
    pub fn find(&self, req: &Request) -> Option<&dyn TrReqHandler> {
        let access_key = (req.access_path().to_string(), req.access_method());
        if let Some(handler) = self.access_handlers_.get(&access_key) {
            return Some(handler.as_ref());
        }
        self.path_handlers_
            .get(req.access_path())
            .map(|handler| handler.as_ref())
    }

    /// 当前注册的路径级 handler 数量。
    pub fn path_handler_len(&self) -> usize {
        self.path_handlers_.len()
    }

    /// 当前注册的“路径 + 方法”级 handler 数量。
    pub fn access_handler_len(&self) -> usize {
        self.access_handlers_.len()
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
    pub async fn serve_channel<Ch, C>(
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
        let request = messaging::decode_request_async_(&mut m, &mut rx, cancel)
            .await
            .map_err(|e| ServeError::Decode(e.to_string()))?;

        // 2. 路由。
        let handler = self
            .router_
            .find(&request)
            .ok_or_else(|| ServeError::NotFound(request.access_path().to_string()))?;

        // 3. 把底层 TrBuff stream 包装成 std::io::Read/Write，交给 handler。
        //    两个方向各自持有一个取消令牌 clone，避免同时可变借用同一个 cancel。
        let mut reader_cancel = cancel.clone();
        let mut writer_cancel = cancel.clone();
        let mut reader = AsStdRead::new(&mut rx, &mut reader_cancel);
        let mut writer = AsStdWrite::new(&mut tx, &mut writer_cancel);
        let ctx = ReqCtx::new(&request, &mut reader, &mut writer);

        handler.handle(ctx).await
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::{
        access_method::View,
        messaging::Response,
        specs::{HeaderVal, Status, StdHeaderKey, StdHeaderVal},
    };

    /// 一个简单的路径级 handler：无论什么方法都返回 200 + 空 body。
    struct AnyHandler;

    impl TrReqHandler for AnyHandler {
        fn handle<'a>(&'a self, ctx: ReqCtx<'a, 'a>) -> BoxFuture<'a, Result<(), ServeError>> {
            Box::pin(async move {
                let resp = Response::new(Status::Ok);
                write_response_head(&resp, ctx.writer)?;
                Ok(())
            })
        }
    }

    /// 一个 View 专用 handler：返回 201，证明方法级路由优先。
    struct ViewHandler;

    impl TrReqHandler for ViewHandler {
        fn handle<'a>(&'a self, ctx: ReqCtx<'a, 'a>) -> BoxFuture<'a, Result<(), ServeError>> {
            Box::pin(async move {
                let resp = Response::new(Status::Created).with_headers({
                    let mut headers = crate::specs::Headers::new();
                    headers.add_or_set_header(
                        &StdHeaderKey::Body_Type.into(),
                        &HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack),
                    );
                    headers
                });
                write_response_head(&resp, ctx.writer)?;
                Ok(())
            })
        }
    }

    impl TrAccessHandler<View> for ViewHandler {}

    #[test]
    fn router_prefers_access_handler() {
        let mut router = Router::new();
        router.add_path_handler("/a", AnyHandler);
        router.add_access_handler::<View, _>("/a", ViewHandler);

        assert_eq!(router.path_handler_len(), 1);
        assert_eq!(router.access_handler_len(), 1);

        // View 应该命中 ViewHandler（方法级优先）。
        let view_req = Request::new_for_test(AccessMethod::View, "/a");
        assert!(router.find(&view_req).is_some());

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
}
