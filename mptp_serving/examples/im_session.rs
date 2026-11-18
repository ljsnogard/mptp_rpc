//! 一个 IM 会话中 `mptp_serving` 的标准装配与请求处理。
//!
//! # 场景
//!
//! `im_server` 为每条连接创建一个 `MptpServlet` + 一个 `RadixRouter`。一次请求进入
//! 后，顺序是：
//!
//! ```text
//! 接受子流 → 读请求前缀 → 路由匹配 → 中间件（按端点元数据校验类型头）
//!          → 终端 handler（读请求体 / 后缀，写回复）→ 收尾
//! ```
//!
//! 本示例围绕 `mptp_im_demo` 的 `im/v1`：
//!
//! 1. A `Call im/v1/session/display_name`：中间件确认 `Data_Type_Id` 是请求 body 类型，
//!    handler 解码名字并回 `200 + 用户信息`；A 看到 `User`。
//! 2. A `Pull im/v1/room/events/42`：中间件确认 `Accept` 的 item 类型；handler 打开
//!    response suffix，逐项写 `Snapshot` / `MessagePosted`；A 每 take 一条。
//! 3. A `Push im/v1/room/messages/42`：中间件确认请求 suffix item 类型；handler 逐项
//!    读请求后缀，读尽后回 200；A 的 `finish_async` 这时才拿到 `Response`。
//! 4. 头缺失 / 不匹配：中间件在 handler 之前短路 `400` / `406`。
//!
//! # 这个示例做什么
//!
//! `main` 演示端点元数据的挂载和类型头判定；路由 / 中间件 / handler 的装配写成
//! 泛型函数（`assemble_im_router`），由 `cargo check` 做类型检查。实际运行需要
//! 一个具体的 `C: mptp_smux::serving::config::TrConfig`（例如 demo 的 `ServingCfg`）
//! 和一条真实子流；示例不绑定某个传输配置。
//!
//! 运行：
//!
//! ```text
//! cargo run -p mptp_serving --example im_session
//! ```

use std::{alloc::Global, sync::Arc};

use abs_async_iter::{TrAsyncIterator, TrFlux};
use abs_buff::{TrBuffRead, TrBuffWrite, buffer::TrConsumerState};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use mptp_serving::{
    Endpoint, EndpointMetadata, HandlerFuture, MethodSet, MiddlewareFuture, MptpContext, NextFn,
    RadixRouter, TrEndpointHandler, TrMiddleware, box_future_,
    x_deps::{abs_mptp, mptp_smux},
};
use mptp_smux::{
    ServeError,
    serving::config::{ChannelRx, ChannelTx, TrConfig},
};
use rpc_core_v1::{
    serving::{
        BodyCodec, DataTypeId, RpcDataTypeInfo, RpcEndpointInfo, decode_body_async, typed_response,
        validate_request_types, write_ok_async,
    },
    stream::{StreamError, TypedJustFlux, TypedSlotFlux},
    x_deps::{abs_async_iter, abs_buff},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 端点元信息：中间件按类型读取；handler 不重复解析 pattern
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `im/v1/session/display_name` 的元信息。
fn display_name_info_() -> RpcDataTypeInfo {
    RpcDataTypeInfo::new(BodyCodec::MsgPack)
        // 示例用 String 代替完整 User DTO；线上契约仍是“请求体一个类型”。
        .with_request_body(DataTypeId::new(0x3001u64).unwrap())
        .with_response_body(DataTypeId::new(0x3001u64).unwrap())
}

/// `im/v1/room/events/{room_id}` 的元信息。
fn room_events_info_() -> RpcDataTypeInfo {
    RpcDataTypeInfo::new(BodyCodec::MsgPack)
        .with_response_suffix(DataTypeId::new(0x3002u64).unwrap())
        .with_accept(rpc_core_v1::serving::AcceptType::new(
            BodyCodec::MsgPack,
            Some(DataTypeId::new(0x3002u64).unwrap()),
        ))
}

/// `im/v1/room/messages/{room_id}` 的元信息。
fn push_messages_info_() -> RpcDataTypeInfo {
    RpcDataTypeInfo::new(BodyCodec::MsgPack)
        .with_request_suffix(DataTypeId::new(0x3003u64).unwrap())
        .with_accept(rpc_core_v1::serving::AcceptType::new(
            BodyCodec::MsgPack,
            None,
        ))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 中间件：按端点元数据在进入 handler 前校验类型头
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 类型头校验中间件。
///
/// 它只读 `endpoint.metadata()`：没有 `RpcDataTypeInfo` 的端点直接放行；有则按方法
/// 校验请求头。失败时返回 `Some(status)`，管线在 handler 之前写无体 400 / 406。
struct TypeCheckMiddleware_;

impl<C> TrMiddleware<C> for TypeCheckMiddleware_
where
    C: TrConfig + 'static,
{
    fn call<'x, 'f, 'a>(
        &'x self,
        endpoint: &'x Endpoint<C>,
        ctx: &'x mut MptpContext<'f, 'a, C>,
        next: &'x mut NextFn<'x, 'f, 'a, C>,
    ) -> MiddlewareFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x,
    {
        let arena = ctx.arena();
        let method = ctx.method();
        let headers = ctx.headers().clone();
        let info = endpoint.metadata().get::<RpcDataTypeInfo>().copied();
        if let Some(info) = info
            && let Err(rejection) = validate_request_types(method, &headers, &info)
        {
            let status = rejection.status();
            return box_future_(arena, async move { Ok(Option::Some(status)) });
        }
        box_future_(arena, async move { next(ctx).await })
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 终端 handler：只关心业务，不碰路由与类型头解析
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `Call im/v1/session/display_name`：解码 String，回 200 + String。
struct DisplayNameHandler_;

impl<C> TrEndpointHandler<C> for DisplayNameHandler_
where
    C: TrConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    fn handle<'f, 'x, 'a>(&'x self, ctx: &'x mut MptpContext<'f, 'a, C>) -> HandlerFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x,
    {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let name: String = decode_body_async(ctx.session_mut()).await?;
            let user = format!("user:{name}");
            write_ok_async(ctx.session_mut(), &user).await?;
            Result::Ok(())
        })
    }
}

/// `Pull im/v1/room/events/{room_id}`：先写 Snapshot，再写增量事件。
struct RoomEventsHandler_;

/// 把流错误映射成 handler 错误。
fn stream_error_(err: StreamError) -> ServeError {
    ServeError::Target {
        detail: err.to_string(),
    }
}

impl<C> TrEndpointHandler<C> for RoomEventsHandler_
where
    C: TrConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    ChannelTx<C>: TrBuffWrite<u8>,
{
    fn handle<'f, 'x, 'a>(&'x self, ctx: &'x mut MptpContext<'f, 'a, C>) -> HandlerFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x,
    {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let info = room_events_info_();
            let stream = typed_response(ctx.session_mut(), Status::Ok, AccessMethod::Pull, &info)
                .suffix_stream_async()
                .await?;
            let mut flux = TypedSlotFlux::<_, String>::new(stream);
            let mut slots = flux.pull_async().await;
            for text in ["snapshot", "hello from B"] {
                slots
                    .next_async()
                    .await
                    .map_err(stream_error_)?
                    .set_async(text.to_string())
                    .await
                    .map_err(stream_error_)?;
            }
            slots.finish_async().await.map_err(stream_error_)?;
            Result::Ok(())
        })
    }
}

/// `Push im/v1/room/messages/{room_id}`：逐项读请求后缀，读尽后回计数。
struct PushMessagesHandler_;

impl<C> TrEndpointHandler<C> for PushMessagesHandler_
where
    C: TrConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
{
    fn handle<'f, 'x, 'a>(&'x self, ctx: &'x mut MptpContext<'f, 'a, C>) -> HandlerFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x,
    {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let view = ctx.req_suffix_async().await?;
            let mut flux = TypedJustFlux::<_, String>::new(view);
            let mut items = flux.pull_async().await;
            let mut count = 0u64;
            loop {
                match items.next_async().await {
                    Result::Ok(just) => {
                        let _message: String = just.take();
                        count += 1u64;
                    }
                    // 服务端读到请求后缀 EOF：客户端已经半关闭。
                    Result::Err(StreamError::Decode(_)) => break,
                    Result::Err(err) => return Result::Err(stream_error_(err)),
                }
            }
            write_ok_async(ctx.session_mut(), &count).await?;
            Result::Ok(())
        })
    }
}

/// 未命中路由：回 404。
struct FallbackHandler_;

impl<C> TrEndpointHandler<C> for FallbackHandler_
where
    C: TrConfig + 'static,
{
    fn handle<'f, 'x, 'a>(&'x self, ctx: &'x mut MptpContext<'f, 'a, C>) -> HandlerFuture<'x, 'a>
    where
        'f: 'x,
        'a: 'x,
    {
        let arena = ctx.arena();
        box_future_(arena, async move {
            ctx.session_mut()
                .response(Status::NotFound)
                .no_body_async()
                .await?;
            Result::Ok(())
        })
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 装配：router + 组元数据 + 端点（编译期示例）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 装配一条连接的 IM router。
///
/// 真实调用点（`im_server`）在 accept 到子流后：
///
/// ```text
/// let servlet = MptpServlet::new(router);
/// let session = servlet.accept_async(&mut listener).await?;
/// servlet.serve_session_async(session).await?;
/// ```
///
/// 这里只构造 router；`C` 由使用方选择（compio/tokio、TCP/UDS 都行）。
#[allow(dead_code)]
fn assemble_im_router_<C>()
-> Result<RadixRouter<C>, mptp_serving::x_deps::matchit_route::InsertError>
where
    C: TrConfig + 'static,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    ChannelTx<C>: TrBuffWrite<u8>,
{
    let router = RadixRouter::<C>::new(Arc::new(FallbackHandler_));
    let router = router.use_middleware(TypeCheckMiddleware_);
    let router = router
        .bind_endpoint(
            Endpoint::new(
                "im/v1/session/display_name",
                MethodSet::CALL,
                Arc::new(DisplayNameHandler_),
            )
            .with_metadata(RpcEndpointInfo {
                namespace: "im/v1",
                service: "session",
                operation: "set_display_name",
                pattern: "im/v1/session/display_name",
            })
            .with_metadata(display_name_info_()),
        )?
        .bind_endpoint(
            Endpoint::new(
                "im/v1/room/events/{room_id}",
                MethodSet::PULL,
                Arc::new(RoomEventsHandler_),
            )
            .with_metadata(RpcEndpointInfo {
                namespace: "im/v1",
                service: "room",
                operation: "events",
                pattern: "im/v1/room/events/{room_id}",
            })
            .with_metadata(room_events_info_()),
        )?
        .bind_endpoint(
            Endpoint::new(
                "im/v1/room/messages/{room_id}",
                MethodSet::PUSH,
                Arc::new(PushMessagesHandler_),
            )
            .with_metadata(RpcEndpointInfo {
                namespace: "im/v1",
                service: "room",
                operation: "push_messages",
                pattern: "im/v1/room/messages/{room_id}",
            })
            .with_metadata(push_messages_info_()),
        )?;
    Result::Ok(router)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// main：可运行的元数据 / 类型头叙事
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 入口：打印一个 IM session 中两端围绕 `mptp_serving` 的动作与反应。
fn main() {
    println!("mptp_serving::im_session —— 一个 IM session 的标准装配");
    println!("（路由 / 中间件 / handler 只做类型检查；运行需要具体 C 与真实子流）");

    let mut metadata = EndpointMetadata::default();
    metadata.insert(RpcEndpointInfo {
        namespace: "im/v1",
        service: "room",
        operation: "events",
        pattern: "im/v1/room/events/{room_id}",
    });
    metadata.insert(room_events_info_());

    let info = metadata
        .get::<RpcDataTypeInfo>()
        .copied()
        .expect("端点应挂类型元信息");
    let endpoint_info = metadata
        .get::<RpcEndpointInfo>()
        .copied()
        .expect("端点应挂身份元信息");
    println!(
        "\n[路由] pattern={} service={} operation={}",
        endpoint_info.pattern, endpoint_info.service, endpoint_info.operation
    );
    println!(
        "[元数据] suffix item={:?}, Accept={:?}",
        info.response_suffix(),
        info.accept()
    );

    // 客户端 Pull 请求头：Body_Type + Accept(item type)。
    let mut headers = rpc_core_v1::x_deps::mptp_smux::Headers::new_in(Global);
    rpc_core_v1::serving::apply_request_type_headers(&mut headers, AccessMethod::Pull, &info);
    rpc_core_v1::serving::write_accept(
        &mut headers,
        rpc_core_v1::serving::AcceptType::new(
            BodyCodec::MsgPack,
            Some(DataTypeId::new(0x3002u64).unwrap()),
        ),
    );
    println!("\n[Call/Pull] A → 请求前缀（Body_Type=MsgPack, Accept=0x3002）");
    println!(
        "[中间件] validate_request_types(Pull) → {:?}",
        validate_request_types(AccessMethod::Pull, &headers, &info)
    );
    println!("[handler] 打开 response suffix，写 Snapshot + MessagePosted");
    println!("[A] 每 take 一条；handler finish 后 A 看到流结束");

    let mut wrong_accept = headers.clone();
    rpc_core_v1::serving::write_accept(
        &mut wrong_accept,
        rpc_core_v1::serving::AcceptType::new(
            BodyCodec::MsgPack,
            Some(DataTypeId::new(0x9999u64).unwrap()),
        ),
    );
    let rejection = validate_request_types(AccessMethod::Pull, &wrong_accept, &info).unwrap_err();
    println!(
        "\n[类型头] A 把期望 item 写成 0x9999 → 中间件短路 {}（handler 未运行）",
        rejection.status().inner()
    );

    let mut wrong_direction = headers.clone();
    rpc_core_v1::serving::write_data_type_id(
        &mut wrong_direction,
        DataTypeId::new(0x3002u64).unwrap(),
    );
    let rejection =
        validate_request_types(AccessMethod::Pull, &wrong_direction, &info).unwrap_err();
    println!(
        "[方向] Pull 请求里多写 Data_Type_Id → 中间件短路 {}（Pull 请求没有 body）",
        rejection.status().inner()
    );

    println!("\n[Push] A 逐槽写请求 suffix；handler 用 TypedJustFlux 逐项 take");
    println!("[handler] 读尽 suffix → 回 200 + count；A 的 finish_async 这时返回");
    println!("\n[收尾] handler 返回后 servlet 收尾；未读完的请求体会被 drop");
    println!("真实装配：RadixRouter::new + use_middleware + bind_endpoint → MptpServlet");
}
