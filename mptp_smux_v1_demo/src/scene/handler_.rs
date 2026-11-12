//! 单一 `TrServeHandler`：路由 + join / update / View / Head / per-planet Pull。
//!
//! 一个 [`SceneHandler`] 只服务**一条连接**；它持有该连接的身份、自己的行星以及共享的
//! 目录 / 订阅中心。路由、方法分派与 404 都在这里；405 由框架按 `allowed_methods`
//! 在进入 handler 之前处理。
//!
//! 行星数据流用 [`SuffixStream`] 写：每个 `PlanetEvent` 用 denco 序列化后作为一个 chunk
//! 写入。`Snapshot` 是流的第一帧，之后是 `Updated` / `Removed`；流一直保持到客户端退订
//! 或连接关闭。
//!
//! # `handler_`: the single scene handler
//!
//! One handler per connection. Routing, 404 and dispatch live here; 405 is handled by the
//! framework through `allowed_methods`. The planet stream is written with `SuffixStream`.

use std::sync::{Arc, Mutex};

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    HeaderVal, MethodSet, ServeError,
    serving::{ServingSession, SuffixStream, TrServeHandler},
    x_deps::{abs_cancel, gen_mcf2},
};

use crate::scene::{
    cancel_::{SceneCancelToken, cancel_pair_},
    cfg_::ServingCfg,
    protocol_::{
        JoinRequest, K_HDR_DIRECTORY_VERSION, K_HDR_PLANET_COUNT, K_SCENE_JOIN, K_SCENE_PLANETS,
        K_SCENE_UNSUBSCRIBE, K_SCENE_UPDATE, PlanetEvent, PlanetId, UnsubscribeAck,
        UnsubscribeRequest, UpdateAck, UpdateRequest, decode_value_async_, encode_event_async_,
        encode_to_vec_async_, parse_planet_uri_,
    },
    state_::{ConnectionCtx, K_SUB_QUEUE_CAP, SceneHub, SubId},
};

/// 场景 handler。
pub struct SceneHandler {
    /// 本连接的上下文。
    ctx_: Arc<ConnectionCtx>,
}

impl SceneHandler {
    /// 用连接上下文构造 handler。
    pub fn new_(ctx: Arc<ConnectionCtx>) -> Self {
        SceneHandler { ctx_: ctx }
    }

    /// 本连接的上下文。
    pub fn ctx_(&self) -> &Arc<ConnectionCtx> {
        &self.ctx_
    }
}

impl TrServeHandler<ServingCfg> for SceneHandler {
    type Err = ServeError;

    fn allowed_methods(&self) -> MethodSet {
        MethodSet::HEAD
            .union(MethodSet::VIEW)
            .union(MethodSet::PULL)
            .union(MethodSet::CALL)
    }

    type HandleAsync<'f>
        = SceneHandleAsync<'f, 'f>
    where
        Self: 'f,
        ServingCfg: 'f;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<ServingCfg>,
    ) -> Self::HandleAsync<'f> {
        SceneHandleAsync::new(self, method, session)
    }
}

/// [`SceneHandler::handle_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(SceneHandle, pub, new(pub(crate)))]
async fn scene_handle_async_<'f, K>(
    handler: &'f SceneHandler,
    method: AccessMethod,
    session: &'f mut ServingSession<ServingCfg>,
    cancel: K,
) -> Result<(), ServeError>
where
    K: TrCancellationToken,
{
    handler.ctx_.touch_();
    let location = session.location().to_string();
    match (method, location.as_str()) {
        (AccessMethod::Call, K_SCENE_JOIN) => {
            join_async_(handler, session, cancel.child_token()).await
        }
        (AccessMethod::Call, K_SCENE_UPDATE) => {
            update_async_(handler, session, cancel.child_token()).await
        }
        (AccessMethod::Call, K_SCENE_UNSUBSCRIBE) => unsubscribe_async_(handler, session).await,
        (AccessMethod::View, K_SCENE_PLANETS) => view_async_(handler, session).await,
        (AccessMethod::Head, K_SCENE_PLANETS) => head_async_(handler, session).await,
        (AccessMethod::Pull, uri) if parse_planet_uri_(uri).is_some() => {
            pull_async_(handler, session, cancel.child_token()).await
        }
        _ => reply_status_async_(session, Status::NotFound).await,
    }
}

/// 读一个可选的 denco 请求体。
async fn read_request_async_<T>(
    session: &mut ServingSession<ServingCfg>,
) -> Result<Option<T>, ServeError>
where
    T: abs_denco::de::TrDecodableOwned,
{
    let has_body = session
        .headers()
        .body_size()
        .is_some_and(|size| size > 0usize);
    if !has_body {
        return Ok(None);
    }
    let mut body = session.req_body_async().await?;
    let value = decode_value_async_::<T, _>(&mut body)
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })?;
    Ok(Some(value))
}

/// 写一条无体状态回复。
async fn reply_status_async_(
    session: &mut ServingSession<ServingCfg>,
    status: Status,
) -> Result<(), ServeError> {
    session.response(status).no_body_async().await?;
    Ok(())
}

/// 写一条定长 denco 回复体。
async fn reply_value_async_<T>(
    session: &mut ServingSession<ServingCfg>,
    status: Status,
    value: &T,
) -> Result<(), ServeError>
where
    T: ?Sized + abs_denco::ser::TrEncodable,
{
    let mut body = encode_to_vec_async_(value)
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })?;
    let len = body.len();
    let mut src: &mut [u8] = body.as_mut_slice();
    session.response(status).sized_async(&mut src, len).await?;
    Ok(())
}

/// `Call /scene/join`。
async fn join_async_<K>(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
    cancel: K,
) -> Result<(), ServeError>
where
    K: TrCancellationToken,
{
    let _ = cancel;
    let request = read_request_async_::<JoinRequest>(session)
        .await?
        .unwrap_or(JoinRequest {
            name: String::new(),
            color: 0u32,
        });

    if handler.ctx_.own_planet_().is_some() {
        // 每个连接只允许 join 一次。
        return reply_status_async_(session, Status::new(409u16)).await;
    }

    let descriptor = handler.ctx_.registry_().lock().unwrap().allocate_planet_(
        handler.ctx_.connection_id_(),
        &request.name,
        request.color,
    );
    handler.ctx_.claim_own_planet_(descriptor.id);

    reply_value_async_(session, Status::Ok, &descriptor).await
}

/// `Call /scene/update`：只操作自己的行星。
async fn update_async_<K>(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
    cancel: K,
) -> Result<(), ServeError>
where
    K: TrCancellationToken,
{
    let _ = cancel;
    let Some(request) = read_request_async_::<UpdateRequest>(session).await? else {
        return reply_status_async_(session, Status::BadRequest).await;
    };
    let Some(planet_id) = handler.ctx_.own_planet_() else {
        // 还没 join：没有「自己的行星」可更新。
        return reply_status_async_(session, Status::new(409u16)).await;
    };

    let updated = handler
        .ctx_
        .registry_()
        .lock().unwrap()
        .update_planet_(planet_id, request.params)
        .ok_or_else(|| ServeError::Target {
            detail: format!("行星 {planet_id} 已不存在"),
        })?;

    let frame = encode_event_async_(&PlanetEvent::Updated(updated.clone()))
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })?;
    handler
        .ctx_
        .hub_()
        .lock().unwrap()
        .broadcast_(planet_id, frame);

    let ack = UpdateAck {
        planet_id,
        version: updated.version,
        params: updated.params,
    };
    reply_value_async_(session, Status::Ok, &ack).await
}

/// `View /scene/planets`：目录，不订阅。
async fn view_async_(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
) -> Result<(), ServeError> {
    let directory = handler.ctx_.registry_().lock().unwrap().directory_();
    reply_value_async_(session, Status::Ok, &directory).await
}

/// `Head /scene/planets`：无体，只给数量与目录版本。
async fn head_async_(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
) -> Result<(), ServeError> {
    let (count, version) = {
        let registry = handler.ctx_.registry_().lock().unwrap();
        (registry.planet_count_(), registry.directory_version_())
    };
    let mut builder = session.response(Status::Ok);
    builder.header(K_HDR_PLANET_COUNT, HeaderVal::from_text(count.to_string()));
    builder.header(
        K_HDR_DIRECTORY_VERSION,
        HeaderVal::from_text(version.to_string()),
    );
    builder.no_body_async().await?;
    Ok(())
}

/// `Call /scene/unsubscribe`：按连接身份移除某颗行星的订阅。
///
/// 这是应用层的清理提示，不是 `mptp_smux` 的传输语义：客户端在丢弃 `Pull` 之前显式
/// 通知服务端，服务端立刻触发该订阅的取消令牌，pull 任务随即退出。
async fn unsubscribe_async_(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
) -> Result<(), ServeError> {
    let Some(request) = read_request_async_::<UnsubscribeRequest>(session).await? else {
        return reply_status_async_(session, Status::BadRequest).await;
    };
    let removed = handler
        .ctx_
        .hub_()
        .lock().unwrap()
        .unsubscribe_connection_(request.planet_id, handler.ctx_.connection_id_());
    let ack = UnsubscribeAck {
        removed: u32::try_from(removed).unwrap_or(u32::MAX),
    };
    reply_value_async_(session, Status::Ok, &ack).await
}

/// 退订守卫：无论正常结束、写失败还是任务被取消，都从 hub 里摘掉订阅。
struct SubGuard_ {
    /// 订阅中心。
    hub_: Arc<Mutex<SceneHub>>,

    /// 行星。
    planet_id_: PlanetId,

    /// 订阅标识。
    sub_id_: SubId,
}

impl Drop for SubGuard_ {
    fn drop(&mut self) {
        let _ = self
            .hub_
            .lock().unwrap()
            .unsubscribe_(self.planet_id_, self.sub_id_);
    }
}

/// 空闲多久写一次心跳。它只用于让服务端发现「客户端已经丢弃 Pull」。
const K_PULL_IDLE_HEARTBEAT: core::time::Duration = core::time::Duration::from_millis(300u64);

/// `Pull /scene/planets/{id}`：先发 `Snapshot`，再持续写更新。
async fn pull_async_<K>(
    handler: &SceneHandler,
    session: &mut ServingSession<ServingCfg>,
    cancel: K,
) -> Result<(), ServeError>
where
    K: TrCancellationToken,
{
    let Some(planet_id) = parse_planet_uri_(session.location()) else {
        return reply_status_async_(session, Status::NotFound).await;
    };
    let Some(snapshot) = handler.ctx_.registry_().lock().unwrap().snapshot_(planet_id) else {
        return reply_status_async_(session, Status::NotFound).await;
    };
    let snapshot_frame = encode_event_async_(&PlanetEvent::Snapshot(snapshot))
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })?;

    // 先登记订阅，再打开后缀流：登记与快照之间没有 await，因此不会漏掉更新。
    let (tx, rx) = flume::bounded::<Arc<[u8]>>(K_SUB_QUEUE_CAP);
    let (canceller, token) = cancel_pair_();
    let sub_id = handler.ctx_.hub_().lock().unwrap().subscribe_(
        planet_id,
        handler.ctx_.connection_id_(),
        tx,
        canceller,
    );
    let _guard = SubGuard_ {
        hub_: handler.ctx_.hub_().clone(),
        planet_id_: planet_id,
        sub_id_: sub_id,
    };

    let mut stream: SuffixStream<'_, ServingCfg> = session
        .response(Status::Ok)
        .suffix_stream_async()
        .may_cancel_with(cancel.child_token())
        .await?;

    // 第一帧：Snapshot。
    stream
        .write_chunk_async(&snapshot_frame)
        .may_cancel_with(token.child_token())
        .await?;

    // 之后：逐帧写更新；队列断开（退订 / 慢订阅被移除 / 行星删除）即结束。
    //
    // 空闲时写心跳：客户端丢弃 `Pull` 后不会再读，写失败就是「订阅已经不存在」的
    // 信号。没有心跳时，服务端会一直停在队列上，无法及时清理订阅者。
    let mut heartbeat = 0u64;
    loop {
        let frame = match compio::time::timeout(K_PULL_IDLE_HEARTBEAT, rx.recv_async()).await {
            Ok(Ok(frame)) => frame,
            // 队列断开：退订 / 行星删除 / 慢订阅被移除。
            Ok(Err(_)) => break,
            Err(_) => {
                heartbeat = heartbeat.saturating_add(1u64);
                let heartbeat_event = PlanetEvent::Heartbeat {
                    sequence: heartbeat,
                };
                match encode_event_async_(&heartbeat_event).await {
                    Ok(frame) => frame,
                    Err(_) => break,
                }
            }
        };
        if stream
            .write_chunk_async(&frame)
            .may_cancel_with(token.child_token())
            .await
            .is_err()
        {
            break;
        }
    }

    // 收尾：半关闭写端、读尽对端。收尾失败已经不影响写出的响应。
    let _ = stream
        .finish_async()
        .may_cancel_with(NonCancellableToken::new())
        .await;
    Ok(())
}

/// 让类型检查器确认 `SceneCancelToken` 可以交给 `mptp_smux` 的取消入口。
const _: fn() = || {
    fn assert_token<T: TrCancellationToken>() {}
    assert_token::<SceneCancelToken>();
};
