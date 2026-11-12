//! 场景服务：controller 形态的端点 + 手写的 `Head` / `Pull` 处理器。
//!
//! `SceneService` 是一个 [`mptp_serving`] controller（每条连接一个实例）：`Call join` /
//! `Call update` / `Call unsubscribe` / `View planets` 四个端点由
//! `#[rpc(interface = "scene")]` 宏生成处理器并注册；`Head planets`（只写头、无体）与
//! `Pull planets/{id}`（后缀流）仍由本模块手写 [`TrEndpointHandler`]。
//!
//! # `handler_`: the scene controller plus hand-written Head / Pull handlers
//!
//! `SceneService` is a per-connection controller. The macro generates handlers for the four
//! unary endpoints; `Head` (headers-only) and `Pull` (suffix stream) are hand-written
//! `TrEndpointHandler` implementations.

use std::sync::{Arc, Mutex};

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use mptp_serving::{HandlerFuture, MethodSet, MptpContext, TrEndpointHandler, box_future_};
use mptp_smux::{
    HeaderVal, ServeError,
    serving::{ServingSession, SuffixStream, config::TrConfig},
};
use rpc_core_v1::serving::{TrReplyError, write_ok_async};
use rpc_svc_macro::rpc;

use crate::scene::{
    cancel_::cancel_pair_,
    protocol_::{
        JoinRequest, K_HDR_DIRECTORY_VERSION, K_HDR_PLANET_COUNT, PlanetDescriptor, PlanetEvent,
        PlanetId, UnsubscribeAck, UnsubscribeRequest, UpdateAck, UpdateRequest,
        encode_event_async_, parse_planet_uri_,
    },
    state_::{ConnectionCtx, K_SUB_QUEUE_CAP, SceneHub, SubId},
};

/// 场景服务（controller）：每条连接一个实例，持有该连接的身份与共享目录 / 订阅中心。
pub struct SceneService {
    /// 本连接的上下文。
    ctx_: Arc<ConnectionCtx>,
}

impl SceneService {
    /// 用连接上下文构造服务。
    pub fn new_(ctx: Arc<ConnectionCtx>) -> Self {
        SceneService { ctx_: ctx }
    }

    /// 本连接的上下文。
    pub fn ctx_(&self) -> &Arc<ConnectionCtx> {
        &self.ctx_
    }
}

/// 场景业务错误：经 [`TrReplyError`] 映射成具体状态码。
#[derive(Debug)]
pub enum SceneError {
    /// 已经加入过场景。
    AlreadyJoined,
    /// 尚未加入场景。
    NotJoined,
    /// 行星不存在。
    PlanetGone { planet_id: PlanetId },
    /// 缺少请求体。
    BadRequest,
    /// 业务值编解码失败。
    Codec(String),
}

impl TrReplyError for SceneError {
    fn status(&self) -> Status {
        match self {
            SceneError::AlreadyJoined => Status::new(409u16),
            SceneError::NotJoined => Status::new(409u16),
            SceneError::PlanetGone { .. } => Status::NotFound,
            SceneError::BadRequest => Status::BadRequest,
            SceneError::Codec(_) => Status::InternalServerError,
        }
    }

    fn message(&self) -> String {
        match self {
            SceneError::AlreadyJoined => "已经加入过场景".to_string(),
            SceneError::NotJoined => "尚未加入场景".to_string(),
            SceneError::PlanetGone { planet_id } => format!("行星 {planet_id} 已不存在"),
            SceneError::BadRequest => "缺少请求体".to_string(),
            SceneError::Codec(detail) => detail.clone(),
        }
    }
}

#[allow(dead_code)]
// 端点方法原样保留但由宏生成的 step 函数驱动；本方法供直接调用 / 测试。
#[rpc(interface = "scene")]
impl SceneService {
    /// `Call /scene/join`：加入场景，创建自己的行星。
    #[rpc(call = "join")]
    async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> {
        if self.ctx_.own_planet_().is_some() {
            return Err(SceneError::AlreadyJoined);
        }
        let descriptor = self.ctx_.registry_().lock().unwrap().allocate_planet_(
            self.ctx_.connection_id_(),
            &req.name,
            req.color,
        );
        self.ctx_.claim_own_planet_(descriptor.id);
        Ok(descriptor)
    }

    /// `Call /scene/update`：只操作自己的行星。
    #[rpc(call = "update")]
    async fn update(&self, req: UpdateRequest) -> Result<UpdateAck, SceneError> {
        let planet_id = self.ctx_.own_planet_().ok_or(SceneError::NotJoined)?;
        let updated = self
            .ctx_
            .registry_()
            .lock()
            .unwrap()
            .update_planet_(planet_id, req.params)
            .ok_or(SceneError::PlanetGone { planet_id })?;
        let frame = encode_event_async_(&PlanetEvent::Updated(updated.clone()))
            .await
            .map_err(|err| SceneError::Codec(err.to_string()))?;
        self.ctx_
            .hub_()
            .lock()
            .unwrap()
            .broadcast_(planet_id, frame);
        Ok(UpdateAck {
            planet_id,
            version: updated.version,
            params: updated.params,
        })
    }

    /// `Call /scene/unsubscribe`：显式退订某颗行星。
    #[rpc(call = "unsubscribe")]
    async fn unsubscribe(&self, req: UnsubscribeRequest) -> Result<UnsubscribeAck, SceneError> {
        let removed = self
            .ctx_
            .hub_()
            .lock()
            .unwrap()
            .unsubscribe_connection_(req.planet_id, self.ctx_.connection_id_());
        Ok(UnsubscribeAck {
            removed: u32::try_from(removed).unwrap_or(u32::MAX),
        })
    }
}

/// `View` / `Head /scene/planets`：同一路径上按访问方法分派。
struct ScenePlanets {
    /// 服务实例。
    svc_: Arc<SceneService>,
}

impl<C: TrConfig, S, R> TrEndpointHandler<C, S, R> for ScenePlanets {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S, R>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            match ctx.method() {
                AccessMethod::View => {
                    let directory = self.svc_.ctx_().registry_().lock().unwrap().directory_();
                    write_ok_async(ctx.session_mut(), &directory).await?;
                    Ok(())
                }
                AccessMethod::Head => {
                    let (count, version) = {
                        let registry = self.svc_.ctx_().registry_().lock().unwrap();
                        (registry.planet_count_(), registry.directory_version_())
                    };
                    let mut builder = ctx.session_mut().response(Status::Ok);
                    builder.header(K_HDR_PLANET_COUNT, HeaderVal::from_text(count.to_string()));
                    builder.header(
                        K_HDR_DIRECTORY_VERSION,
                        HeaderVal::from_text(version.to_string()),
                    );
                    builder.no_body_async().await?;
                    Ok(())
                }
                _ => {
                    ctx.session_mut()
                        .response(Status::MethodNotAllowed)
                        .no_body_async()
                        .await?;
                    Ok(())
                }
            }
        })
    }
}

/// `Pull /scene/planets/{id}`：先发 `Snapshot`，再持续写更新。
struct ScenePull {
    /// 服务实例。
    svc_: Arc<SceneService>,
}

impl<S, R> TrEndpointHandler<crate::scene::cfg_::ServingCfg, S, R> for ScenePull {
    fn handle<'a, 'f>(
        &'a self,
        ctx: &'a mut MptpContext<'f, crate::scene::cfg_::ServingCfg, S, R>,
    ) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let cancel = ctx.cancel().child_token();
            pull_async_(self.svc_.ctx_(), ctx.session_mut(), cancel).await
        })
    }
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
            .lock()
            .unwrap()
            .unsubscribe_(self.planet_id_, self.sub_id_);
    }
}

/// 空闲多久写一次心跳。它只用于让服务端发现「客户端已经丢弃 Pull」。
const K_PULL_IDLE_HEARTBEAT: core::time::Duration = core::time::Duration::from_millis(300u64);

/// `Pull /scene/planets/{id}`：先发 `Snapshot`，再持续写更新。
async fn pull_async_<K>(
    conn: &ConnectionCtx,
    session: &mut ServingSession<crate::scene::cfg_::ServingCfg>,
    cancel: K,
) -> Result<(), ServeError>
where
    K: TrCancellationToken,
{
    let Some(planet_id) = parse_planet_uri_(session.location()) else {
        return reply_status_async_(session, Status::NotFound).await;
    };
    let Some(snapshot) = conn.registry_().lock().unwrap().snapshot_(planet_id) else {
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
    let sub_id =
        conn.hub_()
            .lock()
            .unwrap()
            .subscribe_(planet_id, conn.connection_id_(), tx, canceller);
    let _guard = SubGuard_ {
        hub_: conn.hub_().clone(),
        planet_id_: planet_id,
        sub_id_: sub_id,
    };

    let mut stream: SuffixStream<'_, crate::scene::cfg_::ServingCfg> = session
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
    let mut heartbeat = 0u64;
    loop {
        let frame = match compio::time::timeout(K_PULL_IDLE_HEARTBEAT, rx.recv_async()).await {
            Ok(Ok(frame)) => frame,
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

    // 收尾：半关闭写端、读尽对端。
    let _ = stream
        .finish_async()
        .may_cancel_with(NonCancellableToken::new())
        .await;
    Ok(())
}

/// 写一条无体状态回复。
async fn reply_status_async_<C>(
    session: &mut ServingSession<C>,
    status: Status,
) -> Result<(), ServeError>
where
    C: mptp_smux::serving::config::TrConfig,
{
    session.response(status).no_body_async().await?;
    Ok(())
}

/// 场景根 handler：未命中任何端点时兜底写 404。
struct SceneRoot;

impl<C: TrConfig, S, R> TrEndpointHandler<C, S, R> for SceneRoot {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S, R>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            ctx.session_mut()
                .response(Status::NotFound)
                .no_body_async()
                .await?;
            Ok(())
        })
    }
}

/// 场景路由类型：请求级状态固定为 [`SceneRequestState`]。
///
/// `C = ServingCfg` / 全局分配器 / 无连接状态 / 请求级状态 `SceneRequestState`。
pub type SceneRouter = mptp_serving::RadixRouter<
    crate::scene::cfg_::ServingCfg,
    std::alloc::Global,
    (),
    crate::scene::req_state_::SceneRequestState,
>;

/// 把 controller 端点与手写端点装配进一个 [`mptp_serving::RadixRouter`]。
///
/// 中间件链上先挂 [`PrincipalMiddleware`](crate::scene::req_state_::PrincipalMiddleware)：
/// 它把请求头里的主体写进请求级状态，处理器再只读消费。
///
/// # Errors
///
/// 端点路径冲突或装配失败时返回错误。
pub fn assemble_router(
    svc: Arc<SceneService>,
) -> Result<SceneRouter, mptp_serving::x_deps::matchit_route::InsertError> {
    let router = SceneRouter::new_with_request_state(Arc::new(SceneRoot));
    let router = router.use_middleware(crate::scene::req_state_::PrincipalMiddleware);
    let router = __rpc_controller_SceneService::install_into(router, svc.clone())?;
    let router = router
        .on(
            "scene/planets",
            MethodSet::VIEW.union(MethodSet::HEAD),
            ScenePlanets { svc_: svc.clone() },
        )?
        .pull("scene/planets/{id}", ScenePull { svc_: svc })?;
    Ok(router)
}
