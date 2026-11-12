//! 无 Bevy 的场景测试夹具：真实 TCP + smux + `MptpServlet` / `MptpClient`。
//!
//! - [`SceneFixture`]：最小服务端。每来一条 TCP 连接就建一条 smux 连接、一个
//!   `MptpServlet` 与一个 `SceneHandler`；测试可以从外部读目录 / hub 的计数。
//! - [`SceneTestClient`]：最小客户端，join / View / Head / Pull / update / 退订。
//! - [`FixtureHandler`]：在 `SceneHandler` 之上额外提供两个**仅测试用**的路径，用来把
//!   「多 chunk 顺序」与「一个 denco 对象拆成两个 chunk」变成可断言的事实。
//!
//! 所有传输都走 `mptp_smux` 公开 API；这里没有裸 `ChannelTx`、没有自定义帧格式。
//!
//! # `common`: Bevy-free scene fixtures
//!
//! A minimal server (real TCP + smux + `MptpServlet`) and a minimal client. All transport
//! goes through public `mptp_smux` APIs. Two test-only paths exercise multi-chunk ordering
//! and object splitting.

#![allow(dead_code)]

use std::{
    future::Future,
    net::SocketAddr,
    rc::Rc,
    sync::{Arc, Mutex},
    time::Duration,
};

use abs_art_facade::{TrJoinHandle, TrLocalScope};
use abs_cancel::TrMayCancel;
use abs_mptp::spec::Status;
use anyhow::{Result, anyhow, bail};
use compio::net::TcpListener;
use flume::Receiver;
use mptp_serving::{HandlerFuture, MptpContext, RadixRouter, TrEndpointHandler, box_future_};
use mptp_smux::{
    HeaderKey, HeaderVal, ServeError,
    client::{MptpClient, Response},
    serving::{MptpServlet, SuffixStream},
    x_deps::abs_smux,
};
use mptp_smux_v1_demo::scene::{
    ClientCfg, ConnectionCtx, PlanetDescriptor, PlanetDirectory, PlanetEvent, PlanetId,
    PlanetParams, PlanetRegistry, SceneHub, SceneService, ServingCfg, UpdateAck,
    cfg_::Dock,
    conn_::{accept_conn_async_, dial_async_, share_conn_},
    handler_::assemble_router,
    protocol_::{
        JoinRequest, K_HDR_DIRECTORY_VERSION, K_HDR_PLANET_COUNT, K_SCENE_JOIN, K_SCENE_PLANETS,
        K_SCENE_UNSUBSCRIBE, K_SCENE_UPDATE, UnsubscribeAck, UnsubscribeRequest, UpdateRequest,
        decode_value_async_, encode_to_vec_async_, header_u64_, planet_uri_,
    },
};
use rpc_svc_macro::rpc;

/// 单次测试网络等待的上限。
pub const K_TEST_TIMEOUT: Duration = Duration::from_secs(10u64);

/// 断言「没有事件」时的观察窗口。
pub const K_QUIET_WINDOW: Duration = Duration::from_millis(200u64);

/// 在超时内完成一个 future；超时直接 panic，避免测试挂死。
pub async fn with_timeout_<T>(future: impl Future<Output = T>) -> T {
    compio::time::timeout(K_TEST_TIMEOUT, future)
        .await
        .expect("测试网络等待超时")
}

/// 等待直到条件成立；用于观察服务端异步清理。
pub async fn wait_until_async_(mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + K_TEST_TIMEOUT;
    while !condition() {
        if std::time::Instant::now() > deadline {
            panic!("等待条件成立超时");
        }
        compio::time::sleep(Duration::from_millis(5u64)).await;
    }
}

/// 一个事件的下游队列容量（故意小：便于构造慢订阅者）。
const K_PULL_QUEUE_CAP: usize = 64usize;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 服务端夹具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 最小场景服务端。
pub struct SceneFixture {
    /// 实际监听地址。
    addr_: SocketAddr,

    /// 目录（测试可直接观察）。
    registry_: Arc<Mutex<PlanetRegistry>>,

    /// 订阅中心（测试可直接观察订阅者数量）。
    hub_: Arc<Mutex<SceneHub>>,

    /// `MptpContext` 一级 helper 的观测探针（`/test/ctx` 端点写、测试读）。
    probe_: Arc<Mutex<CtxProbe>>,
}

impl SceneFixture {
    /// 绑定 `127.0.0.1:0` 并启动接受循环。
    ///
    /// # Errors
    ///
    /// 绑定失败时返回错误。
    pub async fn start_async_() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|err| anyhow!("绑定测试监听地址失败：{err}"))?;
        let addr = listener
            .local_addr()
            .map_err(|err| anyhow!("取测试监听地址失败：{err}"))?;
        let registry = Arc::new(Mutex::new(PlanetRegistry::new_()));
        let hub = Arc::new(Mutex::new(SceneHub::new_()));
        let probe = Arc::new(Mutex::new(CtxProbe::default()));
        let scope = mptp_smux_v1_demo::scene::cfg_::compio_rt_().local_scope();

        let registry_for_server = registry.clone();
        let hub_for_server = hub.clone();
        let probe_for_server = probe.clone();
        let accept = scope.clone().spawn_local(async move {
            let mut next_connection = 1u64;
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                let connection_id = next_connection;
                next_connection = next_connection.saturating_add(1u64);
                let registry = registry_for_server.clone();
                let hub = hub_for_server.clone();
                let probe = probe_for_server.clone();
                let task_scope = scope.clone();
                let task = scope.spawn_local(async move {
                    let Ok(conn) = accept_conn_async_(stream).await else {
                        return;
                    };
                    let shared = share_conn_(conn);
                    let ctx = Arc::new(ConnectionCtx::new_(connection_id, registry, hub));
                    let svc = Arc::new(SceneService::new_(ctx.clone()));
                    let router = assemble_router(svc).expect("装配场景路由");
                    let router =
                        __rpc_controller_TestIdSource::install_into(router, Arc::new(TestIdSource))
                            .expect("装配测试 controller");
                    let router = router
                        .pull("test/chunks", TestChunks)
                        .expect("装配测试路由");
                    let router = router.pull("test/split", TestSplit).expect("装配测试路由");
                    let router = router
                        .view(
                            "test/ctx",
                            TestCtx {
                                probe_: probe.clone(),
                            },
                        )
                        .expect("装配测试路由");
                    let router = router
                        .view(
                            "test/ctx-body",
                            TestCtxBody {
                                probe_: probe.clone(),
                            },
                        )
                        .expect("装配测试路由");
                    let router = router
                        .view("test/multi", TestMultiView)
                        .expect("装配测试路由");
                    let router = router
                        .pull("test/multi", TestMultiPull)
                        .expect("装配测试路由");
                    let servlet = Rc::new(MptpServlet::<ServingCfg, _>::new(router));
                    let _cleanup = ConnCleanup_(ctx.clone());
                    let _ = serve_connection_(shared, servlet, ctx, task_scope).await;
                });
                task.detach();
            }
        });
        accept.detach();

        Ok(SceneFixture {
            addr_: addr,
            registry_: registry,
            hub_: hub,
            probe_: probe,
        })
    }

    /// 实际监听地址。
    pub fn addr_(&self) -> SocketAddr {
        self.addr_
    }

    /// 目录句柄。
    pub fn registry_(&self) -> &Arc<Mutex<PlanetRegistry>> {
        &self.registry_
    }

    /// 订阅中心句柄。
    pub fn hub_(&self) -> &Arc<Mutex<SceneHub>> {
        &self.hub_
    }

    /// `MptpContext` 一级 helper 的观测探针。
    pub fn probe_(&self) -> &Arc<Mutex<CtxProbe>> {
        &self.probe_
    }
}

/// 连接任务被取消时的兜底清理（不广播 `Removed`）。
struct ConnCleanup_(Arc<ConnectionCtx>);

impl Drop for ConnCleanup_ {
    fn drop(&mut self) {
        self.0.cleanup_sync_();
    }
}

/// 在一条已建成的连接上绑定默认服务垛口并循环接受子流。
async fn serve_connection_(
    shared: mptp_smux_v1_demo::scene::cfg_::SharedConn,
    servlet: Rc<MptpServlet<ServingCfg, RadixRouter<ServingCfg>>>,
    ctx: Arc<ConnectionCtx>,
    scope: mptp_smux_v1_demo::scene::cfg_::SceneScope,
) -> Result<()> {
    let mut binding = abs_smux::conn::TrConnection::bind_async(
        &*shared,
        Dock::new(u32::from(mptp_smux::K_DEFAULT_SERVING_DOCK)),
    )
    .await
    .map_err(|err| anyhow!("绑定服务垛口失败：{err}"))?;
    let mut listener = servlet
        .listen_async(&mut binding, 8usize)
        .await
        .map_err(|err| anyhow!("监听失败：{err}"))?;
    loop {
        let session = match servlet.accept_async(&mut listener).await {
            Ok(session) => session,
            // 连接失败 / 对端关闭：离开接受循环，交给清理守卫。
            Err(_) => break,
        };
        let servlet = servlet.clone();
        let token = ctx.token_();
        let task = scope.spawn_local(async move {
            let _ = servlet
                .serve_session_async(session)
                .may_cancel_with(token)
                .await;
        });
        task.detach();
    }
    ctx.cleanup_async_().await;
    Ok(())
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 仅测试用的端点：补充两条协议路径
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `Pull /test/chunks`：连续写三个 `Removed` 事件 chunk。
struct TestChunks;

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestChunks {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let mut stream: SuffixStream<'_, C> = ctx
                .session_mut()
                .response(Status::Ok)
                .suffix_stream_async()
                .await?;
            for index in 1u64..=3u64 {
                let event = PlanetEvent::Removed {
                    planet_id: index,
                    version: index,
                };
                let bytes =
                    encode_to_vec_async_(&event)
                        .await
                        .map_err(|err| ServeError::Target {
                            detail: err.to_string(),
                        })?;
                stream.write_chunk_async(&bytes).await?;
            }
            let _ = stream.finish_async().await;
            Ok(())
        })
    }
}

/// `Pull /test/split`：把一个事件拆成两个 chunk 写。
struct TestSplit;

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestSplit {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let event = PlanetEvent::Updated(PlanetDescriptor {
                id: 7u64,
                owner: 1u64,
                name: String::from("split-event"),
                stream_uri: planet_uri_(7u64),
                params: PlanetParams {
                    orbit_speed: 0.5f32,
                    spin_speed: 0.25f32,
                    color: 0x12_34_56u32,
                    ..PlanetParams::default()
                },
                version: 3u64,
            });
            let bytes = encode_to_vec_async_(&event)
                .await
                .map_err(|err| ServeError::Target {
                    detail: err.to_string(),
                })?;
            let middle = bytes.len() / 2usize;
            let mut stream: SuffixStream<'_, C> = ctx
                .session_mut()
                .response(Status::Ok)
                .suffix_stream_async()
                .await?;
            stream.write_chunk_async(&bytes[..middle]).await?;
            stream.write_chunk_async(&bytes[middle..]).await?;
            let _ = stream.finish_async().await;
            Ok(())
        })
    }
}

/// `MptpContext` 一级 helper 的观测探针：`/test/ctx*` 端点写、测试读。
///
/// 端点写出回复前缀之后无法再把「已回复」放进同一条响应头，因此观测结果经共享探针带出
/// 请求作用域；测试在收到回复后轮询 [`CtxProbe::done`]。
///
/// # `CtxProbe`: observations taken inside a `MptpContext` endpoint
///
/// The endpoint writes its findings into this shared probe because a reply header cannot
/// report "the reply has started". Tests poll [`CtxProbe::done`] after receiving the reply.
#[derive(Debug, Default)]
pub struct CtxProbe {
    /// 写回复前 `reply_written()` 的观测值（预期 `false`）。
    pub reply_before: bool,

    /// 写回复后 `reply_written()` 的观测值（预期 `true`）。
    pub reply_after: bool,

    /// `headers_mut` 写入后能否从 `headers()` 读回（预期 `true`）。
    pub headers_mutated: bool,

    /// `req_body_async` 看到的剩余体字节数（无体请求预期 `Some(0)`）。
    pub body_remaining: Option<usize>,

    /// 体读视图是否已经读完（无体请求预期 `true`）。
    pub body_finished: bool,

    /// 端点是否已经跑完（测试轮询位）。
    pub done: bool,
}

/// `View /test/ctx`：验证 `reply_written` / `headers_mut` / `response` 的委托。
///
/// `View /test/ctx`: exercises the `reply_written` / `headers_mut` / `response` delegates.
struct TestCtx {
    /// 观测探针。
    probe_: Arc<Mutex<CtxProbe>>,
}

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestCtx {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let before = ctx.reply_written();
            ctx.headers_mut()
                .set("X-Ctx-Probe", HeaderVal::from_text("mutated"));
            let mutated = ctx
                .headers()
                .try_get(HeaderKey::from_text("X-Ctx-Probe"))
                .and_then(HeaderVal::as_text)
                == Some("mutated");
            ctx.response(Status::Ok).no_body_async().await?;
            let after = ctx.reply_written();
            let mut probe = self.probe_.lock().expect("ctx 探针锁被污染");
            probe.reply_before = before;
            probe.reply_after = after;
            probe.headers_mutated = mutated;
            probe.done = true;
            Ok(())
        })
    }
}

/// `View /test/ctx-body`：验证 `req_body_async` 的委托（无体请求）。
///
/// `View /test/ctx-body`: exercises the `req_body_async` delegate on an empty body.
struct TestCtxBody {
    /// 观测探针。
    probe_: Arc<Mutex<CtxProbe>>,
}

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestCtxBody {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let body = ctx.req_body_async().await?;
            let remaining = body.remaining();
            let finished = body.is_finished();
            drop(body);
            ctx.response(Status::Ok).no_body_async().await?;
            let mut probe = self.probe_.lock().expect("ctx 探针锁被污染");
            probe.body_remaining = remaining;
            probe.body_finished = finished;
            probe.done = true;
            Ok(())
        })
    }
}

/// `View /test/multi`：同路径多方法测试的 View 侧（回复一个标记头）。
///
/// `View /test/multi`: the VIEW side of the same-path multi-method test.
struct TestMultiView;

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestMultiView {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let mut response = ctx.response(Status::Ok);
            response.header("X-Multi-Route", HeaderVal::from_text("view"));
            response.no_body_async().await?;
            Ok(())
        })
    }
}

/// `Pull /test/multi`：同路径多方法测试的 Pull 侧（写一个事件后收尾）。
///
/// `Pull /test/multi`: the PULL side of the same-path multi-method test.
struct TestMultiPull;

impl<C: mptp_smux::serving::config::TrConfig, S> TrEndpointHandler<C, S> for TestMultiPull {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move {
            let event = PlanetEvent::Removed {
                planet_id: 9u64,
                version: 1u64,
            };
            let bytes = encode_to_vec_async_(&event)
                .await
                .map_err(|err| ServeError::Target {
                    detail: err.to_string(),
                })?;
            let mut stream: SuffixStream<'_, C> =
                ctx.response(Status::Ok).suffix_stream_async().await?;
            stream.write_chunk_async(&bytes).await?;
            let _ = stream.finish_async().await;
            Ok(())
        })
    }
}

/// 测试用 controller：验证宏生成的 `Pull` 端点「路径参数解码 → 迭代器逐项写后缀流」。
pub struct TestIdSource;

#[rpc(interface = "test")]
impl TestIdSource {
    /// 拉取 `0..count` 的 id 序列（有限流，写完后半关闭）。
    #[rpc(pull = "ids/{count}")]
    fn ids(
        &self,
        count: u64,
    ) -> impl rpc_core_v1::x_deps::abs_async_iter::TrAsyncIterator<Item = u64> {
        0..count
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 客户端夹具
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 最小场景客户端。
pub struct SceneTestClient {
    /// MPTP 客户端。
    client_: Rc<MptpClient<ClientCfg>>,

    /// 共享连接（保活）。
    shared_: mptp_smux_v1_demo::scene::cfg_::SharedConn,
}

impl SceneTestClient {
    /// 拨号并建出客户端。
    ///
    /// # Errors
    ///
    /// 连接或握手失败时返回错误。
    pub async fn connect_async_(addr: SocketAddr) -> Result<Self> {
        let conn = with_timeout_(dial_async_(addr)).await?;
        let shared = share_conn_(conn);
        let client = Rc::new(MptpClient::<ClientCfg>::new(shared.clone()));
        Ok(SceneTestClient {
            client_: client,
            shared_: shared,
        })
    }

    /// 加入场景。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn join_async_(&self, name: &str, color: u32) -> Result<PlanetDescriptor> {
        let request = JoinRequest {
            name: name.to_string(),
            color,
        };
        let body = encode_to_vec_async_(&request).await?;
        let mut resp = with_timeout_(async {
            self.client_
                .invoke(K_SCENE_JOIN)
                .body_bytes(&body)
                .send_async()
                .await
        })
        .await?;
        ensure_ok_(&resp, K_SCENE_JOIN)?;
        let mut view = resp.body_async().await?;
        let descriptor = decode_value_async_::<PlanetDescriptor, _>(&mut view).await?;
        Ok(descriptor)
    }

    /// 列出目录。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn view_async_(&self) -> Result<PlanetDirectory> {
        let mut resp =
            with_timeout_(async { self.client_.view(K_SCENE_PLANETS).send_async().await }).await?;
        ensure_ok_(&resp, K_SCENE_PLANETS)?;
        let mut view = resp.body_async().await?;
        let directory = decode_value_async_::<PlanetDirectory, _>(&mut view).await?;
        Ok(directory)
    }

    /// `Head /scene/planets`：返回（行星数量, 目录版本）。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或头缺失 / 非法时返回错误。
    pub async fn head_async_(&self) -> Result<(u64, u64)> {
        let resp =
            with_timeout_(async { self.client_.head(K_SCENE_PLANETS).send_async().await }).await?;
        ensure_ok_(&resp, K_SCENE_PLANETS)?;
        let headers = resp.headers().ok_or_else(|| anyhow!("Head 回复没有头表"))?;
        let count = header_u64_(headers, K_HDR_PLANET_COUNT)
            .ok_or_else(|| anyhow!("Head 回复缺少行星数量头"))?;
        let version = header_u64_(headers, K_HDR_DIRECTORY_VERSION)
            .ok_or_else(|| anyhow!("Head 回复缺少目录版本头"))?;
        Ok((count, version))
    }

    /// 用完整参数更新自己的行星。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn update_async_(&self, params: PlanetParams) -> Result<UpdateAck> {
        let request = UpdateRequest { params };
        let body = encode_to_vec_async_(&request).await?;
        let mut resp = with_timeout_(async {
            self.client_
                .invoke(K_SCENE_UPDATE)
                .body_bytes(&body)
                .send_async()
                .await
        })
        .await?;
        ensure_ok_(&resp, K_SCENE_UPDATE)?;
        let mut view = resp.body_async().await?;
        let ack = decode_value_async_::<UpdateAck, _>(&mut view).await?;
        Ok(ack)
    }

    /// 订阅一个行星流（或测试路径）。
    ///
    /// 状态非 200 时返回错误；成功时后台任务持续把 denco 事件送进本地队列。
    ///
    /// # Errors
    ///
    /// 请求失败或状态非 200 时返回错误。
    pub async fn pull_async_(&self, uri: &str) -> Result<ScenePull> {
        self.pull_with_capacity_async_(uri, K_PULL_QUEUE_CAP).await
    }

    /// 用指定本地队列容量订阅；容量 1 可用来构造「慢订阅者」。
    ///
    /// # Errors
    ///
    /// 请求失败或状态非 200 时返回错误。
    pub async fn pull_with_capacity_async_(&self, uri: &str, capacity: usize) -> Result<ScenePull> {
        let mut resp = with_timeout_(async { self.client_.pull(uri).send_async().await }).await?;
        ensure_ok_(&resp, uri)?;
        let (tx, rx) = flume::bounded::<PlanetEvent>(capacity);
        let scope = mptp_smux_v1_demo::scene::cfg_::compio_rt_().local_scope();
        let handle = scope.spawn_local(async move {
            let Ok(mut suffix) = resp.suffix_async().await else {
                return;
            };
            loop {
                match decode_value_async_::<PlanetEvent, _>(&mut suffix).await {
                    // 心跳只用于服务端发现退订，测试客户端直接忽略。
                    Ok(PlanetEvent::Heartbeat { .. }) => continue,
                    Ok(event) => {
                        if tx.send_async(event).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let planet_id_ = mptp_smux_v1_demo::scene::protocol_::parse_planet_uri_(uri);
        Ok(ScenePull {
            rx_: rx,
            handle_: Some(handle),
            client_: self.client_.clone(),
            planet_id_,
        })
    }

    /// 直接发一条 `Pull` 并返回状态码（用于 404 断言）。
    ///
    /// # Errors
    ///
    /// 请求本身失败时返回错误；状态码由调用者判断。
    pub async fn pull_status_async_(&self, uri: &str) -> Result<Status> {
        let resp = with_timeout_(async { self.client_.pull(uri).send_async().await }).await?;
        Ok(resp.status())
    }

    /// 拉取宏生成 Pull 端点 `/test/ids/{count}`，把 `0..count` 的每个 `u64` 解成列表。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或取后缀流失败时返回错误。
    pub async fn pull_ids_async_(&self, count: u64) -> Result<Vec<u64>> {
        let uri = format!("/test/ids/{count}");
        let mut resp = with_timeout_(async { self.client_.pull(&uri).send_async().await }).await?;
        ensure_ok_(&resp, &uri)?;
        let mut suffix = resp.suffix_async().await?;
        let mut ids = Vec::new();
        while let Ok(id) = decode_value_async_::<u64, _>(&mut suffix).await {
            ids.push(id);
        }
        Ok(ids)
    }

    /// 发一条不允许的方法（`Post`），返回状态与 `Allow` 头文本。
    ///
    /// # Errors
    ///
    /// 请求本身失败时返回错误。
    pub async fn post_allow_async_(&self, uri: &str) -> Result<(Status, Option<String>)> {
        let resp = with_timeout_(async { self.client_.post(uri).send_async().await }).await?;
        let allow = resp
            .headers()
            .and_then(|headers| headers.try_get(mptp_smux::serving::K_TMP_ALLOW_HEADER_KEY))
            .and_then(|value| value.as_text().map(str::to_string));
        Ok((resp.status(), allow))
    }

    /// 底层客户端（测试需要自定义请求时使用）。
    pub fn client_(&self) -> &Rc<MptpClient<ClientCfg>> {
        &self.client_
    }

    /// 显式退订一颗行星（业务层清理提示）。
    ///
    /// # Errors
    ///
    /// 请求失败或状态非 200 时返回错误。
    pub async fn unsubscribe_planet_async_(&self, planet_id: u64) -> Result<UnsubscribeAck> {
        let request = UnsubscribeRequest { planet_id };
        let body = encode_to_vec_async_(&request).await?;
        let mut resp = with_timeout_(async {
            self.client_
                .invoke(K_SCENE_UNSUBSCRIBE)
                .body_bytes(&body)
                .send_async()
                .await
        })
        .await?;
        ensure_ok_(&resp, K_SCENE_UNSUBSCRIBE)?;
        let mut view = resp.body_async().await?;
        let ack = decode_value_async_::<UnsubscribeAck, _>(&mut view).await?;
        Ok(ack)
    }

    /// 共享连接（保活）。
    pub fn shared_(&self) -> &mptp_smux_v1_demo::scene::cfg_::SharedConn {
        &self.shared_
    }
}

/// 检查回复状态为 200。
fn ensure_ok_(resp: &Response<ClientCfg>, what: &str) -> Result<()> {
    if resp.status() != Status::Ok {
        bail!("{what} 返回 {}", resp.status().inner());
    }
    Ok(())
}

/// 本地投递任务的句柄类型（固定 compio）。
type PullHandle = mptp_smux_v1_demo::scene::cfg_::SceneJoinHandle<()>;

/// 一条已订阅的行星流：后台任务把事件送进本地队列。
pub struct ScenePull {
    /// 事件队列。
    rx_: Receiver<PlanetEvent>,

    /// 后台读取任务；丢弃即停止本地读取。
    handle_: Option<PullHandle>,

    /// 发退订提示用的客户端。
    client_: Rc<MptpClient<ClientCfg>>,

    /// 行星流 URI 对应的行星 id（测试路径为 `None`）。
    planet_id_: Option<u64>,
}

impl ScenePull {
    /// 取下一个事件；流结束 / 任务被取消时返回错误。
    ///
    /// # Errors
    ///
    /// 队列关闭或超时时返回错误。
    pub async fn next_event_async_(&mut self) -> Result<PlanetEvent> {
        with_timeout_(self.rx_.recv_async())
            .await
            .map_err(|err| anyhow!("等待行星事件失败：{err}"))
    }

    /// 在给定窗口内等待事件；没有事件时返回 `None`。
    ///
    /// # Errors
    ///
    /// 队列已关闭时返回错误。
    pub async fn next_event_within_async_(
        &mut self,
        window: Duration,
    ) -> Result<Option<PlanetEvent>> {
        match compio::time::timeout(window, self.rx_.recv_async()).await {
            Ok(Ok(event)) => Ok(Some(event)),
            Ok(Err(err)) => Err(anyhow!("等待行星事件失败：{err}")),
            Err(_) => Ok(None),
        }
    }

    /// 队列里是否还有未读事件。
    pub fn is_empty_(&self) -> bool {
        self.rx_.is_empty()
    }

    /// 显式退订：先通知服务端移除订阅，再取消后台读取任务。
    ///
    /// # Errors
    ///
    /// 退订提示发送失败时返回错误；本地任务仍会被取消。
    pub async fn unsubscribe_async_(&mut self) -> Result<()> {
        if let Some(planet_id) = self.planet_id_ {
            let request = UnsubscribeRequest { planet_id };
            let body = encode_to_vec_async_(&request).await?;
            let resp = with_timeout_(async {
                self.client_
                    .invoke(K_SCENE_UNSUBSCRIBE)
                    .body_bytes(&body)
                    .send_async()
                    .await
            })
            .await?;
            ensure_ok_(&resp, K_SCENE_UNSUBSCRIBE)?;
        }
        self.handle_.take();
        Ok(())
    }
}

impl Drop for ScenePull {
    fn drop(&mut self) {
        // compio 的 JoinHandle 丢弃即取消任务；这等价于「丢弃 Pull 会话」。
        self.handle_.take();
    }
}

/// 让测试可以显式等待服务端把订阅清理掉。
pub async fn wait_subscribers_(fixture: &SceneFixture, planet_id: PlanetId, expected: usize) {
    wait_until_async_(|| fixture.hub_().lock().unwrap().subscriber_count_(planet_id) == expected)
        .await;
}

/// 构造一个「只有一条连接」的测试客户端与它的行星。
pub async fn connect_and_join_(
    fixture: &SceneFixture,
    name: &str,
    color: u32,
) -> Result<(SceneTestClient, PlanetDescriptor)> {
    let client = SceneTestClient::connect_async_(fixture.addr_()).await?;
    let descriptor = client.join_async_(name, color).await?;
    Ok((client, descriptor))
}

/// 在保持其它外观参数不变的前提下，构造一组新的速度参数。
pub fn params_with_speeds_(
    planet: &PlanetDescriptor,
    orbit_speed: f32,
    spin_speed: f32,
) -> PlanetParams {
    let mut params = planet.params;
    params.orbit_speed = orbit_speed;
    params.spin_speed = spin_speed;
    params
}
