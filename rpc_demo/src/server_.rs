//! 服务端：两级 Trie + 每 service 队列 + 独立 worker 线程。
//!
//! 这是蓝图骨架的手动实现：
//!
//! - `MptpServer` 经本地 `Router`（实现 `TrPathRouter`）把 `ChannelTx` / `ChannelRx` 按值
//!   交给 `RpcRouter`；
//! - `RpcRouter` 先按 interface 前缀、再按 service 路径在运行期 `PathTrie` 中匹配；
//! - 命中的 service 有自己的有界队列，worker 线程只持有该 service 的具体实现；
//! - worker 内按 body enum 的方法变体分派，没有 `Pin<Box<dyn Future>>`。
//!
//! # `server_`: two-level trie, per-service queues, worker threads
//!
//! `MptpServer` dispatches through the demo's local `Router` (a `TrPathRouter` implementation)
//! to `RpcRouter`. No macro-generated match participates in routing. The two dispatch levels
//! are runtime tries; only method dispatch inside a service uses the hand-written body enum.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use thiserror::Error;
use tokio::sync::mpsc;

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mm::CoreAlloc;
use abs_mptp::{
    serving::TrReqHandler,
    spec::{Status, StdHeaderKey, StdHeaderVal, access_method::AccessMethod},
};
use abs_smux::{conn::TrConnection, dock::Dock};
use anyhow::{Result, anyhow};
use gen_mcf2::gen_may_cancel_future;
use mm_ptr::Shared;
use mptp_smux::{
    messaging::{Headers, ResponsePrefix, write_response_prefix_async},
    serving::{
        MptpServer, close_channel_async,
        config::{ChannelRx, ChannelTx},
    },
    x_deps::{abs_cancel, abs_smux, gen_mcf2, mm_ptr},
};
use rpc_core_v1::ServingConfig;

use crate::{
    body_::{
        BodyError, decode_slice_async_, encode_to_vec_async_, read_body_async_, write_body_async_,
    },
    conn_::Conn,
    protocol_::{
        ApiV1, ApiV2, K_EXPECTED_JOBS, RpcInterface, RpcResult, ServiceError, Session, SessionCall,
        SessionService, Workspace, WorkspaceCall, WorkspaceService,
    },
    router_::Router,
    trie_::{PathTrie, split_segments_},
};

/// 服务端的完整配置类型。
type SrvCfg = ServingConfig<Conn>;

/// 监听 dock：协议约定的默认服务垛口。
const K_DOCK: u32 = 0x80u32;

/// 每条 service 队列的容量。
const K_QUEUE_CAP: usize = 32usize;

/// 监听循环的入向邀请上限。
const K_LISTEN_RESERVE: usize = 8usize;

/// 计数达到后的静默期。
const K_DRAIN_PAUSE: Duration = Duration::from_millis(500u64);

/// 内存存储。
#[derive(Debug)]
struct Store {
    /// 下一个 workspace ID。
    next_workspace_: u64,

    /// 下一个 session ID。
    next_session_: u64,

    /// workspace 表。
    workspaces_: BTreeMap<u64, Workspace>,

    /// session 表。
    sessions_: BTreeMap<u64, Session>,
}

impl Store {
    /// 造一个空存储。
    fn new_() -> Self {
        Store {
            next_workspace_: 1u64,
            next_session_: 1u64,
            workspaces_: BTreeMap::new(),
            sessions_: BTreeMap::new(),
        }
    }

    /// 创建 workspace。
    fn create_workspace_(&mut self, name: String) -> Result<Workspace, ServiceError> {
        let id = self.next_workspace_;
        self.next_workspace_ += 1u64;
        let workspace = Workspace { id, name };
        self.workspaces_.insert(id, workspace.clone());
        Result::Ok(workspace)
    }

    /// 列出全部 workspace。
    fn list_workspaces_(&self) -> Vec<Workspace> {
        self.workspaces_.values().cloned().collect()
    }

    /// 查看 workspace。
    fn get_workspace_(&self, id: u64) -> Result<Option<Workspace>, ServiceError> {
        Result::Ok(self.workspaces_.get(&id).cloned())
    }

    /// 重命名 workspace。
    fn rename_workspace_(&mut self, id: u64, name: String) -> Result<Workspace, ServiceError> {
        let workspace = self
            .workspaces_
            .get_mut(&id)
            .ok_or(ServiceError::WorkspaceNotFound(id))?;
        workspace.name = name;
        Result::Ok(workspace.clone())
    }

    /// 删除 workspace；有 session 时拒绝。
    fn delete_workspace_(&mut self, id: u64) -> Result<bool, ServiceError> {
        if self
            .sessions_
            .values()
            .any(|session| session.workspace_id == id)
        {
            return Result::Err(ServiceError::WorkspaceNotEmpty(id));
        }
        Result::Ok(self.workspaces_.remove(&id).is_some())
    }

    /// 创建 session。
    fn create_session_(
        &mut self,
        workspace_id: u64,
        title: String,
    ) -> Result<Session, ServiceError> {
        if !self.workspaces_.contains_key(&workspace_id) {
            return Result::Err(ServiceError::WorkspaceNotFound(workspace_id));
        }
        let id = self.next_session_;
        self.next_session_ += 1u64;
        let session = Session {
            id,
            workspace_id,
            title,
        };
        self.sessions_.insert(id, session.clone());
        Result::Ok(session)
    }

    /// 列出 workspace 下的 session。
    fn list_sessions_(&self, workspace_id: u64) -> Vec<Session> {
        self.sessions_
            .values()
            .filter(|session| session.workspace_id == workspace_id)
            .cloned()
            .collect()
    }

    /// 查看 session。
    fn get_session_(&self, id: u64) -> Result<Option<Session>, ServiceError> {
        Result::Ok(self.sessions_.get(&id).cloned())
    }

    /// 重命名 session。
    fn rename_session_(&mut self, id: u64, title: String) -> Result<Session, ServiceError> {
        let session = self
            .sessions_
            .get_mut(&id)
            .ok_or(ServiceError::SessionNotFound(id))?;
        session.title = title;
        Result::Ok(session.clone())
    }

    /// 删除 session。
    fn delete_session_(&mut self, id: u64) -> Result<bool, ServiceError> {
        Result::Ok(self.sessions_.remove(&id).is_some())
    }
}

/// 工作区 service 的实现。
struct WorkspaceServiceImpl {
    /// 共享存储。
    store_: Arc<Mutex<Store>>,
}

impl WorkspaceServiceImpl {
    /// 由共享存储构造。
    const fn new_(store: Arc<Mutex<Store>>) -> Self {
        WorkspaceServiceImpl { store_: store }
    }

    /// 取存储锁。
    fn lock_(&self) -> Result<std::sync::MutexGuard<'_, Store>, ServiceError> {
        self.store_
            .lock()
            .map_err(|_| ServiceError::Lock("mutex poisoned".to_string()))
    }
}

impl WorkspaceService for WorkspaceServiceImpl {
    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        CreateWorkspaceImplAsync::new(self, name)
    }

    fn list_workspaces<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Workspace>>> {
        ListWorkspacesImplAsync::new(self)
    }

    fn get_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Workspace>>> {
        GetWorkspaceImplAsync::new(self, id)
    }

    fn rename_workspace<'f>(
        &'f self,
        id: u64,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        RenameWorkspaceImplAsync::new(self, id, name)
    }

    fn delete_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>> {
        DeleteWorkspaceImplAsync::new(self, id)
    }
}

/// `WorkspaceServiceImpl::create_workspace` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(CreateWorkspaceImpl, pub(crate), new(pub(crate)))]
async fn create_workspace_impl_<'f, TyTok>(
    service: &'f WorkspaceServiceImpl,
    name: String,
    cancel: TyTok,
) -> RpcResult<Workspace>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.create_workspace_(name)
}

/// `WorkspaceServiceImpl::list_workspaces` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ListWorkspacesImpl, pub(crate), new(pub(crate)))]
async fn list_workspaces_impl_<'f, TyTok>(
    service: &'f WorkspaceServiceImpl,
    cancel: TyTok,
) -> RpcResult<Vec<Workspace>>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    Result::Ok(service.lock_()?.list_workspaces_())
}

/// `WorkspaceServiceImpl::get_workspace` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(GetWorkspaceImpl, pub(crate), new(pub(crate)))]
async fn get_workspace_impl_<'f, TyTok>(
    service: &'f WorkspaceServiceImpl,
    id: u64,
    cancel: TyTok,
) -> RpcResult<Option<Workspace>>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.get_workspace_(id)
}

/// `WorkspaceServiceImpl::rename_workspace` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RenameWorkspaceImpl, pub(crate), new(pub(crate)))]
async fn rename_workspace_impl_<'f, TyTok>(
    service: &'f WorkspaceServiceImpl,
    id: u64,
    name: String,
    cancel: TyTok,
) -> RpcResult<Workspace>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.rename_workspace_(id, name)
}

/// `WorkspaceServiceImpl::delete_workspace` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DeleteWorkspaceImpl, pub(crate), new(pub(crate)))]
async fn delete_workspace_impl_<'f, TyTok>(
    service: &'f WorkspaceServiceImpl,
    id: u64,
    cancel: TyTok,
) -> RpcResult<bool>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.delete_workspace_(id)
}

/// 会话 service 的实现。
struct SessionServiceImpl {
    /// 共享存储。
    store_: Arc<Mutex<Store>>,
}

impl SessionServiceImpl {
    /// 由共享存储构造。
    const fn new_(store: Arc<Mutex<Store>>) -> Self {
        SessionServiceImpl { store_: store }
    }

    /// 取存储锁。
    fn lock_(&self) -> Result<std::sync::MutexGuard<'_, Store>, ServiceError> {
        self.store_
            .lock()
            .map_err(|_| ServiceError::Lock("mutex poisoned".to_string()))
    }
}

impl SessionService for SessionServiceImpl {
    fn create_session<'f>(
        &'f self,
        workspace_id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>> {
        CreateSessionImplAsync::new(self, workspace_id, title)
    }

    fn list_sessions<'f>(
        &'f self,
        workspace_id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Session>>> {
        ListSessionsImplAsync::new(self, workspace_id)
    }

    fn get_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Session>>> {
        GetSessionImplAsync::new(self, id)
    }

    fn rename_session<'f>(
        &'f self,
        id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>> {
        RenameSessionImplAsync::new(self, id, title)
    }

    fn delete_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>> {
        DeleteSessionImplAsync::new(self, id)
    }
}

/// `SessionServiceImpl::create_session` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(CreateSessionImpl, pub(crate), new(pub(crate)))]
async fn create_session_impl_<'f, TyTok>(
    service: &'f SessionServiceImpl,
    workspace_id: u64,
    title: String,
    cancel: TyTok,
) -> RpcResult<Session>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.create_session_(workspace_id, title)
}

/// `SessionServiceImpl::list_sessions` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ListSessionsImpl, pub(crate), new(pub(crate)))]
async fn list_sessions_impl_<'f, TyTok>(
    service: &'f SessionServiceImpl,
    workspace_id: u64,
    cancel: TyTok,
) -> RpcResult<Vec<Session>>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    Result::Ok(service.lock_()?.list_sessions_(workspace_id))
}

/// `SessionServiceImpl::get_session` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(GetSessionImpl, pub(crate), new(pub(crate)))]
async fn get_session_impl_<'f, TyTok>(
    service: &'f SessionServiceImpl,
    id: u64,
    cancel: TyTok,
) -> RpcResult<Option<Session>>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.get_session_(id)
}

/// `SessionServiceImpl::rename_session` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RenameSessionImpl, pub(crate), new(pub(crate)))]
async fn rename_session_impl_<'f, TyTok>(
    service: &'f SessionServiceImpl,
    id: u64,
    title: String,
    cancel: TyTok,
) -> RpcResult<Session>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.rename_session_(id, title)
}

/// `SessionServiceImpl::delete_session` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DeleteSessionImpl, pub(crate), new(pub(crate)))]
async fn delete_session_impl_<'f, TyTok>(
    service: &'f SessionServiceImpl,
    id: u64,
    cancel: TyTok,
) -> RpcResult<bool>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    service.lock_()?.delete_session_(id)
}

/// 一次请求交给 worker 的完整上下文。
struct RpcJob {
    /// 完整 location（用于日志）。
    location_: String,

    /// 请求头。
    headers_: Headers,

    /// 子流发送半边。
    tx_: ChannelTx<SrvCfg>,

    /// 子流接收半边。
    rx_: ChannelRx<SrvCfg>,
}

// 队列元素必须能跨线程交给 worker；这同时证明两个 channel 半边是 `Send` 的。
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<RpcJob>();
};

/// interface 级路由项。
struct InterfaceEntry {
    /// 该 interface 下的 service Trie。
    services_: PathTrie<ServiceEntry>,
}

/// service 级路由项。
struct ServiceEntry {
    /// 该 service 的队列。
    tx_: mpsc::Sender<RpcJob>,
}

/// 两级 Trie 路由器；它本身实现 `TrReqHandler`。
pub(crate) struct RpcRouter {
    /// interface 级 Trie。
    interfaces_: PathTrie<InterfaceEntry>,
}

impl RpcRouter {
    /// 造一个空 router。
    pub(crate) fn new() -> Self {
        RpcRouter {
            interfaces_: PathTrie::new(),
        }
    }

    /// 注册一个 interface 及其 service 队列。
    fn add_interface(
        &mut self,
        interface: &'static str,
        services: Vec<(&'static str, mpsc::Sender<RpcJob>)>,
    ) {
        let mut services_trie = PathTrie::new();
        for (service_path, tx) in services {
            services_trie.insert(service_path, ServiceEntry { tx_: tx });
        }
        self.interfaces_.insert(
            interface,
            InterfaceEntry {
                services_: services_trie,
            },
        );
    }

    /// 两级匹配：interface 前缀 → 该 interface 内的 service 前缀。
    fn route_(&self, location: &str) -> Option<&mpsc::Sender<RpcJob>> {
        let segments = split_segments_(location);
        let (interface, depth) = self.interfaces_.match_longest(&segments)?;
        let (service, _) = interface.services_.match_longest(&segments[depth..])?;
        Option::Some(&service.tx_)
    }
}

impl Default for RpcRouter {
    fn default() -> Self {
        Self::new()
    }
}

/// router 分派错误。
#[derive(Debug, Error)]
pub enum RouterError {
    /// service 队列已关闭。
    #[error("service 队列已关闭")]
    QueueClosed,

    /// 写拒绝响应失败。
    #[error("写拒绝响应失败：{0}")]
    Reject(String),
}

impl TrReqHandler<SrvCfg> for RpcRouter {
    type HandleAsync<'f>
        = RouterHandleAsync<'f, 'f>
    where
        Self: 'f;

    type Err = RouterError;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut Headers,
        chan_tx: ChannelTx<SrvCfg>,
        chan_rx: ChannelRx<SrvCfg>,
    ) -> Self::HandleAsync<'f> {
        RouterHandleAsync::new(self, method, location, headers, chan_tx, chan_rx)
    }
}

/// [`RpcRouter::handle_async`] 的 step 函数。
#[allow(clippy::too_many_arguments)]
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RouterHandle, pub, new(pub(crate)))]
async fn router_handle_async_<'f, TyTok>(
    router: &'f RpcRouter,
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    tx: ChannelTx<SrvCfg>,
    rx: ChannelRx<SrvCfg>,
    cancel: TyTok,
) -> Result<(), RouterError>
where
    TyTok: TrCancellationToken,
{
    if method != AccessMethod::Call {
        return reject_(Status::MethodNotAllowed, "只接受 Call", tx, rx, cancel).await;
    }

    let Option::Some(sender) = router.route_(location) else {
        return reject_(Status::NotFound, location, tx, rx, cancel).await;
    };

    let job = RpcJob {
        location_: location.to_owned(),
        headers_: headers.clone(),
        tx_: tx,
        rx_: rx,
    };
    sender.send(job).await.map_err(|_| RouterError::QueueClosed)
}

/// 回一条错误状态并按序关闭 channel。
async fn reject_<TyTok>(
    status: Status,
    message: &str,
    mut tx: ChannelTx<SrvCfg>,
    mut rx: ChannelRx<SrvCfg>,
    cancel: TyTok,
) -> Result<(), RouterError>
where
    TyTok: TrCancellationToken,
{
    let body = message.as_bytes();
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    let prefix = ResponsePrefix::with_headers(status, Option::Some(headers));
    write_response_prefix_async(&prefix, &mut tx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RouterError::Reject(err.to_string()))?;
    write_body_async_(&mut tx, body, cancel.child_token())
        .await
        .map_err(|err| RouterError::Reject(err.to_string()))?;
    close_channel_async(tx, &mut rx).await;
    Result::Ok(())
}

/// worker 侧的业务/IO 错误。
#[derive(Debug, Error)]
enum WorkerError {
    /// body 编解码失败。
    #[error("body 失败：{0}")]
    Body(String),

    /// 写回复失败。
    #[error("写回复失败：{0}")]
    Write(String),
}

impl From<BodyError> for WorkerError {
    fn from(value: BodyError) -> Self {
        WorkerError::Body(value.to_string())
    }
}

/// 从请求头声明的长度读出 body enum。
async fn read_call_<T>(headers: &Headers, rx: &mut ChannelRx<SrvCfg>) -> Result<T, WorkerError>
where
    T: TrDecodableOwned,
{
    let size = headers.body_size().unwrap_or(0usize);
    let bytes = read_body_async_(rx, size, NonCancellableToken::new()).await?;
    decode_slice_async_(&bytes).await.map_err(WorkerError::from)
}

/// 写成功回复。
async fn reply_ok_<T>(value: &T, tx: &mut ChannelTx<SrvCfg>) -> Result<(), WorkerError>
where
    T: TrEncodable + ?Sized,
{
    let body = encode_to_vec_async_(value).await?;
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    let prefix = ResponsePrefix::with_headers(Status::Ok, Option::Some(headers));
    write_response_prefix_async(&prefix, tx)
        .await
        .map_err(|err| WorkerError::Write(err.to_string()))?;
    write_body_async_(tx, &body, NonCancellableToken::new())
        .await
        .map_err(|err| WorkerError::Write(err.to_string()))?;
    Result::Ok(())
}

/// 写错误回复（纯文本 body）。
async fn reply_err_(
    status: Status,
    message: &str,
    tx: &mut ChannelTx<SrvCfg>,
) -> Result<(), WorkerError> {
    let body = message.as_bytes();
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    let prefix = ResponsePrefix::with_headers(status, Option::Some(headers));
    write_response_prefix_async(&prefix, tx)
        .await
        .map_err(|err| WorkerError::Write(err.to_string()))?;
    write_body_async_(tx, body, NonCancellableToken::new())
        .await
        .map_err(|err| WorkerError::Write(err.to_string()))?;
    Result::Ok(())
}

/// 把业务结果写成回复。
async fn finish_<T>(
    result: Result<T, ServiceError>,
    tx: &mut ChannelTx<SrvCfg>,
) -> Result<(), WorkerError>
where
    T: TrEncodable,
{
    match result {
        Result::Ok(value) => reply_ok_(&value, tx).await,
        Result::Err(err) => reply_err_(err.status_(), &err.to_string(), tx).await,
    }
}

/// 分派 workspace 方法。
async fn dispatch_workspace_<S>(
    service: &S,
    headers: &Headers,
    tx: &mut ChannelTx<SrvCfg>,
    rx: &mut ChannelRx<SrvCfg>,
) -> Result<(), WorkerError>
where
    S: WorkspaceService,
{
    let call: WorkspaceCall = match read_call_(headers, rx).await {
        Result::Ok(call) => call,
        Result::Err(err) => {
            reply_err_(Status::BadRequest, &err.to_string(), tx).await?;
            return Result::Ok(());
        }
    };

    match call {
        WorkspaceCall::create { name } => {
            finish_(
                service
                    .create_workspace(name)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        WorkspaceCall::list => {
            finish_(
                service
                    .list_workspaces()
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        WorkspaceCall::get { id } => {
            finish_(
                service
                    .get_workspace(id)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        WorkspaceCall::rename { id, name } => {
            finish_(
                service
                    .rename_workspace(id, name)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        WorkspaceCall::delete { id } => {
            finish_(
                service
                    .delete_workspace(id)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
    }
}

/// 分派 session 方法。
async fn dispatch_session_<S>(
    service: &S,
    headers: &Headers,
    tx: &mut ChannelTx<SrvCfg>,
    rx: &mut ChannelRx<SrvCfg>,
) -> Result<(), WorkerError>
where
    S: SessionService,
{
    let call: SessionCall = match read_call_(headers, rx).await {
        Result::Ok(call) => call,
        Result::Err(err) => {
            reply_err_(Status::BadRequest, &err.to_string(), tx).await?;
            return Result::Ok(());
        }
    };

    match call {
        SessionCall::create {
            workspace_id,
            title,
        } => {
            finish_(
                service
                    .create_session(workspace_id, title)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        SessionCall::list { workspace_id } => {
            finish_(
                service
                    .list_sessions(workspace_id)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        SessionCall::get { id } => {
            finish_(
                service
                    .get_session(id)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        SessionCall::rename { id, title } => {
            finish_(
                service
                    .rename_session(id, title)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
        SessionCall::delete { id } => {
            finish_(
                service
                    .delete_session(id)
                    .may_cancel_with(NonCancellableToken::new())
                    .await,
                tx,
            )
            .await
        }
    }
}

/// 处理一个 workspace job，并负责收尾。
async fn serve_workspace_job_<S>(service: &S, job: RpcJob)
where
    S: WorkspaceService,
{
    let RpcJob {
        location_,
        headers_,
        mut tx_,
        mut rx_,
    } = job;
    if let Result::Err(err) = dispatch_workspace_(service, &headers_, &mut tx_, &mut rx_).await {
        eprintln!("服务端：{location_} 处理失败：{err}");
    }
    close_channel_async(tx_, &mut rx_).await;
}

/// 处理一个 session job，并负责收尾。
async fn serve_session_job_<S>(service: &S, job: RpcJob)
where
    S: SessionService,
{
    let RpcJob {
        location_,
        headers_,
        mut tx_,
        mut rx_,
    } = job;
    if let Result::Err(err) = dispatch_session_(service, &headers_, &mut tx_, &mut rx_).await {
        eprintln!("服务端：{location_} 处理失败：{err}");
    }
    close_channel_async(tx_, &mut rx_).await;
}

/// 工作区 service 的 worker：持有具体实现、队列与路由 sender。
///
/// `new` 建立队列；`route()` 把 `SERVICE_PATH` 与 sender 交给 router；`spawn()` 把具体实现
/// 搬进独立线程。这样「用户实现的 service → worker → router 的 service path」是一条显式的
/// 装配链，而不是隐藏的映射。
struct WorkspaceWorker<S: WorkspaceService + 'static> {
    /// 用户实现。
    service_: S,

    /// 本 service 的队列。
    queue_: mpsc::Receiver<RpcJob>,

    /// 交给 router 的 sender。
    sender_: mpsc::Sender<RpcJob>,

    /// 完成计数。
    metrics_: Arc<AtomicUsize>,
}

impl<S: WorkspaceService + 'static> WorkspaceWorker<S> {
    /// 建立队列并持有用户实现。
    fn new(service: S, metrics: Arc<AtomicUsize>) -> Self {
        let (sender_, queue_) = mpsc::channel(K_QUEUE_CAP);
        WorkspaceWorker {
            service_: service,
            queue_,
            sender_,
            metrics_: metrics,
        }
    }

    /// 返回 `(service path, queue sender)`，供 router 注册。
    fn route(&self) -> (&'static str, mpsc::Sender<RpcJob>) {
        (S::SERVICE_PATH, self.sender_.clone())
    }

    /// 在独立线程上驱动 worker。
    fn spawn(self) {
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Result::Ok(rt) => rt,
                Result::Err(err) => {
                    eprintln!("建 workspace worker 运行时失败：{err}");
                    return;
                }
            };
            rt.block_on(async move {
                let WorkspaceWorker {
                    service_,
                    mut queue_,
                    sender_: _,
                    metrics_,
                } = self;
                while let Option::Some(job) = queue_.recv().await {
                    serve_workspace_job_(&service_, job).await;
                    metrics_.fetch_add(1usize, Ordering::SeqCst);
                }
            });
        });
    }
}

/// 会话 service 的 worker。
struct SessionWorker<S: SessionService + 'static> {
    /// 用户实现。
    service_: S,

    /// 本 service 的队列。
    queue_: mpsc::Receiver<RpcJob>,

    /// 交给 router 的 sender。
    sender_: mpsc::Sender<RpcJob>,

    /// 完成计数。
    metrics_: Arc<AtomicUsize>,
}

impl<S: SessionService + 'static> SessionWorker<S> {
    /// 建立队列并持有用户实现。
    fn new(service: S, metrics: Arc<AtomicUsize>) -> Self {
        let (sender_, queue_) = mpsc::channel(K_QUEUE_CAP);
        SessionWorker {
            service_: service,
            queue_,
            sender_,
            metrics_: metrics,
        }
    }

    /// 返回 `(service path, queue sender)`，供 router 注册。
    fn route(&self) -> (&'static str, mpsc::Sender<RpcJob>) {
        (S::SERVICE_PATH, self.sender_.clone())
    }

    /// 在独立线程上驱动 worker。
    fn spawn(self) {
        std::thread::spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Result::Ok(rt) => rt,
                Result::Err(err) => {
                    eprintln!("建 session worker 运行时失败：{err}");
                    return;
                }
            };
            rt.block_on(async move {
                let SessionWorker {
                    service_,
                    mut queue_,
                    sender_: _,
                    metrics_,
                } = self;
                while let Option::Some(job) = queue_.recv().await {
                    serve_session_job_(&service_, job).await;
                    metrics_.fetch_add(1usize, Ordering::SeqCst);
                }
            });
        });
    }
}

/// 等服务端处理满 `target` 条。
async fn wait_for_count_(metrics: Arc<AtomicUsize>, target: usize) {
    while metrics.load(Ordering::SeqCst) < target {
        tokio::time::sleep(Duration::from_millis(20u64)).await;
    }
}

/// 服务端：注册两个 interface、三条 service 队列，并等待客户端完成脚本。
pub async fn serve_(conn: Conn) -> Result<()> {
    let shared = Shared::new(conn, CoreAlloc);
    let mut binding = shared
        .bind_async(Dock::new(K_DOCK))
        .await
        .map_err(|err| anyhow!("绑定 dock {K_DOCK} 失败：{err}"))?;

    let store = Arc::new(Mutex::new(Store::new_()));
    let metrics = Arc::new(AtomicUsize::new(0usize));

    // 用户实现 → worker → route：worker 的 `route()` 把 service trait 的
    // `SERVICE_PATH` 与队列 sender 显式交给 router。
    let workspace_worker =
        WorkspaceWorker::new(WorkspaceServiceImpl::new_(store.clone()), metrics.clone());
    let session_worker =
        SessionWorker::new(SessionServiceImpl::new_(store.clone()), metrics.clone());
    let workspace_v2_worker =
        WorkspaceWorker::new(WorkspaceServiceImpl::new_(store), metrics.clone());

    let mut router = RpcRouter::new();
    router.add_interface(
        ApiV1::PATH,
        vec![workspace_worker.route(), session_worker.route()],
    );
    router.add_interface(ApiV2::PATH, vec![workspace_v2_worker.route()]);

    workspace_worker.spawn();
    session_worker.spawn();
    workspace_v2_worker.spawn();

    let mut mptp_router = Router::new();
    // 顶层只有 RpcRouter 一个 handler；空前缀匹配所有路径，两级分派在 RpcRouter 内部完成。
    mptp_router.add_target("", router);
    let server = MptpServer::<SrvCfg, Router<RpcRouter>>::new(mptp_router);

    let listener = server
        .serve_listener_async(&mut binding, K_LISTEN_RESERVE)
        .may_cancel_with(NonCancellableToken::new());
    tokio::pin!(listener);

    tokio::select! {
        result = &mut listener => {
            if let Result::Err(err) = result {
                println!("服务端：监听循环结束（{err}）");
            }
        }
        () = wait_for_count_(metrics.clone(), K_EXPECTED_JOBS) => {
            tokio::time::sleep(K_DRAIN_PAUSE).await;
        }
    }

    println!(
        "服务端：本轮处理 {} 条请求，退出。",
        metrics.load(Ordering::SeqCst)
    );
    Result::Ok(())
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 store 的 workspace/session 生命周期。
    /// Test the store's workspace/session lifecycle.
    ///
    /// - 手段：创建 workspace、在它下面创建 session、列出、改名、删除。
    /// - 判断：每一步的返回值与列表内容符合预期；有 session 时删除 workspace 会失败。
    ///
    /// - Means: create a workspace, add a session, list, rename and delete.
    /// - Verdict: each step matches expectations, and deleting a non-empty workspace fails.
    #[test]
    fn store_crud_lifecycle_() {
        let mut store = Store::new_();
        let workspace = store
            .create_workspace_("alpha".into())
            .expect("创建应当成功");
        assert_eq!(store.list_workspaces_().len(), 1usize);

        let session = store
            .create_session_(workspace.id, "first".into())
            .expect("创建 session 应当成功");
        assert_eq!(store.list_sessions_(workspace.id).len(), 1usize);

        let renamed = store
            .rename_session_(session.id, "renamed".into())
            .expect("改名应当成功");
        assert_eq!(renamed.title, "renamed");

        assert!(
            store.delete_workspace_(workspace.id).is_err(),
            "有 session 时不应允许删除 workspace"
        );
        assert!(
            store
                .delete_session_(session.id)
                .expect("删除 session 应当成功")
        );
        assert!(
            store
                .delete_workspace_(workspace.id)
                .expect("删除 workspace 应当成功")
        );
        assert!(store.list_workspaces_().is_empty());
    }
}
