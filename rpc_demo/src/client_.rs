//! 客户端：手动实现的极简代理 + 通用 unary 调用。
//!
//! `ClientProxy` 运行期只持有 `&MptpClient`；interface 由零大小类型参数提供。它没有
//! `Client` 包装层，也没有 `base_path` 字段。
//!
//! 这里只有**一个**通用的 `ClientCallAsync` 步骤函数；将来 `#[rpc]` 宏会为每个 service 方法
//! 生成类似的具名 future 与方法体，本文件是那一层的手写等价物。

use core::marker::PhantomData;

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mm::CoreAlloc;
use abs_mptp::{
    client::{TrMptpClient, TrMptpSession},
    spec::{Status, StdHeaderKey, StdHeaderVal, access_method::AccessMethod},
};
use anyhow::{Result as AnyResult, ensure};
use gen_mcf2::gen_may_cancel_future;
use mm_ptr::Shared;
use mptp_smux::{
    client::MptpClient,
    messaging::{Headers, RequestPrefix},
    x_deps::gen_mcf2,
};
use rpc_core_v1::ClientConfig;

use crate::{
    body_::{decode_from_async_, encode_to_vec_async_, read_body_async_},
    conn_::Conn,
    protocol_::{
        ApiV1, ApiV2, RpcInterface, RpcResult, ServiceError, Session, SessionCall, SessionService,
        Workspace, WorkspaceCall, WorkspaceService,
    },
};

/// 客户端的完整配置类型。
type CliCfg = ClientConfig<Conn>;

/// 手动版本的客户端代理。
///
/// 运行期只有 `client_` 一个字段；`I` 是零大小的 interface marker。
pub struct ClientProxy<'a, I: RpcInterface + Send + Sync + 'static> {
    /// 底层 MPTP 客户端。
    client_: &'a MptpClient<CliCfg>,

    /// interface marker。
    _iface_: PhantomData<I>,
}

impl<'a, I: RpcInterface + Send + Sync + 'static> ClientProxy<'a, I> {
    /// 由底层客户端构造代理。
    pub const fn new(client: &'a MptpClient<CliCfg>) -> Self {
        ClientProxy {
            client_: client,
            _iface_: PhantomData,
        }
    }
}

/// 通用 unary 调用步骤：构造 body enum → 发请求 → 检查状态 → 解码回复。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ClientCall, pub(crate), new(pub(crate)))]
async fn client_call_<'a, 'f, I, Req, Rep, TyTok>(
    proxy: &'f ClientProxy<'a, I>,
    service: &'static str,
    request: Req,
    cancel: TyTok,
) -> RpcResult<Rep>
where
    'a: 'f,
    I: RpcInterface + Send + Sync + 'static,
    Req: TrEncodable + 'f,
    Rep: TrDecodableOwned + 'f,
    TyTok: TrCancellationToken,
{
    rpc_call(proxy.client_, I::PATH, service, &request, cancel).await
}

impl<'a, I: RpcInterface + Send + Sync + 'static> WorkspaceService for ClientProxy<'a, I> {
    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        ClientCallAsync::new(
            self,
            <Self as WorkspaceService>::SERVICE_PATH,
            WorkspaceCall::create { name },
        )
    }

    fn list_workspaces<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Workspace>>> {
        ClientCallAsync::new(
            self,
            <Self as WorkspaceService>::SERVICE_PATH,
            WorkspaceCall::list,
        )
    }

    fn get_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Workspace>>> {
        ClientCallAsync::new(
            self,
            <Self as WorkspaceService>::SERVICE_PATH,
            WorkspaceCall::get { id },
        )
    }

    fn rename_workspace<'f>(
        &'f self,
        id: u64,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        ClientCallAsync::new(
            self,
            <Self as WorkspaceService>::SERVICE_PATH,
            WorkspaceCall::rename { id, name },
        )
    }

    fn delete_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>> {
        ClientCallAsync::new(
            self,
            <Self as WorkspaceService>::SERVICE_PATH,
            WorkspaceCall::delete { id },
        )
    }
}

impl<'a, I: RpcInterface + Send + Sync + 'static> SessionService for ClientProxy<'a, I> {
    fn create_session<'f>(
        &'f self,
        workspace_id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>> {
        ClientCallAsync::new(
            self,
            <Self as SessionService>::SERVICE_PATH,
            SessionCall::create {
                workspace_id,
                title,
            },
        )
    }

    fn list_sessions<'f>(
        &'f self,
        workspace_id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Session>>> {
        ClientCallAsync::new(
            self,
            <Self as SessionService>::SERVICE_PATH,
            SessionCall::list { workspace_id },
        )
    }

    fn get_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Session>>> {
        ClientCallAsync::new(
            self,
            <Self as SessionService>::SERVICE_PATH,
            SessionCall::get { id },
        )
    }

    fn rename_session<'f>(
        &'f self,
        id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>> {
        ClientCallAsync::new(
            self,
            <Self as SessionService>::SERVICE_PATH,
            SessionCall::rename { id, title },
        )
    }

    fn delete_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>> {
        ClientCallAsync::new(
            self,
            <Self as SessionService>::SERVICE_PATH,
            SessionCall::delete { id },
        )
    }
}

/// 一次 unary 调用的实际收发。
async fn rpc_call<Req, Rep, TyTok>(
    client: &MptpClient<CliCfg>,
    interface: &str,
    service: &str,
    request: &Req,
    cancel: TyTok,
) -> RpcResult<Rep>
where
    Req: TrEncodable + ?Sized,
    Rep: TrDecodableOwned,
    TyTok: TrCancellationToken,
{
    let location = crate::protocol_::location(interface, service);
    let body = encode_to_vec_async_(request)
        .await
        .map_err(|err| ServiceError::Codec(err.to_string()))?;
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    let prefix =
        RequestPrefix::with_headers(AccessMethod::Call, location.clone(), Option::Some(headers));

    let mut session = client
        .request_async(prefix)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServiceError::Transport(format!("发起 {location} 失败：{err}")))?;

    let mut body_src: &[u8] = body.as_slice();
    session
        .send_req_body_async(&mut body_src)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServiceError::Transport(format!("发送 {location} 请求体失败：{err}")))?;

    let response = session
        .recv_resp_prefix_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServiceError::Transport(format!("接收 {location} 回复前缀失败：{err}")))?;

    let mut body_read = session
        .recv_resp_body_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServiceError::Transport(format!("取 {location} 回复体失败：{err}")))?;

    if response.status() != Status::Ok {
        let size = response
            .headers()
            .and_then(|headers| headers.body_size())
            .unwrap_or(0usize);
        let bytes = read_body_async_(&mut body_read, size, cancel.child_token())
            .await
            .map_err(|err| ServiceError::Transport(err.to_string()))?;
        return Err(ServiceError::Remote(format!(
            "RPC {location} 失败（{}）：{}",
            response.status().inner(),
            String::from_utf8_lossy(&bytes)
        )));
    }

    let value: Rep = decode_from_async_::<Rep, _, _>(&mut body_read, cancel.child_token())
        .await
        .map_err(|err| ServiceError::Codec(err.to_string()))?;
    Result::Ok(value)
}

/// 跑完客户端这一轮的 CRUD 脚本。
pub async fn run_(conn: Shared<Conn, CoreAlloc>) -> AnyResult<()> {
    let client = MptpClient::<CliCfg>::new(conn);
    let v1 = ClientProxy::<ApiV1>::new(&client);

    println!("客户端：创建 workspace");
    let workspace = v1
        .create_workspace("alpha".to_string())
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    println!("  -> {workspace:?}");

    println!("客户端：列出 workspace");
    let workspaces = v1
        .list_workspaces()
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(workspaces.len() == 1usize, "应当只有一个 workspace");
    println!("  -> {workspaces:?}");

    println!("客户端：重命名 workspace");
    let renamed = v1
        .rename_workspace(workspace.id, "alpha-renamed".to_string())
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(renamed.name == "alpha-renamed", "workspace 改名未生效");
    println!("  -> {renamed:?}");

    println!("客户端：查看 workspace");
    let fetched_workspace = v1
        .get_workspace(workspace.id)
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(fetched_workspace.is_some(), "workspace 应当存在");
    println!("  -> {fetched_workspace:?}");

    println!("客户端：创建 session");
    let session = v1
        .create_session(workspace.id, "first-session".to_string())
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    println!("  -> {session:?}");

    println!("客户端：列出 session");
    let sessions = v1
        .list_sessions(workspace.id)
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(sessions.len() == 1usize, "应当只有一个 session");
    println!("  -> {sessions:?}");

    println!("客户端：重命名 session");
    let renamed_session = v1
        .rename_session(session.id, "first-renamed".to_string())
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(
        renamed_session.title == "first-renamed",
        "session 改名未生效"
    );
    println!("  -> {renamed_session:?}");

    println!("客户端：查看 session");
    let fetched = v1
        .get_session(session.id)
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(fetched.is_some(), "session 应当存在");
    println!("  -> {fetched:?}");

    println!("客户端：删除 session");
    ensure!(
        v1.delete_session(session.id)
            .may_cancel_with(NonCancellableToken::new())
            .await?,
        "session 应当被删除"
    );
    println!("  -> 已删除");

    println!("客户端：删除 workspace");
    ensure!(
        v1.delete_workspace(workspace.id)
            .may_cancel_with(NonCancellableToken::new())
            .await?,
        "workspace 应当被删除"
    );
    println!("  -> 已删除");

    println!("客户端：通过 ApiV2 再列一次 workspace（验证多 interface 路由）");
    let v2 = ClientProxy::<ApiV2>::new(&client);
    let after = v2
        .list_workspaces()
        .may_cancel_with(NonCancellableToken::new())
        .await?;
    ensure!(after.is_empty(), "删除之后不应再有 workspace");
    println!("  -> {after:?}");

    Ok(())
}
