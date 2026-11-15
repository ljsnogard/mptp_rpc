//! 场景的业务客户端：join / View / Head / Pull / update / 显式退订。
//!
//! 它只做业务装配，传输全部走 `MptpClient` 的公开入口。`Pull` 的后台读取任务把
//! denco 事件送进一个调用方提供的队列；心跳在客户端侧被忽略。
//!
//! # `client_`: the scene business client
//!
//! Only business assembly; transport stays inside `MptpClient`. A background task decodes
//! each denco event and forwards it to the caller's queue, dropping idle heartbeats.

use std::{cell::RefCell, net::SocketAddr, rc::Rc, time::Duration};

use abs_art_facade::TrLocalScope;
use abs_cancel::TrCancellationToken;
use abs_mptp::spec::Status;
use anyhow::{Result, bail};
use flume::Sender;
use mptp_smux::client::MptpClient;

use crate::scene::{
    ClientCfg, PlanetDescriptor, PlanetDirectory, PlanetEvent, PlanetParams,
    cancel_::{SceneCancelToken, SceneCanceller, cancel_pair_},
    cfg_::SharedConn,
    conn_::{dial_async_, share_conn_},
    protocol_::{
        JoinRequest, K_SCENE_JOIN, K_SCENE_PLANETS, K_SCENE_UNSUBSCRIBE, K_SCENE_UPDATE,
        UnsubscribeAck, UnsubscribeRequest, UpdateAck, UpdateRequest, decode_value_async_,
        encode_to_vec_async_,
    },
};

/// 本地投递任务的句柄类型（固定 compio，不随门面裸名变化）。
pub type PullHandle = crate::scene::cfg_::SceneJoinHandle<()>;

/// 场景客户端。
pub struct SceneClient {
    /// MPTP 客户端。
    client_: Rc<MptpClient<ClientCfg>>,

    /// 共享连接（保活）。
    shared_: SharedConn,

    /// 保活任务的取消端。
    keepalive_cancel_: SceneCanceller,

    /// 保活任务句柄。
    keepalive_task_: RefCell<Option<PullHandle>>,
}

impl Drop for SceneClient {
    fn drop(&mut self) {
        // 停掉保活任务；它持有一份 `Rc<MptpClient>`，必须让它释放连接。
        self.keepalive_cancel_.cancel_();
        let _ = self.keepalive_task_.take();
    }
}

/// 客户端保活周期：比服务端 3 秒空闲判死小得多。
const K_KEEPALIVE: Duration = Duration::from_millis(500u64);

impl SceneClient {
    /// 拨号并建出客户端。
    ///
    /// # Errors
    ///
    /// 连接或握手失败时返回错误。
    pub async fn connect_async_(addr: SocketAddr) -> Result<Self> {
        let conn = dial_async_(addr).await?;
        let shared = share_conn_(conn);
        let client = Rc::new(MptpClient::<ClientCfg>::new(shared.clone()));
        let (keepalive_cancel_, keepalive_token) = cancel_pair_();
        let keepalive = start_keepalive_(client.clone(), keepalive_token);
        Ok(SceneClient {
            client_: client,
            shared_: shared,
            keepalive_cancel_,
            keepalive_task_: RefCell::new(Some(keepalive)),
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
        let mut resp = self
            .client_
            .invoke(K_SCENE_JOIN)
            .body_bytes(&body)
            .send_async()
            .await?;
        ensure_ok_(&resp, K_SCENE_JOIN)?;
        let mut view = resp.body_async().await?;
        Ok(decode_value_async_(&mut view).await?)
    }

    /// 列出目录。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn view_async_(&self) -> Result<PlanetDirectory> {
        let mut resp = self.client_.view(K_SCENE_PLANETS).send_async().await?;
        ensure_ok_(&resp, K_SCENE_PLANETS)?;
        let mut view = resp.body_async().await?;
        Ok(decode_value_async_(&mut view).await?)
    }

    /// 用完整参数更新自己的行星。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn update_async_(&self, params: PlanetParams) -> Result<UpdateAck> {
        let request = UpdateRequest { params };
        let body = encode_to_vec_async_(&request).await?;
        let mut resp = self
            .client_
            .invoke(K_SCENE_UPDATE)
            .body_bytes(&body)
            .send_async()
            .await?;
        ensure_ok_(&resp, K_SCENE_UPDATE)?;
        let mut view = resp.body_async().await?;
        Ok(decode_value_async_(&mut view).await?)
    }

    /// 显式退订提示（丢弃 Pull 前的业务清理消息）。
    ///
    /// # Errors
    ///
    /// 请求失败、状态非 200 或回复体解码失败时返回错误。
    pub async fn unsubscribe_async_(&self, planet_id: u64) -> Result<UnsubscribeAck> {
        let request = UnsubscribeRequest { planet_id };
        let body = encode_to_vec_async_(&request).await?;
        let mut resp = self
            .client_
            .invoke(K_SCENE_UNSUBSCRIBE)
            .body_bytes(&body)
            .send_async()
            .await?;
        ensure_ok_(&resp, K_SCENE_UNSUBSCRIBE)?;
        let mut view = resp.body_async().await?;
        Ok(decode_value_async_(&mut view).await?)
    }

    /// 订阅一个行星流；事件被送进 `events`，心跳被忽略。
    ///
    /// 返回的任务句柄 **drop 即取消本地读取**。要同时清理服务端订阅时，先调用
    /// [`SceneClient::unsubscribe_async_`]，再 drop 句柄。
    ///
    /// # Errors
    ///
    /// 请求失败或状态非 200 时返回错误。
    pub async fn pull_async_(&self, uri: &str, events: Sender<PlanetEvent>) -> Result<PullHandle> {
        let mut resp = self.client_.pull(uri).send_async().await?;
        ensure_ok_(&resp, uri)?;
        let scope = crate::scene::cfg_::compio_rt_().local_scope();
        let handle = scope.spawn_local(async move {
            let Ok(mut suffix) = resp.suffix_async().await else {
                return;
            };
            loop {
                match decode_value_async_::<PlanetEvent, _>(&mut suffix).await {
                    Ok(PlanetEvent::Heartbeat { .. }) => continue,
                    Ok(event) => {
                        if events.send_async(event).await.is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(handle)
    }

    /// 底层 `MptpClient`（网络线程做 Pull 时使用）。
    pub fn client_(&self) -> &Rc<MptpClient<ClientCfg>> {
        &self.client_
    }

    /// 共享连接（保活）。
    pub fn shared_(&self) -> &SharedConn {
        &self.shared_
    }
}

/// 启动保活任务：每 500ms 发一次 `Head /scene/planets`。
///
/// 服务端用它更新「最近请求时刻」，从而在客户端进程消失后 3 秒内认定连接已死并广播
/// `Removed`。`Head` 同时是一次轻量的目录探活，不建立订阅。
fn start_keepalive_(client: Rc<MptpClient<ClientCfg>>, token: SceneCancelToken) -> PullHandle {
    let scope = crate::scene::cfg_::compio_rt_().local_scope();
    scope.spawn_local(async move {
        loop {
            // 取消信号先到：退出，释放 `Rc<MptpClient>`。
            if compio::time::timeout(K_KEEPALIVE, token.clone().cancellation())
                .await
                .is_ok()
            {
                break;
            }
            let _ = client.head(K_SCENE_PLANETS).send_async().await;
        }
    })
}

/// 检查回复状态为 200。
fn ensure_ok_(resp: &mptp_smux::client::Response<ClientCfg>, what: &str) -> Result<()> {
    if resp.status() != Status::Ok {
        bail!("{what} 返回 {}", resp.status().inner());
    }
    Ok(())
}
