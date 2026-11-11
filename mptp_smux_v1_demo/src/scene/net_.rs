//! Bevy 与 compio 网络线程之间的桥：只传普通数据。
//!
//! Bevy 主线程从不接触 MPTP / smux / compio 的 `!Send` 类型；它只通过
//! [`NetCommand`] 发送意图、通过 [`NetEvent`] 接收业务事件。网络线程独占
//! `SceneClient`、所有 `Pull` 任务与 compio 运行时。
//!
//! 默认策略：join 后自动订阅目录里的全部行星；UI 之后可以取消 / 重新订阅。
//!
//! # `net_`: the Bevy ↔ compio bridge
//!
//! Only plain data crosses the boundary. The network thread owns `SceneClient`, every
//! pull task and the compio runtime; the Bevy thread only sends commands and drains events.

use std::{
    net::SocketAddr,
    thread,
};

use abs_art_facade::TrLocalScope;
use abs_mptp::spec::Status;
use anyhow::{Result, anyhow};
use flume::{Receiver, Sender};
use mptp_smux::client::MptpClient;
use crate::scene::{
    ClientCfg,
    PlanetDescriptor, PlanetDirectory, PlanetEvent, PlanetId, SceneClient,
    protocol_::decode_value_async_,
};

/// Bevy → 网络线程的命令。
#[derive(Clone, Debug)]
pub enum NetCommand {
    /// 订阅一颗行星的数据流。
    Subscribe {
        /// 行星标识。
        planet_id: PlanetId,

        /// 数据流 URI。
        uri: String,
    },

    /// 退订一颗行星。
    Unsubscribe {
        /// 行星标识。
        planet_id: PlanetId,
    },

    /// 修改自己的公转 / 自转速度。
    UpdateSelf {
        /// 公转速度。
        orbit_speed: f32,

        /// 自转速度。
        spin_speed: f32,
    },

    /// 主动刷新目录。
    RefreshDirectory,

    /// 关闭网络线程。
    Shutdown,
}

/// 网络线程 → Bevy 的事件。
#[derive(Clone, Debug)]
pub enum NetEvent {
    /// 自己的行星已经创建。
    Joined {
        /// 自己的行星描述。
        descriptor: PlanetDescriptor,
    },

    /// 目录刷新。
    Directory {
        /// 当前目录。
        directory: PlanetDirectory,
    },

    /// 某条订阅上的行星事件（心跳已在网络线程过滤）。
    PlanetEvent {
        /// 行星标识。
        planet_id: PlanetId,

        /// 事件。
        event: PlanetEvent,
    },

    /// 一条订阅结束。
    SubscriptionClosed {
        /// 行星标识。
        planet_id: PlanetId,

        /// 结束原因。
        reason: String,
    },

    /// 网络层错误。
    Error(String),

    /// 网络线程已退出。
    Disconnected,
}

/// Bevy 侧持有的双向通道。
pub struct NetHandle {
    /// 命令发送端。
    pub cmd_tx_: Sender<NetCommand>,

    /// 事件接收端。
    pub event_rx_: Receiver<NetEvent>,
}

impl Drop for NetHandle {
    fn drop(&mut self) {
        // 应用退出时让网络线程收尾；失败只表示线程已经不在。
        let _ = self.cmd_tx_.try_send(NetCommand::Shutdown);
    }
}

/// 启动网络线程。线程内建一个 compio 单线程运行时。
///
/// # Errors
///
/// 线程创建失败时返回错误（连接失败通过 [`NetEvent::Error`] 上报）。
pub fn spawn_net_thread_(
    addr: SocketAddr,
    name: String,
    color: u32,
) -> Result<NetHandle> {
    let (cmd_tx, cmd_rx) = flume::bounded::<NetCommand>(64usize);
    let (event_tx, event_rx) = flume::bounded::<NetEvent>(256usize);

    thread::Builder::new()
        .name(String::from("scene-net"))
        .spawn(move || {
            let runtime = match compio::runtime::Runtime::new() {
                Result::Ok(runtime) => runtime,
                Result::Err(err) => {
                    let _ = event_tx.send(NetEvent::Error(format!("建 compio 运行时失败：{err}")));
                    let _ = event_tx.send(NetEvent::Disconnected);
                    return;
                }
            };
            runtime.block_on(async move {
                let scope = crate::scene::cfg_::compio_rt_().local_scope();
                scope
                    .run_until(net_main_(addr, name, color, cmd_rx, event_tx))
                    .await;
            });
        })
        .map_err(|err| anyhow!("创建网络线程失败：{err}"))?;

    Ok(NetHandle {
        cmd_tx_: cmd_tx,
        event_rx_: event_rx,
    })
}

/// 网络线程主循环。
async fn net_main_(
    addr: SocketAddr,
    name: String,
    color: u32,
    cmd_rx: Receiver<NetCommand>,
    event_tx: Sender<NetEvent>,
) {
    let client = match SceneClient::connect_async_(addr).await {
        Result::Ok(client) => client,
        Result::Err(err) => {
            let _ = event_tx
                .send_async(NetEvent::Error(format!("连接服务端失败：{err:#}")))
                .await;
            let _ = event_tx.send_async(NetEvent::Disconnected).await;
            return;
        }
    };

    let descriptor = match client.join_async_(&name, color).await {
        Result::Ok(descriptor) => descriptor,
        Result::Err(err) => {
            let _ = event_tx
                .send_async(NetEvent::Error(format!("join 失败：{err:#}")))
                .await;
            let _ = event_tx.send_async(NetEvent::Disconnected).await;
            return;
        }
    };
    if event_tx
        .send_async(NetEvent::Joined {
            descriptor: descriptor.clone(),
        })
        .await
        .is_err()
    {
        return;
    }

    if let Result::Ok(directory) = client.view_async_().await {
        let _ = event_tx
            .send_async(NetEvent::Directory { directory })
            .await;
    }

    let mut pulls: std::collections::BTreeMap<PlanetId, PullTask_> = std::collections::BTreeMap::new();

    loop {
        let command = match compio::time::timeout(
            std::time::Duration::from_secs(1u64),
            cmd_rx.recv_async(),
        )
        .await
        {
            Ok(Ok(command)) => Some(command),
            Ok(Err(_)) => break,
            // 每秒刷新一次目录；它同时是连接级保活请求。
            Err(_) => None,
        };

        match command {
            Some(NetCommand::Shutdown) => break,
            Some(NetCommand::RefreshDirectory) | None => {
                if let Result::Ok(directory) = client.view_async_().await
                    && event_tx
                        .send_async(NetEvent::Directory { directory })
                        .await
                        .is_err()
                {
                    break;
                }
            }
            Some(NetCommand::UpdateSelf {
                orbit_speed,
                spin_speed,
            }) => {
                if let Result::Err(err) = client.update_async_(orbit_speed, spin_speed).await {
                    let _ = event_tx
                        .send_async(NetEvent::Error(format!("update 失败：{err:#}")))
                        .await;
                }
            }
            Some(NetCommand::Subscribe { planet_id, uri }) => {
                if pulls.contains_key(&planet_id) {
                    continue;
                }
                match spawn_pull_(client.client_().clone(), uri, planet_id, event_tx.clone()).await {
                    Result::Ok(task) => {
                        pulls.insert(planet_id, task);
                    }
                    Result::Err(err) => {
                        let _ = event_tx
                            .send_async(NetEvent::Error(format!("订阅 {planet_id} 失败：{err:#}")))
                            .await;
                    }
                }
            }
            Some(NetCommand::Unsubscribe { planet_id }) => {
                if let Some(task) = pulls.remove(&planet_id) {
                    if let Result::Err(err) = client.unsubscribe_async_(planet_id).await {
                        let _ = event_tx
                            .send_async(NetEvent::Error(format!("退订 {planet_id} 失败：{err:#}")))
                            .await;
                    }
                    drop(task);
                }
            }
        }
    }

    drop(pulls);
    let _ = event_tx.send_async(NetEvent::Disconnected).await;
    // `client` 在这里 drop：保活任务被取消，连接关闭。
    drop(client);
}

/// 一条 Pull 任务：句柄 + 行星标识。
struct PullTask_ {
    /// 后台任务句柄；**字段被 drop 即取消任务**，因此只需要持有它。
    #[allow(dead_code)]
    handle_: crate::scene::cfg_::SceneJoinHandle<()>,
}

/// 发起一条 Pull，把事件转发为 [`NetEvent::PlanetEvent`]。
async fn spawn_pull_(
    client: std::rc::Rc<MptpClient<ClientCfg>>,
    uri: String,
    planet_id: PlanetId,
    event_tx: Sender<NetEvent>,
) -> Result<PullTask_> {
    let mut response = client.pull(&uri).send_async().await?;
    if response.status() != Status::Ok {
        return Err(anyhow!("Pull {uri} 返回 {}", response.status().inner()));
    }
    let scope = crate::scene::cfg_::compio_rt_().local_scope();
    let handle = scope.spawn_local(async move {
        let Ok(mut suffix) = response.suffix_async().await else {
            return;
        };
        loop {
            match decode_value_async_::<PlanetEvent, _>(&mut suffix).await {
                Ok(PlanetEvent::Heartbeat { .. }) => continue,
                Ok(event) => {
                    if event_tx
                        .send_async(NetEvent::PlanetEvent { planet_id, event })
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Err(err) => {
                    let _ = event_tx
                        .send_async(NetEvent::SubscriptionClosed {
                            planet_id,
                            reason: err.to_string(),
                        })
                        .await;
                    break;
                }
            }
        }
    });
    Ok(PullTask_ { handle_: handle })
}

