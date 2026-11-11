//! `scene-smoke`：headless 双客户端端到端验收。
//!
//! 它在**同一个 compio 运行时**里起一个真实服务器和两个真实客户端，验证：
//! join、View 目录、选择性订阅、退订、重新订阅拿新快照、更新广播、连接断开后的
//! `Removed` 清理。
//!
//! # `scene-smoke`: headless two-client end-to-end check
//!
//! A real server plus two real clients on one compio runtime. It checks join, directory,
//! selective subscription, unsubscribe, resubscribe snapshot, update broadcast and
//! `Removed` cleanup after the owner disconnects.

use std::{net::SocketAddr, process::ExitCode, time::Duration};

use abs_art_facade::{TrJoinHandle, TrLocalScope};
use anyhow::{Result, anyhow, bail};
use compio::net::TcpListener;
use flume::Receiver;
use mptp_smux_v1_demo::scene::{
    PlanetDescriptor, PlanetEvent, SceneClient,
    protocol_::planet_uri_,
    server_::serve_on_listener_async_,
};

/// 单步网络等待上限。
const K_TIMEOUT: Duration = Duration::from_secs(10u64);

/// 断言「没有事件」的观察窗口。
const K_QUIET: Duration = Duration::from_millis(300u64);

fn main() -> ExitCode {
    let runtime = match compio::runtime::Runtime::new() {
        Result::Ok(runtime) => runtime,
        Result::Err(err) => {
            eprintln!("建 compio 运行时失败：{err}");
            return ExitCode::FAILURE;
        }
    };
    let outcome = runtime.block_on(async {
        let scope = mptp_smux_v1_demo::scene::cfg_::compio_rt_().local_scope();
        scope.run_until(smoke_async_()).await
    });
    match outcome {
        Result::Ok(()) => {
            println!("scene-smoke：全部通过");
            ExitCode::SUCCESS
        }
        Result::Err(err) => {
            eprintln!("scene-smoke 失败：{err:#}");
            ExitCode::FAILURE
        }
    }
}

/// 启动进程内服务器并在本地地址上跑完双客户端场景。
async fn smoke_async_() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|err| anyhow!("绑定 smoke 服务器失败：{err}"))?;
    let addr: SocketAddr = listener
        .local_addr()
        .map_err(|err| anyhow!("取 smoke 服务器地址失败：{err}"))?;
    {
        let scope = mptp_smux_v1_demo::scene::cfg_::compio_rt_().local_scope();
        let task = scope.spawn_local(async move {
            if let Err(err) = serve_on_listener_async_(listener).await {
                eprintln!("smoke 服务器退出：{err:#}");
            }
        });
        task.detach();
    }

    let a = SceneClient::connect_async_(addr).await?;
    let a_planet = a.join_async_("A", 0x00AA_0055u32).await?;
    let b = SceneClient::connect_async_(addr).await?;
    let b_planet = b.join_async_("B", 0xAA_55_00u32).await?;
    println!("smoke：A/B 已 join（{} / {}）", a_planet.id, b_planet.id);

    // 目录：两条行星都在，且 URI 可发现。
    let directory = a.view_async_().await?;
    require(
        directory.planets.len() == 2usize,
        format!("目录应有 2 颗行星，实际 {}", directory.planets.len()),
    )?;
    require(
        directory
            .planets
            .iter()
            .any(|planet| planet.stream_uri == b_planet.stream_uri),
        "目录里应当有 B 的 stream_uri",
    )?;

    // 选择性订阅 B。
    let (events_tx, events_rx) = flume::bounded(64usize);
    let pull_b = a.pull_async_(&b_planet.stream_uri, events_tx).await?;
    let snapshot = expect_snapshot_(&events_rx, b_planet.id).await?;
    require(snapshot.version >= 1u64, "快照版本应当有效")?;
    println!("smoke：收到 B 快照 v{}", snapshot.version);

    // 更新广播。
    let ack = b.update_async_(1.1f32, 1.2f32).await?;
    let updated = expect_updated_(&events_rx, b_planet.id).await?;
    require(
        updated.version == ack.version,
        "更新事件版本应当等于 ack 版本",
    )?;
    require(updated.orbit_speed == 1.1f32, "公转速度应当同步")?;

    // 退订：显式清理提示 + 丢弃 Pull。
    let removed = a.unsubscribe_async_(b_planet.id).await?;
    println!("smoke：显式退订移除 {}", removed.removed);
    require(removed.removed == 1u32, "应当移除 1 条订阅")?;
    drop(pull_b);
    compio::time::sleep(Duration::from_millis(100u64)).await;

    // 退订后 B 的更新不再到达。
    b.update_async_(2.1f32, 2.2f32).await?;
    match compio::time::timeout(K_QUIET, events_rx.recv_async()).await {
        Err(_) => {}
        Ok(Ok(event)) => bail!("退订后仍收到事件：{event:?}"),
        Ok(Err(_)) => {}
    }

    // 重新订阅：先收到最新 Snapshot。
    let (events_tx, events_rx) = flume::bounded(64usize);
    let _pull_b = a.pull_async_(&b_planet.stream_uri, events_tx).await?;
    let fresh = expect_snapshot_(&events_rx, b_planet.id).await?;
    println!("smoke：重新订阅快照 v{}", fresh.version);
    require(fresh.version == 3u64, "重新订阅应拿到最新版本 3")?;
    require(fresh.orbit_speed == 2.1f32, "重新订阅应拿到最新速度")?;

    // B 断开：A 的订阅收到 Removed，服务端删除行星。
    println!("smoke：丢弃 B 的连接，等待 Removed");
    drop(b);
    let event = timeout_event_(&events_rx).await?;
    match event {
        PlanetEvent::Removed { planet_id, .. } => {
            require(planet_id == b_planet.id, "Removed 应当是 B")?;
        }
        other => bail!("B 断开后应收到 Removed，实际是 {other:?}"),
    }

    // 目录最终只剩 A。
    let deadline = std::time::Instant::now() + K_TIMEOUT;
    loop {
        let directory = a.view_async_().await?;
        if directory.planets.len() == 1usize
            && directory.planets[0].id == a_planet.id
        {
            break;
        }
        if std::time::Instant::now() > deadline {
            bail!("B 断开后目录没有及时收敛到只剩 A");
        }
        compio::time::sleep(Duration::from_millis(20u64)).await;
    }

    require(
        a_planet.stream_uri == planet_uri_(a_planet.id),
        "A 的 URI 应当符合约定",
    )?;
    Ok(())
}

/// 简单断言。
fn require(condition: bool, message: impl Into<String>) -> Result<()> {
    if condition {
        Ok(())
    } else {
        Err(anyhow!(message.into()))
    }
}

/// 等待并取出一个事件。
async fn timeout_event_(events: &Receiver<PlanetEvent>) -> Result<PlanetEvent> {
    compio::time::timeout(K_TIMEOUT, events.recv_async())
        .await
        .map_err(|_| anyhow!("等待行星事件超时"))?
        .map_err(|err| anyhow!("等待行星事件失败：{err}"))
}

/// 等待第一条事件并断言它是快照。
async fn expect_snapshot_(
    events: &Receiver<PlanetEvent>,
    planet_id: u64,
) -> Result<PlanetDescriptor> {
    match timeout_event_(events).await? {
        PlanetEvent::Snapshot(planet) if planet.id == planet_id => Ok(planet),
        other => bail!("期待 Snapshot({planet_id})，实际是 {other:?}"),
    }
}

/// 等待第一条事件并断言它是更新。
async fn expect_updated_(
    events: &Receiver<PlanetEvent>,
    planet_id: u64,
) -> Result<PlanetDescriptor> {
    match timeout_event_(events).await? {
        PlanetEvent::Updated(planet) if planet.id == planet_id => Ok(planet),
        other => bail!("期待 Updated({planet_id})，实际是 {other:?}"),
    }
}
