//! `scene-server` 的装配：单 compio 线程上接受多条 TCP 连接，每条连接一个
//! `MptpServlet` + `SceneHandler`。
//!
//! 线程模型：一个 compio 运行时、一条业务线程。所有连接、所有 session、所有行星订阅
//! 都在这一条线程上；服务端**不渲染、不算显示位置**，只维护行星参数并通过后缀流广播。
//!
//! 连接断开时：取消该连接的所有 session、删除自己的行星、向订阅者广播 `Removed`、
//! 递增目录版本。
//!
//! # `server_`: the single-thread compio scene server
//!
//! One compio runtime, one business thread: accept, every connection, every session and
//! every planet stream live on that thread.

use std::{
    cell::RefCell,
    net::SocketAddr,
    rc::Rc,
};

use abs_art_facade::{TrJoinHandle, TrLocalScope};
use abs_cancel::TrMayCancel as _;
use mptp_smux::x_deps::abs_smux;
use anyhow::{Result, anyhow};
use compio::net::{TcpListener, TcpStream};
use mptp_smux::serving::MptpServlet;

use crate::scene::{
    ConnectionCtx, PlanetRegistry, SceneHandler, SceneHub, ServingCfg,
    cfg_::{Dock, SharedConn},
    conn_::{accept_conn_async_, share_conn_},
    protocol_::ClientId,
};

/// 服务端在缺省端口上监听并永远运行。
///
/// # Errors
///
/// 绑定失败时返回错误；单条连接的错误只打印，不终止服务器。
pub async fn serve_async_(addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| anyhow!("监听 {addr} 失败：{err}"))?;
    let local = listener
        .local_addr()
        .map_err(|err| anyhow!("取监听地址失败：{err}"))?;
    println!("scene-server 已就绪：监听 {local}（单 compio 业务线程）");
    serve_on_listener_async_(listener).await
}

/// 在一个已经绑定的监听套接字上运行服务器。
///
/// # Errors
///
/// 接受失败（监听套接字不可用）时返回错误。
pub async fn serve_on_listener_async_(listener: TcpListener) -> Result<()> {
    let registry = Rc::new(RefCell::new(PlanetRegistry::new_()));
    let hub = Rc::new(RefCell::new(SceneHub::new_()));
    let scope = crate::scene::cfg_::compio_rt_().local_scope();
    let mut next_connection: ClientId = 1u64;

    loop {
        let (stream, peer) = listener
            .accept()
            .await
            .map_err(|err| anyhow!("接受连接失败：{err}"))?;
        let connection_id = next_connection;
        next_connection = next_connection.saturating_add(1u64);
        println!("scene-server：接受连接 #{connection_id}（{peer}）");

        let registry_for_conn = registry.clone();
        let hub_for_conn = hub.clone();
        let task = scope.spawn_local(async move {
            if let Err(err) = serve_connection_async_(
                stream,
                connection_id,
                registry_for_conn,
                hub_for_conn,
            )
            .await
            {
                eprintln!("scene-server：连接 #{connection_id} 结束：{err:#}");
            }
        });
        // 连接任务独立运行；丢弃句柄不影响它。
        task.detach();
    }
}

/// 连接任务被取消时的兜底清理（不广播 `Removed`）。
struct ConnCleanup_ {
    /// 连接上下文。
    ctx_: Rc<ConnectionCtx>,
}

impl Drop for ConnCleanup_ {
    fn drop(&mut self) {
        self.ctx_.cleanup_sync_();
    }
}

/// 一条连接：握手 → 建 servlet → 绑定默认服务垛口 → 循环接受子流。
async fn serve_connection_async_(
    stream: TcpStream,
    connection_id: ClientId,
    registry: Rc<RefCell<PlanetRegistry>>,
    hub: Rc<RefCell<SceneHub>>,
) -> Result<()> {
    let conn = accept_conn_async_(stream).await?;
    let shared: SharedConn = share_conn_(conn);
    let ctx = Rc::new(ConnectionCtx::new_(connection_id, registry, hub));
    let handler = SceneHandler::new_(ctx.clone());
    let servlet = Rc::new(MptpServlet::<ServingCfg, SceneHandler>::new(handler));
    let _cleanup = ConnCleanup_ { ctx_: ctx.clone() };

    let mut binding = abs_smux::conn::TrConnection::bind_async(
        &*shared,
        Dock::new(u32::from(mptp_smux::K_DEFAULT_SERVING_DOCK)),
    )
    .await
    .map_err(|err| anyhow!("绑定服务垛口失败：{err}"))?;
    let mut listener = servlet
        .listen_async(&mut binding, 16usize)
        .await
        .map_err(|err| anyhow!("监听子流失败：{err}"))?;

    let scope = crate::scene::cfg_::compio_rt_().local_scope();
    // 连接级保活：客户端每 500ms 发一次 `Head`，这里用 1 秒的接受超时检查「最近是否
    // 还有请求」。超过 3 秒没有任何请求就判定连接已死；`smux_v1` 在对端断开时不一定
    // 会让 `income_async` 报错，因此需要这条业务级判据。
    const K_ACCEPT_TICK: core::time::Duration = core::time::Duration::from_secs(1u64);
    const K_IDLE_LIMIT: core::time::Duration = core::time::Duration::from_secs(3u64);
    loop {
        let accepted = compio::time::timeout(K_ACCEPT_TICK, async {
            servlet.accept_async(&mut listener).await
        })
        .await;
        let session = match accepted {
            Ok(Ok(session)) => session,
            Ok(Err(err)) => {
                println!("scene-server：连接 #{connection_id} 不再接受子流（{err}）");
                break;
            }
            Err(_) => {
                if ctx.idle_for_() > K_IDLE_LIMIT {
                    println!("scene-server：连接 #{connection_id} 空闲超时，判死");
                    break;
                }
                continue;
            }
        };
        let servlet_for_session = servlet.clone();
        let token = ctx.token_();
        let task = scope.spawn_local(async move {
            if let Err(err) = servlet_for_session
                .serve_session_async(session)
                .may_cancel_with(token)
                .await
            {
                // `405` / handler 错误是单条请求的正常结果，不拆连接。
                if !matches!(
                    err,
                    mptp_smux::ServeError::NotAllowed { .. } | mptp_smux::ServeError::Target { .. }
                ) {
                    eprintln!("scene-server：session 结束（{err}）");
                }
            }
        });
        task.detach();
    }

    // 正常收尾：取消在途 session、广播 Removed、删除行星与订阅。
    ctx.cleanup_async_().await;
    println!("scene-server：连接 #{connection_id} 关闭");
    Ok(())
}
