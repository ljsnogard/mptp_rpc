//! UDS 装配：把一条 Unix domain socket 变成 `smux_v1` 的复用连接。
//!
//! socket 与连接循环独占宿主线程；应用线程只拿 `MuxConnection`。
//!
//! # `conn_`: the UDS + `smux_v1` assembly
//!
//! The socket and the multiplexing loops live on a dedicated host thread; the application
//! thread only receives the `MuxConnection` handle.

use std::{
    path::PathBuf,
    sync::mpsc::{Receiver, channel},
    time::Duration,
};

use abs_art::TrLocalScope;
use anyhow::{Result, anyhow};
use buffex_tokio_adapt::DefaultAllocConfig;
use smux_v1::{
    connection::{DefaultConnCfg, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent, HandshakeDelivery},
        opts::BasicOpts,
    },
    x_deps::abs_art_facade,
};
use tokio::net::{
    UnixListener, UnixStream,
    unix::{OwnedReadHalf, OwnedWriteHalf},
};

use crate::cli::Role;

/// 连接侧写半边（环 → socket）。
pub type Tx = buffex_tokio_adapt::BuffWrite<OwnedWriteHalf>;

/// 连接侧读半边（socket → 环）。
pub type Rx = buffex_tokio_adapt::BuffRead<OwnedReadHalf>;

/// 本 demo 使用的复用连接。
pub type Conn = MuxConnection<DefaultConnCfg<Tx, Rx>>;

/// socket 适配器两侧环的容量。
const K_SOCKET_CAP: usize = 64usize * 1024usize;

/// 客户端拨号的重试次数与间隔。
const K_CONNECT_TRIES: usize = 100usize;
const K_CONNECT_PAUSE: Duration = Duration::from_millis(50u64);

/// 宿主线程回传的两条 channel：先就绪信号，再连接对象。
type HostChannels = (Receiver<Result<()>>, Receiver<Result<Conn>>);

/// 在宿主线程上完成「bind / connect + 适配器 + 握手 + 建连接」，并驱动连接循环。
pub fn spawn_host_(socket: PathBuf, role: Role) -> HostChannels {
    let (ready_tx, ready_rx) = channel::<Result<()>>();
    let (conn_tx, conn_rx) = channel::<Result<Conn>>();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Result::Ok(rt) => rt,
            Result::Err(err) => {
                let message = format!("建宿主线程运行时失败：{err}");
                let _ = ready_tx.send(Result::Err(anyhow!("{message}")));
                let _ = conn_tx.send(Result::Err(anyhow!("{message}")));
                return;
            }
        };
        rt.block_on(async move {
            let art = abs_art_facade::default_rt_();
            let scope = art.local_scope();
            scope
                .run_until(async move {
                    let stream = match prepare_endpoint_(role, &socket, &ready_tx).await {
                        Result::Ok(stream) => stream,
                        Result::Err(err) => {
                            let _ = conn_tx.send(Result::Err(err));
                            return;
                        }
                    };
                    let built = build_conn_on_socket_(stream, role).await;
                    let _ = conn_tx.send(built);
                    core::future::pending::<()>().await;
                })
                .await;
        });
    });

    (ready_rx, conn_rx)
}

/// 备好端点并回传就绪信号：服务端只 bind，客户端连上。
async fn prepare_endpoint_(
    role: Role,
    socket: &PathBuf,
    ready_tx: &std::sync::mpsc::Sender<Result<()>>,
) -> Result<UnixStream> {
    match role {
        Role::Server => {
            let _ = std::fs::remove_file(socket);
            let listener = UnixListener::bind(socket)
                .map_err(|err| anyhow!("监听 {} 失败：{err}", socket.display()))?;
            if ready_tx.send(Result::Ok(())).is_err() {
                return Result::Err(anyhow!("就绪信号无人接收"));
            }
            let (stream, _addr) = listener
                .accept()
                .await
                .map_err(|err| anyhow!("接受连接失败：{err}"))?;
            Result::Ok(stream)
        }
        Role::Client => {
            let stream = connect_retry_(socket).await?;
            if ready_tx.send(Result::Ok(())).is_err() {
                return Result::Err(anyhow!("就绪信号无人接收"));
            }
            Result::Ok(stream)
        }
    }
}

/// 在宿主线程里造适配器、跑握手、建出连接。
async fn build_conn_on_socket_(stream: UnixStream, role: Role) -> Result<Conn> {
    let (read_half, write_half) = stream.into_split();
    let rx = Rx::try_new(read_half, K_SOCKET_CAP, DefaultAllocConfig)
        .map_err(|err| anyhow!("造读半边失败：{err}"))?;
    let tx = Tx::try_new_local(write_half, K_SOCKET_CAP, DefaultAllocConfig)
        .map_err(|err| anyhow!("造写半边失败：{err}"))?;

    let opts = BasicOpts::default();
    let delivery: HandshakeDelivery<Tx, Rx> = match role {
        Role::Server => {
            HandshakeAgent::new(tx, rx)
                .listen_async(&opts, AcceptAllEntries)
                .await
        }
        Role::Client => {
            HandshakeAgent::new(tx, rx)
                .invite_async(&opts, AcceptAllEntries)
                .await
        }
    }
    .map_err(|err| anyhow!("握手失败：{err:?}"))?;

    let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
    Result::Ok(MuxConnection::new(delivery, cfg))
}

/// 带重试地连接对端。
async fn connect_retry_(socket: &PathBuf) -> Result<UnixStream> {
    let mut last: Option<std::io::Error> = Option::None;
    for _ in 0..K_CONNECT_TRIES {
        match UnixStream::connect(socket).await {
            Result::Ok(stream) => return Result::Ok(stream),
            Result::Err(err) => {
                last = Option::Some(err);
                tokio::time::sleep(K_CONNECT_PAUSE).await;
            }
        }
    }
    let detail = last.map_or_else(|| "没有尝试过".to_string(), |err| err.to_string());
    Result::Err(anyhow!(
        "连不上 {}（试了 {K_CONNECT_TRIES} 次）：{detail}",
        socket.display()
    ))
}

/// 从宿主线程的 channel 上收一个值。
pub fn recv_from_host_<T>(rx: &Receiver<Result<T>>) -> Result<T> {
    rx.recv().map_err(|_| anyhow!("宿主线程提前退出"))?
}

/// 删除 UDS 文件；文件不存在时忽略。
pub fn remove_socket_(socket: &PathBuf) {
    let _ = std::fs::remove_file(socket);
}
