//! TCP socket 的装配：把一条连接变成 `abs_smux` 的复用连接。
//!
//! # 为什么 socket 与连接循环要独占一个**宿主线程**
//!
//! `mptp_core` 的收发走 `abs_buff` 的段接口，而这些段是由投递在**本线程本地队列**
//! 上的循环供料的（tokio 下即 `LocalSet`）；socket 的 IO 又由同一个 runtime 的 IO
//! driver 驱动。三者若挤在一条 `current_thread` runtime 上，任何一次「同步等数据」
//! 都会把 IO driver 一起停住。
//!
//! 所以这里分两层：
//!
//! | 线程 | 持有 | 干什么 |
//! | --- | --- | --- |
//! | **宿主线程**（本模块新建） | socket + 适配器 + 连接的五条循环 | `run_until` 一直驱动到进程结束 |
//! | **应用线程**（`main`） | 只持连接对象 | `bind` / `serve` 或 `request` / `recv` |
//!
//! socket 全程不跨 runtime：bind / accept / connect 都在宿主线程里完成，地址与连接经
//! channel 交回应用线程。`MuxConnection` 是 `Send + Sync` 的智能指针，跨线程传递安全。

use std::{
    net::SocketAddr,
    sync::mpsc::{Receiver, channel},
    time::Duration,
};

use abs_art::TrLocalScope;
use abs_mm::CoreAlloc;
use anyhow::{Result, anyhow};
use buffex_tokio_adapt::DefaultAllocConfig;
use mm_ptr::{Owned, x_deps::abs_mm};
use smux_v1::{
    connection::{DefaultConnCfg, MuxChanBuffOwnedBy, MuxConnection},
    flow_ctrl::DefaultPolicy,
    handshake::{
        agent::{AcceptAllEntries, HandshakeAgent, HandshakeDelivery},
        opts::BasicOpts,
    },
    x_deps::{abs_art, mm_ptr},
};
use tokio::net::{TcpListener, TcpStream};

use crate::cli::{Options, Role};

/// 连接侧**写半边**（环 → socket）。
pub type Tx = buffex_tokio_adapt::BuffWrite<tokio::net::tcp::OwnedWriteHalf>;

/// 连接侧**读半边**（socket → 环）。
pub type Rx = buffex_tokio_adapt::BuffRead<tokio::net::tcp::OwnedReadHalf>;

/// 本 demo 使用的复用连接。
pub type Conn = MuxConnection<DefaultConnCfg<Tx, Rx>>;

/// 连接级帧暂存缓冲的容量（字节）：只影响吞吐，不影响正确性。
const K_STAGE_CAP: usize = 64usize * 1024usize;

/// socket 适配器两侧环的容量（字节）。
const K_SOCKET_CAP: usize = 64usize * 1024usize;

/// 客户端拨号的重试次数与间隔：两端启动有先后，先起的一端可能还没 bind 上。
const K_CONNECT_TRIES: usize = 100usize;
const K_CONNECT_PAUSE: Duration = Duration::from_millis(100u64);

/// 宿主线程回传的两条 channel：**先**是实际地址，**再**是连接。
type HostChannels = (Receiver<Result<SocketAddr>>, Receiver<Result<Conn>>);

/// 在**宿主线程**上完成「socket + 适配器 + 握手 + 建连接」，并一直驱动连接循环。
///
/// 服务端的实际地址在 `bind` 成功之后**立刻**回传——编排者（人或脚本）要等它才能起
/// 客户端，所以 `accept` 必须排在回传之后：先 `accept` 会变成「服务端等客户端连、
/// 编排者等地址、客户端等编排者放行」的三方互等。
pub fn spawn_host_(opts: &Options) -> HostChannels {
    let (addr_tx, addr_rx) = channel::<Result<SocketAddr>>();
    let (conn_tx, conn_rx) = channel::<Result<Conn>>();
    let role = opts.role;
    let addr = opts.target_addr();

    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Result::Ok(rt) => rt,
            Result::Err(err) => {
                let message = format!("建宿主线程运行时失败：{err}");
                let _ = addr_tx.send(Result::Err(anyhow!("{message}")));
                let _ = conn_tx.send(Result::Err(anyhow!("{message}")));
                return;
            }
        };
        rt.block_on(async move {
            let art = smux_v1::connection::default_rt_();
            let scope = art.local_scope();
            scope
                .run_until(async move {
                    let endpoint = match prepare_endpoint_(role, addr, &addr_tx).await {
                        Result::Ok(option) => option,
                        Result::Err(err) => {
                            let _ = conn_tx.send(Result::Err(err));
                            return;
                        }
                    };
                    let Option::Some(stream) = endpoint else {
                        // 地址已经报过错了，这里只是结束这条宿主线程。
                        let _ = conn_tx.send(Result::Err(anyhow!("端点没有就绪")));
                        return;
                    };
                    let built = build_conn_on_socket_(stream, role).await;
                    let _ = conn_tx.send(built);
                    // 连接建成之后五条循环仍要靠这条本地队列驱动，故一直挂着。
                    core::future::pending::<()>().await;
                })
                .await;
        });
    });

    (addr_rx, conn_rx)
}

/// 备好端点并回传地址：服务端只 `bind`（**不** accept），客户端直接连上。
///
/// # Errors
///
/// 绑定、连接或地址回传失败时返回错误；服务端那条分支成功时返回的是「已 bind 的
/// 监听器」，真正的 `accept` 由调用方在地址回传之后才做。
async fn prepare_endpoint_(
    role: Role,
    addr: SocketAddr,
    addr_tx: &std::sync::mpsc::Sender<Result<SocketAddr>>,
) -> Result<Option<TcpStream>> {
    match role {
        Role::Server => {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|err| anyhow!("监听 {addr} 失败：{err}"))?;
            let actual = listener
                .local_addr()
                .map_err(|err| anyhow!("取监听地址失败：{err}"))?;
            if addr_tx.send(Result::Ok(actual)).is_err() {
                return Result::Err(anyhow!("地址无人接收"));
            }
            // 地址已经交出去，编排者可以放客户端过来了。
            let (stream, _peer) = listener
                .accept()
                .await
                .map_err(|err| anyhow!("接受连接失败：{err}"))?;
            stream
                .set_nodelay(true)
                .map_err(|err| anyhow!("设置 nodelay 失败：{err}"))?;
            Result::Ok(Option::Some(stream))
        }
        Role::Client => {
            let stream = connect_retry_(addr).await?;
            stream
                .set_nodelay(true)
                .map_err(|err| anyhow!("设置 nodelay 失败：{err}"))?;
            let actual = stream
                .local_addr()
                .map_err(|err| anyhow!("取本地地址失败：{err}"))?;
            if addr_tx.send(Result::Ok(actual)).is_err() {
                return Result::Err(anyhow!("地址无人接收"));
            }
            Result::Ok(Option::Some(stream))
        }
    }
}

/// 在宿主线程里造适配器、跑握手、建出连接。
async fn build_conn_on_socket_(stream: TcpStream, role: Role) -> Result<Conn> {
    let (read_half, write_half) = stream.into_split();
    let rx = Rx::try_new(read_half, K_SOCKET_CAP, DefaultAllocConfig)
        .map_err(|err| anyhow!("造读半边失败：{err}"))?;
    // 写侧走**自驱动**形态：泵挂在本线程的本地队列上，帧一提交进环就会上网。
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

    // 运行时值与作用域由配置自己解决（`TrConnCfg::runtime` + `ScopeHost`），
    // 因此这里只需要给出一个流控策略。
    let (delivery, cfg) = DefaultConnCfg::new(delivery, DefaultPolicy);
    let read_buff = MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc));
    let write_buff = MuxChanBuffOwnedBy::new(Owned::new_uninit_slice(K_STAGE_CAP, CoreAlloc));
    Result::Ok(MuxConnection::new(delivery, cfg, read_buff, write_buff))
}

/// 带重试地连接对端：两端启动有先后，先起的一端可能还没 bind 上。
async fn connect_retry_(addr: SocketAddr) -> Result<TcpStream> {
    let mut last: Option<std::io::Error> = Option::None;
    for _ in 0..K_CONNECT_TRIES {
        match TcpStream::connect(addr).await {
            Result::Ok(stream) => return Result::Ok(stream),
            Result::Err(err) => {
                last = Option::Some(err);
                tokio::time::sleep(K_CONNECT_PAUSE).await;
            }
        }
    }
    let detail = last.map_or_else(|| "没有尝试过".to_string(), |err| err.to_string());
    Result::Err(anyhow!(
        "连不上 {addr}（试了 {K_CONNECT_TRIES} 次）：{detail}"
    ))
}

/// 从宿主线程的 channel 上收一个值。
///
/// # Errors
///
/// 宿主线程提前退出（channel 断开）时返回错误。
pub fn recv_from_host_<T>(rx: &Receiver<Result<T>>) -> Result<T> {
    rx.recv().map_err(|_| anyhow!("宿主线程提前退出"))?
}
