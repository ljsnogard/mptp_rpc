//! MPTP 的最小演示：真实 TCP socket 上的一次一问一答。
//!
//! # 这个 demo 演示什么
//!
//! ```text
//! client ──dial──▶ server             TCP + smux_v1 握手
//!   └── Call /rpc/echo + 请求体 ──▶   请求前缀（method / path / headers）+ 按 Body_Size 写的体
//!   ◀── 200 OK + 回复体               回复前缀 + 按 Body_Size 写的体
//! ```
//!
//! 它刻意只做**一件事**：把「有请求体、也有回复体」这条路径在真实 socket 上跑通。
//! 资源 CRUD、Push / Pull 那些场景压在同一个 demo 里，只会让「哪一层坏了」变得难判。
//!
//! # 怎么跑
//!
//! 两个终端：
//!
//! ```console
//! $ cargo run -p rpc_demo -- server
//! 服务端已就绪：监听 127.0.0.1:7719，dock 1
//! 等待客户端连接……
//!
//! $ cargo run -p rpc_demo -- client
//! 客户端已拨号：本地地址 127.0.0.1:xxxxx，目标 dock 1
//! 服务端回声："hello from rpc_demo"
//! ```
//!
//! # 为什么两端都是「宿主线程 + 应用线程」
//!
//! 见 [`conn_`] 的模块文档：socket 与连接的五条循环必须由同一个本地队列驱动，而应用
//! 侧会同步等数据；把两者放在同一条 `current_thread` runtime 上会互相憋死。

#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

mod cli;
mod client_;
mod conn_;
mod server_;

use std::process::ExitCode;

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_smux::conn::TrConnection;
use anyhow::{Result, anyhow};
use mm_ptr::{Shared, x_deps::abs_mm};
use mptp_core::serving::server::SessionContext;
use smux_v1::{
    connection::Dock,
    x_deps::{abs_buff, abs_smux, mm_ptr},
};

use crate::cli::{Options, Role};

/// 客户端发出去的请求体。
const K_PAYLOAD: &str = "hello from rpc_demo";

fn main() -> ExitCode {
    let opts = match cli::parse_args_(std::env::args()) {
        Result::Ok(opts) => opts,
        Result::Err(message) => {
            // `--help` 也走这条路：它给出的「错误」内容就是用法说明本身。
            let is_help = message == cli::K_HELP;
            println!("{message}");
            return if is_help {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2u8)
            };
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2usize)
        .enable_all()
        .build()
    {
        Result::Ok(rt) => rt,
        Result::Err(err) => {
            eprintln!("建 tokio 运行时失败：{err}");
            return ExitCode::FAILURE;
        }
    };

    match rt.block_on(run_(&opts)) {
        Result::Ok(()) => ExitCode::SUCCESS,
        Result::Err(err) => {
            eprintln!("运行失败：{err:#}");
            ExitCode::FAILURE
        }
    }
}

/// 按角色跑完这一轮。
async fn run_(opts: &Options) -> Result<()> {
    // socket 与连接循环都在宿主线程上；本线程只拿地址与连接对象。
    let (addr_rx, conn_rx) = conn_::spawn_host_(opts);

    match opts.role {
        Role::Server => {
            // 地址要在 bind 成功之后**立刻**打印：对端（或编排脚本）等的是这一行。
            let actual = conn_::recv_from_host_(&addr_rx)?;
            println!("服务端已就绪：监听 {actual}，dock {}", opts.dock);
            println!("等待客户端连接……（在另一个终端跑 `cargo run -p rpc_demo -- client`）");

            let conn = conn_::recv_from_host_(&conn_rx)?;
            let mut binding = conn
                .bind_async(Dock::new(opts.dock))
                .may_cancel_with(NonCancellableToken::new())
                .await
                .map_err(|err| anyhow!("绑定 dock {} 失败：{err}", opts.dock))?;

            let server = server_::build_server_();
            let mut context = SessionContext;
            server_::serve_one_channel_(&server, &mut binding, &mut context).await?;
            println!("服务端：一条子流处理完毕，退出。");
        }
        Role::Client => {
            let local = conn_::recv_from_host_(&addr_rx)?;
            println!("客户端已拨号：本地地址 {local}，目标 dock {}", opts.dock);

            let conn = conn_::recv_from_host_(&conn_rx)?;
            let shared = Shared::new(conn, CoreAlloc);
            let echoed = client_::run_client_(shared, opts.dock, K_PAYLOAD).await?;
            println!("服务端回声：{echoed:?}");
            println!("完成：请求体与回复体都按 Body_Size 完整走了一趟。");
        }
    }

    Result::Ok(())
}
