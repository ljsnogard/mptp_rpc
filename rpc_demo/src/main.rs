//! MPTP RPC 手动实现试验：UDS 上的 workspace / session 分层 CRUD。
//!
//! 这个 demo **不使用** `#[rpc]` 宏，手动实现蓝图里的骨架：
//!
//! - 客户端代理只持有 `MptpClient`，interface 由零大小 marker 提供；
//! - 服务端 `MptpServer` 按值交付 `ChannelTx` / `ChannelRx`；
//! - 服务端在运行期用两级 `PathTrie` 分派 interface → service；
//! - 每个 service 有独立队列与 worker 线程，worker 内按 body enum 分派方法；
//! - 传输是 UDS 上的 `smux_v1`，MPTP 负责前缀与会话。
//!
//! # `rpc_demo`: a hand-written RPC-over-MPTP experiment
//!
//! No `#[rpc]` macro is involved. The client proxy only holds `MptpClient`; the server uses
//! two runtime `PathTrie` levels (interface, then service), per-service queues and worker
//! threads, and a hand-written body enum for method dispatch.

#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
// body enum 与多层 async 类型可能较深，放宽递归上限。
#![recursion_limit = "256"]

mod body_;
mod cli;
mod client_;
mod conn_;
mod protocol_;
mod server_;
mod trie_;

use std::process::ExitCode;

use anyhow::Result;
use core_rpc_mptp_smux_v1::CoreAlloc;
use mm_ptr::Shared;

use crate::cli::{Options, Role};

fn main() -> ExitCode {
    let opts = match cli::parse_args_(std::env::args()) {
        Result::Ok(opts) => opts,
        Result::Err(message) => {
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

/// 按角色跑完一轮。
async fn run_(opts: &Options) -> Result<()> {
    let (ready_rx, conn_rx) = conn_::spawn_host_(opts.socket.clone(), opts.role);

    match opts.role {
        Role::Server => {
            conn_::recv_from_host_(&ready_rx)?;
            println!(
                "服务端已就绪：UDS {}，等待客户端……（另一个终端运行 `cargo run -p rpc_demo -- client --socket {}`）",
                opts.socket.display(),
                opts.socket.display()
            );

            let conn = conn_::recv_from_host_(&conn_rx)?;
            let result = server_::serve_(conn).await;
            conn_::remove_socket_(&opts.socket);
            result?;
        }
        Role::Client => {
            conn_::recv_from_host_(&ready_rx)?;
            println!("客户端已连接：UDS {}", opts.socket.display());

            let conn = conn_::recv_from_host_(&conn_rx)?;
            let shared = Shared::new(conn, CoreAlloc);
            client_::run_(shared).await?;
        }
    }

    Result::Ok(())
}
