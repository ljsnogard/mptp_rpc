//! `mptp_smux` 的下游实现：真实 TCP socket 上的一次一问一答，用来验证被拆出来的
//! `mptp_smux` **符合实际**。
//!
//! # 这个 demo 演示什么
//!
//! ```text
//! client ──dial──▶ server             TCP + smux_v1 握手
//!   ├─ Call /rpc/echo + EchoRequest ──▶  头里声明 Data_Type_Id = EchoRequest 的标识
//!   │  ◀── 200 OK + EchoReply            回复同样声明 Data_Type_Id
//!   ├─ Call /rpc/sum  + SumRequest  ──▶  另一个类型，另一条路径
//!   │  ◀── 200 OK + SumReply
//!   └─ Call /rpc/sum  + EchoRequest ──▶  ◀── 406 NotAcceptable（明确拒绝，不猜）
//! ```
//!
//! # 它验证 `mptp_smux` 的哪几件事
//!
//! | 被验证的东西 | 落在哪 |
//! | --- | --- |
//! | 客户端 ring 注入：`DefaultPrepFactory` + 环境 + `accept_async` | `MptpClient::request_to_dock_async` |
//! | 服务端 ring 注入：每条入向子流现场生产一份新内存 | `MptpServer::serve_listener_async` |
//! | 前缀线格式（由框架统一拥有） | `mptp_smux::messaging` 的四个读写入口 |
//! | 会话状态机：前缀 → 体（边界由前缀决定） | `MptpSession::recv_resp_prefix_async` / `recv_resp_body_async` |
//! | 服务端收尾次序（先半关闭写端，再等对端关闭） | `MptpServer` 内部，本 demo 只管业务 |
//!
//! # 怎么跑
//!
//! 两个终端：
//!
//! ```console
//! $ cargo run -p mptp_smux_v1_demo -- server
//! 服务端已就绪：监听 127.0.0.1:7719，dock 1
//! 等待客户端连接……
//!
//! $ cargo run -p mptp_smux_v1_demo -- client
//! 客户端已拨号：本地地址 127.0.0.1:xxxxx，目标 dock 1
//! 客户端：/rpc/echo 回来 "hello from mptp_smux_v1_demo"（回复声明 0x...）
//! 客户端：/rpc/sum 回来 42（回复声明 0x...）
//! 客户端：把 EchoRequest 发到 /rpc/sum（类型与路径不匹配），服务端回 406
//! ```
//!
//! # 两端都是「宿主线程 + 应用线程」
//!
//! 见 [`conn_`] 的模块文档：socket 与连接的五条循环必须由同一个本地队列驱动，而应用侧会
//! 同步等数据；把两者放在同一条 `current_thread` runtime 上会互相憋死。

#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
// denco 的 GAT future 与多层 helper 让单条 `async fn` 的类型嵌套很深，默认上限不够。
#![recursion_limit = "256"]

mod body_;
mod cfg_;
mod cli;
mod client_;
mod conn_;
mod protocol_;
mod server_;

#[cfg(test)]
mod tests_;

use std::process::ExitCode;

use abs_mm::CoreAlloc;
use anyhow::Result;
use mm_ptr::Shared;

use crate::cli::{Options, Role};

/// 客户端这一轮要走的调用次数：两次正常调用 + 一次故意把类型发错路径。
const K_CALL_COUNT: usize = 3usize;

fn main() -> ExitCode {
    let opts = match cli::parse_args_(std::env::args().skip(1usize)) {
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
            eprintln!("运行失败：{}", cli::format_err_(err));
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
            println!("等待客户端连接……（在另一个终端跑 `cargo run -p mptp_smux_v1_demo -- client`）");

            let conn = conn_::recv_from_host_(&conn_rx)?;
            server_::serve_(conn, opts.dock, K_CALL_COUNT).await?;
        }
        Role::Client => {
            let local = conn_::recv_from_host_(&addr_rx)?;
            println!("客户端已拨号：本地地址 {local}，目标 dock {}", opts.dock);

            let conn = conn_::recv_from_host_(&conn_rx)?;
            let shared = Shared::new(conn, CoreAlloc);
            client_::run_(shared, opts.dock).await?;
            println!("完成：三次调用都按报文头声明的类型标识分派与校验。");
        }
    }

    Result::Ok(())
}
