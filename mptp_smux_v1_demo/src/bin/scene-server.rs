//! `scene-server`：单 compio 线程的太阳系场景服务端。
//!
//! 服务端不渲染、不算显示位置；它只维护行星参数，并通过每颗行星的 `Pull` 后缀流广播。
//!
//! # `scene-server`
//!
//! One compio thread; no rendering and no display positions. It only keeps planet
//! parameters and broadcasts them through per-planet `Pull` suffix streams.

use std::{net::SocketAddr, process::ExitCode};

use abs_art_facade::TrLocalScope;
use anyhow::Result;
use mptp_smux_v1_demo::scene::server_::serve_async_;

/// 缺省监听地址。
const K_DEFAULT_LISTEN: &str = "127.0.0.1:7721";

fn main() -> ExitCode {
    let addr = match parse_args_(std::env::args().skip(1usize)) {
        Result::Ok(addr) => addr,
        Result::Err(message) => {
            println!("{message}");
            return ExitCode::from(2u8);
        }
    };

    let runtime = match compio::runtime::Runtime::new() {
        Result::Ok(runtime) => runtime,
        Result::Err(err) => {
            eprintln!("建 compio 运行时失败：{err}");
            return ExitCode::FAILURE;
        }
    };

    let outcome = runtime.block_on(async {
        // compio 运行时上下文内：取运行时值与本线程队列。
        let scope = mptp_smux_v1_demo::scene::cfg_::compio_rt_().local_scope();
        scope.run_until(serve_async_(addr)).await
    });

    match outcome {
        Result::Ok(()) => ExitCode::SUCCESS,
        Result::Err(err) => {
            eprintln!("scene-server 失败：{err:#}");
            ExitCode::FAILURE
        }
    }
}

/// 解析命令行：`scene-server [--listen 127.0.0.1:7721]`。
fn parse_args_(mut args: impl Iterator<Item = String>) -> Result<SocketAddr, String> {
    let mut listen = K_DEFAULT_LISTEN
        .parse::<SocketAddr>()
        .expect("缺省监听地址必须合法");
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                println!("用法：scene-server [--listen 127.0.0.1:7721]");
                return Result::Err(String::new());
            }
            "--listen" => {
                let value = args
                    .next()
                    .ok_or_else(|| String::from("`--listen` 缺少地址"))?;
                listen = value
                    .parse()
                    .map_err(|err| format!("`--listen` 不是合法地址（{value}）：{err}"))?;
            }
            other => return Result::Err(format!("不认识的参数：{other}")),
        }
    }
    Result::Ok(listen)
}
