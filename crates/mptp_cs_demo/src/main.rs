//! 进程内客户端 / 服务端环回的可执行入口。
//!
//! 运行：`cargo run -p mptp_cs_demo`
//!
//! 同样的场景也有一份集成测试（`tests/local_e2e.rs`），跑 `cargo test -p mptp_cs_demo`
//! 即可。

use anyhow::Result;
use mptp_cs_demo::{K_LISTEN_DOCK, run_local_roundtrip_};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    println!("== MPTP 进程内 cs 环回演示 ==");
    println!("1. 在进程内建一对互连的复用连接（两条全被动环 + 本地握手，不经过 socket）");
    println!("2. 服务端在 dock {K_LISTEN_DOCK} 上监听；客户端向同一个 dock 发起 View /hello");
    println!("3. 服务端解码请求前缀 → 路由到 /hello → handler 回 200 OK");
    println!("4. 客户端解析响应前缀");
    println!();

    // 应用**不**需要（也**不能**）自己驱动本地队列：连接由 `mux_` 里的宿主线程托管，
    // 它一直在跑 `run_until`。这里直接 await 就行 —— 反过来，如果应用也待在
    // `run_until` 的调用栈里，`AsStdRead/Write` 内部的 `block_in_place` 会被 tokio
    // 拒绝（「can call blocking only when running on the multi-threaded runtime」）。
    let status = run_local_roundtrip_().await?;

    println!();
    println!("完成：客户端读到的响应状态 = {}", status.inner());
    Result::Ok(())
}
