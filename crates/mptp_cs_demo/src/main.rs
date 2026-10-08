//! 进程内客户端 / 服务端环回的可执行入口。
//!
//! 运行：`cargo run -p mptp_cs_demo`
//!
//! 同样的场景也有一份集成测试（`tests/local_e2e.rs`），跑 `cargo test -p mptp_cs_demo`
//! 即可。

use abs_art::TrLocalScope;
use anyhow::Result;
use mptp_cs_demo::run_local_roundtrip_;
use smux_v1::x_deps::{abs_art, abs_art_bridge};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    // 连接把五个循环投递到**本地作用域**（`abs_art::TrLocalScope`），因此整段场景
    // 必须由 `run_until` 驱动，否则循环不会被推进。
    let rt = <abs_art_bridge::Runtime<{ abs_art::FULL }>>::current();
    let scope = rt.local_scope();
    let status = scope.run_until(run_local_roundtrip_()).await?;
    println!("进程内 cs 环回成功，响应状态 = {}", status.inner());
    Result::Ok(())
}
