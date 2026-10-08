//! 进程内客户端 / 服务端环回的端到端验收。
//!
//! 这里覆盖的是 `mptp_core` 的完整协议路径：建流（客户端 `open` + 服务端 `accept`）、
//! 请求前缀解码、路由、handler 链、回复前缀写回、客户端解析。传输用的是 `smux_v1`
//! 的真实实现，但两条全被动环直连两个端点，不经过任何 socket。

use mptp_core::specs::Status;
use mptp_cs_demo::run_local_roundtrip_;

/// 测试目标：客户端与服务端在进程内完成一次完整的
/// 「建流 → 请求 → 路由 → 回复 → 解析」往返。
/// - 手段：建一对互连的 `MuxConnection`（本地握手 + 两条全被动传输环），服务端在
///   dock 1 上监听并服务一条子流，客户端向 dock 1 发起 `View /hello`；连接的读 / 写
///   循环由 `mux_` 的宿主线程持续驱动，应用侧直接 await。
/// - 判断：客户端解析出的响应状态必须是 `200 OK`；任一步失败都会带上下文返回 `Err`，
///   测试随即失败。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_roundtrip_returns_ok_() -> anyhow::Result<()> {
    let status = run_local_roundtrip_().await?;
    assert_eq!(status, Status::Ok, "响应状态应当是 200 OK");
    Ok(())
}
