#![feature(impl_trait_in_assoc_type)]

//! 中间件洋葱化（Phase D / G1）的集成测试。
//!
//! 夹具在 `/test/order` 上依次挂了 `OrderMiddleware`（`next` 前后各记一笔）与
//! `GuardMiddleware`（带 `X-Scene-Deny` 头时短路 403），末端是记录 `handler` 的端点。
//! 测试验证「洋葱的包裹顺序」与「短路不会到达终端处理器」。
//!
//! # `onion_`: onion middleware ordering and short-circuiting
//!
//! The fixture puts `OrderMiddleware` (records around `next`) and `GuardMiddleware` (403 on
//! `X-Scene-Deny`, without calling `next`) in front of a handler that records `handler`.
//! The tests check wrap order and that a short-circuit never reaches the handler.

mod common;

use abs_mptp::spec::Status;
use common::{SceneFixture, SceneTestClient, wait_until_async_, with_timeout_};
use mptp_smux::HeaderVal;

/// 读取洋葱记录快照。
fn trace_snapshot_(fixture: &SceneFixture) -> Vec<&'static str> {
    fixture.trace_().lock().expect("洋葱记录锁被污染").clone()
}

/// 测试中间件在终端处理器前后各执行一次（洋葱包裹）。
/// Test that middleware runs before and after the terminal handler.
///
/// - 手段：`View /test/order`，`OrderMiddleware` 在调用 `next` 前写 `before`、之后写
///   `after`，端点写 `handler`。
/// - 判断：状态 `200 Ok`，记录顺序恰好是 `before → handler → after`。
///
/// - Means: `View /test/order`; `OrderMiddleware` records `before` around `next` and the
///   endpoint records `handler`.
/// - Verdict: status is `200 Ok` and the trace is exactly `before → handler → after`.
#[compio::test]
async fn onion_wraps_handler_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/order").send_async().await })
        .await
        .expect("发送 View /test/order");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");

    wait_until_async_(|| {
        fixture
            .trace_()
            .lock()
            .map(|trace| trace.len() >= 3usize)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        trace_snapshot_(&fixture),
        vec!["before", "handler", "after"],
        "洋葱顺序应为 before → handler → after"
    );
}

/// 测试中间件短路后终端处理器不会被调用。
/// Test that a short-circuiting middleware never reaches the terminal handler.
///
/// - 手段：`View /test/order` 带 `X-Scene-Deny` 头；`GuardMiddleware` 不调用 `next`，
///   直接返回 `Some(403)`，由管线写无体回复。
/// - 判断：状态 `403 Forbidden`，记录只有 `before → after`（没有 `handler`）。
///
/// - Means: `View /test/order` with `X-Scene-Deny`; `GuardMiddleware` returns `Some(403)`
///   without calling `next`, and the pipeline writes the no-body reply.
/// - Verdict: status is `403 Forbidden` and the trace is `before → after` without `handler`.
#[compio::test]
async fn short_circuit_skips_handler_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async {
        client
            .client_()
            .view("/test/order")
            .header("X-Scene-Deny", HeaderVal::from_text("1"))
            .send_async()
            .await
    })
    .await
    .expect("发送 View /test/order");
    assert_eq!(resp.status(), Status::Forbidden, "短路中间件应让管线回 403");

    wait_until_async_(|| {
        fixture
            .trace_()
            .lock()
            .map(|trace| trace.len() >= 2usize)
            .unwrap_or(false)
    })
    .await;
    assert_eq!(
        trace_snapshot_(&fixture),
        vec!["before", "after"],
        "短路后不应出现 handler"
    );
}
