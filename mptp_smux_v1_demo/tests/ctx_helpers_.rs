//! `MptpContext` 一级 helper 的集成测试（Phase A）。
//!
//! 这些 helper 是 `mptp_smux::serving::ServingSession` 之上的薄委托，单测无法构造会话
//! （`ServingSession::new_` 是 crate 私有），因此走真实 TCP + smux + `MptpServlet`：
//! `tests/common/mod.rs` 里的 `/test/ctx*` 端点在真实请求里调用 helper，把观测结果写进
//! [`CtxProbe`]，测试收到回复后断言。
//!
//! # `ctx_helpers_`: integration tests for the `MptpContext` level-one helpers
//!
//! These helpers only delegate to `ServingSession`, whose constructor is crate-private, so
//! the tests run over a real TCP + smux + `MptpServlet` stack: the `/test/ctx*` endpoints in
//! `tests/common/mod.rs` call the helpers and record their findings into [`CtxProbe`].

mod common;

use abs_mptp::spec::Status;
use common::{CtxProbe, SceneFixture, SceneTestClient, wait_until_async_, with_timeout_};

/// 取一份探针快照（锁中毒即视为测试失败）。
fn probe_snapshot_(fixture: &SceneFixture) -> CtxProbe {
    let probe = fixture.probe_().lock().expect("ctx 探针锁被污染");
    CtxProbe {
        reply_before: probe.reply_before,
        reply_after: probe.reply_after,
        headers_mutated: probe.headers_mutated,
        body_remaining: probe.body_remaining,
        body_finished: probe.body_finished,
        done: probe.done,
    }
}

/// 测试 `reply_written` / `headers_mut` / `response` 三个 helper 的委托行为。
/// Test that `reply_written`, `headers_mut` and `response` delegate correctly.
///
/// - 手段：`View /test/ctx`；端点先读 `reply_written()`（应为 `false`），用 `headers_mut`
///   写入 `X-Ctx-Probe` 再从 `headers()` 读回，随后用 `response(Ok)` 写无体回复，最后再读
///   一次 `reply_written()`（应为 `true`），把三个观测值写进探针。
/// - 判断：客户端收到 `200 Ok`；探针里 `reply_before == false`、`reply_after == true`、
///   `headers_mutated == true`——说明 helper 确实落在同一个会话上。
///
/// - Means: `View /test/ctx` reads `reply_written` (expected `false`), mutates a header via
///   `headers_mut` and reads it back, writes a no-body reply via `response(Ok)`, then reads
///   `reply_written` again (expected `true`), recording all three into the probe.
/// - Verdict: the client sees `200 Ok` and the probe reports `false` / `true` / `true`.
#[compio::test]
async fn reply_state_and_headers_delegate_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/ctx").send_async().await })
        .await
        .expect("发送 /test/ctx");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");

    wait_until_async_(|| {
        fixture
            .probe_()
            .lock()
            .map(|probe| probe.done)
            .unwrap_or(false)
    })
    .await;

    let probe = probe_snapshot_(&fixture);
    assert!(!probe.reply_before, "写回复前 reply_written 应为 false");
    assert!(probe.reply_after, "写回复后 reply_written 应为 true");
    assert!(probe.headers_mutated, "headers_mut 写入应能被 headers 读回");
}

/// 测试 `req_body_async` 委托返回可用的体读视图（无体请求）。
/// Test that `req_body_async` delegates to a usable body read view (empty body).
///
/// - 手段：`View /test/ctx-body`；端点 `await` 出体读视图后取 `remaining()` 与
///   `is_finished()`，再写无体回复。
/// - 判断：客户端收到 `200 Ok`；探针里 `body_remaining == Some(0)` 且
///   `body_finished == true`——视图来自真实的会话体计划，而不是一个空壳。
///
/// - Means: `View /test/ctx-body` awaits the body view, records `remaining()` and
///   `is_finished()`, then writes a no-body reply.
/// - Verdict: the client sees `200 Ok`; the probe reports `Some(0)` and `true`.
#[compio::test]
async fn body_view_delegates_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/ctx-body").send_async().await })
        .await
        .expect("发送 /test/ctx-body");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");

    wait_until_async_(|| {
        fixture
            .probe_()
            .lock()
            .map(|probe| probe.done)
            .unwrap_or(false)
    })
    .await;

    let probe = probe_snapshot_(&fixture);
    assert_eq!(probe.body_remaining, Some(0usize), "无体请求剩余字节应为 0");
    assert!(probe.body_finished, "无体请求的体视图应已完成");
}
