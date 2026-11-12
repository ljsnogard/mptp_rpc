#![feature(impl_trait_in_assoc_type)]

//! 兜底 500 与错误观测 hook（Phase E / G3）的集成测试。
//!
//! 夹具在场景路由上装了错误观测 hook（记录 `err.to_string()`），并注册三个端点：
//! `/test/no-reply`（Ok 但不写回复）、`/test/fail`（直接 Err）、`/test/fail-after-reply`
//! （先写 200 再 Err）。测试验证设计稿 §7④/⑤ 的三条收尾契约。
//!
//! # `fallback_`: fallback 500 and the error observation hook
//!
//! The fixture installs an error hook and three endpoints: `no-reply` (Ok, nothing written),
//! `fail` (bare `Err`) and `fail-after-reply` (200 first, then `Err`).

mod common;

use abs_mptp::spec::Status;
use common::{SceneFixture, SceneTestClient, wait_until_async_, with_timeout_};

/// 测试处理器返回 `Ok` 但没写回复时，客户端收到兜底 500。
/// Test that a handler returning `Ok` without a reply yields the fallback 500.
///
/// - 手段：`View /test/no-reply`，端点只返回 `Ok(())`。
/// - 判断：客户端状态是 `500 Internal Server Error`——框架在管线之后补了无体回复。
///
/// - Means: `View /test/no-reply`, whose handler only returns `Ok(())`.
/// - Verdict: the client sees `500 Internal Server Error`.
#[compio::test]
async fn missing_reply_falls_back_to_500_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/no-reply").send_async().await })
        .await
        .expect("发送 View /test/no-reply");
    assert_eq!(
        resp.status(),
        Status::InternalServerError,
        "未回复的请求应被兜底成 500"
    );
}

/// 测试处理器返回错误时，hook 收到错误且客户端只看到 500。
/// Test that a handler error reaches the hook while the client only sees a 500.
///
/// - 手段：`View /test/fail`，端点返回 `ServeError::Target { detail: "boom" }`。
/// - 判断：客户端状态是 500；hook 记录里出现含 `boom` 的文本——错误只走观测通道，
///   不写到线上。
///
/// - Means: `View /test/fail` returns `ServeError::Target { detail: "boom" }`.
/// - Verdict: the client sees a 500 and the hook log contains `boom`.
#[compio::test]
async fn handler_error_is_reported_and_500_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/fail").send_async().await })
        .await
        .expect("发送 View /test/fail");
    assert_eq!(
        resp.status(),
        Status::InternalServerError,
        "错误应兜底成 500"
    );

    wait_until_async_(|| {
        fixture
            .errors_()
            .lock()
            .map(|errors| !errors.is_empty())
            .unwrap_or(false)
    })
    .await;
    let errors = fixture.errors_().lock().expect("错误记录锁被污染").clone();
    assert!(
        errors.iter().any(|err| err.contains("boom")),
        "hook 应收到含 boom 的错误，实际是 {errors:?}"
    );
}

/// 测试已经写出回复后返回错误时，不覆盖原回复。
/// Test that an error after a reply has started does not overwrite the reply.
///
/// - 手段：`View /test/fail-after-reply`，端点先写 `200 Ok` 无体回复，再返回错误。
/// - 判断：客户端状态仍是 200（回复已经开始，兜底 500 不会改写）；hook 仍收到错误。
///
/// - Means: `View /test/fail-after-reply` writes `200 Ok` then returns an error.
/// - Verdict: the client still sees 200 and the hook still receives the error.
#[compio::test]
async fn error_after_reply_keeps_reply_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async {
        client
            .client_()
            .view("/test/fail-after-reply")
            .send_async()
            .await
    })
    .await
    .expect("发送 View /test/fail-after-reply");
    assert_eq!(resp.status(), Status::Ok, "已写出的回复不应被覆盖");

    wait_until_async_(|| {
        fixture
            .errors_()
            .lock()
            .map(|errors| errors.iter().any(|err| err.contains("boom-after")))
            .unwrap_or(false)
    })
    .await;
}
