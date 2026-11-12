//! 请求级运行期状态 `R`（Phase C / G2）的集成测试。
//!
//! 场景路由现在带 `R = SceneRequestState`：`PrincipalMiddleware` 把请求头
//! `X-Scene-Principal` 写进 `ctx.request_state_mut()`，`/test/state` 端点再只读回写。
//! 这验证「中间件 stash → 处理器读」确实发生在**同一个请求**的状态上。
//!
//! # `request_state_`: per-request runtime state
//!
//! The scene router now carries `R = SceneRequestState`: `PrincipalMiddleware` stashes the
//! `X-Scene-Principal` header into `ctx.request_state_mut()` and `/test/state` reads it back,
//! proving that the stash and the read share one per-request state.

mod common;

use abs_mptp::spec::Status;
use common::{SceneFixture, SceneTestClient, with_timeout_};
use mptp_smux::{HeaderKey, HeaderVal};
use mptp_smux_v1_demo::scene::K_HDR_PRINCIPAL;

/// 测试中间件写入的主体能被同一请求的处理器读到。
/// Test that a principal stashed by middleware is visible to the handler of the same request.
///
/// - 手段：`View /test/state` 并带 `X-Scene-Principal: alice`；中间件把 `alice` 写进
///   `ctx.request_state_mut()`，端点从 `ctx.request_state()` 读回并写进响应头。
/// - 判断：状态 `200 Ok`，响应头 `X-Scene-Principal` 等于 `alice`。
///
/// - Means: `View /test/state` with `X-Scene-Principal: alice`; the middleware stashes it
///   into `ctx.request_state_mut()` and the endpoint echoes `ctx.request_state()` back.
/// - Verdict: status is `200 Ok` and the echoed header is `alice`.
#[compio::test]
async fn middleware_stashes_principal_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async {
        client
            .client_()
            .view("/test/state")
            .header(K_HDR_PRINCIPAL, HeaderVal::from_text("alice"))
            .send_async()
            .await
    })
    .await
    .expect("发送 View /test/state");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");
    let echoed = resp
        .headers()
        .and_then(|headers| headers.try_get(HeaderKey::from_text(K_HDR_PRINCIPAL)))
        .and_then(HeaderVal::as_text);
    assert_eq!(echoed, Some("alice"), "处理器应读到中间件写入的主体");
    // 组标记来自 router 的 `with_group_metadata`，端点自身没有声明——证明继承生效。
    let group = resp
        .headers()
        .and_then(|headers| headers.try_get(HeaderKey::from_text("X-Scene-Group")))
        .and_then(HeaderVal::as_text);
    assert_eq!(group, Some("scene"), "端点应继承 router 的组默认元数据");
}

/// 测试没有主体头时请求级状态是匿名的。
/// Test that the per-request state is anonymous when no principal header is sent.
///
/// - 手段：`View /test/state` 不带 `X-Scene-Principal`。
/// - 判断：状态 `200 Ok`，响应头 `X-Scene-Principal` 等于 `anonymous`——中间件写了
///   `None`，处理器按约定降级。
///
/// - Means: `View /test/state` without the principal header.
/// - Verdict: status is `200 Ok` and the echoed header is `anonymous`.
#[compio::test]
async fn missing_principal_is_anonymous_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/state").send_async().await })
        .await
        .expect("发送 View /test/state");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");
    let echoed = resp
        .headers()
        .and_then(|headers| headers.try_get(HeaderKey::from_text(K_HDR_PRINCIPAL)))
        .and_then(HeaderVal::as_text);
    assert_eq!(echoed, Some("anonymous"), "无主体头时应降级为匿名");
}
