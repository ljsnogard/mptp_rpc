#![feature(impl_trait_in_assoc_type)]

//! 同路径多方法端点（Phase B / G4）的集成测试。
//!
//! 夹具在 `/test/multi` 上同时挂了 `VIEW` 与 `PULL` 两个端点：前者写标记头，后者写一个
//! 后缀事件。测试验证「匹配按方法选端点」与「405 的 `Allow` 取并集」这两条契约。
//!
//! # `multi_method_`: same-path multi-method endpoints
//!
//! The fixture registers both `VIEW` and `PULL` endpoints on `/test/multi`. The tests check
//! method-based endpoint selection and the union semantics of the 405 `Allow` header.

mod common;

use abs_mptp::spec::Status;
use common::{SceneFixture, SceneTestClient, with_timeout_};
use mptp_smux::{HeaderKey, MethodSet};
use mptp_smux_v1_demo::scene::PlanetEvent;

/// 测试同一路径上的 VIEW 与 PULL 各自命中自己的端点。
/// Test that VIEW and PULL on one path each reach their own endpoint.
///
/// - 手段：先 `View /test/multi`，检查回复头 `X-Multi-Route: view`；再 `Pull /test/multi`，
///   读取后缀流的第一个事件。
/// - 判断：View 为 `200 Ok` 且标记头是 `view`；Pull 解出 `Removed { 9, 1 }`——同一个
///   pattern 上的两个端点不会互相覆盖。
///
/// - Means: `View /test/multi` must reply with `X-Multi-Route: view`; `Pull /test/multi`
///   must stream one event.
/// - Verdict: the view is `200 Ok` with the marker header; the pull decodes `Removed { 9, 1 }`.
#[compio::test]
async fn view_and_pull_share_one_path_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let resp = with_timeout_(async { client.client_().view("/test/multi").send_async().await })
        .await
        .expect("发送 View /test/multi");
    assert_eq!(resp.status(), Status::Ok, "View 应回复 200");
    let marker = resp
        .headers()
        .and_then(|headers| headers.try_get(HeaderKey::from_text("X-Multi-Route")))
        .and_then(|value| value.as_text());
    assert_eq!(marker, Some("view"), "View 应命中 view 端点");

    let mut pull = client.pull_async_("/test/multi").await.expect("发送 Pull");
    let event = with_timeout_(pull.next_event_async_())
        .await
        .expect("读后缀事件");
    match event {
        PlanetEvent::Removed { planet_id, version } => {
            assert_eq!(planet_id, 9u64, "Pull 应命中 pull 端点");
            assert_eq!(version, 1u64);
        }
        other => panic!("期待 Removed，实际是 {other:?}"),
    }
}

/// 测试同路径 405 的 `Allow` 是集合内所有端点方法的并集。
/// Test that the 405 `Allow` header is the union of every endpoint on the path.
///
/// - 手段：对 `/test/multi` 发 `Post`，解析 `Allow` 头文本。
/// - 判断：状态是 `405 Method Not Allowed`，`Allow` 等于 `VIEW` 与 `PULL` 的并集
///   （用 `MethodSet::to_allow_value` 生成期望文本，避免依赖方法枚举顺序）。
///
/// - Means: send `Post` to `/test/multi` and parse the `Allow` header.
/// - Verdict: the status is 405 and `Allow` equals the union of VIEW and PULL.
#[compio::test]
async fn allow_header_is_union_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let (status, allow) = client
        .post_allow_async_("/test/multi")
        .await
        .expect("发送 Post /test/multi");
    assert_eq!(status, Status::MethodNotAllowed, "应回复 405");
    let expected = MethodSet::VIEW.union(MethodSet::PULL).to_allow_value();
    assert_eq!(
        allow.as_deref(),
        Some(expected.as_str()),
        "Allow 应为 VIEW+PULL 的并集"
    );
}
