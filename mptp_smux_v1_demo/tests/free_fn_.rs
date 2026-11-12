#![feature(impl_trait_in_assoc_type)]

//! 自由函数 + 尾参取消令牌 handler 的端到端集成测试（round 32 的验收补充）。
//!
//! 夹具在 `/test/free-token` 上注册了 `common::free_token`（一个带尾参 `TrCancellationToken`
//! 的自由函数）：客户端发 `Call`，服务端把 `count + 1` 写回。既验证自由函数 handler 的
//! `install_into` 与无状态单元 handler 真实跑通，也验证取消令牌参数没有参与 body 解码。
//!
//! # `free_fn_`: end-to-end test for the free-function + trailing-token handler
//!
//! The fixture registers `common::free_token` on `/test/free-token`; a client `Call` echoes
//! `count + 1`.

mod common;

use abs_mptp::spec::Status;
use common::{SceneFixture, SceneTestClient, with_timeout_};
use mptp_smux_v1_demo::scene::protocol_::{decode_value_async_, encode_to_vec_async_};

/// 测试自由函数 + 尾参取消令牌的端点能被真实请求驱动。
/// Test that a free-function endpoint with a trailing token is served end-to-end.
///
/// - 手段：`Call /test/free-token`，请求体编码 `41u64`；服务端 `free_token` 用
///   `ctx.cancel()` 派生的令牌驱动 step，返回 `count + 1`。
/// - 判断：状态 `200 Ok`，回复体解码为 `42u64`——自由函数 handler 与尾参令牌两者叠加，
///   且令牌没有占用 body 参数位。
///
/// - Means: `Call /test/free-token` with body `41u64`; the server returns `count + 1`.
/// - Verdict: status `200 Ok` and the body decodes to `42u64`.
#[compio::test]
async fn free_fn_with_token_serves_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");

    let body = encode_to_vec_async_(&41u64).await.expect("编码请求体");
    let mut resp = with_timeout_(async {
        client
            .client_()
            .invoke("/test/free-token")
            .body_bytes(&body)
            .send_async()
            .await
    })
    .await
    .expect("发送 Call /test/free-token");
    assert_eq!(resp.status(), Status::Ok, "端点应回复 200");
    let mut view = resp.body_async().await.expect("取回复体");
    let got = decode_value_async_::<u64, _>(&mut view)
        .await
        .expect("解码回复");
    assert_eq!(got, 42u64, "free_token 应把 41 递增为 42");
}
