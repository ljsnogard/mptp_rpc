//! `rpc_core_v1::preludes` 的可用性测试。
//!
//! 只验证一次 glob 就能拿到协议、错误状态与取消令牌入口，不建立连接。
//!
//! # Availability test for `rpc_core_v1::preludes`
//!
//! A single glob must expose the protocol result, status and cancellation entry points;
//! no connection is opened.

use rpc_core_v1::preludes::*;

/// 测试 prelude 同时转出结果、状态与取消令牌。
/// Test that the prelude exports the result, status and cancellation types together.
///
/// - 手段：从 `preludes` glob 导入后，构造 `RpcResult`、`Status` 与
///   `NonCancellableToken`。
/// - 判断：三者都能在类型 / 值层面使用，说明 re-export 路径无冲突。
///
/// - Means: import via the prelude glob and construct `RpcResult`, `Status` and
///   `NonCancellableToken`.
/// - Verdict: all three are usable, so the re-export set has no conflicting names.
#[test]
fn imports_common_rpc_items_() {
    let _: RpcResult<()> = Result::Ok(());
    assert_eq!(Status::Ok.inner(), 200u16);
    let _ = NonCancellableToken::new();
}
