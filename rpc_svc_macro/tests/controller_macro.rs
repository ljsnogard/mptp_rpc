//! `#[rpc(interface = "...")]` controller 形态的编译与行为测试。
//!
//! 这些用例验证宏能把 `async fn` 端点方法重写成 `gen_may_cancel_future` step 函数，并验证
//! 生成的 step 函数可以编译、可以驱动。它们不建立真实连接。
//!
//! # Compile and behavior tests for `#[rpc(interface = "...")]`
//!
//! These tests verify that the macro rewrites an `async fn` endpoint into a
//! `gen_may_cancel_future` step function and that the generated step function compiles and
//! runs. No real connection is opened.

#![feature(impl_trait_in_assoc_type)]
#![recursion_limit = "256"]

use core::future::Future;
use core::pin::pin;
use core::task::{Context, Poll, Waker};

use abs_cancel::{NonCancellableToken, TrMayCancel};
use mptp_serving::TrEndpointHandler;
use rpc_core_v1::serving::TrReplyError;
use rpc_core_v1::x_deps::abs_mptp::spec::Status;
use rpc_svc_macro::rpc;

/// 加入场景的请求体。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct JoinRequest {
    /// 期望显示名。
    pub name: String,
}

/// 行星描述。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct PlanetDescriptor {
    /// 行星标识。
    pub id: u64,

    /// 显示名。
    pub name: String,
}

/// 业务错误（示意；不要求实现 `Error`）。
#[derive(Debug)]
pub struct SceneError {
    /// 错误描述。
    pub detail_: &'static str,
}

impl TrReplyError for SceneError {
    fn status(&self) -> Status {
        Status::InternalServerError
    }

    fn message(&self) -> String {
        self.detail_.to_string()
    }
}

/// 场景服务：一个 controller。
pub struct SceneService {
    /// 下一个行星标识。
    pub next_id: u64,
}

#[rpc(interface = "scene")]
impl SceneService {
    /// 加入场景。
    #[rpc(call = "join")]
    #[allow(dead_code)] // 测试里走 step 函数，原方法保留但本测试不直接调用。
    async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> {
        // 引用 `self` 字段，验证宏把方法体里的 `self` 重写成了 `svc`。
        let _ = self.next_id;
        Ok(PlanetDescriptor {
            id: 1,
            name: req.name,
        })
    }
}

/// 测试 controller 形态生成的 step 函数能编译并被正确驱动。
/// Test that the generated step function compiles and runs after the `self → svc` rewrite.
///
/// - 手段：引用生成的 step 函数，用 `NonCancellableToken` 驱动一次。
/// - 判断：返回 `Ok` 且行星名与请求一致——证明方法体里对 `self` 字段的引用被正确重写成
///   `svc`。
///
/// - Means: reference the generated step function and drive it once with a non-cancellable
///   token.
/// - Verdict: it returns `Ok` and the planet name matches the request, showing `self` was
///   rewritten to `svc`.
#[test]
fn controller_step_fn_runs_() {
    let svc = SceneService { next_id: 7 };
    let req = JoinRequest {
        name: "terra".to_string(),
    };
    let result = block_on_(
        __rpc_controller_SceneService::SceneService_joinAsync::new(&svc, req)
            .may_cancel_with(NonCancellableToken::new())
            .into_future(),
    );
    let desc = result.expect("join 应当成功");
    assert_eq!(desc.id, 1u64);
    assert_eq!(desc.name, "terra");
}

/// 用立即完成的执行器驱动纯内存 future。
fn block_on_<TyFut>(future: TyFut) -> TyFut::Output
where
    TyFut: Future,
{
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

/// 编译期验证生成的端点处理器实现了 `TrEndpointHandler`。
/// Compile-time check that the generated endpoint handler implements `TrEndpointHandler`.
///
/// - 手段：用只要求 `TrEndpointHandler<C, S>` 的泛型断言实例化生成的处理器类型。
/// - 判断：编译通过。
///
/// - Means: instantiate the generated handler through a generic assertion requiring
///   `TrEndpointHandler<C, S>`.
/// - Verdict: it compiles.
#[allow(dead_code)]
fn handler_implements_endpoint_handler_<C, S>()
where
    C: mptp_serving::x_deps::mptp_smux::serving::config::TrConfig,
{
    fn assert_handler<C, S, H>()
    where
        C: mptp_serving::x_deps::mptp_smux::serving::config::TrConfig,
        H: TrEndpointHandler<C, S>,
    {
    }

    assert_handler::<C, S, __rpc_controller_SceneService::SceneService_join>();
}
