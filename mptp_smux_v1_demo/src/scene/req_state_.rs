//! 场景的请求级运行期状态与写入它的中间件（`mptp_serving` Phase C 的端到端样例）。
//!
//! [`MptpContext`](mptp_serving::MptpContext) 的 `R` 参数承载**本请求的运行期事实**：
//! 与端点元数据（装配期声明）不同，它随请求生灭、可被中间件写、被处理器读。这里的样例是
//! 「鉴权中间件把主体写进 `R`，处理器只读 `R`」——设计稿 §6.4/§6.5 的最小落地。
//!
//! # `req_state_`: per-request runtime state for the scene
//!
//! `MptpContext`'s `R` parameter carries facts about *this request* (as opposed to endpoint
//! metadata, which is an assembly-time declaration). The sample here is the design note's
//! minimal auth flow: a middleware stashes the principal, handlers read it back.

use mptp_serving::{Endpoint, MiddlewareFuture, MptpContext, NextFn, TrMiddleware};
use mptp_smux::{HeaderKey, HeaderVal, serving::config::TrConfig};

/// 请求头：测试 / 演示用的主体名（真实业务应换成令牌解析）。
pub const K_HDR_PRINCIPAL: &str = "X-Scene-Principal";

/// 场景请求级运行期状态：中间件写、处理器读。
///
/// 每请求一份、由框架 [`Default::default()`] 新建；请求结束即丢弃，不跨请求共享，
/// 也不写回端点元数据。
#[derive(Clone, Debug, Default)]
pub struct SceneRequestState {
    /// 本次请求的主体名；`None` 表示匿名请求。
    pub principal: Option<String>,
}

/// 鉴权中间件：把请求头里的主体名写进请求级状态。
///
/// 它只依赖「本端点是否消费主体」这一约定：没有主体头时写 `None` 并放行，由处理器自行
/// 决定匿名请求是拒绝还是降级。
pub struct PrincipalMiddleware;

impl<C, S> TrMiddleware<C, S, SceneRequestState> for PrincipalMiddleware
where
    C: TrConfig,
{
    fn call<'a, 'f>(
        &'a self,
        _endpoint: &'a Endpoint<C, S, SceneRequestState>,
        ctx: &'a mut MptpContext<'f, C, S, SceneRequestState>,
        next: &'a mut NextFn<'a, 'f, C, S, SceneRequestState>,
    ) -> MiddlewareFuture<'a> {
        let arena = ctx.arena();
        mptp_serving::box_future_(arena, async move {
            let principal = ctx
                .headers()
                .try_get(HeaderKey::from_text(K_HDR_PRINCIPAL))
                .and_then(HeaderVal::as_text)
                .map(str::to_string);
            ctx.request_state_mut().principal = principal;
            // 洋葱：写入请求级状态后把控制权交给下一环。
            next(ctx).await
        })
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试请求级状态默认是匿名的。
    /// Test that the default per-request state is anonymous.
    ///
    /// - 手段：`SceneRequestState::default()`。
    /// - 判断：`principal` 为 `None`。
    ///
    /// - Means: build `SceneRequestState::default()`.
    /// - Verdict: `principal` is `None`.
    #[test]
    fn default_state_is_anonymous_() {
        let state = SceneRequestState::default();
        assert!(state.principal.is_none());
    }
}
