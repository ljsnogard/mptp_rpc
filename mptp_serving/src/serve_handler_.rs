//! 分层 router 作为单一 [`TrServeHandler`] 的胶水。
//!
//! `mptp_smux` 的 `MptpServlet` 只认一个 [`TrServeHandler`]。本模块让 [`RadixRouter`]
//! 实现该 trait：匹配 → 调用端点 handler；`405` 与根兜底都在这里收口，收尾仍由 servlet
//! 兜底。
//!
//! # `serve_handler_`: the router as a single handler
//!
//! This makes [`RadixRouter`] a [`TrServeHandler`] so it can be handed to `MptpServlet`.
//! Matching, `405` and the root fallback are handled here; teardown stays with the servlet.

use alloc::alloc::Global;
use core::alloc::Allocator;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::spec::Status;
use abs_mptp::spec::access_method::AccessMethod;
use abs_str::str_alloc::String as AbsString;
use bumpalo::Bump;
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::{K_TMP_ALLOW_HEADER_KEY, ServingSession, TrServeHandler};
use mptp_smux::{HeaderVal, ResponsePrefix, ServeError};

use super::router_::ErrorHook;
use super::{MatchOutcome, MptpContext, RadixRouter, RouteParams, TrRouteMatcher};

impl<C, A, S, R> TrServeHandler<C> for RadixRouter<C, A, S, R>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync + 'static,
    S: Send + Sync + 'static,
    R: Default + 'static,
{
    type Err = ServeError;

    type HandleAsync<'f>
        = RouterHandleAsync<'f, 'f, C, A, S, R>
    where
        Self: 'f,
        C: 'f,
        R: 'f;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<C>,
    ) -> Self::HandleAsync<'f> {
        RouterHandleAsync::new(self, method, session)
    }
}

/// [`RadixRouter`] 作为 [`TrServeHandler`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(RouterHandle, pub, new(pub(crate)))]
async fn router_handle_async_<'f, C, A, S, R, K>(
    router: &'f RadixRouter<C, A, S, R>,
    method: AccessMethod,
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync + 'static,
    S: Send + Sync + 'static,
    R: Default + 'static,
    K: TrCancellationToken,
{
    // 克隆 location：匹配结果的 params 借用 location，而调用 handler 需要 `&mut session`；
    // 两者不能同时借 session，因此把 location 放到独立的堆上（带分配器的 String）。
    let location = AbsString::from_str_in(session.location(), Global);
    // 请求级 arena：handler future 从它分配，随本次请求结束整体释放。
    let bump = Bump::new();
    // 请求级运行期状态：中间件 stash、处理器读；随本次请求结束整体释放。
    let request_state = bump.alloc(R::default());

    match router.match_route(method, &location) {
        MatchOutcome::Found {
            value: endpoint,
            params,
        } => {
            let cancel = router.cancel_token();
            let mut ctx = MptpContext::new_(
                &bump,
                method,
                &location,
                params,
                session,
                router.state(),
                request_state,
                cancel,
            );
            let result = router.pipeline().run(endpoint, &mut ctx).await;
            settle_reply_(&mut ctx, result, router.error_hook_()).await
        }
        MatchOutcome::MethodNotAllowed { allowed } => {
            let mut prefix = ResponsePrefix::new(Status::MethodNotAllowed);
            prefix.headers_mut().set(
                K_TMP_ALLOW_HEADER_KEY,
                HeaderVal::from_text(allowed.to_allow_value()),
            );
            session
                .response_prefix(prefix)
                .no_body_async()
                .may_cancel_with(cancel.child_token())
                .await?;
            let _ = session
                .finish_async()
                .may_cancel_with(cancel.child_token())
                .await?;
            Result::Ok(())
        }
        MatchOutcome::Root => {
            let params = RouteParams::empty();
            let cancel = router.cancel_token();
            let mut ctx = MptpContext::new_(
                &bump,
                method,
                &location,
                params,
                session,
                router.state(),
                request_state,
                cancel,
            );
            let result = router.root().handle(&mut ctx).await;
            settle_reply_(&mut ctx, result, router.error_hook_()).await
        }
    }
}

/// 收尾一次请求的回复：错误上报 + 未回复兜底 500。
///
/// 设计稿 §7④/⑤：处理器返回 `Ok` 但没写回复、或返回错误但还没写回复时，框架补一个
/// `500 Internal Server Error` 的无体回复；错误本身只经观测回调上报，**不**写到线上。
/// 已经写出回复前缀的请求不再改写状态码（回复已经开始，改不了）。
async fn settle_reply_<'f, C, S, R>(
    ctx: &mut MptpContext<'f, C, S, R>,
    result: Result<(), ServeError>,
    hook: Option<&ErrorHook>,
) -> Result<(), ServeError>
where
    C: TrConfig + 'static,
    S: Send + Sync + 'static,
    R: 'static,
{
    if let Result::Err(err) = result
        && let Option::Some(hook) = hook
    {
        hook(&err);
    }
    if !ctx.reply_written() {
        ctx.session_mut()
            .response(Status::InternalServerError)
            .no_body_async()
            .await?;
    }
    Result::Ok(())
}
