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

use alloc::boxed::Box;
use core::alloc::Allocator;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use abs_str::str_alloc::String as AbsString;
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    HeaderKey, HeaderVal, PageSize, ResponsePrefix, ServeError, SessionAlloc,
    serving::{K_TMP_ALLOW_HEADER_KEY, ServingSession, TrServeHandler, config::TrConfig},
};

use super::{
    MatchOutcome, MptpContext, RadixRouter, RouteParams, TrRouteMatcher, router_::ErrorHook,
};

/// 请求级 context arena 的页大小。
///
/// handler / 中间件 future 由编译器生成，常见多 KB；比会话 arena 大一档可以避免大多数
/// future 回退到 `Global`。单块仍然超过该页时由 `arena_future_` 兜底。
const K_CONTEXT_PAGE_SIZE: PageSize = PageSize::P16K;

impl<C, S, R, A> TrServeHandler<C> for RadixRouter<C, S, R, A>
where
    C: TrConfig + 'static,
    S: Send + Sync + 'static,
    R: Default + 'static,
    A: Allocator + Send + Sync + 'static,
{
    type Err = ServeError;

    type HandleAsync<'f>
        = RouterHandleAsync<'f, 'f, C, S, R, A>
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
async fn router_handle_async_<'f, C, S, R, A, K>(
    router: &'f RadixRouter<C, S, R, A>,
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
    // 请求级 arena：location、路由参数所借用的文本、请求级运行期状态以及 handler /
    // 中间件 future 都从它分配；context 持有句柄，请求结束整体释放。
    //
    // 页大小比会话 arena 大一档：handler / 中间件 future 是编译器生成的 async 块，
    // 常见多 KB；放不下的单块会由 `arena_future_` 回退到 `Global`，不会 abort。
    let arena = SessionAlloc::new(K_CONTEXT_PAGE_SIZE).expect("K_CONTEXT_PAGE_SIZE 应当受支持");
    // 克隆 location：匹配结果的 params 借用 location，而调用 handler 需要 `&mut session`；
    // 两者不能同时借 session，因此把 location 放到独立的堆上（带分配器的 String）。
    let location = AbsString::from_str_in(session.location(), arena.clone());
    // 请求级运行期状态：中间件 stash、处理器读；随本次请求结束整体释放。
    let mut request_state = Box::new_in(R::default(), arena.clone());

    match router.match_route(method, &location) {
        MatchOutcome::Found {
            value: endpoint,
            params,
        } => {
            let cancel = router.cancel_token();
            let mut ctx = MptpContext::new_(
                arena,
                method,
                &location,
                params,
                session,
                router.state(),
                &mut *request_state,
                cancel,
            );
            let result = router.pipeline().run(endpoint, &mut ctx).await;
            settle_reply_(&mut ctx, result, router.error_hook_()).await
        }
        MatchOutcome::MethodNotAllowed { allowed } => {
            let mut prefix =
                ResponsePrefix::new(Status::MethodNotAllowed, session.allocator().clone());
            prefix.headers_mut().set(
                HeaderKey::from_num(K_TMP_ALLOW_HEADER_KEY),
                HeaderVal::from_text(allowed.to_allow_value(), session.allocator().clone()),
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
                arena,
                method,
                &location,
                params,
                session,
                router.state(),
                &mut *request_state,
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
