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
use abs_mptp::spec::access_method::AccessMethod;
use abs_mptp::spec::Status;
use abs_str::str_alloc::String as AbsString;
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::serving::{K_TMP_ALLOW_HEADER_KEY, ServingSession, TrServeHandler};
use mptp_smux::{HeaderVal, ResponsePrefix, ServeError};

use super::{MatchOutcome, MptpContext, RadixRouter, RouteParams, TrRouteMatcher};

impl<C, A, S> TrServeHandler<C> for RadixRouter<C, A, S>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
    S: Send + Sync + 'static,
{
    type Err = ServeError;

    type HandleAsync<'f>
        = RouterHandleAsync<'f, 'f, C, A, S>
    where
        Self: 'f,
        C: 'f;

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
async fn router_handle_async_<'f, C, A, S, K>(
    router: &'f RadixRouter<C, A, S>,
    method: AccessMethod,
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
    S: Send + Sync + 'static,
    K: TrCancellationToken,
{
    // 克隆 location：匹配结果的 params 借用 location，而调用 handler 需要 `&mut session`；
    // 两者不能同时借 session，因此把 location 放到独立的堆上（带分配器的 String）。
    let location = AbsString::from_str_in(session.location(), Global);

    match router.match_route(method, &location) {
        MatchOutcome::Found { value: endpoint, params } => {
            let mut ctx = MptpContext::new_(method, &location, params, session, router.state());
            router.pipeline().run(endpoint, &mut ctx).await
        }
        MatchOutcome::MethodNotAllowed { allowed } => {
            let mut prefix = ResponsePrefix::new(Status::MethodNotAllowed);
            prefix
                .headers_mut()
                .set(K_TMP_ALLOW_HEADER_KEY, HeaderVal::from_text(allowed.to_allow_value()));
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
            let mut ctx = MptpContext::new_(method, &location, params, session, router.state());
            router.root().handle(&mut ctx).await
        }
    }
}
