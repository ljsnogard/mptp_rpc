//! 服务端的单一 handler 契约。
//!
//! `mptp_smux` 在服务端只认**一个** handler：路由、鉴权、方法分派都由它内部决定。
//! 框架只负责把一条已经解码好请求前缀的 [`ServingSession`] 交给它，并在它返回后兜底
//! 写 `500`、收尾。
//!
//! 旧 [`TrReqHandler`](abs_mptp::serving::TrReqHandler) 通过 [`LegacyHandler`] 适配：
//! 它拿走两个半边自行收尾，框架不再插手。
//!
//! # `handler_`: the single server handler
//!
//! `mptp_smux` has exactly one server handler. Routing, authentication and method
//! dispatch live inside it. The framework only hands over a decoded `ServingSession`,
//! writes a fallback `500` when needed, and finishes the channel. The old
//! `TrReqHandler` is adapted through [`LegacyHandler`].

use alloc::string::ToString;

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_mptp::{serving::TrReqHandler, spec::access_method::AccessMethod};
use gen_mcf2::gen_may_cancel_future;

use crate::{
    error_::{ServeError, StateError},
    method_set_::MethodSet,
    x_deps::{abs_cancel, abs_mptp, gen_mcf2},
};
use super::{config::TrConfig, session_::ServingSession};

/// 服务端单一 handler。
pub trait TrServeHandler<C>
where
    C: TrConfig,
{
    /// handler 自己的错误。
    type Err: core::error::Error;

    /// 本 handler 支持的方法集合；缺省为全部。
    ///
    /// 框架在调用 [`Self::handle_async`] 前检查；不在集合内直接写 `405` + `Allow`，
    /// 不进入 handler。
    fn allowed_methods(&self) -> MethodSet {
        MethodSet::ALL
    }

    type HandleAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>
    where
        Self: 'f,
        C: 'f;

    /// 一次请求的处理入口。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<C>,
    ) -> Self::HandleAsync<'f>;
}

/// 旧 [`TrReqHandler`] 的适配器。
///
/// 它把 `ServingSession` 的两个半边、头表与位置按值交给旧 handler；旧 handler 负责
/// 写回复与收尾。框架看到会话的两个半边已经被取走，就不再兜底或 finish。
pub struct LegacyHandler<H> {
    /// 被适配的旧 handler。
    handler_: H,
}

impl<H> LegacyHandler<H> {
    /// 包住一个旧 handler。
    pub const fn new(handler: H) -> Self {
        LegacyHandler { handler_: handler }
    }

    /// 取回被适配的 handler。
    pub const fn handler(&self) -> &H {
        &self.handler_
    }
}

impl<C, H> TrServeHandler<C> for LegacyHandler<H>
where
    C: TrConfig,
    H: TrReqHandler<C>,
{
    type Err = ServeError;

    type HandleAsync<'f>
        = LegacyHandleAsync<'f, 'f, C, H>
    where
        Self: 'f,
        C: 'f;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<C>,
    ) -> Self::HandleAsync<'f> {
        LegacyHandleAsync::new(self, method, session)
    }
}

/// [`LegacyHandler::handle_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(LegacyHandle, pub, new(pub(crate)))]
async fn legacy_handle_async_<'f, C, H, K>(
    handler: &'f LegacyHandler<H>,
    method: AccessMethod,
    session: &'f mut ServingSession<C>,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig,
    H: TrReqHandler<C>,
    K: TrCancellationToken,
{
    let Some((tx, rx, mut headers, location)) = session.take_parts() else {
        return Result::Err(ServeError::State(StateError::ChannelTaken));
    };
    handler
        .handler_
        .handle_async(method, &location, &mut headers, tx, rx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Target {
            detail: err.to_string(),
        })?;
    Result::Ok(())
}
