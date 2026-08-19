use std::{future, ops::Try};

use abs_cancel::TrCancellationToken;
use buffex::x_deps::abs_cancel;

/// 服务端内部使用的取消令牌。
///
/// 当前实现是一个“不可取消”的占位令牌，主要让 `HandlerChain` 可以在还没有
/// 接入真实连接级取消机制前正常工作。后续可以替换为基于共享状态的取消令牌，
/// 例如由连接断开事件驱动取消。
#[derive(Clone, Debug)]
pub struct ServiceCancelToken();

impl ServiceCancelToken {
    /// for test purpose only
    pub(crate) const fn dummy_new() -> Self {
        ServiceCancelToken()
    }

    pub fn can_be_cancelled(&self) -> bool {
        false
    }

    pub fn is_cancelled(&self) -> bool {
        false
    }

    pub fn cancellation(&mut self) -> impl IntoFuture {
        // 当前不可取消，因此返回一个永远不会完成的 future。
        future::pending::<()>()
    }
}

impl TrCancellationToken for ServiceCancelToken {
    #[inline]
    fn can_be_cancelled(&self) -> bool {
        ServiceCancelToken::can_be_cancelled(self)
    }

    fn is_cancelled(&self) -> bool {
        ServiceCancelToken::is_cancelled(self)
    }

    fn cancellation(&mut self) -> impl IntoFuture {
        ServiceCancelToken::cancellation(self)
    }

    fn try_spawn_child_token(&mut self) -> impl Try<Output: TrCancellationToken> {
        Option::Some(self.clone())
    }
}
