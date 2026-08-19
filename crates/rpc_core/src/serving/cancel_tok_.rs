use std::{
    future,
    ops::Try,
    sync::Arc,
};

use abs_cancel::TrCancellationToken;
use buffex::x_deps::abs_cancel;

#[derive(Clone, Debug)]
pub struct ServiceCancelToken();

impl ServiceCancelToken {
    /// for test purpose only
    pub(crate) const fn dummy_new() -> Self {
        ServiceCancelToken()
    }

    pub fn can_be_cancelled(&self) -> bool {
        todo!()
    }

    pub fn is_cancelled(&self) -> bool {
        todo!()
    }

    pub fn cancellation(&mut self) -> impl IntoFuture {
        // this is just a dummy implementation
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

#[derive(Debug)]
struct SvcCanTokInner {

}
