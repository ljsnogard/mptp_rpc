use core::error;

use abs_cancel::TrMayCancel;

use abs_buff::{
    x_deps::abs_cancel,
    TrBuffTryRead, TrBuffTryWrite,
};

/// 可提供双向双工流复用，并且携带通信双方身份信息的连接
pub trait TrMuxConn {
    type Channel: TrChannel;
    type Id: ?Sized + Eq;
    type Err: error::Error;

    fn local_id(&self) -> Option<&Self::Id>;

    fn remote_id(&self) -> Option<&Self::Id>;

    fn open_channel_async<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<Self::Channel, Self::Err>>;
}

pub trait TrChannel {
    type Tx<'f>: TrBuffTryWrite where Self: 'f;
    type Rx<'f>: TrBuffTryRead where Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>);
}
