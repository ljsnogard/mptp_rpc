use core::error;

use abs_buff::{TrBuffTryRead, TrBuffTryWrite, x_deps::abs_cancel};
use abs_cancel::TrMayCancel;
use buffex::x_deps::abs_buff;

/// 可提供双向双工流复用，并且携带通信双方身份信息的连接
pub trait TrMuxConn {
    type Channel: TrChannel;

    /// We always assume the connection is trusted and reliable.
    /// That means ID is known to either side of communication, though the
    /// meaning of different types of ID may not be the same.
    type Id: ?Sized + Eq;

    /// Connection error types defined by the connection provider.
    type Err: error::Error;

    fn local_id(&self) -> Option<&Self::Id>;

    fn remote_id(&self) -> Option<&Self::Id>;

    fn open_channel_async<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = Result<Self::Channel, Self::Err>>;
}

/// A channel is a pair of streams with opposite data flow directions.
pub trait TrChannel {
    type Tx<'f>: TrBuffTryWrite where Self: 'f;
    type Rx<'f>: TrBuffTryRead where Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>);

    /// 关闭当前 channel 的写入方向。
    ///
    /// 对于无请求体 / 无后续 push 的简单 RPC，客户端写完请求头后应调用本方法，
    /// 让服务端能通过 EOF 感知“请求已经完整发送”。默认实现为空操作。
    fn close_write(&mut self) {}
}
