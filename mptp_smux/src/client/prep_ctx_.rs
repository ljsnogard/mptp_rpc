//! 客户端侧的准备环境。

use abs_smux::conf::TrMuxConfig;

use crate::{
    ring_prepare_::{PeerRole, TrPrepareCtx},
    x_deps::abs_smux,
};

/// 客户端侧的准备环境。
///
/// # 谁来构造
///
/// `mptp_smux` 构造，工厂实现者只读：框架自己的裁决点从 `bind_async` 的结果读出本端 dock，
/// 再带上本次请求的目标 dock；「自己写 accept 流程」的调用者则用 [`ClientPrepCtx::new`]
/// 自己交出一对 dock。
///
/// # 为什么两端各有一个
///
/// 客户端与服务端**刻意不共享**环境的类型（与两侧的配置、分配约定同一个理由）：客户端这份
/// 将来要加「请求方法 / 路径」这类信息时，只改本类型与 `client_` 里的构造点，服务端那份以及
/// 它的所有工厂都不必跟着动。
pub struct ClientPrepCtx<'a, C>
where
    C: TrMuxConfig,
{
    /// 本端子流身份里的本端 dock。
    local_dock_: &'a C::Dock,

    /// 目标（对端）dock。
    remote_dock_: &'a C::Dock,
}

impl<'a, C> ClientPrepCtx<'a, C>
where
    C: TrMuxConfig,
{
    /// 由「本端 dock + 目标 dock」构造。
    ///
    /// 两个 dock 都必须由调用者当场掌握：框架从 `bind_async` 的结果读，自写 accept 流程的
    /// 调用者从自己的 bind 结果读。
    pub const fn new(local_dock: &'a C::Dock, remote_dock: &'a C::Dock) -> Self {
        ClientPrepCtx {
            local_dock_: local_dock,
            remote_dock_: remote_dock,
        }
    }
}

impl<C> TrPrepareCtx for ClientPrepCtx<'_, C>
where
    C: TrMuxConfig,
{
    type Dock = C::Dock;

    fn local_dock(&self) -> &Self::Dock {
        self.local_dock_
    }

    fn remote_dock(&self) -> &Self::Dock {
        self.remote_dock_
    }

    /// 客户端恒为发起方：这条子流是它自己 `open_channel_async` 出来的。
    fn role(&self) -> PeerRole {
        PeerRole::Initiator
    }
}
