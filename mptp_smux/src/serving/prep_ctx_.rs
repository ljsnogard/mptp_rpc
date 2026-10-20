//! 服务端侧的准备环境。

use abs_mptp::x_deps::abs_smux;
use abs_smux::conf::TrMuxConfig;

use crate::ring_prepare_::{PeerRole, TrPrepareCtx};

/// 服务端侧的准备环境。
///
/// # 谁来构造
///
/// `mptp_smux` 构造，工厂实现者只读：框架自己的 accept 循环从句柄读出本条子流的两个 dock；
/// 「自己写 accept 流程」的调用者则用 [`ServingPrepCtx::new`] 自己交出一对 dock。
///
/// # 为什么两端各有一个
///
/// 与客户端那份**刻意不共享**：服务端这份将来要加「会话上下文 / accept 序号」这类信息时，
/// 只改本类型与 `server_` 里的构造点，客户端那份以及它的所有工厂都不必跟着动。
pub struct ServingPrepCtx<'a, C>
where
    C: TrMuxConfig,
{
    /// 本条子流身份里的本端 dock（服务端监听的那个）。
    local_dock_: &'a C::Dock,

    /// 本条子流身份里的远端 dock。
    remote_dock_: &'a C::Dock,
}

impl<'a, C> ServingPrepCtx<'a, C>
where
    C: TrMuxConfig,
{
    /// 由「本端 dock + 远端 dock」构造。
    ///
    /// 两个 dock 都必须由调用者当场掌握：框架从句柄读，自写 accept 流程的调用者同样从句柄读
    /// （`TrChannelHalf` 按值给出两个 dock，因此调用者要先把它们存成局部变量再借）。
    pub const fn new(local_dock: &'a C::Dock, remote_dock: &'a C::Dock) -> Self {
        ServingPrepCtx {
            local_dock_: local_dock,
            remote_dock_: remote_dock,
        }
    }
}

impl<C> TrPrepareCtx for ServingPrepCtx<'_, C>
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

    /// 服务端恒为响应方：这条子流是对端 `open_channel_async` 出来、由本端裁决的。
    fn role(&self) -> PeerRole {
        PeerRole::Responder
    }
}
