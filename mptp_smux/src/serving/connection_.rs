//! 连接作用域 owner：一条 MPTP 连接上的「binding + servlet + 状态」。
//!
//! [`MptpServlet`] 只持有 handler，不持有 binding、也不持有 per-connection 状态；「一条
//! 连接 = 一个 binding + 一个 servlet + 一份状态」此前是**调用约定**。本模块把它升级为
//! **类型不变量**：一个 [`MptpConnection`] 拥有自己的 `DockBinding`、`MptpServlet` 与
//! 连接状态 `Arc<S>`，因此不可能被误用到另一条连接。
//!
//! 状态 `S` 是每连接一份的共享状态（join 身份、订阅表、保活……），由本对象持有，处理器 /
//! 中间件经 [`crate::x_deps::abs_mptp`] 之上的 `MptpContext` 拿 `&S`。
//!
//! # `connection_`: the per-connection owner
//!
//! `MptpServlet` holds only a handler; "one connection = one binding + one servlet + one
//! state" was a calling convention. `MptpConnection` owns its `DockBinding`, `MptpServlet`
//! and `Arc<S>`, so it cannot be reused across connections.

use alloc::sync::Arc;

use abs_buff::{TrBuffRead, buffer::TrConsumerState};
use abs_smux::chan::TrChannelHalf;

use crate::x_deps::{abs_buff, abs_smux};

use super::config::{ChannelHandle, ChannelRx, DockBinding, MuxCfg, TrConfig};
use super::handler_::TrServeHandler;
use super::servlet_::{MptpServlet, ServeListenerAsync};

/// 一条 MPTP 连接的 owner。
///
/// `C` 是服务端配置类型，`H` 是单一 handler，`S` 是连接作用域状态（缺省 `()`）。
pub struct MptpConnection<C, H, S = ()>
where
    C: TrConfig,
    H: TrServeHandler<C>,
{
    /// 本连接的 serving dock 绑定；不可复用到别的连接。
    binding_: DockBinding<C>,

    /// 本连接的 servlet（持有单一 handler）。
    servlet_: MptpServlet<C, H>,

    /// 本连接的状态；跨 session 共享只读，可变性由 `S` 内部处理。
    state_: Arc<S>,
}

impl<C, H, S> MptpConnection<C, H, S>
where
    C: TrConfig,
    H: TrServeHandler<C>,
    S: Send + Sync + 'static,
{
    /// 由「binding + handler + 状态」构造。
    ///
    /// 调用方负责先把连接绑定到默认 serving dock（`bind_async`），再把 binding 交进来；
    /// `state` 是共享状态，通常 `Arc::clone` 一份给 handler（如 `mptp_serving::RadixRouter`
    /// 的 `new_with_state`）。
    pub fn new(binding: DockBinding<C>, handler: H, state: Arc<S>) -> Self {
        MptpConnection {
            binding_: binding,
            servlet_: MptpServlet::new(handler),
            state_: state,
        }
    }

    /// 本连接的 servlet。
    pub fn servlet(&self) -> &MptpServlet<C, H> {
        &self.servlet_
    }

    /// 连接作用域状态（共享借用）。
    pub fn state(&self) -> &S {
        &self.state_
    }

    /// 在当前 binding 上循环 accept 并 inline 处理（单线程路径）。
    ///
    /// 需要并发时用 [`MptpConnection::servlet`] 的 `accept_async` + 调用者自己的 spawner。
    pub fn serve_async<'f>(
        &'f mut self,
        reserve: usize,
    ) -> ServeListenerAsync<'f, 'f, C, H>
    where
        C: 'f,
        H: 'f,
        ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        self.servlet_.serve_listener_async(&mut self.binding_, reserve)
    }
}
