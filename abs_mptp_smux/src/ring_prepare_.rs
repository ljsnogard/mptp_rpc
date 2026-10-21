use core::{
    alloc::AllocatorClone,
    mem::MaybeUninit,
};

use abs_cancel::TrMayCancel;
use abs_smux::{
    chan::TrPrepareRing,
    dock::TrDock,
};
use abs_mm::res_man::TrUnique;

use crate::x_deps::{abs_cancel, abs_mm, abs_smux};

/// 本端在「开一条 channel」这件事里扮演的角色。
///
/// 两个入口的角色是静态可知的：客户端走
/// [`open_channel_async`](abs_smux::conn::TrDockBinding::open_channel_async)，恒为
/// [`Initiator`](PeerRole::Initiator)；服务端走
/// [`income_async`](abs_smux::conn::TrChannelListener::income_async)，恒为
/// [`Responder`](PeerRole::Responder)。
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum PeerRole {
    /// 本端主动发起这条子流。
    Initiator,

    /// 本端是响应方，这条子流由对端发起。
    Responder,
}

/// 构造工厂时交给它的**环境**抽象。
///
/// # 这是本契约唯一的变化承受点
///
/// 它只描述「开这条 channel 时能知道什么」。要加信息（请求方法、会话上下文、序号……）就
/// 给本 trait 加方法、给实现类型加字段——**不影响** [`TrPrepFactory`] 的形状，也不影响
/// 另一端的实现类型与它的所有使用者。
///
/// # 谁实现它
///
/// `mptp_smux` 自己：客户端侧是 [`ClientPrepCtx`](crate::client::ClientPrepCtx)，服务端侧是
/// [`ServingPrepCtx`](crate::serving::ServingPrepCtx)。工厂实现者只读环境，不实现它，也不
/// 构造它。
pub trait TrPrepareCtx {
    /// 本端子流身份里的 dock 类型。
    type Dock: 'static + TrDock;

    /// 本端 dock。
    fn local_dock(&self) -> &Self::Dock;

    /// 远端 dock。
    fn remote_dock(&self) -> &Self::Dock;

    /// 本端这次建流的角色。
    fn role(&self) -> PeerRole;
}

/// 由「环境」造出「准备器」的工厂，每次开启一条 channel 用一次。
///
/// 两端共用同一份契约，但**各自绑定各自的环境类型**：客户端工厂写
/// `type Ctx<'a> = ClientPrepCtx<'a, MyCfg>;`，服务端工厂写
/// `type Ctx<'a> = ServingPrepCtx<'a, MyCfg>;`。一个工厂类型只服务一端——这是类型系统的
/// 必然（一个 GAT 只能是一个类型），也正好与「两端配置刻意不共享」一致。
///
/// # 默认与覆盖
///
/// - **默认**：策略以**类型**的形式声明在配置里
///   （[`TrConfig::DefaultPrepFactory`](crate::client::config::TrConfig::DefaultPrepFactory) /
///   [`TrConfig::DefaultPrepFactory`](crate::serving::config::TrConfig::DefaultPrepFactory)），
///   没有实例需要提前注入，因此 `Client::new` / `Server::new` 的签名不必带上策略。两端都走
///   这条路径。
/// - **覆盖**：只有客户端有——`Client::request_with_async` 让调用者**当场交出准备器**
///   （不是工厂，因此那条路径连环境都不需要构造）。服务端没有对应入口：一次
///   `serve_listener` 要接受 N 条子流，而一个准备器交出即被消费，第二条就无物可交；要换
///   服务端的内存来源就换配置里的工厂类型。
///
/// # Examples
///
/// 以客户端工厂为例（服务端只是把环境类型换成 `ServingPrepCtx`）。两个要点：`with_ctx`
/// **没有接收者**，环境是它的参数；工厂实例短命，`prepare_async` 因此可以放心用 `&mut self`。
///
/// 本仓纪律要求对外异步产物可取消，所以 `prepare_async` 的真实实现必须用
/// `gen_may_cancel_future` 包装一个 `async fn`——下面把这段包装也写全。本示例不参与
/// doctest：它要拖进宏与取消令牌，而那两样东西只会把「形状」讲得更糊。
///
/// ```ignore
/// use core::mem::MaybeUninit;
///
/// use abs_buff::gen_may_cancel_future;
/// use abs_cancel::{TrCancellationToken, TrMayCancel};
/// use mptp_smux::client::ClientPrepCtx;
/// use mptp_smux::x_deps::abs_mm::CoreAlloc;
/// use mptp_smux::x_deps::mm_ptr::Owned;
/// use mptp_smux::{PeerRole, RingBuffs, TrPrepFactory};
///
/// /// 每次都为一条子流交付两块 ring：发起方 4 KiB、响应方 2 KiB。
/// struct RoleSized {
///     cap_: usize,
/// }
///
/// impl TrPrepFactory for RoleSized {
///     type Ctx<'a> = ClientPrepCtx<'a, MyMuxCfg>;
///     type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;
///     type Prep = RingBuffs<Self::RingBuff>;
///     type Err = core::convert::Infallible;
///
///     fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
///         // 环境参与的是**构造**：容量在这一刻就定下来了。
///         let cap_ = match ctx.role() {
///             PeerRole::Initiator => 4096usize,
///             PeerRole::Responder => 2048usize,
///         };
///         RoleSized { cap_ }
///     }
///
///     fn prepare_async<'f>(
///         &'f mut self,
///     ) -> impl TrMayCancel<'f, MayCancelOutput = Result<Self::Prep, Self::Err>>
///     where
///         Self: 'f,
///     {
///         PrepareRoleSizedAsync::new(self)
///     }
/// }
///
/// /// `prepare_async` 的 step 函数：宏补齐可取消的那套生成物。
/// #[gen_may_cancel_future(PrepareRoleSized, pub, new(pub(crate)))]
/// async fn prepare_role_sized_async_<'f, TyTok>(
///     factory: &'f mut RoleSized,
///     cancel: TyTok,
/// ) -> Result<RingBuffs<Owned<[MaybeUninit<u8>], CoreAlloc>>, core::convert::Infallible>
/// where
///     TyTok: TrCancellationToken,
/// {
///     let _ = cancel;
///     let cap = factory.cap_;
///     Result::Ok(RingBuffs::new(
///         Owned::new_uninit_slice(cap, CoreAlloc),
///         Owned::new_uninit_slice(cap, CoreAlloc),
///     ))
/// }
/// ```
pub trait TrPrepFactory {
    /// 本工厂接受的环境。
    ///
    /// **具体类型由 `mptp_smux` 决定并构造**：客户端侧是
    /// [`ClientPrepCtx`](crate::client::ClientPrepCtx)，服务端侧是
    /// [`ServingPrepCtx`](crate::serving::ServingPrepCtx)。配置层用一条高阶等式把本关联类型
    /// 钉死到其中之一，所以工厂实现里写下的那个类型不是自由选择，而是与它被使用的那一端绑定。
    type Ctx<'a>: TrPrepareCtx;

    /// 本工厂交出的 ring 存储智能指针类型。
    ///
    /// 数据元素类型写死 `u8`：`mptp_smux` 的配置层已把 `Data` 收紧为 `u8`，不必再从配置
    /// 参数里问一次。
    type RingBuff: 'static + Send + Sync + TrUnique<Item = [MaybeUninit<u8>], Alloc: AllocatorClone>;

    /// 本工厂交出的**准备器**：实现上游 [`TrPrepareRing`]，由 `accept_async` 拿去建环。
    ///
    /// 交付物的形状由实现者决定。只要两块现成内存时，用 [`RingBuffs`] 即可。
    type Prep: 'static + TrPrepareRing<Self::RingBuff, u8>;

    /// 准备失败（内存池耗尽、配额不足……）。
    ///
    /// 它必须能被转成给调用者看的错误，因此要求是 `Error`（带 `Display`）。
    type Err: core::error::Error;

    /// 由环境构造工厂。**无接收者**：环境按参数给，所以这条路在类型层面就能拿到上下文。
    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self
    where
        Self: Sized;

    type GetPreparedAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Prep, Self::Err>>
    where
        Self: 'f;

    /// 生产**一份**准备器，供一条子流使用。
    ///
    /// 每次裁决调用一次，实现必须交出**新的**一份：上游 `accept_async` 会拿走所有权，
    /// 同一个准备器无法交给第二条子流。
    fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f>;
}
