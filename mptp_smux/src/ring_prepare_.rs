//! 「开一条 channel 时，ring 内存怎么准备」的注入契约。
//!
//! 由三部分组成：
//!
//! 1. [`TrPrepareCtx`]：**环境**的抽象。它只回答「开这条 channel 时，调用者能现场知道
//!    什么」（两个 dock、角色；将来要加请求信息或会话上下文，就给它加方法）。它故意是
//!    **trait** 而不是一个固定 struct：客户端与服务端各有自己的环境类型
//!    （[`ClientPrepCtx`](crate::client::ClientPrepCtx) /
//!    [`ServingPrepCtx`](crate::serving::ServingPrepCtx)），两端独立演化，谁都不必为对方的
//!    新字段买单。
//! 2. [`TrPrepFactory`]：**工厂**。`with_ctx` 没有接收者，环境是它的参数；工厂用 GAT
//!    `type Ctx<'a>` 声明**接受哪一种环境**，而环境的类型与构造都由 `mptp_smux` 决定——
//!    使用者只写工厂，不写环境，也不写环境的构造。
//! 3. [`RingBuffs`]：最常用的交付物（两块现成内存），实现上游
//!    [`TrPrepareRing`](abs_smux::chan::TrPrepareRing)。
//!
//! # 为什么这么切
//!
//! - **信息只走环境**。工厂 trait 不带任何配置参数：dock 本来就该从环境取。带配置参数的
//!   代价是每个工厂要为每组配置各实现一次，且同一类型对多组配置实现同一 trait 会让方法解析
//!   出现歧义。
//! - **环境是 `mptp_smux` 的类型，两端各一个**，因此「加一个字段」只改 `mptp_smux`：工厂
//!   通过访问器读它，加字段不会破坏任何已有工厂，也不会让另一端被动跟进。
//! - **GAT 让环境能携带借用**（将来把请求、会话上下文放进去时），那时不必改本 trait 的
//!   形状，只是 `Ctx<'a>` 真的用上那个 `'a`。
//! - **准备器是短命的**：`accept_async` 会拿走它的所有权，所以每条子流都要新的一份；工厂
//!   因此「每条子流现场构造、用完即弃」，不需要被 `Client` / `Server` 长期持有——这正好让
//!   「构造签名干净」与「有默认工厂」在**静态分派**下兼得：默认只是一个**类型**。
//! - **生产是异步的**：内存来源可能要等（池、配额、背压），而这里本来就是异步上下文。
//!
//! # `ring_prepare_`: the ring-memory preparation contract
//!
//! Three parts: [`TrPrepareCtx`] (the *environment*: which docks, which role),
//! [`TrPrepFactory`] (a factory whose `with_ctx` takes the environment as a parameter), and
//! [`RingBuffs`] (the common "two ready-made buffers" deliverable). The environment is a
//! trait with one implementation per side, so adding a field never disturbs the other side
//! nor any existing factory. The factory declares which environment it accepts through the
//! GAT `type Ctx<'a>`, and the environment type plus its construction stay in `mptp_smux`.

use core::{
    alloc::AllocatorClone,
    marker::PhantomData,
    mem::MaybeUninit,
};

use abs_cancel::TrMayCancel;
use abs_smux::{
    chan::{RingBuffAlloc, TrPrepareRing},
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

/// 最常用的准备器：两块已经造好的 ring 内存（各自由一个智能指针 `B` 持有）。
///
/// 给出这一档，是为了让「只关心两块现成内存」的实现者不必自己写包装类型——否则
/// 「交付物类型由实现者定」这条自由就会变成每个人都要抄一遍的样板。
///
/// 上游 [`TrPrepareRing`] 与 [`RingBuffAlloc`] 都属 `abs_smux`，本 crate 不能给它们写
/// blanket impl（trait 与类型都是外部的）；`RingBuffs` 是**本 crate 自己的**类型，因此这条
/// 孤儿规则不适用，实现者可以零样板地用它。
///
/// # Examples
///
/// ```
/// use core::mem::MaybeUninit;
///
/// use mptp_smux::RingBuffs;
/// use mptp_smux::x_deps::abs_smux::chan::TrPrepareRing;
/// use mptp_smux::x_deps::abs_mm::CoreAlloc;
/// use mptp_smux::x_deps::mm_ptr::Owned;
///
/// type Buff = Owned<[MaybeUninit<u8>], CoreAlloc>;
///
/// let ready = RingBuffs::<Buff>::new(
///     Owned::new_uninit_slice(1024usize, CoreAlloc),
///     Owned::new_uninit_slice(1024usize, CoreAlloc),
/// );
/// let buffs = ready.prepare();
///
/// assert_eq!(buffs.tx_buff.len(), 1024usize);
/// ```
pub struct RingBuffs<B, T = u8>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    /// 发送方向的 ring 内存。
    tx_: B,

    /// 接收方向的 ring 内存。
    rx_: B,

    /// `T` 只在 `B` 的约束里出现，因此需要一个标记字段。
    _use_t_: PhantomData<fn() -> T>,
}

impl<B, T> RingBuffs<B, T>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    /// 由两个方向的内存构造。
    pub const fn new(tx_buff: B, rx_buff: B) -> Self {
        RingBuffs {
            tx_: tx_buff,
            rx_: rx_buff,
            _use_t_: PhantomData,
        }
    }

    /// 拆回 `(发送方向, 接收方向)`。
    pub fn destruct(self) -> (B, B) {
        (self.tx_, self.rx_)
    }

    /// 发送方向的内存。
    pub const fn tx_buff(&self) -> &B {
        &self.tx_
    }

    /// 接收方向的内存。
    pub const fn rx_buff(&self) -> &B {
        &self.rx_
    }
}

impl<B, T> TrPrepareRing<B, T> for RingBuffs<B, T>
where
    B: 'static + TrUnique<Item = [MaybeUninit<T>]>,
    B::Alloc: AllocatorClone,
    T: 'static + Sized,
{
    fn prepare(self) -> RingBuffAlloc<B, T> {
        RingBuffAlloc::new(self.tx_, self.rx_)
    }
}

#[cfg(test)]
mod tests_ {
    //! ring 准备契约的单元测试。
    //!
    //! 这里刻意不建连接：环境、工厂构造与准备器交付都是**纯本地**的构造与所有权搬运，不碰
    //! IO。要求实现者「每条子流生产一份新内存」这条纪律，只能在这一层用计数与指针判定。
    //!
    //! Unit tests for the ring-preparation contract. No connection is built here: the
    //! environment, the factory construction and the deliverable are all local construction and
    //! ownership moves. "One fresh buffer per channel" is only checkable at this level, through
    //! a counter and pointer identity.

    use core::sync::atomic::{AtomicUsize, Ordering};

    use abs_buff::gen_may_cancel_future;
    use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
    use abs_smux::{conf::TrMuxConfig, dock::Dock};

    use super::*;
    use crate::client::ClientPrepCtx;
    use crate::x_deps::{abs_mm, abs_smux, mm_ptr};

    use abs_mm::CoreAlloc;
    use mm_ptr::Owned;

    /// 测试用的 mux 配置：只有 dock 与数据类型，没有任何连接行为。
    struct TestMuxCfg;

    impl TrMuxConfig for TestMuxCfg {
        type Data = u8;

        type Dock = Dock<u32>;
    }

    /// 测试用的准备失败错误。
    #[derive(Debug)]
    struct TestErr;

    impl core::fmt::Display for TestErr {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            f.write_str("测试用错误")
        }
    }

    impl core::error::Error for TestErr {}

    /// 测试工厂：容量由**环境**里的角色在构造时决定，并记录被生产了几次。
    struct RoleSizedFactory {
        cap_: usize,
        calls_: AtomicUsize,
    }

    impl TrPrepFactory for RoleSizedFactory {
        type Ctx<'a> = ClientPrepCtx<'a, TestMuxCfg>;

        type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

        type Prep = RingBuffs<Self::RingBuff>;

        type Err = TestErr;

        fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
            let cap_ = match ctx.role() {
                PeerRole::Initiator => 64usize,
                PeerRole::Responder => 32usize,
            };
            RoleSizedFactory {
                cap_,
                calls_: AtomicUsize::new(0usize),
            }
        }

        type GetPreparedAsync<'f> = PrepareRoleSizedAsync<'f, 'f>
        where
            Self: 'f;

        fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f> {
            PrepareRoleSizedAsync::new(self)
        }
    }

    /// `RoleSizedFactory::prepare_async` 的 step 函数。
    ///
    /// 生成的类型保持**私有**：它的 `MayCancelOutput` 里带着测试私有的错误类型，声明成
    /// `pub(crate)` 会撞上「私有类型出现在公开接口」。
    #[allow(clippy::needless_lifetimes)]
    #[gen_may_cancel_future(PrepareRoleSized, new(pub(crate)))]
    async fn prepare_role_sized_async_<'f, TyTok>(
        factory: &'f mut RoleSizedFactory,
        cancel: TyTok,
    ) -> Result<RingBuffs<Owned<[MaybeUninit<u8>], CoreAlloc>>, TestErr>
    where
        TyTok: TrCancellationToken,
    {
        let _ = cancel;
        factory.calls_.fetch_add(1usize, Ordering::SeqCst);
        let cap = factory.cap_;
        Result::Ok(RingBuffs::new(
            Owned::new_uninit_slice(cap, CoreAlloc),
            Owned::new_uninit_slice(cap, CoreAlloc),
        ))
    }

    /// 测试环境把构造时的两个 dock 与角色原样交给实现者。
    /// Test that the environment hands its two docks and its role over unchanged.
    ///
    /// - 手段：以 `(7, 9)` 两个 dock 造一个客户端环境，再逐个读三个访问器。
    /// - 判断：本端 / 远端 dock 与构造时逐项相等，角色恒为 `Initiator`——环境不带隐式改写。
    ///
    /// - Means: build a client environment from the docks `(7, 9)` and read its three
    ///   accessors.
    /// - Verdict: both docks equal what was passed in and the role is always `Initiator`, so the
    ///   environment rewrites nothing.
    #[test]
    fn env_exposes_docks_and_role_() {
        let (local, remote) = (Dock::new(7u32), Dock::new(9u32));
        let ctx = ClientPrepCtx::<TestMuxCfg>::new(&local, &remote);

        assert_eq!(ctx.local_dock(), &Dock::new(7u32));
        assert_eq!(ctx.remote_dock(), &Dock::new(9u32));
        assert_eq!(ctx.role(), PeerRole::Initiator);
    }

    /// 测试工厂在**构造时**就看得到环境：角色不同，容量不同。
    /// Test that the factory sees the environment at *construction* time.
    ///
    /// - 手段：同一个工厂类型由一个角色为发起方的环境构造，读出它准备交付的接收环容量。
    /// - 判断：容量为 64 字节（`with_ctx` 里按发起方定的那个数）。若环境没有进入工厂构造，
    ///   这里拿到的就会是响应方的 32——这正是「上下文参与构造」的判据。
    ///
    /// - Means: build the factory from an environment whose role is the initiator and read the
    ///   capacity of the receive buffer it is about to hand over.
    /// - Verdict: 64 bytes, the initiator size chosen inside `with_ctx`; a factory that never saw
    ///   the environment would report the responder's 32 instead.
    #[tokio::test]
    async fn factory_sees_env_at_construction_() {
        let (local, remote) = (Dock::new(1u32), Dock::new(2u32));
        let ctx = ClientPrepCtx::<TestMuxCfg>::new(&local, &remote);

        let mut factory = RoleSizedFactory::with_ctx(&ctx);
        let prep = factory
            .get_prepared_async()
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("发起方工厂应当能生产准备器");

        assert_eq!(prep.rx_buff().len(), 64usize);
    }

    /// 测试同一个工厂被反复调用时，每次都生产**新的一份**内存。
    /// Test that repeated calls on one factory each produce a *fresh* buffer.
    ///
    /// - 手段：同一个工厂实例连续生产两次，两份同时存活，读出指针并核对计数。
    /// - 判断：生产计数为 2，且两次的接收缓冲指针不同——上游 `accept_async` 会拿走所有权，
    ///   把同一块内存交给两条子流必然出错。
    ///
    /// - Means: produce twice from one factory instance, keep both alive, and read their
    ///   pointers plus the call counter.
    /// - Verdict: the counter is 2 and the two receive buffers have different pointers —
    ///   upstream `accept_async` takes ownership, so handing the same memory to two channels
    ///   would break.
    #[tokio::test]
    async fn factory_produces_new_buffs_per_call_() {
        let (local, remote) = (Dock::new(1u32), Dock::new(2u32));
        let ctx = ClientPrepCtx::<TestMuxCfg>::new(&local, &remote);
        let mut factory = RoleSizedFactory::with_ctx(&ctx);

        let first = factory
            .get_prepared_async()
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("第一次生产应当成功");
        let second = factory
            .get_prepared_async()
            .may_cancel_with(NonCancellableToken::new())
            .await
            .expect("第二次生产应当成功");

        assert_eq!(factory.calls_.load(Ordering::SeqCst), 2usize);
        assert_ne!(first.rx_buff().as_ptr(), second.rx_buff().as_ptr());
    }

    /// 测试 `RingBuffs` 把两块内存原样交给上游契约。
    /// Test that `RingBuffs` hands both buffers to the upstream contract unchanged.
    ///
    /// - 手段：把一对缓冲包进 `RingBuffs`，再经 `TrPrepareRing::prepare` 取回。
    /// - 判断：取回的两块缓冲与装入时的指针、容量完全一致——这一档只搬所有权，不复制、
    ///   不另分配；否则「实现者自己造的内存」会被悄悄换掉。
    ///
    /// - Means: wrap a pair of buffers in `RingBuffs` and take them back through
    ///   `TrPrepareRing::prepare`.
    /// - Verdict: pointers and capacities match exactly — this tier only moves ownership, so the
    ///   memory an implementor built is never silently replaced.
    #[test]
    fn ring_buffs_hands_over_buffs_unchanged_() {
        let tx: Owned<[MaybeUninit<u8>], CoreAlloc> = Owned::new_uninit_slice(16usize, CoreAlloc);
        let rx: Owned<[MaybeUninit<u8>], CoreAlloc> = Owned::new_uninit_slice(16usize, CoreAlloc);
        let tx_ptr = tx.as_ptr();
        let rx_ptr = rx.as_ptr();

        let ready = RingBuffs::new(tx, rx).prepare();

        assert_eq!(ready.tx_buff.as_ptr(), tx_ptr);
        assert_eq!(ready.rx_buff.as_ptr(), rx_ptr);
        assert_eq!(ready.tx_buff.len(), 16usize);
    }
}
