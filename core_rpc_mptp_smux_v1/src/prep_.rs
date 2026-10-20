//! 两端共用的 **ring 内存准备工厂**：把「每条子流的 ring 从哪来、多大」钉下来。
//!
//! # 为什么是一个类型，而不是一个实例
//!
//! [`mptp_smux`] 的 [`TrPrepFactory`] 没有配置参数：工厂接受的**环境**由 GAT
//! `type Ctx<'a>` 声明，而环境（两个 dock + 角色）由 `mptp_smux` 在每次最终裁决前现场构造。
//! 因此默认策略可以以**类型**的形式声明在配置里
//! （[`TrConfig::DefaultPrepFactory`](mptp_smux::client::config::TrConfig::DefaultPrepFactory)），
//! `Client::new` / `Server::new` 的签名里不必出现任何策略参数。
//!
//! # 分配器从哪来
//!
//! 工厂持有一个**分配器实例**，它的类型来自使用者的分配约定
//! （[`TrClienAllocConfig`](crate::client::TrClienAllocConfig) /
//! [`TrServingAllocConfig`](crate::serving::TrServingAllocConfig) 的 `RingAlloc`）。实例在
//! `with_ctx` 里现场取——环境只带「哪条子流」的信息，内存来源是**配置**的事，不该混进环境。
//!
//! 因此「按本文档这两个工厂造内存」这条路上，`RingAlloc` 必须能无参构造（`Default`）且能
//! 跨线程共享（`Send + Sync`）：`accept_async` 会把准备器连同分配器一起搬进连接的循环。
//! 分配器要带状态（池、配额）时，写自己的工厂类型，并在自己的配置里声明它。
//!
//! # 两端为什么是两个类型
//!
//! 工厂的环境类型是 GAT，一个类型只能写死一种环境，而客户端与服务端的环境
//! （[`ClientPrepCtx`] / [`ServingPrepCtx`]）刻意不共享——它们可以跑在两个进程里，各自独立
//! 升级。因此这里给出两个工厂：将来各自按本端关心的信息选内存时，互不影响。
//!
//! # 产物为什么是具名的 future 类型
//!
//! 契约把 `get_prepared_async` 的返回类型写成关联类型（而不是 RPITIT），是为了让 future 成为
//! 使用者手里**可命名**的一等值。本模块因此走 [`gen_may_cancel_future`]：它给每个 step 函数
//! 生成一个具名、可取消的产物类型。
//!
//! # `prep_`: where the per-channel ring memory comes from
//!
//! The factory carries no configuration: the environment it accepts is a GAT, built by
//! `mptp_smux` right before each hand-over, so the default strategy can live in the config as
//! a **type**. The allocator instance itself comes from the allocation policy in that config.

use core::{
    alloc::AllocatorClone,
    convert::Infallible,
    mem::MaybeUninit,
};

use abs_mm::CoreAlloc;
use mm_ptr::Owned;
use mptp_smux::{
    RingBuffs, TrPrepFactory, TrPrepareCtx,
    client::{ClientPrepCtx, config as client_cfg},
    serving::{ServingPrepCtx, config as serving_cfg},
};

use crate::{
    client::TrRpcClienAllocConfig,
    serving::TrServingAllocConfig,
    x_deps::{abs_mm, abs_smux, mptp_smux},
};

/// 一条子流向一个方向的 ring 容量（字节）。
///
/// 它是**本实现**的选择，不是协议常量：`abs_smux` 把容量交给 `accept_async` 的实参，因此
/// 「多大」只由交出内存的那一方决定。
pub const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 子流 ring 存储的拥有者类型。
///
/// 分配器是参数，缺省是系统分配器：把「这块内存从哪来」写在类型里，调用者换分配器时不必改
/// 任何函数签名。
pub type RingBuff<CyAlloc = CoreAlloc> = Owned<[MaybeUninit<u8>], CyAlloc>;

/// 造一块 `cap` 字节的 ring 内存（尚未初始化）。
///
/// 每条入向子流都必须**新造两块**：上游 `accept_async` 会拿走准备器的所有权，把同一块内存交给
/// 两条子流必然出错。这个函数就是「造一块新的」这条纪律的落点。
pub fn new_ring_buff_<CyAlloc>(cap: usize, alloc: CyAlloc) -> RingBuff<CyAlloc>
where
    CyAlloc: AllocatorClone,
{
    Owned::new_uninit_slice(cap, alloc)
}

/// 造一份准备器：两块各 `cap` 字节的新 ring，分配器取自同一个实例（分配器本身可克隆）。
pub fn new_ring_buffs_<CyAlloc>(cap: usize, alloc: CyAlloc) -> RingBuffs<RingBuff<CyAlloc>>
where
    CyAlloc: AllocatorClone,
{
    RingBuffs::new(
        new_ring_buff_(cap, alloc.clone()),
        new_ring_buff_(cap, alloc),
    )
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 客户端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 客户端侧的 ring 准备工厂：每条子流按分配约定交出两块**新的**内存。
///
/// 「新的」不是风格问题：上游 `accept_async` 会拿走准备器的所有权，把同一块内存交给两条
/// 子流必然出错。
///
/// 泛型参数是**使用者配置**与**分配约定**：工厂的环境类型由前者推导，ring 的分配器由后者
/// 给出。
pub struct ClientPrepFactory<CyCfg, CyAllocCfg>
where
    CyAllocCfg: TrRpcClienAllocConfig,
{
    /// 本次建流的容量；目前由环境确认后就取常量，将来可按角色 / dock 分档。
    cap_: usize,

    /// 本端 ring 存储的分配器实例。
    alloc_: CyAllocCfg::RingAlloc,

    /// 配置类型只出现在关联类型的推导里，字段本身不含它。
    _use_cfg_: core::marker::PhantomData<fn() -> CyCfg>,
}

impl<TyCfg, CyAllocCfg> TrPrepFactory for ClientPrepFactory<TyCfg, CyAllocCfg>
where
    TyCfg: client_cfg::TrConfig,
    CyAllocCfg: TrRpcClienAllocConfig,
{
    type Ctx<'a> = ClientPrepCtx<'a, client_cfg::MuxCfg<TyCfg>>;

    type RingBuff = RingBuff<CyAllocCfg::RingAlloc>;

    type Prep = RingBuffs<Self::RingBuff>;

    /// 本实现的准备不会失败；真实实现（内存池、配额）在这里报自己的错误类型。
    type Err = Infallible;

    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
        // 环境参与的是**构造**：要按本条子流的身份（两个 dock、角色）选内存就在这里做。
        let _ = (ctx.local_dock(), ctx.remote_dock(), ctx.role());
        ClientPrepFactory {
            cap_: K_CHANNEL_CAP,
            alloc_: CyAllocCfg::RingAlloc::default(),
            _use_cfg_: core::marker::PhantomData,
        }
    }

    /// 生产一份准备器的产物类型。
    ///
    /// 它是 [`core::future::Ready`]：本实现的准备是**纯本地、不会失败、没有等待点**的构造，
    /// 因此产物不必是宏生成的那套可取消 future——一个已经就绪的值就是全部内容，取消令牌也
    /// 没有可着力的地方（这条取向与 `TrMayCancel` 的要求相容：`Ready` 本身就是 `IntoFuture`）。
    type GetPreparedAsync<'f>
        = core::future::Ready<Result<Self::Prep, Self::Err>>
    where
        Self: 'f;

    fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f> {
        // 每次都是**新的**两块：上游 `accept_async` 会拿走所有权。
        let cap = self.cap_;
        core::future::ready(Result::Ok(new_ring_buffs_(cap, self.alloc_.clone())))
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 服务端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 服务端侧的 ring 准备工厂。
///
/// 与客户端那个**是两个类型**（理由见模块文档）；行为目前相同。
pub struct ServingPrepFactory<TyCfg, TyAllocCfg>
where
    TyAllocCfg: TrServingAllocConfig,
{
    /// 每条入向子流都按这个容量交两块新内存。
    cap_: usize,

    /// 本端 ring 存储的分配器实例。
    alloc_: TyAllocCfg::RingAlloc,

    /// 配置类型只出现在关联类型的推导里，字段本身不含它。
    _use_cfg_: core::marker::PhantomData<fn() -> TyCfg>,
}

impl<CyCfg, CyAllocCfg> TrPrepFactory for ServingPrepFactory<CyCfg, CyAllocCfg>
where
    CyCfg: serving_cfg::TrConfig,
    CyAllocCfg: TrServingAllocConfig,
{
    type Ctx<'a> = ServingPrepCtx<'a, serving_cfg::MuxCfg<CyCfg>>;

    type RingBuff = RingBuff<CyAllocCfg::RingAlloc>;

    type Prep = RingBuffs<Self::RingBuff>;

    type Err = Infallible;

    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
        let _ = (ctx.local_dock(), ctx.remote_dock(), ctx.role());
        ServingPrepFactory {
            cap_: K_CHANNEL_CAP,
            alloc_: CyAllocCfg::RingAlloc::default(),
            _use_cfg_: core::marker::PhantomData,
        }
    }

    /// 生产一份准备器；与客户端那份同形（见那里的说明）。
    type GetPreparedAsync<'f> =
        core::future::Ready<Result<Self::Prep, Self::Err>>
    where
        Self: 'f;

    fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f> {
        let cap = self.cap_;
        core::future::ready(Result::Ok(new_ring_buffs_(cap, self.alloc_.clone())))
    }
}

// 让「工厂交出的准备器确实实现了上游契约」这条约束在编译期被检查一次：`RingBuffs` 若不满足
// `TrPrepareRing`，`TrPrepFactory` 的关联类型约束就会在这里炸掉。
const _: fn() = || {
    fn assert_prepare_ring<CyB, Ty, CyP>()
    where
        CyP: abs_smux::chan::TrPrepareRing<CyB, Ty>,
        CyB: 'static + Send + Sync + abs_mm::res_man::TrUnique<Item = [MaybeUninit<Ty>]>,
        CyB::Alloc: AllocatorClone,
        Ty: 'static + Sized,
    {}
    assert_prepare_ring::<RingBuff, u8, RingBuffs<RingBuff>>();
};
