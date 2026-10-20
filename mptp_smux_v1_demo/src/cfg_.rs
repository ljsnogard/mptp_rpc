//! 两端的配置与 **ring 准备工厂**：把「连接从哪来」「ring 内存从哪来」两件事钉下来。
//! 报文前缀的线格式由 `mptp_smux::messaging` 统一拥有，不在本 demo 里重复实现。
//!
//! # 工厂为什么由**环境**构造
//!
//! `mptp_smux` 的 [`TrPrepFactory`] 没有配置参数：工厂接受的**环境**由 GAT `type Ctx<'a>`
//! 声明，而环境（两个 dock + 角色）是框架在裁决前现场构造的。本 demo 的工厂因此可以：
//!
//! - 在 `with_ctx` 里读到**这一条子流**的身份（本端 / 远端 dock、本端是发起方还是响应方）；
//! - 每次 `prepare_async` 交出**新的**两块内存——上游 `accept_async` 会拿走所有权。
//!
//! `MptpClient::new` / `MptpServer::new` 的签名里没有任何策略参数：默认工厂是一个**类型**。
//!
//! # `cfg_`: the two sides' configuration and the ring-preparation factories
//!
//! The factories carry no configuration argument: the *environment* they accept is declared
//! through the GAT `type Ctx<'a>` and built by `mptp_smux` right before each hand-over. So a
//! factory can read the identity of **this** channel and must hand out **fresh** buffers on
//! every call.

use core::{alloc::AllocatorClone, convert::Infallible, mem::MaybeUninit};

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use abs_mm::{CoreAlloc, res_man::TrUnique};
use abs_smux::{chan::TrPrepareRing, dock::Dock as MuxDock};
use mm_ptr::{Owned, Shared};
use mptp_smux::{
    K_DEFAULT_SERVING_DOCK, RingBuffs, TrPrepFactory, TrPrepareCtx,
    client::{ClientPrepCtx, config as client_cfg},
    messaging::{Headers, RequestPrefix, ResponsePrefix},
    serving::{ServingPrepCtx, config as serving_cfg},
    x_deps::abs_smux,
};

use crate::conn_::Conn;

/// 一条子流向一个方向的 ring 容量（字节）。
///
/// 它是**本实现**的选择，不是协议常量：`abs_smux` 把容量交给 `accept_async` 的实参，
/// 于是「多大」只由交出内存的那一方决定。
pub const K_CHANNEL_CAP: usize = 4usize * 1024usize;

/// 子流 ring 存储的拥有者类型。
pub type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 本 demo 使用的垛口类型（`smux_v1` 定死了内层是 `u32`）。
pub type Dock = MuxDock<u32>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 客户端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 客户端侧配置。
pub struct ClientCfg;

impl abs_mptp::client::config::TrConfig for ClientCfg {
    type MuxConn = Conn;

    type SharedConn = Shared<Conn, CoreAlloc>;

    type ReqPrefix = RequestPrefix;

    type RespPrefix = ResponsePrefix;
}

impl client_cfg::TrConfig for ClientCfg {
    type DefaultPrepFactory = ClientPrepFactory;

    /// 缺省把请求发往协议约定的服务垛口（`0x80`）。
    ///
    /// `0x80` 是协议常量，而「它在本端垛口类型上长什么样」是本端的事实——所以这里做一次
    /// `Dock::new`，而不是让 `mptp_smux` 去猜。
    fn default_serving_dock() -> client_cfg::Dock<Self> {
        Dock::new(u32::from(K_DEFAULT_SERVING_DOCK))
    }
}

/// 客户端侧的 ring 准备工厂：**容量从环境里读**（本 demo 两端相同，但走的是环境这条通路）。
pub struct ClientPrepFactory {
    /// 本端在这次建流里的角色决定的容量。
    cap_: usize,
}

impl TrPrepFactory for ClientPrepFactory {
    type Ctx<'a> = ClientPrepCtx<'a, client_cfg::MuxCfg<ClientCfg>>;

    type RingBuff = RingBuff;

    type Prep = RingBuffs<Self::RingBuff>;

    /// 本 demo 的准备不会失败；真实实现（内存池、配额）在这里报自己的错误类型。
    type Err = Infallible;

    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
        // 环境参与的是**构造**：想看「按本条子流的身份选内存」就在这里做。本 demo 只用它
        // 记一笔容量，但通路是活的。
        let _ = (ctx.local_dock(), ctx.remote_dock(), ctx.role());
        ClientPrepFactory {
            cap_: K_CHANNEL_CAP,
        }
    }

    /// 生产准备器的产物类型。
    ///
    /// 它写成 GAT 而**不是** RPITIT：`mptp_smux` 的工厂契约刻意让返回类型**可命名**——
    /// future 是使用者手里的一等值，要能存进结构体、也要能在 where 子句里加约束。
    type GetPreparedAsync<'f>
        = PrepareClientAsync<'f, 'f>
    where
        Self: 'f;

    fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f> {
        PrepareClientAsync::new(self)
    }
}

/// `ClientPrepFactory::get_prepared_async` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(PrepareClient, pub, new(pub(crate)))]
async fn prepare_client_async_<'f, TyTok>(
    factory: &'f mut ClientPrepFactory,
    cancel: TyTok,
) -> Result<RingBuffs<RingBuff>, Infallible>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    let cap = factory.cap_;
    // 每次都是**新的**两块：上游 `accept_async` 会拿走所有权。
    Result::Ok(RingBuffs::new(
        Owned::new_uninit_slice(cap, CoreAlloc),
        Owned::new_uninit_slice(cap, CoreAlloc),
    ))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 服务端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 服务端侧配置。
pub struct ServingCfg;

impl abs_mptp::serving::config::TrConfig for ServingCfg {
    type MuxConn = Conn;

    type ReqHeaders = Headers;

    type RespHeaders = Headers;

    type ReqPrefix = RequestPrefix;

    type RespPrefix = ResponsePrefix;
}

impl serving_cfg::TrConfig for ServingCfg {
    type DefaultPrepFactory = ServingPrepFactory;
}

/// 服务端侧的 ring 准备工厂。
///
/// 它与客户端那个**是两个类型**：工厂的 `Ctx<'a>` 是 GAT，一个类型只能服务一端。这正好与
/// 「两端配置刻意不共享」一致——服务端将来要按会话上下文选内存时，不必动客户端那一份。
pub struct ServingPrepFactory {
    /// 每条入向子流都按这个容量交两块新内存。
    cap_: usize,
}

impl TrPrepFactory for ServingPrepFactory {
    type Ctx<'a> = ServingPrepCtx<'a, serving_cfg::MuxCfg<ServingCfg>>;

    type RingBuff = RingBuff;

    type Prep = RingBuffs<Self::RingBuff>;

    type Err = Infallible;

    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
        let _ = (ctx.local_dock(), ctx.remote_dock(), ctx.role());
        ServingPrepFactory {
            cap_: K_CHANNEL_CAP,
        }
    }

    type GetPreparedAsync<'f>
        = PrepareServingAsync<'f, 'f>
    where
        Self: 'f;

    fn get_prepared_async<'f>(&'f mut self) -> Self::GetPreparedAsync<'f> {
        PrepareServingAsync::new(self)
    }
}

/// `ServingPrepFactory::get_prepared_async` 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(PrepareServing, pub, new(pub(crate)))]
async fn prepare_serving_async_<'f, TyTok>(
    factory: &'f mut ServingPrepFactory,
    cancel: TyTok,
) -> Result<RingBuffs<RingBuff>, Infallible>
where
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    let cap = factory.cap_;
    Result::Ok(RingBuffs::new(
        Owned::new_uninit_slice(cap, CoreAlloc),
        Owned::new_uninit_slice(cap, CoreAlloc),
    ))
}

// 让「工厂交出的准备器确实实现了上游契约」这条约束在编译期被检查一次：`RingBuffs` 若不
// 满足 `TrPrepareRing`，`TrPrepFactory` 的关联类型约束就会在这里炸掉。
const _: fn() = || {
    fn assert_prepare_ring<B, T, P>()
    where
        P: TrPrepareRing<B, T>,
        B: 'static + TrUnique<Item = [MaybeUninit<T>]> + Send + Sync,
        B::Alloc: AllocatorClone,
        T: 'static + Sized,
    {
    }
    assert_prepare_ring::<RingBuff, u8, RingBuffs<RingBuff>>();
};
