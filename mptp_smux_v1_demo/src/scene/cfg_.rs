//! compio 两端的 `mptp_smux` 配置与 ring 准备工厂。
//!
//! `mptp_smux` 的工厂由**环境**现场构造，因此这里只需要声明「每条子流交付两块新内存」。
//! 传输半边是 `buffex_compio_adapt` 的 `BuffRead` / `BuffWrite`：它们必须在正在运行的
//! compio runtime 内构造，且类型 `!Send`。
//!
//! # `cfg_`: compio configurations and ring-preparation factories
//!
//! The factories hand out fresh ring memory per channel. Transport halves come from
//! `buffex_compio_adapt` and must be built inside a running compio runtime.

use core::{convert::Infallible, mem::MaybeUninit};

use abs_buff::{gen_may_cancel_future, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use abs_mm::{
    CoreAlloc,
    res_man::{TrAllocBy, TrUnique},
};
use buffex_compio_adapt::{
    BuffRead, BuffWrite, DefaultAllocConfig, SharedRingOf,
};
use mm_ptr::{Owned, Shared};
use mptp_smux::{
    K_DEFAULT_SERVING_DOCK, RingBuffs, TrPrepFactory, TrPrepareCtx,
    client::{ClientPrepCtx, config as client_cfg},
    messaging::{Headers, RequestPrefix, ResponsePrefix},
    serving::{ServingPrepCtx, config as serving_cfg},
    x_deps::abs_smux,
};

use smux_v1::connection::{DefaultConnCfg, MuxConnection};

/// 一条子流向一个方向的 ring 容量（字节）。
///
/// 场景事件很小；64 KiB 给慢消费者留出足够缓冲，又不会让每条订阅占用过多内存。
pub const K_CHANNEL_CAP: usize = 64usize * 1024usize;

/// 子流 ring 存储的拥有者类型。
pub type RingBuff = Owned<[MaybeUninit<u8>], CoreAlloc>;

/// 本 demo 使用的垛口类型（`smux_v1` 定死了内层是 `u32`）。
pub type Dock = abs_smux::dock::Dock<u32>;

/// TCP 读半边：设备 → 环。
pub type Rx = BuffRead<
    compio::net::TcpStream,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// TCP 写半边：环 → 设备。
pub type Tx = BuffWrite<
    compio::net::TcpStream,
    DefaultAllocConfig,
    SharedRingOf<DefaultAllocConfig>,
>;

/// 本 demo 的复用连接。
pub type Conn = MuxConnection<DefaultConnCfg<Tx, Rx>>;

/// 共享连接：客户端把它交给 `MptpClient`。
pub type SharedConn = Shared<Conn, CoreAlloc>;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 客户端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 客户端侧配置。
pub struct ClientCfg;

impl abs_mptp::client::config::TrConfig for ClientCfg {
    type MuxConn = Conn;

    type SharedConn = SharedConn;

    type ReqPrefix = RequestPrefix;

    type RespPrefix = ResponsePrefix;
}

impl client_cfg::TrConfig for ClientCfg {
    type DefaultPrepFactory = ClientPrepFactory;

    fn default_serving_dock() -> client_cfg::Dock<Self> {
        Dock::new(u32::from(K_DEFAULT_SERVING_DOCK))
    }
}

/// 客户端侧的 ring 准备工厂。
pub struct ClientPrepFactory {
    /// 每条子流的容量。
    cap_: usize,
}

impl TrPrepFactory for ClientPrepFactory {
    type Ctx<'a> = ClientPrepCtx<'a, client_cfg::MuxCfg<ClientCfg>>;

    type RingBuff = RingBuff;

    type Prep = RingBuffs<Self::RingBuff>;

    type Err = Infallible;

    fn with_ctx(ctx: &Self::Ctx<'_>) -> Self {
        let _ = (ctx.local_dock(), ctx.remote_dock(), ctx.role());
        ClientPrepFactory {
            cap_: K_CHANNEL_CAP,
        }
    }

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
pub struct ServingPrepFactory {
    /// 每条入向子流的容量。
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

// 编译期检查：交付物确实满足上游契约。
const _: fn() = || {
    fn assert_prepare_ring<B, T, P>()
    where
        P: abs_smux::chan::TrPrepareRing<B, T>,
        B: 'static
            + TrUnique<Item = [MaybeUninit<T>]>
            + TrAllocBy<Alloc: core::alloc::AllocatorClone>
            + Send
            + Sync,
        T: 'static + Sized,
    {
    }
    assert_prepare_ring::<RingBuff, u8, RingBuffs<RingBuff>>();
};
