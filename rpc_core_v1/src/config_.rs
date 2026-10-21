//! 本 crate 的**现成配置**：把「连接从哪来」「ring 内存从哪来」组装成两个可以直接用的类型。
//!
//! 线上格式已由 [`mptp_smux`] 拥有，因此本模块不再回答「报文怎么编解码」，
//! 只回答 `mptp_smux` 留空的那一块：**内存分配约定**。
//!
//! # 本 crate 在这条链上的位置
//!
//! [`mptp_smux`] 拥有「开流 → 交出 ring → 收发前缀」这条流程，并要求使用者以关联类型回答
//! 连接、ring 准备与缺省垛口；本模块给出它的一份具体配置。ring 内存由
//! [`ClientPrepFactory`] / [`ServingPrepFactory`] 生产，分配器由使用者的
//! [`TrRpcClienAllocConfig`] / [`TrServingAllocConfig`] 指定。
//!
//! # 这两个类型是「一份实现」，不是「唯一的配置」
//!
//! 使用者要实现自己的配置类型时，走的是
//! [`TrRpcClientConfig`](crate::client::TrRpcClientConfig) /
//! [`TrServingConfig`](crate::serving::TrServingConfig)：那两条 trait 才规定「配置要回答
//! 什么」。本模块的 `ClientConfig` / `ServingConfig` 只是其中**最省事的那一份**——它的三个
//! 类型参数分别是连接类型、垛口类型与分配约定，把它插进对应 trait 的 impl 里即可。
//!
//! # 连接类型与垛口类型为什么是泛型参数
//!
//! 连接来自**使用者的传输层**（`smux_v1` 的 `MuxConnection<DefaultConnCfg<Tx, Rx>>`），而
//! `Tx` / `Rx` 取决于使用者用什么 IO（tokio 的 TCP、compio、内存管道……）。垛口类型同理：
//! 协议只规定「默认服务垛口的数值是 `0x80`」，而「这个数值在本端垛口类型上长什么样」是本端
//! 的事实。本 crate 因此**既不选传输、也不选垛口**：两个类型都由使用者给出，框架的其余部分
//! 从它们推导收发半边与垛口。
//!
//! # 两端为什么是两组配置
//!
//! 与上游同一条纪律：客户端与服务端**可能跑在两个进程里**，由两份代码各自编译、各自升级。
//! 因此连接、内存策略与分配约定都各写一份。
//!
//! # `config_`: this crate's ready-made configurations
//!
//! `mptp_smux` owns the flow and asks for the connection, ring factory and default dock; this
//! module answers them concretely. Neither the transport nor the dock type is chosen here — both
//! come from the user, together with the two allocation policies.

use core::marker::PhantomData;

use crate::{
    client::{DefaultRpcAllocConfig, TrRpcClientAllocConfig, TrRpcClientConfig},
    prep::{ClientPrepFactory, ServingPrepFactory},
    serving::{DefaultServingAllocConfig, TrServingAllocConfig, TrServingConfig},
    x_deps::{
        abs_mptp,
        abs_smux::{
            conf::TrMuxConfig,
            conn::TrConnection,
            dock::{Dock, TrDock},
        },
        mm_ptr::Shared,
        mptp_smux::{self, client::config as mptp_cli_cfg, serving::config as mptp_srv_cfg},
    },
};

/// 使用者连接类型在配置里的约束。
///
/// MPTP 的报文前缀与报文体都以字节为单位，因此 `Data` 收紧为 `u8`；`Config` 必须能回答垛口
/// 与「怎么等」（`TrMuxConfig`），且它的垛口类型被显式参数 `CyDock` 钉住。本 trait 把这条
/// 约束收在一处，免得每个 `impl` 抄一遍。
///
/// # 为什么它还要回答「默认服务垛口怎么造」
///
/// 协议只规定默认服务垛口的**数值**（`0x80`，见 [`mptp_smux::K_DEFAULT_SERVING_DOCK`]），而
/// 「这个数值在本端垛口类型上长什么样」是本端的事实：`abs_smux` 的 [`TrDock`] 只要求
/// `unspecified()` / `wildcard()`，**没有**从数值构造的入口——数值化是具体实现（如
/// `smux_v1` 的 `Dock<u32>`）自己的事。因此这条转换必须由使用者回答，本 crate 不替它假设
/// 垛口内部表示。
pub trait TrUserConn<TyDock>
where
    Self: TrConnection<Config: TrMuxConfig<Data = u8, Dock = TyDock>>,
    TyDock: TrDefaultDock,
{
    /// 把协议约定的默认服务垛口数值变成**本端垛口类型上的取值**。
    ///
    /// 实现通常是 `<本端 Dock 类型>::new(u32::from(value))` 这样一次构造。
    fn default_serving_dock() -> TyDock;

    /// 把任意一个垛口数值变成本端垛口类型上的取值（需要指定资源所在垛口时用）。
    ///
    /// TODO(重构): 目前没有使用者（一次访问的目标垛口由调用点直接给出 `CyDock` 值）。等
    /// 「同一份配置要按数值寻址多个垛口」成为实际需求时，再决定它应当由使用者实现，还是
    /// 通过一条 `CyDock: From<u32>` 约束要求本端垛口类型自己提供——后者需要 `abs_smux` 的
    /// `Dock` 放宽构造入口，不是本 crate 能定的。
    fn dock_of(value: u32) -> TyDock;
}

/// 「本端垛口类型怎么造出**协议约定的缺省服务垛口**」。
///
/// # 为什么需要使用者回答
///
/// 协议只规定默认服务垛口的**数值**（`0x80`，见 [`mptp_smux::K_DEFAULT_SERVING_DOCK`]），而
/// `abs_smux` 的 [`TrDock`] 只要求 `unspecified()` / `wildcard()`——它**没有**从数值构造的
/// 入口，也没有 `Default`：数值化是具体实现（如 `smux_v1` 的 `Dock<u32>`）自己的事。因此
/// 「这个数值在本端垛口类型上长什么样」只能由使用者回答，本 crate 不替它假设内部表示。
///
/// 实现通常是一行：`fn default_serving_dock() -> Self { Dock::new(0x80u32) }`。
pub trait TrDefaultDock: TrDock {
    /// 协议约定的缺省服务垛口在本端垛口类型上的取值。
    fn default_serving_dock() -> Self;
}

// `abs_smux` 自带的 `Dock<T>` 是绝大多数使用者的垛口类型（`smux_v1` 用的就是 `Dock<u32>`），
// 而它只有 `new`：这里替它回答「协议约定的缺省服务垛口是哪一个」，使用者因此不必自己包一层。
// 孤儿规则允许——`TrDefaultDock` 是本 crate 的 trait，且本 crate 直接依赖 `abs_smux`。
// `abs_smux` 只为**具体宽度**实现 `TrDock`（`Dock<u8>` / `Dock<u16>` / `Dock<u32>` /
// `Dock<u64>`），因此这里也只能逐个宽度实现，不能写一条对 `Dock<T>` 泛型的 impl。
macro_rules! impl_default_dock_ {
    ($ty_inner:ty) => {
        impl TrDefaultDock for Dock<$ty_inner> {
            fn default_serving_dock() -> Self {
                Dock::new(<$ty_inner>::from(mptp_smux::K_DEFAULT_SERVING_DOCK))
            }
        }
    };
}

impl_default_dock_!(u8);
impl_default_dock_!(u16);
impl_default_dock_!(u32);
impl_default_dock_!(u64);

impl<CyConn, CyDock> TrUserConn<CyDock> for CyConn
where
    CyDock: TrDefaultDock,
    CyConn: TrConnection<Config: TrMuxConfig<Data = u8, Dock = CyDock>>,
{
    fn default_serving_dock() -> CyDock {
        <CyDock as TrDefaultDock>::default_serving_dock()
    }

    fn dock_of(value: u32) -> CyDock {
        let _ = value;
        <CyDock as TrDefaultDock>::default_serving_dock()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 客户端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 「只出现在关联类型里的类型参数」的落点。
///
/// 三个类型参数都不进字段，但又必须出现在类型里（否则编译器认为它们是自由的），因此用
/// `PhantomData` 标记一份。抽成别名只是为了不让每个配置类型都写一遍那个三元组。
type TypeMark_<TyConn, TyDock, TyAllocCfg> = PhantomData<fn() -> (TyConn, TyDock, TyAllocCfg)>;

/// 客户端侧的完整配置：一份可以直接插进 [`TrRpcClientConfig`] 的现成实现。
///
/// 它同时回答两份契约：
///
/// - [`abs_mptp::client::config::TrConfig`]：协议侧要的连接、共享连接与报文类型；
/// - [`mptp_smux::client::config::TrConfig`]：实现侧要的 ring 内存来源与缺省垛口；
///
/// 并在本 crate 的 [`TrRpcClientConfig`] 里补上分配约定。
///
/// 三个泛型参数都是**使用者给出的**：连接类型、垛口类型、分配约定（理由见模块文档）。后两个
/// 各有缺省值，只关心传输的场合可以只写第一个。
pub struct ClientConfig<
    TyConn,
    TyDock = <<TyConn as TrConnection>::Config as TrMuxConfig>::Dock,
    TyAllocCfg = DefaultRpcAllocConfig,
> {
    /// 三个类型参数只出现在关联类型与函数体里，字段本身不含它们。
    _use_types_: TypeMark_<TyConn, TyDock, TyAllocCfg>,
}

impl<TyConn, TyDock, TyAllocCfg> ClientConfig<TyConn, TyDock, TyAllocCfg> {
    /// 造一份客户端配置。
    ///
    /// 它是一个**零大小的类型标记**：连接、垛口与分配约定都在类型参数里，运行期没有任何状态
    /// 需要携带。保留这个构造函数是为了让使用者不必知道内部那个 `PhantomData` 字段的存在。
    pub const fn new_() -> Self {
        ClientConfig {
            _use_types_: PhantomData,
        }
    }
}

impl<TyConn, TyDock, TyAllocCfg> abs_mptp::client::config::TrConfig
    for ClientConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrConnection<Config: TrMuxConfig<Data = u8, Dock = TyDock>>,
    TyDock: TrDefaultDock,
    TyAllocCfg: TrRpcClientAllocConfig,
{
    type MuxConn = TyConn;

    /// 共享连接对象的分配器来自分配约定，而不是本 crate 的固定选择。
    type SharedConn = Shared<TyConn, TyAllocCfg::SharedConnAlloc>;

    type ReqPrefix = mptp_smux::messaging::RequestPrefix;

    type RespPrefix = mptp_smux::messaging::ResponsePrefix;
}

impl<TyConn, TyDock, TyAllocCfg> mptp_cli_cfg::TrConfig for ClientConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrUserConn<TyDock> + 'static,
    TyDock: TrDefaultDock + 'static,
    TyAllocCfg: TrRpcClientAllocConfig + 'static,
{
    /// ring 内存的来源：按分配约定造新内存的工厂。
    type DefaultPrepFactory = ClientPrepFactory<Self, TyAllocCfg>;

    /// 缺省把请求发往协议约定的服务垛口（`0x80`）。
    ///
    /// 数值是协议常量，但「它在本端垛口类型上长什么样」由使用者的
    /// [`TrUserConn::default_serving_dock`] 回答——本 crate 不替它假设垛口内部表示。
    fn default_serving_dock() -> mptp_cli_cfg::Dock<Self> {
        <TyConn as TrUserConn<TyDock>>::default_serving_dock()
    }
}

impl<TyConn, TyDock, TyAllocCfg> TrRpcClientConfig for ClientConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrUserConn<TyDock> + 'static,
    TyDock: TrDefaultDock + 'static,
    TyAllocCfg: TrRpcClientAllocConfig + 'static,
{
    type AllocCfg = TyAllocCfg;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 服务端
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 服务端侧的完整配置：一份可以直接插进 [`TrServingConfig`] 的现成实现。
///
/// 与 [`ClientConfig`] **刻意不共享**（理由见模块文档），并同时回答两份契约：
/// [`abs_mptp::serving::config::TrConfig`] 与 [`mptp_smux::serving::config::TrConfig`]。
pub struct ServingConfig<
    TyConn,
    TyDock = <<TyConn as TrConnection>::Config as TrMuxConfig>::Dock,
    TyAllocCfg = DefaultServingAllocConfig,
> {
    /// 三个类型参数只出现在关联类型与函数体里，字段本身不含它们。
    _use_types_: TypeMark_<TyConn, TyDock, TyAllocCfg>,
}

impl<TyConn, TyDock, TyAllocCfg> ServingConfig<TyConn, TyDock, TyAllocCfg> {
    /// 造一份服务端配置；理由同 [`ClientConfig::new_`]。
    pub const fn new_() -> Self {
        ServingConfig {
            _use_types_: PhantomData,
        }
    }
}

impl<TyConn, TyDock, TyAllocCfg> abs_mptp::serving::config::TrConfig
    for ServingConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrConnection<Config: TrMuxConfig<Data = u8, Dock = TyDock>>,
    TyDock: TrDefaultDock,
    TyAllocCfg: TrServingAllocConfig,
{
    type MuxConn = TyConn;

    type ReqHeaders = mptp_smux::messaging::Headers;

    type RespHeaders = mptp_smux::messaging::Headers;

    type ReqPrefix = mptp_smux::messaging::RequestPrefix;

    type RespPrefix = mptp_smux::messaging::ResponsePrefix;
}

impl<TyConn, TyDock, TyAllocCfg> mptp_srv_cfg::TrConfig
    for ServingConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrUserConn<TyDock> + 'static,
    TyDock: TrDefaultDock + 'static,
    TyAllocCfg: TrServingAllocConfig + 'static,
{
    /// ring 内存的来源：按分配约定造新内存的工厂。
    type DefaultPrepFactory = ServingPrepFactory<Self, TyAllocCfg>;
}

impl<TyConn, TyDock, TyAllocCfg> TrServingConfig for ServingConfig<TyConn, TyDock, TyAllocCfg>
where
    TyConn: TrUserConn<TyDock> + 'static,
    TyDock: TrDefaultDock + 'static,
    TyAllocCfg: TrServingAllocConfig + 'static,
{
    type AllocCfg = TyAllocCfg;
}
