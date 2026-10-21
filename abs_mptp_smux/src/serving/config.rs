//! 服务端侧的公开契约：在 [`abs_mptp`] 的协议形状之上，补出「每条入向子流的 ring 内存
//! 从哪来」这条**实现**约定。
//!
//! # 为什么不复用 [`TrConfig`](crate::client::config::TrConfig)
//!
//! 服务端与客户端**可能跑在两个不同的进程里**，由两份不同的代码编译、可以各自独立升级。
//! 它们的连接类型、内存策略各自独立；把两者合成一个配置，就等于假定「客户端与服务端永远
//! 是同一份二进制」，而这恰恰是 RPC 框架不该假定的东西。
//!
//! 请求 / 回复前缀的具体类型相同，是因为它们就是同一种线上格式（[`crate::messaging`]），
//! 不是因为两端共享配置。头表同样由那一层拥有：它既是线上形态，也是 handler 拿到的可变
//! 形态。
//!
//! # `serving::config`: what a server implementation must answer
//!
//! The two sides keep separate configurations because they may live in different processes
//! and evolve independently. Prefix and header types come from [`crate::messaging`] — they
//! are the wire format, not a per-side knob — while this trait only answers where the ring
//! memory of each incoming channel comes from.

// 把协议层派生的类型别名原样转出，使用者在 `mptp_smux::serving::config` 一个路径下就能拿到
// 全套（`ChannelTx` / `ChannelRx`…… 的推导链在 `abs_mptp` 里，不必让使用者记两条路径）。
//
// `MuxCfg` 也在其中：工厂实现者要写 `type Ctx<'a> = ServingPrepCtx<'a, MuxCfg<MyCfg>>`，
// 拿不到它就写不出那个 GAT。
pub use abs_mptp::serving::config::{
    ChannelHandle, ChannelRx, ChannelTx, Dock, DockBinding, Listener, MuxCfg, MuxConn,
    ReqPrefix, RespPrefix,
    TrConfig as TrMptpServConfig,
};

use crate::ring_prepare_::TrPrepFactory;

/// 一个 MPTP 服务端实现所需的**全部**配置。
///
/// 需要 `Sized`：环境类型以 `Self` 参数化（见 [`TrConfig::DefaultPrepFactory`]）。
///
/// # 请求 / 回复与头表被钉成 `messaging` 的具体类型
///
/// 服务端 handler 拿到的是**请求前缀的头表**（[`Headers`]）的可变引用：框架从前缀只读地
/// 借出它、克隆一份再交出去，因此 handler 往里加 / 改 / 删头不会影响已经解出的前缀。
/// 回复前缀同样是 [`ResponsePrefix`]，由 handler 通过
/// [`write_response_prefix_async`](crate::messaging::write_response_prefix_async) 写出。
///
/// 这三条等式不是「填空」，而是「线上格式由本 crate 拥有」在类型层面的落地。
pub trait TrConfig
where
    Self: Sized + TrMptpServConfig,
{
    /// 默认的 ring 准备**工厂类型**。
    ///
    /// 声明的是类型，不是实例：工厂由 `mptp_smux` 在**每条**入向子流裁决前用环境现场构造
    /// （环境就是它的参数），所以没有实例需要提前注入，
    /// [`MptpServer::new`](crate::serving::MptpServer::new) 的签名因此不必带上策略。
    ///
    /// 服务端**没有**「调用者当场交出准备器」那条覆盖路径：一次 `serve_listener` 要接受 N 条
    /// 子流，而上游 `accept_async` 交出即消费——一个准备器只能交一次，第二条就无物可交。要换
    /// 内存来源就换本关联类型。
    type DefaultPrepFactory: TrPrepFactory;
}
