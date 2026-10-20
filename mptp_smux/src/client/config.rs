//! 客户端侧的公开契约：在 [`abs_mptp`] 的协议形状之上，补出「ring 内存从哪来」与
//! 「缺省服务垛口」两条**实现**约定。
//!
//! # 为什么是 `abs_mptp::client::config::TrConfig` 的子 trait
//!
//! [`abs_mptp`] 定的是协议层：连接类型、请求 / 回复前缀的形状。本 crate 定的是实现层：
//! 前缀的线上格式由 [`crate::messaging`] **自己拥有**，因此本 trait 只再回答「每条子流的
//! ring 内存怎么准备」（[`TrPrepFactory`]）与「缺省发往哪个垛口」。两者分开的理由很实际
//! ——**协议不关心内存从哪来**，而实现必须回答它。
//!
//! 服务端那份契约在 [`crate::serving::config`]，两者**刻意不共享**：它们可能跑在两个进程
//! 里，由两份代码各自编译、各自升级。请求 / 回复前缀的具体类型相同，是因为它们就是同一种
//! 线上格式，而不是因为两端共享配置。

// 把协议层派生的类型别名原样转出，使用者在 `mptp_smux::client::config` 一个路径下就能拿到
// 全套（`ChannelTx` / `ChannelRx`…… 的推导链在 `abs_mptp` 里，不必让使用者记两条路径）。
//
// `MuxCfg` 也在其中：工厂实现者要写 `type Ctx<'a> = ClientPrepCtx<'a, MuxCfg<MyCfg>>`，
// 拿不到它就写不出那个 GAT。
pub use abs_mptp::client::config::{
    ChannelHandle, ChannelListener, ChannelRx, ChannelTx, Dock, DockBinding, MuxCfg, MuxConn,
    ReqPrefix, RespPrefix,
    TrConfig as TrMptpClientConfig,
};

use crate::{
    messaging::{RequestPrefix, ResponsePrefix},
    ring_prepare_::TrPrepFactory,
};
use super::prep_ctx_::ClientPrepCtx;

/// 一个 MPTP 客户端实现所需的**全部**配置。
///
/// 需要 `Sized`：环境类型以 `Self` 参数化（见 [`TrConfig::DefaultPrepFactory`]）。
///
/// 请求 / 回复前缀被钉成 [`crate::messaging`] 的具体类型：它们**就是** MPTP 的线格式，
/// 不是使用者可以替换的「编码方式」。业务类型怎么序列化仍然是使用者自己的事；本 crate 只
/// 负责把前缀按协议写出去、把对端的前缀读回来。
pub trait TrConfig
where
    Self: Sized
        + TrMptpClientConfig<
            ReqPrefix = RequestPrefix,
            RespPrefix = ResponsePrefix,
        >,
{
    /// 默认的 ring 准备**工厂类型**。
    ///
    /// 声明的是类型，不是实例：工厂由 `mptp_smux` 在每次最终裁决前用
    /// [`TrPrepFactory::with_ctx`] 现场构造（环境就是它的参数），所以**没有实例需要提前
    /// 注入**——[`MptpClient::new`](crate::client::MptpClient::new) 的签名因此不必带上策略。
    ///
    /// 高阶等式 `Ctx<'a> = ClientPrepCtx<'a, ..>` 是刻意的：它把工厂能接受的环境**钉死**成
    /// 客户端的那一个，于是环境类型与其构造都留在 `mptp_smux` 手里，工厂只是它的读者。
    type DefaultPrepFactory: for<'a> TrPrepFactory<Ctx<'a> = ClientPrepCtx<'a, MuxCfg<Self>>>;

    /// **默认服务垛口**：协议约定的那个数值在本端垛口类型上的表示。
    ///
    /// 缺省路径（[`TrMptpClient::request_async`](abs_mptp::client::TrMptpClient::request_async)）把
    /// 请求发往这里；要去别处就用
    /// [`MptpClient::request_to_dock_async`](crate::client::MptpClient::request_to_dock_async)。
    /// `0x80` 是协议常量（[`K_DEFAULT_SERVING_DOCK`](crate::K_DEFAULT_SERVING_DOCK)），而
    /// 「它在本端垛口类型上长什么样」是本端的事实，所以由配置回答：一次 `Dock::new(0x80)`
    /// 而已。
    fn default_serving_dock() -> Dock<Self>;
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 供本 crate 内部使用的派生别名
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 配置里声明的默认 ring 准备工厂类型。
pub(crate) type DefaultPrepFactory<C> = <C as TrConfig>::DefaultPrepFactory;
