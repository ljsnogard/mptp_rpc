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

use crate::ring_prepare_::TrPrepFactory;
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
    Self: Sized + TrMptpClientConfig,
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
