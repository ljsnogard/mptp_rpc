use crate::messaging::TrRequestPrefix;
use super::config as srv_config;

/// 服务端上下文
pub trait TrServingContext<C>
where
    C: srv_config::TrConfig,
{
    type ReqPrefix: TrRequestPrefix;

    /// 请求前缀，包含访问方法，路径，请求头，体信息等。
    fn prefix(&self) -> &Self::ReqPrefix;

    /// 可用于发送回复的底层 channel 字节流
    fn chan_tx(&mut self) -> &mut srv_config::ChannelTx<C>;

    /// 完成前缀解析后的底层 channel 字节流
    fn chan_rx(&mut self) -> &mut srv_config::ChannelRx<C>;
}
