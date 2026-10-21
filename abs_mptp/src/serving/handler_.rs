use abs_buff::x_deps::abs_cancel;
use abs_cancel::TrMayCancel;

use crate::spec::access_method::AccessMethod;
use super::config as srv_config;

/// 路径级 handler：在一条子流上响应某个具体路径的任意方法请求。
///
/// 实现者应当：
///
/// 1. 根据 `method` 决定如何处理；
/// 2. 从 `chan_rx`（子流接收半边）读取请求体 / 持续流；
/// 3. 向 `chan_tx`（子流发送半边）写入响应头、body 或持续流。
///
/// # 两个半边按值交付
///
/// `chan_tx` / `chan_rx` 由 `MptpServer` 按值交给 handler：handler 可以把它们移进队列、
/// 交给 worker 线程，或在本函数里直接使用。**收尾也由 handler 负责**——框架不再在 handler
/// 返回后替它 `drop(tx)` / drain `rx`。标准收尾顺序是：
///
/// ```text
/// 写回复 → 半关闭 tx → 读完 rx → 释放 rx
/// ```
pub trait TrReqHandler<C>
where
    C: srv_config::TrConfig,
{
    type HandleAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>
    where
        Self: 'f;

    type Err: core::error::Error;

    /// 处理一次已经完成请求头解码的 MPTP 请求。
    ///
    /// 两个 channel 半边按值传入；handler 返回前必须完成（或转交给 worker 完成）收尾。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
        headers: &'f mut srv_config::ReqHeaders<C>,
        chan_tx: srv_config::ChannelTx<C>,
        chan_rx: srv_config::ChannelRx<C>,
    ) -> Self::HandleAsync<'f>;
}
