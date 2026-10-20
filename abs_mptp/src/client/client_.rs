use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::TrMayCancel;

use crate::{
    messaging::read::TrMptpBuffRead,
    x_deps::abs_cancel,
};
use super::{config as client_config};


/// MPTP 客户端。和 RPC 客户端不同，MPTP 客户端不对具体业务类型负责，只
/// 负责完成 MPTP 协议层面的数据手法和消息解析。不参与任何任何 RPC 业务。
pub trait TrMptpClient {
    type Config: client_config::TrConfig;

    type Session: TrMptpSession<Self::Config>;

    type ReqErr: core::error::Error;

    type RequestAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Session, Self::ReqErr>>
    where
        Self: 'f;

    /// 开一个 channel 然后向对端发送一个请求。如果发送成功，返回一个关于这个请求
    /// 的会话对象。
    fn request_async<'f>(
        &'f self,
        prefix: client_config::ReqPrefix<Self::Config>,
    ) -> Self::RequestAsync<'f>;
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SessionState {
    /// 准备发送请求体
    SendingBody,

    /// 准备发送后缀流（用于Push 或者 Call）
    SendingSuffix,

    /// 请求已整体发出，等待对端回复
    WaitingResp,

    /// 响应已到达，且会话状态就绪可以接收
    PrefixReady,

    /// 回复存在恢复体且会话状态就绪可以接收
    BodyReady,

    /// 会话存在 Suffix Stream 且就绪可以接收
    SuffixReady,

    /// 会话已结束（可能出错或者没有出错）
    Terminated,
}



/// 一个与对端之间的会话对象。通常一个会话就是围绕一个 abs_smux 概念里的
/// channel 进行的通信过程。
///
/// 简单的会话只包括一问一答。针对推送或者拉取类请求的会话，则会维持到推流结束。
pub trait TrMptpSession<C>
where
    C: client_config::TrConfig,
{
    type RespBodyRead<'f>: 'f + TrMptpBuffRead where Self: 'f;
    type RespSuffixRead<'f>: 'f + TrMptpBuffRead where Self: 'f;

    type Err: core::error::Error;

    fn state(&self) -> SessionState;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // send_req_body_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type SendReqBodyAsync<'f, R>: TrMayCancel<'f, MayCancelOutput =
        Result<SessionState, Self::Err>>
    where
        R: 'f + TrBuffRead<u8> + TrConsumerState,
        Self: 'f;

    fn send_req_body_async<'f, R>(
        &'f mut self,
        body: &'f mut R,
    ) -> Self::SendReqBodyAsync<'f, R>
    where
        R: TrBuffRead<u8> + TrConsumerState;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // send_req_suffix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type SendReqSuffixAsync<'f, R>: TrMayCancel<'f, MayCancelOutput =
        Result<SessionState, Self::Err>>
    where
        R: 'f + TrBuffRead<u8> + TrConsumerState,
        Self: 'f;

    fn send_req_suffix_async<'f, R>(
        &'f mut self,
        suffix: &'f mut R,
    ) -> Self::SendReqSuffixAsync<'f, R>
    where
        R: TrBuffRead<u8> + TrConsumerState;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_prefix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    /// 接收对端就本次请求给出的响应前缀（状态码 + 头）。
    ///
    /// 注意返回的是**响应**前缀（`RespPrefix`）：它与本端发出的请求前缀是两个独立的类型
    /// ——对端可能由另一份代码编译、独立升级。
    type RecvRespPrefixAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<client_config::RespPrefix<C>, Self::Err>>
    where
        Self: 'f;

    /// 接收对端就本次请求给出的响应前缀（状态码 + 头）。
    fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f>;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_body_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RecvRespBodyAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::RespBodyRead<'f>, Self::Err>>
    where
        Self: 'f;

    /// 读取回复体。如果在调用时并非处于能读取回复体的状态则报错。
    fn recv_resp_body_async<'f>(
        &'f mut self,
    ) -> Self::RecvRespBodyAsync<'f>;

    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
    // recv_resp_suffix_async
    // -- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

    type RecvSuffixAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::RespSuffixRead<'f>, Self::Err>>
    where
        Self: 'f;

    /// 读取本次回复的后缀流。如果在调用时并非处于能读取后缀流的状态则报错。
    fn recv_resp_suffix_async<'f>(
        &'f mut self,
    ) -> Self::RecvSuffixAsync<'f>;
}
