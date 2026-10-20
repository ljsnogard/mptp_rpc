use abs_cancel::TrMayCancel;
use abs_mptp::spec::{
    Status,
    access_method::{AccessMethod, TrAccessMethod},
};

use crate::x_deps::{abs_cancel, abs_mptp};
use super::config::{
    Dock, ReqHeaders, RespHeaders,
};

/// 一次 RPC 请求的形状：**位置、方法、头，以及业务体的类型**。
///
/// 它描述的是「这次访问要做什么」，不是「报文怎么上线」——后者由 MPTP 层负责。体在类型上
/// 是业务类型（[`TrRpcRequest::Body`]），MPTP 层完全不需要理解它。
pub trait TrRpcRequest<C>
where
    C: super::config::TrRpcConfig,
{
    /// 本条请求携带的业务体类型；没有体时用该层的「无体」标记。
    type Body;

    fn remote_dock(&self) -> Dock<C>;

    fn access_method(&self) -> AccessMethod;

    fn location(&self) -> &str;

    fn headers(&self) -> &ReqHeaders<C>;

    /// 借出业务体；没有体时返回 `None`。
    fn body(&self) -> Option<&Self::Body>;
}

/// 一次 RPC 答复的形状：**状态、以及业务体的类型**。
///
/// 它描述的是「访问得到了什么」，不是「答复如何解析」——后者由 MPTP 层负责。体在类型上
/// 是业务类型（[`TrRpcResponse::Body`]），MPTP 层完全不需要理解它。
pub trait TrRpcResponse<C>
where
    C: super::config::TrRpcConfig,
{
    type Body;

    fn status(&self) -> Status;

    fn headers(&self) -> &RespHeaders<C>;

    fn body(&self) -> Option<&Self::Body>;
}


pub trait TrRpcSession<M, C>
where
    M: TrAccessMethod,
    C: super::config::TrRpcConfig,
{
    fn access_method() -> AccessMethod {
        M::method()
    }

    fn remote_dock(&self) -> Dock<C>;

    fn location(&self) -> &str;
}


/// A client for resource related request and response. These requests
/// are typically with access method of `HEAD`, `View`, `POST`, `DROP`.
pub trait TrRespClient<M, C>
where
    M: TrAccessMethod,
    C: super::config::TrRpcConfig,
{
    type Req: TrRpcRequest<C>;
    type Resp;
    type Err: core::error::Error;

    type RequestAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Resp, Self::Err>>
    where
        Self: 'f;

    /// 发送请求，并在收到完整的答复后就关闭会话，忽略可能的后缀流。
    ///
    /// 适合一问一答的访问类型，例如 HEAD, View, POST, DROP。
    fn resp_for_req_async<'f>(
        &'f self,
        request: &'f Self::Req,
    ) -> Self::RequestAsync<'f>;
}

/// A client for session related request and response. These requests
/// are typically with access method of `PULL`, `PUSH`, and sometimes
/// `CALL`.
pub trait TrSessClient<M, C>
where
    M: TrAccessMethod,
    C: super::config::TrRpcConfig,
{
    type Req: TrRpcRequest<C>;
    type Sess: TrRpcSession<M, C>;
    type Err: core::error::Error;

    type InitAsync<'f>: TrMayCancel<'f, MayCancelOutput =
        Result<Self::Sess, Self::Err>>
    where
        Self: 'f;

    /// 发送请求的完整流程，开启一个会话对象来处理请求的后缀流等。如果请求
    /// 的类型只有答复，则返回一个只能读取答复的 session。
    ///
    /// 适合涉及推流、拉流等动作的访问类型，例如 PUSH, PULL, CALL。
    fn sess_for_req_async<'f>(
        &'f self,
        request: &'f Self::Req,
    ) -> Self::InitAsync<'f>;
}
