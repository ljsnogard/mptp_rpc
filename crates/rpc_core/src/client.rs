use core::{borrow::Borrow, marker::PhantomData, mem::MaybeUninit};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use abs_buff::{TrBuffRead, gen_may_cancel_future, x_deps::abs_iter};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_iter::TrAsSliceMut;
use buffex::x_deps::{abs_buff, abs_cancel};


use crate::{
    access_method::{AccessMethod, TrAccessMethod},
    messaging::{self, TrRpcMessage},
    specs::{Headers, StdHeaderVal},
    transport::{TrChannel, TrMuxConn},
};

#[derive(Debug, Error)]
pub enum OperationError {
    #[error("IO error: {0}")]
    IoErr(String),

    #[error("Rpc error: {0}")]
    RpcErr(String),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Async operation cancelled by user")]
    Cancelled,

    #[error("Error occurs during sending request: {0}")]
    ReqErr(String),

    #[error("Error occurs during recving response: {0}")]
    RespErr(String),
}

pub struct Client<TyBorrow, TyConn>
where
    TyBorrow: Borrow<TyConn>,
    TyConn: TrMuxConn,
{
    conn_: TyBorrow,
    _con_: PhantomData<TyConn>,
}

impl<TyBorrow, TyConn> Client<TyBorrow, TyConn>
where
    TyBorrow: Borrow<TyConn>,
    TyConn: TrMuxConn,
{
    pub const fn new(conn: TyBorrow) -> Self {
        Client {
            conn_: conn,
            _con_: PhantomData,
        }
    }
}

struct ReqBuilderInner {
    method: Option<AccessMethod>,
    path: Option<String>,
    headers: Option<Headers>,
}

impl ReqBuilderInner {
    const fn new() -> Self {
        ReqBuilderInner {
            method: Option::None,
            path: Option::None,
            headers: Option::None,
        }
    }
}

pub struct RequestBuilder(Box<ReqBuilderInner>);

impl RequestBuilder {
    pub fn new() -> Self {
        RequestBuilder(Box::new(ReqBuilderInner::new()))
    }

    pub fn builder<M: TrAccessMethod>() -> Self {
        Self::new().method(M::method())
    }

    pub fn method(mut self, access_method: AccessMethod) -> Self {
        self.0.method = Option::Some(access_method);
        self
    }

    pub fn path(mut self, path: &str) -> Self {
        self.0.path = Option::Some(path.to_string());
        self
    }

    pub fn headers(self, headers: Headers) -> Self {
        todo!()
    }

    pub fn body<T>(self, body: T) -> Self
    where
        T: Serialize + DeserializeOwned,
    {
        todo!()
    }
}

struct HeaderBuilderInner;

pub struct HeadersBuilder(Box<HeaderBuilderInner>);

/// 在一条信道上发送一个“无请求体”的请求，并接收回复。
///
/// 返回 `(回复头, 回复体)`：
/// - `Option::Some(body)`：回复头声明了回复体（且请求类型允许），
///   已按 `Body_Type` 解码为 `TyBody`；
/// - `Option::None`：回复没有声明回复体，或按协议不该有回复体。
#[gen_may_cancel_future(ChannelRequesNilBody)]
async fn channel_req_nil_body_async_<'f, TyChannel, TyCancel>(
    channel: &'f mut TyChannel,
    request: &'f messaging::Request,
    cancel: &'f mut TyCancel,
) -> Result<messaging::Response, ClientError>
where
    TyChannel: TrChannel,
    TyCancel: TrCancellationToken + Clone,
{
    let (mut tx, mut rx) = channel.split();
    let mut encode = messaging::Encode::new(request);
    let Option::Some(task) = encode.try_write(&mut tx) else {
        todo!()
    };
    let send_res = task.may_cancel_with(cancel).await;
    if let Result::Err(err) = send_res {
        return Result::Err(ClientError::ReqErr(err.to_string()));
    };
    let mut m_resp = MaybeUninit::<messaging::Response>::uninit();
    let resp_res = messaging::decode_msg_async_(&mut m_resp, &mut rx, cancel).await;
    let resp = match resp_res {
        Result::Err(err) => return Result::Err(ClientError::RespErr(err.to_string())),
        Result::Ok(resp) => resp,
    };

    // 根据“自身请求的类型”和“实际返回的回复头”决定是否进一步读取并解析回复体
    if should_read_response_body(request, &resp) {
        todo!()
    }

    // 走到这里说明回复头没有声明回复体（正常情况，回复到此结束）。
    //
    // 例外：若请求类型按协议不允许回复体（Head / Drop，见
    // `should_read_response_body`），而服务端仍然声明了回复体，则属于
    // 协议违规——这里直接报错，调用方应当放弃这条信道；
    // 残留的回复体字节不再消费，但信道被丢弃后不会影响其它信道的流对齐。
    if resp.try_get_body_type().is_some() || resp.try_get_body_size_str().is_some() {
        return Result::Err(ClientError::RespErr(
            "protocol violation: response declares a body for a request type that must not have one".to_string(),
        ));
    }
    // This is just to please the compiler. This function is not completed.
    Result::Err(ClientError::Cancelled)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Decode：解析完回复头之后，决定是否进一步读取并解析回复体
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 客户端在解析完回复头之后，是否需要进一步读取并解析回复体。
///
/// 意图：这是“回复体处理”的决策入口，判定同时参考两方面的信息——
///
/// 1. 实际返回的回复头（线级事实，见 messaging.rs 中 `TrRpcMessage` 的说明）：
///    - 服务端声明了 `Body_Type` 或 `Body_Size` 标准头 → 流上确实跟着回复体；
///    - 两者都没有 → 流上没有回复体，客户端绝不能继续读，否则会吞掉
///      下一条报文的字节，破坏信道的流对齐。
///
/// 2. 自身请求的类型（语义期待，见 access_method.rs 对各方法的注释）：
///    - Head / Drop：按协议回复不带本体内容；即使服务端声明了回复体也视为
///      协议违规，客户端不应把它当作回复体解析（调用处会将其判为错误）；
///    - View / Pull / Call：回复体就是客户端索要的结果 → 声明了就读取并解析；
///    - Post / Push：正常路径不期待回复体，但服务端可能附带错误详情等
///      附加内容 → 声明了就读取并解析，由调用方决定是否使用。
pub fn should_read_response_body(request: &messaging::Request, resp: &messaging::Response) -> bool {
    // 服务端未在回复头中声明回复体 → 一定没有回复体
    if resp.try_get_body_type().is_none() && resp.try_get_body_size_str().is_none() {
        return false;
    }
    // 服务端声明了回复体 → 再按自身请求的类型决定
    match request.method() {
        // 按协议这两个方法不带本体内容；若服务端仍声明了回复体，视为协议违规
        AccessMethod::Head | AccessMethod::Drop => false,
        // 其余方法：声明了回复体就读取并解析
        AccessMethod::View
        | AccessMethod::Post
        | AccessMethod::Push
        | AccessMethod::Pull
        | AccessMethod::Call => true,
    }
}
