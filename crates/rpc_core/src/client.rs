use core::{
    borrow::Borrow,
    marker::PhantomData,
    mem::MaybeUninit,
};

use thiserror::Error;

use serde::de::DeserializeOwned;

use abs_cancel::{TrCancellationToken, TrMayCancel};

use abs_buff::{
    BuffReadAsInput, TrBuffRead, x_deps::{abs_cancel, abs_iter::TrAsSliceMut},
};

use crate::{
    access_method::AccessMethod,
    messaging,
    conn::{TrMuxConn, TrChannel},
    specs::StdHeaderVal,
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

pub(crate) struct RequestBuilder;

pub struct HeadRequestBuilder;

pub struct ViewRequestBuilder;

pub struct PostRequestBuilder;

pub struct DropRequestBuilder;

pub struct PushRequestBuilder;

pub struct PullRequestBuilder;

pub struct CallRequestBuilder;


/// 在一条信道上发送一个“无请求体”的请求，并接收回复。
///
/// 返回 `(回复头, 回复体)`：
/// - `Option::Some(body)`：回复头声明了回复体（且请求类型允许），
///   已按 `Body_Type` 解码为 `TyBody`；
/// - `Option::None`：回复没有声明回复体，或按协议不该有回复体。
///
/// 为什么不用 `#[gen_may_cancel_future]`：该宏要求最后一个泛型参数是
/// 取消令牌类型，并且无法表达“只出现在返回类型里的泛型”——而 `TyBody`
/// 正是这样的泛型（见 gen_mcf_macro 的约定）。这里直接写成普通 async
/// 函数，await 点仍然通过 `may_cancel_with(cancel)` 支持取消。
async fn channel_req_nil_body_async_<'f, TyChannel, TyBody, TyCancel>(
    channel: &'f mut TyChannel,
    request: &'f messaging::Request,
    cancel: &'f mut TyCancel,
) -> Result<(messaging::Response, Option<TyBody>), ClientError>
where
    TyChannel: TrChannel,
    TyBody: DeserializeOwned,
    TyCancel: TrCancellationToken,
{
    let (mut tx, mut rx) = channel.split();
    let mut encode = messaging::EncoderNilBody::new(request);
    let Option::Some(task) = encode.try_write(&mut tx) else {
        todo!()
    };
    let send_res = task.may_cancel_with(cancel).await;
    if let Result::Err(err) = send_res {
        return Result::Err(ClientError::ReqErr(err.to_string()))
    };
    let mut m_resp = MaybeUninit::<messaging::Response>::uninit();
    let resp_res = messaging::decode_msg_async_(&mut m_resp, &mut rx, cancel).await;
    let resp = match resp_res {
        Result::Err(err) => return Result::Err(ClientError::RespErr(err.to_string())),
        Result::Ok(resp) => resp,
    };

    // 根据“自身请求的类型”和“实际返回的回复头”决定是否进一步读取并解析回复体
    if should_read_response_body(request, &resp) {
        // 服务端声明了回复体（且请求类型允许）→ 按 Body_Size 读出字节，
        // 再按 Body_Type 解码成调用方期望的 TyBody
        let body: TyBody = decode_response_body(&mut rx, &resp, cancel).await?;
        return Result::Ok((resp, Option::Some(body)));
    }

    // 走到这里说明回复头没有声明回复体（正常情况，回复到此结束）。
    //
    // 例外：若请求类型按协议不允许回复体（Head / Drop，见
    // `should_read_response_body`），而服务端仍然声明了回复体，则属于
    // 协议违规——这里直接报错，调用方应当放弃这条信道；
    // 残留的回复体字节不再消费，但信道被丢弃后不会影响其它信道的流对齐。
    if resp.try_get_body_type().is_some() || resp.try_get_body_size_str().is_some() {
        return Result::Err(ClientError::RespErr(
            "protocol violation: response declares a body for a request type that must not have one"
                .to_string(),
        ));
    }
    Result::Ok((resp, Option::None))
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
pub fn should_read_response_body(
    request: &messaging::Request,
    resp: &messaging::Response,
) -> bool {
    // 服务端未在回复头中声明回复体 → 一定没有回复体
    if resp.try_get_body_type().is_none() && resp.try_get_body_size_str().is_none() {
        return false;
    }
    // 服务端声明了回复体 → 再按自身请求的类型决定
    match request.access_method() {
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

/// 从回复流中读取并解析回复体。
///
/// 意图：
/// - 读取的字节数以回复头 `Body_Size` 声明的长度为准（没有该头就无法确定
///   回复体的边界，按协议属于坏报文）；
/// - 解码方式以回复头 `Body_Type` 声明的 MIME 类型为准：当前支持 MsgPack
///   （`StdHeaderVal::Mime_Body_Type_MsgPack`），JSON 留待后续实现；
/// - 用 `AsStdRead` 把 `TrBuffTryRead` 包装成 `std::io::Read`，与 messaging.rs
///   解析回复头的方式保持一致。
///
/// 为什么是同步函数：读取走的是 `TrBuffTryRead::try_read`（同步的“尝试读”），
/// 整个函数没有 await 点；取消仍然生效——`cancel` 传入 `AsStdRead` 后，
/// 每次循环读取都会检查取消标志。
async fn decode_response_body<'f, TyRx, TyCancel, TyBody>(
    rx: &'f mut TyRx,
    resp: &'f messaging::Response,
    cancel: &'f mut TyCancel,
) -> Result<TyBody, ClientError>
where
    TyRx: TrBuffRead,
    TyCancel: TrCancellationToken,
    TyBody: DeserializeOwned,
{
    // 1. 取 Body_Size：确定回复体的字节边界
    let body_size = resp.try_get_body_size().ok_or_else(|| {
        ClientError::RespErr(
            "response declares a body but Body_Size header is missing or invalid".to_string(),
        )
    })?;

    // 2. 取 Body_Type：决定用什么格式解码
    let body_type = resp.try_get_body_type().ok_or_else(|| {
        ClientError::RespErr(
            "response declares a body but Body_Type header is missing".to_string(),
        )
    })?;

    // 3. 按声明的长度从流中读出回复体字节
    let mut buf = std::vec::Vec::<u8>::new();
    buf.resize(body_size, 0u8);

    let read_buf = unsafe {
        let p = buf.as_slice_mut() as *mut [u8] as *mut [MaybeUninit<u8>];
        &mut *p
    };
    let mut buff_read_as_input = BuffReadAsInput::<&'_ mut TyRx, TyRx, u8>::new(rx);
    let read_res = buff_read_as_input
        .read_async(read_buf)
        .may_cancel_with(cancel)
        .await;
    let mut got = 0usize;
    if let Option::Some(read_len) = read_res.as_ref().pick_left() {
        got = *read_len;
    };
    if got < body_size {
        // 服务端声明的长度大于实际能读到的字节数 → 截断的报文
        return Result::Err(ClientError::RespErr(format!(
            "response body truncated: declared {body_size} bytes, got {got}"
        )));
    }

    // 4. 按 Body_Type 声明的 MIME 类型解码
    if body_type == &StdHeaderVal::Mime_Body_Type_MsgPack.into() {
        rmp_serde::from_slice::<TyBody>(&buf)
            .map_err(|e| ClientError::RespErr(format!("failed to decode msgpack body: {e}")))
    } else if body_type == &StdHeaderVal::Mime_Body_Type_Json.into() {
        // 意图：JSON 格式的回复体需要引入 serde_json 依赖后再实现，
        // 解码方式与 MsgPack 相同：先按 Body_Size 读出字节，再反序列化为 TyBody。
        todo!("decode JSON body: add serde_json and deserialize the Body_Size bytes into TyBody")
    } else {
        Result::Err(ClientError::RespErr(format!(
            "unsupported body MIME type: {body_type:?}"
        )))
    }
}
