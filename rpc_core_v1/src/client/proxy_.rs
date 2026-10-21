//! 客户端极简代理与通用 unary 调用。
//!
//! 客户端侧**只保留一个**代理：[`ClientProxy`] 在运行期只持有 `&MptpClient<C>`，
//! interface 信息由零大小的类型参数 `I` 携带。它没有 `Client` 包装层，也没有
//! `base_path` 字段；一个代理可以实现协议端里的**所有** service trait。
//!
//! 一次 unary 调用由 [`call_unary_async`] 完成：拼路径 → 请求头声明分块 →
//! 编码器与 `send_req_body_async` 通过 ring 边编码边发送 → 检查状态 → 流式解码回复。
//! `#[rpc(service = "...")]` 宏生成的 service 方法体只是构造对应的 body enum 变体，
//! 然后把这一步交给本模块。
//!
//! # `proxy_`: the minimal client proxy and unary calls
//!
//! The client keeps exactly one proxy: [`ClientProxy`] holds only `&MptpClient<C>` at
//! runtime, while the interface is a zero-sized type parameter. [`call_unary_async`]
//! performs one unary exchange; macro-generated methods only build the body enum variant.

use core::{marker::PhantomData, mem::MaybeUninit};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use abs_mptp::{
    client::{TrMptpClient, TrMptpSession},
    spec::{
        Status, StdHeaderKey, StdHeaderVal,
        access_method::AccessMethod,
    },
};
use gen_mcf2::gen_may_cancel_future;
use mptp_smux::{
    client::MptpClient,
    messaging::{Headers, RequestPrefix},
};

use crate::{
    client as rpc_client,
    messaging::{RpcError, RpcResult, TrRpcInterface, concat_location},
    messaging::body_::{
        BodyRing_, K_BODY_RING_CAP_,
        decode_from_async_,
        encode_into_async_,
        read_body_async_,
    },
    x_deps::{abs_cancel, abs_mptp, gen_mcf2},
};

/// 客户端极简代理。
///
/// 运行期只有 `client_` 一个字段；`I` 是零大小的 interface marker，只用来携带
/// [`TrRpcInterface::PATH`]。它实现协议端声明的 service trait（由 `#[rpc]` 宏生成），
/// 因此调用点可以写成：
///
/// ```text
/// let proxy = client.call::<ApiV1>();
/// let goods = proxy.find("name").await?;
/// ```
pub struct ClientProxy<'a, C, I>
where
    C: rpc_client::TrRpcClientConfig,
    I: TrRpcInterface,
{
    /// 底层 MPTP 客户端。
    client_: &'a MptpClient<C>,

    /// interface marker。
    _iface_: PhantomData<I>,
}

impl<'a, C, I> ClientProxy<'a, C, I>
where
    C: rpc_client::TrRpcClientConfig,
    I: TrRpcInterface,
{
    /// 由底层客户端构造代理。
    ///
    /// 通常不需要直接调用：使用 [`TrRpcClientExt::call`] 或
    /// [`ServiceSelector::select`] 即可。
    pub const fn new(client: &'a MptpClient<C>) -> Self {
        ClientProxy {
            client_: client,
            _iface_: PhantomData,
        }
    }

    /// 取底层客户端。
    pub const fn client(&self) -> &'a MptpClient<C> {
        self.client_
    }
}

/// 让 `MptpClient` 直接获得 `call::<I>()`。
///
/// # Examples
///
/// ```
/// use core_rpc_mptp_smux_v1::{
///     TrRpcInterface,
///     client::TrRpcClientExt,
///     x_deps::mptp_smux::client::MptpClient,
/// };
///
/// struct ApiV1;
///
/// impl TrRpcInterface for ApiV1 {
///     const PATH: &'static str = "api/v1";
/// }
///
/// fn select<C>(client: &MptpClient<C>)
/// where
///     C: core_rpc_mptp_smux_v1::client::TrRpcClientConfig,
/// {
///     let _proxy = client.call::<ApiV1>();
/// }
/// ```
pub trait TrRpcClientExt<C>
where
    C: rpc_client::TrRpcClientConfig,
{
    /// 以 `I` 为 interface 选择一份客户端视图。
    fn call<I: TrRpcInterface>(&self) -> ClientProxy<'_, C, I>;
}

impl<C> TrRpcClientExt<C> for MptpClient<C>
where
    C: rpc_client::TrRpcClientConfig,
{
    fn call<I: TrRpcInterface>(&self) -> ClientProxy<'_, C, I> {
        ClientProxy::new(self)
    }
}

/// interface 到客户端视图的选择器。
///
/// 接口宏会为每个 `#[rpc(path = "...")]` 类型生成它的实现；`select` 的返回值就是
/// `ClientProxy<'a, C, I>`。它让调用点不必显式写 `call::<I>()`。
pub trait ServiceSelector<C>
where
    C: rpc_client::TrRpcClientConfig,
{
    /// 本 interface 的客户端视图。
    type View<'a> where Self: 'a, C: 'a;

    /// 从底层客户端选出本 interface 的视图。
    fn select<'a>(client: &'a MptpClient<C>) -> Self::View<'a>;
}

/// 一次 unary 调用的 step 函数。
///
/// `interface` 由类型参数 `I::PATH` 提供，`service` 是 trait 的 `SERVICE_PATH`；
/// 两者拼成 `/{interface}/{service}`。方法名只出现在 body enum 里，不参与路由。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(UnaryCall, pub(crate), new(pub(crate)))]
async fn unary_call_async_<'a, 'f, C, I, Req, Rep, TyTok>(
    proxy: &'f ClientProxy<'a, C, I>,
    service: &'static str,
    request: Req,
    cancel: TyTok,
) -> RpcResult<Rep>
where
    'a: 'f,
    C: rpc_client::TrRpcClientConfig,
    I: TrRpcInterface,
    Req: TrEncodable + 'f,
    Rep: TrDecodableOwned + 'f,
    TyTok: TrCancellationToken,
{
    let client = proxy.client();
    let location = concat_location(I::PATH, service);

    // 业务体不预编码：请求头只声明「分块」，编码器与 `send_req_body_async` 通过
    // 一对 SPSC ring 并发推进——编码写满时等待，发送搬走多少就再编多少。
    let mut headers = Headers::new();
    headers.set_num(
        StdHeaderKey::Body_Transfer,
        StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    let prefix =
        RequestPrefix::with_headers(AccessMethod::Call, location.clone(), Option::Some(headers));

    let mut session = client
        .request_async(prefix)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("发起 {location} 失败：{err}")))?;

    let mut ring = BodyRing_::new_unchecked([MaybeUninit::uninit(); K_BODY_RING_CAP_]);
    let (mut writer, mut reader) = BodyRing_::split(&mut ring);
    let encode_cancel = cancel.child_token();
    let encode_fut = async move {
        let result = encode_into_async_(&request, &mut writer, encode_cancel).await;
        // 无论编码成功还是失败都关闭写端：读端据此看到 EOF，不会永久等待。
        writer.close();
        result
    };
    let send_fut = session
        .send_req_body_async(&mut reader)
        .may_cancel_with(cancel.child_token());
    let (encode_res, send_res) = futures::future::join(encode_fut, send_fut).await;
    encode_res?;
    send_res.map_err(|err| RpcError::Transport(format!("发送 {location} 请求体失败：{err}")))?;

    let response = session
        .recv_resp_prefix_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("接收 {location} 回复前缀失败：{err}")))?;

    let mut body_read = session
        .recv_resp_body_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| RpcError::Transport(format!("取 {location} 回复体失败：{err}")))?;

    if response.status() != Status::Ok {
        let size = response
            .headers()
            .and_then(Headers::body_size)
            .unwrap_or(0usize);
        let body = read_body_async_(&mut body_read, size, cancel.child_token()).await?;
        return Result::Err(RpcError::Remote {
            status: response.status(),
            body,
        });
    }

    decode_from_async_::<Rep, _, _>(&mut body_read, cancel.child_token()).await
}

/// 发起一次 unary 调用，返回具名、可取消的 future。
///
/// 这是 `#[rpc(service = "...")]` 生成的客户端方法体使用的入口；普通使用者不需要
/// 直接调用它，直接调用 service trait 的方法即可。
///
/// # Errors
///
/// future 完成后返回：
///
/// - [`RpcError::Transport`]：建流、发前缀 / body 或收前缀 / body 失败；
/// - [`RpcError::Encode`] / [`RpcError::Decode`]：业务体编解码失败；
/// - [`RpcError::Remote`]：对端返回非 `200` 状态，`body` 保留原始错误体。
#[allow(clippy::needless_lifetimes)]
pub fn call_unary_async<'a, 'f, C, I, Req, Rep>(
    proxy: &'f ClientProxy<'a, C, I>,
    service: &'static str,
    request: Req,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Rep>> + 'f
where
    'a: 'f,
    C: crate::client::TrRpcClientConfig + 'f,
    I: TrRpcInterface + 'f,
    Req: TrEncodable + 'f,
    Rep: TrDecodableOwned + 'f,
{
    UnaryCallAsync::<'a, 'f, 'f, C, I, Req, Rep>::new(proxy, service, request)
}
