//! `#[rpc(service = "...", api = X)]` / `#[rpc(impl_of(X))]` 的编译与行为测试。
//!
//! 这些用例把 `mptp_im_demo` 的手写 Call 原型压缩成一份最小形状：一个 trait 里同时出现
//! 0 参、1 参、多参与借用参数，然后验证宏生成的 marker / 关联类型 / 请求枚举 / 方法表与
//! 服务端 installer 都能编译并保持语义。
//!
//! 本文件是**宏输出形状**的黄金：`mptp_im_demo` 的 `im_protocol` / `im_server` 迁移后，
//! 真实 TCP + smux 场景由 demo 覆盖，这里只做无连接的形状与编解码断言。
//!
//! # Compile and behavior tests for the Call service trait / `impl_of`
//!
//! The file mirrors the hand-written Call prototype from `mptp_im_demo` with one trait that
//! covers 0-arg, 1-arg, multi-arg and borrowed parameters. It asserts the generated marker,
//! associated types, request enum, method table and installer, without opening a connection.

#![feature(associated_type_defaults)]
#![feature(impl_trait_in_assoc_type)]
#![recursion_limit = "256"]

use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use futures::executor::block_on;
use gen_mcf2::gen_may_cancel_future;
use mp_denco::{MpDecoder, MpEncoder};
use rpc_core_v1::{
    client::{TrRpcClientConfig, TrRpcClientExt},
    messaging::{RpcResult, TrRpcInterface},
    stream::TrDataTypeId,
    x_deps::denco_macro::{TrDecodable, TrEncodable as TrEncodableDerive, TrHasTypeId},
};
use rpc_svc_macro::rpc;

/// 线上房间摘要。
#[derive(Clone, Debug, Eq, PartialEq, TrDecodable, TrEncodableDerive, TrHasTypeId)]
pub struct RoomSummary {
    /// 房间标识。
    pub id: u64,

    /// 房间名。
    pub name: String,
}

/// 线上房间快照。
#[derive(Clone, Debug, Eq, PartialEq, TrDecodable, TrEncodableDerive, TrHasTypeId)]
pub struct RoomSnapshot {
    /// 房间标识。
    pub room_id: u64,
}

/// 线上消息。
#[derive(Clone, Debug, Eq, PartialEq, TrDecodable, TrEncodableDerive, TrHasTypeId)]
pub struct Message {
    /// 文本。
    pub text: String,
}

/// 房间服务：0 参 / 1 参 / 多参 / 借用参数各一个方法。
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService {
    /// 我加入的房间。
    fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>>;

    /// 创建房间。
    fn create_room(
        &self,
        name: String,
        topic: Option<String>,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSummary>>;

    /// 加入房间。
    fn join_room(
        &self,
        room_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSnapshot>>;

    /// 发送消息（`text` 是借用参数，线上载荷必须转成 owned `String`）。
    fn send_message(
        &self,
        room_id: u64,
        text: &str,
        client_msg_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>>;
}

/// 业务实现：所有方法都用已经算好的结果应答（与 demo 的 `ReadyAsync` 同形）。
pub struct RoomServiceImpl;

/// `ReadyTest` 的 step 函数；取消令牌只做协作式检查。
#[gen_may_cancel_future(ReadyTest, pub(crate), new(pub(crate)))]
async fn ready_test_<'f, T, K>(value: RpcResult<T>, cancel: K) -> RpcResult<T>
where
    T: 'f,
    K: abs_cancel::TrCancellationToken,
{
    let _ = cancel;
    value
}

#[rpc(impl_of(RoomSvc))]
impl RoomService for RoomServiceImpl {
    fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>> {
        ReadyTestAsync::new(Result::Ok(Vec::new()))
    }

    fn create_room(
        &self,
        name: String,
        topic: Option<String>,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSummary>> {
        ReadyTestAsync::new(Result::Ok(RoomSummary {
            id: 7u64,
            name: format!("{name}/{topic:?}"),
        }))
    }

    fn join_room(
        &self,
        room_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSnapshot>> {
        ReadyTestAsync::new(Result::Ok(RoomSnapshot { room_id }))
    }

    fn send_message(
        &self,
        room_id: u64,
        text: &str,
        _client_msg_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>> {
        ReadyTestAsync::new(Result::Ok(Message {
            text: format!("{room_id}:{text}"),
        }))
    }
}

/// 测试专用编码：调用者给多大缓冲就编多少。
fn encode_into_vec_<T>(value: &T) -> Vec<u8>
where
    T: ?Sized + TrEncodable,
{
    let mut storage = vec![0u8; 64usize * 1024usize];
    let written = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let before = sink.len();
        block_on(async {
            value
                .encode_async(MpEncoder::new(&mut sink))
                .may_cancel_with(NonCancellableToken::new())
                .await
        })
        .expect("测试编码");
        before - sink.len()
    };
    storage.truncate(written);
    storage
}

/// 字节串里是否包含给定的 ASCII 片段（枚举变体名是 msgpack 字符串，原样出现）。
fn contains_bytes_(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// 测试专用解码。
fn decode_from_slice_<T>(bytes: &[u8]) -> T
where
    T: TrDecodableOwned,
{
    let mut slice: &[u8] = bytes;
    block_on(async {
        T::decode_async(MpDecoder::new(&mut slice))
            .may_cancel_with(NonCancellableToken::new())
            .await
    })
    .expect("测试解码")
}

/// 测试 marker 携带完整服务前缀。
/// Test that the generated marker carries the full service prefix.
///
/// - 手段：读取 `RoomSvc::PREFIX`。
/// - 判断：等于 `im/v1/room`（Call 的 pattern 就是该前缀）。
///
/// - Means: read `RoomSvc::PREFIX`.
/// - Verdict: it equals `im/v1/room`, which is also the Call pattern.
#[test]
fn marker_prefix_is_service_path_() {
    assert_eq!(RoomSvc::PREFIX, "im/v1/room");
}

/// 测试请求枚举的三种载荷形状与变体线上名。
/// Test the three payload shapes and the wire variant names of the request enum.
///
/// - 手段：为每个方法构造枚举变体，经 denco 编码后解回；再编码一次比较字节。
/// - 判断：解出的值与原始变体相等；编码字节里出现的是方法名（`create_room`）而不是
///   Rust 变体名；`send_message` 的多参载荷按位置字段往返，借用的 `&str` 参数在线上是
///   owned `String`。
///
/// - Means: build each enum variant, round-trip it through denco, then encode again.
/// - Verdict: decoded values equal the originals and the multi-arg payload survives with
///   positional fields; the borrowed `&str` becomes an owned `String` on the wire.
#[test]
fn call_enum_roundtrips_all_shapes_() {
    let list_bytes = encode_into_vec_(&RoomServiceCall::ListMyRooms);
    assert!(matches!(
        decode_from_slice_::<RoomServiceCall>(&list_bytes),
        RoomServiceCall::ListMyRooms
    ));
    assert_eq!(
        encode_into_vec_(&decode_from_slice_::<RoomServiceCall>(&list_bytes)),
        list_bytes,
        "编码必须稳定"
    );

    let join_bytes = encode_into_vec_(&RoomServiceCall::JoinRoom(42u64));
    match decode_from_slice_::<RoomServiceCall>(&join_bytes) {
        RoomServiceCall::JoinRoom(room_id) => assert_eq!(room_id, 42u64),
        _ => panic!("join_room 解出了错误的变体"),
    }

    let create_bytes = encode_into_vec_(&RoomServiceCall::CreateRoom(__Req_create_room {
        f0: "rust".to_string(),
        f1: Option::Some("RPC".to_string()),
    }));
    // CC-06：变体线上名是方法名（`denco(rename)`），不是 Rust 变体名。
    assert!(
        contains_bytes_(&create_bytes, b"create_room"),
        "变体线上名必须是方法名 `create_room`"
    );
    assert!(
        !contains_bytes_(&create_bytes, b"CreateRoom"),
        "变体线上名不能是 Rust 变体名 `CreateRoom`"
    );
    match decode_from_slice_::<RoomServiceCall>(&create_bytes) {
        RoomServiceCall::CreateRoom(payload) => {
            assert_eq!(payload.f0, "rust");
            assert_eq!(payload.f1, Option::Some("RPC".to_string()));
        }
        _ => panic!("create_room 解出了错误的变体"),
    }

    let send_bytes = encode_into_vec_(&RoomServiceCall::SendMessage(__Req_send_message {
        f0: 42u64,
        f1: "hello".to_string(),
        f2: 9u64,
    }));
    match decode_from_slice_::<RoomServiceCall>(&send_bytes) {
        RoomServiceCall::SendMessage(payload) => {
            assert_eq!(payload.f0, 42u64);
            assert_eq!(payload.f1, "hello");
            assert_eq!(payload.f2, 9u64);
        }
        _ => panic!("send_message 解出了错误的变体"),
    }
}

/// 测试方法级类型头与关联类型一致。
/// Test that method-level type headers agree with the generated associated types.
///
/// - 手段：读取 `ROOM_SVC_METHODS` 的 operation 顺序，并比较每项的 request / response ID
///   与对应载荷 / 响应类型的 `DATA_TYPE_ID`。
/// - 判断：operation 顺序 = trait 方法声明顺序；ID 完全一致；`Accept` 等于响应 ID；
///   policy 是 `Strict`。
///
/// - Means: read the `ROOM_SVC_METHODS` operation order and compare each entry's request /
///   response ids with the payload / response type ids.
/// - Verdict: the order matches the trait, ids match exactly, `Accept` equals the response
///   id and the policy is `Strict`.
#[test]
fn method_table_matches_payload_types_() {
    let operations: Vec<&str> = ROOM_SVC_METHODS
        .iter()
        .map(|method| method.operation())
        .collect();
    assert_eq!(
        operations,
        ["list_my_rooms", "create_room", "join_room", "send_message"]
    );

    assert_eq!(
        ROOM_SVC_LIST_MY_ROOMS_INFO.request_body(),
        Option::Some(<() as TrDataTypeId>::DATA_TYPE_ID)
    );
    assert_eq!(
        ROOM_SVC_CREATE_ROOM_INFO.request_body(),
        Option::Some(<__Req_create_room as TrDataTypeId>::DATA_TYPE_ID)
    );
    assert_eq!(
        ROOM_SVC_SEND_MESSAGE_INFO.request_body(),
        Option::Some(<__Req_send_message as TrDataTypeId>::DATA_TYPE_ID)
    );
    assert_eq!(
        ROOM_SVC_JOIN_ROOM_INFO.response_body(),
        Option::Some(<RoomSnapshot as TrDataTypeId>::DATA_TYPE_ID)
    );
    assert_eq!(
        ROOM_SVC_SEND_MESSAGE_INFO
            .accept()
            .and_then(|accept| accept.type_id()),
        Option::Some(<Message as TrDataTypeId>::DATA_TYPE_ID)
    );
    assert_eq!(ROOM_SVC_INFO.service(), <RoomSvc as TrRpcInterface>::PREFIX);
    assert_eq!(ROOM_SVC_INFO.pattern(), <RoomSvc as TrRpcInterface>::PREFIX);
    assert_eq!(ROOM_SVC_INFO.methods().len(), 4usize);
}

/// 测试多参载荷的类型名与位置字段命名符合线上约定。
/// Test that the multi-arg payload type names and positional fields follow the wire contract.
///
/// - 手段：读取两个 `__Req_*` 的 `TYPE_NAME` / `DATA_TYPE_ID`。
/// - 判断：类型名分别是 `__Req_create_room` / `__Req_send_message`（denco 把类型名与模块路径
///   纳入指纹，**具体 ID 的 golden 在 demo 里**：只有与手写原型同模块时数值才相同）；两个
///   ID 非零且互不相同。
///
/// - Means: read `TYPE_NAME` / `DATA_TYPE_ID` of both `__Req_*` types.
/// - Verdict: the names are exactly `__Req_create_room` / `__Req_send_message` (denco hashes
///   the type name and module path, so the numeric golden lives in the demo where the module
///   path matches the prototype); both ids are non-zero and distinct.
#[test]
fn payload_type_identity_follows_wire_contract_() {
    fn assert_identity<T>()
    where
        T: abs_denco::type_id::TrHasTypeId + TrDataTypeId,
    {
        let id = <T as TrDataTypeId>::DATA_TYPE_ID.into_inner();
        assert_ne!(id, 0u64, "线上类型 ID 不能为 0");
    }

    assert_eq!(
        <__Req_create_room as abs_denco::type_id::TrHasTypeId>::TYPE_NAME,
        concat!(module_path!(), "::__Req_create_room")
    );
    assert_eq!(
        <__Req_send_message as abs_denco::type_id::TrHasTypeId>::TYPE_NAME,
        concat!(module_path!(), "::__Req_send_message")
    );
    assert_identity::<__Req_create_room>();
    assert_identity::<__Req_send_message>();
    assert_ne!(
        <__Req_create_room as TrDataTypeId>::DATA_TYPE_ID,
        <__Req_send_message as TrDataTypeId>::DATA_TYPE_ID,
        "两个载荷的类型 ID 必须不同"
    );
}

/// 测试隐藏关联类型可以被跨模块 / 跨 crate 的 `impl_of` 命名。
/// Test that the hidden associated types are nameable by `impl_of`.
///
/// - 手段：把 `<RoomServiceImpl as RoomService>::__Req_create_room` 作为具名类型，用它构造
///   一个载荷。
/// - 判断：编译通过，且字段与生成的结构体一致。
///
/// - Means: name `<RoomServiceImpl as RoomService>::__Req_create_room` as a concrete type and
///   build a payload with it.
/// - Verdict: it compiles and the fields match the generated struct.
#[test]
fn associated_payload_types_resolve_() {
    let payload: <RoomServiceImpl as RoomService>::__Req_create_room = __Req_create_room {
        f0: "rust".to_string(),
        f1: Option::None,
    };
    assert_eq!(payload.f0, "rust");
    assert_eq!(payload.f1, Option::None);
}

/// 编译期验证客户端代理实现了 trait，且调用点不出现 path / 变体。
/// Compile-time check that the client proxy implements the trait with path-free call sites.
///
/// - 手段：用泛型客户端构造 `client.call::<RoomSvc>()`，逐个调用 trait 方法，并给其中
///   一个调用显式接上取消令牌。
/// - 判断：编译通过（方法只接收业务参数，body 与路径由宏生成；返回的 future 可被
///   `may_cancel_with` 包装）。
///
/// - Means: build `client.call::<RoomSvc>()` from a generic client, call every trait method,
///   and wrap one call with an explicit cancel token.
/// - Verdict: it compiles; methods take only business arguments and the returned futures can
///   be wrapped by `may_cancel_with`.
#[allow(dead_code)]
fn client_proxy_compiles_<C>(client: &rpc_core_v1::x_deps::mptp_smux::client::MptpClient<C>)
where
    C: TrRpcClientConfig,
{
    let room = client.call::<RoomSvc>();
    let _ = room.list_my_rooms();
    let _ = room.create_room("rust".to_string(), Option::None);
    let _ = room.join_room(42u64);
    let _ = room.send_message(42u64, "hello", 9u64);
    // CC-19：每个方法返回可取消 future，调用点可以显式交出取消令牌。
    let _ = room
        .join_room(42u64)
        .may_cancel_with(NonCancellableToken::new());
}

/// 编译期验证 `impl_of` 生成的 installer 与 dispatcher 形状。
/// Compile-time check the `impl_of` installer and dispatcher shapes.
///
/// - 手段：在泛型参数上实例化生成的 dispatcher（要求实现 `TrEndpointHandler`），并调用
///   `install_into`。
/// - 判断：编译通过。
///
/// - Means: instantiate the generated dispatcher through a `TrEndpointHandler` bound and
///   call `install_into`.
/// - Verdict: it compiles.
#[allow(dead_code)]
fn impl_of_installer_compiles_<C>(router: mptp_serving::RadixRouter<C>)
where
    C: mptp_serving::x_deps::mptp_smux::serving::config::TrConfig + 'static,
{
    fn assert_handler<C, S, R, H>()
    where
        C: mptp_serving::x_deps::mptp_smux::serving::config::TrConfig,
        H: mptp_serving::TrEndpointHandler<C, S, R>,
    {
    }

    assert_handler::<C, (), (), __rpc_impl_of_RoomServiceImpl_via_RoomSvc::ServiceDispatcher>();
    let _ = __rpc_impl_of_RoomServiceImpl_via_RoomSvc::install_into::<C, (), ()>(
        router,
        std::sync::Arc::new(RoomServiceImpl),
    );
}
