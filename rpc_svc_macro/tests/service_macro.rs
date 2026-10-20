//! `#[rpc(service = "...")]` 的编译与行为测试。
//!
//! 这些用例真实编译宏生成的四块代码：trait 常量、两个 body enum、客户端代理 impl、
//! 服务端 dispatch。它们不建立真实连接；收发路径由 core 的单元测试覆盖。
//!
//! # Compile and behavior tests for `#[rpc(service = "...")]`
//!
//! These tests compile all four generated pieces: the trait constant, both body enums,
//! the client proxy impl and the serving dispatch. No real connection is opened.

#![feature(impl_trait_in_assoc_type)]
#![recursion_limit = "256"]

use core::{
    future::{Future, IntoFuture},
    pin::pin,
    task::{Context, Poll, Waker},
};

use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{TrDecodable, TrDecodableOwned},
    ser::TrEncodable,
};
use core_rpc_mptp_smux_v1::{
    RpcResult, TrRpcInterface,
    client::{ClientProxy, ServiceSelector, TrRpcClientConfig},
    serving::{RpcCallCtx, RpcRouter, ServiceRoute, TrServingConfig},
    x_deps::mptp_smux::client::MptpClient,
};
use gen_mcf2::gen_may_cancel_future;
use mp_denco::{MpDecoder, MpEncoder};
use rpc_svc_macro::rpc;

/// 业务载体。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct Item {
    /// 商品 ID。
    pub id: u64,

    /// 商品名。
    pub name: String,
}

/// 商品服务：同时覆盖无参、借用参数与按值参数。
#[rpc(service = "inventory")]
pub trait InventoryService: Send + Sync {
    /// 一个带默认值的关联常量，宏应当原样保留。
    const DEFAULT_LIMIT: u32 = 32u32;

    /// 列出全部商品。
    fn list(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Item>>>;

    /// 按名字查找。
    fn find<'f>(
        &'f self,
        name: &'f str,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Item>>>;

    /// 按值创建一个商品。
    fn create(&self, item: Item) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>>;

    /// 按 ID 删除。
    fn remove(&self, id: u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<bool>>;

    /// `&String` 借用参数。
    #[allow(clippy::ptr_arg)]
    fn label(&self, name: &String) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<String>>;

    /// `&[u8]` 借用参数。
    fn checksum(&self, bytes: &[u8]) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>>;

    /// `&Copy` 标量借用参数。
    fn bump(&self, id: &u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>>;
}

/// 系统服务：所有参数都是按值类型，客户端 body enum 不需要生命周期参数。
#[rpc(service = "system")]
pub trait SystemService {
    /// 健康检查。
    fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<bool>>;

    /// 两数相加。
    fn add(&self, a: u32, b: u32) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u32>>;
}

/// 嵌套模块里的 service，用来验证 interface 能引用别的模块的 trait。
///
/// Nested service used to verify that an interface can list a trait from another module.
pub mod nested {
    use super::*;

    /// 回显一个整数。
    #[rpc(service = "nested")]
    pub trait NestedService {
        /// 回显。
        fn echo(&self, value: u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>>;
    }
}

/// 商品服务与系统服务组成的 interface；验证 interface 宏的生成代码。
#[rpc(path = "api/v1", services = [InventoryService, SystemService])]
pub struct ApiV1;

/// 只含一个跨模块 service 的 interface，验证 hidden module 路径推导。
#[rpc(path = "api/v2", services = [nested::NestedService])]
pub struct ApiV2;

/// 三个 service 的 interface，用来验证 worker 组的嵌套 `try_join` 树。
#[rpc(
    path = "api/v3",
    services = [InventoryService, SystemService, nested::NestedService],
)]
pub struct ApiV3;

/// 只用于验证 `SERVICE_PATH` 注入的占位实现。
struct DummyInventory;

impl InventoryService for DummyInventory {
    fn list(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Item>>> {
        DummyOkAsync::<'_, '_, Vec<Item>>::new(Vec::new())
    }

    fn find<'f>(
        &'f self,
        _name: &'f str,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Item>>> {
        DummyOkAsync::<'f, 'f, Option<Item>>::new(None)
    }

    fn create(&self, _item: Item) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>> {
        DummyOkAsync::<'_, '_, u64>::new(1u64)
    }

    fn remove(&self, _id: u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<bool>> {
        DummyOkAsync::<'_, '_, bool>::new(true)
    }

    fn label(&self, name: &String) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<String>> {
        DummyOkAsync::<'_, '_, String>::new(name.clone())
    }

    fn checksum(&self, _bytes: &[u8]) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>> {
        DummyOkAsync::<'_, '_, u64>::new(0u64)
    }

    fn bump(&self, id: &u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>> {
        DummyOkAsync::<'_, '_, u64>::new(*id)
    }
}

/// 测试用的立即成功 future；让占位实现返回真正的 `TrMayCancel` 类型。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(DummyOk, pub(crate), new(pub(crate)))]
async fn dummy_ok_<'f, T, TyTok>(value: T, cancel: TyTok) -> RpcResult<T>
where
    T: 'f,
    TyTok: TrCancellationToken,
{
    let _ = cancel;
    Result::Ok(value)
}

/// 用立即完成的执行器驱动纯内存 future。
fn block_on_<TyFut>(future: TyFut) -> TyFut::Output
where
    TyFut: Future,
{
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

/// 把值编码成一段内存字节。
fn encode_to_vec_<T>(value: &T) -> Vec<u8>
where
    T: ?Sized + TrEncodable,
{
    let mut storage = vec![0u8; 4096usize];
    let written = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        block_on_(
            value
                .encode_async(MpEncoder::new(&mut sink))
                .may_cancel_with(NonCancellableToken::new())
                .into_future(),
        )
        .expect("编码应当成功");
        4096usize - sink.len()
    };
    storage.truncate(written);
    storage
}

/// 从内存字节解出一个 owned 值。
fn decode_owned_<'de, T>(bytes: &'de [u8]) -> T
where
    T: TrDecodableOwned,
{
    let mut slice: &'de [u8] = bytes;
    block_on_(
        <T as TrDecodable<'de>>::decode_async(MpDecoder::new(&mut slice))
            .may_cancel_with(NonCancellableToken::new())
            .into_future(),
    )
    .expect("解码应当成功")
}

/// 测试宏把 `service = "..."` 注入成 `SERVICE_PATH`。
/// Test that the macro injects `service = "..."` as `SERVICE_PATH`.
///
/// - 手段：为两个 service 分别写占位实现，读取关联常量。
/// - 判断：常量值分别等于 `inventory` 与 `system`。
///
/// - Means: implement both services as placeholders and read the associated constants.
/// - Verdict: they equal `inventory` and `system` respectively.
#[test]
fn injects_service_path_() {
    assert_eq!(DummyInventory::SERVICE_PATH, "inventory");
    assert_eq!(
        <DummyInventory as InventoryService>::SERVICE_PATH,
        "inventory"
    );
    assert_eq!(DummyInventory::DEFAULT_LIMIT, 32u32);
}

/// 测试客户端 body enum 能编码、服务端 owned enum 能解码同一个方法变体。
/// Test that the borrowed client enum encodes into the owned serving enum.
///
/// - 手段：构造 `Call::find { name: "hammer" }`，编码后解码回 `Call`。
/// - 判断：解码结果的 `find` 变体携带 `"hammer"`。
///
/// - Means: encode `Call::find { name: "hammer" }` and decode it back as `Call`.
/// - Verdict: the decoded `find` variant carries `"hammer"`.
#[test]
fn borrowed_variant_roundtrips_() {
    use __rpc_service_InventoryService::Call;

    let request = Call::find {
        name: "hammer".to_string(),
    };
    let bytes = encode_to_vec_(&request);
    let decoded: Call = decode_owned_(&bytes);

    match decoded {
        Call::find { name } => assert_eq!(name, "hammer"),
        _ => panic!("应当解出 find 变体，实际不是 find"),
    }
}

/// 测试无参变体与按值变体都能跨客户端 / 服务端 enum 还原。
/// Test that unit and by-value variants survive the client/serving enum pair.
///
/// - 手段：分别编码 `Call::list` 与 `Call::create { item }` 并解码。
/// - 判断：解出对应的 `list` 与 `create` 变体，商品字段一致。
///
/// - Means: encode `Call::list` and `Call::create { item }` and decode both.
/// - Verdict: the matching variants are produced and the item fields match.
#[test]
fn unit_and_owned_variants_roundtrip_() {
    use __rpc_service_InventoryService::Call;

    let decoded_list = decode_owned_::<Call>(&encode_to_vec_(&Call::list));
    assert!(matches!(decoded_list, Call::list), "应当解出 list 变体");

    let item = Item {
        id: 7u64,
        name: "hammer".to_string(),
    };
    let created = decode_owned_::<Call>(&encode_to_vec_(&Call::create { item: item.clone() }));
    match created {
        Call::create { item: decoded } => assert_eq!(decoded, item),
        _ => panic!("应当解出 create 变体，实际不是 create"),
    }
}

/// 测试没有借用参数的 service 会生成不带生命周期的 body enum。
/// Test that a service without borrowed arguments gets a lifetime-free body enum.
///
/// - 手段：编码 `Call::add { a, b }` 并用 `Call` 解码。
/// - 判断：解码结果的字段与输入一致；这条路径本身不需要 `'f`。
///
/// - Means: encode `Call::add { a, b }` and decode it back as `Call`.
/// - Verdict: the decoded fields match, and this enum has no lifetime parameter.
#[test]
fn lifetime_free_service_roundtrips_() {
    use __rpc_service_SystemService::Call;

    let decoded = decode_owned_::<Call>(&encode_to_vec_(&Call::add { a: 2u32, b: 3u32 }));
    match decoded {
        Call::add { a, b } => {
            assert_eq!(a, 2u32);
            assert_eq!(b, 3u32);
        }
        _ => panic!("应当解出 add 变体，实际不是 add"),
    }
}

/// 编译期验证客户端代理实现了 service trait 的借用参数方法。
/// Compile-time check that the client proxy implements a borrowed-argument method.
#[allow(dead_code)]
fn client_find_<'a, 'f, C, I>(
    proxy: &'f ClientProxy<'a, C, I>,
    name: &'f str,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Item>>> + 'f
where
    'a: 'f,
    C: TrRpcClientConfig + Send + Sync + 'static,
    <C as core_rpc_mptp_smux_v1::x_deps::abs_mptp::client::config::TrConfig>::SharedConn: Sync,
    I: TrRpcInterface + Send + Sync + 'static,
{
    proxy.find(name)
}

/// 编译期验证客户端代理实现了无参方法与按值方法。
/// Compile-time check that the client proxy implements unit and by-value methods.
#[allow(dead_code)]
fn client_create_<'a, 'f, C, I>(
    proxy: &'f ClientProxy<'a, C, I>,
    item: Item,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<u64>> + 'f
where
    'a: 'f,
    C: TrRpcClientConfig + Send + Sync + 'static,
    <C as core_rpc_mptp_smux_v1::x_deps::abs_mptp::client::config::TrConfig>::SharedConn: Sync,
    I: TrRpcInterface + Send + Sync + 'static,
{
    let _ = proxy.list();
    proxy.create(item)
}

/// 编译期验证服务端 dispatch 的签名与 body 能通过类型检查。
/// Compile-time check for the generated serving dispatch signature and body.
#[allow(dead_code)]
fn dispatch_inventory_<'a, 'f, S, C>(
    service: &'f S,
    ctx: &'f mut RpcCallCtx<'a, C>,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
where
    'a: 'f,
    S: InventoryService,
    C: TrServingConfig,
{
    __rpc_service_InventoryService::dispatch_async(service, ctx)
}

/// 编译期验证无生命周期 service 的 dispatch 也能通过类型检查。
/// Compile-time check for the dispatch of a lifetime-free service.
#[allow(dead_code)]
fn dispatch_system_<'a, 'f, S, C>(
    service: &'f S,
    ctx: &'f mut RpcCallCtx<'a, C>,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<()>> + 'f
where
    'a: 'f,
    S: SystemService,
    C: TrServingConfig,
{
    __rpc_service_SystemService::dispatch_async(service, ctx)
}

/// 编译期验证 service 宏生成的服务端 worker 能构造、取 route 并产生 run future。
/// Compile-time check that the generated serving worker can be built, routed and run.
///
/// - 手段：在泛型函数里构造 `Worker`，取 `ServiceRoute`，再生成 `run_async` future。
/// - 判断：编译通过；不建立真实连接，运行期行为由 router 的单元测试覆盖。
///
/// - Means: build a `Worker`, take its `ServiceRoute` and create the `run_async` future.
/// - Verdict: it compiles; no real connection is opened.
#[allow(dead_code)]
fn worker_inventory_<C, S>(service: S)
where
    C: TrServingConfig,
    S: InventoryService,
{
    let mut worker = __rpc_service_InventoryService::Worker::<S, C>::new(service);
    let _route: ServiceRoute<C> = worker.route();
    let _service: &S = worker.service();
    let _run = worker.run_async();
}

/// 编译期验证 interface 宏生成 `TrRpcInterface` / `ServiceSelector` / worker 组。
/// Compile-time check for the interface macro's trait impls and worker group.
///
/// - 手段：构造两个 service 的 `ApiV1::workers`，收集 route 并注册到 `RpcRouter`，
///   同时生成 `run_async`。
/// - 判断：编译通过；`ApiV1::PATH` 在运行期等于 `api/v1`。
///
/// - Means: build `ApiV1::workers`, collect routes, register them on an `RpcRouter`, and
///   create the group's `run_async` future.
/// - Verdict: it compiles, and `ApiV1::PATH` is `api/v1` at runtime.
#[allow(dead_code)]
fn interface_workers_<C, S1, S2>(inventory: S1, system: S2)
where
    C: TrServingConfig,
    S1: InventoryService,
    S2: SystemService,
{
    let mut workers = ApiV1::workers::<C, _, _>(inventory, system);
    let routes = workers.routes();
    let mut router = RpcRouter::<C>::new();
    router
        .add_interface(ApiV1::PATH, routes)
        .expect("service 路径不应重复");
    let _run = workers.run_async();
}

/// 编译期验证 interface 宏生成的 `ServiceSelector` 返回统一客户端代理。
/// Compile-time check that the generated `ServiceSelector` returns the unified proxy.
#[allow(dead_code)]
fn interface_selector_<'a, C>(client: &'a MptpClient<C>) -> ClientProxy<'a, C, ApiV1>
where
    C: TrRpcClientConfig,
{
    <ApiV1 as ServiceSelector<C>>::select(client)
}

/// 测试 interface 宏注入 `PATH`，并保留原来的单元结构体。
/// Test that the interface macro injects `PATH` and keeps the unit struct.
///
/// - 手段：读取 `ApiV1::PATH` 并构造 `ApiV1`。
/// - 判断：路径为 `api/v1`，结构体可正常构造。
///
/// - Means: read `ApiV1::PATH` and instantiate `ApiV1`.
/// - Verdict: the path is `api/v1` and the marker stays a constructible unit struct.
#[test]
fn interface_injects_path_() {
    assert_eq!(ApiV1::PATH, "api/v1");
    let _marker = ApiV1;
}

/// 编译期验证 interface 能引用其它模块里的 service trait。
/// Compile-time check that an interface can list a service trait from another module.
///
/// - 手段：用 `ApiV2::workers` 构造一个 `nested::NestedService` worker 组。
/// - 判断：编译通过；跨模块的 hidden module 路径 `nested::__rpc_service_NestedService`
///   能被正确解析。
///
/// - Means: build an `ApiV2::workers` group from a `nested::NestedService`.
/// - Verdict: it compiles, so the cross-module hidden module path resolves correctly.
#[allow(dead_code)]
fn nested_interface_workers_<C, S>(service: S)
where
    C: TrServingConfig,
    S: nested::NestedService,
{
    let mut workers = ApiV2::workers::<C, _>(service);
    let _routes: Vec<ServiceRoute<C>> = workers.routes();
    let _run = workers.run_async();
}

/// 编译期验证三个 service 的 worker 组会生成嵌套 `try_join` 树。
/// Compile-time check that a three-service group generates a nested `try_join` tree.
///
/// - 手段：构造 `ApiV3::workers` 并生成 `run_async`。
/// - 判断：编译通过，证明二分递归的 `try_join` 组合对奇数个 worker 也成立。
///
/// - Means: build `ApiV3::workers` and create its `run_async` future.
/// - Verdict: it compiles, so the recursive `try_join` tree works for an odd worker count.
#[allow(dead_code)]
fn triple_interface_workers_<C, S1, S2, S3>(inventory: S1, system: S2, nested: S3)
where
    C: TrServingConfig,
    S1: InventoryService,
    S2: SystemService,
    S3: nested::NestedService,
{
    let mut workers = ApiV3::workers::<C, _, _, _>(inventory, system, nested);
    let _routes: Vec<ServiceRoute<C>> = workers.routes();
    let _run = workers.run_async();
}
