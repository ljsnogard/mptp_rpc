# rpc_svc_macro

`#[rpc]` 属性宏：从 service trait / interface marker 生成 MPTP RPC 的双端样板。

Use it in the protocol crate shared by the client and the server:

```rust
use abs_cancel::TrMayCancel;
use core_rpc_mptp_smux_v1::RpcResult;
use rpc_svc_macro::rpc;

#[derive(Clone, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct GoodsInfo {
    pub id: u64,
}

#[rpc(service = "customer/goods")]
pub trait GoodsCustomerService {
    fn list(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<GoodsInfo>>>;

    fn find<'f>(
        &'f self,
        name: &'f str,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<GoodsInfo>>>;
}

#[rpc(path = "api/v1", services = [GoodsCustomerService])]
pub struct ApiV1;
```

## 生成内容

### service 宏

- trait 里注入 `const SERVICE_PATH: &'static str = "customer/goods";`；
- 隐藏模块 `__rpc_service_GoodsCustomerService` 里的一个 owned body enum `Call`：
  - 变体名 = 方法名，字段名 = 参数名；
  - 无参方法用 unit variant；
  - 同时派生 `TrEncodable` 与 `TrDecodable`；
- `impl GoodsCustomerService for ClientProxy<'_, C, I>`：
  - 每个方法把参数转成 owned 字段，构造 `Call` 变体；
  - 调用 `core_rpc_mptp_smux_v1::client::call_unary_async`；
- 隐藏模块里的 `dispatch_async`：
  - 解码 `Call`，`match` 方法变体；
  - 调用用户实现，把成功值写成 `200 Ok`，业务错误按 `RpcError::status()` 写成错误回复；
- 隐藏模块里的 `Worker<S, C>`：
  - `new(service)` / `with_capacity(service, cap)` 建立本 service 的有界队列；
  - `route()` 返回 `(SERVICE_PATH, Sender<RpcJob<C>>)` 的 `ServiceRoute<C>`；
  - `run_async()` 循环取 job → 调用 `dispatch_async` → 收尾；返回的 future 可
    `.may_cancel_with(token)`，等待下一个 job 时也会监听取消信号；
  - worker 不 spawn，也不选择运行时。

### interface 宏

- `impl TrRpcInterface for ApiV1`，`PATH = "api/v1"`；
- `impl ServiceSelector<C> for ApiV1`，返回 `ClientProxy<'_, C, ApiV1>`；
- `ApiV1::workers(service...)`：按 `services` 顺序建立 worker，返回 worker 组；
- worker 组：
  - `routes()` 返回 `Vec<ServiceRoute<C>>`，交给 `RpcRouter::add_interface`；
  - `run_async()` 用 `try_join` 树并发驱动全部 worker，返回的 future 同样可取消。

服务端装配示意（`ServingCfg` 是使用者的 `TrServingConfig` 实现）：

```rust
let mut workers = ApiV1::workers::<ServingCfg, _>(goods_impl);
let mut router = RpcRouter::<ServingCfg>::new();
router.add_interface(ApiV1::PATH, workers.routes())?;
// 由使用者选择线程 / 运行时：
spawn(workers.run_async().may_cancel_with(server_token));
```

## 使用约束

- service trait 必须是非泛型、非 unsafe 的普通 trait，方法接收者只能是 `&self`；
- 方法不能是 `async fn`，返回类型必须是
  `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`；
- `T` 必须实现 denco 的 `TrEncodable` / `TrDecodable`（当前 `()` 不在其中）；
- 参数支持按值类型，以及这些顶层借用：
  - `&str` → `String`
  - `&[u8]` → `Vec<u8>`
  - `&String` / `&Vec<u8>` → clone
  - `&Copy` 标量（整数、浮点、`bool`、`char`）→ 解引用
  - 其它 `&T` 请改成 owned 参数；`&mut T` 不支持；
- **body enum 是 owned 的**：`denco_macro` 当前不支持带生命周期参数的派生类型，
  因此客户端与服务端共用一个 owned enum；借用参数在客户端被转成 owned 字段；
- 变体顺序由方法声明顺序决定。稳定协议上只应在末尾追加方法；插入 / 重排方法会改变
  线上的变体编号；
- interface marker 必须是单元结构体；`services` 写 service trait 的路径。trait 与 interface
  不在同一模块时请写完整路径（例如 `super::goods::GoodsCustomerService`），因为 worker
  类型名由 service 宏生成在 trait 所在模块里；
- `ApiV1::workers` 的配置类型 `C` 通常由后续 `RpcRouter` 的使用推导；推导失败时用
  `ApiV1::workers::<ServingCfg, _, _>(...)` 显式指定；
- worker 的 `run_async()` 可直接 await，也可 `.may_cancel_with(token)`；等待 job 时同样
  监听取消信号。worker 不 spawn，线程 / 运行时由使用者选择。

## 服务端接入

core 提供 `RpcJob<C>` / `ServiceRoute<C>` / `RpcRouter<C>`：路由器按 interface → service
两级运行期 Trie 匹配，命中后把 job 发进对应 service 队列；未命中写 `404`，非 `Call`
写 `405`。worker 收尾顺序是「写回复 → 半关闭 tx → 读完 rx」。

一个 server 可以注册多个 interface；每个 interface 的 worker 组独立 `routes()` /
`run_async()`。interface 与 service 路径都按完整路径段匹配，`api/v10` 不会误命中
`api/v1`。

## 依赖

生成代码使用 `core_rpc_mptp_smux_v1` 的运行时与 `x_deps`。协议 crate 的 denco 派生会引用
`abs_denco`、`abs_cancel` 与 `gen_mcf2`；`#[rpc]` 生成的隐藏模块已经为 body enum 补齐了这些
导入，但用户自己的业务类型派生仍需要它们（或通过 `use core_rpc_mptp_smux_v1::x_deps::...`
引入）。`gen_mcf2` 的展开目前要求 crate 打开
`#![feature(impl_trait_in_assoc_type)]`。
