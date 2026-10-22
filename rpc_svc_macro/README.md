# rpc_svc_macro

`#[rpc]` 属性宏：从 service trait / interface marker / 服务端 impl 块生成 MPTP RPC 样板。

Use it in the protocol crate shared by the client and the server:

```rust
use abs_cancel::TrMayCancel;
use rpc_core_v1::messaging::RpcResult;
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
- trait 里注入隐藏的默认方法 `__rpc_dispatch_async`，它转到隐藏模块里的 dispatch；
- 隐藏模块 `__rpc_service_GoodsCustomerService` 里的一个 owned body enum `Call`：
  - 变体名 = 方法名，字段名 = 参数名；
  - 无参方法用 unit variant；
  - 同时派生 `TrEncodable` 与 `TrDecodable`；
- `impl GoodsCustomerService for ClientProxy<'_, C, I>`：
  - 每个方法把参数转成 owned 字段，构造 `Call` 变体；
  - 调用 `rpc_core_v1::client::call_unary_async`；
- 隐藏模块里的 `dispatch_async`：
  - 解码 `Call`，`match` 方法变体；
  - 调用用户实现，把成功值写成 `200 Ok`，业务错误按 `RpcError::status()` 写成错误回复。

**不再生成** `Worker`、队列、`route()` 或 `run_async()`。

### interface 宏

- `impl TrRpcInterface for ApiV1`，`PATH = "api/v1"`；
- `impl ServiceSelector<C> for ApiV1`，返回 `ClientProxy<'_, C, ApiV1>`；
- 不再生成 worker 组或任何服务端组织代码。

### service_of 宏

```rust
#[rpc(service_of(ApiV1))]
impl GoodsCustomerService for GoodsCustomerServiceImpl {
    // 用户实现
}
```

它保留原来的 impl，并为 `GoodsCustomerServiceImpl` 补一个
`abs_rpc_serv::service::TrRpcService<ApiV1, C>` 实现：

- `SERVICE_PATH` 取自 `<Self as GoodsCustomerService>::SERVICE_PATH`；
- `dispatch_async` 转到 trait 的隐藏方法，因此不需要知道隐藏模块路径；
- `Err` 是 `rpc_core_v1::messaging::RpcError`。

## 服务端：用户自己组织

`RpcRouter` 是 interface 级的**内联** router；用户逐个 `add_service`，再用顶层
`Router` 按 interface 前缀登记：

```rust
use mptp_smux::serving::{MptpServer, Router};
use rpc_core_v1::serving::RpcRouter;

let router = Router::<ServingCfg, _>::new()
    .call(|ctx| {
        RpcRouter::<ApiV1>::new(ctx)
            .add_service::<GoodsCustomerServiceImpl>(move |_| {
                GoodsCustomerServiceImpl::new(other_data)
            })
    })
    .call(|ctx| {
        RpcRouter::<ApiV2>::new(ctx)
            .add_service::<GoodsAdminServiceImpl>(move |_| {
                GoodsAdminServiceImpl::new(another_args)
            })
    });
let server = MptpServer::<ServingCfg, _>::new(router);
```

- `Router` 按最长路径前缀把请求交给对应的 `RpcRouter`；
- `RpcRouter` 剥掉 `ApiV1::PATH`，再按 service 的 `SERVICE_PATH` 匹配；
- 未命中写 `404`，非 `Call` 写 `405`；
- 命中后直接解码、调用 service、写回复、收尾；没有队列，也不替使用者 spawn。

需要队列 / 背压时，可以继续使用 `rpc_router_trie` 的 `RpcRouter` +
`TrRpcServiceRoute` 装配链（见 `rpc_core_v1::serving` 的旧一代契约），或在外层实现自己的
`TrReqHandler`。

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
- interface marker 必须是单元结构体；`services` 写 service trait 的路径。`services` 仍会
  被解析与校验，但不再据此生成 worker；
- `service_of(Interface)` 的 impl 块暂不支持泛型参数与 where 子句；同一个 impl 类型在
  **同一个 interface** 下只能对应一个 service trait（不同 interface 可以各有一条实现）；
- 生成代码会调用 `gen_mcf2`，协议 crate 需要打开
  `#![feature(impl_trait_in_assoc_type)]`。

## 依赖

生成代码使用 `rpc_core_v1` 的运行时与 `x_deps`（包括转出的 `abs_rpc_serv`）。协议 crate
的 denco 派生会引用 `abs_denco`、`abs_cancel` 与 `gen_mcf2`；`#[rpc]` 生成的隐藏模块已经为
body enum 补齐了这些导入，但用户自己的业务类型派生仍需要它们（或通过
`use rpc_core_v1::x_deps::...` 引入）。
