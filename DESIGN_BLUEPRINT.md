# 关于 RPC 框架该如何设计

这个文档描述了使用者的使用场景。

## 本文中涉及的几个角色：

1. RPC 框架用户：简称用户，是 Rust 开发人员，他们只对自己的逻辑业务感兴趣；
2. RPC 框架自身：简称框架，为了便于阐述而赋予拟人的角度；
3. 框架设计者和维护者：“我们”，包括 Agent（AI 编程助手）；

## 框架要完成的部分和用户要完成的部分

一个 RPC 通信过程必须有以下三方 crate
1. 客户端 crate，决定何时以及如何发起 RPC 请求
2. 服务端 crate，决定如何响应 RPC 请求
3. 协议端 crate，决定 RPC 通信内容，必须为客户端和服务端共同引用。

# RPC API 描述

用户应该编写一个独立的 mod 甚至 crate 来描述客户端和服务端之间的语义协议。
例如，业务要求有两大类 RPC API，给顾客用的商品（Goods）服务和给管理员用的。
那么用户可以编写 **协议端 crate** 的代码，并设计如下的 trait :

```rust

#[derive(TrEncodable, TrDecodable)]
pub struct GoodsId(u64);

#[derive(TrEncodable, TrDecodable)]
pub struct GoodsInfo {
    // 省略
}

/// 顾客订单服务
#[rpc(path = "customer/goods")]
pub trait GoodsCustomerService {
    fn list(&self) -> impl TrMayCancel<'_, MayCancelOutput = Result<Vec<GoodsInfo>>>;

    fn find<'f>(&'f self, name: &'f str) -> impl TrMayCancel<'f, MayCancelOutput = Result<Vec<GoodsInfo>>>;
}

/// 管理员货架管理服务
#[rpc(path = "admin/goods")]
pub trait GoodsAdminService {
    fn register(&self, info: GoodsInfo) -> impl TrMayCancel<'_, MayCancelOutput = Result<GoodsId>>;

    fn unregister(&self, id: GoodsId) -> impl TrMayCancel<'_, MayCancelOutput = Result<GoodsInfo>>;
}

#[rpc(
    interface = "api/v1",
    services = [
        GoodsCustomerService,
        GoodsAdminService,
    ],
)]
pub struct ApiV1; // 定义 RPC Interface

```

上面的代码，经过宏的处理之后，会生成类似于如下的客户端代码

```rust

pub struct ApiV1_RpcClient<'a>(
    &'a Client,
);

/// 实现了 GoodsCustomerService
impl<'a> GoodsCustomerService for ApiV1_RpcClient<'a> {
    fn find<'f>(&'f self, name: &'f str) -> /* 省略 */ {
        // 将参数打包成 mptp 请求，并发往指定的路径 "customer/goods"
        // 并根据 <ApiV1 as RpcClientInterface>::path 的指定合成完成路径 
        // "/api/v1/customer/goods"
    }
}
/// 实现了 GoodsAdminService
impl<'a> GoodsAdminService for ApiV1_RpcClient<'a> {
    // .. 省略
}

/// RpcClientInterface 是框架提供的 trait 
impl RpcClientInterface for ApiV1 {
    type Proxy<'a> = ApiV1_RpcClient<'a>;

    fn path() -> &'str {
        "api/v1"  // 正是 rpc(path) 指定的
    }

    fn select<'a>(client: &'a MptpClient) -> ApiV1_RpcClient<'a> {
        ApiV1_RpcClient(client)
    }
}

```

于是，在 **客户端 crate** 里，用户就可以通过这样的代码发起 RPC 请求：

```rust
let client = Client; // 示意，非真实代码
let goods_list: Vec<GoodsInfo> = client
    .call::<ApiV1>() // &ApiV1_ClientProxy
    .find("some keywords") // GoodsCustomerService::find
    .may_cancel_with(cancel_tok.child_token())
    .await?;
```

在服务端一侧的 crate 里，用户则可以通过这样的代码定义 RPC 服务的实现：

```rust

pub struct GoodsCustomerServiceImpl {
    // 用户编写的任意内容，省略
}


impl GoodsCustomerService for GoodsCustomerServiceImpl {
    fn find<'f>(&'f self, name: &'f str) -> impl TrMayCancel<'_, MayCancelOutput = Result<Vec<GoodsInfo>>> {
        // GoodsCustSvcFindGoodsAsync 是由 gen_mcf2 生成的，
        // 也可以由用户手写生成。
        GoodsCustSvcFindGoodsAsync::new(self, name)
    }
}

#[gen_mcf2::gen_may_cancel_future(GoodsCustSvcFindGoods)]
async fn list_goods_<'f>(
    svc: &'f GoodsCustomerServiceImpl,
) -> Result<Vec<GoodsInfo>> {
    // 用户实现功能
}

```

并且可以通过这样的代码，将这一系列服务实现注册到服务器：

```rust
// 一次性注册这些服务路径，能被 AccessMethod::Call 访问
// `/api/v1/customer/goods`
// `/api/v1/admin/goods`
let router = Router::with_path("/").call::<ApiV1>();
```

---

## 实现现状（2026-10-20）

`rpc_svc_macro` 已经实现了蓝图中的两种宏：

- **service 宏** `#[rpc(service = "...")]`：
  - 注入 `const SERVICE_PATH`；
  - 生成隐藏模块里的 owned body enum（变体名 = 方法名，字段名 = 参数名）；
  - 为 `ClientProxy` 实现 service trait；
  - 生成服务端 dispatch（解码 body enum → `match` → 调用用户实现 → 写回复）；
  - 生成每 service 的 `Worker<S, C>`：有界队列、`route()`、可取消的 `run_async()`。
- **interface 宏** `#[rpc(path = "...", services = [...])]`：
  - 实现 `TrRpcInterface` 与 `ServiceSelector`；
  - 生成 `ApiV1::workers(...)` 与具体类型的 worker 组：`routes()` / `run_async()`。

core 侧同时补上了服务端运行时与 router 契约：

- `RpcJob<C>`：一次调用的 location、头表与收发半边；
- `ServiceRoute<C>`：service 路径 + 有界队列发送端；
- `TrRpcServiceRoute<C>`：把 job 交给 service 的抽象，允许换成自定义队列；
- `TrRpcRouter<C>`：interface / service 匹配的抽象；
- `RpcRouterHandler<R, C>`：把任意 router 适配成 `TrReqHandler`，统一处理未命中 `404`、
  非 `Call` `405`、`RpcJob` 构造与入队。

缺省的两级 `PathTrie` 实现拆到了独立 crate `rpc_router_trie`：

- `RpcRouter<C>`：interface → service 两级运行期 `PathTrie`，实现 `TrRpcRouter<C>`；
- `RpcRouterError`：注册重复；
- 不再直接实现 `TrReqHandler`，统一经 `RpcRouterHandler` / `into_handler()` 接入。

实际装配形状与本文最初的示意略有差异（以现有 API 为准）：

```rust
use rpc_router_trie::RpcRouter;

let mut workers = ApiV1::workers::<ServingCfg, _>(customer_impl);
let mut router = RpcRouter::<ServingCfg>::new();
router.add_interface(ApiV1::PATH, workers.routes())?;
let handler = router.into_handler();

// worker 组由使用者选择线程 / 运行时来驱动；core 不替使用者 spawn。
spawn(workers.run_async().may_cancel_with(server_token));
```

与本文最初设计的两点差异仍然成立：

1. `denco_macro` 当前不支持带生命周期参数的派生类型，因此客户端不再生成借用版
   `Call<'f>`；客户端与服务端共用一个 owned body enum，客户端把 `&str` / `&[u8]` /
   `&String` / `&Vec<u8>` / `&Copy` 标量转成 owned 字段。
2. 方法级分派仍由 body enum 的 `match` 完成；interface / service 两级由运行期
   `PathTrie` 完成，宏不生成路径 `match`。

细节、限制与验证结果见
[`dev-notes/rpc-framework-20261020-0332.md`](dev-notes/rpc-framework-20261020-0332.md)
第 12 节。
