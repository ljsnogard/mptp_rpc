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

// 假设这里的代码都在一个叫做 goods 的 crate

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

use goods::{ApiV1, GoodsInfo};

let client = Client; // 示意，非真实代码
let goods_list: Vec<GoodsInfo> = client
    .call::<ApiV1>() // &ApiV1_ClientProxy
    .find("some keywords") // GoodsCustomerService::find
    .may_cancel_with(cancel_tok.child_token())
    .await?;
```

在服务端一侧的 crate 里，用户则可以通过这样的代码定义 RPC 服务的实现：

```rust
use goods::{ApiV1, GoodsCustomerService, GoodsAdminService, GoodsInfo};

pub struct GoodsCustomerServiceImpl {
    // 用户编写的任意内容，省略
}

#[rpc(service_of(ApiV1))]  // 使用 rpc(service_of) 与 `ApiV1` 关联起来
impl GoodsCustomerService for GoodsCustomerServiceImpl {
    fn find<'f>(&'f self, name: &'f str) -> impl TrMayCancel<'_, MayCancelOutput = Result<Vec<GoodsInfo>>> {
        // GoodsCustSvcFindGoodsAsync 是由 gen_mcf2 生成的，
        // 也可以由用户手写生成。
        GoodsCustSvcFindGoodsAsync::new(self, name)
    }
}

#[rpc(service_of(ApiV1))]  // 使用 rpc(service_of) 与 `ApiV1` 关联起来
impl GoodsAdminService for GoodsAdminServiceImpl {
    // 省略实现代码
}

#[gen_mcf2::gen_may_cancel_future(GoodsCustSvcFindGoods)]
async fn list_goods_<'f>(
    svc: &'f GoodsCustomerServiceImpl,
) -> Result<Vec<GoodsInfo>> {
    // 用户实现功能
}

```

并且可以通过这样的代码，将这一系列服务实现注册到服务器。服务端装配既可以在启动阶段
一次性完成（最常见），也可以在运行期继续绑定（资源可以动态出现 / 消失）；router 的
具体形状见下一节「Router 设计目标」。

```rust
let other_data;
let another_args;

// 顶层 router：以自己为根，根 handler 是绝对根；规则路径相对自己。
let top = Arc::new(RadixRouter::<ServingCfg>::new(EndpointRoot));

// RPC 层辅助：interface 只是挂在 "api/v1" 下的子 router；
// add_service 再为每个 service 建一个子 router 挂在 SERVICE_PATH 下。
// 将根据协议端描述的路径，等价于挂载：
// `/api/v1/customer/goods`
// `/api/v1/admin/goods`
top.rpc_interface::<ApiV1>(|api| {
    api.add_service::<GoodsAdminServiceImpl>(move |cx| {
        GoodsAdminServiceImpl::new(cx, other_data)
    })?;
    api.add_service::<GoodsCustomerServiceImpl>(move |cx| {
        GoodsCustomerServiceImpl::new(cx, another_args)
    })?;
    Ok(())
})?;

// 普通规则：一条规则要么绑定 handler，要么绑定子 router；
// 同一 router 内两者路径不得重叠 / 互为前缀。
top.bind_handler("health", MethodSet::ALL, Arc::new(HealthHandler))?;

// 子 router：挂载在 "docs"；它内部的规则相对自己，父层不知道具体规则。
// 路径参数类型同时提供规则与 handler 参数结构。
#[derive(TrRouteParams)]
#[route("items/{id}")]
struct DocParams {
    id: DocId,          // 必须与模式里的 {id} 同名；derive 宏编译期校验
}

let docs = Arc::new(RadixRouter::<ServingCfg>::new(DocsRoot));
docs.view::<DocParams>(DocView)?;
docs.pull::<DocParams>(DocPull)?;
top.bind_router("docs", docs)?;

let server = MptpServer::<ServingCfg, _>::new(top);
```

## Router 设计目标

（详细讨论与开放问题见 [`dev-notes/router-20261106-0112.md`](dev-notes/router-20261106-0112.md)；
以下只列对框架使用者的形状约定。该设计**尚未实施**。）

1. **分层 router**：每个 router 以自己为根维护「根 handler + handler 规则 + 子 router
   规则」。父 router 只知道「在哪个前缀挂了子 router」，不知道子 router 内部规则，也
   不知道父层还有哪些兄弟路由；子 router 也不知道父层与自己被挂载的绝对前缀。
2. **添加规则只有两个入口**：
   - `bind_handler(rule, methods, handler)`：一条**精确路径规则**绑定 handler；
   - `bind_router(rule, child_router)`：一条**静态前缀规则**绑定子 router。
   七种访问方法用方法糖（`.head/.view/.post/.drop/.push/.pull/.call`）或
   `.on` + `MethodSet` 绑定；一个 handler 可以同时响应多种方法。
   **路径参数类型化**：规则字符串不直接传给绑定调用，而是由参数类型
   `P: TrRouteParams` 提供 `P::PATTERN`；`#[derive(TrRouteParams)]` 在编译期校验
   模式中的 `{id}` 与结构体字段名一致，handler 通过 `TrRouteHandler<C, P>` 拿到
   `params: P`（例如 `params.id`），因此不存在「规则里没有 id 却要求 handler 取
   id」的组合；参数值解析失败时在进入 handler 前返回 400。
3. **匹配递归向下**：本层 handler 规则 → 子 router → 本层根 handler。子 router 未命中
   由它自己的根 handler 兜底，**不会**自动上抛给父层或绝对根。
4. **每个 router 必须有根 handler**：顶层 router 的根 handler 是**绝对根**；子 router
   的根 handler 是**相对根**，负责该命名空间里未命中的请求（404 / 405 / redirect）。
5. **契约分离**：注册契约 `TrRouter`（两个添加方法 + 撤销）与匹配契约
   `TrRouteMatcher` 分离；`MptpServer` 只依赖 `TrRouteMatcher`，因此可以注入自定义
   router 实现。默认实现 `RadixRouter` 同时实现两者。

---

> **2026-11-11 重构后**：`mptp_smux` 的 `serving` 模块已移除全部 router API；
> `Router` / `RouterCtx` / `RouterError` / `TrPathRouter` / `TrPathHandler` 等迁到
> `rpc_core_v1::serving`。`MptpServer` 改名 `MptpServlet`，只服务一条 MPTP 连接，
> 持有一个单一 `TrServeHandler`；路由与 404 由应用 / `rpc_core_v1` 决定。下文关于
> `mptp_smux::serving::Router` 与 `MptpServer` 的描述是重构前的形状，保留作历史记录。

## 实现现状（2026-11-10，matchit_route 已接管路径匹配）

> 注意：`matchit_route`（matchit 0.9.2 的 no_std 移植）已经接管
> `mptp_smux::serving::Router` 与 `rpc_core_v1::serving::RpcRouter` 的路径匹配，
> `rpc_router_trie` 与队列版 router 契约已删除。分层 `RadixRouter` / 子 router / 方法糖
> 仍按 [`dev-notes/router-20261106-0112.md`](dev-notes/router-20261106-0112.md) 设计，
> **尚未实施**；下面的「实际装配形状」仍是当前形状。

服务端宏已按「宏只出样板，用户自己组织」重构完成。当前形状：

### 宏

- **service 宏** `#[rpc(service = "...")]` 保留：
  - `SERVICE_PATH`；
  - 隐藏模块里的 owned body enum（变体名 = 方法名，字段名 = 参数名）；
  - 隐藏模块里的 dispatch 与公开包装（泛型于 `TrRpcCallCtx`）；
  - 客户端 `ClientProxy` 的 service trait 实现。
- service 宏**删除**了 `Worker`、队列、`route()`、`run_async()`；
- **interface 宏**只保留 `TrRpcInterface`、`ServiceSelector` 与 `PATH`，删除
  `ApiV1::workers(...)` / worker 组 / `routes()` / `run_async()`；
- 新增 **`#[rpc(service_of(ApiV1))]`**：用于服务端 trait impl 块。它保留用户 impl，
  并为该类型补一个 `abs_rpc_serv::service::TrRpcService<ApiV1, C>` 实现：
  - `SERVICE_PATH` 取自 service trait；
  - `dispatch_async` 通过 trait 里的隐藏默认方法转到隐藏模块，不需要知道模块路径。

### 抽象契约与辅助类型

- `abs_rpc_serv::service::TrRpcCallCtx<C>`：一次调用的上下文抽象；宏生成的 dispatch 只依赖它；
- `abs_rpc_serv::service::TrRpcService<I, C>`：service 实现如何被调度；
- `rpc_core_v1::serving::RpcCallCtx` 实现 `TrRpcCallCtx`；
- `rpc_core_v1::serving::RpcRouter<I, S>`：interface 级**内联** router；
  `add_service::<Impl>(closure)` 把具体实现装进 `tlist` 类型级列表，命中后直接解码 /
  dispatch / 回复 / 收尾；
- `mptp_smux::serving::Router`：顶层 MPTP router，按最长 interface 前缀把请求交给一个
  `RpcRouter`；它自身实现 `TrReqHandler`，可直接交给 `MptpServer`。interface 前缀匹配
  由 `matchit_route` 完成；
- `rpc_core_v1::serving::RpcRouter` 的 service 路径匹配同样委托给 `matchit_route`；
- 旧的队列链 `RpcJob` / `ServiceRoute` / `TrRpcServiceRoute` / `TrRpcRouter` /
  `RpcRouterHandler` 与 `rpc_router_trie` 已删除（2026-11-10）；需要背压时在
  handler / target 内部实现队列，不再引入第二套路径匹配算法。

### 当前实际装配形状（旧）

```rust
use mptp_smux::serving::{MptpServer, Router};
use rpc_core_v1::serving::RpcRouter;

let router = Router::<ServingCfg, _>::new()
    .call(|ctx| {
        RpcRouter::<ApiV1>::new(ctx)
            .add_service::<GoodsAdminServiceImpl>(move |_| {
                GoodsAdminServiceImpl::new(other_data)
            })
            .add_service::<GoodsCustomerServiceImpl>(move |_| {
                GoodsCustomerServiceImpl::new(another_args)
            })
    })
    .call(|ctx| {
        RpcRouter::<ApiV2>::new(ctx)
            .add_service::<GoodsAdminServiceImpl>(move |_| {
                GoodsAdminServiceImpl::new(other_data)
            })
    });
let server = MptpServer::<ServingCfg, _>::new(router);
```

与本文最初示意的差异：

1. `RpcRouter::new` 接收顶层 `Router::call` 提供的 `RouterCtx`；`add_service` 的闭包接收
   一个零大小的 `RpcServiceCtx`，业务数据由闭包自己捕获；
2. 顶层 `Router` 支持多个 interface：每次 `.call` 按 `TrPathHandler::PATH` 追加一条前缀
   路由，内部用 `tlist` 静态分派，不需要 enum 或 `dyn`（service 与 interface 列表都
   没有固定数量上限）；
3. 默认是内联分派，没有队列 / 背压；需要时用旧的队列链或自己包一层 `TrReqHandler`；
4. `service_of` 暂不支持泛型 impl / where 子句；同一个 impl 类型在同一个 interface 下
   只能对应一个 service trait（不同 interface 可以各有一条实现）。

### 验证

- `rpc_svc_macro` 的集成测试真实编译了 `service_of`、`RpcRouter::add_service` 与顶层
  `Router` 的装配；
- `mptp_cs_smoke` 已迁移到新装配，compio + UDS 的 11 条 RPC（含 `ApiV1` / `ApiV2`）
  端到端通过；
- workspace 的 check / clippy / test 与 rustdoc 均通过。

> 以上验证对应的是**旧 router 形状**。分层 router 落地后，`mptp_cs_smoke`、两个 demo
> 与宏集成测试都需要按新的装配形状（`RadixRouter::new` + `rpc_interface` +
> `add_service` + `bind_router`）迁移并重新验证；具体迁移清单见
> [`dev-notes/router-20261106-0112.md`](dev-notes/router-20261106-0112.md) 第 8 节。

细节、限制与验证结果见
[`dev-notes/rpc-framework-20261020-0332.md`](dev-notes/rpc-framework-20261020-0332.md)
第 17 节。
