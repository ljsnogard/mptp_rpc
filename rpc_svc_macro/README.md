# rpc_svc_macro

`#[rpc]` 属性宏：把一段服务端业务代码展开成 MPTP RPC 的**端点处理器**、`RadixRouter`
**登记函数**与（controller 形态的）**客户端代理**。它只做「形状转换」，运行期由
[`mptp_serving`](../mptp_serving) 承担。

## 快速开始

```rust
use rpc_svc_macro::rpc;

/// 一个 controller：状态放在 struct 里，每个方法是一个端点。
pub struct Greeter {
    prefix: String,
}

#[rpc(interface = "greet")]
impl Greeter {
    /// `Call /greet/hello`
    #[rpc(call = "hello")]
    async fn hello(&self, req: HelloRequest) -> Result<HelloReply, GreetError> {
        Ok(HelloReply {
            text: format!("{}{}", self.prefix, req.name),
        })
    }
}

// 装配：controller 生成 `__rpc_controller_<Self>::install_into`。
let router = mptp_serving::RadixRouter::<ServingCfg>::new(std::sync::Arc::new(root));
let router = __rpc_controller_Greeter::install_into(
    router,
    std::sync::Arc::new(Greeter {
        prefix: "hi ".to_string(),
    }),
)?;
let servlet = mptp_smux::serving::MptpServlet::<ServingCfg, _>::new(router);
```

`root` 是 `RadixRouter` 的兜底端点（未命中时由它响应）。上面的 `HelloRequest` /
`HelloReply` 需要 denco 的 `TrEncodable` / `TrDecodable`，`GreetError` 需要实现
`rpc_core_v1::serving::TrReplyError`。

## 组织服务端代码的五种方式

### 1. controller：有状态、多端点、带客户端代理

`#[rpc(interface = "...")] impl Service { ... }`。每个带 `#[rpc(方法 = "路径")]` 的方法
是一个端点，相对路径拼在 `interface` 前缀之后；`Arc<Self>` 被所有端点共享。

```rust
#[rpc(interface = "scene")]
impl SceneService {
    #[rpc(call = "join")]
    async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }
}
```

### 2. 自由函数：无状态、单端点、无客户端代理

```rust
/// `Call /scene/ping` —— 没有 `self`，路径取完整值。
#[rpc(call = "scene/ping")]
async fn ping(req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }

// 装配：自由函数生成 `__rpc_handler_<fn>::install_into(router)`。
let router = __rpc_handler_ping::install_into(router)?;
```

### 3. 尾参取消令牌：两种写法都能叠加

unary 端点可以把请求级取消令牌作为**最后一个参数**显式接住：

```rust
#[rpc(call = "join")]
async fn join<K>(&self, req: JoinRequest, cancel: K) -> Result<PlanetDescriptor, SceneError>
where
    K: abs_cancel::TrCancellationToken,
{
    let _ = cancel.child_token(); // 也可以交给 may_cancel_with
    ...
}
```

`K` 必须是唯一的泛型类型参数、`cancel` 必须是最后一个参数；不写时宏自动注入。

### 4. Pull 流端点：后缀流

`Pull` 是普通 `fn`，返回 `impl TrAsyncIterator<Item = T, Err = E>`；路径参数由宏解码后，
逐项把 item 写进回复后缀流。

```rust
#[rpc(pull = "items/{count}")]
fn items(&self, count: u64) -> impl TrAsyncIterator<Item = u64> {
    0..count
}
```

### 5. 手写 `TrEndpointHandler`：完全控制（逃生口）

需要读请求后缀、手改头表、`take_parts` 自己收尾等能力时，直接在 `mptp_serving` 层
`impl TrEndpointHandler`，用 `router.bind_endpoint(...)` 登记；可与宏生成的端点混挂。

```rust
struct Raw;
impl<C: TrConfig, S, R> TrEndpointHandler<C, S, R> for Raw {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S, R>) -> HandlerFuture<'a> {
        let arena = ctx.arena();
        box_future_(arena, async move { ... })
    }
}
```

## 如何选择

| 需求 | 选哪种 |
| --- | --- |
| 有状态、多端点、想要类型化客户端代理 | controller（方式 1） |
| 无状态、单端点、不关心客户端代理 | 自由函数（方式 2） |
| 需要请求级取消 | 在方式 1 / 2 上加尾参令牌（方式 3） |
| 回复是持续流（订阅、推送） | Pull 端点（方式 4） |
| 读请求后缀、手改头表、手控收尾 | 手写 `TrEndpointHandler`（方式 5） |

**闭包不做**：`MptpContext` 是借用的，而 handler future 会借用它；`TrEndpointHandler` 的
`handle<'a, 'f>` 又要求两个独立生命周期，`Fn` 闭包无法表达「返回借用参数的 future」
（高阶 return type 不可写）。请用自由函数覆盖「无捕获」、controller 覆盖「有状态」。

## 生成内容

对 controller（隐藏模块 `__rpc_controller_<Self>`）与自由函数（隐藏模块
`__rpc_handler_<fn>`）分别生成：

- 每个 unary 端点：
  - `gen_may_cancel_future` step 函数（`{Self}_{Method}Async` / `{Method}Async`）；
  - 一个 `TrEndpointHandler`（controller 持有 `Arc<Self>`，自由函数是无状态单元结构体）；
- 每个 `Pull` 端点：一个流处理器（解码路径参数 → 调方法取迭代器 → 逐项写后缀流）；
- `install_into`：把处理器登记进 `RadixRouter`；
- controller 另生成客户端代理 `{Self}Rpc<'a, C>` 与构造器 `rpc(&client)`。

客户端代理只覆盖 **1 个 body 参数的 unary 端点**：

```rust
let proxy = __rpc_controller_Greeter::rpc(&client);
let reply: HelloReply = proxy.hello(req).may_cancel_with(token).await?;
```

## 使用约束

- controller 必须是**固有 impl 块**，暂不支持泛型 impl / impl 级 where 子句；
- 端点方法用 `#[rpc(方法 = "路径")]` 声明访问方法与路径，支持
  call / view / head / post / drop / push / pull；
- unary 端点：`async fn`，返回 `Result<T, E>`（`E: TrReplyError`），**0 或 1 个 body 参数**
  （路径参数只对 `Pull` 生效）；
- `Pull` 端点：普通 `fn`，返回 `impl TrAsyncIterator<Item = T, Err = E>`，路径参数按参数名
  从路由取；
- 尾参令牌：唯一泛型类型参数 + 最后一个参数 + `TrCancellationToken` 约束；
- 自由函数：路径取完整值（无 `interface` 前缀）、不生成客户端代理、端点声明写在
  `#[rpc(方法 = "完整路径")]` 上；
- 业务类型需要 denco 的 `TrEncodable` / `TrDecodable`；
- 生成代码会调用 `gen_mcf2`，协议 crate 需要
  `#![feature(impl_trait_in_assoc_type)]`。

## 依赖

生成的隐藏模块会引用 `mptp_serving`、`rpc_core_v1`（含 `x_deps`）、`gen_mcf2` 与
`abs_cancel`；业务类型的 denco 派生还需要 `abs_denco`。协议 crate 的 `Cargo.toml` 里
把这些依赖列全即可（`rpc_core_v1::x_deps` 也转出了其中大部分）。
