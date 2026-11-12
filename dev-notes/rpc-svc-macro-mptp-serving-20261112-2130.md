# RPC 宏面向 mptp_serving 的改造方向（设计稿）

- 日期：2026-11-12 21:30
- 状态：**设计稿**；本文只固定「rpc_svc_macro 应该往哪里改」，不实施代码，不改公开 API
- 范围：`rpc_svc_macro` 的新宏形状（方法 + 路径 + 元数据 + 参数来源）、它与 `mptp_serving`
  的映射、客户端代理与服务端装配的收敛方向、边界取舍，以及 `mptp_smux_v1_demo` 场景的
  改造前后对照与分阶段建议。
- 相关文档：
  - [`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md)
  - [`mptp-api-plan-20261112-1850.md`](mptp-api-plan-20261112-1850.md)
  - [`integration-decisions-20261112-2045.md`](integration-decisions-20261112-2045.md)
  - [`../DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)

> 按 `AGENTS.md` 第 1 条，本文所有涉及公开 API 的变更均**只提出、待确认**；本文不修改任何
> 代码。文中「现状」均以 `feat/serv-framework` 分支上的代码为准（2026-11-12）。

## 0. 一句话结论

**应当改，且方向是把 `rpc_svc_macro` 从「service trait + interface marker + owned body
enum 分派」改造成「每个方法 = 一个 `Endpoint`」的 C# Attribute 形状**：

- 每个方法声明自己的 `AccessMethod` + 路径 + 元数据（`[Timeout]` / `[Authorize]` /
  `[AcceptType]` …，交给中间件链）+ 参数来源（`[FromBody]` / 路径参数 / 头表）；
- 宏直接落在 `mptp_serving` 的 `RadixRouter` / `Endpoint` / `EndpointMetadata` /
  `TrEndpointHandler` 上，不再生成 body enum、不再需要 `RpcRouter` / `TrRpcService` /
  `TrRpcCallCtx` 这套「service 级单端点 + body enum 分派」的中介。

一句话的因果链：旧模型的「一个 service 共享一条 `SERVICE_PATH`、方法名塞进 body enum」
无法按方法挂元数据、无法表达 `View` / `Head` / `Pull` 这类非 `Call` 的流语义；而
`mptp_serving` 已经把「路径 → 端点 → 元数据 → 中间件 → 处理器」这条链搭好，RPC 宏只需要
把每个方法编译成这条链上的一个端点即可。

## 1. 为什么旧的 body enum 模型在新框架下站不住

旧宏的关键假设是「**一个 service 一个路径，方法名走 body**」：

```rust
#[rpc(service = "customer/goods")]
pub trait GoodsCustomerService {
    fn list(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<GoodsInfo>>>;
    fn find<'f>(&'f self, name: &'f str) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<GoodsInfo>>>;
}
```

展开后：

1. trait 里注入 `SERVICE_PATH = "customer/goods"`；
2. 生成一个 `Call` body enum（变体名 = 方法名，字段名 = 参数名）；
3. 客户端把参数转成 owned、构 `Call::Find { name }` 变体，发到唯一路径；
4. 服务端 `match Call { ... }` 解码 body enum、调用实现、写回复。

这套模型的问题在新框架下很直接：

| 关注点 | body enum 模型 | mptp_serving 需要 |
| --- | --- | --- |
| 路由粒度 | 到 service 级（`customer/goods`） | 到方法级（`/scene/join`、`/scene/planets`） |
| 方法维度 | 只有 `Call` | `Call` / `View` / `Head` / `Pull` / `Push` … 七种 |
| 元数据 | 无处安放（挂在哪个「方法」上？） | 每端点一份 `EndpointMetadata` |
| 参数来源 | 全部进 body（owned enum） | body / 路径 / 头表，显式区分 |
| 流语义 | 无法表达（只有 unary） | `Pull` 回复后缀流、`Push` 请求后缀流 |

`mptp_serving` 的设计（[`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md) §5）
已经明确「路由只匹配、端点带元数据、中间件按元数据执行策略、处理器只写业务」。RPC 宏只要
把「方法 → 端点」这一步编译出来，body enum、`TrRpcService`、`RpcRouter` 就都成了多余的中介。

## 2. 前置：框架侧必须先补齐的三个缺口（先于宏）

宏改造依赖 `mptp_serving` 三个目前**还不满足**的条件。它们都属于框架公开 API 变更，
实施前须按 `AGENTS.md` 第 1 条确认；本文把它们列出来作为 P0，而不是混进宏的改动里。

### 2.1 handler / 中间件 future 去掉 `+ Send`

[`integration-decisions-20261112-2045.md`](integration-decisions-20261112-2045.md) §2.1
已给出结论：`gen_mcf2` 生成的带高阶类型的 future 无法被编译器穿透，future 链天然
`!Send`，因此 handler / 中间件的 future **不要加 `Send`**，`Send` 边界放到分派层。

但当前代码还没改：

- `mptp_serving/src/handler_.rs`：`handle` 返回 `Pin<Box<dyn Future + Send>>`；
- `mptp_serving/src/middleware_.rs`：`MiddlewareFuture` 仍是 `+ Send`；
- `mptp_serving/src/req_handler_target_.rs` 同样要求 `Send`。

宏生成的处理器内部必然要 `await` 一个 `gen_mcf2` future（或 `abs_buff` / `abs_denco` 链），
这个 future 无法装箱成 `Box<dyn Future + Send>`。**不先去 `Send`，新宏无法落地。**

### 2.2 补一个「带元数据绑定端点」的入口

`Endpoint::with_metadata` 已存在（`mptp_serving/src/endpoint_.rs`），但
`RadixRouter::bind` 系列（`mptp_serving/src/router_.rs`）只接受
`(pattern, methods, handler)`，**内部直接 `Endpoint::new(...)` 构造端点，没有把元数据
接进来的通道**：

```rust
// 现状：bind 里 endpoint 是现造的，外部无法再 with_metadata
pub fn bind(mut self, pattern, methods, handler) -> Result<Self, InsertError> {
    let endpoint = Endpoint::new(pattern, methods, Arc::new(handler));
    self.trie_.insert(normalize_(endpoint.pattern()), endpoint)?;
    Ok(self)
}
```

框架设计 §9 的示意 `top.view::<FeedParams>(...).with_metadata(Timeout(..))` 目前并不成立。
宏要挂 `#[metadata(...)]`，需要框架补二选一：

1. `RadixRouter::bind_endpoint(self, endpoint: Endpoint<C, S>) -> Result<Self, InsertError>`
   （最小、最直接），或
2. 让 `bind` 系列返回一个 `EndpointBuilder`，提供 `.with_metadata(...)` 后再 `.finish()`。

倾向 1：它不动现有 `bind` 签名，`Endpoint` 已是公开类型。

### 2.3 `MptpContext` 补 `cancel` 与 `headers()` 便捷访问

- **取消令牌**：[`integration-decisions-20261112-2045.md`](integration-decisions-20261112-2045.md)
  决策 A 把 `RequestCancel` 进 `MptpContext` 标记为「暂缓待确认」。宏生成的处理器要用
  取消令牌驱动 `gen_mcf2` future（`AGENTS.md` 第 4 条：不能假定 `NonCancellableToken`）。
  没有它，处理器只能退化成 `NonCancellableToken::new()`，违背纪律。
- **头表**：`MptpContext` 目前**没有** `headers()` 方法；头表经
  `ctx.session().headers()`（`ServingSession::headers()`）间接访问。头参数来源需要一个
  稳定的 `ctx.headers()`（只读）便捷入口，或宏直接生成 `ctx.session().headers()` 的调用
  （后者的耦合更紧，但可先这样）。

## 3. 理想语法（宏形状）

下面以 `mptp_smux_v1_demo` 的恒星系场景为范例给出**理想写法**（示意，不保证直接编译）。
协议端只写类型（`JoinRequest` / `PlanetDescriptor` / `PlanetDirectory` / `PlanetEvent`
等，已存在，见 `mptp_smux_v1_demo/src/scene/protocol_.rs`），服务端一个 service 按方法挂
Attribute。

### 3.1 服务端：一个 controller，每个方法一个端点

```rust
/// 场景服务：每条连接一个实例（对应今天的 `SceneHandler`）。
#[rpc_interface("scene")]
pub struct SceneService {
    ctx_: Rc<ConnectionCtx>,   // 连接身份 + 共享目录/订阅中心（沿用现有 ConnectionCtx）
}

impl SceneService {
    #[call("join")]
    #[metadata(Timeout(Duration::from_secs(5)))]
    async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> {
        // 只写业务：重复 join 拒绝、分配行星、claim。解码/编码/写回复由宏生成。
    }

    #[call("update")]
    async fn update(&self, req: UpdateRequest) -> Result<UpdateAck, SceneError> {
        // 只操作自己的行星，广播 Updated，回 ack。
    }

    #[call("unsubscribe")]
    async fn unsubscribe(&self, req: UnsubscribeRequest) -> Result<UnsubscribeAck, SceneError> {
        // 显式退订提示。
    }

    #[view("planets")]
    async fn view_planets(&self) -> Result<PlanetDirectory, SceneError> {
        // 目录，不订阅。
    }

    #[head("planets")]
    async fn head_planets(&self) -> Result<SceneHeadMeta, SceneError> {
        // 无体，只给数量 + 目录版本两个头。SceneHeadMeta 由宏映射成 ResponseBuilder 头。
    }

    #[pull("planets/{id}")]
    fn pull_planet(&self, id: PlanetId)
        -> impl abs_async_iter::TrAsyncIterator<Item = PlanetEvent, Err = SceneError> {
        // 返回一条后缀流：先 Snapshot，再 Updated/Removed/Heartbeat。
        // 订阅、SubGuard、心跳、退订清理都在这个返回的流内部封装。
        // 流结束 / 出错用 `Err` 表达（abs_async_iter 的 `IterEndedError` 约定）。
    }
}
```

要点：

- `#[rpc_interface("scene")]` 提供接口前缀，方法路径既可写相对路径（`"join"` →
  `scene/join`），也可写带前缀的绝对路径（`"/scene/join"`）；宏统一 `trim_matches('/')`
  后拼接，与 `RadixRouter` 的 `normalize_` 一致。
- `&self` 是 controller（每连接一份，等价今天的 `SceneHandler`）；连接身份 / 共享目录都
  在 `self.ctx_` 里，因此方法签名保持「只写业务」，不需要框架类型。
- `#[metadata(...)]` 是**可重复**的，编译成 `Endpoint::with_metadata(...)`。
- `async fn` 是**表面糖**：宏内部仍把它重写成一个 `gen_may_cancel_future` future 并注入
  取消令牌，业务开发者不需要手写 cancel token。取消令牌的注入与传播细节见 §3.5、§3.6。

### 3.2 方法 attribute（AccessMethod + 路径）

七种访问方法对应七个小写 attribute；`#[on(...)]` 兜底表达多方法端点。

| attribute | `MethodSet` | 语义 |
| --- | --- | --- |
| `#[call("p")]` | `CALL` | 功能调用：请求/回复通常有体，后缀流可选 |
| `#[view("p")]` | `VIEW` | 读资源：请求无体，回复有体 |
| `#[head("p")]` | `HEAD` | 读元信息：无体，只回状态 + 头 |
| `#[post("p")]` | `POST` | 上传资源：请求有体 |
| `#[drop("p")]` | `DROP` | 删除资源：请求/回复无体 |
| `#[push("p")]` | `PUSH` | 上传流：**请求**后缀流必需 |
| `#[pull("p")]` | `PULL` | 拉取流：**回复**后缀流由服务端写 |
| `#[on(CALL | VIEW, "p")]` | 自定义 | 一个 handler 响应多种方法 |

### 3.3 参数来源 attribute（[FromBody] / 路径 / 头表 / 状态 / 上下文）

| attribute | 来源 | 类型约束 | 生成代码 |
| --- | --- | --- | --- |
| `#[from_body]`（缺省，当只有一个 body 参数时） | 请求体 | `TrDecodableOwned` | `ctx.session_mut().req_body_async()` + denco 解码 |
| `#[from_path]` / `#[from_path("id")]` | 路径参数 | `TrFromRouteParam` | `ctx.route().get_typed::<T>("id")`，缺失/解析失败 → 400 |
| `#[from_header("x-key")]` | 头表 | 一个轻量 `TrFromHeader`（或直接 `&str` / `u64`） | `ctx.headers().try_get(HeaderKey)` |
| `#[from_state]` | 连接作用域状态 `S` | `T = S` | `ctx.state()` |
| `#[from_ctx]` | 逃逸舱口 | `&mut MptpContext<..>` | 直接把 `ctx` 交给方法（罕见） |

编译期校验：路径模板里的每个 `{name}` 必须有且只有一个 `#[from_path]`（或同名）参数，且该
参数类型实现 `TrFromRouteParam`；反之 `#[from_path("name")]` 的名字必须出现在模板里。这
与 [`router-20261106-0112.md`](router-20261106-0112.md) 「derive 编译期校验模式与字段一致」
的既有约定一脉相承。

### 3.4 返回类型与流语义

| 方法 | 返回类型 | 宏生成的处理 |
| --- | --- | --- |
| `call` / `view` / `post` | `Result<T, E>` | `Ok(v)` → 编码 body + `200`；`Err(e)` → 状态回复 |
| `drop` | `Result<(), E>` | 无体成功 / 状态回复 |
| `head` | `Result<HeadMeta, E>`（或经 `#[from_ctx]` 直接写头） | 只写头 + 无体 |
| `pull` | `impl TrAsyncIterator<Item = T, Err = E>` | 处理器逐项 `iter.next_async().may_cancel_with(token)` 后 `SuffixStream::write_chunk_async` |
| `push` | 参数带 `impl TrAsyncIterator<Item = T, Err = E>`（请求后缀源） | 从请求后缀逐项读，喂给方法 |

> **关于 `TrAsyncIterator`（澄清）**：这里指 `abs_async_iter::TrAsyncIterator`（no_std，
> 已是工作区依赖，`Cargo.lock` 里 `abs_async_iter 0.1.0`），**不是** std 尚未稳定的
> `AsyncIterator`。它的 `next_async(&mut self)` 返回
> `impl TrMayCancel<'f, MayCancelOutput = Result<Item, Err>>`——**每个 item 的推进天然可取消**，
> 因此处理器用 `next_async().may_cancel_with(token.child_token())` 就能把取消逐项送进流里；
> 流结束 / 出错统一用 `Err` 表达（sync `Iterator` 的 blanket impl 用 `IterEndedError` 表示
> 到末尾）。

`E` 需要可映射成 `Status + message`：建议定义一个小契约（如 `TrReplyError`），`SceneError`
实现它；框架默认把 `Err(e)` 写成一个约定的错误状态（`500` 或 `e.status()`），不把错误文本
泄漏到线上（对齐现有 `RpcError` 的 `into_reply_parts` 精神）。

### 3.5 `async fn` 是表面糖：宏内部仍是 `gen_mcf2`，业务开发者不用手写 cancel

§3.1 里的 `async fn` 是给**业务开发者**看的表面糖，不是让宏直接处理一个裸 `async fn`
future。真正落地的 future 仍是 `gen_mcf2` 的 `gen_may_cancel_future`。展开后的形状（以
`join` 为例，示意）：

```rust
// 用户写的表面糖：
async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }

// 宏展开（示意）：把方法体重写成一个可取消 future 的 step 函数。
#[gen_may_cancel_future(SceneJoin, pub(crate), new(pub(crate)))]
async fn scene_join_async_<'f, C>(
    svc: &'f SceneService,
    req: JoinRequest,
    cancel: C,
) -> Result<PlanetDescriptor, SceneError>
where
    C: TrCancellationToken,
{
    // 原方法体，self → svc；这里用户代码不必出现 cancel.child_token()。
    ...
}

// 生成的处理器里（示意）：
let fut = SceneJoinAsync::new(self.svc_.as_ref(), req);
match fut.may_cancel_with(ctx.cancel().child_token()).await {
    Ok(v)  => write_ok(ctx, &v).await,
    Err(e) => write_err(ctx, e).await,
}
```

这样**业务开发者不需要显式考虑 cancel token**：他写 `async fn + Result<T, E>`，宏负责注入
`cancel: C`、生成 `gen_may_cancel_future`、在处理器里用 `ctx.cancel().child_token()` 驱动。
这与客户端「`gen_mcf2` future 由**调用处**用 `.may_cancel_with(token)` 决定是否真传令牌」
的语义一致——服务端只是把「调用处」固定为「宏生成的处理器收尾路径」，令牌来源是 `ctx`
（连接 / 会话作用域），而不是让每个业务方法自己拿令牌。

代价与边界，如实说明：

- **gen_mcf2 的取消是「协作式」的**：令牌作为最后一个参数传入函数体，函数体内的每个 await
  点需要自己 `.may_cancel_with(cancel.child_token())` 或检查 `cancel.is_cancelled()` 才会
  真正中断。宏**不能自动**把用户方法体内的任意 `.await` 改写成可取消版本。
- 因此宏采用**两层分工**：
  1. **处理器负责 I/O 的取消**：decode body、encode+write reply、写 suffix 这些**宏生成的**
     await 点，宏都带上 `cancel.child_token()`，这是取消能生效的主路径（令牌在这些点被
     `.may_cancel_with` 真正检查）。
  2. **业务方法通常不需要取消粒度**：`join/update/view/head` 这类 unary 方法体基本是同步
     业务（`borrow_mut()`、`update_planet_()`）+ 极少量短 await；真正长命的只有 `pull`。
     业务方法内部的短 await 若不做 `.may_cancel_with`，最坏是「多算完一步」，处理器会在
     **下一个它自己生成的 I/O await 点**（encode+write reply）观察到令牌并中止；方法
     future 被 drop 时它的 `Drop` 清理照常跑，不泄漏。
- **长命流（`pull`/`push`）是例外**：它的取消必须落到每个 `next_async` 上。好在
  `abs_async_iter::TrAsyncIterator::next_async` 返回的**已经是 `TrMayCancel`**（见 §3.4），
  宏在驱动循环里写 `iter.next_async().may_cancel_with(cancel.child_token())` 即可把取消
  逐项送进去；需要「边跑边查 `is_cancelled()`」的极少数场景，才把令牌显式传给业务方法
  （逃逸能力，不作为默认要求）。

一句话：**unary 方法零负担；流方法由宏在 `TrAsyncIterator` 驱动层逐项送令牌；只有需要
主动协作取消的长循环才让开发者碰令牌。**

### 3.6 取消信号如何从客户端到达服务端（与客户端 gen_mcf2 的语义一致性）

先澄清一个关键点：**MPTP 没有「取消」这条线上的控制消息**。`abs_cancel::TrCancellationToken`
是**本地**信号（`is_cancelled()` / `cancellation()` 就绪），它不会自己跨过网络。客户端
取消到达服务端靠的是**两条正交的机制**，宏要做的只是「不破坏它们」并把服务端一侧接上：

1. **传输层可观测**：客户端 future 被令牌中断后，它会 drop 掉自己持有的 `MptpSession` /
   子流（半关闭或 reset）。服务端正 `await` 在那个子流上（读 body / 写 reply / 写 suffix），
   下一次读写就会得到传输错误，从而退出。这正是 demo 里 `Pull` 的「心跳 + 写失败即退订」
   机制（`handler_.rs` 的 `pull_async_`：`write_chunk_async` 失败即 `break`）。

2. **服务端本地令牌**：服务端对每一条 session / 订阅持有自己的取消令牌——
   - 连接令牌：`ConnectionCtx::token_()`（连接断开时 `cancel_()` 触发，`state_.rs`）；
   - 订阅令牌：慢订阅者被移除、`unsubscribe` 显式退订时触发（`SceneHub::broadcast_` /
     `unsubscribe_connection_`）。
   这些令牌在服务端本地把「连接没了 / 订阅没了」翻译成「在途 future 该停了」。

因此「与客户端 gen_mcf2 语义一致」的准确含义是：

- 客户端：`gen_mcf2` future 由调用处 `.may_cancel_with(token)` 决定是否可被本地取消；
- 服务端：宏生成的处理器同样是一个 `gen_mcf2` future，由**处理器**用
  `ctx.cancel().child_token()`（P0.3 把 `RequestCancel` 进 `MptpContext` 之后）驱动；
- 跨端：客户端本地取消 → 子流 teardown → 服务端传输错误 + 服务端连接/订阅令牌，二者共同
  保证服务端在途工作会退出，**不需要**发明一条线级 cancel 消息。

结论：宏不需要新造取消协议；它只需（a）把 `ctx.cancel()` 接进处理器（依赖 P0.3），
（b）保证 `pull/push` 的 `TrAsyncIterator` 驱动循环里逐项 `.may_cancel_with(token)`，即可
与客户端 `gen_mcf2` 语义保持一致。

## 4. 映射到 mptp_serving（问题 2）

每个方法编译成「一个 `Endpoint` + 一个终端处理器」，注册进 `RadixRouter`。以
`join` 为例的生成形状（示意）：

```rust
// ① 每方法一个处理器：实现 TrEndpointHandler<C, S>，只做「取参数 → 调业务 → 写回复」。
struct SceneService_Join { svc_: Arc<SceneService> }

impl<C, S> TrEndpointHandler<C, S> for SceneService_Join {
    fn handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, C, S>)
        -> Pin<Box<dyn Future<Output = Result<(), ServeError>> + 'a>>   // 注：已去掉 + Send（P0）
    {
        Box::pin(async move {
            let req: JoinRequest = decode_body(ctx.session_mut()).await?;      // from_body
            let cancel = ctx.cancel();                                        // P0.3 之后
            match self.svc_.join(req).may_cancel_with(cancel.child_token()).await {
                Ok(v) => write_ok(ctx, &v).await,                              // 编码 body + 200
                Err(e) => write_err(ctx, e).await,                            // 状态回复
            }
        })
    }
}

// ② 注册：把方法路径 + 方法集合 + 元数据挂到端点。
fn install_into(router: &mut RadixRouter<C, Global, S>, svc: Arc<SceneService>) {
    let ep = Endpoint::new(
        "scene/join",
        MethodSet::CALL,
        Arc::new(SceneService_Join { svc_: svc }),
    )
    .with_metadata(Timeout(Duration::from_secs(5)));   // 依赖 P0.2 的 bind_endpoint
    router.bind_endpoint(ep)?;
    // ... 其余方法同理
}
```

逐条对应问题 2 的要求：

| 要求 | 落点 |
| --- | --- |
| 每方法生成 Endpoint | `Endpoint::new(pattern, methods, handler)`，`pattern` = 接口前缀 + 方法路径 |
| 元数据交中间件链 | `Endpoint::with_metadata(...)`，中间件 `TrMiddleware::before` 读 `endpoint.metadata()` |
| 注册进 RadixRouter | `bind_endpoint`（P0.2 补）或现有 `bind` + 逐方法调用 |
| 参数来源 = body | `from_body` → `ctx.session_mut().req_body_async()` + denco 解码 |
| 参数来源 = 路径 | `from_path` → `ctx.route().get_typed::<T>(name)`，失败写 400 |
| 参数来源 = 头表 | `from_header` → `ctx.headers()`（P0.3 补便捷访问） |
| 非 `Call` 流语义 | `pull` → 处理器把 `TrAsyncIterator` 逐项写入 `SuffixStream`；`push` 反向 |

一个关键收益：**body enum 分派消失后，`AcceptType` 元数据第一次变得有意义**。旧模型下
`/customer/goods` 一个路径收多个方法，`Data_Type_Id` 校验只能「宽匹配」；新模型下
`/scene/join` 就只收 `JoinRequest`，端点可以挂 `AcceptType(JoinRequest::TYPE_ID)`，由
`TypeCheck` 中间件在进处理器前拒绝类型不符的请求（写 406）。这正是
[`mptp-api-plan-20261112-1850.md`](mptp-api-plan-20261112-1850.md) P2 「带 `AcceptType`
元数据」的原意，只是把「一个 service = 一个端点」修正为「一个方法 = 一个端点」。

## 5. 客户端代理形态（问题 3）

**结论：保留 `TrRpcClientExt::call::<I>()` 作为「进入 interface 视图」的入口，但把每个
方法展开成「方法糖 + 类型状态 builder」，与 `MptpClient` 的 `head/view/post/pull` 对齐。
代理是 `MptpClient` 方法糖之上的**薄类型门面**，不重新实现传输。**

```rust
// 由 #[rpc_interface("scene")] 生成一个类型化代理（名字可派生为 SceneServiceRpc / SceneRpc）。
let proxy: SceneRpc<'_, ClientCfg> = client.call::<SceneInterface>();  // 或 client.scene()
let desc = proxy.join(JoinRequest { .. }).may_cancel_with(tok).await?;
let dir  = proxy.view_planets().await?;
let head = proxy.head_planets().await?;
let mut stream = proxy.pull_planet(planet_id).await?;    // 客户端后缀流读视图
while let Some(ev) = stream.next_event().await? { .. }
```

分方法映射：

| 方法 | 代理返回 | 底层委托 |
| --- | --- | --- |
| `call` / `view` / `post` | `impl TrMayCancel<'f, MayCancelOutput = Result<T, RpcError>>` | `MptpClient::invoke/view/post(path)` + `.body_bytes(encode(req))` + `.send_async()` + 解码 |
| `head` | `impl TrMayCancel<..., Result<HeadMeta, RpcError>>` | `MptpClient::head(path)` |
| `drop` | `impl TrMayCancel<..., Result<(), RpcError>>` | `MptpClient::drop(path)` |
| `pull` | 客户端后缀流读视图（`SuffixRead` 的逐 chunk 读，`mptp-api-plan` P3 拟补） | `MptpClient::pull(path).send_stream_async()` |
| `push` | `impl TrMayCancel<..., Result<T, RpcError>>`（方法吃一个请求后缀源） | `MptpClient::push(path)` + `suffix(...)` |

取舍说明：

- **保留 `call::<I>()`**：它已经是「零大小 interface marker → 客户端视图」的薄入口，且
  `mptp_smux::MptpClient` 已把「Call 方法糖」命名为 `invoke` 以避免与方法名冲突，所以
  `call::<I>()` 的语义空间仍然干净。要改的只是**代理内部**从「构造 body enum → 发到
  service 路径」变成「构造 typed 请求 → 发到方法路径」。
- **不把代理做成独立传输**：`MptpClient` 的 `RequestBuilder`（类型状态）已经是正确的
  「体 / 后缀 / 半关闭 / 超时 / 取消」载体；代理只负责把「方法签名」翻译成对它的调用，
  超时 / 取消 / 半关闭仍由 `RequestBuilder` 一条链走完。

## 6. 服务端装配（问题 4）

**结论：`#[rpc_interface]` 收敛替代 `RpcRouter::add_service`；`TrRpcService` /
`TrRpcCallCtx` 退役，分派直接落在 `TrEndpointHandler` 上。**

### 6.1 装配形状

```rust
let top = RadixRouter::<ServingCfg, Global, SceneConnState>::new(AppRoot);

// 旧：RpcRouter::<ApiV1>::new(ctx).add_service::<Impl>(|_| Impl::new(...))
// 新：interface 直接往顶层 router 装端点。
SceneService::install_into(&top, Arc::new(scene_service))?;

let servlet = MptpServlet::new(top);   // RadixRouter 已实现 TrServeHandler
```

`RadixRouter` 已经实现 `TrServeHandler`（`mptp_serving/src/serve_handler_.rs`），所以装配
边界不再需要 `RpcRouter` / 顶层 `Router` / `LegacyHandler` 那一层适配。

### 6.2 `TrRpcService` / `TrRpcCallCtx` 去留

| 类型 | 现状职责 | 去留 |
| --- | --- | --- |
| `abs_rpc_serv::service::TrRpcCallCtx<C>` | 解码 body / 写 ok / 写 status / 写 error 的最小契约 | **退役**：职责由 `MptpContext` + `ServingSession`（`req_body_async` / `response`）承担 |
| `rpc_core_v1::serving::RpcCallCtx` | 具体实现（持有两半 channel + 收尾） | **退役**：处理器经 `MptpContext` 读/写，收尾由 servlet 兜底 |
| `abs_rpc_serv::service::TrRpcService<I, C>` | `SERVICE_PATH` + `dispatch_async` | **退役**：被「每方法一个 `TrEndpointHandler`」取代，不再有「service 级单 dispatch」 |
| `rpc_core_v1::serving::RpcRouter` / tlist / `TrRpcServiceList` | interface 级内联分派（已 deprecated） | **删除**（P3 清理） |

保留的只有「接口 = 命名空间」这一层语义，由 `TrRpcInterface`（或新的接口 marker）继续提供
`PATH`；它同时是客户端 `call::<I>()` 与服务端 `install_into` 共用的锚点。

一句话：**旧的「service 契约 + 上下文契约」被 `mptp_serving` 的「端点 + 上下文」吸收；
`TrEndpointHandler` 成为服务端唯一分派目标。**

## 7. 边界取舍（问题 5）

沿用并强化既有三条约法，明确「学什么、不学什么」：

1. **不学 Tower 的类型体操**：不引入 `Service<Request>` / `Layer` / `ServiceBuilder` /
   `poll_ready`。中间件仍是对象安全的 `TrMiddleware`（线性前置，`next` 是普通循环），
   元数据决定行为，而不是层层泛型包装。宏生成的处理器只是普通 `TrEndpointHandler`
   实现，不产生嵌套服务栈类型。
2. **不学 Salvo 的 Depot**：不引入 `HashMap<TypeId, Box<dyn Any>>` 状态袋。跨中间件的
   运行期状态（鉴权主体、配额）放 `MptpContext` / 连接状态 `S`，宏生成的参数注入
   `#[from_state]` 就是显式类型化的 `&S`，不擦除类型。
3. **`!Send` future 不带 `Send`**：宏生成的处理器 / 中间件 future **不加 `+ Send`**
   （`gen_mcf2` 高阶类型 future 无解）；`Send` 边界放在分派层（servlet 多线程，或把请求
   打包成 `Send` job 经 `mpmc-chan` 分派）。这是 [`integration-decisions-20261112-2045.md`](integration-decisions-20261112-2045.md)
   的结论，也是 P0.1 的依据。
4. **no_std + alloc + 显式分配器 / 域**：宏**不生成**任何无分配器的 `String` / `Vec` /
   `Box`，不生成 `std::sync::OnceLock` / `RwLock` / `Mutex`。生成的路径是 `&'static str`
   （零拷贝）；解码 / 编码走 `ServingSession` 现有读视图（流式，不攒整包）；元数据标记是
   用户提供的 `'static` 值（`EndpointMetadata` 已用 `Any + Send + Sync + 'static`）。宏
   本身在 `proc_macro` 端可以用 `std`，但**生成的运行时代码**必须遵守 `mptp_serving` 的
   内存纪律（[`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md) §6.8）。
5. **流生成能力的边界（新增）**：流用 `abs_async_iter::TrAsyncIterator`（no_std，非 std
   未稳定的 `AsyncIterator`），其 `next_async` 返回 `TrMayCancel`，因此取消可逐项送达
   （§3.4 / §3.5）。但「怎样把业务逻辑写成一条 `TrAsyncIterator`」仍是宏改造里**最大
   未知数**：`gen_mcf2` 只做可取消 future，不提供 async generator 语法糖；`pull_planet`
   返回的流要么手写 `TrAsyncIterator` 实现、要么靠一个「把订阅队列 + 取消令牌封装成迭代器」
   的辅助类型。本文将其单列为 P2 的探索项，不假定 `gen_mcf2` 能覆盖。
6. **宏的体与参数约束**：沿用旧宏的保守边界——方法 `&self`、拒绝泛型方法 / `&mut` 参数 /
   高阶生命周期；`async fn` 作为**表面糖**，宏内部把函数体重写进一个 `gen_may_cancel_future`
   future 并注入取消令牌（而不是真的 `async fn in trait`，后者是 nightly 且未来 `!Send`）。

## 8. 场景 demo 改造前后对比

### 8.1 改造前（现状，`mptp_smux_v1_demo/src/scene/handler_.rs`）

一个 `SceneHandler` 手写 `match (method, location)`，每个分支自己解码、写回复、收尾：

```rust
// 现状（节选）：路由 + 分派 + 404 全在 handler，每个方法各写一遍 decode/encode。
match (method, location.as_str()) {
    (AccessMethod::Call, K_SCENE_JOIN)        => join_async_(handler, session, cancel).await,
    (AccessMethod::Call, K_SCENE_UPDATE)      => update_async_(handler, session, cancel).await,
    (AccessMethod::Call, K_SCENE_UNSUBSCRIBE) => unsubscribe_async_(handler, session).await,
    (AccessMethod::View, K_SCENE_PLANETS)     => view_async_(handler, session).await,
    (AccessMethod::Head, K_SCENE_PLANETS)     => head_async_(handler, session).await,
    (AccessMethod::Pull, uri) if parse_planet_uri_(uri).is_some() => pull_async_(..).await,
    _ => reply_status_async_(session, Status::NotFound).await,
}
```

`pull_async_` 尤其重：`parse_planet_uri_` 手解路径参数、`read_request` 手解码、`SuffixStream`
手开流、订阅 + `SubGuard_` + 心跳 + 退订清理全在一个 100 行的 `loop` 里。

### 8.2 改造后（理想）

路由、方法分派、404/405、解码、编码、元数据、收尾全部下沉到 `mptp_serving` + 宏；业务
只剩方法体。逐端点对照：

| 端点 | 改造前 | 改造后 |
| --- | --- | --- |
| `Call /scene/join` | `join_async_` 手解 `JoinRequest` + 手写 `reply_value_async_` | `#[call("join")] async fn join(&self, req) -> Result<..>` |
| `Call /scene/update` | `update_async_` 手解 `UpdateRequest` + 广播 + 手写 ack | `#[call("update")] async fn update(&self, req) -> Result<..>` |
| `Call /scene/unsubscribe` | `unsubscribe_async_` 手解 + 手写 ack | `#[call("unsubscribe")] async fn unsubscribe(&self, req) -> Result<..>` |
| `View /scene/planets` | `view_async_` 手写目录 | `#[view("planets")] async fn view_planets(&self) -> Result<..>` |
| `Head /scene/planets` | `head_async_` 手写两个头 | `#[head("planets")] async fn head_planets(&self) -> Result<SceneHeadMeta, ..>` |
| `Pull /scene/planets/{id}` | `pull_async_` 手解 `{id}` + 手开 `SuffixStream` + 订阅循环 | `#[pull("planets/{id}")] fn pull_planet(&self, id: PlanetId) -> impl TrAsyncIterator<Item = PlanetEvent, Err = SceneError>` |

改造前 `allowed_methods()` 返回的 `HEAD | VIEW | PULL | CALL`、`match` 里的 404、每个方法
里的 `read_request_async_` / `reply_value_async_` 全部消失；`Pull` 的订阅 + 守卫 + 心跳被
收进 `pull_planet` 返回的流里，处理器只负责把流逐项写进 `SuffixStream`。

> 说明：本文只主张「理想形状」，`pull` 的流封装依赖 §7.5 的流生成能力，属 P2 探索项，不
> 是本轮要落地的东西。

## 9. 分阶段建议

按「先框架补齐 → 再宏 + 服务端 → 再客户端 / 流 → 最后清理」的顺序；每阶段**先新增、后
切换、最后删除**，不破坏既有 `#[rpc(service)]` / `#[rpc(interface)]` / `#[rpc(service_of)]`
的现有用户（含 `rpc_demo`，它只是概念验证器，不优先迁移）。

### P0：框架侧补齐（`mptp_serving`，不碰宏）

1. `TrEndpointHandler` / `TrMiddleware` future 去掉 `+ Send`（落实 §2.1）；
2. `RadixRouter::bind_endpoint(Endpoint)`（或 builder）落地（§2.2）；
3. `MptpContext` 补 `cancel`（待决策 A 确认）与 `headers()` 便捷访问（§2.3）；
4. `ReqHandlerTarget` 同步放宽 `Send`（旧 handler 无改动接入）。

**验收**：一个手写的带 `Timeout` 元数据的端点能经 `bind_endpoint` 挂上并被中间件短路；
`cargo check` / `clippy` / `test` 不回归。

### P1：宏语法 + 服务端映射（`rpc_svc_macro`，demo 场景落地）

1. 实现 `#[rpc_interface("...")]` + 方法 attribute（`call/view/head/post/drop/push/pull/on`）
   + `#[metadata(...)]` + 参数来源（`from_body` / `from_path` / `from_header` / `from_state`）；
2. 每方法生成 `TrEndpointHandler` 实现 + 注册 helper（`install_into`）；
3. 用 `mptp_smux_v1_demo` 场景改写（Call/View/Head 先落地，`Pull` 暂用手写流或待 P2）；
4. 旧三形态宏标 `deprecated`，与新宏并存一版。

**验收**：`scene` 的 `join/update/unsubscribe/view/head` 端到端走通（compio 单线程，
`!Send` future 已满足）；`cargo test --workspace`、`clippy` 不回归。

### P2：客户端代理 + 流语义

1. 生成类型化客户端代理（方法糖 + 类型状态 builder，对齐 `MptpClient`，§5）；
2. `pull` 返回客户端后缀流读视图、`push` 消费请求后缀流；
3. 探索「业务逻辑 → `TrAsyncIterator`」的生成/封装方式（手写实现或辅助类型，见 §7.5），
   补 `mptp_smux` 客户端 `SuffixRead` 逐 chunk 读视图（`mptp-api-plan` P3 拟补）；
4. 决定 `TrRpcClientExt::call::<I>()` 的去留与命名（倾向保留）。

**验收**：`client.scene().view_planets().await?` 与 `client.scene().pull_planet(id)` 可编译、
可跑；`Pull` 逐事件消费。

### P3：清理与收口

1. 删除 `RpcRouter` / tlist 机器 / `TrRpcService` / `TrRpcCallCtx` / `RpcCallCtx` / body
   enum 分派（旧 `#[rpc]` 三形态一并移除或冻结为 deprecated）；
2. 更新 `preludes.rs`、`DESIGN_BLUEPRINT.md`、`dev-notes/README.md` 索引；
3. `rpc_demo` 仅作概念验证回归（不优先），`mptp_cs_smoke` 仓库外回归 11 条 RPC。

**验收**：新宏完全替代旧宏；旧中介类型零引用；文档与 prelude 同步。

## 10. 公开 API 变更清单（待团队确认）

| 变更 | 类型 | 说明 |
| --- | --- | --- |
| `mptp_serving::TrEndpointHandler` / `TrMiddleware` future 去掉 `+ Send` | 破坏（渐进） | 落实 !Send 结论，宏落地前提 |
| `mptp_serving::RadixRouter` 新增 `bind_endpoint(Endpoint)`（或 builder） | 新增 | 让元数据能挂上端点 |
| `mptp_serving::MptpContext` 新增 `cancel` / `headers()` | 新增 | 取消令牌 + 头表便捷访问 |
| `rpc_svc_macro` 新增 `#[rpc_interface]` + 方法/元数据/参数来源 attribute | 新增 | 新宏形状 |
| `rpc_svc_macro` 旧三形态标 `deprecated` → 删除 | 破坏（渐进） | 与旧 `RpcRouter` 一起退役 |
| `rpc_core_v1` 删除 `RpcRouter` / tlist / `TrRpcService` / `TrRpcCallCtx` / `RpcCallCtx` | 破坏 | 收敛到 `TrEndpointHandler` |

## 11. 英文摘要

This note concludes that `rpc_svc_macro` should be reshaped to match `mptp_serving`'s
ASP.NET-Core-like separation of concerns. Instead of one service sharing a `SERVICE_PATH`
with method names hidden in an owned body enum, each method becomes its own `Endpoint`:
the method attribute declares the `AccessMethod` + path (`#[call]`/`#[view]`/`#[head]`/
`#[post]`/`#[drop]`/`#[push]`/`#[pull]`/`#[on]`), repeatable `#[metadata(...)]` markers
(`Timeout`/`Authorize`/`AcceptType`) feed the middleware chain, and parameter attributes
(`#[from_body]`/`#[from_path]`/`#[from_header]`/`#[from_state]`/`#[from_ctx]`) make
parameter sources explicit. The macro emits one `TrEndpointHandler` per method and registers
endpoints into `RadixRouter`, retiring the body-enum dispatch and the `RpcRouter` /
`TrRpcService` / `TrRpcCallCtx` intermediaries so `TrEndpointHandler` becomes the single
dispatch target. The client keeps `TrRpcClientExt::call::<I>()` as the interface entry point
but expands each method into sugar + a type-state builder aligned with `MptpClient`'s
`head/view/post/pull`. Boundary rules are restated: no Tower `Service`/`Layer` machinery, no
Salvo `Depot` bag, no `+ Send` on handler/middleware futures (the `Send` boundary lives in
the dispatch layer), and `no_std + alloc` with explicit allocators/arenas. Three framework
gaps must be closed first (drop `+ Send`, add `bind_endpoint` for metadata, add
`cancel`/`headers` to `MptpContext`); the phases are P0 framework, P1 macro + server
mapping, P2 client proxy + stream semantics, P3 cleanup.

On cancellation (§3.5/§3.6): the `async fn` in the ideal syntax is surface sugar — the macro
rewrites each method into a `gen_may_cancel_future` step function and injects the cancel
token, so business authors never name it; the generated handler drives it with
`ctx.cancel().child_token()`. Streams use `abs_async_iter::TrAsyncIterator` (not std's
unstable `AsyncIterator`), whose `next_async` already returns a `TrMayCancel` future, so the
handler threads the token per item. Client cancellation is not a wire-level control message:
it is the client-local token dropping the substream, which the server observes as a transport
error, combined with server-local connection/subscription tokens — matching the existing
`gen_mcf2` semantics on both sides without inventing a cancel protocol.
