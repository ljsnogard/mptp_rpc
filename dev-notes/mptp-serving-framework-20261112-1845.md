# MPTP 服务端框架设计：借鉴 ASP.NET Core 的分层与 Salvo 的路径树

- 日期：2026-11-12 18:45
- 状态：**设计稿**；本文只固定“框架长什么样”，不实施代码，不改公开 API
- 范围：MPTP 之上的**服务端框架**如何分层、如何分离关注点；不涉及客户端、传输绑定、
  业务体编解码、具体线格式
- 相关文档：
  - [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md)
  - [`router-20261106-0112.md`](router-20261106-0112.md)
  - [`mptp-smux-refactor-20261111-0650.md`](mptp-smux-refactor-20261111-0650.md)
  - [`../DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)
  - 配套计划：[`mptp-api-plan-20261112-1850.md`](mptp-api-plan-20261112-1850.md)

## 0. 一句话

把 ASP.NET Core 的**关注点分离**（路由只匹配、端点携带元数据、中间件按元数据执行策略、
处理器只关心业务语义）搬到 MPTP 上，用 Salvo 的**基于路由规则的路径树**来组织各功能，
但**不用 Salvo 的 Depot**（类型擦除的运行时键值袋），也**不用 Tower 的 `Service`/`Layer`
类型体操**（对开发者心智负担太大）。

## 1. 目标与非目标

### 1.1 目标

1. **路由只负责匹配**：把 `(method, location)` 匹配成一个“端点”，不执行业务；
2. **端点携带元数据**：端点是“将要运行什么”的**描述**，元数据挂在端点上，供后续策略读取；
3. **中间件按元数据执行策略**：鉴权、限流、超时、类型校验、日志等横切关注点，在一条有序的
   中间件链里根据端点元数据决定是否介入、是否短路；
4. **处理器只关心业务语义**：业务函数拿到一个封装好的请求 / 回复上下文，不知道路由、
   不知道中间件、不知道元数据；
5. **路径树组织功能**：像 Salvo 一样用一棵基于路由规则（路径段 + 参数 + 通配）的树把
   各功能组织起来；树上已经存在 `matchit_route` 可作为默认实现。

### 1.2 非目标

- 不重做 `mptp_wire` 的线格式，不重做 `mptp_smux` 的会话驱动；
- 不定义业务体编解码、不定义 RPC 错误体格式（那是 `abs_rpc_serv` / `rpc_core_v1` 的事）；
- 不引入 `dyn Any` 类型擦除的键值袋（Depot 模式）；
- 不引入 `Service<Request>` / `Layer` / `poll_ready` 那一套泛型服务栈（Tower 模式）；
- 不替调用者 spawn、不选择运行时、不管理多连接 / 连接池。

## 2. 参考对象一：ASP.NET Core 的架构

ASP.NET Core 的请求管线把四件事拆得极干净，这正是本文要搬过来的东西。

### 2.1 路由只匹配

`EndpointRoutingMiddleware` 用一个 DFA / 树把 `(method, path)` 匹配成一个 `Endpoint`，并把它
放进 `HttpContext`（`SetEndpoint`）。它**不执行** endpoint，只负责“这条请求该由谁处理”。

### 2.2 端点是“描述”，不是“执行”

`Endpoint` 携带：

- `RequestDelegate`（最终处理器，后面才调用）；
- `EndpointMetadataCollection Metadata`——一串类型化标记。

`app.MapGet("/x", handler)` 返回的是一个可继续装配的 builder，最终产出一个 `Endpoint`。

### 2.3 中间件按元数据执行策略

`AuthorizationMiddleware` 读 endpoint 上的 `IAuthorizeData` 元数据、`CorsMiddleware` 读
`ICorsMetadata`、限流读限流元数据……每个横切关注点都是一个中间件，它只问两个问题：

1. 这个 endpoint 有没有我需要遵守的元数据？
2. 有，就执行我的策略（可能直接短路写 401 / 403 / 429）；没有，就 `next()`。

中间件是**有序的洋葱管线**，顺序由装配者决定，与业务处理器无关。

### 2.4 处理器只关心业务语义

Minimal API 的处理器就是 `(HttpContext ctx) => ...` 形态的委托：它读请求、写回复，不感知
路由树、不感知鉴权中间件。业务逻辑里没有“我怎么被找到”和“谁在我前面拦了一下”。

## 3. 参考对象二：Salvo 的路径树（要学什么、不学什么）

### 3.1 要学的：基于路由规则的路径树组织

Salvo 的 `Router` 是一棵**前缀树（radix/trie）**：每个节点是一个 `Router`，路径按段匹配，
支持 `{id}` 参数与 `**` 通配，树上的节点挂着自己的 handler 集合与中间件。这种“把功能按路径
组织成一棵树”的方式清晰、可局部增删，正是我们要的组织方式。

本项目已有 `matchit_route`（matchit 0.9.2 的 no_std 移植），默认实现直接复用它做路径树，
不新造第二套匹配算法。

### 3.2 不学的：Depot 与类型体操

- **Depot**：Salvo 用 `Depot`（`HashMap<TypeId, Box<dyn Any>>`）沿中间件链传递共享状态。
  它方便，但类型不安全——拿错类型要等到运行期才炸，且让中间件之间的“隐式契约”变得不可
  见。**本文不用它**：跨中间件共享的状态应该显式地放进每请求上下文（`MptpContext`）或
  连接作用域状态（`S`），而不是塞进一个类型擦除的袋子。
- **Tower 的 `Service`/`Layer` 类型体操**：Tower 用 `Service<Req> -> Future<Resp>` +
  `Layer` + `ServiceBuilder` 做可组合服务栈，泛型参数多、心智负担重。**本文不用它**：
  中间件用一个简单的对象安全 trait 或 `Fn` 链表达，元数据决定行为，而不是靠层层泛型包装。

## 4. 概念映射：ASP.NET Core → MPTP

| ASP.NET Core | MPTP 服务端框架 |
| --- | --- |
| `HttpContext` | `MptpContext<C>`：封装一次请求 / 回复的上下文（方法、位置、头表、请求体 / 后缀读视图、回复 helper、连接状态句柄） |
| `HttpRequest` / `HttpResponse` | `MptpContext` 的读半边 / 写半边方法（前缀已经解码好；体 / 后缀按需读取；回复由 helper 写） |
| `Endpoint` | `Endpoint<C>`：`(pattern, MethodSet, metadata, terminal handler)` |
| `EndpointMetadataCollection` | 一串**类型化元数据标记**（`Authorize` / `Timeout` / `AcceptType` / `MaxBodySize` / `RateLimit` …） |
| `EndpointRoutingMiddleware` | 路径树匹配：`(method, location) -> Endpoint + RouteValues` |
| 鉴权 / CORS / 限流中间件 | 每个横切关注点一个中间件，读 `Endpoint.Metadata` 决定介入或短路 |
| `RequestDelegate`（terminal） | `TrEndpointHandler<C, P>`：`handle(ctx, params)`，只关心业务 |
| `WebApplication` / `UseXxx` 装配 | 分层 router + 中间件链的装配 |

## 5. 框架分层与边界

```text
┌───────────────────────────────────────────────────────────────┐
│ 应用：协议端 crate + service 实现 + 资源 handler                │
│       只写 TrEndpointHandler（业务语义）                        │
├───────────────────────────────────────────────────────────────┤
│ 服务端框架（本文）                                             │
│   ① 路径树匹配：(method, location) -> Endpoint + RouteValues   │
│   ② 组装 MptpContext（请求上下文 + 连接状态 + 取消令牌）        │
│   ③ 中间件链：按 Endpoint.Metadata 执行横切策略，可短路        │
│   ④ terminal handler：只做业务                                 │
│   ⑤ 收尾：drain / 半关闭 / 兜底 500                            │
├───────────────────────────────────────────────────────────────┤
│ MPTP 会话：前缀 / 体边界 / 后缀流 / 一次请求的会话顺序          │
│       abs_mptp（形状） + mptp_wire（线格式） + 会话驱动         │
├───────────────────────────────────────────────────────────────┤
│ 多流传输：open / accept / read / write / half-close / reset     │
│       smux_v1（当前）/ QUIC（候选）                             │
└───────────────────────────────────────────────────────────────┘
```

关注点分离的硬边界：

1. **路径树不知道业务**：它只产出一个 `Endpoint` + 路由参数（`RouteValues`）；
2. **端点不执行策略**：`Endpoint` 只描述“谁来跑 + 要遵守什么元数据”；
3. **中间件不写业务**：中间件只读元数据、只决定放行 / 短路 / 改写上下文，不碰业务体；
4. **处理器不感知横切**：`TrEndpointHandler` 只拿到 `MptpContext` 与参数，不知道路由与
   中间件如何装配；
5. **收尾统一**：框架在处理器返回后按统一次序收尾（先排空已写回复 → 半关闭写端 → 读尽
   对端），处理器不自己半关闭（除非走 escape hatch）。

## 6. 核心类型设计

> 本节类型名是**草案名**，用于固定形状，不保证可直接编译。默认实现放在
> `rpc_core_v1::serving`（或独立 crate），`mptp_smux` 仍不承担路由与中间件。

### 6.1 `MptpContext<C>`：请求 / 回复上下文

```rust
/// 一次请求的上下文：把「读请求、写回复、拿连接状态」收拢到一个对象。
///
/// 它对应 ASP.NET Core 的 `HttpContext`。处理器与中间件都通过它访问请求 / 回复，
/// 不直接操作两个 channel 半边。
pub struct MptpContext<'f, C> {
    method: AccessMethod,
    location: &'f str,
    headers: &'f mut Headers,
    route: RouteValues,            // 路由参数（路径参数 + 命中信息）
    cancel: RequestCancel,         // 请求级取消令牌
    state: &'f S,                  // 连接作用域状态（见 §6.2；'static 时可为 () ）
    session: ServingSession<C>,    // 底层会话：体 / 后缀读视图 + 回复 helper
}

impl<'f, C> MptpContext<'f, C> {
    pub fn method(&self) -> AccessMethod;
    pub fn location(&self) -> &str;
    pub fn headers(&self) -> &Headers;
    pub fn headers_mut(&mut self) -> &mut Headers;
    pub fn route(&self) -> &RouteValues;
    pub fn state(&self) -> &S;
    pub fn cancel(&self) -> &RequestCancel;

    pub fn req_body_async(&mut self) -> ...;      // 按头表声明的体边界取读视图
    pub fn req_suffix_async(&mut self) -> ...;    // 丢弃未读体后把后缀流交出去
    pub fn response(&mut self, status: Status) -> ResponseBuilder<'_, C>;
}
```

处理器拿到的就是 `&mut MptpContext`；它不需要知道 session 的内部状态机。

### 6.2 连接作用域状态 `S`

`MptpContext` 携带 `&S`：**一条连接一份**的状态（join 身份、订阅表、保活、连接级取消）。
`S` 由装配者提供，框架不解释它。处理器只拿 `&S`（共享借用），因为同一连接上可以并发多个
session；需要可变性时 `S` 内部用 `RefCell` / 原子，**不要**给处理器 `&mut S`。

> 这解决了“per-connection 状态 owner 缺位”的问题：连接状态不再隐式地藏在 handler 的闭包
> 捕获里，而是显式地由连接对象持有并注入。

### 6.3 `Endpoint<C>`：端点 + 元数据

```rust
/// 一个路由目标：pattern + 方法集合 + 元数据 + 终端处理器。
pub struct Endpoint<C> {
    pattern: &'static str,        // 相对所属 router，如 "feeds/{id}"
    methods: MethodSet,
    metadata: EndpointMetadata<C>,
    handler: Arc<dyn TrEndpointHandler<C>>,   // 类型擦除只发生在装配边界
}
```

- `pattern` 与 `methods` 决定它是否命中；
- `metadata` 是**有序的类型化标记集合**；
- `handler` 是终端处理器，只有命中后才会被调用。

### 6.4 `EndpointMetadata<C>`：类型化元数据，不是 Depot

```rust
/// 端点上的类型化元数据集合。
///
/// 与 Salvo `Depot` 的区别：这里不存运行期状态，只存「声明」；中间件用 `get::<T>()` 读取，
/// 缺失即代表“本端点不需要这项策略”。
pub struct EndpointMetadata<C> {
    entries: Vec<Arc<dyn TrMetadataEntry<C>>>,
}

impl<C> EndpointMetadata<C> {
    pub fn get<T: TrMetadata<C> + 'static>(&self) -> Option<&T>;
    pub fn insert<T: TrMetadata<C> + 'static>(&mut self, value: T);
}

/// 一个元数据标记：零成本、可比较，只描述“要遵守什么”，不携带运行期可变状态。
pub trait TrMetadata<C>: Send + Sync { /* marker */ }
```

典型元数据：

```rust
pub struct Authorize { pub allow: AuthzPolicy }       // 鉴权
pub struct Timeout(pub Duration);                     // 每请求超时
pub struct AcceptType(pub TypeId);                    // Data_Type_Id 校验
pub struct MaxBodySize(pub usize);                    // 体上限
pub struct RateLimit { pub key: &'static str, pub per: u32, pub window: Duration }
```

注意：这些是**不可变声明**。跨中间件共享的**运行期状态**（如“这个请求已通过鉴权、主体是谁”）
放在 `MptpContext` 里，而不是塞回元数据；这与 ASP.NET Core 用 `HttpContext.User` 而非元数据
存放运行时主体的做法一致。

### 6.5 中间件：按元数据执行策略

```rust
/// 中间件：读 endpoint 元数据，决定放行 / 短路 / 改写上下文，再决定是否调用 next。
///
/// 对象安全：中间件在装配期装箱一次；请求路径上没有泛型服务栈。
pub trait TrMiddleware<C>: Send + Sync {
    fn call<'f>(
        &'f self,
        endpoint: &'f Endpoint<C>,
        ctx: &'f mut MptpContext<'f, C>,
        next: &'f mut dyn FnMut(&'f mut MptpContext<'f, C>) -> MiddlewareFuture<'f>,
    ) -> MiddlewareFuture<'f>;
}
```

一个鉴权中间件的形状：

```rust
impl<C> TrMiddleware<C> for AuthMiddleware {
    fn call<'f>(&'f self, ep: &'f Endpoint<C>, ctx: &'f mut MptpContext<'f, C>,
                next: &'f mut dyn FnMut(..) -> ..) -> .. {
        Box::pin(async move {
            match ep.metadata().get::<Authorize>() {
                None => next(ctx).await,                 // 本端点不要求鉴权
                Some(authz) => match check(ctx, authz).await {
                    Ok(principal) => { ctx.set_principal(principal); next(ctx).await }
                    Err(_) => ctx.response(Status::Unauthorized).no_body_async().await,
                },
            }
        })
    }
}
```

要点：

- 中间件**不知道**下一个是谁、也不包装泛型类型；`next` 是一个普通回调，短路的代价很小；
- 中间件只看 `Endpoint.Metadata`，不看业务体——策略与业务彻底分开；
- 中间件链的顺序 = 装配顺序（洋葱：`A → B → handler → B → A`）。

### 6.6 终端处理器：只关心业务语义

```rust
/// 终端处理器。参数结构由 `P` 决定，handler 只拿到 `ctx` 与 `params`。
pub trait TrEndpointHandler<C>: Send + Sync {
    fn handle<'f>(&'f self, ctx: &'f mut MptpContext<'f, C>) -> HandlerFuture<'f>;
}
```

业务代码里看不到 `MethodSet`、`EndpointMetadata`、中间件链、路径树。

> **关于 future 的 `Send`（2026-11-12 修正）**：`HandlerFuture` / `MiddlewareFuture` **不要
> 加 `+ Send`**。`gen_mcf2` 生成的带高阶类型的 future 无法被 Rust 编译器穿透解释，整条
> `abs_buff` / `abs_denco` future 链天然 `!Send`，这是无解的。`Send` 边界应放在**分派层**：
> `MptpServlet` 处理请求这一层可多线程分派，或把请求打包成 `Send` 对象经 `mpmc-chan` 交给
> 另一条流水线；handler / 中间件在单条流水线上跑，future 无需 `Send`。handler / 中间件
> 类型本身仍可 `Send + Sync`（`Arc<dyn TrEndpointHandler>` 装箱不受影响）。

### 6.7 路由树：复用 `matchit_route`，Salvo 式组织

沿用 [`router-20261106-0112.md`](router-20261106-0112.md) 的分层 router：每个 router 以
自己为根维护一棵局部路径树；树的叶子是 `Endpoint`（带方法集合与元数据），中间节点可以挂子
router。`matchit_route` 提供静态 > 参数 > 通配的段匹配；`MethodSet` 提供方法维度。

```text
RadixRouter<C>
  ├─ root endpoint（本层兜底）
  ├─ handler 规则（精确路径模式 + MethodSet → Endpoint）
  │    ├─ "echo"                    (CALL|POST) → EchoEndpoint
  │    ├─ "feeds/{id}"              (VIEW)      → FeedViewEndpoint  [Timeout, AcceptType]
  │    └─ "feeds/{id}"              (PULL)      → FeedPullEndpoint  [Authorize, Timeout]
  └─ 子 router 规则（静态前缀 → 子 router）
       ├─ "api/v1"  → interface router
       └─ "admin"   → admin router
```

匹配产物不是“一个 target”，而是 `(Endpoint, RouteValues)`；随后框架组装 `MptpContext`、
跑中间件链、最后调用 `endpoint.handler`。

### 6.8 运行环境与内存纪律（no_std + alloc + 显式分配器 / 域）

整套框架的目标运行环境是 **`#![no_std]` + `alloc`**，与 `mptp_wire` / `abs_mptp` /
`mptp_smux` / `matchit_route` 一致。因此**任何堆上对象都必须显式说明它从哪来**，三条路
只能走其一：

1. **带分配器参数**：`Vec<T, A>` / `Box<T, A>` / `BTreeMap<K, V, A>` 等一律带
   `A: Allocator`；`String` 一律用 [`abs_str::String<A>`]（默认 `A = Global`，不是
   `std::string::String`），构造用 `new_in` / `from_str_in` / `with_capacity_in` 显式传
   分配器（`matchit_route` 已经这样用：`abs_str::str_alloc::String<Global>`）；
2. **进域（scope / arena）**：生命周期明确归某个 owner 的堆对象，用 `sw-scope` 的 `Scope`
   （`put_*` / `Retain<T>` / `Owning<T>`，字符串用 `ScopeStr`）或 `bumpalo` 的自有 arena
   （`bumpalo::collections::Vec` / `bumpalo::boxed::Box`）管理，随 owner 一起释放；
3. **借用（零拷贝）**：能在匹配 / 请求生命周期内借用的，优先借 `&str`，不复制。

落到本文各类型上：

| 类型 | 内存策略 |
| --- | --- |
| `Endpoint.pattern` / `methods` | `&'static str` / `MethodSet`（零拷贝 + 栈上值） |
| `RouteValues`（路径参数） | 借用 `matchit_route` 的 `Params`：参数名借路由表、值借 `location`，**不**用 owned `Vec<(String, String)>` |
| `MptpContext.location` / `headers` | 借用 `&str` / `&mut Headers`，不复制 |
| `EndpointMetadata.entries` | `Vec<Arc<dyn TrMetadataEntry>, A>` 带分配器；或放进 endpoint 自有的 `bumpalo` arena |
| 错误文本（`RouterError` / 中间件拒绝文案） | `abs_str::String<A>`（或 `ScopeStr`），不用 `std::string::String` |
| 路径树（`matchit_route`） | 已用 router 自有 `bumpalo` arena，无需改 |

**禁止项**：`std::sync::OnceLock` / `RwLock` / `Mutex` 等标准库同步原语；不带分配器的
`String` / `Vec` / `Box` / `VecDeque`。需要同步用项目现有的 `abs_sync` / `atomic_sync`
体系；需要 `Send + Sync` 时按 `matchit_route` 的手写 `unsafe impl` 模式并附安全注释。

> 这是硬约束，不是可选优化：`mptp_smux` 已为移除 `std::sync::OnceLock` / `futures` /
> `thiserror` 默认 feature 做过一轮；本框架从第一行代码就按这个纪律写，避免后面再返工。

## 7. 执行模型

```text
servlet accept 一条子流，解出 (method, location, headers)
    │
    ▼
① 匹配：顶层 router.match(method, location)
    │   -> (Endpoint, RouteValues, fallback)
    ▼
② 组装：MptpContext { method, location, headers, RouteValues, cancel, &S, ServingSession }
    ▼
③ 中间件链（洋葱）：按 Endpoint.Metadata 依次执行
    │   任一中间件短路 -> 写 401/403/429/... -> 跳到 ⑤
    ▼
④ 终端处理器：endpoint.handler.handle(ctx) —— 只做业务
    │   返回 Ok：若未回复，兜底 500
    │   返回 Err：若未回复，兜底 500，错误经观测通道上报，不泄漏到线上
    ▼
⑤ 收尾：drain 未读请求体 / 后缀 → 半关闭写端 → 读尽对端 → 释放
```

关键点：

- **只有 ① 关心路由，只有 ③ 关心横切，只有 ④ 关心业务**；三段互不知道内部细节；
- 405 由“路径命中但方法不在 `Endpoint.methods`”触发，`Allow` 取该路径上所有 endpoint 的
  方法并集；404 由“本层无 endpoint 命中”落到本层根 endpoint；
- 请求级取消令牌 `RequestCancel` 贯穿 ②③④⑤，任何 await 点都可取消；
- 收尾在框架里统一做，处理器默认不半关闭（除非显式走 escape hatch 拿回 session）。

## 8. 与现有分层 router 的关系

本文**不是**推翻 [`router-20261106-0112.md`](router-20261106-0112.md)，而是在它的分层
router 之上再长一层：

| router 设计稿里的概念 | 本文的对应 |
| --- | --- |
| `RadixRouter` / `TrRouter` / `TrRouteMatcher` | 不变：匹配骨架 |
| `TrRouteTarget` / `RouteRequest` / `RouteTargetFuture` | 收敛为 `Endpoint` + `MptpContext` |
| `RouteRequest.params`（运行期 `RouteParams`） | `MptpContext.route()`，处理器仍可用类型化 `P` |
| `RequestCancel` | 不变，进入 `MptpContext` |
| （无） | 新增 `EndpointMetadata` + `TrMiddleware` + `TrEndpointHandler` |

也就是说：**路由层产出端点与元数据，中间件层消费元数据，处理器层只吃上下文**。分层的价值
正在于这三层可以独立演进、独立测试。

## 9. 示例用法（示意）

```rust
// 业务处理器：只写语义。
struct FeedView { feed_: Arc<Feed> }

impl TrEndpointHandler<SrvCfg> for FeedView {
    fn handle<'f>(&'f self, ctx: &'f mut MptpContext<'f, SrvCfg>) -> HandlerFuture<'f> {
        Box::pin(async move {
            let id: FeedId = ctx.route().get_typed("id")?;   // 或类型化 P
            let item = self.feed_.get(id).await?;
            let mut out = encode(item)?;
            ctx.response(Status::Ok).chunked_async(&mut out).await?;
            Ok(())
        })
    }
}

// 装配：路由 + 元数据 + 中间件。
let top = Arc::new(RadixRouter::<SrvCfg>::new(AppRootEndpoint));

top.view::<FeedParams>(FeedView::new(...))
    .with_metadata(Timeout(Duration::from_secs(5)))
    .with_metadata(AcceptType(FeedItem::TYPE_ID))?;

top.pull::<FeedParams>(FeedPull::new(...))
    .with_metadata(Authorize { allow: READ_FEEDS })?;

let pipeline = Pipeline::new()
    .use_middleware(TimeoutMiddleware)
    .use_middleware(AuthMiddleware)
    .use_middleware(TypeCheckMiddleware);

let server = MptpServlet::new(RouterApp::new(top, pipeline));
```

处理器 `FeedView` 完全不知道有超时、鉴权、类型校验这些中间件——它们由元数据驱动，在处理器
前后按序执行。

## 10. 明确的“不做”清单（防退化）

1. **不做 Depot 式 `dyn Any` 状态袋**：跨中间件状态放 `MptpContext` 的显式字段或连接状态
   `S`，不放类型擦除的 map；
2. **不做 Tower 式 `Service`/`Layer` 服务栈**：中间件是对象安全的 `TrMiddleware`，`next`
   是普通回调，不搞泛型包装器；
3. **不做“中间件也能写业务体”**：中间件最多写拒绝回复（401/403/429…）并短路，不参与业务
   体编解码；
4. **不做“路由层执行业务”**：路由只产端点与参数；
5. **不做“处理器自己半关闭 / 收尾”作为默认**：收尾统一在框架，处理器只走 helper；
6. **不把元数据当运行期状态**：元数据是声明，运行期主体 / 配额等放在上下文里；
7. **不隐藏 spawn、不选运行时**：并发、任务调度仍是装配者的事；
8. **不引入无分配器的堆容器**：任何 `String` / `Vec` / `Box` / `VecDeque` 要么带
   `A: Allocator`（`String` 用 `abs_str::String<A>`），要么放进 `sw-scope` / `bumpalo`
   域；不用 `std::sync::OnceLock` / `RwLock` / `Mutex` 这类标准库同步原语。

## 11. 开放问题

1. 元数据 `get::<T>()` 的类型擦除粒度：是 `Vec<Arc<dyn Any>>`（内部一次 downcast，代价小、
   面向标记），还是每类一个独立 `Option<T>` 字段（更显式、但装配更啰嗦）？倾向前者，因为
   它只在端点装配期构造、请求路径只读。
2. 中间件的错误如何与 ⑤ 收尾衔接：中间件短路写回复后，是否仍让后续中间件 / 处理器感知？
   建议短路即直接进入收尾，不调用 `next`。
3. `MptpContext` 的 `S` 是否需要 `'static`：若 `S` 借用装配者，`TrEndpointHandler` 需要
   变成 `'static` 才能装箱；建议 `S: Send + Sync + 'static`，连接状态由 `Arc<MptpConnection>`
   持有，处理器拿 `&S`。
4. 元数据是否需要“继承”（子 router 默认继承父层元数据，如全局 `Timeout`）？ASP.NET Core
   有 `GroupMetadata` 的组概念；建议第一版提供“组”（挂子 router 时可附加默认元数据），
   端点上可覆盖。
5. 与 RPC 层 `TrRpcService` / `#[rpc]` 宏的关系：RPC 的 service 就是一个“只接受 `Call`、
   带 `AcceptType` 元数据、handler 做 body enum 分派”的端点；宏生成物不改，装配期用
   `rpc_interface` 收敛。
6. **no_std 下的惰性初始化替代**：`rpc_core_v1` 当前用 `std::sync::OnceLock` 做
   `matchit_route` 索引表的惰性构建，本框架是 no_std，需要一个替代（`abs_sync` 体系的
   `OnceLock` 等价物，或改成“装配期一次性构建 + 只读匹配”，避免惰性初始化）。倾向后者：
   路由表在装配期就绪，请求路径零锁、零惰性；运行期增删用读多写少锁（`abs_sync` 的
   `RwLock` 等价物）或写时拷贝。

## 12. 英文摘要

This note designs the MPTP server framework by porting ASP.NET Core's separation of
concerns onto MPTP and organizing features with a Salvo-style path tree. Routing only
matches `(method, location)` into an `Endpoint` carrying typed metadata; an ordered
middleware chain runs cross-cutting policies (auth, timeout, type check, rate limit) by
reading that metadata and may short-circuit; the terminal `TrEndpointHandler` only sees
an `MptpContext` and does business logic. We deliberately reject Salvo's type-erased
`Depot` state bag and Tower's `Service`/`Layer` type machinery: runtime state lives on
`MptpContext`/connection-scoped `S`, and middleware is an object-safe trait with an
ordinary `next` callback. The layered `RadixRouter` from the router note stays as the
matching backbone; this note adds the endpoint/metadata/middleware/handler layer above it.
