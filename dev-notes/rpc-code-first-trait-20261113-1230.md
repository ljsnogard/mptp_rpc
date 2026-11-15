# 恢复 trait 定义协议的 code-first RPC（ASP.NET Core gRPC 调研）

- 日期：2026-11-13 12:30
- 状态：**调研 + 设计稿 + 2026-11-13 评审决策已记录**；本文只回答「ASP.NET Core 怎么做」
  「mptp_rpc 该怎么落」，不实施代码。评审确认的决策见下方「决策记录」；实施阶段若再出现
  新的公开 API 变更，仍按 `AGENTS.md` 第 1 条先确认。
- 范围：
  - 恢复「协议 crate 用 trait 定义客户端 / 服务端 RPC 契约」的能力；
  - 保持 `mptp_serving` 现有的 `Endpoint` / `EndpointMetadata` / `TrMiddleware` /
    `RadixRouter` 自由装配能力；
  - trait 形式的 RPC 只支持 `CALL`，其它访问方法（`VIEW` / `HEAD` / `PULL` / `PUSH` …）
    仍走 controller / 手写 handler，不在本文范围；
  - 与 `mptp_cs_smoke`（重构前形状）和 `DESIGN_BLUEPRINT.md`（协议 crate 形状）对齐。
- 相关文档：
  - [`../DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)
  - [`rpc-svc-macro-mptp-serving-20261112-2130.md`](rpc-svc-macro-mptp-serving-20261112-2130.md)
  - [`mptp-serving-gap-plan-20261113-0050.md`](mptp-serving-gap-plan-20261113-0050.md)
- 代码基准：`feat/serv-framework`；开始调研时 `mptp_rpc` 工作区干净
  （`git status --short` 为空），本文只新增文档。

## 决策记录（2026-11-13 评审，已生效）

| # | 决策 | 落点 |
| --- | --- | --- |
| 1 | **不兼容旧线上格式**，不生成 service 级别名端点 | §0.3、§3.2、§6.1 |
| 2 | **接受多开一个 nightly feature**：`associated_type_defaults`；不再考虑 PhantomData 替代方案 | §3.5、§6.2 |
| 3 | **人体工学 API 放到独立 crate**（工作名 `rpc_serv_ext`），协议 crate 与业务 impl crate 不必依赖它；只有装配 crate（composition root）按需依赖 | §3.9、§6.1 |
| 4 | **增加 `RpcEndpointInfo`** 方法级元数据 | §3.8、§4.5 |
| 5 | `#[rpc(service_of(...))]` **改名 `#[rpc(impl_of(...))]`** | 全文（legacy 历史引用保留旧名） |
| 6 | **协议层禁用 `path` 一词**：改为 `namespace` / `service` / `operation` / `pattern`；区分「pattern」与「router 挂载点」 | §3.1、§3.3.1、§3.8 |
| 7 | **denco 已实现 `()` 的 `TrEncodable` / `TrDecodable`**，`RpcResult<()>` 可直接使用（已落地） | §3.7、§3.4 |
| 8 | **客户端挂载点改为 service trait 自己生成的 marker，并取消 namespace 概念**：`#[rpc(service = "im/v1/room", api = X)]` 生成 `X`；`service` 是**完整服务前缀**；客户端 `client.call::<X>()`。`TrRpcInterface` 只携带 `PREFIX`；interface marker 不再需要 | §11 |
| 9 | **Call 使用 service 级路径 + 生成的请求枚举**：`CALL /im/v1/room`，body 是 `RoomServiceCall` 枚举；不再为每个 Call 方法分配 path。`RpcEndpointInfo` 重构为 service + 方法表；方法级校验在解码 payload 前完成 | §12 |

> 第 6 条的因果链：用户指出「具体 path 由 router 挂载时决定」，因此协议宏再声明一个叫
> `path` 的字符串会与挂载位置冲突/混淆。本文把协议层的东西改名为
> `NAMESPACE`（契约命名空间）+ `SERVICE_NAME` + `OPERATION`，把宏生成的规则叫
> `PATTERN`（相对 router 根），并把「挂载点」单独建模（§3.3.1）。

## 0. 结论摘要

### 0.1 ASP.NET Core 的 code-first 不是「另一套 RPC」

C# 的 code-first gRPC（protobuf-net.Grpc）把 **interface 方法在绑定期翻译成标准 gRPC
描述符** `Method<TRequest, TResponse>`（含 service name、method name、Marshaller），
之后：

- 客户端：`CreateGrpcService<T>()` 生成接口代理，代理把方法调用交给 `CallInvoker`；
- 服务端：`AddCodeFirstGrpc()` 只注册一个 `IServiceMethodProvider<TService>`，
  它在 `MapGrpcService<T>()` 构建路由时把每个方法登记成 **一条独立的 ASP.NET Core
  Endpoint**（`/Service/Method`），并挂 `GrpcMethodMetadata`；
- 请求到达时由 `ServerCallHandler` 反序列化、`IGrpcServiceActivator` 激活服务实例、
  调用用户方法、写回响应。

也就是说：**code-first 的产物仍然是「一方法一路由一 Endpoint」**，因此 ASP.NET Core 的
路由、授权、中间件、限流全部按方法粒度生效。这是本文最需要照搬的部分。

### 0.2 mptp_rpc 的落地结论

恢复三个宏入口，并让每个 trait 方法编译成 `mptp_serving` 的一个普通 `Endpoint`：

```text
#[rpc(service = "workspace")] trait WorkspaceService { ... }
#[rpc(namespace = "api/v1", services = [...])] struct ApiV1;
#[rpc(impl_of(ApiV1))] impl WorkspaceService for WorkspaceServiceImpl { ... }
```

- **路由规则（pattern）**：`{ApiV1::NAMESPACE}/{WorkspaceService::SERVICE_NAME}/{operation}`，
  例如 `api/v1/workspace/create_workspace`。它是**相对 router 根**的规则，不是绝对
  URL path；实际线上路径 = router 挂载点 + pattern（§3.3.1）；
- **方法粒度 Endpoint**：每个方法一个 `TrEndpointHandler`，用
  `MethodSet::CALL` 绑进同一个 `RadixRouter`，与 controller / 自由函数 / 手写 handler
  **混挂在同一张路由表上**；
- **请求体**：按方法参数个数选择 `()` / 单参数 owned 类型 / 多参数字段结构体（`f0`、`f1`…），
  不再用「一个 service 一个 body enum」；
- **pattern 拼接**：`#[rpc(impl_of(I))]` 展开时 `I` 与具体 impl 都是**具体类型**，
  用 `constcat::concat!(<I as TrRpcInterface>::NAMESPACE, "/",
  <Impl as Trait>::SERVICE_NAME, "/", "operation")` 在编译期得到 `&'static str`；
  **不需要改 `mptp_serving` 的 `Endpoint` pattern 类型，也不需要嵌套子 router**；
- **客户端**：复用现有 `ClientProxy` + `TrRpcClientExt::call::<I>()` +
  `call_unary_async`，调用点与重构前完全一致；
- **不复活** `RpcRouter` / `TrRpcService` / `TrRpcCallCtx` / body enum 分派
  （即 `rpc-svc-macro-mptp-serving-20261112-2130.md` 已判定退役的那套中介）。

一句话因果链：ASP.NET Core 的 code-first 之所以不丢中间件能力，是因为它仍然产出
**per-method Endpoint**；mptp_rpc 已经有 `Endpoint` + 元数据 + 洋葱中间件，因此恢复
trait 形态的正确做法不是恢复「service 级单端点」，而是**把 trait 方法编译成 per-method
Endpoint**。

### 0.3 代价与边界（先说清楚）

1. **线上格式会变，且不再兼容旧格式（已决策）**：旧形状是
   `/{namespace}/{service}` + 共享 body enum；新形状是
   `/{namespace}/{service}/{operation}` + 每方法请求结构体。旧客户端不能直连新服务端，
   也不生成 service 级别名端点；客户端与服务端一起升级。
2. **协议 crate 需要多开一个 nightly feature（已决策接受）**：
   `associated_type_defaults`（用于把隐藏请求类型挂到 trait 上，让服务端宏在别的
   crate 里也能命名它）。工作区本来就需要 `impl_trait_in_assoc_type`。
3. **协议 crate 的代码生成会引用 `::alloc`**：不假设 `std`，避免把 no_std 协议 crate
   排除掉；服务端生成的代码用 `::alloc::sync::Arc` / `::alloc::alloc::Global`。
4. **service impl 必须 `Send + Sync + 'static`**：`TrEndpointHandler: Send + Sync`
   （`mptp_serving/src/handler_.rs:36`），handler 持有 `Arc<Impl>`；这是新框架的既有
   约束，不是本次新增。
5. **只支持 `CALL`**：trait 方法固定绑 `MethodSet::CALL`；其它方法访问同一 pattern 会
   得到框架统一的 `405`（`mptp_serving/src/serve_handler_.rs:102-118`）。
6. **`RpcResult<()>` 可用（已落地）**：denco 已为 `()` 实现 `TrEncodable` /
   `TrDecodable`；mptp_rpc 侧不需要为「空成功类型」写特例，服务端照常
   `write_ok_async(&())`，客户端照常解码 `()`（§3.7）。

---

## 1. 背景与现状

### 1.1 当前的服务端组织方式

重构后的 `mptp_serving` 把「路由 → 端点 → 元数据 → 中间件 → 处理器」拆开：

- `Endpoint<C, S, R, A>` = `pattern + MethodSet + EndpointMetadata + Arc<dyn
  TrEndpointHandler>`（`mptp_serving/src/endpoint_.rs:24-40`）；
- `RadixRouter` 以 `matchit_route` 做路径匹配，`bind_endpoint` 接受一个已经构造好的
  `Endpoint`（`mptp_serving/src/router_.rs:388-407`），`bind_router` 可挂子 router；
- 中间件是洋葱：`TrMiddleware::call(endpoint, ctx, next)`，可以读
  `endpoint.metadata()`（`mptp_serving/src/middleware_.rs:63-75`）；
- handler 只拿 `MptpContext`，写回复即可（`mptp_serving/src/handler_.rs:36-45`）；
- `MptpContext` 提供 `method()` / `location()` / `route()` / `headers()` /
  `session_mut()` / `request_state()` / `cancel()`（`mptp_serving/src/context_.rs`）。

`#[rpc]` 当前的两种形态都直接落在这一层：

- controller：`#[rpc(interface = "...")] impl Service { #[rpc(call = "...")] async fn ... }`
  → 每方法一个 handler + `install_into` + 客户端代理（`rpc_svc_macro/src/expand_.rs`）；
- 自由函数：`#[rpc(call = "full/path")] async fn ...` → 单元 handler + `install_into`。

两者都通过宏展开期的 `full_path_lit_` 把 interface 与相对路径拼成字面量
（`rpc_svc_macro/src/expand_.rs:598-605`），因此不涉及运行期路径拼接。

### 1.2 重构删掉了什么

`#[rpc]` 现在只分派 `syn::Item::Impl`（controller）与 `syn::Item::Fn`（自由函数）
（`rpc_svc_macro/src/lib.rs:153-172`）。重构前的三个入口——`#[rpc(service = "...")]`
trait、`#[rpc(path = "...", services = [...])]` interface marker、
`#[rpc(service_of(I))]` 服务端 impl——在 P3「delete legacy RPC serving layer」
（commit `50ecae1`）里随旧 serving 层一起删除。**新设计把后两者改名为
`namespace = "..."` 与 `impl_of(...)`**（§6，决策 5/6）；下面的历史引用保留旧名。

删除前那套代码的形状可从 git 历史取回，是本次恢复的直接参考：

```bash
git show 50ecae1^:rpc_svc_macro/src/lib.rs
git show 50ecae1^:rpc_svc_macro/src/analyze_.rs       # 1691 行，参数/返回类型/trait 校验齐全
git show 50ecae1^:rpc_svc_macro/src/expand_.rs        # 1123 行，body enum + dispatch + client impl
git show 50ecae1^:rpc_svc_macro/tests/service_macro.rs # 636 行，形状与行为测试
git show 50ecae1^:abs_rpc_serv/src/service.rs         # TrRpcCallCtx / TrRpcService
git show 50ecae1^:rpc_core_v1/src/serving/rpc_router_.rs
```

生产代码里保留的「用户形状」在两处：

- `mptp_cs_smoke/src/lib.rs`（legacy 旧名）：
  `#[rpc(service = "workspace")] pub trait WorkspaceService`、
  `#[rpc(path = "api/v1", services = [WorkspaceService, SessionService])] pub struct ApiV1`；
- `DESIGN_BLUEPRINT.md:24-189`：同一套 trait / interface / 服务端 impl / 装配示意。

注意：`mptp_cs_smoke` 是**重构前**的端到端示例，当前不能编译——它的 `src/server.rs`
仍调用已删除的 `Router` / `RpcRouter` / `MptpServer`。它应作为参考而不是基线或者目标。

### 1.3 已经保留、可以直接复用的部分

恢复的重点是宏展开，运行时的大半不需要新写：

| 能力 | 现状位置 | 复用方式 |
| --- | --- | --- |
| 客户端代理 `ClientProxy<'a, C, I>` | `rpc_core_v1/src/client/proxy_.rs:53-85` | trait 宏只生成 `impl Trait for ClientProxy` |
| `client.call::<I>()` | `TrRpcClientExt`，`proxy_.rs:111-126` | 原样保留 |
| interface 选择器 | `ServiceSelector`，`proxy_.rs:132-144` | interface 宏照旧生成 |
| 通用 unary 调用 | `call_unary_async`，`proxy_.rs:245-258` | 每方法传相对路由键 `service/operation` |
| interface 命名空间 + 拼接 | `TrRpcInterface` / `concat_location`，`rpc_core_v1/src/messaging/interface_.rs:41-68` | 客户端不变（`PATH` 常量改名 `NAMESPACE`，见 §6） |
| 请求体解码 / 回复写出 | `rpc_core_v1/src/serving/body_.rs` | `decode_body_async` / `write_ok_async` / `write_status_async` |
| 业务错误 → 线上状态 | `RpcError::into_reply_parts`，`rpc_core_v1/src/messaging/error_.rs:130` | 服务端 handler 直接用 |
| 端点 / 路由 / 元数据 / 中间件 | `mptp_serving` | 生成的 endpoint 用 `bind_endpoint` 挂上 |
| 取消令牌 | `MptpContext::cancel()`，`context_.rs:230` | handler 里 `.may_cancel_with(...)` |

### 1.4 不可丢的现有能力

恢复后的 trait 端点必须是「普通端点」，才能同时满足以下几条：

1. 与 controller / 自由函数 / 手写 `TrEndpointHandler` 混挂同一个 `RadixRouter`；
2. 能被 `router.use_middleware(...)` 的洋葱链包住；
3. 能继承 `with_group_metadata`，也能挂端点自己的元数据；
4. `405` / `404` / 兜底 `500` 的收尾逻辑不变（`serve_handler_.rs:138-165`）；
5. 请求级状态 `R`、连接状态 `S`、取消源都仍由顶层 router 提供。

因此**不能**恢复旧 `Router` / `RpcRouter` / `MptpServer` 那条独立分派链——那会把
trait 端点变成路由表之外的「黑盒」。

---

## 2. ASP.NET Core gRPC code-first 调研

> 版本基准：`grpc-dotnet` `master`、`protobuf-net.Grpc` `main`（2026-11 抓取）。
> 本节只提炼与 mptp_rpc 设计相关的机制，不展开 C# 语法细节。

### 2.1 契约就是普通接口

protobuf-net.Grpc 用 `[ServiceContract]` / `[OperationContract]`（或它自带的
`[Service]` / `[Operation]`）标注一个普通 C# 接口，消息类型用
`[DataContract]` / `[DataMember]` 或 `[ProtoContract]` / `[ProtoMember]`：

```csharp
[ServiceContract]
public interface IGreeterService
{
    [OperationContract]
    Task<HelloReply> SayHelloAsync(HelloRequest request, CallContext context = default);
}
```

官方文档明确：code-first 只适合「整个系统都用 .NET」，因为 .NET 类型不能直接给其它
语言用；跨语言仍需要等价的 `.proto`
（[MS Learn: Code-first gRPC](https://learn.microsoft.com/en-us/aspnet/core/grpc/code-first)）。
protobuf-net.Grpc 是社区项目，作用是把 code-first 能力加到 `Grpc.AspNetCore` 与
`Grpc.Net.Client` 上。

### 2.2 契约 → `Method<TRequest, TResponse>` 描述符

服务端入口是 `ProtoBuf.Grpc.Configuration.ServerBinder.Bind<TService>`。它用
`ServiceBinder` 决定命名（服务名默认取「命名空间 + 去掉前导 `I` 的接口名」，方法名默认
去掉 `Async` 后缀），再把每个方法归一为 `(MethodType, From, To, ContextKind)`，最后构造
gRPC 描述符：

```csharp
var grpcMethod = new Method<TRequest, TResponse>(methodType, serviceName, operationName,
    marshallerCache.GetMarshaller<TRequest>(), marshallerCache.GetMarshaller<TResponse>());
```

见 [ServerBinder.cs](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Configuration/ServerBinder.cs)。

`Method<TRequest, TResponse>` 携带 `MethodType`、`ServiceName`、`Name` 与两个
`Marshaller`；它的 `FullName` 就是线上 path `/{ServiceName}/{Name}`。
`Marshaller` 由 `ProtoBufMarshallerFactory` 生成，内部是
`Grpc.Core.Marshaller<T>`（序列化 / 反序列化回调）。

这一步是关键：**code-first 没有发明新的线上协议**，只是把接口方法「翻译」成与
`.proto` 生成的代码相同的 `Method` 描述符。

### 2.3 客户端：接口代理 → `CallInvoker`

```csharp
using var channel = GrpcChannel.ForAddress("https://localhost:7184");
var client = channel.CreateGrpcService<IGreeterService>();
var reply = await client.SayHelloAsync(new HelloRequest { Name = "GreeterClient" });
```

`CreateGrpcService<T>()` 通过 `ProxyEmitter`（`Reflection.Emit`）在运行时生成一个接口
实现：每个方法持有静态 `Method<,>` 字段，方法体只把 `CallInvoker` + `Method` + 请求交给
`Reshape.UnaryTaskAsync` / `BlockingUnaryCall` / 流式变体
（[ProxyEmitter.cs](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Internal/ProxyEmitter.cs)、
[GrpcClient.cs](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Client/GrpcClient.cs)）。
`CallOptions`（deadline、headers、`CancellationToken`）由 protobuf-net 的 `CallContext`
包裹。

### 2.4 服务端：provider → Endpoint → handler → 激活

`AddCodeFirstGrpc()` 不是另一套服务器，它等于 `AddGrpc()` 再注册一个
`IServiceMethodProvider<TService>` 实现：

```csharp
public static IGrpcServerBuilder AddCodeFirstGrpc(this IServiceCollection services, Action<GrpcServiceOptions>? configureOptions)
{
    var builder = configureOptions == null ? services.AddGrpc() : services.AddGrpc(configureOptions);
    services.TryAddEnumerable(ServiceDescriptor.Singleton(typeof(IServiceMethodProvider<>), typeof(CodeFirstServiceMethodProvider<>)));
    // ...
}
```

见 [ServicesExtensions.cs](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc.AspNetCore/ServicesExtensions.cs)。

`MapGrpcService<TCalculator>()` 从 DI 取 `ServiceRouteBuilder<TService>` 并 `Build(...)`：

1. 遍历 `IServiceMethodProvider<TService>.OnServiceMethodDiscovery`；
2. protobuf-net 的 provider 在其中跑 `ServerBinder.Bind`，`TryBind` 重载把每个方法注册成
   `context.AddUnaryMethod(method, metadata, delegate)`（流式有另外三个变体）；
3. `ServiceMethodProviderContext.AddUnaryMethod` 用 `ServerCallHandlerFactory` 创建
   `UnaryServerCallHandler`，并用 `RoutePatternFactory.Parse(method.FullName)` 生成路由模板；
4. `ServiceRouteBuilder.Build` 对每个方法执行
   `endpointRouteBuilder.Map(method.Pattern, method.RequestDelegate)`，生成**独立 Endpoint**，
   挂 `GrpcMethodMetadata(typeof(TService), method.Method)` 与契约/实现上的特性元数据。

见 [ServiceRouteBuilder.cs](https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.AspNetCore.Server/Model/Internal/ServiceRouteBuilder.cs)。

请求到达 `ServerCallHandlerBase` 后：校验 content-type 与 HTTP/2|HTTP/3 → 建
`HttpContextServerCallContext` → 用 `Method.RequestMarshaller.ContextualDeserializer` 从
`Request.BodyReader` 读**一条**消息 → 调 invoker → 用
`ResponseMarshaller.ContextualSerializer` 写回。服务实例由
`IGrpcServiceActivator<TService>.Create(httpContext.RequestServices)` 激活；默认实现从 DI
取，取不到再按 transient 创建，`Release` 只释放自己创建的实例
（[IGrpcServiceActivator.cs](https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.AspNetCore.Server/IGrpcServiceActivator.cs)）。

### 2.5 中间件 / Endpoint / interceptor 的分工

- **每方法一个 Endpoint** 意味着 ASP.NET Core 的路由、授权、限流、CORS 都按方法粒度
  生效；
- 中间件在 handler 之前执行，可以从 `Endpoint.Metadata` 读到
  `GrpcMethodMetadata`（服务类型 + `IMethod`）；
- protobuf-net 用 `ServiceBinder.GetMetadata` 把契约接口 / 实现类上的特性（例如
  `[Authorize]`）一并塞进 endpoint metadata，所以授权可以在反序列化**之前**拒绝；
- **Interceptor** 在 gRPC 层，拿到的是反序列化后的消息与 `ServerCallContext`，能捕获
  服务异常；它和 ASP.NET Core 中间件不是同一个层次
  （[MS Learn: gRPC interceptors](https://learn.microsoft.com/en-us/aspnet/core/grpc/interceptors)）。

### 2.6 多参数与限制

gRPC 线上只有一条请求 + 一条响应，`Method<TRequest, TResponse>` 也只有两个类型参数。
protobuf-net 的规则（实现见 `Internal/ContractOperation.cs` 与 `ServerInvokerLookup.cs`）：

- 业务参数 **0 或 1 个**；`CallContext` / `CancellationToken` / `CallOptions` /
  `ServerCallContext` 这类上下文参数必须是最后一个；
- 多个业务值必须自己包成一个 request message；同名重载会撞在同一条
  `Service/Name` 路由上，需要显式改名；
- 客户端只接受 interface；服务端是 class 实现 interface；
- 服务默认 transient（DI 未注册时按调用创建）；要 singleton / scoped 必须把实现类注册进 DI；
- 运行时代理依赖 `Reflection.Emit`，AOT / 裁剪不友好。

### 2.7 哪些可以迁移到 Rust，哪些不能

**可迁移：**

1. 三段式：trait → 每方法静态描述符（service、method、path、codec、call kind）→
   per-method route；
2. 描述符与传输解耦：客户端 generated impl 只做「方法 → 一次 unary 调用」；
   服务端 generated binder 只做「方法 → 一个 Endpoint」；
3. per-method Endpoint + 方法元数据，让中间件 / 授权按方法粒度生效；
4. 服务激活抽象：Rust 里就是「`Arc<Impl>` 由装配者提供，handler 只持 `Arc`」，
   不需要 DI 容器；
5. 显式 `CallCtx`（取消 / 元数据 / 状态）比 C# 的「最后一个可选参数」更清晰。

**不能直接迁移：**

1. 运行时反射 / `Reflection.Emit`：Rust 用 proc-macro 在编译期生成；
2. 多参数打包 hack：Rust 端每个方法生成显式请求结构体（见 §3.4），而不是 tuple 编码；
3. DI 容器解析服务：mptp 的装配是显式 `Arc<Impl>`；
4. 把 code-first 当跨语言契约：Rust 侧仍应保留「字段顺序 / 名称稳定」的规则，
   需要跨语言时导出等价的 schema（本文不设计）。

---

## 3. mptp_rpc 的落点与设计选择

> 本章是 2026-11-14 讨论后的**现行设计**。它取代 2026-11-13 的「每个 Call 方法一个
> endpoint / interface marker / namespace」写法；§3.2 说明为什么 Call 改回 service 级
> 路径 + 请求枚举，而非 Call 仍保持每方法 endpoint。完整验收见 §12.10。

### 3.1 目标用户形状

协议 crate（客户端与服务端共享）：

```rust
use rpc_core_v1::preludes::*;
use rpc_svc_macro::rpc;

#[rpc(service = "api/v1/workspace", api = WorkspaceSvc)]
pub trait WorkspaceService {
    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>>;

    fn list_workspaces(
        &self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Workspace>>>;
}

#[rpc(service = "api/v1/session", api = SessionSvc)]
pub trait SessionService {
    fn whoami(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Option<User>>>;
}
```

服务端 crate：

```rust
#[rpc(impl_of(WorkspaceSvc))]
impl WorkspaceService for WorkspaceServiceImpl {
    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>> {
        ReadyAsync::new(self.store.create_workspace(name))
    }
}
```

客户端 crate：

```rust
let ws = client.call::<WorkspaceSvc>();
let workspace = ws
    .create_workspace("alpha".to_string())
    .may_cancel_with(token)
    .await?;
```

要点：

- `service` 是**完整服务前缀**（`api/v1/workspace`），没有 namespace / interface
  marker；版本段就是前缀的一部分；
- `api = WorkspaceSvc` 由 trait 宏生成零大小 marker 与 `TrRpcInterface::PREFIX`；
  客户端 `client.call::<WorkspaceSvc>()` 的代理只实现 `WorkspaceService`；
- Call 的路由是 `CALL api/v1/workspace`；方法在 `WorkspaceServiceCall` 请求枚举的
  变体里，不再各占一个 pattern；
- 需要 VIEW / PULL / PUSH / DROP 的端点不走这个 trait，仍由 controller 的每方法
  path 表达（§3.2）。

### 3.2 粒度：Call = service endpoint + 方法表；非 Call = 每方法 endpoint

2026-11-13 的决策 1 选择了「方法 = endpoint」，理由是 per-operation 路由、中间件、
`RpcEndpointInfo` 与 405。2026-11-14 的讨论修正了 **Call 部分**：

- Call 是 RPC 语义：`service` + `method` + payload，方法名放在请求体里更自然，也避免
  方法名与资源路径的 `{param}` 混在同一层；一个 service 只占一条路径；
- 方法级校验并没有丢失：请求枚举的**变体名是 body 的第一个值**，dispatcher 先读变体名，
  选中方法的 `RpcMethodInfo`，用 `validate_request_types` 做方法级校验，通过后才解码
  变体载荷、调用 impl。校验仍在业务 handler 之前；
- `RpcEndpointInfo` 从「一个 endpoint 一个 operation」重构为「service endpoint + 方法
  表」；operation 是表项，不是 path 段；
- 非 Call（View / Head / Drop / Pull / Push / Post）保持每方法 endpoint：它们本身就是
  资源 / 上下文 / 流式语义，per-method path、per-endpoint `RpcDataTypeInfo` 与 405
  仍然合适。

| 维度 | Call（现行） | 非 Call（现行） |
| --- | --- | --- |
| 路由 | `CALL /SERVICE_PREFIX` | `VIEW/PULL/PUSH/DROP/... /SERVICE_PREFIX/...` |
| operation 位置 | 请求枚举变体（body） | pattern 段 |
| 方法级校验 | dispatcher 读变体名后、payload 解码前 | router / middleware 在 handler 前 |
| 端点元数据 | `RpcEndpointInfo { service, pattern, methods }` | per-endpoint `RpcDataTypeInfo` |
| 方法级中间件 | 变体名读取后、payload 解码前 | endpoint 级中间件 |
| 405 / `Allow` | service 级 `CALL` | per-method |
| 类型 ID | 每方法 payload 类型 | 每方法 request / response 类型 |
| path 占用 | 一个 service 一条 | 每方法一条 |

**不再生成 Call 的 `/{prefix}/{operation}` 别名**；Call 与旧 body enum 是同一个方向，
但这次有生成的方法表 + 两阶段校验，不是旧 dispatcher 的“先解整个 body 再分派”。

### 3.3 路径与术语

Call 与非 Call 的术语分开：

| 术语 | 谁定义 | 含义 |
| --- | --- | --- |
| `SERVICE_PREFIX` | `#[rpc(service = "api/v1/workspace")]` | 完整服务前缀；Call 的 pattern 就是它 |
| `OPERATION` | trait 方法名（`unraw` 后） | Call：枚举变体 / 方法表键；非 Call：pattern 段 |
| `PATTERN` | Call：trait 宏 / `impl_of`；非 Call：controller | 相对 router 根的路由规则 |
| `MOUNT` | 装配期 router | 父 router 的前缀；协议宏不感知 |

生效线上路径 = `MOUNT + "/" + PATTERN`。例如：

- `CALL api/v1/workspace`（挂根）；
- 挂在 `tenant/a` 下：`tenant/a/api/v1/workspace`；
- 非 Call：`VIEW api/v1/room/list`。

`constcat` 不再需要参与 Call：`service` 是字面量，`PATTERN` 直接就是该
`&'static str`；`impl_of` 不需要拼 `NAMESPACE + SERVICE_NAME + OPERATION`。
非 Call 的 controller 路径仍是它自己的字面量。因此原计划为 Call 加到
`rpc_core_v1` 的 `constcat` 依赖可以取消（除非后续非 Call 组合需要）。

#### 3.3.1 挂载点

- `SERVICE_PREFIX` 是契约 key，不是绝对 URL；`MOUNT` 由 router 决定；
- 协议宏不知道最终挂载点；客户端必须使用与装配一致的 base prefix（如果有）；
- 子 router 的 pipeline / root 当前不会执行（`mptp_serving/src/router_.rs`、
  `serve_handler_.rs` 的既有语义），中间件应加在顶层 router；这不是本次引入。
- 若未来要求「挂载点完全取代前缀参与线上路由」，需要把端点安装成不含前缀的相对
  pattern，并补齐子 router 语义；建议单独立项。

### 3.4 请求载荷：0/1/多参 → 枚举变体

Call 的方法载荷仍按参数个数选择，但外面套一层请求枚举：

| 参数个数 | 方法 payload | 枚举变体 |
| --- | --- | --- |
| 0 | `()` | unit 变体 `ListWorkspaces` |
| 1 | 该参数的 owned 类型 | newtype 变体 `JoinRoom(u64)` |
| ≥2 | `__Req_<operation> { f0, f1, … }` | newtype 变体 `CreateWorkspace(__Req_create_workspace)` |

```rust
#[doc(hidden)]
#[derive(TrEncodable, TrDecodable)]
pub enum WorkspaceServiceCall {
    #[denco(rename = "list_workspaces")]
    ListWorkspaces,
    #[denco(rename = "create_workspace")]
    CreateWorkspace(__Req_create_workspace),
    #[denco(rename = "get_workspace")]
    GetWorkspace(u64),
}
```

要点：

- 变体线上名 = trait 方法名（`denco(rename)`）；重命名方法 = 线上破坏；
- 多参字段按位置命名 `f0`、`f1`…；trait 宏与 impl 宏不需要共享参数名；
- 借用参数转 owned：`&str` → `String`、`&[u8]` / `&Vec<u8>` → `Vec<u8>`、
  `&String` → `String`、`&Copy` 标量 → 标量；其它借用拒绝；
- `()` 的 `TrEncodable` / `TrDecodable` 已由 denco 提供（决策 7）；
- **枚举只是分派信封**：不 derive `TrHasTypeId`，不作为 `Data_Type_Id`；每方法
  payload 的类型才是线上类型；
- 0 参变体没有 payload：dispatcher 读到变体名即完成；两阶段读取必须固定
  「变体名 + payload」顺序，不能依赖完整枚举解码。

### 3.5 服务端代码生成的职责划分

关键约束不变：**协议 crate 不应被迫依赖 `mptp_serving`**。

| 展开点 | 所在 crate | 生成内容 | 依赖 |
| --- | --- | --- | --- |
| `#[rpc(service = "...", api = X)]` trait | 协议 crate | marker `X`、`TrRpcInterface::PREFIX`、`XxxServiceCall`、每方法 payload 关联类型、`impl Trait for ClientProxy` | `rpc_core_v1` |
| `#[rpc(impl_of(X))]` impl | 服务端 crate | service dispatcher、方法表、一个 `Endpoint`、`install_into`、`RpcEndpointInfo` | `mptp_serving` + `rpc_core_v1` |
| controller（非 Call） | 服务端 crate | 每方法 `Endpoint` + handler + `RpcDataTypeInfo` | `mptp_serving` |

- 不再有 interface marker / `services` 列表 / `SERVICE_NAME`；`PREFIX` 就是完整前缀；
- 请求 payload 的关联类型桥仍然需要 `associated_type_defaults`：

```rust
pub trait WorkspaceService {
    #[doc(hidden)]
    type __Req_list_workspaces = ();
    #[doc(hidden)]
    type __Req_create_workspace = __rpc_service_WorkspaceService::__Req_create_workspace;
    #[doc(hidden)]
    type __Req_get_workspace = u64;
    // ... 用户方法
}
```

- `impl_of` 在另一个 crate 里用
  `<Impl as WorkspaceService>::__Req_create_workspace` 解码；
- 这些关联类型是 `#[doc(hidden)]`，并在 trait 文档里注明「不要覆盖」；
- `cfg` 从 trait 方法继承到 payload / 枚举变体 / 客户端方法 / 服务端方法表项，两端
  必须来自同一个协议 crate 的条件编译。

### 3.6 客户端代码生成

trait 宏为 `ClientProxy` 生成实现：

```rust
impl<'__a, __C> WorkspaceService
    for ::rpc_core_v1::client::ClientProxy<'__a, __C, WorkspaceSvc>
where
    __C: ::rpc_core_v1::client::TrRpcClientConfig + Send + Sync + 'static,
{
    fn create_workspace(
        &self,
        name: String,
    ) -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
        '_,
        MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<Workspace>,
    > {
        ::rpc_core_v1::client::call_envelope_async::<__C, WorkspaceSvc, _, Workspace>(
            self,
            "create_workspace",
            WorkspaceServiceCall::CreateWorkspace(__Req_create_workspace { f0: name }),
        )
    }
}
```

- 路径 = `<WorkspaceSvc as TrRpcInterface>::PREFIX`（`api/v1/workspace`），不再有
  per-method 相对路由键；
- `call_envelope_async` 写 `Body_Type`、`Data_Type_Id = 方法 payload ID`、
  `Accept = 方法响应 ID`，body 是枚举变体；响应按 `Rep` 解码并校验类型头；
- 枚举变体在宏展开期构造，不涉及运行期映射表；`format!` 只用于路径拼接；
- `Send + Sync` supertrait 的额外约束照旧保留；
- 返回类型全限定为 `TrMayCancel` / `RpcResult`；用户额外 bound（如 `+ Send`）不满足
  时编译期报错，README 明确不支持。

### 3.7 错误、校验与取消

服务端 dispatcher 的流程：

```text
1. 读 body 第一个值：variant name
2. match → RpcMethodInfo { operation, data: RpcDataTypeInfo }
3. validate_request_types(CALL, headers, &method.data)
      Body_Type / Data_Type_Id / Accept / policy
      缺失或错误 → 400 / 406，在 payload 解码前
4. 解码该变体的 payload（() / T / __Req_*）
5. 调用 impl；按 method.data.response_body 写响应头
```

- 未知变体 → 404（或团队约定状态），在 payload 解码前；连接仍可用；
- `Data_Type_Id` 与变体不匹配 → 406；`Accept` 存在但不匹配 → 406；
- payload 解码失败 → 400（dispatcher 里 catch，而不是落到兜底 500）；
- 业务错误仍走 `RpcError::into_reply_parts()`；`RpcError::Service { message }` 原样
  写出；
- 取消是协作式的：dispatcher 只取消**方法返回的 future**；业务实现内部是否观察令牌
  由实现决定；
- `decode_body_async` 当前内部用 `NonCancellableToken`；若要求请求级取消中断解码，
  需要可取消变体，属后续增强；
- `RpcResult<()>` 走普通 `write_ok_async(&())`，不需要特例。

### 3.8 元数据、中间件与共存

`RpcEndpointInfo` 重构为 service 级 + 方法表：

```rust
#[derive(Clone, Copy, Debug)]
pub struct RpcEndpointInfo {
    /// 完整服务前缀，如 `api/v1/workspace`。
    pub service: &'static str,
    /// 相对 router 根；Call 的 pattern 与 service 相同。
    pub pattern: &'static str,
    /// 方法表：operation + 方法级 RpcDataTypeInfo。
    pub methods: &'static [RpcMethodInfo],
}

#[derive(Clone, Copy, Debug)]
pub struct RpcMethodInfo {
    pub operation: &'static str,
    pub data: RpcDataTypeInfo,
}
```

- `impl_of` 生成的 Call endpoint 挂这个标记；非 Call 端点继续用 per-endpoint
  `RpcDataTypeInfo`（或 `RpcEndpointInfo.methods` 只有一项）；
- **service 级中间件**在 router 层，读 `RpcEndpointInfo.methods` 做整组策略；
- **方法级中间件**在 dispatcher 读到变体名后、payload 解码前执行；框架提供
  `RpcMethodInfo` / `DispatchContext`；这与 HTTP/JSON-RPC 的“解析 operation 后做
  方法级策略”一致；
- 405 / `Allow` 是 service 级 `CALL`；Call trait 只有 CALL，per-method 方法集不存在；
  需要 VIEW/POST 的方法走 controller 的 per-method endpoint；
- 冲突规则：同一 `pattern + MethodSet` 唯一；`CALL api/v1/workspace` 与
  `VIEW api/v1/workspace/list` 是不同 pattern + 方法，可共存；若 controller 也绑定
  `CALL api/v1/workspace` 才是冲突；
- `bind_endpoint` 同 pattern 同方法仍是先到先得（G-10）；README 要求命名约定 +
  装配期 review，不做运行期穷举。

### 3.9 装配形状

**最小形状：**

```rust
let router = RadixRouter::<SrvCfg, ConnState, ()>::new_with_state_and_request(
    std::sync::Arc::new(AppRoot),
    state,
);
let router = router.use_middleware(PrincipalMiddleware);
let router = __rpc_impl_of_WorkspaceServiceImpl_via_WorkspaceSvc::install_into(
    router,
    std::sync::Arc::new(WorkspaceServiceImpl::new_()),
)?;
let router = __rpc_impl_of_SessionServiceImpl_via_SessionSvc::install_into(
    router,
    std::sync::Arc::new(SessionServiceImpl::new_()),
)?;
let servlet = mptp_smux::serving::MptpServlet::<SrvCfg, _>::new(router);
```

每个 marker 一个 installer；一个 installer 绑定**一个** service 路径 + 方法表。
同一 impl 挂多个前缀时，每个前缀一个 marker / installer：

```rust
let router = __rpc_impl_of_DiagnosticsServiceImpl_via_DiagV1::install_into(router, diag.clone())?;
let router = __rpc_impl_of_DiagnosticsServiceImpl_via_DiagV2::install_into(router, diag)?;
```

**人体工学 API：独立的 `rpc_serv_ext` crate（决策 3 不变）。**

没有 namespace / interface marker 后，builder 按**前缀字符串**分组，而不是按
interface 类型：

```rust
let router = rpc_serv_ext::prefix("api/v1", router)
    .service(|r| __rpc_impl_of_WorkspaceServiceImpl_via_WorkspaceSvc::install_into(r, ws))?
    .service(|r| __rpc_impl_of_SessionServiceImpl_via_SessionSvc::install_into(r, ss))?
    .finish();
```

- builder 只做分组与 `prefix` 校验，不重写 pattern；
- 依赖矩阵不变：协议 crate 只依赖 `rpc_core_v1`；业务 impl crate 依赖
  `rpc_core_v1` + `mptp_serving`；`rpc_serv_ext` 只在装配 crate 按需使用；
- `prefix("api/v1")` 不参与 service 路径拼接（service 已经是完整前缀），只用于
  装配期分组 / 检查；如果不需要分组，直接调 `install_into` 即可。

### 3.10 一个 trait 的方法如何映射到 service endpoint + 方法表

#### 3.10.1 身份

Call 的访问点由两个键确定：

| 键 | 来源 | 例子 |
| --- | --- | --- |
| `SERVICE_PREFIX` | trait 的 `service = "..."` / marker `PREFIX` | `api/v1/workspace` |
| `OPERATION` | trait 方法名（unraw） | `create_workspace` |

- `SERVICE_PREFIX` 是 service 的身份；`OPERATION` 是方法表项的身份；
- wire 路由 = `CALL SERVICE_PREFIX`；operation 在请求枚举 / 方法表里，不在 path；
- 非 Call 则是 `SERVICE_PREFIX/...` 的 per-method pattern，operation 在 path。

#### 3.10.2 编译期：一次展开

`#[rpc(impl_of(WorkspaceSvc))] impl WorkspaceService for WorkspaceServiceImpl`：

1. 枚举 impl 块的 `&self` 方法；
2. 生成**一个** service handler + 一个 `Endpoint`（pattern = `SERVICE_PREFIX`）+
   方法表；
3. 每个方法生成一个 `RpcMethodInfo`（operation + `RpcDataTypeInfo`）；
4. 生成一个 `match` 变体 → 调用 impl 的分派；
5. 所有方法共享同一个 `Arc<WorkspaceServiceImpl>`；
6. 生成一个 `install_into`。

trait 宏侧同时生成 `WorkspaceServiceCall` 枚举与客户端代理，因此
「trait 方法 ↔ 枚举变体 ↔ 方法表项 ↔ 客户端方法」是同一份宏输入的一一对应：

| trait 项 | 是否方法表项 | payload | 变体 |
| --- | --- | --- | --- |
| `&self` 方法（0 参） | 是 | `()` | unit |
| `&self` 方法（1 参） | 是 | owned 参数类型 | newtype |
| `&self` 方法（≥2 参） | 是 | `__Req_<op>` | newtype |
| 关联 const / type | 否 | — | — |
| 非 `&self` / 默认实现 | 拒绝 | — | — |

默认实现必须在分析层拒绝：`impl_of` 枚举 impl 块；如果 trait 方法有默认实现且 impl
没有重写，impl 块里不会出现该方法，方法表 / 枚举会缺项，而客户端代理仍会生成。
helper 逻辑放固有 impl 或普通 trait。

`cfg` 从 trait 方法继承到 payload / 枚举变体 / 客户端方法 / 方法表项；两端必须一致。

#### 3.10.3 运行期：operation 在方法表 / body 里

```rust
RpcEndpointInfo {
    service: "api/v1/workspace",
    pattern: "api/v1/workspace",
    methods: &[
        RpcMethodInfo { operation: "create_workspace", data: ... },
        RpcMethodInfo { operation: "list_workspaces", data: ... },
    ],
}
```

- service 级策略：读 `RpcEndpointInfo.service` / `methods`，在 router 层执行；
- 方法级策略：dispatcher 读到变体名后，选中 `RpcMethodInfo`，在 payload 解码前执行；
- 405 / `Allow`：service 级 `CALL`；Call trait 的方法都是 CALL；
- router 仍然只看到一条 Call endpoint；它不需要理解 trait，方法表由 dispatcher 持有。

#### 3.10.4 注册单元

`install_into(router, Arc<Impl>)`：

- 一次绑定一个 service endpoint（Call）或一组 per-method endpoint（非 Call）；
- 消费式 `RadixRouter`：绑定失败返回 `Err`，不留下半个 service；
- 同一 `Arc<Impl>` 被所有方法 / handler 共享；
- 同一 impl + trait 挂多个前缀：每个前缀一个 marker / installer，`SERVICE_PREFIX`
  不同，互不覆盖。

#### 3.10.5 客户端：调用点与访问点同源

```text
WorkspaceService::create_workspace
  ──►  WorkspaceServiceCall::CreateWorkspace(__Req_create_workspace)
  ──►  CALL api/v1/workspace
  ──►  Data_Type_Id = __Req_create_workspace, Accept = Workspace
```

调用点是 trait 方法，访问点是 service 路径 + 方法表项；两者由同一份 trait 宏输入
推导，不需要手写映射表。

## 4. 将要实现的形状（样式代码）

> **2026-11-14 注**：本章样式代码仍按 2026-11-13 的 interface marker + 每方法 Call
> path 编写，**不是现行形状**。Call 的现行设计（service 级路径 + `XxxServiceCall`
> 枚举 + 方法表）见 §3 与 §12；下一轮应整体替换本章示例。

> 以下代码是**目标形状**，不是当前可编译代码；`...` 表示省略。

### 4.1 协议 crate

```rust
#![feature(impl_trait_in_assoc_type, associated_type_defaults)]

use rpc_core_v1::preludes::*;
use rpc_svc_macro::rpc;

#[derive(Clone, Debug, TrEncodable, TrDecodable)]
pub struct Workspace {
    pub id: u64,
    pub name: String,
}

#[derive(Clone, Debug, TrEncodable, TrDecodable)]
pub struct Session {
    pub id: u64,
    pub workspace_id: u64,
    pub title: String,
}

#[rpc(service = "workspace")]
pub trait WorkspaceService {
    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>>;

    fn list_workspaces(
        &self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Workspace>>>;

    fn get_workspace(
        &self,
        id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Option<Workspace>>>;
}

#[rpc(service = "session")]
pub trait SessionService {
    fn create_session(
        &self,
        workspace_id: u64,
        title: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Session>>;

    fn list_sessions(
        &self,
        workspace_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Session>>>;
}

#[rpc(namespace = "api/v1", services = [WorkspaceService, SessionService])]
pub struct ApiV1;

#[rpc(namespace = "api/v2", services = [WorkspaceService])]
pub struct ApiV2;
```

### 4.2 客户端

```rust
use rpc_core_v1::preludes::*;

async fn demo(client: &MptpClient<CliCfg>, token: impl TrCancellationToken) -> RpcResult<()> {
    let v1 = client.call::<ApiV1>();

    let workspace = v1
        .create_workspace("alpha".to_string())
        .may_cancel_with(token.child_token())
        .await?;

    let sessions = v1
        .list_sessions(workspace.id)
        .may_cancel_with(token.child_token())
        .await?;

    let _ = sessions;
    Ok(())
}
```

### 4.3 服务端实现与装配

```rust
#[rpc(impl_of(ApiV1))]
#[rpc(impl_of(ApiV2))]
impl WorkspaceService for WorkspaceServiceImpl {
    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>> {
        ReadyAsync::new(self.store.create_workspace_(name))
    }

    fn list_workspaces(
        &self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Workspace>>> {
        ReadyAsync::new(Ok(self.store.list_workspaces_()))
    }

    fn get_workspace(
        &self,
        id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Option<Workspace>>> {
        ReadyAsync::new(Ok(self.store.get_workspace_(id)))
    }
}

#[rpc(impl_of(ApiV1))]
impl SessionService for SessionServiceImpl {
    // ...
}

pub fn build_router(
    state: std::sync::Arc<ConnState>,
) -> Result<mptp_serving::RadixRouter<SrvCfg, ConnState, ()>, mptp_serving::x_deps::matchit_route::InsertError> {
    let router = mptp_serving::RadixRouter::<SrvCfg, ConnState, ()>::new_with_state_and_request(
        std::sync::Arc::new(AppRoot),
        state,
    );
    let router = router
        .use_middleware(PrincipalMiddleware)
        .use_middleware(RpcAuditMiddleware);

    let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1::install_into(
        router,
        std::sync::Arc::new(WorkspaceServiceImpl::new_()),
    )?;
    let router = __rpc_impl_of_SessionServiceImpl_via_ApiV1::install_into(
        router,
        std::sync::Arc::new(SessionServiceImpl::new_()),
    )?;
    let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV2::install_into(
        router,
        std::sync::Arc::new(WorkspaceServiceImpl::new_()),
    )?;

    Ok(router)
}
```

### 4.4 宏展开形状（trait / client）

```rust
pub trait WorkspaceService {
    const SERVICE_NAME: &'static str = "workspace";

    // 0 参 → ()
    #[doc(hidden)]
    type __Req_list_workspaces = ();
    // 1 参 → 直接使用 owned 参数类型
    #[doc(hidden)]
    type __Req_create_workspace = ::alloc::string::String;
    #[doc(hidden)]
    type __Req_get_workspace = u64;
    // ≥2 参 → hidden 位置字段结构体
    #[doc(hidden)]
    type __Req_create_session =
        __rpc_service_WorkspaceService::__Req_create_session;

    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>>;
    // ...
}

#[doc(hidden)]
#[allow(non_camel_case_types, non_snake_case)]
pub mod __rpc_service_WorkspaceService {
    use super::*;

    /// 只有多参数方法需要请求结构体；单参数 / 0 参数不生成。
    #[derive(
        ::rpc_core_v1::x_deps::denco_macro::TrEncodable,
        ::rpc_core_v1::x_deps::denco_macro::TrDecodable,
    )]
    pub struct __Req_create_session {
        pub f0: u64,
        pub f1: ::alloc::string::String,
    }
}

impl<'__a, __C, __I> WorkspaceService
    for ::rpc_core_v1::client::ClientProxy<'__a, __C, __I>
where
    __C: ::rpc_core_v1::client::TrRpcClientConfig + Send + Sync + 'static,
    __I: ::rpc_core_v1::messaging::TrRpcInterface + Send + Sync + 'static,
{
    fn create_workspace(
        &self,
        name: String,
    ) -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
        '_,
        MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<Workspace>,
    > {
        ::rpc_core_v1::client::call_unary_async::<__C, __I, _, Workspace>(
            self,
            "workspace/create_workspace",
            name,
        )
    }

    fn list_workspaces(
        &self,
    ) -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
        '_,
        MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<Vec<Workspace>>,
    > {
        ::rpc_core_v1::client::call_unary_async::<__C, __I, _, Vec<Workspace>>(
            self,
            "workspace/list_workspaces",
            (),
        )
    }

    // ...
}
```

### 4.5 宏展开形状（impl_of 的一个方法 + 安装）

```rust
impl WorkspaceService for WorkspaceServiceImpl {
    // 用户原来的 impl，原样保留
}

#[doc(hidden)]
#[allow(non_camel_case_types, non_snake_case, unused_imports)]
pub mod __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1 {
    use super::*;
    use ::mptp_serving::{
        Endpoint, HandlerFuture, MethodSet, MptpContext, RadixRouter,
        TrEndpointHandler, box_future_,
    };
    use ::mptp_serving::x_deps::mptp_smux::{
        ServeError,
        serving::config::TrConfig,
    };
    use ::rpc_core_v1::serving::{
        decode_body_async, write_ok_async, write_status_async,
    };
    use ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel;

    pub struct __CreateWorkspace {
        pub svc_: ::alloc::sync::Arc<WorkspaceServiceImpl>,
    }

    impl<C, S, R> TrEndpointHandler<C, S, R> for __CreateWorkspace
    where
        C: TrConfig + 'static,
    {
        fn handle<'a, 'f>(
            &'a self,
            ctx: &'a mut MptpContext<'f, C, S, R>,
        ) -> HandlerFuture<'a> {
            let arena = ctx.arena();
            box_future_(arena, async move {
                let req: <WorkspaceServiceImpl as WorkspaceService>::__Req_create_workspace =
                    decode_body_async(ctx.session_mut()).await?;

                let result = <WorkspaceServiceImpl as WorkspaceService>::create_workspace(
                    &*self.svc_,
                    req,
                )
                .may_cancel_with(ctx.cancel().child_token())
                .await;

                match result {
                    Ok(value) => write_ok_async(ctx.session_mut(), &value).await?,
                    Err(error) => {
                        let (status, message) = error.into_reply_parts();
                        write_status_async(ctx.session_mut(), status, message).await?;
                    }
                }

                Result::Ok(())
            })
        }
    }

    pub fn install_into<C, S, R>(
        router: RadixRouter<C, S, R, ::alloc::alloc::Global>,
        svc: ::alloc::sync::Arc<WorkspaceServiceImpl>,
    ) -> Result<
        RadixRouter<C, S, R, ::alloc::alloc::Global>,
        ::mptp_serving::x_deps::matchit_route::InsertError,
    >
    where
        C: TrConfig + 'static,
        S: Send + Sync + 'static,
        R: 'static,
        WorkspaceServiceImpl: Send + Sync + 'static,
    {
        const PATTERN: &'static str = ::rpc_core_v1::x_deps::constcat::concat!(
            <ApiV1 as ::rpc_core_v1::messaging::TrRpcInterface>::NAMESPACE,
            "/",
            <WorkspaceServiceImpl as WorkspaceService>::SERVICE_NAME,
            "/",
            "create_workspace",
        );

        let endpoint = Endpoint::new(
            PATTERN,
            MethodSet::CALL,
            ::alloc::sync::Arc::new(__CreateWorkspace { svc_: svc.clone() }),
        )
        .with_metadata(::rpc_core_v1::serving::RpcEndpointInfo {
            namespace: <ApiV1 as ::rpc_core_v1::messaging::TrRpcInterface>::NAMESPACE,
            service: <WorkspaceServiceImpl as WorkspaceService>::SERVICE_NAME,
            operation: "create_workspace",
            pattern: PATTERN,
        });

        let router = router.bind_endpoint(endpoint)?;
        Result::Ok(router)
    }
}
```

> 注：`__CreateWorkspace::svc_` 若被多个方法共享，可在模块里定义一次
> `pub struct __ServiceHandle { svc_: Arc<WorkspaceServiceImpl> }` 供所有 handler 持有；
> 上面的写法为了逐方法独立可读，重复了一个 `Arc` 字段。

### 4.6 中间件（方法级策略）

```rust
use mptp_serving::{
    Endpoint, MiddlewareFuture, MptpContext, NextFn, TrMiddleware,
};
use mptp_smux::serving::config::TrConfig;
use rpc_core_v1::serving::RpcEndpointInfo;

pub struct RpcAuditMiddleware;

impl<C, S, R> TrMiddleware<C, S, R> for RpcAuditMiddleware
where
    C: TrConfig,
{
    fn call<'a, 'f>(
        &'a self,
        endpoint: &'a Endpoint<C, S, R>,
        ctx: &'a mut MptpContext<'f, C, S, R>,
        next: &'a mut NextFn<'a, 'f, C, S, R>,
    ) -> MiddlewareFuture<'a> {
        let info = endpoint.metadata().get::<RpcEndpointInfo>().copied();
        let arena = ctx.arena();
        mptp_serving::box_future_(arena, async move {
            if let Some(info) = info {
                // 方法级策略：审计 / 指标 / 限流键
                let _ = (info.namespace, info.service, info.operation, info.pattern);
            }
            next(ctx).await
        })
    }
}
```

### 4.7 与 controller / 手写端点共存

```rust
let router = router
    .use_middleware(PrincipalMiddleware)
    .call("scene/ping", PingHandler)?;              // 手写端点
let router = __rpc_controller_SceneService::install_into(router, scene_svc)?; // controller
let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1::install_into(router, ws)?; // trait
```

三者都在同一张 `RadixRouter` 上，中间件、元数据、405/404 收尾完全一致。

---

## 5. 实施计划

> 每阶段先新增、后切换；不删除 controller / 自由函数形态。所有新增公开 API 在开工前
> 按 `AGENTS.md` 第 1 条确认。

### P0：框架侧只做「加法」

1. `rpc_core_v1/Cargo.toml` 增加 `constcat`（已验证 0.3.1 / 0.5 / 0.6.1 可用），
   `x_deps` 重新导出；
2. `TrRpcInterface::PATH` 改名 `NAMESPACE`，同步 `rpc_core_v1/src/client/proxy_.rs`、
   `messaging/interface_.rs` 的调用点 / 文档与 rustdoc 示例（决策 6）；
3. 新增 `rpc_core_v1::serving::RpcEndpointInfo`（决策 4；协议 crate 已依赖
   `rpc_core_v1`，不新增依赖）；
4. 新增独立 crate `rpc_serv_ext`（决策 3）：只含 `InterfaceBuilder` + 闭包式
   `service()` + `finish()`；依赖 `rpc_core_v1` + `mptp_serving`；不依赖
   `rpc_svc_macro` / 协议 crate；
5. 依赖侧：denco 仓库的 `()` `TrEncodable` / `TrDecodable` 已实现（决策 7）；
   本期直接加一个 `RpcResult<()>` 的端到端 / 集成用例验证。

**验收**：`cargo check --workspace` 不回归；constcat 在生成代码路径里可用；
`rpc_serv_ext` 能独立编译；`RpcResult<()>` 的端到端用例通过。

### P1：恢复分析层（`rpc_svc_macro/src/model_.rs` / `analyze_.rs`）

从 `50ecae1^` 恢复并调整：

- `parse_rpc_args_` 恢复 `Service { namespace }` / `Interface { namespace, services }` /
  `ImplOf { interface }` 三个分支；
- `analyze_service_`：校验非泛型 trait、`&self` 方法、**无默认实现**（`TraitItemFn.default`
  必须为 `None`，否则拒绝并提示「helper 放到固有 impl / 普通 trait」）、返回
  `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`、参数 owned / 借用的白名单；
  注入 `SERVICE_NAME`、每方法请求类型（关联类型）；
- `analyze_interface_`：单元结构体、非空 `services`、namespace 校验；
- `analyze_impl_of_`：trait impl、非泛型、无 where 子句、方法列表；
- 复用旧 `analyze_method_` / `analyze_arg_` / `borrowed_to_owned_` / 返回类型解析，
  只把「隐藏 dispatch 方法」改成「请求结构体 + 关联类型」；
- 保留 legacy 的 `__rpc_` 前缀拒绝规则，并把 `__Req_` 前缀也列为宏保留名，避免用户
  方法 / 关联类型与生成项冲突。

**验收**：新增单元测试覆盖合法 / 非法 trait、interface、impl；测试函数带中英双语文档
注释；单测脚本 1 分钟超时。

### P2：恢复 trait / interface 展开（`expand_.rs`）

> **2026-11-14 修订**：`#[rpc(service = "...", api = X)]` 时，`expand_service_` 额外生成
> `pub struct X;`（若 trait 是 `pub`）与 `impl TrRpcInterface for X { const NAMESPACE = ...; }`，
> 并生成 `impl Trait for ClientProxy<'_, C, X>`。interface marker 展开保留为可选路径。

- `expand_service_`：原 trait + `SERVICE_NAME` + 关联类型 + hidden 请求结构体 +
  `impl Trait for ClientProxy`；
- `expand_interface_`：`TrRpcInterface` + `ServiceSelector`（与 legacy 一致）；
- 所有生成代码使用 `::alloc` / `::rpc_core_v1::x_deps` 绝对路径，不假设 `std`。

**验收**：`rpc_svc_macro/tests/service_macro.rs` 编译通过；覆盖 no-arg、`&str`、
`&[u8]`、`&String`、`&Copy`、多参数、嵌套模块 trait、多 interface。

### P3：实现 `impl_of` 的服务端展开

> **2026-11-14 修订**：`impl_of(X)` 的 `X` 可以是 per-service marker（§11）；
> `NAMESPACE` 从 `X` 取，`SERVICE_NAME` 从 trait 取，二者都必须是 `const`。

- 每方法一个 `TrEndpointHandler`（`box_future_` + `decode_body_async` +
  `.may_cancel_with(ctx.cancel().child_token())` + `write_ok_async` /
  `write_status_async`）；
- 每方法一个 `const PATTERN`（constcat）+ `Endpoint::new(..., MethodSet::CALL, ...)` +
  `with_metadata(RpcEndpointInfo { namespace, service, operation, pattern })` +
  `bind_endpoint`；`RpcEndpointInfo` 是本期的必需产物（决策 4）；
- 生成 `install_into`（唯一服务端注册入口）；**不生成** `TrRpcServiceOf` 之类的跨
  crate trait impl（决策 3），人体工学 builder 在 `rpc_serv_ext` 里通过闭包组合。

**验收**：编译期验证生成代码；一个最小 `mptp_serving` 集成测试把生成的端点挂到
`RadixRouter` 并断言 pattern / 方法集合 / 元数据；不建立真实连接。另加一个关系测试：
对同一 trait 的 N 个 `&self` 方法，`install_into` 后应能枚举/命中 N 个 operation，
且每个端点的 `RpcEndpointInfo.operation` 与 trait 方法名一一对应（若本期生成
`RpcServiceDef`，直接断言其 `operations` 表；否则用 test-only 的 operation 常量表）。

### P4：端到端迁移 `mptp_cs_smoke`

- 协议 `src/lib.rs` 补 `associated_type_defaults` feature，并把 interface marker 的
  `path = "..."` 改为 `namespace = "..."`；trait 方法签名与业务类型不变；
- `src/server.rs` 的服务端装配从 `Router` / `RpcRouter` / `MptpServer` 改成
  `RadixRouter` + `install_into` + `MptpServlet`；
- `src/client.rs` 不变（这正是「恢复 code-first」的验收点）；
- 跑通原有 11 条 RPC（compio + UDS）。

**验收**：客户端代码零改动通过 11 条 RPC；`cargo test` / `clippy` / rustdoc 通过。

### P5：文档与清理

- `rpc_svc_macro/README.md` 增加「trait 形态」章节，明确只支持 `CALL`、pattern 规则、
  请求字段顺序稳定性、`associated_type_defaults` 要求；
- `rpc_core_v1/README.md` / `preludes` 视新增 API 更新；
- `DESIGN_BLUEPRINT.md` 的「实现现状」改为新形状；
- `dev-notes/README.md` 增加本文索引；
- `mptp_cs_smoke/README.md` 说明迁移后的装配。

---

## 6. 公开 API 变更与风险

### 6.1 变更清单（已决策）

| 变更 | 类型 | 说明 |
| --- | --- | --- |
| `#[rpc(service = "im/v1/room", api = X)]` / `#[rpc(impl_of(X))]` 恢复；`#[rpc(namespace = ..., services = ...)]` **不再需要** | 新增 | 决策 8：per-service marker + 完整前缀；不改动现有 controller / 自由函数形态 |
| 协议 crate 需要 `#![feature(associated_type_defaults)]` | 新增约束 | 决策 2 接受；用于隐藏请求类型的跨 crate 投影 |
| `TrRpcInterface::PATH` 改名 `PREFIX` | **破坏（改名）** | 决策 8；常量是完整服务前缀（`im/v1/room`），`concat_location(I::PREFIX, operation)` |
| `rpc_core_v1` 增加 `constcat` 依赖并 `x_deps` 重导出 | 依赖变更 | 不改变 `mptp_serving` 公开 API |
| `rpc_core_v1::serving::RpcEndpointInfo { service, pattern, methods }` + `RpcMethodInfo { operation, data }` | 新增 / 去 `namespace` / 去单 `operation` | 决策 4 + 8 + 9；service 级元数据 + 方法表 |
| 生成的 `XxxServiceCall` 枚举 + service dispatcher | 新增（宏生成） | 决策 9；Call 一条路径，方法在 body 枚举；方法级校验在 payload 解码前 |
| 新 crate `rpc_serv_ext` | 新增 crate | 决策 3；只含闭包式 `InterfaceBuilder`，依赖 `rpc_core_v1` + `mptp_serving` |
| denco 的 `()` `TrEncodable` / `TrDecodable` | 已完成 | 决策 7；mptp_rpc 只需消费 |
| 线上格式：pattern 增加 `/{operation}`，body 变为每方法结构体 | **破坏** | 决策 1：不兼容旧格式、不生成别名端点 |
| `mptp_cs_smoke` 协议 / 服务端装配迁移 | 仓库外变更 | trait 属性改为 per-service marker（`service` 完整前缀）、服务端 `impl_of(X)` + `RadixRouter`；客户端调用逻辑不变 |

### 6.2 风险

1. **nightly feature 约束（已接受）**：`associated_type_defaults` 要求每个协议 crate
   打开 feature。与既有的 `impl_trait_in_assoc_type` 一样写进 README；不再提供
   PhantomData 备选。
2. **`constcat` 的具体类型限制**：生成代码里 `const` 只能引用具体类型（`ApiV1`、
   `Impl`），不能引用泛型 `I` / `T`。`impl_of` 的展开点满足这一条件；如果未来要
   写「泛型 `add_service<I, T>` 内部拼 pattern」，必须改用 owned pattern 方案。
3. **`Send + Sync` 约束**：service impl 必须 `Send + Sync + 'static`；compio 单线程的
   实现如果内部持有 `Rc` / `RefCell`，会编译失败。这是新框架的既有边界，需要在
   README 中说明（`mptp_cs_smoke` 的实现用 `Arc<Mutex<_>>`，满足）。
4. **operation 即 trait 方法名**：重命名 trait 方法会改变线上路由；需要文档约定或显式
   `#[rpc(name = "...")]`（本次不引入，避免形状膨胀）。
5. **同 pattern 冲突无检测**：同一 namespace 下重复的 `SERVICE_NAME + operation` 会静默
   先到先得；需要文档约定，后续可加运行期检查。
6. **协议 crate 的 `::alloc` 假设**：生成代码用 `::alloc::...` 而不是 `::std::...`；
   std crate 实测可用，no_std crate 需要 `extern crate alloc`。要在 README 写明。
7. **`NAMESPACE` 与 `MOUNT` 的文档风险**：两者都不是 `path`，但装配者容易把
   `namespace` 当绝对路径、或忘记挂载点会前置。§3.3.1 的公式
   `MOUNT + PATTERN` 必须写进 `rpc_svc_macro` README 和 `rpc_serv_ext` 的 builder 文档；
   `RpcEndpointInfo.pattern` 明确标注「相对 router 根」。
8. **`rpc_serv_ext` 无法校验 namespace 一致性**：闭包式 builder 只负责把 `install_into`
   串起来，不读 `I::NAMESPACE`，因此「客户端 namespace 与服务端实际 pattern」是否一致
   仍靠装配约定。若需要编译期校验，只能回到 trait-based 方案（与决策 3 冲突）。

---

## 7. 决策闭环与剩余实现级问题

### 7.1 已闭环（2026-11-13 评审）

1. **线上兼容**：不兼容旧格式，不生成别名端点（决策 1）。
2. **nightly feature**：接受 `associated_type_defaults`，不做 PhantomData 分支（决策 2）。
3. **人体工学 API**：放独立 crate `rpc_serv_ext`，采用闭包式 builder；协议 crate 与
   业务 impl crate 不必依赖它，只有装配 crate 按需依赖（决策 3）。
4. **`RpcEndpointInfo`**：本期就加（决策 4）。
5. **宏命名**：`#[rpc(impl_of(...))]`（决策 5）。
6. **术语**：协议层不用 `path`；使用 `namespace` / `service` / `operation` / `pattern`，
   并单独定义 `MOUNT`（决策 6，§3.3.1）。
7. **`RpcResult<()>`**：denco 已实现 `()` 的 `TrEncodable` / `TrDecodable`（决策 7）。

### 7.2 剩余实现级问题（不阻塞设计）

- `rpc_serv_ext` 的最终 crate 名与发布位置（工作区成员 / 独立仓库）。
- 是否让 `impl_of` 额外生成一个 `Impl::install_into_via_<I>` 固有方法，减少装配处写
  hidden 模块路径；它不增加依赖，纯属宏糖。
- 是否引入 `#[rpc(name = "...")]` 让 operation 名与 Rust 方法名解耦（本次不做）。

---

## 8. 验证记录（本次调研实际跑过的探针）

以下探针都在 `mptp_rpc/.tmp/` 下（gitignored），不修改任何 tracked 文件：

1. **constcat + 关联常量**：具体类型 / 外层泛型函数内的具体 const 均通过；
   `no_std` 通过；`constcat 0.3.1 / 0.5 / 0.6.1` 均可用；泛型参数出现在 const
   表达式里报 `E0401`（因此只用于具体 `I` / `Impl`）。
2. **`rpc_core_v1::x_deps::constcat::concat!` 跨 crate 重导出**：通过。
3. **跨 crate 关联类型默认值投影**：trait 在 `proto` crate 定义
   `type __ReqCreate = hidden::CreateWorkspace`，下游 `server` crate 用
   `<Impl as Svc>::__ReqCreate` 并访问 `req.f0`，编译通过。
4. **泛型 `H<Svc>` + `Arc<dyn TrEndpointHandler>`**：用真实 `mptp_serving` 编译通过；
   `H<Svc>: Send + Sync` 要求 `Svc: Send + Sync + 'static`。
5. **handler 内直接调用 trait 方法 + `.may_cancel_with(...).await`**：owned body →
   `&owned` 传参的 borrowed / owned 两种形状都编译通过，不需要 `gen_mcf2` dispatch。
6. **子 router 语义**：`bind_router` 的子 router `pipeline` / `root` / `state` /
   `cancel_source` 不会执行；父层在第一个前缀命中后直接返回
   （`router_.rs:506-511`、`serve_handler_.rs:99`/`132`）。因此否决嵌套方案。
7. **`Endpoint` 拥有 pattern 的借用坑**：`pattern() -> &str` 后先 `set.push(endpoint)`
   再 `trie.insert(normalized, ...)` 会 `E0505`；需要先复制 key。本文的 constcat 方案
   不触发这个问题。
8. **denco `()`**：用户已确认 denco 实现对 `()` 的 `TrEncodable` / `TrDecodable`；本期
   只需在 `impl_of` 的集成用例里覆盖 0 参请求与 `RpcResult<()>` 返回。

---

## 9. 参考链接

**ASP.NET Core / grpc-dotnet**

- [MS Learn: Code-first gRPC services and clients with .NET](https://learn.microsoft.com/en-us/aspnet/core/grpc/code-first)
- [MS Learn: gRPC interceptors](https://learn.microsoft.com/en-us/aspnet/core/grpc/interceptors)
- [protobuf-net.Grpc: Getting Started](https://grpc.protobuf-net.dev/gettingstarted)
- [protobuf-net.Grpc: `ServicesExtensions.cs`（`AddCodeFirstGrpc`）](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc.AspNetCore/ServicesExtensions.cs)
- [protobuf-net.Grpc: `ServerBinder.cs`（`Method<TRequest, TResponse>` 构造）](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Configuration/ServerBinder.cs)
- [protobuf-net.Grpc: `GrpcClient.cs`（`CallInvoker` 入口）](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Client/GrpcClient.cs)
- [protobuf-net.Grpc: `ProxyEmitter.cs`（客户端代理生成）](https://github.com/protobuf-net/protobuf-net.Grpc/blob/main/src/protobuf-net.Grpc/Internal/ProxyEmitter.cs)
- [grpc-dotnet: `ServiceRouteBuilder.cs`（方法 → Endpoint）](https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.AspNetCore.Server/Model/Internal/ServiceRouteBuilder.cs)
- [grpc-dotnet: `GrpcEndpointRouteBuilderExtensions.cs`（`MapGrpcService`）](https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.AspNetCore.Server/GrpcEndpointRouteBuilderExtensions.cs)
- [grpc-dotnet: `IGrpcServiceActivator.cs`](https://github.com/grpc/grpc-dotnet/blob/master/src/Grpc.AspNetCore.Server/IGrpcServiceActivator.cs)

**mptp_rpc 内部**

- `mptp_serving/src/endpoint_.rs`、`router_.rs`、`handler_.rs`、`middleware_.rs`、
  `serve_handler_.rs`、`context_.rs`
- `rpc_core_v1/src/client/proxy_.rs`、`messaging/interface_.rs`、`serving/body_.rs`、
  `messaging/error_.rs`
- `rpc_svc_macro/src/lib.rs`、`analyze_.rs`、`expand_.rs`
- `mptp_cs_smoke/src/lib.rs`、`src/server.rs`、`src/client.rs`
- `DESIGN_BLUEPRINT.md`、`dev-notes/rpc-svc-macro-mptp-serving-20261112-2130.md`

---

## 10. 英文摘要

Code-first gRPC in ASP.NET Core (protobuf-net.Grpc) does not invent a second protocol: it
translates interface methods into standard `Method<TRequest, TResponse>` descriptors
(service name, method name, Marshallers), turns each descriptor into its own ASP.NET Core
Endpoint (`/Service/Method`) via `IServiceMethodProvider` / `ServiceRouteBuilder`, and lets
the existing `ServerCallHandler` + `IGrpcServiceActivator` pipeline deserialize, activate
and invoke the service. That is why code-first keeps routing, authorization, endpoint
metadata and middleware per method.

For mptp_rpc, the right restoration is therefore not the old "one service route + body
enum" dispatcher, but a macro expansion that compiles every trait method into an ordinary
`mptp_serving::Endpoint` whose pattern is
`{NAMESPACE}/{SERVICE_NAME}/{OPERATION}`, with `MethodSet::CALL`, a per-method owned
request struct, and a `TrEndpointHandler` generated by `#[rpc(impl_of(I))]`. The trait macro
only emits the contract, request types, and the `ClientProxy` impl; the server macro owns
the `mptp_serving` dependency. Patterns are concatenated at compile time with
`constcat::concat!` from concrete associated consts, so no change to `Endpoint`'s
`&'static str` pattern is needed and no nested sub-router (whose pipeline/root are
currently never executed) is required. `NAMESPACE` is a contract key, not an absolute URL
path: the effective wire route is the router's mount prefix plus the generated pattern.

A trait with N methods becomes N operation endpoints sharing the service key
`(NAMESPACE, SERVICE_NAME)`: the `impl_of` expansion emits one handler and one endpoint per
method, all handlers share the same `Arc<Impl>`, and the whole set is bound by one generated
`install_into` (the service-level unit). Each endpoint carries `RpcEndpointInfo` with
`namespace`, `service`, `operation` and `pattern`, so middleware can group by namespace,
service or operation. The router itself remains endpoint-only; there is no service-level
route and no second dispatcher.

Per the 2026-11-13 review: legacy wire compatibility is explicitly dropped (no alias
endpoint), the extra `associated_type_defaults` feature is accepted, `RpcEndpointInfo` is
added, the macro is named `impl_of`, the word `path` is removed from the protocol surface
in favour of `namespace`/`service`/`operation`/`pattern`, denco now implements
`TrEncodable`/`TrDecodable` for `()` (so 0-arg requests and `RpcResult<()>` need no wrapper),
and the ergonomic interface builder lives in a separate `rpc_serv_ext` crate that only the
assembly crate depends on — the protocol crate and the service-implementation crate stay
free of it.

---

## 11. 2026-11-14 修订：per-service API marker 与取消 namespace

> 本节是**现行设计**，取代 §3.1 / §4.4 / §5 中「客户端必须经过 `ApiV1` / `ImV1`
> interface marker」以及「`{namespace}/{service}/{operation}` 三段式」的写法。本文其余
> 章节里的 `namespace` 是历史引用；按本节读作「完整 service 前缀里的一段」，不再有
> 独立的 `namespace` 属性或字段。

### 11.1 动机

`ImV1` / `ApiV1` 是为了让多个 service 共享一个 `namespace` 而造出来的**挂载点类型**，
它本身不是业务概念。客户端要写 `client.call::<ImV1>()`，再调用具体 service 方法，仍然
要先理解“interface / namespace”这一层；而调用点真正想说的是“我用 `RoomService` 这个
契约”。

Rust 不允许把 trait 名当类型参数（`client.call::<RoomService>()` 不成立），但可以让
trait 的声明自己给出一个**替代类型名**，由宏生成：

```rust
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService { ... }

let room = client.call::<RoomSvc>();
let summary = room.create_room("rust".into(), None).await?;
```

这就是 `gen_mcf2` 已经在用的模式：用户给 `DoSth` 前缀，宏生成 `DoSthAsync` /
`DoSthFuture`。这里用户给 `RoomSvc`，宏生成这个 marker 类型与它的 impl。

### 11.2 可行性

- `ClientProxy<'a, C, I>` 已经是 `I: TrRpcInterface` 的泛型结构，运行期只持
  `&MptpClient<C>`；`I` 只提供一个路径前缀常量。换成 per-service marker 不需要改
  `ClientProxy` 的字段或生命周期。
- `TrRpcInterface` 的常量改为 `PREFIX`（完整服务前缀，如 `im/v1/room`）；
  `concat_location(PREFIX, operation)` 得到 `/im/v1/room/create_room`。没有运行期注册、
  没有反射、没有 `unsafe`。
- 取消 namespace 后不再需要单独的 `SERVICE_NAME`：`PREFIX` 已经是完整服务前缀
  （`im/v1/room`），客户端方法用 `PREFIX + "/" + operation` 拼路径；`operation` 由
  方法名提供。若诊断需要，可从 `PREFIX` 的最后一段派生，但不再参与路由。
- `rpc_core_v1` 内部要把 `unary_call_async_` 的 `service: &'static str` 参数改成
  `operation`，路径 = `concat_location(I::PREFIX, operation)`；这是内部改动，不影响
  调用点。
- 跨 crate：`RoomSvc` 与 trait 同在协议 crate；`im_client` 只依赖协议 crate；
  `im_server` 的 `#[rpc(impl_of(RoomSvc))]` 也引用它，不依赖 `im_client`。
- marker 是普通 `struct` + `impl`，proc macro 完全可控；trait 的可见性决定 marker
  的可见性（`pub trait` → `pub struct`）。
- 结论：**可以做到，且不需要改 `ClientProxy` 的结构**；只需要 `TrRpcInterface` 的
  常量名 + `expand_service_` 多生成一个类型和若干 impl。

### 11.3 语法与展开

```rust
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService {
    fn create_room(&self, name: String, topic: Option<String>)
        -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSummary>>;
    // ...
}
```

生成（除原有 trait / `__Req_*` / 客户端方法外）：

```rust
#[derive(Clone, Copy, Debug)]
pub struct RoomSvc;

impl ::rpc_core_v1::messaging::TrRpcInterface for RoomSvc {
    /// 完整服务前缀；pattern = PREFIX + "/" + operation。
    const PREFIX: &'static str = "im/v1/room";
}
```

协议 crate 的 `lib.rs` 记得 `pub use` marker（`pub use services::{RoomSvc, SessionSvc};`），
否则客户端 crate 拿不到类型。

客户端：

```rust
let room = client.call::<RoomSvc>();          // ClientProxy<'_, C, RoomSvc>
let summary = room.create_room("rust".into(), None).await?;
```

服务端：

```rust
#[rpc(impl_of(RoomSvc))]
impl RoomService for RoomServiceImpl { ... }
```

每个方法的 pattern = `im/v1/room/<operation>`；`RpcEndpointInfo` 用
`RoomSvc::PREFIX` + 方法名。

> 语法选择：推荐 `api = RoomSvc` 这种带 key 的形式；如果希望完全按
> `#[rpc(service = "im/v1/room", RoomSvc)]` 的裸 ident 写，也可以作为 sugar 解析，但
> 它与未来新增参数容易歧义，不建议作为唯一形式。

### 11.4 为什么可以取消 namespace

没有协议层的理由：MPTP 的 pattern 就是一个字符串。namespace 只是一个约定，用来：

1. 拼 `{namespace}/{service}/{operation}`；
2. 让多个 service 共享一个前缀，减少重复；
3. 给 `RpcEndpointInfo` 提供 namespace / service 两级分组。

改用完整 service 前缀后：

- 拼接仍是 `prefix + "/" + operation`；
- 多个 service 共享 `im/v1` 时，各自写 `service = "im/v1/xxx"`；重复的是字符串，
  不是类型/逻辑；
- `RpcEndpointInfo` 改为 `{ service: &'static str, operation: &'static str,
  pattern: &'static str }`，`service` 就是完整前缀；中间件按 `service` 或 `pattern`
  前缀分组即可。如果确实需要“版本段”，用 `service.rsplit_once('/')` 或
  `starts_with("im/v1/")` 做字符串判断，不需要协议层字段。

代价：

- 版本前缀在每个 trait 上重复一次；改版本时逐个 trait 改（可以接受，trait 数量有限）；
- 失去编译期的 `(namespace, service)` 结构化拆分；`RpcEndpointInfo` 只有 `service`
  整段。需要拆分的中间件自己做字符串处理；
- `TrRpcInterface::PATH` → `PREFIX`、`RpcEndpointInfo` 去 `namespace` 是公开 API 变更；
  code-first 尚未实现，现在改成本最低。

**结论：没有必须保留 namespace 的理由；取消它，`service` 用完整前缀。**

### 11.5 多 service / 多前缀

- **多个 service 共享 `im/v1`**：各自写 `service = "im/v1/session"` /
  `"im/v1/room"`；不需要 interface marker。
- **同一个 trait 挂多个前缀**（demo 的 `ImV2`）：per-service marker 只有一个 `PREFIX`。
  推荐用**一个 attribute 列出多组**：
  ```rust
  #[rpc(apis = [(DiagV1, "im/v1/diag"), (DiagV2, "im/v2/diag")])]
  pub trait DiagnosticsService { ... }
  ```
  宏生成 `DiagV1` / `DiagV2` 两个 marker、两份客户端 impl；服务端
  `#[rpc(impl_of(DiagV1))]` / `#[rpc(impl_of(DiagV2))]`。单组时 `service` + `api` 是
  它的 sugar。
  - 该 `apis` 语法是后续增强；本期 Call 验收只要求单组
    `service = "im/v1/room", api = RoomSvc`。多前缀不阻塞主验收。
- **interface marker**：在没有 namespace 之后不再需要；`services = [...]` 的多 service
  客户端视图可以保留为可选 ergonomics，但不作为主路径，也不要求实现。

### 11.6 验收标准（code-first Call）

验收表已按 §12（service 级路径 + 请求枚举）更新，编号 `CC-01..CC-18`，见 §12.10。
**§11 的 marker / 客户端挂载 / `PREFIX` 条款仍然有效；§12 只改 wire 形状与校验时机。**

### 11.7 风险

- **marker 名字冲突**：`api = X` 与同模块已有类型同名时，宏应报「生成类型 X 与现有
  类型冲突」；默认不自动改名，避免客户端调用点漂移。
- **可见性**：`pub trait` 在私有模块里时，marker 也私有，客户端 crate 无法
  `call::<X>()`；文档要求协议 trait 与 marker 放在 `pub` 模块。
- **语法歧义**：裸 ident 形式与未来参数（如 `typed`）容易冲突；推荐 `api = X`。
- **公开 API 变更**：`TrRpcInterface::PATH` → `PREFIX`、`RpcEndpointInfo` 去
  `namespace` / 单 `operation` 并新增 `methods`、`RpcMethodInfo`，需要更新 examples /
  tests / README；code-first 尚未实现，现在改成本最低。
- **多前缀**：见 11.5；不要把多个前缀硬塞进单 marker。
- **重复前缀**：每个 trait 重复写 `im/v1`；接受这个重复，换掉 namespace 的一层概念。

---

## 12. Call 请求信封（body enum）——2026-11-14 决定

> 本节是现行设计，取代 §11 里“每方法一个 Call pattern”的 wire 形状。trait API、
> per-service marker、`PREFIX`、取消 namespace 都不变；变的是 Call 的路由/校验/
> 元信息组织。

### 12.1 决定

- 同一个 service 的所有 `CALL` 方法共用**一条路径** `/{prefix}`（如 `im/v1/room`）；
- 请求 body 是一个生成的枚举 `XxxServiceCall`，变体 = 方法，变体载荷 = 该方法的请求；
- 不再为每个 Call 方法分配 path；非 `CALL`（View/Pull/Push/Drop）仍每方法一个 path；
- `RpcEndpointInfo` 从“一个 endpoint 一个 operation”重构为“一个 service endpoint +
  方法表”；
- 方法级类型校验在**解码变体载荷之前**完成；`Data_Type_Id` 仍是方法 payload 的类型，
  不是外层枚举的类型。

### 12.2 为什么之前的顾虑不成立

1. **方法级校验**：枚举的变体名是 body 的第 1 个值。dispatcher 先读变体名 → 选中
   `RpcMethodInfo` → 用该方法的 `RpcDataTypeInfo` 跑 `validate_request_types`
   （`Body_Type` / `Data_Type_Id` / `Accept` / strict）→ 通过后才解码变体载荷 →
   调用 impl。校验发生在业务 handler 之前，和 JSON-RPC “先解析/校验 body 再进
   handler”完全同构；不需要把 operation 塞进 header。
2. **`RpcEndpointInfo`**：HTTP / JSON-RPC 也是“一个端点 + 方法在 body 里”。把
   `RpcEndpointInfo` 理解为“service endpoint + `methods` 表”即可；operation 是表项，
   不是 endpoint 字段。方法级中间件在变体名读取后、载荷解码前运行（service 级中间件
   仍在 router 层）。
3. **header 表达力**：operation / payload 结构放 body 才是 RPC 的自然形状；header 只
   保留 codec / `Data_Type_Id` / `Accept` / policy 这类传输协商信息。不再考虑
   operation header 折中。
4. **剩余真实代价**：405 / `Allow` 是 service 级（Call trait 只有 CALL，不影响）；
   操作级中间件从 router 层移到 dispatcher 的“读变体后”阶段；这两点见 §12.6。

### 12.3 生成形状

```rust
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService { ... }
```

生成（`#[doc(hidden)]`）：

```rust
#[derive(TrEncodable, TrDecodable)]
pub enum RoomServiceCall {
    #[denco(rename = "list_my_rooms")]
    ListMyRooms,                       // 0 参 → unit
    #[denco(rename = "create_room")]
    CreateRoom(__Req_create_room),     // ≥2 参 → newtype 包 __Req_*
    #[denco(rename = "join_room")]
    JoinRoom(u64),                    // 1 参 → newtype
    #[denco(rename = "send_message")]
    SendMessage(__Req_send_message),
}
```

- 变体线上名 = trait 方法名（`#[denco(rename)]`）；重命名方法 = 线上破坏；
- 枚举只是**分派信封**：不 derive `TrHasTypeId`，不作为 `Data_Type_Id`；payload 的
  类型 ID 才是线上类型；
- 客户端代理的每个方法：写 `/{prefix}`、`Body_Type`、`Data_Type_Id = payload ID`、
  `Accept = response ID`，body 是枚举对应变体；
- 响应没有外层信封：单个方法响应值 + 响应 `Data_Type_Id`。

### 12.4 服务端两阶段

```text
1. 读 body 第 1 个值：variant name（String）
2. match 到 RpcMethodInfo { operation, data: RpcDataTypeInfo }
3. validate_request_types(CALL, headers, &method.data)   // 400 / 406 / 未知操作
4. 解码该变体的 payload（() / T / __Req_*）
5. 调用 impl 方法；按 method.data.response_body 写响应头
```

- 第 1 步只读变体名，不解 payload；第 3 步在 payload 解码前完成；
- 未知变体：404（或 400，按团队约定；建议 404，因为“方法不存在”）；
- `Data_Type_Id` 与变体不匹配：406，在 payload 解码前；
- `Accept` 与方法响应类型不匹配：406；
- 0 参方法的 payload 是 `()`；仍走同一流程；
- 枚举的编码是 variant name + payload（denco 枚举的现有形状）；dispatcher 的两阶段
  读取与它一致。

### 12.5 `RpcEndpointInfo` 重构

```rust
pub struct RpcEndpointInfo {
    /// 完整服务前缀，如 `im/v1/room`。
    pub service: &'static str,
    pub pattern: &'static str,          // 与 service 相同（相对 router 根）
    pub methods: &'static [RpcMethodInfo],
}

pub struct RpcMethodInfo {
    pub operation: &'static str,        // "create_room"
    pub data: RpcDataTypeInfo,          // request / response ID, codec, policy
}
```

- `RpcEndpointInfo` 是 service 级；`RpcMethodInfo` 是方法级；
- 方法级中间件 / 授权 / 限流：dispatcher 读到变体名后，选中 `RpcMethodInfo`，在
  payload 解码前执行；service 级中间件仍在 router；
- 旧的 `RpcEndpointInfo { namespace, service, operation, pattern }` 删除
  `namespace` / `operation`，新增 `methods`；这是公开 API 变更，code-first 未实现，
  现在改成本最低；
- 非 Call 端点（controller/Pull/Push/Drop）可继续用 `RpcDataTypeInfo`；
  若也挂 `RpcEndpointInfo`，`methods` 只有一项。

### 12.6 405 / Allow 与中间件

- service 端点只有 `CALL`；`Allow: CALL`。Call trait 的方法都是 CALL，不存在
  per-method `Allow`；
- 需要 VIEW/POST/DROP 的方法不属于 Call，走 controller 的 per-method path；
- service 级中间件：router 层，读 `RpcEndpointInfo.methods`，但还不知道 operation；
- 操作级中间件：dispatcher 读到变体名后、payload 解码前执行；框架提供
  `RpcMethodInfo` + 一个方法级 hook / `DispatchContext`；
- 这与 HTTP/JSON-RPC 的“解析 operation 后再做方法级策略”一致。

### 12.7 类型 ID / 清单

- `Data_Type_Id` = 方法 payload 类型（`__Req_*` / scalar / `()`），方法级稳定；
- 新增方法只增加一个新变体 + 新 payload ID，**不改变已有方法的内层 ID**；
- 枚举本身不参与 `denco_type_ids!`；清单列每方法的 request / response 类型；
- 变体名（operation 名）是 wire 契约的一部分；改名/删除方法破坏旧客户端；
- `Accept` = 方法响应类型；strict 在 §12.4 第 3 步校验。

### 12.8 客户端

```rust
let room = client.call::<RoomSvc>();
let summary = room.create_room("rust".into(), None).await?;
```

- 调用点不出现 path / 变体 / 编码；
- 代理内部：`RoomServiceCall::CreateRoom(__Req_create_room { ... })` + `/{prefix}` +
  方法级 `Data_Type_Id` / `Accept`；
- 响应按方法返回类型解码并校验 `Data_Type_Id`。

### 12.9 非 Call

View / Pull / Push / Drop 保持 §11 + endpoint-design 的每方法 path 与
`RpcDataTypeInfo`；Call 的 service 级路径不会与它们冲突（例如 `CALL /im/v1/room`
与 `VIEW /im/v1/room/list` 是不同 pattern + 方法）。

### 12.10 验收标准（code-first Call）

| ID | 验收项 | 判定 |
| --- | --- | --- |
| CC-01 | 语法：`#[rpc(service = "im/v1/room", api = RoomSvc)] pub trait RoomService` 可编译 | 生成 `RoomSvc` + `RoomServiceCall`；trait 原方法、`__Req_*` 都在 |
| CC-02 | marker：`RoomSvc` 零大小、可见性同 trait、`TrRpcInterface::PREFIX = "im/v1/room"` | `RoomSvc::PREFIX` 断言 |
| CC-03 | 客户端挂载：`client.call::<RoomSvc>()` 返回实现 `RoomService` 的代理 | 编译 + 调用 |
| CC-04 | 调用点无 path / operation / 变体 / body 编解码 | 源码检查 |
| CC-05 | 路由：Call 只有 `CALL /im/v1/room`；**没有** `CALL /im/v1/room/<operation>` | 路由表断言 |
| CC-06 | 枚举：0 参 unit、1 参 newtype、≥2 参 `__Req_*` newtype；变体线上名 = 方法名 | 编解码 roundtrip；`denco(rename)` |
| CC-07 | 两阶段校验：先读变体名 → 选 `RpcMethodInfo` → `validate_request_types` → 再解码 payload；缺失/错误 400/406 在 impl 前 | 错误用例断言 handler 未运行 |
| CC-08 | 未知变体：404（或约定状态），在 payload 解码前；连接仍可用 | 原始探针 |
| CC-09 | dispatcher：变体 → impl 方法；响应按方法 `response_body` 写头；同一 `Arc` 共享 | 每方法命中；响应头正确 |
| CC-10 | 类型 ID：`Data_Type_Id` = 方法 payload ID（不是枚举 ID）；新增方法不改变已有方法 ID | golden / 清单 |
| CC-11 | `RpcEndpointInfo { service, pattern, methods }`；`RpcMethodInfo { operation, data }` 正确 | 探针中间件 |
| CC-12 | 方法级中间件在变体名读取后、payload 解码前运行；service 级中间件在 router 层 | 探针顺序断言 |
| CC-13 | 405 / `Allow` 是 service 级 `CALL`；Call trait 不出现 per-method 方法集 | 原始探针 |
| CC-14 | 客户端响应校验：响应 `Data_Type_Id` 与方法返回类型不一致 → 406 / 解码错误 | 错误用例 |
| CC-15 | 跨 crate：`im_client` 只依赖协议 crate；`im_server` 依赖协议 + `mptp_serving` | `cargo tree` |
| CC-16 | 错误 marker：`RoomSvc` 代理上调用 `SessionService` 方法编译失败 | trybuild |
| CC-17 | 重复 / 非法变体（同名方法、默认实现、泛型方法等）编译失败 | trybuild |
| CC-18 | 非 Call 混挂：`CALL /im/v1/room` 与 `VIEW /im/v1/room/list` / `PULL .../events/{id}` 共存 | 路由命中 |
| CC-19 | 取消：每个方法可 `.may_cancel_with(token)` | 编译 + 取消用例 |
| CC-20 | 多前缀：`DiagV1` / `DiagV2` 两个 marker，各自 `CALL /im/v1/diag` / `CALL /im/v2/diag` | 路由 + 客户端挂载 |

### 12.11 风险

- **枚举 schema 变化**：新增变体对旧客户端是兼容的（旧客户端不发送新变体）；删除/
  改名变体是破坏。枚举不参与类型 ID，避免“任一方法变动就全量 ID 变化”。
- **两阶段解码**：dispatcher 必须按 denco 枚举的“variant name + payload”形状读；
  0 参变体没有 payload，读到名字即完成。实现要固定这个顺序，不能依赖完整枚举解码。
- **方法级中间件**：需要 `DispatchContext` / `RpcMethodInfo` 的框架支持；在落地前，
  方法级策略只能放 dispatcher 内联。
- **405**：service 级；Call trait 的方法不能混入其他访问方法。
- **`RpcEndpointInfo` 公开 API 变更**：删 `namespace`/`operation`，加 `methods`；
  code-first 未实现，现在改成本最低。
- **path 冲突**：`CALL /im/v1/room` 与 `VIEW /im/v1/room` 同 pattern 不同方法可共存；
  若 controller 也注册 `CALL /im/v1/room` 才是冲突。
