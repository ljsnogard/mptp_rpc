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

### 3.1 目标用户形状

> **2026-11-14 修订**：客户端挂载点改为每个 service trait 自己生成的 marker（§11），
> 并**取消 namespace 概念**：`service` 是完整服务前缀（如 `im/v1/room`）。下面
> `ApiV1` / `WorkspaceService` 的 interface marker 形状是历史设计；单 service 直接写
> `#[rpc(service = "im/v1/workspace", api = WorkspaceSvc)]`，客户端
> `client.call::<WorkspaceSvc>()`。

协议 crate（客户端与服务端共享）保持 `mptp_cs_smoke` / `DESIGN_BLUEPRINT` 的形状：

```rust
#[rpc(service = "workspace")]
pub trait WorkspaceService {
    fn create_workspace(
        &self,
        name: String,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Workspace>>;

    fn list_workspaces(
        &self,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<Workspace>>>;
}

#[rpc(namespace = "api/v1", services = [WorkspaceService, SessionService])]
pub struct ApiV1;
```

服务端 crate：

```rust
#[rpc(impl_of(ApiV1))]
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
let v1 = client.call::<ApiV1>();
let workspace = v1
    .create_workspace("alpha".to_string())
    .may_cancel_with(token)
    .await?;
```

协议 crate 的 trait 方法签名与客户端调用点与重构前一致；改变的是 interface marker /
服务端 attribute 的命名（`namespace` / `impl_of`，决策 5/6）和宏展开后的服务端实现。

### 3.2 粒度：方法级 Endpoint，而不是 service 级单端点

| 维度 | 旧形状（service 级 + body enum） | 新建议（方法级 Endpoint） |
| --- | --- | --- |
| 路由规则 | `{namespace}/{service}` | `{namespace}/{service}/{operation}` |
| 操作选择 | body enum 变体 | pattern 最后一段 |
| 中间件粒度 | service | **operation** |
| 端点元数据 | 无处安放 | 每方法一份 |
| 405 | 自己不处理 | 框架统一 `405` + `Allow` |
| `AcceptType` 类型校验 | 只能宽匹配 | 每端点可绑定请求类型 |
| 与 controller 共存 | 需要独立 router | 同一 `RadixRouter` |
| 兼容旧客户端 | 是 | **明确不要求**（决策 1） |

结论：采用方法级 Endpoint。理由不是「新潮」，而是它正好复用了 `mptp_serving` 的
既有能力；service 级单端点会重新造一个「路由表之外的分派器」，与
`rpc-svc-macro-mptp-serving-20261112-2130.md` 已经确定的「方法 = 端点」方向相反。

**不做兼容档（决策 1）**：不生成 `/{namespace}/{service}` 的 service 级别名端点，
也不保留旧共享 `Call` enum；客户端与服务端一起升级。这样每个方法只有一套 handler，
`AcceptType`、per-method metadata 和 405 都只有一条路径。

### 3.3 pattern 拼接：`constcat`（已验证，零框架改动）

问题：`I` 的命名空间在 protocol crate 的 interface 宏里，`SERVICE_NAME` 在 trait 宏里，
操作名在 service trait 里；`#[rpc(impl_of(I))]` 是第三次宏展开。三者分开写，
无法用内建 `concat!`（只接受字面量）。

但 `impl_of` 展开时，`I` 与 `impl` 类型都是**具体类型**，因此：

```rust
const PATTERN: &'static str = ::rpc_core_v1::x_deps::constcat::concat!(
    <ApiV1 as ::rpc_core_v1::messaging::TrRpcInterface>::NAMESPACE,
    "/",
    <WorkspaceServiceImpl as WorkspaceService>::SERVICE_NAME,
    "/",
    "create_workspace",
);
```

已验证：

- `constcat 0.3.1 / 0.5 / 0.6.1` 都能对**具体类型的关联常量**做 const 拼接，
  `no_std`、stable（MSRV 1.66）可用；
- 外层函数可以是泛型（`install_into<C, S, R, A>`），只要 const 表达式里不出现
  **泛型参数**；constcat 对 `<I as Trait>::CONST` 中 `I` 为泛型参数会报 `E0401`；
- 生成的 `&'static str` 可直接传给现有的 `Endpoint::new`，**不需要改
  `mptp_serving` 的 `Endpoint` pattern 类型**。

备选（不推荐，除非未来需要运行期 / 多租户前缀）：

1. **`Endpoint` 拥有 pattern**（`Cow<'static, str>` + `new_owned`），用
   `concat_location` 在装配期拼 `String`。改动集中，但 `pattern()` 从
   `&'static str` 变 `&str` 是公开 API 变更，且 `bind_endpoint` 里要先复制 key 再
   move endpoint（否则 `E0505`）；
2. **嵌套子 router**（interface router → service router）：`bind_router` 当前**不会**
   执行子 router 的 `pipeline` / `root` / `state` / `cancel_source`；父层只调用
   `child.match_route(...)` 并在第一个前缀命中后直接返回
   （`mptp_serving/src/router_.rs:506-511`、`serve_handler_.rs:99`/`132`）。
   因此子 router 上的中间件会静默不生效，不能用来承载 service 级中间件；
3. **内建 `concat!`**：不接受关联常量，直接排除。

`constcat` 的依赖落点：在 `rpc_core_v1` 的 `Cargo.toml` 加 `constcat`，并在
`rpc_core_v1::x_deps` 重新导出（`pub use constcat;`）。生成代码通过
`::rpc_core_v1::x_deps::constcat::concat!` 使用，下游协议 / 服务端 crate 不必各自声明
依赖。这个引用方式已验证可跨 crate 解析宏。操作名进 pattern 前用
`syn::ext::IdentExt::unraw()` 去掉 `r#` 前缀（legacy 的 hidden 模块命名也做了同样处理）。

#### 3.3.1 术语与挂载点：`namespace` / `service` / `operation` / `pattern` / `mount`

决策 6 要求协议层不再用 `path`。五个概念必须分开：

| 术语 | 谁定义 | 含义 |
| --- | --- | --- |
| `NAMESPACE` | interface marker 的 `namespace = "..."` | 协议命名空间（若干段），是**契约 key**，不是绝对 URL |
| `SERVICE_NAME` | `#[rpc(service = "...")]` | 服务在 namespace 下的段 |
| `OPERATION` | trait 方法名（`unraw` 后） | 操作段 |
| `PATTERN` | `impl_of` 宏生成 | **相对 router 根**的路由规则 = `NAMESPACE/SERVICE_NAME/OPERATION` |
| `MOUNT` | 装配期 router | 父 router 的前缀（`bind_router`）或根位置；协议宏不感知 |

生效线上路径 = `MOUNT + "/" + PATTERN`（按 `RadixRouter` 的段规范化）。例如：

- 端点直接装在顶层 router（根）：`api/v1/workspace/create_workspace`；
- 顶层 router 先被挂在 `tenant/a` 下：`tenant/a/api/v1/workspace/create_workspace`。

因此：

- 协议宏里的 `NAMESPACE` **不是**绝对 path，只是契约路由键的一段；这样它不会假装
  知道 router 最终把服务挂在哪里；
- `MOUNT` 由装配决定；若挂载点非空，客户端必须使用同一前缀（例如客户端连接 / 代理的
  base prefix 配置），框架不做隐藏改写；
- `PATTERN` 仍是 `&'static str`（`NAMESPACE` / `SERVICE_NAME` / `OPERATION` 都是编译期
  常量）；`MOUNT` 由 router 在匹配时剥离，不参与宏展开；
- 若把 RPC 端点装进**子 router** 再 `bind_router` 挂载，当前框架只执行**顶层** router
  的 pipeline，子 router 的 pipeline / root 不会执行
  （`mptp_serving/src/router_.rs:506-511`、`serve_handler_.rs:99`/`132`）；中间件应加在
  顶层 router 上。这是既有语义，不是本次引入。

> 若未来要求「挂载点完全取代 `NAMESPACE` 参与线上路由」，需要把端点安装成**不含
> `NAMESPACE` 的相对 pattern**，并补齐子 router 的 pipeline / root 语义；那会改变
> `mptp_serving` 的嵌套 router 行为，建议单独立项。本文按「`NAMESPACE` 是契约 key、
> `MOUNT` 是装配前缀」记录，避免把两者混成一个 `path`。

### 3.4 请求体：按方法参数个数选择 `()` / 单参数类型 / 多参数字段结构体

旧形状的 body enum 有两个问题：方法不能独立类型校验；`impl_of` 在别的 crate 里
无法命名 hidden 模块里的 enum（除非再引入一层 trait 隐藏方法）。denco 已经支持
`()` 的编解码，因此新形状可以按参数个数直接选请求类型，不再为每个方法都造结构体：

| 参数个数 | 请求类型 | 说明 |
| --- | --- | --- |
| 0 | `()` | 无业务参；客户端发 unit，服务端解码 unit 后调用 |
| 1 | 该参数的 owned 类型 | `&str`→`String`、`&[u8]`→`Vec<u8>`、`&Copy`→标量、owned 业务类型原样 |
| ≥2 | 隐藏结构体 `__Req_<operation> { f0, f1, … }` | 字段按位置命名，derive `TrEncodable`/`TrDecodable` |

```rust
#[doc(hidden)]
#[allow(non_camel_case_types)]
pub mod __rpc_service_WorkspaceService {
    /// 仅多参数方法需要；单参数直接用参数类型，0 参数用 `()`。
    #[derive(TrEncodable, TrDecodable)]
    pub struct __Req_create_session {
        pub f0: u64,
        pub f1: ::alloc::string::String,
    }
}

pub trait WorkspaceService {
    // 0 参 → ()
    #[doc(hidden)]
    type __Req_list_workspaces = ();
    // 1 参 → 直接使用 owned 类型
    #[doc(hidden)]
    type __Req_create_workspace = ::alloc::string::String;
    #[doc(hidden)]
    type __Req_get_workspace = u64;
    // ≥2 参 → hidden 结构体
    #[doc(hidden)]
    type __Req_create_session =
        __rpc_service_WorkspaceService::__Req_create_session;
    // ...
}
```

要点：

- **多参数字段按位置命名 `f0`、`f1`…**，不沿用参数名。原因：`impl_of` 看到的是
  **impl** 的方法签名，参数名允许与 trait 声明不同；按位置命名让 trait 宏与 impl 宏
  无需共享参数名。
- 借用参数按旧规则转 owned：`&str` → `String`、`&[u8]` / `&Vec<u8>` → `Vec<u8>`、
  `&String` → `String`、`&Copy` 标量 → 标量；其它借用继续拒绝（旧分析器规则）。
- `()` / 单参数类型的规则让 `AcceptType` / 类型校验中间件可以直接使用业务类型，
  不必再为每个方法生成一个 wrapper。
- 新增 / 重命名参数会改变该方法的 body（破坏兼容）；重命名 **Rust 参数名**不影响线上
  字段名（多参时始终是 `f0`…），这是位置命名的额外好处。
- **多参数字段顺序**：`f0`、`f1`… 的顺序 = trait 声明顺序。新增参数只能追加在末尾；
  重排 / 删除属于破坏性变更，需要写进 README。

**返回值 `()`（决策 7，denco 已实现）**：`RpcResult<()>` 直接走 `write_ok_async(&())`，
客户端 `Rep = ()` 正常解码，不需要宏特例。

### 3.5 服务端代码生成的职责划分

关键约束：**协议 crate 不应被迫依赖 `mptp_serving`**（客户端也要引用协议 crate；
legacy 正是用 `abs_rpc_serv::service::TrRpcCallCtx` 把协议层与服务端运行时解耦）。
因此分工如下：

| 展开点 | 所在 crate | 生成内容 | 依赖 |
| --- | --- | --- | --- |
| `#[rpc(service)]` trait | 协议 crate | `SERVICE_NAME`、每方法请求类型、关联类型桥、`impl Trait for ClientProxy` | `rpc_core_v1` |
| `#[rpc(namespace, services)]` interface | 协议 crate | `TrRpcInterface`、`ServiceSelector` | `rpc_core_v1` |
| `#[rpc(impl_of(I))]` impl | 服务端 crate | 每方法 `TrEndpointHandler` + `install_into` + 端点元数据 | `mptp_serving` + `rpc_core_v1` |

注意：`#[rpc(namespace = "...", services = [...])]` 里的 `services` 列表**仍然只做声明与校验**
（与 legacy 一致），不驱动服务端注册；实际注册哪些 impl、注册到哪个 interface，由
`#[rpc(impl_of(I))]` 的 `install_into` 决定。这样同一个 impl 可以分别挂到多个
interface（`mptp_cs_smoke` 的 `WorkspaceServiceImpl` 同时挂 `ApiV1` / `ApiV2`）。

关联类型桥（trait 宏生成，用于让 `impl_of` 在**另一个 crate** 里命名请求类型）：

```rust
pub trait WorkspaceService {
    const SERVICE_NAME: &'static str = "workspace";

    #[doc(hidden)]
    type __Req_create_workspace = ::alloc::string::String;
    #[doc(hidden)]
    type __Req_list_workspaces = ();
    #[doc(hidden)]
    type __Req_get_workspace = u64;
    #[doc(hidden)]
    type __Req_create_session =
        __rpc_service_WorkspaceService::__Req_create_session;

    // ... 用户方法
}
```

- `associated_type_defaults` 是 nightly feature；协议 crate 需要
  `#![feature(associated_type_defaults)]`；
- 实现者（包括 `ClientProxy` 与用户的 `WorkspaceServiceImpl`）**不需要**重复写这些
  关联类型；默认值自动继承；
- `impl_of` 侧写
  `let req: <Impl as WorkspaceService>::__Req_create_workspace = ...;`，
  跨 crate 使用已验证可编译；
- 这些关联类型是 `#[doc(hidden)]`，并在 trait 文档中注明「不要覆盖」。

> **已决策（决策 2）**：接受协议 crate 多开 `associated_type_defaults`，采用关联类型
> 方案。原备选的 PhantomData 隐藏方法方案（每方法一个
> `fn __rpc_req_<op>_() -> PhantomData<__Req_<op>>` + `decode_body_tagged_async`）
> 不再采用，避免在 `rpc_core_v1` 里增加只为绕过 feature 而存在的 API。

### 3.6 客户端代码生成

trait 宏为 `ClientProxy` 生成实现（legacy 已经验证过的形状）：

```rust
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
}
```

- `call_unary_async` 内部用 `concat_location(I::NAMESPACE, service)` 拼出
  `api/v1/workspace/create_workspace`（`proxy_.rs:167`；该函数名保留既有 API，
  语义是「namespace + 相对路由键」），客户端不需要新的调用入口；
- 相对路由键 `"workspace/create_workspace"` 由宏在展开期用两个**字面量**拼成，
  不涉及运行期分配（除了 `call_unary_async` 原有的 `format!`）；
- trait 若声明 `Send + Sync` supertrait，客户端 impl 需要额外的
  `SharedConn: Sync` 约束（legacy 的 `requires_send_sync_` 逻辑），照旧保留；
- 返回类型重写成全限定的 `TrMayCancel` / `RpcResult`，并保留用户额外 bound（legacy
  的 `return_type_` 逻辑）；若用户写 `+ Send`，生成的 future 不满足，编译期报错，
  README 明确不支持。

### 3.7 错误与取消

服务端每方法 handler 的大致形状：

```rust
let result = <Impl as Trait>::method(&*self.svc_, args...)
    .may_cancel_with(ctx.cancel().child_token())
    .await;

match result {
    Ok(value) => write_ok_async(ctx.session_mut(), &value).await?,
    Err(error) => {
        let (status, message) = error.into_reply_parts();
        write_status_async(ctx.session_mut(), status, message).await?;
    }
}
```

- `RpcError::into_reply_parts()` 保证 `RpcError::Service { message }` 原样写出，
  不带 `Display` 前缀（`error_.rs:130` 的既有测试覆盖）；
- 解码失败目前走 `ServeError` → 收尾兜底 `500`；与 controller 形态现状一致。
  后续若要细分成 `400`，在 handler 里 catch 解码错误并写
  `Status::BadRequest` 即可，不改变宏接口；
- 取消是协作式的：handler 只取消**方法返回的 future**；业务实现内部是否观察令牌由
  实现决定（与旧 dispatch 语义一致，见 `rpc-svc-macro-mptp-serving-20261112-2130.md`
  §3.5/§3.6）；
- `decode_body_async` 当前内部用 `NonCancellableToken`
  （`rpc_core_v1/src/serving/body_.rs:59`）；若要求「请求级取消能中断解码」，需要给
  body helper 增加可取消变体，属后续增强，不是本次恢复的阻塞项；
- **`RpcResult<()>`（决策 7，已落地）**：denco 已为 `()` 实现 `TrEncodable` /
  `TrDecodable`，`write_ok_async(ctx.session_mut(), &())` 与客户端 `Rep = ()` 解码都走
  普通路径，宏不需要为「空成功体」生成特例；`impl_of` 的测试可以直接覆盖 `RpcResult<()>`。

### 3.8 元数据、中间件与共存

生成的端点用 `bind_endpoint` 挂进 router，因此：

- 与 controller 生成的端点（用 `full_path_lit_` 的 `interface/method` pattern）、自由函数
  端点、手写端点完全同构；`use_middleware` / `with_group_metadata` / `with_error_hook`
  都照常作用；
- **新增 `RpcEndpointInfo`（决策 4）**，放在 `rpc_core_v1::serving`（协议 crate 已经依赖
  `rpc_core_v1`，不增加新依赖）：

```rust
#[derive(Clone, Copy, Debug)]
pub struct RpcEndpointInfo {
    /// 契约命名空间（不含 router 挂载点）。
    pub namespace: &'static str,
    /// service 段。
    pub service: &'static str,
    /// operation 段（trait 方法名，unraw 后）。
    pub operation: &'static str,
    /// 相对 router 根的 pattern（namespace/service/operation）。
    pub pattern: &'static str,
}
```

中间件可以：

```rust
let info = endpoint.metadata().get::<RpcEndpointInfo>();
if info.is_some_and(|i| i.operation == "create_workspace") {
    // 方法级策略：限流 / 审计 / 指标
}
```

- `impl_of` 生成的端点默认挂上这个标记；用户也可以在自己的中间件里读
  `endpoint.pattern()`，但结构化的 `RpcEndpointInfo` 更稳定（不依赖字符串解析）；
- pattern 冲突：同一 namespace 下两个 service 的 `SERVICE_NAME` 相同且 operation 相同，会落到
  同一 `pattern + CALL`；`bind_endpoint` 按插入顺序取第一个，不报冲突
  （`endpoint_.rs:151-161`）。这与 controller 的既有约定一致；README 需要写明
  「同一 namespace 下 service 名应唯一」。可选的改进是在 `install_into` 里做一次
  运行期冲突检查，但那需要 router 暴露查询能力，本文不展开。

### 3.9 装配形状

**最小形状（推荐先落地）：**

```rust
let router = RadixRouter::<SrvCfg, ConnState, ()>::new_with_state_and_request(
    std::sync::Arc::new(AppRoot),
    state,
);
let router = router.use_middleware(PrincipalMiddleware);
let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1::install_into(
    router,
    std::sync::Arc::new(WorkspaceServiceImpl::new_()),
)?;
let router = __rpc_impl_of_SessionServiceImpl_via_ApiV1::install_into(
    router,
    std::sync::Arc::new(SessionServiceImpl::new_()),
)?;
let servlet = mptp_smux::serving::MptpServlet::<SrvCfg, _>::new(router);
```

**人体工学 API：独立的 `rpc_serv_ext` crate（决策 3）。**

目标是把「按 interface 分组装配」的糖放在**独立 crate**，并且不让协议 crate / 业务
impl crate 依赖它。采用**闭包式 builder**，不生成跨 crate 的 `TrRpcServiceOf` impl：

```rust
// 新 crate：rpc_serv_ext（工作名）
// 依赖：rpc_core_v1 + mptp_serving；不依赖 rpc_svc_macro，也不依赖协议 crate。
pub struct InterfaceBuilder<I, C, S, R> {
    router_: RadixRouter<C, S, R>,
    _namespace_: core::marker::PhantomData<I>,
}

pub fn interface<I, C, S, R>(
    router: RadixRouter<C, S, R>,
) -> InterfaceBuilder<I, C, S, R>
where
    I: ::rpc_core_v1::messaging::TrRpcInterface,
{
    InterfaceBuilder { router_: router, _namespace_: core::marker::PhantomData }
}

impl<I, C, S, R> InterfaceBuilder<I, C, S, R> {
    pub fn service(
        self,
        install: impl FnOnce(
            RadixRouter<C, S, R>,
        ) -> Result<RadixRouter<C, S, R>, ::mptp_serving::x_deps::matchit_route::InsertError>,
    ) -> Result<Self, ::mptp_serving::x_deps::matchit_route::InsertError>;
    pub fn finish(self) -> RadixRouter<C, S, R>;
}
```

使用：

```rust
let router = rpc_serv_ext::interface::<ApiV1>(router)
    .service(|r| __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1::install_into(r, ws))?
    .service(|r| __rpc_impl_of_SessionServiceImpl_via_ApiV1::install_into(r, ss))?
    .finish();
```

依赖矩阵（这是决策 3 的核心）：

| crate | 依赖 | 说明 |
| --- | --- | --- |
| 协议 crate | `rpc_core_v1` | trait / interface / 请求结构体 / client impl；**不依赖** `mptp_serving`、`rpc_serv_ext` |
| 业务 impl crate | `rpc_core_v1` + `mptp_serving` | `#[rpc(impl_of)]` 生成的 handler 需要 `mptp_serving`；**不依赖** `rpc_serv_ext` |
| 装配 crate（composition root） | `rpc_serv_ext` | 只有这里按需使用 builder 糖 |
| `rpc_core_v1` | 不加 `mptp_serving` | 保持现有依赖图；`RpcEndpointInfo` 仍放这里（新增类型，不新增依赖） |

小项目里「业务 impl crate」与「装配 crate」经常是同一个 crate；此时它会多一个
`rpc_serv_ext` 依赖。这是**按需**选择（不用 builder 就不加），而不是像把 trait 放进
`rpc_core_v1` 那样对所有协议 / 业务 crate 强制生效。

`interface::<I>()` 只做分组与 `I: TrRpcInterface` 的编译期校验，**不重写 pattern**，
也不读取 `I::NAMESPACE` 去改写路由；真正的绑定仍由 `impl_of` 生成的
`install_into` 完成（constcat 在具体类型上展开，不受 builder 泛型影响）。因此 builder
是纯装配糖，去掉它不影响语义。

> 备选（不采用）：在 `impl_of` 里生成 `impl TrRpcServiceOf<I> for Impl`，让 builder 支持
> `.add_service::<Impl>(svc)`。它更好看，但会把 `rpc_serv_ext`（或至少它的 trait）变成
> 业务 impl crate 的必选依赖，与决策 3 的「减少业务 crate 依赖」相冲突。

### 3.10 一个 trait 的多个方法如何对应多个访问点

这是本方案的核心关系模型：**trait 不是一条路由，而是一组访问点的契约与共同 key**。

#### 3.10.1 身份三元组

每个 RPC 访问点由三个键唯一确定：

| 键 | 来源 | 例子 |
| --- | --- | --- |
| `NAMESPACE` | interface marker `namespace = "..."` | `api/v1` |
| `SERVICE_NAME` | service trait 的 `#[rpc(service = "...")]` | `workspace` |
| `OPERATION` | trait 方法名（`unraw` 后） | `create_workspace` |

- `(NAMESPACE, SERVICE_NAME)` 是「这个 RPC API / service」的身份；
- `OPERATION` 是「这个 service 的哪一个方法」的身份；
- 访问点 `PATTERN = NAMESPACE/SERVICE_NAME/OPERATION`（相对 router 根，§3.3.1）。

因此 service 不是第四条路由，而是这组访问点的**公共 key**；router 仍然只看到一条条
具体 endpoint，不需要理解 trait。

#### 3.10.2 编译期：一次展开、一一对应

`#[rpc(impl_of(ApiV1))] impl WorkspaceService for WorkspaceServiceImpl` 展开时：

1. 枚举 impl 块中属于该 trait 的所有 `&self` 方法；
2. 每个方法生成一个 handler + 一个 `Endpoint`（pattern 由三元组拼成）；
3. 所有 handler 共享同一个 `Arc<WorkspaceServiceImpl>`；
4. 所有 `Endpoint` 由同一个 `install_into` 安装；
5. 生成代码全部在同一个 hidden 模块里（模块名含 impl 与 interface）。

所以「trait 方法 ↔ 访问点」的关系不是运行期查表，而是**编译期生成的一一对应**：

| trait 项 | 是否访问点 | 请求类型 | operation |
| --- | --- | --- | --- |
| `&self` 方法（0 参） | 是 | `()` | 方法名 |
| `&self` 方法（1 参） | 是 | owned 参数类型 | 方法名 |
| `&self` 方法（≥2 参） | 是 | `__Req_<op>` 结构体 | 方法名 |
| 关联 const / type | 否 | — | — |
| supertrait（如 `Send + Sync`） | 否 | — | — |
| 非 `&self` 方法 | 拒绝 | — | — |
| 带默认实现的 trait 方法 | 拒绝（v1） | — | — |

Rust 的类型系统负责兜底：trait 增加一个方法，impl 必须实现它；宏在同一个 impl 块上
展开，看到的方法集合就是生成的端点集合，不会出现「trait 有方法、服务端漏挂」的情况。
客户端的 `impl Trait for ClientProxy` 由同一个 trait 宏生成，同样是「每方法一个代理
方法」，因此两端的 operation 集合天然一致。

**默认实现必须在分析层拒绝**：`impl_of` 依赖 impl 块枚举方法；如果 trait 方法有默认
实现且 impl 没有重写，impl 块里就不会出现该方法，服务端会漏挂端点，而客户端的 trait
宏仍然会生成对应代理方法。为保持两端一致，`#[rpc(service)]` 的方法必须是纯声明
（无 body）；helper 逻辑放在 impl 类型自己的固有 impl 或另一个普通 trait 里。
supertrait 的方法也不属于本 service 的 operation，需要显式写进 service trait。

带 `#[cfg]` 的方法：分析层把方法上的 `cfg` 继承到请求类型 / 关联类型 / 客户端代理方法；
`impl_of` 把 impl 方法上的同一组 `cfg` 继承到 handler / endpoint 绑定。两端的 cfg 必须
一致（它们来自同一个协议 crate 的条件编译），否则会退化成「一端有、一端没有」。

#### 3.10.3 运行期：service key 在元数据里，而不是在 router 里

每个生成的 `Endpoint` 挂：

```rust
RpcEndpointInfo {
    namespace: "api/v1",
    service: "workspace",
    operation: "create_workspace",
    pattern: "api/v1/workspace/create_workspace",
}
```

中间件可以按三个粒度分组：

- `operation`：单方法策略（限流键、审计、指标）；
- `service`：整组方法策略（例如「workspace 服务只允许某个角色」）；
- `namespace`：版本 / 租户级策略。

router 本身不为 service 增加任何结构：它只按 pattern 匹配。service 的「实体」在装配期
由 generated installer 表示，在运行期由 `(namespace, service)` 元数据表示。

> 可选增强（不在本期范围）：`impl_of` 额外生成一个静态的
> `RpcServiceDef { namespace, service, operations: &[RpcOperationDef] }`，用于内省、
> 文档生成、测试「trait 方法数 = 端点数」。它不需要参与路由，也不改变本节的关联模型；
> 若团队需要，再加。

#### 3.10.4 注册单元：一个 installer = 一个 service 的全部访问点

`install_into(router, Arc<Impl>)` 是唯一（宏生成的）服务端注册入口：

- 一次绑定该 service 的全部 operation endpoint；
- 消费式 `RadixRouter`：任一端点绑定失败（pattern 冲突 / 非法）就返回 `Err`，router 被
  drop，**不会留下半个 service**；
- 同一个 `Arc<Impl>` 被所有 operation handler 共享，因此 trait 方法看到的是同一份
  业务状态（与 legacy 的 service 实例语义一致）。

同一 impl + trait 注册到多个 interface 时，每个 interface 生成一个独立 installer 和
一组 endpoint；`NAMESPACE` 不同，`(namespace, service)` 不同，互不覆盖：

```rust
let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV1::install_into(router, ws.clone())?;
let router = __rpc_impl_of_WorkspaceServiceImpl_via_ApiV2::install_into(router, ws)?;
```

同一 impl 实现多个 trait 时，每个 trait 是一个独立 service（不同的 `SERVICE_NAME`），
各自 installer；它们可以共享同一个 `Arc<Impl>`，也可以各自构造。

#### 3.10.5 客户端：调用点与访问点同源

`client.call::<ApiV1>()` 返回的 `ClientProxy` 实现该 interface 下的每个 service trait；
每个代理方法体把 `(SERVICE_NAME, OPERATION)` 作为相对路由键交给
`call_unary_async`，`NAMESPACE` 由 `I` 提供。因此：

```text
WorkspaceService::create_workspace  ──►  api/v1/workspace/create_workspace
WorkspaceService::list_workspaces   ──►  api/v1/workspace/list_workspaces
```

调用点是 trait 方法，访问点是 pattern；两者由同一份宏输入推导，不需要手写映射表。

#### 3.10.6 为什么不是「一个 service 一条路由 + body 选方法」

| 维度 | service 级单访问点 | 方法级访问点（本方案） |
| --- | --- | --- |
| trait ↔ 路由 | 1 : 1 | 1 : N（按 `(namespace, service)` 分组） |
| 方法选择 | body enum 变体 | pattern 最后一段 |
| 中间件看到的 operation | 解码 body 之后才知道 | 进入 handler 前就知道 |
| 每方法 metadata / 405 / 类型校验 | 做不了或只能宽匹配 | 每端点独立 |
| 路由匹配依赖 body | 是（必须先解码） | 否 |
| 与 controller / 手写端点共存 | 需要独立分派器 | 同一 `RadixRouter` |

service 级单访问点的唯一优点是「trait 与路由一一对应、关系简单」；代价是重新引入
「路由之外的第二层分派器」，这正是重构已经删除的模型。ASP.NET Core 的 code-first 也
是 service → 多个 method endpoint，而不是 service 一个 endpoint（§2.4）。

#### 3.10.7 冲突与命名

- 同一个 trait 内方法名唯一（Rust 保证），因此 operation 不会重复；
- 两个 trait 使用相同 `SERVICE_NAME` + 相同 `NAMESPACE` 且方法同名时，pattern 会冲突；
  当前 `RadixRouter::bind_endpoint` 不报冲突（同 pattern 同方法先到先得），需要文档约定
  「同一 namespace 下 service 名唯一」，或在测试 / 装配层做一次 pattern 唯一性检查；
- 同一 impl 的同一 trait 挂到不同 namespace 是合法的，不构成冲突；
- 重命名 trait 方法 = 改 operation 段 = 改线上路由；本次不引入
  `#[rpc(name = "...")]`，需要稳定路由时应在 trait 设计阶段固定方法名。

---

## 4. 将要实现的形状（样式代码）

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
| `rpc_core_v1::serving::RpcEndpointInfo { service, operation, pattern }` | 新增 / 去 `namespace` | 决策 4 + 8；方法级元数据 |
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

以下编号 `CC-xx` 是本轮新增的 Call 验收口径；`mptp_im_demo/ACCEPTANCE.md` §6.6 使用
同一组编号（`C-xx` 已被客户端测试占用）。

| ID | 验收项 | 判定 |
| --- | --- | --- |
| CC-01 | 语法：`#[rpc(service = "im/v1/room", api = RoomSvc)] pub trait RoomService` 可编译 | 生成 `RoomSvc`；trait 原方法、`__Req_*` 都在；不再生成 `SERVICE_NAME` |
| CC-02 | marker：`RoomSvc` 是零大小、可见性与 trait 一致、实现 `TrRpcInterface::PREFIX = "im/v1/room"` | 类型检查；`RoomSvc::PREFIX` 断言 |
| CC-03 | 客户端挂载：`client.call::<RoomSvc>()` 返回 `ClientProxy<'_, C, RoomSvc>`，且实现 `RoomService` | 编译 + 调用 `create_room` |
| CC-04 | 调用点不出现 `ImV1` / pattern / 前缀 / body 编解码 | 源码检查；`CC-01` 示例逐字通过 |
| CC-05 | 服务端：`#[rpc(impl_of(RoomSvc))]` 生成 per-method endpoint；pattern = `im/v1/room/<operation>` | router 命中；`RpcEndpointInfo { service, operation, pattern }` 正确 |
| CC-06 | 请求类型：0 参 → `()`；1 参 → 该 owned 类型；≥2 参 → `__Req_<op> { f0.. }`；借用参数回填 | 编译 + 编解码 roundtrip；`__Req_*` 实现 `TrHasTypeId` |
| CC-07 | 类型头：请求 `Body_Type` + `Data_Type_Id=Req` + `Accept=Resp`；响应 `Body_Type` + `Data_Type_Id=Resp`；strict | 正确头通过；缺失 / 错误在 handler 前 400/406 |
| CC-08 | 跨 crate：`im_client` 只依赖协议 crate；`im_server` 只依赖协议 + `mptp_serving` | `cargo tree -p im_client --depth 1` 无 `im_server` |
| CC-09 | 多 service：`SessionSvc` / `RoomSvc` / `DiagSvc` 各自挂载、前缀可以共享 `im/v1`；不需要 interface marker | 三个 `client.call::<...>()` 都能调；pattern 不冲突 |
| CC-10 | 错误 marker：在 `RoomSvc` 代理上调用 `SessionService` 方法编译失败 | `trybuild` / `compile_fail` |
| CC-11 | 名字冲突：`api = X` 与已有类型同名时给出可读编译错误 | compile-fail 用例 |
| CC-12 | 取消：每个客户端方法返回 `impl TrMayCancel`，可 `.may_cancel_with(token)` | 编译 + 取消用例 |
| CC-13 | 清单：生成的 `__Req_*` 进入 `denco_type_ids!` / golden；客户端 / 服务端不手写 ID | 清单长度 + golden |
| CC-14 | installer：每个 `impl_of` 只生成一个 `install_into`；同一 `Arc` 被所有方法共享 | 装配测试 |
| CC-15 | 多前缀（`DiagV1` / `DiagV2`）：另立决策；本期只要求单组 `service` + `api` 主路径 | 设计决策记录；不阻塞 `CC-01..CC-14` |

### 11.7 风险

- **marker 名字冲突**：`api = X` 与同模块已有类型同名时，宏应报「生成类型 X 与现有
  类型冲突」；默认不自动改名，避免客户端调用点漂移。
- **可见性**：`pub trait` 在私有模块里时，marker 也私有，客户端 crate 无法
  `call::<X>()`；文档要求协议 trait 与 marker 放在 `pub` 模块。
- **语法歧义**：裸 ident 形式与未来参数（如 `typed`）容易冲突；推荐 `api = X`。
- **公开 API 变更**：`TrRpcInterface::PATH` → `PREFIX`、`RpcEndpointInfo` 去
  `namespace`，需要更新 examples / tests / README；code-first 尚未实现，现在改成本最低。
- **多前缀**：见 11.5；不要把多个前缀硬塞进单 marker。
- **重复前缀**：每个 trait 重复写 `im/v1`；接受这个重复，换掉 namespace 的一层概念。
