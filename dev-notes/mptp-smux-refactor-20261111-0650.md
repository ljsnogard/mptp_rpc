# `mptp_smux` 重构设计：七种 AccessMethod 标准实现 + `no_std` + feature 边界

- 日期：2026-11-11 07:30（根据评审意见修订）
- 状态：**设计草案 v2，待公开 API 破坏性变更清单确认后实施**
- 基线：分支 `feat/mptp_wire`，提交 `3a7beb8`；当前工作分支
  `feat/mptp-smux-refactor`
- 范围：`mptp_smux` 的客户端 / 服务端标准 API、错误类型、`no_std` 边界、
  `client` / `server` feature，以及会话状态机与 `mptp_quic` 的边界
- 不在范围：`mptp_wire` 的线上格式、RPC service trait / `#[rpc]` 宏、业务 body 编解码、
  通道内请求复用、运行时选择、多连接资源管理

相关文档：

- [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md) §2、§5、§8–§13
- [`mptp-smux-20261110-0730.md`](mptp-smux-20261110-0730.md) §4、§6、§14
- [`router-20261106-0112.md`](router-20261106-0112.md)
- [`../DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)
- [`../mptp_smux/README.md`](../mptp_smux/README.md)
- [`../mptp_wire/README.md`](../mptp_wire/README.md)

## 0. 评审后的结论摘要

1. **`serving` 模块不再包含任何 router API。** `Router` / `RouterCtx` / `RouterError` /
   `TrPathRouter` / `TrPathHandler` / `TrPathRouterMut` / `RouteValues` /
   `TrPathRouterList` 全部移出 `mptp_smux`；`matchit_route` / `tlist` 不再是本 crate
   的依赖。路由是应用策略，迁到下游（首选 `rpc_core_v1::serving`）。
2. **`MptpServer` 改名为 `MptpServlet`。** 它只关注**一条 MPTP 连接**上的入向子流，
   不负责多连接、监听拓扑或服务器级资源管理。它持有一个共享连接与**单一 handler**；
   下游在这个单一 handler 上自行实现路由、鉴权、分派。
3. **服务端与客户端共用会话核心。** `MptpSession` 仍是客户端一次请求 / 回复的对象；
   服务端新增 `ServingSession<C>`，与客户端共享 `SessionCore<C>`、体边界判定、
   体读视图与收尾原语。一条连接上可以并行持有多个 `MptpSession` /
   `ServingSession`；`mptp_smux` 的职责是把这些 session 映射到 smux 子流上，
   不替调用者 spawn。
4. **客户端仍采用 per-method 方法糖 + 统一 `RequestBuilder` / `Response`。**
   方法糖只提供可发现入口与默认语义；实际有无体仍由 `Body_Size` / `Body_Transfer`
   唯一决定。
5. **`Head` / `Drop` 的回复体校验在服务端 `ResponseBuilder` 与客户端
   `MptpSession::recv_resp_prefix_async` 两处同时做。** 服务端不写，客户端拒读。
6. **handler 未消费的请求体 / 后缀由框架收尾时代读丢弃（drain），不主动 reset。**
   只有 drain 自身失败或 handler 显式拿走两半才走 reset / 自定义收尾。
7. **`Allow` 头临时使用用户自定义键 `0x009E`**，文本形态的方法名列表；协议分配
   标准键后替换常量，不改变上层 API。
8. **本次不新建 `mptp_session` crate**，但按「客户端 / 服务端共享 `SessionCore`」组织，
   为后续抽取铺路；`mptp_quic` 不会依赖 `mptp_smux`，也不会复制一份状态机。
9. **`#![no_std]` + `alloc` 与 feature 拆分一起实施**；移除 `futures`、
   `std::sync::OnceLock`，`thiserror` 关闭默认 feature，`MptpSession` 中的
   `format!` 全部改为结构化错误变体。
10. **公开 API 有破坏性变更**，集中列在第 11 节；确认后按第 12 节分阶段提交。

## 1. 调整后的职责边界

### 1.1 `mptp_smux` 负责

- 把 MPTP 的一次请求 / 回复映射到一条 smux 子流；
- 一条连接上多条子流的 open / accept / ring 注入 / 半关闭 / reset；
- 一次 session 内的协议顺序：前缀 → 体 → 后缀 → 半关闭 → 回复；
- 客户端 `MptpClient` / `MptpSession`；
- 服务端 `MptpServlet` / `ServingSession` / 单一 `TrServeHandler`；
- 请求头 / 回复头的读取与构造 helper；
- 取消传播与错误分类；
- 收尾（先半关闭写端，再读尽对端）。

### 1.2 `mptp_smux` 不负责

- 应用路由、路径匹配、interface / service 分派；
- 多连接监听、连接池、负载均衡、服务器级资源管理；
- RPC service trait、业务 body 编解码；
- 运行时选择、spawn、socket / TLS；
- 通道内请求复用（一条通道一次请求）。

### 1.3 与上下游的关系

```text
应用 / RPC：业务类型、路由、service、连接管理
  rpc_core_v1 / abs_rpc_serv / 应用代码
        │  使用 MptpServlet + TrServeHandler；路由在应用层
        ▼
MPTP over smux：单连接、多子流、一次请求的会话
  mptp_smux：MptpClient / MptpSession / MptpServlet / ServingSession
        │  使用前缀 / 体格式
        ▼
mptp_wire：前缀、头表、限长体、分块体
```

## 2. 客户端标准 API

### 2.1 取舍：per-method 方法糖 + 统一 builder

| 方案 | 优点 | 缺点 |
| --- | --- | --- |
| 只有 per-method 方法糖 | 可发现性最好 | 七份实现容易漂移，escape hatch 仍要另开 |
| 只有统一 builder | 一份状态机、体积小 | `client.request(Method::View, ...)` 不够直观 |
| **两者并存（选定）** | 方法糖只是薄封装，语义集中在 builder | 多一层类型 |

```rust
impl<C: client::config::TrConfig> MptpClient<C> {
    /// 统一入口：显式方法 + 位置。
    pub fn request(&self, method: AccessMethod, location: impl Into<String>)
        -> RequestBuilder<'_, C>;

    /// 七种方法糖；都返回同一个 RequestBuilder。
    pub fn head(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn view(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn post(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn drop(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn push(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn pull(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;
    pub fn call(&self, location: impl Into<String>) -> RequestBuilder<'_, C>;

    /// escape hatch：完全自定义前缀（含头、方法、位置）。
    pub fn request_prefix(&self, prefix: RequestPrefix) -> RequestBuilder<'_, C>;

    /// 原有的两条入口保持不变。
    pub fn request_to_dock_async<'f>(&'f self, dock: Dock<C>, request: ReqPrefix<C>)
        -> ClientRequestToDockAsync<'f, 'f, C>;
    pub fn request_to_dock_with_prep_async<...>(...) -> ...;
}
```

`RequestBuilder<'a, C>` 持有 `&MptpClient<C>`、`RequestPrefix` 与可选目标 dock。
`.dock(dock)` 覆盖目标垛口；缺省取 `C::default_serving_dock()`。

### 2.2 体与后缀的显式声明

| 入口 | 自动写入的头 | 说明 |
| --- | --- | --- |
| `.body_bytes(&'a [u8])` | `Body_Size: <len>` | 定长；长度 0 时不写头（无体） |
| `.body_sized(&'a mut R, size)` | `Body_Size: size` | 调用者保证 `R` 至少给出 `size` 字节 |
| `.body_chunked(&'a mut R)` | `Body_Transfer: Chunked` | 调用者提供完整分块体源 |
| `.body_raw()` | 不写 | 前缀已带体声明；发送阶段按前缀唯一决定 |
| `.suffix(&'a mut R)` | 不写（后缀无头） | `Push` / `Call` 的请求后缀 |

这些入口**显式覆盖**头：`body_bytes` / `body_sized` 先移除 `Body_Transfer` 再写
`Body_Size`；`body_chunked` 反之。`body_raw` 完全不动头。

类型状态：

```rust
pub struct RequestBuilder<'a, C, B = NoBody, S = NoSuffix> {
    client_: &'a MptpClient<C>,
    prefix_: RequestPrefix,
    dock_: Option<Dock<C>>,
    body_: B,
    suffix_: S,
}

pub struct NoBody;
pub struct SizedBody<'a, R> { src_: &'a mut R, size_: usize }
pub struct ChunkedBody<'a, R> { src_: &'a mut R }
pub struct NoSuffix;
pub struct Suffix<'a, R> { src_: &'a mut R }
```

`.body_*` / `.suffix` 改变类型参数，`send_async(self)` 是唯一终结入口：

```text
1. 目标垛口 = dock_.unwrap_or(C::default_serving_dock())
2. client.request_to_dock_async(dock, prefix) -> MptpSession
3. 若 body 有：session.send_req_body_async(body)
4. 若 suffix 有：session.send_req_suffix_async(suffix)
5. session.close_req_async()       // 半关闭写端
6. session.recv_resp_prefix_async() -> ResponsePrefix
7. 返回 Response<C>（拥有 session + prefix）
```

`close_req_async` 是本次给 `MptpSession` 增加的方法：把内部发送半边取出并 drop，
保留接收半边继续读回复。它对应协议 §12.1 的「半关闭写方向」。

### 2.3 回复对象

```rust
pub struct Response<C: TrConfig> {
    session_: MptpSession<C>,
    prefix_: ResponsePrefix,
}

impl<C: TrConfig> Response<C> {
    pub fn status(&self) -> Status;
    pub fn headers(&self) -> Option<&Headers>;
    pub fn prefix(&self) -> &ResponsePrefix;

    /// 按回复前缀声明的体边界取读视图；Head / Drop 声明了体会在这里被拒绝。
    pub fn body_async(&mut self) -> ResponseBodyAsync<'_, C>;

    /// 先丢弃未读完的回复体，再把接收半边当后缀流交出去。
    pub fn suffix_async(&mut self) -> ResponseSuffixAsync<'_, C>;

    /// escape hatch：拿回会话，自己按旧原语驱动。
    pub fn into_session(self) -> MptpSession<C>;
}
```

`body_async` / `suffix_async` 返回的视图借用 `&mut self`，因此不能在持有体视图时
调用 `suffix_async`；这正好把协议约束 4（后缀流前必须处理完体）交给借用检查。

示例：

```rust
let mut resp = client
    .post("/api/v1/goods")
    .header(StdHeaderKey::Data_Type_Id, type_id_text)
    .body_bytes(&encoded)
    .send_async()
    .may_cancel_with(token)
    .await?;

if resp.status() != Status::Ok {
    return Err(...);
}
let mut body = resp.body_async().await?;
let value = decode(&mut body).await?;
```

```rust
let mut resp = client.pull("/stream/logs").body_bytes(&query).send_async().await?;
let mut body = resp.body_async().await?;
// ... 读回复体 ...
drop(body);
let mut suffix = resp.suffix_async().await?;
// ... 持续读服务端推送的裸字节 ...
```

## 3. 服务端标准 API

### 3.1 `MptpServlet`：单连接 + 单 handler

```rust
pub struct MptpServlet<C, H>
where
    C: serving::config::TrConfig,
    H: TrServeHandler<C>,
{
    /// 本条 MPTP 连接；`MptpServlet` 只服务这一条连接。
    conn_: SharedConn<C>,

    /// 单一 handler；路由 / 鉴权 / 方法分派都在它内部。
    handler_: H,
}

impl<C, H> MptpServlet<C, H> {
    pub const fn new(conn: SharedConn<C>, handler: H) -> Self;
    pub fn conn(&self) -> &SharedConn<C>;
    pub fn handler(&self) -> &H;

    /// 在 binding 上进入监听；返回上游 listener。
    pub fn listen_async<'f>(&'f self, binding: &'f mut DockBinding<C>, reserve: usize)
        -> ListenAsync<'f, 'f, C>;

    /// 接受一条入向子流：income → ring → accept → 读请求前缀。
    /// 返回的 session 可以并发处理；本方法不 spawn。
    pub fn accept_async<'f>(&'f self, listener: &'f mut Listener<C>)
        -> AcceptAsync<'f, 'f, C>;

    /// 用单一 handler 处理一条 session：方法检查、handler、兜底回复、收尾。
    pub fn serve_session_async<'f>(&'f self, session: ServingSession<C>)
        -> ServeSessionAsync<'f, 'f, C, H>;

    /// 便捷：在当前 binding 上循环 accept 并 inline 处理。
    /// 需要并发时用 `accept_async` + 调用者自己的 spawner。
    pub fn serve_listener_async<'f>(&'f self, binding: &'f mut DockBinding<C>, reserve: usize)
        -> ServeListenerAsync<'f, 'f, C, H>;
}
```

`MptpServlet` **不持有 listener / binding**，也不管理多连接。调用者可以先 bind 多个
dock、把每个 binding 的 listener 交给 `accept_async`，再把返回的 `ServingSession`
并发交给 `serve_session_async`。

### 3.2 单一 handler 契约

```rust
pub trait TrServeHandler<C>
where
    C: serving::config::TrConfig,
{
    type Err: core::error::Error;

    /// 本 handler 支持的方法集合；缺省为全部。
    /// 框架在调用 handle 前检查；不在集合内直接写 405 + Allow，不进入 handler。
    fn allowed_methods(&self) -> MethodSet {
        MethodSet::ALL
    }

    type HandleAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>
    where
        Self: 'f;

    /// 一次请求的处理入口；`session` 提供方法、位置、头表、请求体 / 后缀读取与
    /// 回复 helper。handler 不需要自己收尾：返回后框架会兜底并 finish。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<C>,
    ) -> Self::HandleAsync<'f>;
}
```

**没有 `TrTargetResolver`、没有 `SingleTarget`、没有 `RootedResolver`。**
404 由 handler 自己写（例如 `session.response(Status::NotFound).no_body_async()`），
因为只有 handler 知道自己的路由策略。405 由框架根据 `allowed_methods` 在调用前判，
也可以由 handler 自己再细分。

旧 `TrReqHandler` 通过适配器继续可用：

```rust
pub struct LegacyHandler<H> { handler_: H }

impl<C, H> TrServeHandler<C> for LegacyHandler<H>
where
    C: serving::config::TrConfig,
    H: TrReqHandler<C>,
{
    type Err = H::Err;
    // handle_async: session.take_parts() -> 按值交给旧 handler；旧 handler 自行收尾。
}
```

`LegacyHandler` 的 `allowed_methods` 为 `MethodSet::ALL`；它不提供体 / 后缀 helper，
因为旧 handler 直接拿 `ChannelTx` / `ChannelRx`。

### 3.3 `ServingSession`：服务端会话对象

```rust
pub struct ServingSession<C: serving::config::TrConfig> {
    core_: SessionCore<C>,
    method_: AccessMethod,
    location_: String,
    headers_: Headers,
    state_: ServingState,
    responded_: bool,
}

impl<C: serving::config::TrConfig> ServingSession<C> {
    pub fn method(&self) -> AccessMethod;
    pub fn location(&self) -> &str;
    pub fn headers(&self) -> &Headers;
    pub fn headers_mut(&mut self) -> &mut Headers;

    /// 按请求前缀声明的体边界取读视图。
    pub fn req_body_async(&mut self) -> RequestBodyAsync<'_, C>;

    /// 丢弃未读完的请求体，然后把接收半边当裸后缀流交出去。
    pub fn req_suffix_async(&mut self) -> RequestSuffixAsync<'_, C>;

    /// 构造回复 helper；终结时才写前缀。
    pub fn response(&mut self, status: Status) -> ResponseBuilder<'_, C>;
    pub fn response_prefix(&mut self, prefix: ResponsePrefix) -> ResponseBuilder<'_, C>;

    /// 收尾：丢弃剩余请求体 / 后缀、半关闭写端、读尽对端。
    /// 返回丢弃的请求字节数。
    pub fn finish_async(self) -> FinishSessionAsync<C>;

    /// 已经写出的回复前缀（若有）。
    pub fn response_prefix_sent(&self) -> Option<&ResponsePrefix>;

    /// escape hatch：拿走两个半边与头表，自己驱动；之后框架不做兜底收尾。
    pub fn take_parts(&mut self)
        -> Option<(ChannelTx<C>, ChannelRx<C>, Headers, String)>;
}
```

`ServingSession` 与客户端 `MptpSession` 共享：

- `SessionCore<C>`：两个半边、请求前缀、体边界 / 体进度、状态；
- `BodyPlan` / `BodyState`；
- `BodyRead` 体视图（客户端用于回复体，服务端用于请求体）；
- `drain_body_async` / `drain_to_eof_async`；
- 半关闭与收尾顺序。

差异只在协议方向：

| 能力 | `MptpSession`（客户端） | `ServingSession`（服务端） |
| --- | --- | --- |
| 请求前缀 | 写出 | 已读入 |
| 请求体 | 写 tx | 读 rx |
| 请求后缀 | 写 tx | 读 rx |
| 回复前缀 | 读 rx | 写 tx |
| 回复体 | 读 rx | 写 tx |
| 回复后缀 | 读 rx | 写 tx |
| 状态枚举 | `SessionState` | `ServingState`（镜像） |

### 3.4 并发模型

一条连接上可以并行存在多个 session：

```text
MptpClient ── request_to_dock_async ──▶ MptpSession A ──▶ Response A
           ├ request_to_dock_async ──▶ MptpSession B ──▶ Response B
           └ request_to_dock_async ──▶ MptpSession C ──▶ Response C

MptpServlet ── accept_async ──▶ ServingSession A ──▶ spawn(serve_session_async(A))
            ├ accept_async ──▶ ServingSession B ──▶ spawn(serve_session_async(B))
            └ accept_async ──▶ ServingSession C ──▶ spawn(serve_session_async(C))
```

`mptp_smux` 不选运行时、不 spawn：`accept_async` 返回可移动的 `ServingSession`，
`serve_session_async` 消费它；调用者用自己的 executor / spawner 并发处理。
`serve_listener_async` 只是「accept + inline serve」的便捷循环，适合单线程 demo；
真正并发用 `accept_async` + spawn。

### 3.5 回复 helper

```rust
pub struct ResponseBuilder<'a, C: serving::config::TrConfig> {
    session_: &'a mut ServingSession<C>,
    prefix_: ResponsePrefix,
}

impl<'a, C> ResponseBuilder<'a, C> {
    pub fn header(&mut self, key: impl Into<HeaderKey>, val: impl Into<HeaderVal>) -> &mut Self;

    /// 状态码 + 头 + 无体；若前缀/方法声明了体，返回协议错误。
    pub fn no_body_async(self) -> ResponseNoBodyAsync<'a, C>;

    /// 定长回复体：自动写 `Body_Size`，再按长度搬运。
    pub fn sized_async<R>(self, body: &'a mut R, size: usize) -> ResponseSizedAsync<'a, C, R>
    where R: TrBuffRead<u8> + TrConsumerState;

    /// 分块回复体：自动写 `Body_Transfer: Chunked`，再逐块编码。
    pub fn chunked_async<R>(self, body: &'a mut R) -> ResponseChunkedAsync<'a, C, R>
    where R: TrBuffRead<u8> + TrConsumerState;

    /// 回复体之后的裸后缀流（`Pull` / `Call`）。
    pub fn suffix_async<R>(self, suffix: &'a mut R) -> ResponseSuffixAsync<'a, C, R>
    where R: TrBuffRead<u8> + TrConsumerState;
}
```

终结语义：

1. **`Head` / `Drop`**：`sized_async` / `chunked_async` / `suffix_async` 直接返回
   `ServeError::Protocol(ProtocolError::ResponseBodyOnHeadOrDrop { method })`；
   `no_body_async` 若发现 `prefix_` 里已有 `Body_Size` / `Body_Transfer`，同样返回
   该错误。客户端会话读到回复前缀时会再校验一次，形成两端双保险。
2. `sized_async` 覆盖前缀里的 `Body_Size`（按实参 `size`），并移除
   `Body_Transfer`；`chunked_async` 反之。显式设置冲突头时以 helper 实参为准，
   避免「头声明 10、实际写 20」的错位。
3. 终结方法只写回复前缀 / 体 / 后缀，**不 drop tx**；收尾由
   `ServingSession::finish_async` 统一完成。这样 `Call` 可以在写回复后继续读请求
   后缀。
4. `serve_session_async` 的顺序：

   ```text
   allowed_methods 检查 -> 405 + Allow（未允许，直接 finish）
   handler.handle_async(method, &mut session)
     Ok  -> 若未回复，写 500 无体
     Err -> 若未回复，写 500 无体；返回 ServeError::Target
   session.finish_async()
   ```

5. handler 通过 `take_parts` 拿走两半：框架不再写兜底、不再 finish，由 handler
   自行收尾（这是 escape hatch，不是默认路径）。

### 3.6 请求体 / 后缀未消费时的语义

**选定：代读丢弃（drain），不 reset。**

理由：

- 协议 §13.2 明确「框架在收尾时会把剩余请求数据读尽，避免提前重置导致在途响应丢失」；
- 先 reset 会让已经写出的回复被下层抹掉，对端只看到 EOF；
- 请求体 / 后缀在收尾阶段已经无人消费，读进栈上暂存再丢弃即可。

规则：

1. handler 读过一部分体就放下视图：视图 Drop 时把剩余额度写回 `ServingSession`；
2. `finish_async` 先用剩余额度把体读尽，继续读到 EOF；
3. `Push` 的请求后缀如果 handler 没读，同样在收尾阶段被丢弃；
4. drain 自身失败（非 `Closing`）返回 `ServeError::Transport`，由调用方决定是否
   reset / 关闭；框架不把失败伪装成成功；
5. handler 想主动 reset，用 `take_parts` 拿走两半自行处理。

### 3.7 服务端示例

```rust
struct EchoHandler;

impl TrServeHandler<ServingCfg> for EchoHandler {
    type Err = HandlerError;

    fn allowed_methods(&self) -> MethodSet {
        MethodSet::CALL | MethodSet::POST | MethodSet::VIEW
    }

    type HandleAsync<'f> = HandleEchoAsync<'f> where Self: 'f;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        session: &'f mut ServingSession<ServingCfg>,
    ) -> Self::HandleAsync<'f> {
        HandleEchoAsync::new(self, method, session)
    }
}

async fn handle_echo_async_(
    method: AccessMethod,
    session: &mut ServingSession<ServingCfg>,
) -> Result<(), HandlerError> {
    match method {
        AccessMethod::View | AccessMethod::Call => {
            let mut body = session.req_body_async().await?;
            // ... 读取 / 解码 ...
            let mut out = build_reply()?;
            session
                .response(Status::Ok)
                .sized_async(&mut out, out.len())
                .await?;
        }
        _ => {
            session.response(Status::NotImplemented).no_body_async().await?;
        }
    }
    Ok(())
}
```

装配：

```rust
let servlet = MptpServlet::<ServingCfg, EchoHandler>::new(conn, EchoHandler);
let mut listener = servlet.listen_async(&mut binding, 8).await?;

loop {
    let session = servlet.accept_async(&mut listener).await?;
    // 单线程 demo：inline；真实并发：spawn(servlet.serve_session_async(session))
    servlet.serve_session_async(session).await?;
}
```

## 4. 七种方法的默认语义

| 方法 | 请求体 | 回复体 | 后缀流 | 客户端方法糖 | 服务端 helper |
| --- | --- | --- | --- | --- | --- |
| `Head` | 不应有 | 不应有 | 无 | `head()` | 只允许 `no_body_async` |
| `View` | 不应有 | 通常有 | 无 | `view()` | `sized_async` / `chunked_async` |
| `Post` | 通常有 | 可选 | 无 | `post()` | 任意无体 / 体 helper |
| `Drop` | 不应有 | 不应有 | 无 | `drop()` | 只允许 `no_body_async` |
| `Push` | 可选 | 可选 | 客户端 → 服务端，必需 | `push()` + `.suffix()` | `req_suffix_async` 读请求后缀 |
| `Pull` | 可选 | 可选 | 服务端 → 客户端，成功时必需 | `pull()` | `suffix_async` 写回复后缀 |
| `Call` | 通常有 | 通常有 | 可选，双向 | `call()` + `.suffix()` | `req_suffix_async` / `suffix_async` |

**关键纪律：方法糖不能绕过协议头。**

- 方法糖只提供入口与默认语义；实际有无体仍由 `Body_Size` / `Body_Transfer`
  唯一决定；
- `head` / `drop` 的客户端 builder 仍提供 `.body_*`（escape hatch 与显式覆盖是要求
  的一部分），但文档明确这与方法意图不符；框架只对**回复体**做强校验；
- `head` / `drop` 的回复体是唯一做强校验的地方：服务端不写，客户端拒读；
- `Push` / `Pull` 的后缀方向由方法糖的可用入口表达：`Push` builder 提供 `.suffix`，
  `Pull` builder 不提供 `.suffix`（用 `request(AccessMethod::Pull, ...)` 可绕过）；
  服务端不按方法禁止 `suffix_async`，由 handler 自己按 `method` 决定。

## 5. 会话核心与 `mptp_quic` 边界

### 5.1 本次不新建 `mptp_session`，但先共享 `SessionCore`

`MptpSession` 的「前缀 → 体 → 后缀 → 半关闭」不是 smux 特有。本次：

- 把客户端 `MptpSession` 与服务端 `ServingSession` 的共同部分收敛到
  `SessionCore<C>`（通道、请求前缀、体边界、体进度、收尾）；
- 客户端 / 服务端各自保留方向性的方法（写请求 vs 读请求）；
- 体视图 `BodyRead<'f, C>` 泛型于接收半边，客户端别名 `RespBodyRead`、服务端别名
  `ReqBodyRead`；
- 不新增 crate，不改变 `mptp_wire`。

理由：抽 `mptp_session` 需要先定义「半关闭」的抽象 trait，并把 `ChannelTx` /
`ChannelRx` 包成 newtype（否则孤儿规则不允许 `mptp_smux` 为外部类型实现
`mptp_session` 的 trait）；这是另一项跨 crate 改造。本次先交付七方法、`no_std`、
feature 拆分，避免四重风险叠加。

### 5.2 `mptp_session` 抽取接口草案（后续）

```rust
// 未来的 mptp_session
pub trait TrSessionIo {
    type Tx: TrBuffWrite<u8> + TrProducerState;
    type Rx: TrBuffRead<u8> + TrConsumerState;
    type Err: core::error::Error;

    fn split(self) -> (Self::Tx, Self::Rx);
    fn half_close(tx: Self::Tx);       // smux: drop；QUIC: finish
    fn reset(tx: Self::Tx, code: u32); // smux: RESET；QUIC: RESET_STREAM
}
```

`mptp_quic` 届时提供自己的 `TrSessionIo`，复用 `mptp_session` 的
`ClientSession` / `ServingSession`；`mptp_smux` 只剩 smux 绑定、ring 注入、dock
映射、错误映射与收尾。

### 5.3 `mptp_smux` 最终应只剩

- smux 绑定：bind / open / listen / income / accept；
- ring 注入：`TrPrepFactory` / `ClientPrepCtx` / `ServingPrepCtx` / `RingBuffs`；
- dock 映射；
- 客户端 / 服务端包装：`MptpClient` / `MptpServlet`；
- 取消与错误映射；
- 半关闭 / reset 的 smux 映射。

## 6. 错误类型

### 6.1 客户端

```rust
#[derive(Debug)]
pub enum ClientError {
    Cancelled,
    Transport(TransportError),
    Protocol(ProtocolError),
    State(StateError),
}
```

```rust
#[derive(Debug)]
pub enum TransportError {
    Bind { source: String },
    OpenChannel { source: String },
    Accept { source: String },
    RingPrepare { source: String },
    Write { source: String },
    Read { source: String },
}

#[derive(Debug)]
pub enum ProtocolError {
    PrefixDecode(MessagingError),
    InvalidBodySize { text: String },
    RequestBodyTruncated { declared: usize, read: usize },
    RequestBodyRead { source: String },
    RequestBodyWrite { source: String },
    ResponseBodyRead { source: String },
    ResponsePrefixDecode(MessagingError),
    ResponseBodyOnHeadOrDrop { method: AccessMethod },
    SuffixRead { source: String },
    SuffixWrite { source: String },
    DrainBody { source: String },
}

#[derive(Debug)]
pub enum StateError {
    ExpectSendingBody { actual: SessionState },
    ExpectWaitingResp { actual: SessionState },
    ExpectPrefixReady { actual: SessionState },
    ExpectBodyOrPrefixReady { actual: SessionState },
    NoRequestPrefix,
    BodyNotDeclared,
    AlreadyResponded,
}
```

实现 `Display` + `core::error::Error`（手写或 `thiserror` 关闭默认 feature）。

**source 保留策略**：协议错误里的底层 `MessagingError` / `ChunkedReadError` /
`LimitedReaderError` 能保留具体类型的地方直接保留；传输错误的具体类型是配置关联
类型（泛型），非泛型 `ClientError` 无法命名它，因此用 `source: String` 保存描述，
同时用变体名保留「哪一步失败」。如果未来需要 typed source，可把 `ClientError`
泛型化为 `ClientError<TyTransport>`，不作为本次目标。

### 6.2 服务端

```rust
pub enum ServeError {
    Cancelled,
    Transport(TransportError),
    Protocol(ProtocolError),
    State(StateError),
    Target { source: String },          // handler 报错
    NotAllowed { method: AccessMethod, allow: MethodSet },
}
```

**没有 `NotFound`**：404 是 handler 的业务响应，不是框架错误；handler 用
`session.response(Status::NotFound).no_body_async()` 写它。

### 6.3 与 `SessionState` / `ServingState` 的关系

- `ClientError::State(StateError)` 携带 `actual: SessionState`；
- `ServeError::State(StateError)` 携带服务端状态；
- 所有 `format!` 从会话实现删除；需要文本时在 `Display` 里用 `write!` 组合字段。

## 7. `Allow` 头的临时方案

协议尚未给 `Allow` 分配标准头键。临时方案：

```rust
/// 临时 `Allow` 头键：用户自定义区最高位，协议分配标准键后替换。
pub const K_TMP_ALLOW_HEADER_KEY: HeaderKey = HeaderKey::from_num(0x009E);

/// 值形态：逗号分隔的方法名，全大写，顺序固定。
/// 例：`"HEAD,VIEW,POST,CALL"`。
pub const K_TMP_ALLOW_HEADER_NAME: &str = "allow";
```

- `MethodSet::to_allow_value()` 生成文本；
- `MethodSet::from_allow_value(&str)` 解析，未知项忽略；
- 405 响应写 `K_TMP_ALLOW_HEADER_KEY: K_TMP_ALLOW_HEADER_NAME` 的文本值；
- 协议分配标准键后，只需替换这两个常量与文档，`MethodSet` 与上层 API 不变。

## 8. Router 迁出 `mptp_smux`

### 8.1 `mptp_smux` 删除的 API

- `serving::Router` / `RouterCtx` / `RouterError`；
- `serving::TrPathRouter` / `TrPathHandler` / `TrPathRouterMut`；
- `serving::{RouteValues, TrPathRouterList}`；
- `serving::{EitherMayCancel, MapErrMayCancel, ReadyMayCancel}`（router 分派专用）；
- `matchit_route` / `tlist` 依赖。

### 8.2 迁移目标

| 原位置 | 新位置 | 说明 |
| --- | --- | --- |
| `mptp_smux::serving::Router` | `rpc_core_v1::serving::Router` | RPC 层顶层路由；保持路径前缀匹配 |
| `RouterCtx` / `RouterError` | `rpc_core_v1::serving` | 同上 |
| `TrPathRouter` / `TrPathHandler` | `rpc_core_v1::serving` | RPC 层可选接缝 |
| `EitherMayCancel` 等 | `rpc_core_v1::serving::cancel_future_` | `RpcRouter` / `Router` 内部使用 |
| demo 的本地 `Router` | 各 demo 自己 | 直接实现 `TrServeHandler`，不再依赖 mptp_smux 的 router trait |

`rpc_core_v1::serving::Router` 实现新的 `TrServeHandler<C>`：

- `allowed_methods` 返回 `MethodSet::ALL`（由内部 handler 自己判）；
- `handle_async(method, session)` 按 `session.location()` 匹配路径；
- 命中后把 session 的两半按值交给旧的 `TrReqHandler`（迁移期）或新 handler；
- 未命中时，Router 只是「没有匹配」；404 由 Router 或它的根 handler 写。

### 8.3 兼容策略

- `mptp_smux` 不再提供任何 router trait；下游不能通过旧路径编译；
- `LegacyHandler<H>` 只适配 `TrReqHandler`，不做路径匹配；
- `rpc_core_v1` 迁移完成前，其 `Router` 可用 `LegacyHandler` 包装 `RpcRouter`；
- `mptp_cs_smoke`（仓库外）需要把 `mptp_smux::serving::Router` 换成
  `rpc_core_v1::serving::Router`，并改用 `MptpServlet`。

## 9. `no_std` 边界与依赖清单

### 9.1 目标

`mptp_smux` 库本体 `#![no_std]` + `extern crate alloc`；没有 `std::` 路径。

### 9.2 逐项处理

| 依赖 / 用法 | 现状 | 处理 |
| --- | --- | --- |
| `std::sync::OnceLock` | `Router` 延迟构建索引 | Router 迁出后消失；mptp_smux 不再有 |
| `futures` | `Either` / `MapErr` | 删除；`rpc_core_v1` 若仍需要，由它自己保留 |
| `thiserror` | 默认 std | `default-features = false`，错误实现 `core::error::Error` |
| `String` / `format!` | `ClientError` / `ServeError` / `MptpSession` | 结构化错误变体；文本字段用 `alloc::string::String`；`Display` 用 `write!` |
| `alloc` | 隐含 std | `extern crate alloc`，显式用 `alloc::string::String` / `alloc::vec::Vec` |
| `tokio` | 仅 dev | 保持 dev-dependencies |
| `matchit_route` / `tlist` | 默认依赖 | 从 `mptp_smux` 删除 |
| `mm_ptr` / `abs_smux` / `abs_buff` / `abs_mptp` / `mptp_wire` | 已是 no_std / 无 std 假设 | 不改 |

### 9.3 预计剩余 std 依赖

目标为零。若 `abs_smux` 的某个类型在 `no_std` 下不可用：

1. 先确认是否只是 feature 开关问题；
2. 若是传输绑定本身需要 std，则把该部分隔离到 `#[cfg(feature = "std")]` 的模块，
   并在 README / 设计文档列出；
3. 不在核心状态机里引入 `std`。

### 9.4 验证

```console
cargo check -p mptp_smux --no-default-features
cargo check -p mptp_smux --no-default-features --features client
cargo check -p mptp_smux --no-default-features --features server
cargo check -p mptp_smux --all-features
```

## 10. feature 表

```toml
[features]
default = ["client", "server"]
client = []
server = []
```

**没有 `router` feature。** 路由已经迁出本 crate。

| feature | 提供 | 典型使用者 |
| --- | --- | --- |
| `default` | `client` + `server` | 需要 P2P 的应用 |
| `client` | `MptpClient` / `MptpSession` / `RequestBuilder` / `Response` / 客户端配置 / `ClientPrepCtx` | 只发请求的一端 |
| `server` | `MptpServlet` / `ServingSession` / `TrServeHandler` / `ResponseBuilder` / 服务端配置 / `ServingPrepCtx` | 只接请求的一端 |

`messaging`（`mptp_wire` 别名）、`K_DEFAULT_SERVING_DOCK`、`ring_prepare_` 等两端
共享面始终编译，不设 feature。`--no-default-features` 只保留协议别名与共享工具，
不编译客户端 / 服务端。

## 11. 公开 API 破坏性变更清单（待确认）

> 按 `AGENTS.md` 第 1 条，以下变更实施前需要确认。

### 11.1 serving / router

1. **删除** `mptp_smux::serving::Router`、`RouterCtx`、`RouterError`、
   `TrPathRouter`、`TrPathHandler`、`TrPathRouterMut`、`RouteValues`、
   `TrPathRouterList`。
2. **删除** `mptp_smux` 对 `matchit_route` / `tlist` 的依赖；不提供 `router` feature。
3. `MptpServer` **改名** `MptpServlet`；`new` 签名从 `new(router)` 改为
   `new(conn, handler)`；`serve_channel_async(tx, rx)` 删除，改为
   `accept_async` + `serve_session_async`。
4. 新增 `TrServeHandler<C>`（单一 handler）、`ServingSession<C>`、
   `ResponseBuilder<'_, C>`、`MethodSet`、`LegacyHandler<H>`。
5. `ServeError` 移除 `Listen` / `Accept` / `RingPrepare` / `Decode` / `NotFound` /
   `Handler` / `Io`；新增 `Cancelled` / `Transport` / `Protocol` / `State` /
   `Target` / `NotAllowed`。

### 11.2 客户端

6. 新增 `MptpClient::{request, head, view, post, drop, push, pull, call,
   request_prefix}` 与 `RequestBuilder` / `Response`。
7. `MptpSession` 新增 `close_req_async`；内部发送半边改为 `Option`（字段不公开）。
8. `ClientError` 移除 `ConnectionLost(String)` / `ReqErr(String)` / `RespErr(String)` /
   `RingPrepare(String)` / `State(String)`；新增 `Cancelled` / `Transport` /
   `Protocol` / `State`。

### 11.3 会话

9. 新增 `SessionCore<C>`（`pub(crate)` 或 `#[doc(hidden)]`）与 `ServingState`；
   `MptpSession` / `ServingSession` 共享体视图与收尾。
10. `MptpSession` 的 `RespBodyRead` 仍是公开别名；服务端新增 `RequestBodyRead`。

### 11.4 no_std

11. 库本体 `#![no_std]`；下游在 std 环境使用不受影响，但不能再依赖其内部 `std` 类型。
12. `thiserror` 关闭默认 feature；`futures` 从 `mptp_smux` 删除；
    `serving::{EitherMayCancel, MapErrMayCancel, ReadyMayCancel}` 删除。

### 11.5 下游迁移

13. `rpc_core_v1::serving::Router` 接替原顶层 Router；`rpc_core_v1::preludes` 改导出。
14. `rpc_svc_macro` 的测试 / 文档示例改用 `rpc_core_v1::serving::Router`。
15. 两个 demo 的本地 `Router` 改为实现 `TrServeHandler`；`MptpServer` 改
    `MptpServlet`。
16. `mptp_cs_smoke`（仓库外）同步迁移。

### 11.6 兼容保留

17. `messaging` 别名、`RequestPrefix` / `ResponsePrefix` / `Headers` 重导出不变。
18. `MptpClient::request_async` / `request_to_dock_async` /
    `request_to_dock_with_prep_async` 保留。
19. `close_channel_async` 保留（handler / `take_parts` 路径仍可用）。
20. `K_DEFAULT_SERVING_DOCK` 保留。
21. `TrReqHandler` 仍由 `abs_mptp` 提供；`LegacyHandler` 适配。

## 12. 实施顺序与提交划分

1. **docs**：本设计文档；
2. **no_std + feature 基线**：`#![no_std]`、`extern crate alloc`、feature 表、
   移除 `futures` / `OnceLock` / Router 依赖、错误类型结构化；先让 feature 矩阵通过；
3. **SessionCore + ServingSession**：抽出共享核心，客户端 `MptpSession` 改用它；
   增加 `close_req_async`；
4. **客户端七方法**：`RequestBuilder` / `Response` / 方法糖 + 单元测试；
5. **MptpServlet + 单一 handler**：`TrServeHandler` / `ServingSession` /
   `ResponseBuilder` / 405 / 404（handler 侧）+ 单元测试；
6. **Router 迁移**：把 router 代码 / 测试迁到 `rpc_core_v1::serving`，删除
   `mptp_smux` 的 router API；
7. **demo / smoke 迁移**：两个 demo 用新 API；`rpc_svc_macro` / preludes 更新；
8. **文档**：README、dev-notes 更新；
9. **全量验证**：feature 矩阵、workspace test / clippy / doc、`just e2e-smux` /
   `just e2e-rpc`。

提交划分：设计、no_std/feature、会话核心、七方法、servlet、router 迁移、文档分开
提交。

## 13. 验收与测试矩阵

### 13.1 单元测试

| 测试 | 覆盖 |
| --- | --- |
| `head_rejects_response_body_` | 服务端 Head 回复体拒绝；客户端读 Head 回复体拒绝 |
| `drop_rejects_response_body_` | 同上 |
| `view_response_sized_` | View 回复定长体读写 |
| `post_request_body_sized_` | Post 请求定长体 |
| `push_request_suffix_` | Push 请求后缀写读与半关闭 |
| `pull_response_suffix_` | Pull 回复后缀写读 |
| `call_chunked_` | Call 分块请求 / 回复体 |
| `method_set_allow_` | `MethodSet` 文本生成 / 解析 |
| `method_not_allowed_405_` | 未允许方法写 405 + Allow |
| `unconsumed_body_drained_` | handler 不读体时收尾 drain |
| `error_classification_` | 取消 / 传输 / 协议 / 状态四类 |
| `concurrent_sessions_` | 同一连接上多个 `ServingSession` 并发 |
| feature 矩阵 | 见 §9.4 |

测试全部带中英双语文档注释，单元测试加 `timeout 60`，全量 `timeout 300`。

### 13.2 集成 / 端到端

- `mptp_smux_v1_demo`：至少覆盖 `Call` 定长体；若装配变化，同步
  `scripts/e2e_socket.py`；
- `rpc_demo`：覆盖 `Call` 定长体与 406；
- 新增 `mptp_smux` 内部集成测试覆盖 `Push` / `Pull` 后缀；
- 新增并发测试：同一连接上两个 `ServingSession` 交错处理；
- `just e2e-smux`、`just e2e-rpc` 通过。

### 13.3 静态检查

```console
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo doc --workspace --no-deps
python3 format_use_imports.py --check
```

## 14. 未决问题

1. `Allow` 头的正式协议编号：本设计用临时键 `0x009E`；若协议组先分配标准键，
   替换常量即可。
2. `mptp_session` 抽取时机：本次先共享 `SessionCore`；若 `mptp_quic` 先落地，
   应优先抽 `mptp_session`，而不是复制会话。
3. `rpc_core_v1::serving::Router` 的迁移方式：复制原顶层 Router 并实现
   `TrServeHandler`；`RpcRouter` 迁移期用 `LegacyHandler` 包装。
4. `MptpServlet` 是否需要内置 spawner 接缝：本次不内置，调用者用
   `accept_async` + 自己的 executor；若下游重复样板过多，再考虑可选的
   `TrSpawner`。
5. `ClientError` 是否需要 typed transport source：本次用 `String` 保存传输描述；
   若下游需要 downcast，再泛型化。

## 15. 英文摘要

This note designs the next `mptp_smux` refactor after review. The server side removes
all router APIs and replaces `MptpServer` with `MptpServlet`, which serves incoming
sub-streams of **one** MPTP connection through a **single** `TrServeHandler`. Client
and server sessions share a `SessionCore`; the client keeps per-method sugar over one
`RequestBuilder`, while the server exposes `ServingSession` plus `ResponseBuilder`.
Applications can hold many sessions concurrently and spawn them with their own
executor. Errors are classified as cancelled / transport / protocol / state, the crate
becomes `#![no_std]` + `alloc`, and features are split into `client` / `server` only
(no router feature). The router code moves to `rpc_core_v1::serving`; the wire format
stays in `mptp_wire`; an `mptp_session` extraction plan is recorded so a future
`mptp_quic` never depends on or duplicates `mptp_smux`.
