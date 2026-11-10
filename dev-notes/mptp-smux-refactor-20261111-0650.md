# `mptp_smux` 重构设计：七种 AccessMethod 标准实现 + `no_std` + feature 边界

- 日期：2026-11-11 06:50
- 状态：**设计草案，待公开 API 破坏性变更清单确认后实施**
- 基线：分支 `feat/mptp_wire`，提交 `3a7beb8`
- 范围：`mptp_smux` 的客户端 / 服务端标准 API、错误类型、`no_std` 边界、
  `client` / `server` / `router` feature，以及会话状态机与 `mptp_quic` 的边界
- 不在范围：`mptp_wire` 的线上格式、RPC service trait / `#[rpc]` 宏、业务 body 编解码、
  通道内请求复用、运行时选择

相关文档：

- [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md) §2、§5、§8–§13
- [`mptp-smux-20261110-0730.md`](mptp-smux-20261110-0730.md) §4、§6、§14
- [`router-20261106-0112.md`](router-20261106-0112.md)
- [`../DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)
- [`../mptp_smux/README.md`](../mptp_smux/README.md)
- [`../mptp_wire/README.md`](../mptp_wire/README.md)

## 0. 结论摘要

1. **七种方法采用「per-method 方法糖 + 统一 builder / escape hatch」并存**。
   `head/view/post/drop/push/pull/call` 是发现入口，底层都落到同一个
   `RequestBuilder`；`MptpClient::request_async` / `request_to_dock_async` 原样保留，
   作为「显式前缀 / 自定义方法语义」的 escape hatch。
2. **服务端采用单一 `handle_async(method, ctx)` 契约 + `MethodSet` 声明**，不新增
   七个必须分别实现关联 future 的 trait。框架按 `MethodSet` 在调用前判 405；未命中
   target 判 404；`ctx` 提供请求体 / 后缀读取与回复 helper。
3. **`Head` / `Drop` 的回复体校验在服务端 `ResponseBuilder` 与客户端
   `MptpSession::recv_resp_prefix_async` 两处同时做**：发送端不写、接收端拒读。
   `Push` 请求后缀由客户端在请求方向写、由服务端 `ServeCtx::req_suffix_async` 读；
   `Pull` 回复后缀由服务端 `ResponseBuilder::suffix_async` 写、由客户端
   `Response::suffix_async` 读。关闭责任统一落在「写端 drop / 半关闭」。
4. **handler 未消费的请求体 / 后缀由框架在收尾时代读丢弃（drain），不主动 reset**；
   只有 drain 自身失败或 handler 显式要求 reset 才走 reset。语义与协议 §13.2 一致。
5. **`Allow` 头临时使用用户自定义区的键 `0x009E`**（文本形态的方法名列表），
   协议分配标准键后替换常量，不改变上层 API。
6. **本次不新建 `mptp_session` crate**。先把七方法层做成只依赖
   [`abs_mptp::client::TrMptpSession`] 与 `mptp_wire` 的泛型层，`MptpSession<C>` 仍是
   smux 绑定；在设计里记录 `mptp_session` 的抽取接口与后续步骤，避免本次同时承担
   「新 crate + no_std + 七方法 + 服务端重构」四重风险。
7. **`Router` / `TrPathRouter` 分阶段处理**：`TrPathRouter` 保留为可选接缝；
   内置 `Router` 移到 `router` feature（依赖 `matchit_route` / `tlist`），`server`
   feature 不再默认携带它。`MptpServer` 只依赖新的 `TrTargetResolver` 契约，
   `TrPathRouter` 通过 blanket impl 适配，旧 `TrReqHandler` 用 `LegacyTarget` 适配。
8. **`mptp_smux` 库本体改为 `#![no_std]` + `alloc`**；移除 `futures`、
   `std::sync::OnceLock`，`thiserror` 关闭默认 feature，`MptpSession` 中的
   `format!` 全部改为结构化错误变体。`tokio` 只在 dev-dependencies。
9. **公开 API 有破坏性变更**，集中列在第 11 节；确认后按第 12 节分阶段提交。

## 1. 现状与问题

当前 `mptp_smux` 已经退到「smux 绑定 + 会话驱动 + 两端包装」，但仍有四个结构性问题：

| 问题 | 现状 | 本次处理 |
| --- | --- | --- |
| 七种方法没有标准入口 | 只有 `request_async(prefix)` + `send_req_body_async` 等通用原语，调用者自己拼方法、头、体、后缀 | 新增 per-method builder 与 `Response` |
| 服务端强制 `TrPathRouter` | `MptpServer<C, R: TrPathRouter>` 把路由策略焊进核心 | 新增 `TrTargetResolver`，`TrPathRouter` 变可选适配 |
| 错误是 `String` | `ClientError::ReqErr(String)` / `ServeError::Decode(String)` 等 | 拆成取消 / 传输 / 协议 / 状态四类具体变体 |
| std 事实 | `Router` 用 `std::sync::OnceLock`，`futures` 默认 std，`format!` 到处是 | `#![no_std]` + `alloc`，移除 `futures` / `OnceLock`，清 `format!` |

## 2. 客户端标准 API

### 2.1 取舍：per-method 方法糖 + 统一 builder

三种候选：

| 方案 | 优点 | 缺点 |
| --- | --- | --- |
| 只有 per-method 方法糖 | 可发现性最好，签名各自贴合语义 | 七份实现容易漂移，escape hatch 仍要另开一条路 |
| 只有统一 builder | 一份状态机、体积小 | `client.request(Method::View, ...)` 不够直观，方法语义无处落 |
| **两者并存（选定）** | 方法糖只是薄封装；所有语义集中在 `RequestBuilder`；显式前缀仍走旧入口 | 多一层类型，但可接受 |

选定形状：

```rust
impl<C: client::config::TrConfig> MptpClient<C> {
    /// 统一入口：显式方法 + 显式前缀能力。
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

`RequestBuilder<'a, C>` 持有：

```rust
pub struct RequestBuilder<'a, C: TrConfig> {
    client_: &'a MptpClient<C>,
    prefix_: RequestPrefix,
    dock_: Option<Dock<C>>,
}
```

`.dock(dock)` 覆盖目标垛口；缺省取 `C::default_serving_dock()`。

### 2.2 体与后缀的显式声明

方法糖只设置**默认语义**，不替调用者猜体：

| 入口 | 自动写入的头 | 说明 |
| --- | --- | --- |
| `.body_bytes(&'a [u8])` | `Body_Size: <len>` | 定长；长度 0 时不写头（无体） |
| `.body_sized(&'a mut R, size)` | `Body_Size: size` | 调用者保证 `R` 至少能给出 `size` 字节 |
| `.body_chunked(&'a mut R)` | `Body_Transfer: Chunked` | 调用者自己保证 `R` 是完整的分块体源 |
| `.body_raw()` / `.body_unspecified()` | 不写 | 前缀已经带体声明时用；发送阶段按前缀唯一决定 |
| `.suffix(&'a mut R)` | 不写（后缀无头） | `Push` / `Call` 的请求后缀 |

这些入口**显式覆盖**头：`body_bytes` / `body_sized` 会先移除 `Body_Transfer` 再写
`Body_Size`；`body_chunked` 反过来。`body_raw` 完全不动头，用于「头已经在
`request_prefix` 里写好」的场景。

`RequestBuilder` 用类型状态记录体 / 后缀是否存在，避免运行期 `Option` 的借用与
生命周期纠缠：

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

```rust
impl<'a, C, B, S> RequestBuilder<'a, C, B, S>
where
    C: TrConfig,
    B: TrBodySource,
    S: TrSuffixSource,
{
    pub fn send_async(self) -> SendRequestAsync<'a, C, B, S> { ... }
}
```

`send_async` 的步骤是固定的：

```text
1. 目标垛口 = dock_.unwrap_or(C::default_serving_dock())
2. client.request_to_dock_async(dock, prefix) -> MptpSession
3. 若 body 有：session.send_req_body_async(body)
4. 若 suffix 有：session.send_req_suffix_async(suffix)
5. session.close_req_async()       // 半关闭写端
6. session.recv_resp_prefix_async() -> ResponsePrefix
7. 返回 Response<C>（拥有 session + prefix）
```

`close_req_async` 是本次给 `MptpSession` 增加的方法：把内部 `tx_` 取出并 drop，
保留 `rx_` 继续读回复。它对应协议 §12.1 的「半关闭写方向」。

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

`body_async` / `suffix_async` 返回的视图借用 `&mut self`，因此调用者不能在持有体
视图时调用 `suffix_async`；这正好把协议约束 4（后缀流前必须处理完体）交给借用检查。

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
let mut resp = client
    .pull("/stream/logs")
    .body_bytes(&query)
    .send_async()
    .await?;
let mut body = resp.body_async().await?;
// ... 读回复体 ...
drop(body);
let mut suffix = resp.suffix_async().await?;
// ... 持续读服务端推送的裸字节 ...
```

### 2.4 状态机映射

客户端状态机沿用 `SessionState`，只补三点：

1. `close_req_async` 后仍处于 `WaitingResp`，允许 `recv_resp_prefix_async`；
2. `send_async` 在体 / 后缀发送后调用 `close_req_async`，因此标准路径总是半关闭写端；
3. `Response::body_async` / `suffix_async` 只做状态检查与视图装配，边界判定仍在
   `MptpSession` 内，不复制一份。

## 3. 服务端标准 API

### 3.1 取舍：单一 `handle_async(method, ctx)` + `MethodSet`

三种候选：

| 方案 | 优点 | 缺点 |
| --- | --- | --- |
| 七方法 trait | 方法入口最直观 | 七个关联 future 类型，默认 405 无法给出统一返回类型，样板极重 |
| `handle(method, ctx)` + `MethodSet`（选定） | 一个入口、错误统一、405 可由框架在调用前判 | 方法分派写在 handler 里，靠 helper / 文档保证 |
| target 枚举 | 框架可精确知道实现了哪些方法 | 把业务方法集合封成枚举，扩展性差 |

选定契约：

```rust
pub trait TrServeTarget<C>
where
    C: serving::config::TrConfig,
{
    type Err: core::error::Error;

    /// 本 target 支持的方法集合；缺省为全部。
    /// 框架在调用 handle 前检查；不在集合内直接写 405 + Allow，不进入 handler。
    fn allowed_methods(&self) -> MethodSet {
        MethodSet::ALL
    }

    type HandleAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<(), Self::Err>>
    where
        Self: 'f;

    /// 一次请求的处理入口；`cx` 提供方法、位置、头表、请求体 / 后缀读取与回复 helper。
    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        cx: &'f mut ServeCtx<C>,
    ) -> Self::HandleAsync<'f>;
}
```

handler 内部按 `method` match；`MethodSet` 是框架与 handler 之间唯一的「能力声明」，
避免框架猜 target 实现了哪些方法。

### 3.2 target 解析与 404

```rust
pub trait TrTargetResolver<C>
where
    C: serving::config::TrConfig,
{
    type Target: TrServeTarget<C>;

    /// 按 location 找 target；未命中返回 None，由框架写 404 + 收尾。
    fn resolve<'f>(&'f self, location: &'f str) -> Option<&'f Self::Target>;
}

/// 单 target：所有 location 都交给同一个 target。
pub struct SingleTarget<T> { target_: T }

/// 根 target：resolver 未命中时回落到它。
pub struct RootedResolver<R, T> { resolver_: R, root_: T }
```

`MptpServer` 只依赖 `TrTargetResolver`：

```rust
pub struct MptpServer<C, R>
where
    C: serving::config::TrConfig,
    R: TrTargetResolver<C>,
{
    resolver_: R,
    _use_c_: PhantomData<fn() -> C>,
}

impl<C, R> MptpServer<C, R> {
    pub const fn new(resolver: R) -> Self;
    pub fn serve_channel_async(...);
    pub fn serve_listener_async(...);
}
```

404 与 405 都由框架处理：

```text
resolve(location):
  None  -> 写 404（无体） -> 丢弃请求体/后缀 -> 半关闭 -> drain -> 结束
  Some  -> allowed_methods().contains(method)?
            否 -> 写 405 + Allow（临时头，见 §6） -> 收尾
            是 -> target.handle_async(method, cx)
```

`RootedResolver` 用于「当前层的根 handler」：resolver 返回 `None` 时用 `root_`，
从而表达「未知资源交给当前层根 handler」；是否继续上抛由应用自己组合 resolver 决定，
框架不自动上抛。

### 3.3 `ServeCtx`

```rust
pub struct ServeCtx<C: serving::config::TrConfig> {
    method_: AccessMethod,
    location_: String,
    headers_: Headers,
    tx_: Option<ChannelTx<C>>,
    rx_: Option<ChannelRx<C>>,
    req_body_plan_: BodyPlan,
    req_body_state_: BodyState,
    responded_: bool,
    dropped_req_bytes_: usize,
}

impl<C: serving::config::TrConfig> ServeCtx<C> {
    pub fn method(&self) -> AccessMethod;
    pub fn location(&self) -> &str;
    pub fn headers(&self) -> &Headers;
    pub fn headers_mut(&mut self) -> &mut Headers;

    /// 按请求前缀声明的体边界取读视图。
    pub fn req_body_async(&mut self) -> ReqBodyAsync<'_, C>;

    /// 丢弃未读完的请求体，然后把接收半边当裸后缀流交出去。
    pub fn req_suffix_async(&mut self) -> ReqSuffixAsync<'_, C>;

    /// 构造回复 helper；终结时才写前缀。
    pub fn response(&mut self, status: Status) -> ResponseBuilder<'_, C>;
    pub fn response_prefix(&mut self, prefix: ResponsePrefix) -> ResponseBuilder<'_, C>;

    /// escape hatch：拿走两个半边自己驱动；之后框架只做「未回复则 500」的兜底。
    pub fn take_channel(&mut self) -> Option<(ChannelTx<C>, ChannelRx<C>)>;

    /// 已经丢弃的请求字节数（收尾后由框架记录）。
    pub fn dropped_req_bytes(&self) -> usize;
}
```

`req_body_async` 与客户端 `RespBodyRead` 同构：一个类型、两种档位，由请求前缀唯一
决定；`Head` / `Drop` 的请求体按协议「不应有」，但请求方向**不做**强制拒绝——协议
只对「回复体」明确要求违规处理，请求体的实际存在仍由头唯一决定，方法糖不能绕过
协议头。

### 3.4 回复 helper

```rust
pub struct ResponseBuilder<'a, C: serving::config::TrConfig> {
    cx_: &'a mut ServeCtx<C>,
    prefix_: ResponsePrefix,
}

impl<'a, C> ResponseBuilder<'a, C> {
    pub fn header(&mut self, key: impl Into<HeaderKey>, val: impl Into<HeaderVal>) -> &mut Self;

    /// 状态码 + 头 + 无体；若前缀/方法声明了体，返回协议错误。
    pub async fn no_body_async(self) -> Result<(), ServeError>;

    /// 定长回复体：自动写 `Body_Size`，再按长度搬运。
    pub async fn sized_async<R>(self, body: &mut R, size: usize) -> Result<(), ServeError>
    where R: TrBuffRead<u8> + TrConsumerState;

    /// 分块回复体：自动写 `Body_Transfer: Chunked`，再逐块编码。
    pub async fn chunked_async<R>(self, body: &mut R) -> Result<(), ServeError>
    where R: TrBuffRead<u8> + TrConsumerState;

    /// 回复体之后的裸后缀流（`Pull` / `Call`）。
    pub async fn suffix_async<R>(self, suffix: &mut R) -> Result<(), ServeError>
    where R: TrBuffRead<u8> + TrConsumerState;
}
```

终结语义：

1. **`Head` / `Drop`**：`sized_async` / `chunked_async` / `suffix_async` 直接返回
   `ProtocolError::ResponseBodyOnHeadOrDrop`；`no_body_async` 若发现 `prefix_` 里已有
   `Body_Size` / `Body_Transfer`，同样返回该错误。客户端会话也会在读到回复前缀时
   再校验一次，形成两端双保险。
2. `sized_async` 会**覆盖**前缀里的 `Body_Size`（按实参 `size`），并移除
   `Body_Transfer`；`chunked_async` 反之。显式设置冲突头的调用者以 helper 实参为准，
   避免「头声明 10、实际写 20」的错位。
3. 终结方法依次执行：

   ```text
   写回复前缀 -> 写回复体（若声明） -> 写回复后缀（若有）
   -> drop(tx) 半关闭写端
   -> 丢弃未读完的请求体/后缀（读 rx 到 EOF）
   -> 释放 rx
   ```

   收尾顺序与 `close_channel_async` 一致：**先半关闭写端，再读尽对端**。
4. handler 未调用任何终结方法就返回 `Ok(())`：框架写 `500` 无体并收尾；返回
   `Err(e)`：框架写 `500` 无体并收尾，错误经 `ServeError::Target` 上报，不泄漏到
   线上。

### 3.5 请求体 / 后缀未消费时的语义

**选定：代读丢弃（drain），不 reset。**

理由：

- 协议 §13.2 明确「框架在收尾时会把剩余请求数据读尽，避免提前重置导致在途响应丢失」；
- 先 reset 会让已经写出的回复被下层抹掉，对端只看到 EOF；
- 请求体 / 后缀在收尾阶段已经无人消费，读进栈上暂存再丢弃即可。

具体规则：

1. handler 读过一部分体就放下视图：视图 Drop 时把剩余额度写回 `ServeCtx`；
2. 终结方法先写回复，再 `drop(tx)`，然后用剩余额度把体读尽，继续读到 EOF；
3. `Push` 的请求后缀如果 handler 没读，同样在收尾阶段被丢弃；
4. drain 自身失败（非 `Closing`）时返回 `ServeError::Transport`，由调用方决定是否
   reset / 关闭；框架不把失败伪装成成功；
5. handler 想主动 reset，用 `ServeCtx::take_channel` 拿走两半自行处理，框架不再插手。

### 3.6 服务端示例

```rust
struct EchoTarget;

impl TrServeTarget<ServingCfg> for EchoTarget {
    type Err = HandlerError;

    fn allowed_methods(&self) -> MethodSet {
        MethodSet::CALL | MethodSet::POST | MethodSet::VIEW
    }

    type HandleAsync<'f> = HandleEchoAsync<'f> where Self: 'f;

    fn handle_async<'f>(
        &'f self,
        method: AccessMethod,
        cx: &'f mut ServeCtx<ServingCfg>,
    ) -> Self::HandleAsync<'f> {
        HandleEchoAsync::new(self, method, cx)
    }
}

async fn handle_echo_async_(
    method: AccessMethod,
    cx: &mut ServeCtx<ServingCfg>,
) -> Result<(), HandlerError> {
    match method {
        AccessMethod::View => {
            let mut body = cx.req_body_async().await?;
            // ... 读取 ...
            let mut out = build_reply()?;
            cx.response(Status::Ok)
                .sized_async(&mut out, out.len())
                .await?;
        }
        AccessMethod::Call => {
            let mut body = cx.req_body_async().await?;
            // ... 读取 ...
            let mut out = build_reply()?;
            cx.response(Status::Ok)
                .sized_async(&mut out, out.len())
                .await?;
        }
        _ => {
            // MethodSet 已在框架层挡住其余方法；这里只处理已允许的。
            cx.response(Status::NotImplemented).no_body_async().await?;
        }
    }
    Ok(())
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

- 方法糖只提供默认入口与类型状态；实际有无体仍由 `Body_Size` / `Body_Transfer`
  唯一决定；
- `head` / `drop` 的客户端 builder 不提供 `.body_*` 的类型状态？**提供**，因为
  escape hatch 与显式覆盖是要求的一部分；但文档明确「这与方法意图不符，框架只对
  回复体做强校验」；
- `head` / `drop` 的**回复体**是唯一做强校验的地方：服务端不写，客户端拒读；
- `Push` / `Pull` 的后缀方向由方法糖的可用入口表达：`Push` builder 提供 `.suffix`，
  `Pull` builder 不提供 `.suffix`（用 `request(AccessMethod::Pull, ...)` 可绕过）；
  服务端不按方法禁止 `suffix_async`，由 handler 自己按 `method` 决定。

## 5. 错误类型

### 5.1 客户端

```rust
#[derive(Debug)]
pub enum ClientError {
    /// 等待期间调用方取消。
    Cancelled,
    /// 连接 / 子流 / ring 工厂的传输失败。
    Transport(TransportError),
    /// 前缀解码、体声明、截断、Head/Drop 回复体等协议违规。
    Protocol(ProtocolError),
    /// 会话状态机不接受当前操作。
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

实现 `Display` + `core::error::Error`（手写或用 `thiserror` 关闭默认 feature）。

**source 保留策略**：协议错误里的底层 `MessagingError` / `ChunkedReadError` /
`LimitedReaderError` 可以保留具体类型的地方直接保留；传输错误的具体类型是配置关联
类型（泛型），非泛型 `ClientError` 无法命名它，因此用 `source: String` 保存描述，
同时用变体名保留「哪一步失败」。如果未来需要 typed source，可把 `ClientError`
泛型化为 `ClientError<TyTransport>`，但不作为本次目标。

### 5.2 服务端

```rust
pub enum ServeError {
    Cancelled,
    Transport(TransportError),
    Protocol(ProtocolError),
    State(StateError),
    Target { source: String },      // handler 报错
    NotAllowed { method: AccessMethod, allow: MethodSet },
    NotFound { location: String },
}
```

`RouterError`（`router` feature）保留 `NotFound` / `Handler`，但改成结构化字段：

```rust
pub enum RouterError {
    NotFound { location: String },
    Handler { location: String, source: String },
}
```

### 5.3 与 `SessionState` 的关系

`StateError` 携带 `actual: SessionState`，不再用 `format!("会话处于 ...")`。所有
`format!` 从 `MptpSession` 删除；需要文本时在 `Display` 实现里用 `write!` 组合
字段。

## 6. `Allow` 头的临时方案

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

选择文本而不是位掩码的理由：`Allow` 的主要使用者是调试与跨语言实现，文本方法名
比「把位掩码再解释一遍」更直接；位掩码只作为 `MethodSet` 内部表示。

## 7. 会话状态机与 `mptp_quic` 边界

### 7.1 本次不抽 `mptp_session`

`MptpSession` 的「前缀 → 体 → 后缀 → 半关闭」不是 smux 特有，但本次不新建 crate。
理由：

1. 抽 `mptp_session` 需要先定义「半关闭」的抽象 trait，并把 `ChannelTx` / `ChannelRx`
   包成 newtype（否则孤儿规则不允许 `mptp_smux` 为外部类型实现 `mptp_session` 的
   trait）；这是另一项跨 crate 改造；
2. 本次已经包含 no_std、feature 拆分、七方法、服务端 target 四块高风险改动；
3. 七方法层可以**先与 smux 类型解耦**：`RequestBuilder` / `Response` 只依赖
   `abs_mptp::client::TrMptpSession` 与 `mptp_wire`，不直接触碰 dock / ring；
   `ServeCtx` / `ResponseBuilder` 只依赖 `abs_mptp::serving::config` 的类型别名；
4. 记录抽取计划，避免 `mptp_quic` 复制。

### 7.2 `mptp_session` 抽取接口草案（后续）

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

`mptp_quic` 届时提供自己的 `TrSessionIo`，并复用 `mptp_session::ClientSession` /
`mptp_session::ServeCtx`；`mptp_smux` 只剩 smux 绑定、ring 注入、dock 映射、错误
映射与收尾。

### 7.3 本次落点

- `MptpSession` 继续住在 `mptp_smux::client`，但 no_std 化、错误结构化；
- 七方法层放在 `mptp_smux::client::method` / `mptp_smux::serving::target`，
  不依赖 `MptpClient` / `MptpServer` 的具体字段，只依赖 trait 与类型别名；
- `abs_mptp_smux` 仍无使用者，本次不动；后续评估删除或并入 `mptp_session`。

## 8. `Router` / `TrPathRouter` 的剥离

### 8.1 目标

- `server` feature 不再默认依赖 `matchit_route` / `tlist`；
- `Router` 与内置路径匹配移到 `router` feature；
- `MptpServer` 只依赖 `TrTargetResolver`；
- 应用自定义路由（demo 的 `Router`、`RpcRouter`）继续可用。

### 8.2 兼容层

```rust
// server feature
impl<C, R> TrTargetResolver<C> for R
where
    C: TrConfig + 'static,
    R: TrPathRouter,
    R::Handler: TrServeTarget<C> + 'static,
{
    type Target = R::Handler;

    fn resolve<'f>(&'f self, location: &'f str) -> Option<&'f Self::Target> {
        self.match_path(location)
    }
}
```

`TrPathRouter` 的 `match_path` 已经返回 `Option<&Handler>`，因此适配是零成本的。

旧 `TrReqHandler` 适配：

```rust
pub struct LegacyTarget<H> { handler_: H }

impl<C, H> TrServeTarget<C> for LegacyTarget<H>
where
    C: TrConfig,
    H: TrReqHandler<C>,
{
    type Err = H::Err;
    // handle_async: 把 ctx 的两个半边按值交给旧 handler，旧 handler 自行收尾。
}
```

`LegacyTarget` 不提供请求体 / 后缀 helper，因为旧 handler 直接拿 `ChannelTx` /
`ChannelRx`；它只是让现有 demo / 下游在迁移期继续工作。

### 8.3 内置 `Router`

- 移到 `#[cfg(feature = "router")]`；
- 去掉 `std::sync::OnceLock`：`route(self)` 消费式构建，每次登记后重新构建
  `matchit_route` 索引，`Router` 内直接存 `MatchitRouter<usize>`；
- `Router` 实现 `TrTargetResolver<C>`（`Target = Self`）与 `TrServeTarget<C>`
  （按 method 分派给列表中的 handler），不再要求 `TrReqHandler`；
- 现有 `TrPathHandler` / `TrPathRouter` / `TrPathRouterMut` 保留在 `router` feature；
  `rpc_core_v1::serving::RpcRouter` 迁移到 `TrServeTarget`，或在迁移期用
  `LegacyTarget` 包一层。

## 9. `no_std` 边界与依赖清单

### 9.1 目标

`mptp_smux` 库本体 `#![no_std]` + `extern crate alloc`；没有 `std::` 路径。

### 9.2 逐项处理

| 依赖 / 用法 | 现状 | 处理 |
| --- | --- | --- |
| `std::sync::OnceLock` | `Router` 延迟构建索引 | 删除；构造 / 登记时直接构建 `matchit_route` |
| `futures` | `Either` / `MapErr` | 删除；在 `cancel_future_` 自定义 `EitherFuture` / `MapErrFuture` |
| `thiserror` | 默认 std | `default-features = false`，错误实现 `core::error::Error` |
| `String` / `format!` | `ClientError` / `ServeError` / `MptpSession` | 结构化错误变体；文本字段用 `alloc::string::String`；`Display` 用 `write!` |
| `alloc` | 隐含 std | `extern crate alloc`，显式用 `alloc::string::String` / `alloc::vec::Vec` |
| `tokio` | 仅 dev | 保持 dev-dependencies |
| `matchit_route` / `tlist` | 默认依赖 | 仅 `router` feature 依赖 |
| `mm_ptr` / `abs_smux` / `abs_buff` / `abs_mptp` / `mptp_wire` | 已是 no_std / 无 std 假设 | 不改 |

### 9.3 预计剩余 std 依赖

目标为零。若 `abs_smux` 的某个类型在 `no_std` 下不可用，按以下顺序处理：

1. 先确认是否只是 feature 开关问题；
2. 若是传输绑定本身需要 std，则把该部分隔离到 `#[cfg(feature = "std")]` 的模块，
   并在 README / 设计文档列出；
3. 不在核心状态机里引入 `std`。

### 9.4 验证

```console
cargo check -p mptp_smux --no-default-features
cargo check -p mptp_smux --no-default-features --features client
cargo check -p mptp_smux --no-default-features --features server
cargo check -p mptp_smux --no-default-features --features router
cargo check -p mptp_smux --all-features
```

## 10. feature 表

```toml
[features]
default = ["client", "server"]
client = []
server = []
router = ["server", "dep:matchit_route", "dep:tlist"]
```

| feature | 提供 | 典型使用者 |
| --- | --- | --- |
| `default` | `client` + `server` | 需要 P2P 的应用 |
| `client` | `MptpClient` / `MptpSession` / `RequestBuilder` / `Response` / 客户端配置 / `ClientPrepCtx` | 只发请求的一端 |
| `server` | `MptpServer` / `TrServeTarget` / `TrTargetResolver` / `ServeCtx` / `ResponseBuilder` / 服务端配置 / `ServingPrepCtx` | 只接请求的一端 |
| `router` | 内置 `Router` / `TrPathRouter` / `TrPathHandler` / `tlist` 支持 | 需要默认前缀路由的应用 |

`messaging`（`mptp_wire` 别名）、`K_DEFAULT_SERVING_DOCK`、`ring_prepare_` 等两端
共享面始终编译，不设 feature。

## 11. 公开 API 破坏性变更清单（待确认）

> 按 `AGENTS.md` 第 1 条，以下变更实施前需要确认。

### 11.1 feature 与依赖

1. `default` 从「全部 + 内置 Router」改为 `["client", "server"]`，**不再包含
   `router`**。使用 `mptp_smux::serving::Router` 的下游必须显式启用 `router`。
2. `matchit_route` / `tlist` 不再是默认依赖；`server` 不启用 `router` 时不再编译
   内置 `Router`。

### 11.2 客户端

3. 新增 `MptpClient::{request, head, view, post, drop, push, pull, call,
   request_prefix}` 与 `RequestBuilder` / `Response`。
4. `MptpSession` 新增 `close_req_async`；内部 `tx_` 改为 `Option`（不公开）。
5. `ClientError` 变体变更：移除 `ConnectionLost(String)` / `ReqErr(String)` /
   `RespErr(String)` / `RingPrepare(String)` / `State(String)`；新增
   `Cancelled` / `Transport(TransportError)` / `Protocol(ProtocolError)` /
   `State(StateError)`。匹配这些旧变体的下游会破坏。
6. `ClientError` 的 `core::error::Error` 实现不变（std 下仍可用），但 `source()` 的
   内容与变体名变化。

### 11.3 服务端

7. `MptpServer<C, R>` 的约束从 `R: TrPathRouter` 改为 `R: TrTargetResolver<C>`；
   `R::Handler: TrReqHandler<C>` 不再直接约束。
8. 新增 `TrServeTarget<C>`、`TrTargetResolver<C>`、`ServeCtx<C>`、
   `ResponseBuilder<'_, C>`、`SingleTarget<T>`、`RootedResolver<R, T>`、
   `MethodSet`。
9. `ServeError` 变体变更：移除 `Listen(String)` / `Accept(String)` /
   `RingPrepare(String)` / `Decode(String)` / `NotFound(String)` /
   `Handler(String)` / `Io(String)`；新增 `Cancelled` / `Transport` / `Protocol` /
   `State` / `Target` / `NotAllowed` / `NotFound`。
10. `TrReqHandler` 不再被 `MptpServer` 直接调用；旧 handler 需用
    `LegacyTarget<H>` 适配，或迁移到 `TrServeTarget`。
11. `Router` 从 `server` 默认面移到 `router` feature；`Router` 的
    `TrReqHandler` 实现改为 `TrServeTarget` / `TrTargetResolver`（保留
    `TrPathRouter` 兼容）。
12. `RouterError` 变体改为结构化字段（`NotFound { location }` /
    `Handler { location, source }`）。

### 11.4 `no_std` 与错误

13. 库本体 `#![no_std]`；下游在 std 环境使用不受影响，但不能再依赖
    `mptp_smux` 内部任何 `std` 类型。
14. `mptp_smux::x_deps` 不再导出 `futures` 相关类型（原本也未导出）；`serving`
    的 `EitherMayCancel` / `MapErrMayCancel` 仍是 `#[doc(hidden)]`，但内部类型
    从 `futures::Either` 换成本地实现。

### 11.5 兼容保留

15. `messaging` 别名、`RequestPrefix` / `ResponsePrefix` / `Headers` 重导出不变。
16. `MptpClient::request_async` / `request_to_dock_async` /
    `request_to_dock_with_prep_async` 保留。
17. `MptpServer::serve_channel_async` / `serve_listener_async` 保留签名形状，
    但约束变为 `TrTargetResolver`。
18. `close_channel_async` 保留。
19. `TrPathRouter` / `TrPathHandler` / `TrPathRouterMut` 保留（`router` feature），
    通过 blanket impl 继续可用于 `MptpServer`。
20. `K_DEFAULT_SERVING_DOCK` 保留。

## 12. 实施顺序与提交划分

1. **docs**：本设计文档（已写）；
2. **no_std / feature 基线**：`#![no_std]`、feature 表、移除 `futures` /
   `OnceLock`、错误类型结构化；行为不变，先让 feature 矩阵通过；
3. **会话原语**：`MptpSession::close_req_async`、`Response` 所需的状态检查；
4. **客户端七方法**：`RequestBuilder` / `Response` / 方法糖 + 单元测试；
5. **服务端 target**：`TrServeTarget` / `TrTargetResolver` / `ServeCtx` /
   `ResponseBuilder` / 404 / 405 + 单元测试；
6. **Router 剥离**：`router` feature、`Router` 去 `OnceLock`、兼容适配；
7. **demo / smoke 迁移**：两个 demo 至少能用新 API；`rpc_core_v1` 按需迁移；
8. **文档**：README、dev-notes 更新；
9. **全量验证**：feature 矩阵、workspace test / clippy / doc、`just e2e-smux` /
   `just e2e-rpc`。

提交划分与上表一致：设计、no_std/feature、七方法、Router 剥离、文档分开提交。

## 13. 验收与测试矩阵

### 13.1 单元测试（每种方法）

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
| `unknown_target_404_` | resolver 未命中写 404 |
| `method_not_allowed_405_` | 未允许方法写 405 + Allow |
| `unconsumed_body_drained_` | handler 不读体时收尾 drain |
| `error_classification_` | 取消 / 传输 / 协议 / 状态四类 |
| feature 矩阵 | 见 §9.4 |

测试全部带中英双语文档注释，单元测试加 `timeout 60`，全量 `timeout 300`。

### 13.2 集成 / 端到端

- `mptp_smux_v1_demo`：至少覆盖 `Call` 定长体；若装配变化，同步
  `scripts/e2e_socket.py`；
- `rpc_demo`：覆盖 `Call` 定长体与 406；
- 新增一个 `mptp_smux` 内部集成测试（或 demo 子命令）覆盖 `Push` / `Pull` 后缀；
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
2. `mptp_session` 抽取时机：本次记录接口草案，不新建 crate；若 `mptp_quic` 先落地，
   应优先抽 `mptp_session`，而不是复制 `MptpSession`。
3. `rpc_core_v1::serving::RpcRouter` 的迁移方式：迁移到 `TrServeTarget`，或在
   迁移期用 `LegacyTarget` 包一层；优先迁移，避免长期保留两条服务端路径。
4. `Router` 是否最终独立成 crate：本次先 feature 化；若 router 设计（分层 /
   `bind_handler` / `bind_router`）落地，再评估独立 crate。
5. `ClientError` 是否需要 typed transport source：本次用 `String` 保存传输描述；
   若下游需要 downcast，再泛型化。

## 15. 英文摘要

This note designs the next `mptp_smux` refactor: per-method client sugar on top of one
`RequestBuilder`, a server `TrServeTarget` / `TrTargetResolver` seam with a single
`handle_async(method, ctx)` entry, concrete four-way error classification
(cancelled / transport / protocol / state), `#![no_std]` plus `alloc`, and a feature split
(`client`, `server`, optional `router`). The wire format stays in `mptp_wire`; the
per-request session state machine stays in `mptp_smux` for this round, with an explicit
`mptp_session` extraction plan so a future `mptp_quic` never depends on or duplicates
`mptp_smux`.
