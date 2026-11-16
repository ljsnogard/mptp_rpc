# 提案：MPTP 会话与 Cookie 语义（客户端自动附凭据 + 服务端会话钩子）

- 日期：2026-11-16
- 状态：**提案草案（待评审，未实施）**
- 触发：`mptp_im_demo` 的 admin header 切面（[`authz-middleware-20261115-1600.md`](../../mptp_im_demo/dev-notes/authz-middleware-20261115-1600.md)）
- 前序：[`rpc-client-headers-20261115-1600.md`](rpc-client-headers-20261115-1600.md)（C1：typed unary 单次带 header）
- 定位：`mptp_im_demo` 只用于发现 / 验证能力；本提案的目标是 **`mptp_rpc` 框架本身**（客户端 + 服务端），让「登录一次、之后自动带凭据」成为框架能力，而不是每个业务客户端手写。

---

## 0. 结论摘要

1. 现在 demo 的 `ImClient::admin_headers_` 是**应用层 workaround**：它把 token 存在连接对象里、每次调用手动附头。它有效，但不是框架语义；换一个业务客户端就要重写一遍。
2. 浏览器 / ASP.NET Core 的「自动带凭据」不是魔法：**Cookie 标准（RFC 6265）+ 服务端 session store + 框架中间件**。MPTP 没有浏览器，但 `mptp_rpc` 可以提供等价的三层：
   - **线格式层**：`Cookie` / `Set-Cookie` 的 MPTP 子集（解析 / 序列化，no_std + alloc）；
   - **客户端层**：连接级 `CookieJar`，自动处理响应里的 `Set-Cookie`、按 scope / 过期时间自动附 `Cookie`；
   - **服务端层**：回复头挂载点（让中间件 / handler 在回复写出前放 `Set-Cookie`）+ `SessionStore` 抽象 + 参考 `SessionMiddleware`。
3. **Session 是 MPTP 的一等约定**：客户端与服务端用一个固定 `SessionId` 标记逻辑会话；它只占一个头值，所以当前 `Headers` 的单值实现不构成障碍。**不是协议硬约束**：`Headers` 是单值 map，只影响「多个独立 cookie」；单会话不受影响，多 cookie 是可选的后续扩展。MPTP 没有 domain / 浏览器，`Domain` / `SameSite` / `HttpOnly` 无语义；`Path` 映射为 **MPTP location 前缀**（如 `im/v1/admin`）。
4. 兼容性：默认不启用 jar；显式请求头优先于 jar。`MptpClient` 的公共结构（当前是 tuple struct）若要加 jar 字段，属于公开 API 变更，需单独确认；本文给出「直接加字段」与「wrapper」两条路线并推荐前者（因为不牵动宏与所有 helper 签名）。
5. 分阶段：P0 线格式 → P1 客户端 jar → P2 服务端回复头挂载 → P3 `SessionStore` + 中间件 → P4 demo 迁移与验收。
6. **宏不表达授权**：v1 不在 `#[rpc]` 里加 session 语法。授权是服务端 router /
   middleware / endpoint metadata 的职责，客户端只靠连接级 CookieJar 自动附凭据；
   401/403 就是契约。若需要给宏生成端点附加任意元数据，只加一个与 session 无关的通用
   `install_into_with_metadata` hook。理由与取舍见 §13。

---

## 1. 现状与缺口

### 1.1 客户端

- [`rpc_core_v1/src/client/proxy_.rs`](../rpc_core_v1/src/client/proxy_.rs)：`ClientProxy::with_headers` 是**每次调用**显式挂头；没有「连接级凭据」。
- `mptp_smux::client::MptpClient` 只持有 `SharedConn`：

  ```rust
  pub struct MptpClient<C>(SharedConn<C>)
  where
      C: TrConfig;
  ```

  它不解析 `Set-Cookie`，也不在请求里补 `Cookie`。
- demo 的 `ImClient` 自己存 `admin_headers_` 并在每个 admin 方法上重复 `.with_headers(...)`；这是应用层补齐，不是框架能力。
- 每个 unary 请求都会新建子流（`bind_async(unspecified)`），但**连接不变**；这正好是连接级 jar 的作用域。

### 1.2 服务端

- `ResponseBuilder::header` 已经能写回复头（[`session_.rs`](../mptp_smux/src/serving/session_.rs)），`typed_response` 也能带类型头；但：
  - 宏生成的 Call handler 走 `write_typed_value_async`，没有「附加回复头」的参数；
  - 宏生成的业务 impl **拿不到 `MptpContext`**（`AdminServiceImpl` 只持有 `Arc<ConnCtx>`），所以 `login` 里无法直接 `ctx.set_response_header(...)`；
  - `write_status_async` 走 `session.response(status)`，与 `typed_response` 是两个入口，需要统一挂载点。
- 没有 `SessionStore` / `SessionMiddleware` / cookie 解析的框架 API；demo 用 `Hub::admins_` 充当连接级授权事实。

### 1.3 线格式

- `Headers` 是单值 map：`Set-Cookie` 只能出现一次；`Cookie` 请求头可以把多个 `name=value` 放进同一个值。
- `StdHeaderKey::Cookie`（0xB3）/ `Set_Cookie`（0xD3）已经存在，所以**不需要新协议头编号**；但需要定义 MPTP 下哪些属性有意义。
- `HeaderKey` 支持文本键，`HeaderVal` 支持文本值；cookie 解析/序列化可以完全放在 `mptp_wire`。

### 1.4 语义

MPTP 是「一条连接 = 一个对端」，没有浏览器、没有跨域导航：

| HTTP 概念 | MPTP 下是否有意义 | 建议 |
| --- | --- | --- |
| `Domain` | 无（单 peer） | 忽略 / 拒绝 |
| `Path` | 有：location 前缀（service / endpoint pattern） | 映射为 `CookieScope::LocationPrefix` |
| `Expires` / `Max-Age` | 有 | v1 只支持 `Max-Age`（避免 no_std 日期解析） |
| `Secure` | 取决于传输是否加密 | 由连接配置 `secure` 标志决定；默认 false 时拒绝 `Secure` cookie |
| `HttpOnly` | 无 DOM | 解析并忽略 |
| `SameSite` | 无跨站导航 | 解析并忽略 |
| 重定向 | 无 | 不实现 |

---

## 2. 目标 / 非目标

### 目标

1. 客户端：连接级 `CookieJar`，自动保存服务端下发的会话凭据，并按 scope / 过期时间自动附到后续请求；支持显式头覆盖；Pull / Push / unary 一致。
2. 服务端：提供「回复写出前挂回复头」的通用机制（覆盖宏生成 Call、手写端点、错误回复），以及可选的 `SessionStore` / `SessionMiddleware` 参考实现。
3. 不把 token 放进 URL / body；不在日志 / 错误文案里回显。
4. `no_std + alloc`，不强制 `std`；同步原语与时钟来源可注入。
5. 默认关闭，现有调用零行为变化。

### 非目标

- 浏览器兼容（`SameSite` / `HttpOnly` / 跨域）；
- OAuth / JWT / OIDC、TLS、通用 RBAC；
- 多 cookie / 多租户 cookie 隔离（v1 不做，见 §3.4）；
- 自动重连 / 重放（客户端重连后是否复用会话由应用决定）。

---

## 3. 语义设计

### 3.0 MPTP Session 约定（客户端 / 服务端共同理解）

在讨论 cookie 之前，先把「会话」定成两端的共同约定，而不是某个业务客户端的私有字段：

```text
SessionId       = 128-bit 不透明值，由服务端签发；客户端只保存、不解释。
SessionHeader   = 一个固定头名（候选：Cookie: sid=... / x-mptp-session: ...）。
SessionRecord   = 服务端记录：主体（user_id / principal）、过期时间、scope、in-flight 计数。
SessionPolicy   = Sequential | Concurrent（见下）。
SessionDock     = 可选：{ local_dock, remote_dock }，只作为传输提示，不是身份。
```

- **身份**：`SessionId` 是身份；dock 号是线上可见的传输标识，**不是秘密，也不作为认证依据**。
- **顺序**：`Sequential` 表示同一会话**至多一个 in-flight 请求**：
  - 客户端把并发调用排队（或直接报错）；
  - 服务端按 `SessionId` 维护 in-flight 计数，并发请求回 `409 Conflict`（或排队），
    这样「会话内有序、无并发」不依赖 dock。
  - `Concurrent` 是默认（HTTP/1.1 多连接、HTTP/2 多流都是并发语义）。
- **传输**：v1 用「每请求一条普通 channel」（Path A）；`SessionDock` 的固定 dock 对方案见
  §12：它不是「每请求复用同一 pair」，而是**每会话一条持久 channel**（MPTP keep-alive），
  在 MPTP 层可实现，不需要改 `smux_v1` 的 `WaitClose`；作为 Path B 的扩展。
- **生命周期**：登录签发、请求携带、服务端续期 / 撤销；连接断开不自动等于会话结束
  （取决于 store 与 TTL）。

有了这层约定，cookie 只是 `SessionId` 的一种承载方式；客户端 jar 与服务端 store 都围绕
`SessionId`，而不是围绕「某个 ImClient 的字段」。

### 3.1 Cookie 模型

```rust
// mptp_wire/src/cookie_.rs（建议位置）
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub scope: CookieScope,
    pub max_age_ms: Option<u64>,
    pub secure: bool,
}

pub enum CookieScope {
    /// 本连接所有请求。
    Connection,
    /// 只发给以该 location 前缀开头的请求（对应 HTTP `Path`）。
    LocationPrefix(String),
}

pub struct CookieJar {
    cookies_: Vec<Cookie>,
    // 可选：按 name 去重（同名后写覆盖）
}
```

- `value` 对框架不透明；会话 id / JWT 都由应用决定。
- 同名 cookie：后收到的覆盖先前的（同 scope 下）。
- `secure`：连接未加密时，`CookieJar::attach` 不发送；服务端解析 `Set-Cookie` 时若连接不 secure 则拒绝带 `Secure` 的 cookie（或降级为不设置，需拍板）。

### 3.2 scope 匹配

```text
Connection            → 所有 location
LocationPrefix(prefix) → location == prefix || location.starts_with(prefix + "/")
```

`Path` 的映射规则：

- `Set-Cookie: sid=...; Path=/im/v1/admin` → `LocationPrefix("im/v1/admin")`（去掉前导 `/`，与 MPTP location 的写法对齐）；
- 缺省 `Path` → 用请求 location 的父前缀（与 HTTP default-path 同形）；
- `Path=/` → `Connection`。

### 3.3 线格式：复用 `Set-Cookie` / `Cookie`

建议**复用标准头名**（`StdHeaderKey::Set_Cookie` / `Cookie`），只支持子集：

```http
Set-Cookie: sid=abc123; Path=/im/v1/admin; Max-Age=3600; Secure
Cookie: sid=abc123
```

理由：

- 不需要新的标准头编号，HTTP gateway / 抓包工具可以直接理解；
- `Cookie` 请求头本来就是「多个 name=value 放在一个值里」，与单值 `Headers` 兼容；
- 与 HTTP 的差异集中在一个小的解析器里，文档明确「MPTP 子集」。

解析规则（v1）：

- 属性大小写不敏感；`Path` / `Max-Age` / `Secure` 解析；`Domain` / `Expires` / `HttpOnly` / `SameSite` 解析后忽略（`Expires` 可在后续版本支持）；
- 未知属性忽略（宽容），非法值默认丢弃整条 cookie（严格），可配置；
- `Max-Age <= 0` 表示删除同名 cookie。

### 3.4 单值 `Headers` 的现状（不是协议硬约束）

`Headers` 是 `Arc<BTreeMap<HeaderKey, HeaderVal>>`，因此**一个回复只能有一个 `Set-Cookie` 值**。

这不是 MPTP 协议的限制，而是当前头表实现的现状；对**单会话**没有影响：一个 `SessionId`
只占一个头值。只有当需要「同一回复下发多个独立 cookie」时才需要扩展：

- 多会话 / 多 cookie 的后续方案：
  1. 扩展 `Headers` 为多值 map（影响面大，需单独提案）；
  2. 用一个头值承载多个 `name=value`（`Cookie` 请求头本来就允许；`Set-Cookie` 需要自定义编码）；
  3. 用 `x-mptp-session` 自定义头承载「一个会话 + 多个 scope」，不追求 HTTP 兼容。

**v1 单会话单 cookie 足够**；多会话是扩展，不是本方案的前提。

---

## 4. 客户端设计

### 4.1 jar 放在哪：三条路线

| 方案 | 形态 | 优点 | 代价 |
| --- | --- | --- | --- |
| A（推荐） | `MptpClient` 增加 `jar_: Option<Arc<...>>`；`MptpSession` 持有同一句柄 | 所有 RPC helper（unary / Pull / Push）自动生效；宏与 helper 签名零改动；裸 MPTP 也能用 | 改 `MptpClient` 公共结构；`mptp_smux` 增加 cookie / 锁依赖 |
| B | `rpc_core_v1::client::ClientSession<C>` wrapper（持 `MptpClient` + jar），所有 helper 改收 `&ClientSession` | 分层干净，`mptp_smux` 不变 | 所有 helper / 宏生成代码 / 调用点都要改；Pull / Push 另做；公共 API 破坏更大 |
| C | jar 只挂在 `ClientProxy` 上 | 改动最小 | 只覆盖 typed unary，Pull / Push 仍手写；与「框架能力」目标不符 |

**推荐 A**：cookie 是传输层语义，放在 `MptpClient` 这一层最自然；RPC 层不需要知道 cookie。

### 4.2 挂载点

请求侧（所有路径的共同入口）：

```rust
// mptp_smux/src/client/client_.rs
async fn client_request_to_dock_async_<'f, C, K>(
    client: &'f MptpClient<C>,
    dock: Dock<C>,
    mut request: ReqPrefix<C>,
    cancel: K,
) -> Result<MptpSession<C>, ClientError>
where C: TrConfig, K: TrCancellationToken,
{
    if let Some(jar) = client.cookie_jar() {
        jar.lock_().attach(request.location(), client.is_secure(), request.headers_mut());
    }
    // ... 原逻辑
}
```

响应侧：

```rust
// mptp_smux/src/client/session.rs
async fn session_recv_resp_prefix_async_<'f, C, K>(
    session: &'f mut MptpSession<C>,
    cancel: K,
) -> Result<RespPrefix<C>, ClientError>
{
    let prefix = { /* 读前缀 */ };
    if let Some(jar) = session.cookie_jar()
        && (200..300).contains(&prefix.status().inner())   // 只在 2xx 处理 Set-Cookie
    {
        let location = session.request_location();  // 发起时保存
        jar.lock_().apply_response(location, prefix.headers(), session.now_ms());
    }
    // ... 原逻辑
}
```

- `MptpSession` 需要保存发起时的 `location`（`RequestPrefix::location()` 已在 `SessionCore` 里，可直接读）。
- `now_ms` 来源：`MptpClient` 构造时注入 `Arc<dyn Fn() -> u64 + Send + Sync>`（见 §10 未决问题）；不引入 `std::time`。
- Pull / Push / 错误回复都走同一前缀读取，因此 jar 自动生效；后缀流不参与 cookie。

### 4.3 优先级与显式覆盖

- 请求已有 `Cookie` 头：**显式值优先**，jar 只补缺失的 name；
- 请求已有 `Authorization` 头：与 jar 无关，照旧发送；
- `ClientProxy::with_headers` 仍然只影响单次调用；jar 是连接级默认值，二者合并顺序：jar 先、显式后。

### 4.4 并发与同步

- `MptpClient` 可能在 `Arc` 下被多任务共享；jar 必须可 `Send + Sync`（或明确只在单线程用）。
- 推荐用 `abs_sync::sync_mutex::Mutex<CookieJar>`（同家族、no_std）或 `spin`；**不引入 `std`**。
- 锁只在「attach / apply_response」的短临界区持有，不跨 `.await`。

### 4.5 生命周期

- `MptpClient` drop → jar drop（连接级会话结束）；如果要跨重连复用，应用把 `Arc<CookieJar>` 在重连时传给新的 `MptpClient`。
- `CookieJar::clear()` / `remove(name)` 供登出。

---

## 5. 服务端设计

### 5.1 回复头挂载点：`ServingSession` 的 pending headers

核心问题：中间件 / handler 必须在**回复前缀写出之前**放头；而宏生成的 handler 拿不到 `ctx`。最小改动是让 `ServingSession` 持有「待写回复头」，`response(status)` 自动合并：

```rust
impl<C> ServingSession<C> {
    /// 在回复前缀写出前追加一个回复头。
    pub fn set_response_header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal);

    /// 带条件的回复头：只有最终状态是 2xx 才写出。
    pub fn set_success_response_header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal);
}

impl<C> MptpContext<'_, C, S, R> {
    pub fn set_response_header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal) {
        self.session_mut().set_response_header(key, val);
    }
}
```

- `session.response(status)` 构造 `ResponseBuilder` 时，把 pending headers 合并进 prefix；
- `typed_response` / `write_typed_value_async` / `write_status_async` 都走 `session.response(...)`，因此**宏生成的 Call 自动获得该能力，不需要改宏**；
- `set_success_response_header` 解决「中间件提前签发，但 handler 可能失败」：只有 2xx 才写出（401/403 不泄漏新 session）；判定用 `(200..300).contains(&status.inner())`（`Status` 目前没有 `is_success` 辅助方法，可顺手加一个）。

### 5.2 宏生成 Call 的会话签发（运行时原语；宏级表达见 §13）

由于宏生成的业务 impl 看不到 `MptpContext`，`login` 不能直接写回复头。三种做法：

1. **中间件签发（推荐 v1）**：给端点挂 `IssueSession` 元数据；`SessionMiddleware` 在认证通过后生成 sid、存 store、`set_success_response_header("Set-Cookie", ...)`，再 `next(ctx)`。业务 `login` 只返回票据（或什么都不用改）。
2. **DTO 字段**：`AdminTicket` 增加 `session_id`（`u64` 保持 `Copy`），客户端保存；不需要回复头。缺点是每个 service 的登录 DTO 都要带字段，且 Pull 没有 DTO。
3. **框架把 `MptpContext` / 主体访问器注入 impl**：更大的 API 变更，属于后续提案（也是之前记录的「宏 impl 看不到 R」缺口）。

推荐 1 + 2 兜底：中间件负责发头（Pull / Call 一致），DTO 字段作为兼容路径。

### 5.3 `SessionStore` 抽象

```rust
// mptp_serving/src/session_.rs（建议）
pub struct SessionId([u8; 16]);

pub struct SessionRecord {
    pub user_id: u64,          // 应用自定义，或泛型 T
    pub expires_at_ms: u64,
}

pub trait TrSessionStore: Send + Sync {
    type Record: Clone + Send + Sync + 'static;
    fn load(&self, id: &SessionId) -> Option<Self::Record>;
    fn create(&self, record: Self::Record, ttl_ms: u64, now_ms: u64) -> SessionId;
    fn refresh(&self, id: &SessionId, ttl_ms: u64, now_ms: u64) -> bool;
    fn remove(&self, id: &SessionId);
}
```

- v1 只要求同步 store（内存 `HashMap` / `Hub`-backed）；Redis / 异步 store 后续用 `TrMayCancel` 版本。
- `TrSessionStore` 不要求 `std`；demo 的 `Hub` 用 `Mutex<HubInner_>` 实现。
- 跨连接会话放 store；连接内缓存可以再放 `S`（避免每请求查表）。

### 5.4 参考 `SessionMiddleware`

```rust
pub struct SessionMiddleware<Store> { store_: Arc<Store> }

impl<C, S, R, Store> TrMiddleware<C, S, R> for SessionMiddleware<Store>
where /* ... */
{
    fn call(...) -> MiddlewareFuture<'a> {
        // 1) 读 Cookie / Set-Session 里的 sid
        // 2) store.load(sid) -> principal
        // 3) 写 R.principal（或调用应用提供的 setter）
        // 4) 若端点挂了 IssueSession 且认证已通过：
        //    sid = store.create(...); ctx.set_success_response_header("Set-Cookie", ...)
        // 5) next(ctx).await
    }
}
```

- 与现有 `AdminAuthMiddleware` 的关系：`AdminAuthMiddleware` 负责「Bearer / session 认证 + AdminOnly 短路」；`SessionMiddleware` 负责「会话加载 / 续期 / 签发」。可以合成一个，也可以分层（认证 → 会话）。
- **`R` / `S` 是应用自定义类型**：框架级 `SessionMiddleware` 不能直接写 `R.principal` 或调用 `S` 的业务方法。三条路：
  1. 定义 trait（如 `TrSessionRequestState { fn set_principal(&mut self, p: Principal); }`），中间件泛型于 `R: TrSessionRequestState`；
  2. 框架固定一个 `MptpSessionState` 作为 `R`（应用自有状态放 `S`）；
  3. 框架只提供 `TrSessionStore` + `cookie` helper + pending-header 原语，**策略中间件由应用写**（最保守，v1 推荐）。
  本文 §5.4 的 `SessionMiddleware` 是示例形状，不是必须由框架提供的唯一实现；推荐 v1 走 3，等两个以上消费方后再上 1 / 2。
- 401 挑战：返回 `Ok(Some(Status::Unauthorized))`；如果要携带 `Set-Cookie` 删除过期会话，需要 `set_response_header` 在短路回复上也生效（`response(status)` 统一合并）。

### 5.5 请求侧 cookie 解析

- 提供 `fn cookie_value_(headers: &Headers, name: &str) -> Option<&str>`（`mptp_wire::cookie`）；
- 或 `MptpContext::cookie(name)` 的便利方法；
- 不强制走 `Cookie` 头：应用也可以用 `Authorization` / 自定义头，框架不绑定策略。

---

## 6. API 草案（汇总）

### 6.1 `mptp_wire`

```rust
pub mod cookie {
    pub struct Cookie { /* name, value, scope, max_age_ms, secure */ }
    pub enum CookieScope { Connection, LocationPrefix(String) }
    pub struct CookieJar { /* ... */ }

    impl Cookie {
        pub fn parse_set_cookie(location: &str, value: &str) -> Option<Cookie>;
        pub fn to_set_cookie_value(&self) -> String;
    }

    impl CookieJar {
        pub fn new() -> Self;
        pub fn apply_response(&mut self, location: &str, headers: &Headers, now_ms: u64);
        pub fn attach(&self, location: &str, secure: bool, now_ms: u64, headers: &mut Headers);
        pub fn clear(&mut self);
        pub fn remove(&mut self, name: &str);
    }
}
```

### 6.2 `mptp_smux::client`

```rust
impl<C: TrConfig> MptpClient<C> {
    /// 挂一个连接级 cookie jar（可选）。
    pub fn with_cookie_jar(self, jar: Arc<Mutex<CookieJar>>) -> Self;
    pub fn cookie_jar(&self) -> Option<&Arc<Mutex<CookieJar>>>;
    pub fn set_secure(&mut self, secure: bool); // 或随 config
}
```

### 6.3 `mptp_smux::serving` / `mptp_serving`

```rust
impl<C: TrConfig> ServingSession<C> {
    pub fn set_response_header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal);
    pub fn set_success_response_header(&mut self, key: impl Into<HeaderKey>, val: HeaderVal);
}
```

### 6.4 `mptp_serving::session_`

```rust
pub struct SessionId([u8; 16]);
pub trait TrSessionStore { /* §5.3 */ }
pub struct SessionMiddleware<Store> { /* §5.4 */ }
pub struct IssueSession; // 端点元数据：认证成功后签发会话
```

---

## 7. 兼容性与迁移

- 默认 `jar_ = None`：现有 `MptpClient::new` / RPC helper / 宏生成代码行为不变。
- `ClientProxy::with_headers` 仍是单次覆盖；新增的 jar 只补缺失的 `Cookie`。
- **公开 API 变更点（需确认）**：
  1. `MptpClient` 从 tuple struct 改为具名 struct（保留 `new`）。tuple 字段默认私有，下游本来就只能用 `new`；本 crate 内若有直接构造 / 模式匹配需同步改（当前 workspace 内只用 `new`）。仍按公开 API 变更走确认。
  2. `MptpSession` 增加 jar 句柄（内部构造，`pub(crate)` 不受影响）。
  3. `ServingSession` 增加 pending headers（新增方法，行为向后兼容：没有 pending 时与现在完全一致）。
- 若不想动 `MptpClient` 公共形状，可先走方案 B（`ClientSession` wrapper），但那样 `rpc_core_v1` 的所有 helper 与宏生成代码都要改签名，**总改动更大**。
- demo 迁移路径：`ImClient` 不再手写 `admin_headers_`，改为 `MptpClient::with_cookie_jar(...)`；`admin_login_async` 后 jar 自动保存 `Set-Cookie`，后续调用自动附头；`AdminAuthMiddleware` 改为先查 session store，再回退 Bearer（或只保留 session）。
- 现有 `rpc-client-headers` 的 C1 保留：显式 `with_headers` 仍是「一次性 / 特殊请求」的入口；jar 是「连接级默认凭据」。

---

## 8. 安全

- **sid 必须不可猜**：128-bit 随机；no_std 下用应用注入的 RNG（`getrandom` feature 或 `TrRng` trait），不要用递增计数器。
- **会话固定**：登录成功后轮换 sid；登出 / 提权时删除旧 sid。
- **TTL / 滑动过期**：`Max-Age` + 服务端 `expires_at`；刷新只延长，不改变 sid（或按策略轮换）。
- **撤销**：store 删除即失效；无状态签名 cookie 需要短 TTL + denylist。
- **不记录**：`Set-Cookie` / `Cookie` 值不进日志、错误体、URL；框架的 error hook 不应打印头表。
- **Secure**：传输未加密时不发 `Secure` cookie；`mptp_smux` 尚无 TLS，先用连接配置标志，等 TLS 后端再自动判断。
- **多主体复用连接**：连接级 jar 假设「一条连接一个主体」；如果多个逻辑客户端共享一条连接，必须每请求显式头或 per-substream 会话，不能用连接级 jar。

---

## 9. 分阶段与测试

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| P0 | `mptp_wire::cookie` 解析 / 序列化；`Set-Cookie` 子集；单测 | 合法 / 非法值、`Path` / `Max-Age` / `Secure`、删除、同名覆盖 |
| P1 | `CookieJar` + `MptpClient::with_cookie_jar`；请求 attach / 响应 apply | 连续 unary 自动带；过期不发；scope 不匹配不发；显式头优先；Pull 同样生效 |
| P2 | `ServingSession` pending headers + `response` 合并；宏 Call / 手写端点 / 401 都能带头 | 登录 200 带 `Set-Cookie`；403 不带；Pull 首项前带 |
| P3 | `TrSessionStore` + `SessionMiddleware` + `IssueSession` | 登录 → 后续请求免 header；过期 / 删除 401；跨重连 store 命中 |
| P4 | demo 迁移 + 回归 | admin 场景从 Bearer 切到 session；现有 25 场景 / 5 actor / 5 admin 全绿 |
| P5 | 文档 / 验收 | `mptp_rpc` README / dev-notes；`mptp_im_demo` ACCEPTANCE 增行 |

测试重点：

- 客户端：同连接连续 3 次 unary，只有第一次带 `Authorization`，后续带 `Cookie`；
- 服务端：`Set-Cookie` 只在 2xx；`Secure` 在非 secure 连接被拒；
- 作用域：`Path=im/v1/admin` 不影响 `im/v1/session`；
- 过期：客户端到点不发；服务端到点 401；
- 显式覆盖：请求已带 `Cookie: sid=manual`，jar 不覆盖；
- 多 cookie：v1 明确「只支持一个」，测试记录限制。

---

## 10. 未决问题

1. **头名**：复用 `Set-Cookie` / `Cookie`（推荐，HTTP 兼容）还是自定义 `Set-Session` / `Session`（语义更干净）？单值 `Headers` 下多 cookie 怎么办？
2. **时钟**：`CookieJar` 的 `now_ms` 从哪来？建议 `MptpClient::with_cookie_jar(jar, clock)`，`clock: Arc<dyn Fn() -> u64 + Send + Sync>`；或给 `TrConfig` 加 `now_ms`。
3. **锁**：`abs_sync` / `spin` / `std` feature？`MptpClient` 是否必须 `Sync`？
4. **RNG**：框架生成 sid 还是应用生成？no_std 下 `getrandom` 的 feature 策略。
5. **`MptpClient` 形状**：直接加字段（推荐）还是 wrapper？需要团队确认公开 API 变更。
6. **宏 Call 的回复头**：中间件 `IssueSession` + success-only 头够不够？还是要把 `MptpContext` 注入 impl？
7. **多 cookie / 多 scope**：v1 单 cookie 是否够？未来是否扩展 `Headers` 多值？
8. **跨重连**：jar 由应用跨重连复用，还是框架提供 `ClientSessionStore`？
9. **`R` 泛型约束**：`SessionMiddleware` 如何写主体？trait（`TrSessionRequestState`）/ 固定 `R` / 应用自写中间件（§5.4 推荐 v1 走第三条）。

---

## 11. 与 HTTP 的对照

| HTTP | 本提案（MPTP） |
| --- | --- |
| `Set-Cookie`（多值） | `Set-Cookie`（单值，v1 单 cookie） |
| `Cookie: a=1; b=2` | 同一个头值里多个 `name=value` |
| `Domain` | 不适用（单 peer） |
| `Path` | location 前缀 |
| `Expires` | 暂不支持，用 `Max-Age` |
| `Secure` | 连接配置的 secure 标志 |
| `HttpOnly` | 忽略（无 DOM） |
| `SameSite` | 忽略（无跨站导航） |
| 浏览器 cookie jar | `MptpClient` 的连接级 `CookieJar` |
| 服务端 session store | `TrSessionStore`（应用实现，`Hub` / Redis） |
| session middleware | `SessionMiddleware` + `IssueSession` 元数据 |


---

## 12. 扩展方案研究：固定 Dock 对 + 持久 channel（MPTP keep-alive）

> 结论先行：**上一版把「关闭 channel」当成 smux 的固定行为是错的**。关闭是 MPTP 层做的
> 决定：`MptpServlet` 在每次 handler 返回后调用 `ServingSession::finish_async`，客户端在
> `MptpSession` 结束后丢弃两个半边。smux 只提供双向字节流。把「一次请求后关闭」改成
> 「会话期间不关闭、同一 channel 连续跑 unary 请求」是 **MPTP 层**的改动，不需要改
> `smux_v1` 的 `WaitClose`。
>
> 这正是 HTTP/1.1 keep-alive 的形状：一条持久连接上依次 `请求1 → 响应1 → 请求2 → …`。
> MPTP 的前缀已经声明了体边界（`Body_Size` / `Body_Transfer: Chunked` / 无体），
> **unary 请求/回复是自定界的**，所以连续请求不需要新的顶层帧格式。

### 12.1 设想

```text
Session = { session_id, local_dock, remote_dock, channel }
- 会话开始时：客户端取一个 local dock 并保持 DockBinding；
- 在该 binding 上 open 一条 channel，保持 tx/rx 不关闭；
- 会话内所有 unary 请求都在这条 channel 上顺序发送、顺序收回复；
- 服务端 accept 一次，循环读请求前缀 / 处理 / 写回复；
- 会话结束（或错误）才关闭 channel；此时才可能出现一次 WaitClose。
```

与上一版的关键区别：**不是每请求 open 一条 channel**，而是**每会话 open 一条 channel**。
这样根本不会触发 `Duplicate` / `WaitClose`，也没有“每请求 1–5 秒”的问题。

### 12.2 `smux_v1` 与 MPTP 的职责边界

#### 12.2.1 `smux_v1` 只提供流；关闭由 MPTP 决定

- `smux_v1` 的 `ChannelTx` / `ChannelRx` 是双向字节流的两半；只要两半不被丢弃 / 半关闭，
  channel 就保持活跃；
- [`servlet_serve_session_async_`](../mptp_smux/src/serving/servlet_.rs) 在 handler 返回后：

  ```rust
  let dropped_req_bytes = if session.has_channel_() {
      session.finish_async().await?
  } else { 0usize };
  ```

- [`finish_session_async_`](../mptp_smux/src/serving/session_.rs) 做的是「丢弃剩余请求体 →
  半关闭写端 → 读尽对端 → 丢弃两个半边」，其中 `take_tx_` / `take_rx_` 就是关闭；
- 客户端 `MptpSession` 在响应读完后被 drop，两个半边随之 drop；`close_req_async` 是显式
  半关闭入口，typed unary 并没有调用它，但 drop 仍然会关闭。

因此：**channel 关闭是 MPTP 会话驱动的策略，不是 smux 的硬约束**。把 `finish_async` /
drop 换成“继续读下一个请求前缀”，channel 就活着；smux 的 `WaitClose` 只在真正关闭后
才出现。

#### 12.2.2 现有框架已经留了「自己驱动」的口子

[`ServingSession::take_parts`](../mptp_smux/src/serving/session_.rs) 可以拿走
`(ChannelTx, ChannelRx, Headers, String)`，之后 `has_channel_()` 为 false，
`MptpServlet` **不会**再调用 `finish_async`。也就是说，一个自定义的
`TrServeHandler` 完全可以在 handler 里接管两个半边，循环读下一个请求前缀；框架不会
替你关闭。

客户端侧目前没有对称的 `take_parts`（`MptpSession` 的 `SessionCore` 是私有的），所以
需要新增一个 `SessionChannel` / `MptpSession::take_channel`，但这是 MPTP 层的小口子，
不是 smux 改动。

#### 12.2.3 unary 的线格式自定界，但「一通道一请求」是当前协议契约

- 请求前缀声明体是 `Body_Size` / `Body_Transfer: Chunked` / 无体；体结束后就是下一条
  报文；
- 回复前缀同样声明体边界；
- 所以「请求1 前缀+体 → 回复1 前缀+体 → 请求2 前缀+体 → …」在**字节流上是可解析的**，
  顺序 unary 不需要新的帧格式；
- **但是** [`mptp-protocol` §2.2](mptp-protocol-20261020-0200.md) 当前明确规定：

  ```text
  一条通道只承载一次请求和它对应的回复；
  若要在一条通道上复用多次请求，必须另行定义请求边界；
  本文不定义，当前实现也不支持。
  ```

  也就是说，“不关闭 channel”本身不够；必须新增一个 **MPTP keep-alive 约定**：双方
  同意在同一 channel 上依次承载多次 unary 请求，并规定请求边界 = 上一条回复体结束之后
  的下一个请求前缀；没有 request id、没有流水线。
- 这个约定可以用一个头（`Connection: keep-alive` / `close`，或 `Mptp-Session: keep-alive`）
  协商；它改的是**协议语义**，不是 smux。
- 如果要**流水线 / 并发**（多个请求同时在途），就必须引入 request id / 新的帧格式；
  本方案只需要顺序，因此不做。

这只对 **没有 suffix 流** 的 unary 成立：`Pull` / `Push` / `Call` 的请求或回复 suffix
靠半关闭表示结束，半关闭后 channel 不能复用（见 §12.2.4）。

#### 12.2.4 哪些情况不能 keep-alive

- `Pull`：回复后缀流靠服务端半关闭表示结束；结束后写方向已关，不能复用；
- `Push`：请求后缀流靠客户端半关闭表示结束；同上；
- `Call` 若使用请求 / 回复 suffix：同上；
- 任何 `take_parts` 后由 handler 自己接管的 channel：框架不能再假设可复用，应由 handler
  决定关闭；
- 协议错误 / 405 / 无法完整排空请求体：建议关闭，不做 keep-alive（与 HTTP 的
  `Connection: close` 同义）。

#### 12.2.5 smux 的空闲超时会杀 channel

`smux_v1` 有 `max_channel_timeout`（默认 30 秒，连接级协商）：活跃 channel 长时间完全
静默会被判空闲关闭。keep-alive 会话需要：

- 会话内请求间隔小于超时；或
- 调大 `max_channel_timeout`（连接级 `BasicOpts`，最小 1 秒）；或
- 增加 MPTP 级心跳（例如一个 `Ping`/`Head` 请求，或空 body 的轻量调用）在空闲时续命。

这是传输层参数 / MPTP 心跳策略，仍然不需要改 `smux_v1` 的 `WaitClose`。

### 12.3 可行性格子（修正版）

| 方案 | 结果 |
| --- | --- |
| 固定 pair，**每请求 open/close 一条 channel** | `Duplicate`（活跃）或 `WaitClose`（关闭后 1–5s）；**不是本方案** |
| 固定 pair，**每会话一条 channel，unary 顺序复用** | **可行**：不触发 WaitClose；需要新增 MPTP keep-alive 协议约定 + 会话循环（不是仅“不关闭”） |
| 固定 pair，同一 channel 并发请求 | 单 channel 天然串行；要并发需请求 id / 流水线，超出本方案 |
| 固定 pair，Pull / Push / 任意 suffix 流 | 不可复用：suffix 以半关闭结束，必须关 channel |
| 固定 pair，会话结束后关闭 | 关闭后一次 WaitClose；只发生在会话边界，不影响请求延迟 |
| 只固定 remote dock | 当前 unary 已经固定 `0x80`；不提供会话身份 |
| 只固定 local dock、remote 变 | 不是固定 pair；会话身份仍靠 session id |

### 12.4 Path B（修正）：MPTP keep-alive session channel

这是「固定一对 dock + 有序无并发」的自洽方案，改动集中在 `mptp_smux` / `rpc_core_v1`，
**不碰 `smux_v1` 的 WaitClose**。

#### 12.4.1 客户端

```rust
pub struct SessionChannel<C: TrConfig> {
    // 保持 local dock 不被回收；会话期间不 drop
    binding_: DockBinding<MuxCfg<C>>,
    // 同一条 channel 的收发半边；请求之间不关闭
    tx_: ChannelTx<C>,
    rx_: ChannelRx<C>,
}

impl<C: TrConfig> SessionChannel<C> {
    pub async fn open_async(client: &MptpClient<C>) -> Result<Self, ClientError> {
        let mut binding = client.bind_async(Dock::unspecified()).await?; // 取任意空闲 dock
        let mut handle = binding.open_channel_async(C::default_serving_dock(), &mut &[][..]).await?;
        let (tx, rx) = handle.accept_async(&mut &[][..], prepare).await?;
        Ok(Self { binding_: binding, tx_: tx, rx_: rx })
    }

    pub async fn call_async<Req, Rep>(&mut self, path: &str, req: Req) -> Result<Rep, ClientError> {
        // 1) 写请求前缀（Body_Transfer: Chunked）+ 请求体
        // 2) 读回复前缀 + 回复体（自定界）
        // 3) 不关闭 tx/rx；状态复位，准备下一次
        todo!()
    }
}
```

- 第一请求的 `RequestPrefix` 带 `SessionId` / `Authorization`；后续请求可以只带
  `SessionId`（或什么都不带，服务端按 channel 绑定身份）；
- 同一会话一次只发一个请求（`&mut self` 天然串行）；要并发必须另开 channel 或做流水线；
- `SessionChannel` drop 时才关闭 channel；`DockBinding` drop 解绑。

#### 12.4.2 服务端

```rust
// 一个 TrServeHandler 包装器：接管一条 channel，循环处理 unary 请求
async fn serve_keep_alive_async<C, H>(servlet: &MptpServlet<C, H>, session: ServingSession<C>)
    -> Result<ServeOutcome, ServeError>
{
    let mut session = session;
    loop {
        // 1) 处理当前请求（router / handler / 写回复）
        servlet.handler_.handle_async(session.method(), &mut session).await?;

        // 2) 排空剩余请求体（但不半关闭！）
        session.drain_req_body_async().await?;

        // 3) 拿走两个半边；框架不再 finish
        let Some((tx, rx, _headers, _location)) = session.take_parts() else { break };
        if !session.keep_alive_requested() { break; } // Connection: close / suffix / 错误

        // 4) 读下一个请求前缀，重建 ServingSession
        let prefix = read_request_prefix_async(&mut rx).await?;
        session = ServingSession::new_(prefix, tx, rx)?;
    }
    Ok(ServeOutcome { dropped_req_bytes: 0 })
}
```

- 需要给 `ServingSession` 增加一个 `drain_req_body_async`（只排空、不半关闭）；
- `MptpServlet` 要么新增 `serve_session_keep_alive_async`，要么让 `TrServeHandler` 自己
  接管（现有 `take_parts` 已经允许后者）；
- 405 / 错误 / suffix 流：写回复后走 `finish_async` 关闭，不做 keep-alive。

#### 12.4.3 与固定 pair 的关系

- 客户端持 `DockBinding` + 一条 channel；`(local_dock, remote_dock)` 在会话期间不变；
- 服务端 accept 一次，channel 期间不重新 open，因此**不会**遇到 `Duplicate` /
  `WaitClose`；
- 服务端可以从 `ServingSession`（需加 accessor）或首请求的 `SessionId` 识别会话；
- 会话结束才关闭 channel，`WaitClose` 只出现一次。

### 12.5 改动范围与风险

需要改的地方（全部在 MPTP / RPC 层）：

1. `mptp_smux::client`：新增 `SessionChannel`（或 `MptpClient::open_session_async`），
   暴露「写请求前缀 / 读回复」的循环；保留 `MptpSession` 原有一次请求路径。
2. `mptp_smux::serving`：新增 keep-alive 驱动（包装 `ServingSession` / `take_parts`），
   增加 `drain_req_body_async` 与「是否允许复用」的判定。
3. `mptp_wire`：`Connection: keep-alive` / `close` 头或 `Mptp-Session` 约定；无需新帧格式。
4. `rpc_core_v1::client`：`ClientSession` 把 typed unary 调用映射到 `SessionChannel`；
   Pull / Push 仍走一次性 `MptpSession`。
5. 服务端认证：首请求带 `SessionId` / token；channel 绑定身份；后续请求可免头。
6. 空闲超时：MPTP 心跳或连接级 `max_channel_timeout` 配置；文档化。

风险：

- **单 channel 串行**：所有请求排队；长请求会阻塞后续请求（HOL blocking）。这正是
  「会话内不能并发」的代价，不是 bug；
- **请求体排空**：handler 若提前返回，必须先排空剩余请求体才能读下一个前缀，否则错位；
- **错误 / suffix**：必须关闭，不能复用；需要明确的 `Connection: close` 规则；
- **空闲超时**：30 秒静默会被 smux 关闭；需要心跳或调大超时；
- **客户端 API 复杂度**：`SessionChannel` 的取消 / 超时 / 错误恢复需要设计；
- **服务端 keep-alive 驱动**：要复用现有 router / `S`，但每请求重新构造 `R` 与
  `MptpContext`；`take_parts` 后 `ServingSession` 的 body/state 需要干净复位。

### 12.6 与 Path A 的关系

- **Path A（`SessionId` + 每请求 channel + jar/store）**：适合“不要求固定 pair、只要求
  登录一次自动带凭据”；改动最小，不碰 `mptp_smux` 的请求模型。
- **Path B（keep-alive session channel）**：适合“固定 pair、严格有序、无并发、想省掉
  每请求建流开销”；改动集中但更大。
- 两者可以共存：
  - `SessionChannel` 的**首请求**用 `SessionId` / Bearer 建立会话；
  - 短连接 / Pull / Push 走 Path A 的 jar；
  - 固定 pair 的长会话走 Path B。
- 不是二选一；建议先把 Path A 落地，再按需求加 Path B。

### 12.7 结论（修正）

1. **你说得对**：关闭 channel 是 MPTP 的决定；`WaitClose` 只在“每请求 open/close”时
   成为问题。固定 pair 的正确做法是**每会话一条持久 channel**，不是改 `smux_v1`。
   但要注意：MPTP 协议当前规定「一通道一请求」，所以这不是单纯的“不关闭”开关，而是
   要新增 **keep-alive 协议约定**（`Connection: keep-alive` + 请求边界规则）。
2. **可行**：`DockBinding` 可保持、`open_channel` 一次、unary 线格式自定界、服务端
   `take_parts` 已允许自定义驱动；因此 keep-alive 是 MPTP 层可实现的能力。
3. **不需要新帧格式**（unary 无 suffix）；需要的是 MPTP 会话循环、`Connection` 头 /
   策略、客户端 `SessionChannel`、服务端 keep-alive 驱动、请求体排空与空闲心跳。
4. **限制**：只适合 unary / 无 suffix；Pull / Push / suffix 必须关闭；单 channel 串行
   有 HOL blocking；空闲 30s 超时需要心跳或调参。
5. **建议**：v1 先做 Path A（cookie/session id + jar + store）；如果固定 pair 是硬需求，
   再做 Path B，并单独写一份 “MPTP session channel” 提案（帧格式、生命周期、流控、
   取消、测试矩阵）。
6. 上述结论来自 `mptp_smux` / `smux_v1` 代码与现有测试；Path B 的 keep-alive 循环尚未
   做 runtime spike，建议下一步先用一条 `Call` channel 手工发两次 unary 请求验证：
   - 服务端确实能读第二个前缀；
   - 请求体 / 回复体边界正确；
   - `finish_async` 不调用时 channel 保持；
   - 空闲 30s 的行为。



---

## 13. 决策：v1 不在 `#[rpc]` 宏里表达 Session / 授权

> 结论：**授权不是 RPC 协议的一部分，也不建议放进 `#[rpc]` 宏**。Cookie jar 负责
> “客户端自动带凭据”，服务端在组织 router 时挂 `SessionMiddleware` / 端点元数据负责
> “谁可以访问”。这两件事通过**通用头 + 401/403 状态码 + cookie 语义**形成不成文约定，
> 不需要宏、也不需要协议新增授权语义。宏只保留 API 形状 / 编解码 / 代理生成。

### 13.1 问题

之前考虑过：

```text
#[rpc(service = "...", session(AdminSession))]
#[rpc(session(AdminSession, public, issue))]
```

用宏声明“哪个 session scheme / 中间件介入”，让客户端代理和服务端 installer 自动生成
session 行为。现在重新评估：如果客户端已经有连接级 CookieJar，服务端完全可以在 router
装配时挂授权中间件，那么宏层面的显式声明是否仍有必要？

### 13.2 结论

v1 **不做**：

1. 不在 `#[rpc]` 增加 `session(...)` / `require = ...` / `issue = ...` 语法；
2. 不在 RPC 协议层表达授权语义；
3. 授权只作为**服务端 router / middleware / endpoint metadata** 的职责；
4. 客户端只负责**通用凭据携带**（CookieJar / 显式 header），不感知哪个端点要授权；
5. 登录签发是服务端 handler / middleware 的动作，客户端 jar 捕获 `Set-Session`。

保留的框架改动：

- `mptp_wire` / `mptp_smux` 的 CookieJar（客户端自动附 `Cookie`、处理 `Set-Cookie`）；
- `mptp_serving` 的 `SessionMiddleware` / `SessionStore` / `SessionGrant`；
- 端点元数据 `SessionRequired` / `SessionIssue`（或等价 marker）；
- 一个**与 session 无关的通用 installer hook**（可选，见 §13.7），用于给宏生成的端点
  附加任意元数据。

### 13.3 为什么不需要宏表达

#### 13.3.1 HTTP / ASP.NET Core 本身也不在协议里表达授权

- HTTP 协议只有通用头（`Cookie` / `Authorization`）和状态码（401/403）；它不知道
  “这个路径需要管理员”；
- ASP.NET Core 的 `[Authorize(Policy = "Admin")]` 是**服务端框架的端点元数据**，不是 HTTP；
  `UseAuthorization()` 中间件按元数据执行；`[AllowAnonymous]` 是显式豁免；
- 登录是 handler 里的 `SignInAsync(scheme, principal)`，不是路由 / 协议属性；
- 换一个客户端（curl、浏览器、另一个语言）完全不需要知道 policy，照样能访问：带上
  cookie / bearer，服务端决定 200 / 401 / 403。

参考：
[Policy-based authorization](https://learn.microsoft.com/en-us/aspnet/core/security/authorization/policies)、
[Cookie authentication](https://learn.microsoft.com/en-us/aspnet/core/security/authentication/cookie)、
[Minimal API auth](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/minimal-apis/security)、
[Custom policy providers](https://learn.microsoft.com/en-us/aspnet/core/security/authorization/custom-authorization-policy-providers)。

对应到 MPTP：

- `Cookie` / `Set-Cookie` / 401 / 403 = 通用传输语义；
- 哪个端点要授权 = 服务端 endpoint metadata / router 配置；
- “登录成功签发会话” = 服务端 handler / middleware；
- 客户端 jar = 浏览器 cookie jar，**不需要知道端点策略**。

#### 13.3.2 客户端本来就“不需要知道”

CookieJar 的设计就是“把当前连接可用的凭据附到请求上”：

- 服务端保护 `/im/v1/admin`，客户端照常发请求；
- 没有会话 → 服务端 401 → 客户端触发登录 / 重新 `login`；
- 有会话但无权限 → 403 → 客户端显示提示。

如果客户端预先知道“这个端点需要 admin session”，它能做的也只是提前报错或提前登录；
这只是 UX 优化，不是正确性要求。服务端才是授权事实的唯一来源。

#### 13.3.3 宏应保持“协议形状”职责

`#[rpc]` 当前负责：

- 请求枚举 / 方法表 / 类型头；
- dispatcher / installer；
- 客户端代理；
- 与业务 API 形状一一对应。

授权策略是**部署 / 装配期决策**：

- 同一个 service 可能在不同部署里挂不同策略；
- 策略变更不应该要求重新生成客户端或改协议；
- 多语言客户端不需要理解 Rust 宏里的 scheme marker；
- 宏解析器 / 模型 / 展开 / 客户端代理的复杂度会明显上升（见 §13.6）。

把授权塞进宏，会把“协议契约”和“服务端策略”耦合在一起；而 HTTP 的经验是把它们分开。

#### 13.3.4 “默契”是必要的，但不需要宏

客观默契一定存在，但它可以只是：

```text
- Cookie / Session 头名与格式（§3）；
- 401 = 没有有效会话；403 = 有会话但策略不允许；
- 登录端点成功后返回 Set-Session；
- 客户端 jar 自动保存 / 附上。
```

这些是**协议 / 传输约定**，不是每个 service 的宏声明。宏声明反而会把“默契”变成
“每个方法都要重复标注”，增加漂移风险。

### 13.4 服务端最小机制（不依赖宏）

#### 13.4.1 元数据 + 中间件

```rust
// 端点元数据：声明需要哪个 scheme（scheme 逻辑在 handler 里）
#[derive(Clone, Copy, Debug)]
pub struct SessionRequired<S>(pub PhantomData<S>);

// 中间件：读元数据 -> 查 registry -> 加载 SessionGrant -> 拒绝 / 放行
let router = router.use_middleware(SessionMiddleware::new(registry));
```

装配时手工 / 半自动挂元数据：

```rust
// 手写端点
Endpoint::new(K_ADMIN_EVENTS, MethodSet::PULL, Arc::new(AdminEventsHandler::new_(ctx)))
    .with_metadata(admin_events_info())
    .with_metadata(SessionRequired::<AdminSession>(PhantomData));

// 宏生成的端点：用通用 installer hook（§13.7）或 with_group_metadata
```

#### 13.4.2 默认拒绝 / 显式豁免

比“逐个端点加 require”更安全的是**默认拒绝**：

- `SessionMiddleware` 默认拒绝没有 `Public` 标记的端点；
- 公开端点显式 `Public`（登录 / 健康检查 / 大厅快照）；
- 或按前缀保护：`protect_prefix("im/v1/admin")`，其他前缀放行。

两种都可接受；关键是有**一处集中策略**，而不是散落在业务 handler 的 `if` 里。
ASP.NET Core 的 fallback policy 同理：
[Default and fallback policies](https://learn.microsoft.com/en-us/aspnet/core/security/authorization/policies#default-and-fallback-policies)。

#### 13.4.3 登录 / 登出

- 登录端点标 `Public`，handler / middleware 在凭据校验成功后：
  - 生成 session record；
  - 写 `Set-Session` 响应头（仅 2xx）；
  - 客户端 jar 自动保存。
- 登出：删除 record + 清 cookie。
- 宏生成的 Call impl 没有 `MptpContext`（现有边界），因此：
  - 登录优先写成手写 `TrEndpointHandler`，或
  - 由 `SessionMiddleware` 在配置的 login 路径上代签，或
  - 后续给宏加一个**通用结果 hook**（不是 session 专用语法）。

#### 13.4.4 方法级授权

Call service 只有一个 `CALL /<prefix>` 端点，中间件在解码请求枚举之前看不到 operation。
因此：

- service 级策略 → endpoint metadata / middleware；
- 方法级策略 → dispatcher hook（如果需要）或**按策略拆分 service**；
- 这是既有边界，不因为“宏表达”而消失。

### 13.5 客户端最小机制

```rust
let client = ImClient::connect_admin_async_(addr, token).await?;
// 连接级 jar 自动处理 Set-Session / Cookie；业务方法不需要 session 参数
let snap = client.admin_snapshot_async().await?;
```

- 客户端**不解析**端点策略；
- jar 把当前连接可用的 cookie 附到每个请求；
- 401 → 触发登录 / 重新认证；403 → 提示无权限；
- 多 scheme：jar 可以保存多个 cookie，服务端按请求路径 / scheme 选择；
- 可选 `ClientSession` 只用于 UX（提前提示 / 预登录），不是正确性依赖。

### 13.6 宏表达能带来什么 / 代价

**收益（主要是开发体验）**：

- 编译期 / 代码生成层能看到“这个 service 需要 AdminSession”；
- 多语言 typed client 生成时能导出 security scheme（类似 OpenAPI）；
- 客户端可以提前失败 / 自动选择 scheme；
- 策略与 API 声明放在一起，便于 review。

**代价**：

- 宏解析器 / 模型 / 展开 / 客户端代理都要增加 session 概念；
- 协议 / 生成代码与一种授权模型耦合；策略变更可能需要重新生成客户端；
- `impl_of` 看不到 trait 属性的跨宏问题需要 marker 常量等额外机制；
- 方法级策略仍要 dispatcher 检查，宏并不能消除这个边界；
- 多语言 / 非 Rust 客户端无法复用宏里的声明。

**判断**：v1 的收益不足以抵消复杂度；先把 cookie jar + 服务端中间件做稳。
如果将来出现“多语言 generated client 需要 security scheme 元数据”或“客户端必须编译期
知道会话要求”的强需求，再在**独立提案**里加宏表达，且应作为**可选元数据导出**，而不是
协议必需。

### 13.7 唯一建议的宏改动：通用 metadata hook（可选）

现在宏生成的 Call service `install_into` 只 `.with_metadata(SERVICE_INFO_)`，外部拿不到
endpoint 去追加 `SessionRequired`。两个选择：

1. **不做**：用 `with_group_metadata` + 绑定顺序（demo 当前做法），或按前缀策略；
2. **加一个与 session 无关的通用 hook**：

```rust
pub fn install_into_with_metadata<C, S, R, F>(
    router: RadixRouter<C, S, R, Global>,
    svc: Arc<#self_ty>,
    decorate: F,
) -> Result<RadixRouter<C, S, R, Global>, InsertError>
where
    F: FnOnce(Endpoint<C, S, R, Global>) -> Endpoint<C, S, R, Global>,
{
    let endpoint = decorate(
        Endpoint::new(PREFIX, MethodSet::CALL, Arc::new(ServiceDispatcher { svc_: svc }))
            .with_metadata(SERVICE_INFO_),
    );
    router.bind_endpoint(endpoint)
}
```

- 不解析任何 session 语法；只是让装配者能附加任意元数据；
- 手写端点 / controller / 自由函数也可以用同一个模式；
- 这是宏复杂度最低、收益最直接的一步。

### 13.8 与 §5 / §12 的关系

- §5 的 `SessionStore` / 回复头 / 中间件，是本节服务端机制的核心；
- §12 的持久 channel 首请求建立会话，仍然通过 `Set-Session` / `ctx.sign_in` /
  `SessionMiddleware`，与宏无关；
- §3 的 cookie 语义是客户端自动附凭据的唯一“默契”，不需要每个 RPC 方法声明。

### 13.9 最终建议

1. v1 **不**在 `#[rpc]` 增加 session 语法；
2. 协议层只保留通用 `Cookie` / `Set-Session` / 401 / 403；
3. 服务端集中挂 `SessionMiddleware` + endpoint metadata / 前缀策略，默认拒绝或显式保护；
4. 客户端连接级 cookie jar 自动附凭据，不感知端点策略；
5. 如果确实需要给宏生成端点附加元数据，只加**通用** `install_into_with_metadata` hook；
6. 宏级 session 声明作为未来可选提案，条件是多语言 typed client / 编译期契约成为刚需。
