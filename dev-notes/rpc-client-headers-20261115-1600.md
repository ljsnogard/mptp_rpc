# 决策记录：typed unary 客户端携带请求头（C1）

- 日期：2026-11-15
- 状态：**已实施**
- 触发方：`mptp_im_demo` 的 monitor admin 授权切面
  （[`mptp_im_demo/dev-notes/authz-middleware-20261115-1600.md`](../../mptp_im_demo/dev-notes/authz-middleware-20261115-1600.md)）
- 基线：`mptp_rpc` `74cfd01`

---

## 1. 问题

`mptp_serving` 的授权切面要求凭据走标准请求头
`Authorization: Bearer <token>`，从端点元数据 + 顶层中间件短路。服务端已经够用，但
**客户端 typed unary 无法在业务方法参数表不变的前提下带额外请求头**：

- `rpc_core_v1::client::call_endpoint_typed_async` 自己构造 `Headers`（类型头 +
  `Body_Transfer`），没有 headers 参数；
- 宏生成的客户端代理只调这个入口；
- Pull 侧已有低层 `RequestBuilder::header(...)` / demo 的
  `open_pull_with_headers_async`，但那绕过了 typed unary 代理；
- 业务方法如果为了带 token 增加参数，就把「横切数据」重新塞回业务 API。

## 2. 选型

| 方案 | 形态 | 结论 |
| --- | --- | --- |
| C1 | `ClientProxy` 携带可选 `Headers`；新增 `call_endpoint_typed_with_headers_async`；宏生成的代理把代理级头交给新入口 | **采用**：只影响显式 `with_headers` 的调用；业务 API 零参数；可复用到其他 service |
| C2 | `MptpClient` 连接级默认 headers，每次请求合并 | 不采用：token 会带到该连接的所有请求，语义太宽；且要改 `mptp_smux` 的 `MptpClient` / `RequestBuilder` |
| C3 | demo 局部 wire 适配器（`request_prefix` + 手工编解码 / 调 `call_endpoint_typed_async` 的复制版） | 不采用：绕过 typed proxy，重复 wire / 解码代码；只适合一次性 spike |

## 3. 变更

### 3.1 `rpc_core_v1`

```rust
// client/proxy_.rs
pub struct ClientProxy<'a, C, I> {
    client_: &'a MptpClient<C>,
    headers_: Option<Headers>,   // 新增
    _iface_: PhantomData<I>,
}

impl ClientProxy {
    pub const fn new(client: &'a MptpClient<C>) -> Self; // headers_ = None
    pub fn with_headers(mut self, headers: Headers) -> Self; // 消费式
    pub fn headers(&self) -> Option<&Headers>;
}

// 新公开入口；旧入口签名 / 语义不变（等价于传 None）
pub fn call_endpoint_typed_with_headers_async<'f, C, Req, Rep>(
    client: &'f MptpClient<C>,
    method: AccessMethod,
    path: &'static str,
    info: &'static RpcDataTypeInfo,
    request: Req,
    extra_headers: Option<&Headers>,
) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Rep>> + 'f;
```

合并顺序：**额外头先写、协议头后写**。因此 `Body_Transfer` / `Body_Type` /
`Data_Type_Id` / `Accept` 不会被调用方覆盖；额外头只用于 `Authorization` 这类业务 / 安全
头。`extra_headers.cloned()` 在进入 future 前完成，请求前缀仍然独占头表。

### 3.2 `rpc_svc_macro`

生成的客户端代理方法体从

```text
call_endpoint_typed_async(client, Call, PREFIX, &INFO, body)
```

改为

```text
call_endpoint_typed_with_headers_async(client, Call, PREFIX, &INFO, body, self.headers())
```

业务 trait 的方法签名、请求枚举、服务端 dispatcher / `install_into` 全部不变。

### 3.3 导出

`rpc_core_v1::client::call_endpoint_typed_with_headers_async` 与
`rpc_core_v1::preludes` 同步导出。

## 4. 兼容性与迁移

- **源码兼容**：`call_endpoint_typed_async`、`ClientProxy::new`、`ClientProxy::client`
  签名与行为不变；宏生成代码仍是「调用一个公开入口」，只是入口换了，业务 trait 不变。
- **行为兼容**：没有 `with_headers` 的调用与之前完全一致（`None` 不写任何额外头）。
- **重新编译即可**：宏与 `rpc_core_v1` 在同一个 workspace / 相同版本发布，不存在
  「旧宏 + 新库」或「新宏 + 旧库」的中间态需要支持。
- **其他使用方**：`mptp_smux_v1_demo` 等宏使用方 `cargo check --workspace --all-targets`
  通过，无需改代码。

## 5. 验证

```bash
cargo check --workspace --all-targets        # mptp_rpc：宏生成代码 + demo 全部编译
```

`mptp_im_demo` 侧的真实网络证据：

- `im_testkit::tests::admin`（5 场景）：头缺失 / 错误 / 格式错 → `401`；正确头 →
  login / snapshot / room_history / admin Pull；普通端点不受影响；
- `im_monitor` 无界面流程：UI token 输入 → actor 构造 header → 真实服务端监控流。

## 6. 未做 / 后续

- C2（连接级默认 headers）：如确实需要「一次设置、所有请求携带」，再在这个入口之外单独
  评估，不混进本轮。
- `call_endpoint_async` / `call_endpoint_no_body_async` 的 headers 变体：目前 demo 不需要；
  需要时按同一模式（`*_with_headers_async`）扩展，先提案。
- Pull typed 代理（`open_pull_async`）的 headers 参数：demo 的 Pull 适配器已经用
  `RequestBuilder::header(...)` 实现（`open_pull_with_headers_async`），本轮不改框架。
