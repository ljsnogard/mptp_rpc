# mptp_smux

`mptp_smux` 是 **MPTP（Multiplexed P2P Transportation Protocol）在 `smux_v1` 多流传输层上的具体实现**。

关系类似 **HTTP over TCP**：MPTP 定义一条字节流通道上「一次请求 / 回复长什么样」，`smux_v1`
负责提供可靠、有序、双向、可并行且支持半关闭的子流。`mptp_smux` 把两者接起来。

> **当前状态**：库本体是 `#![no_std]` + `alloc`。`client` / `server` 是独立 feature，
> 默认同时开启。**`serving` 模块不再包含任何 router**——路由、鉴权、方法分派都由应用层的
> 单一 handler 决定。

## 它是什么

- MPTP 的 **smux_v1 传输绑定**：dock 寻址、开子流 / 接子流、ring 内存注入、半关闭与收尾；
- **一次请求的会话对象**：客户端 `MptpSession`、服务端 `ServingSession`，两者共享
  `SessionCore`（两个半边、请求前缀、体边界、体进度、收尾原语）；
- **七种 AccessMethod 的标准入口**：客户端 `RequestBuilder` + 方法糖，服务端单一
  `TrServeHandler` + `ResponseBuilder`；
- **单连接上的多子流**：一条 MPTP 连接可以并行持有多个会话；`mptp_smux` 不替调用者
  spawn，也不管理多连接；
- 两端对等：同一条 `smux_v1` 连接的两端都可以同时发请求和接请求。

当前 MPTP 的一条通道只承载**一次**请求 / 回复；没有请求 ID，也没有通道内复用帧。
`mptp_smux` 不会自行添加复用语义。

## 它不是什么

`mptp_smux` **不是**：

- MPTP 协议本身——协议形状、访问方法、状态码、标准头与体声明属于 `abs_mptp`；
  体帧格式由 `mptp_wire` 实现；
- RPC 框架——service trait、`#[rpc]` 宏、方法派发、RPC 错误体属于 `abs_rpc_serv` /
  `rpc_core_v1`；
- 业务 body 编解码器——MessagePack / JSON / 自定义类型由应用自己决定；
- 应用路由框架——路径匹配、分层、运行期增删由应用 / `rpc_core_v1` 决定；
- 多连接服务器框架——监听拓扑、连接池、负载均衡、服务器级资源管理不在本 crate；
- 运行时或服务器框架——不选 tokio / compio / smol，不替调用者 spawn，不管 socket / TLS；
- QUIC 的抽象层——`mptp_quic` 如果出现，是它的**兄弟绑定**，不是它的子类型。

## 分层位置

```text
┌─────────────────────────────────────────────────────────────┐
│ 应用 / RPC：业务类型、序列化、service、路由、连接管理        │
│   rpc_core_v1 / abs_rpc_serv / 应用代码                      │
├─────────────────────────────────────────────────────────────┤
│ MPTP：前缀、头表、体边界、后缀流、一次请求的会话顺序          │
│   abs_mptp（形状） + mptp_wire（具体线格式）                  │
├─────────────────────────────────────────────────────────────┤
│ 多流传输：open / accept / read / write / half-close / reset   │
│   smux_v1（当前） / QUIC（候选）                              │
├─────────────────────────────────────────────────────────────┤
│ 字节流载体：TCP / UDS / ...                                   │
└─────────────────────────────────────────────────────────────┘
```

## 客户端：七种方法 + 统一 builder

```rust
use abs_mptp::spec::Status;
use mptp_smux::client::MptpClient;

let mut resp = client
    .post("/api/v1/goods")
    .header(abs_mptp::spec::StdHeaderKey::Data_Type_Id, type_id_text)
    .body_bytes(&encoded)
    .send_async()
    .await?;

if resp.status() != Status::Ok {
    return Err(/* ... */);
}
let mut body = resp.body_async().await?;
let value = decode(&mut body).await?;
```

| 方法糖 | 方法 | 默认语义 |
| --- | --- | --- |
| `head()` | `Head` | 请求 / 回复都不应有体 |
| `view()` | `View` | 请求通常无体，回复通常有体 |
| `post()` | `Post` | 请求通常有体 |
| `drop()` | `Drop` | 请求 / 回复都不应有体 |
| `push()` | `Push` | 请求后缀流必需 |
| `pull()` | `Pull` | 回复后缀流由服务端写 |
| `invoke()` | `Call` | 请求 / 回复体通常都有，后缀流可选 |

`invoke` 而不是 `call`，是为了避免与 RPC 层 `TrRpcClientExt::call::<I>()` 的固有方法
同名（固有方法会遮蔽扩展 trait 方法）。

方法糖只提供**可发现的入口**与默认语义；实际有无体仍由 `Body_Size` / `Body_Transfer`
两个标准头唯一决定。需要完全自定义前缀时用 `MptpClient::request_prefix`，或退回
`MptpClient::request_async` / `request_to_dock_async`。

`Body_Size` / `Body_Transfer` 由 `.body_bytes` / `.body_sized` / `.body_chunked` 写入；
`.suffix` 只挂请求后缀源，不写头。

## 服务端：`MptpServlet` + 单一 handler

```rust
use mptp_smux::serving::{MptpServlet, ServeOutcome, ServingSession, TrServeHandler};

struct EchoHandler;

impl TrServeHandler<ServingCfg> for EchoHandler {
    type Err = HandlerError;

    fn allowed_methods(&self) -> MethodSet {
        MethodSet::CALL | MethodSet::POST
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

let servlet = MptpServlet::<ServingCfg, EchoHandler>::new(EchoHandler);
let mut listener = servlet.listen_async(&mut binding, 8).await?;

loop {
    let session = servlet.accept_async(&mut listener).await?;
    // 单线程 demo：inline；真实并发：交给调用者自己的 executor
    servlet.serve_session_async(session).await?;
}
```

- `MptpServlet` 只服务**一条连接**；它持有一个**单一 handler**，不管理多连接。
- `accept_async` 返回可移动的 `ServingSession`，调用者可以用自己的 executor 并发处理
  多条子流；`mptp_smux` 不 spawn。
- `allowed_methods()` 缺省为全部；不在集合内的方法由框架写 `405` + `Allow`，不进入
  handler。
- **没有 404 框架路径**：404 是 handler 的业务响应，用
  `session.response(Status::NotFound).no_body_async()` 写。
- 回复 helper：`no_body_async` / `sized_async` / `chunked_async` / `suffix_async`。
  `Head` / `Drop` 的体方法会被拒绝；`no_body_async` 也会拒绝已经声明了体的前缀。
- handler 未写回复就返回：框架写 `500` 无体并收尾；handler 返回错误：同样兜底
  `500`，错误经 `ServeError::Target` 上报，不泄漏到线上。
- handler 未消费的请求体 / 后缀由框架收尾时代读丢弃（drain），不主动 reset。
- 旧 `TrReqHandler` 用 `LegacyHandler::new(handler)` 适配；适配器把两个半边按值交给
  旧 handler，旧 handler 自行收尾。

## `mptp_wire` 分离后，这一层还剩什么

| 职责 | `mptp_wire` | `mptp_smux` | `mptp_quic`（未来） |
| --- | --- | --- | --- |
| 前缀 / 头表 / 限长体 / 分块体编解码 | **负责** | 不负责 | 不负责 |
| 一次请求的会话顺序 | 不负责 | **负责**（`SessionCore` + 两端会话） | 应复用同一份会话逻辑 |
| 子流 open / accept / reset | 不负责 | smux 绑定 | QUIC 绑定 |
| 子流标识 | 不负责 | dock | stream ID |
| 每子流内存 / 缓冲 | 不负责 | ring 注入 | QUIC buffer / flow control |
| 半关闭 / reset 的传输映射 | 不负责 | smux FIN / RESET | QUIC FIN / RESET_STREAM |
| 客户端 / 服务端角色包装 | 不负责 | `MptpClient` / `MptpServlet` | 对应 QUIC 包装 |
| 一次请求交给 handler | 不负责 | `serve_session_async` | 对应 QUIC 循环 |
| 取消传播 | 不负责 | 传输 await 点 | 传输 await 点 |
| 应用路由 | 不负责 | **完全不负责** | 同左 |

## feature

```toml
[features]
default = ["client", "server"]
client = []
server = []
```

| feature | 提供 |
| --- | --- |
| `default` | `client` + `server` |
| `client` | `MptpClient` / `MptpSession` / `RequestBuilder` / `Response` / 客户端配置 / `ClientPrepCtx` |
| `server` | `MptpServlet` / `ServingSession` / `TrServeHandler` / `ResponseBuilder` / 服务端配置 / `ServingPrepCtx` |

`messaging`（`mptp_wire` 别名）、`K_DEFAULT_SERVING_DOCK`、`ring_prepare_` 等两端共享面
始终编译，不设 feature。

## `no_std`

库本体是 `#![no_std]` + `extern crate alloc`：

- 错误类型实现 `core::error::Error`；
- `thiserror` 关闭默认 feature；
- 没有 `futures`、没有 `std::sync::OnceLock`；
- `String` / `format!` 从会话实现中移除，改用结构化错误变体与 `alloc::string::String`；
- `tokio` 只在 dev-dependencies。

验证：

```console
cargo check -p mptp_smux --no-default-features
cargo check -p mptp_smux --no-default-features --features client
cargo check -p mptp_smux --no-default-features --features server
cargo check -p mptp_smux --all-features
```

## 与 `mptp_quic`

`mptp_quic` 不是 `mptp_smux` 的子类型；它自己负责 QUIC 特有的连接、stream、buffer、
FIN / RESET 与关闭。它应当复用：

- `mptp_wire` 的前缀 / 头表 / 体格式；
- `abs_mptp` 的协议 trait 与 `TrReqHandler`；
- 目标中的共享 `mptp_session` 会话状态机。

本次不新建 `mptp_session`：客户端 / 服务端先共享 `SessionCore`，并记录抽取接口草案
（见 [`dev-notes/mptp-smux-refactor-20261111-0650.md`](../dev-notes/mptp-smux-refactor-20261111-0650.md) §5）。
`mptp_quic` 不应依赖 `mptp_smux`，也不应复制一份 `MptpSession`。

## 相关 crate

| crate | 关系 |
| --- | --- |
| `abs_mptp` | MPTP 协议形状、访问方法 / 状态码 / 标准头、会话与 handler trait |
| `mptp_wire` | 前缀、头表、限长 / 分块体与 MessagePack 线格式；本 crate 的兼容别名指向它 |
| `abs_smux` / `smux_v1` | 多流传输与连接实现 |
| `abs_buff` | ring / 段读写接口 |
| `rpc_core_v1` / `abs_rpc_serv` | RPC 语义层、`RpcRouter` 与顶层 `Router`（路由已迁到这里） |
| `rpc_svc_macro` | 经 `rpc_core_v1` 使用；示例中的路由来自 `rpc_core_v1::serving` |

## 开发与验证

```console
# 单元测试
cargo test -p mptp_smux

# 真实 TCP 端到端（mptp_smux_v1_demo）
just e2e-smux

# 全量测试与静态检查
just unit
just clippy
```

## 文档

- 协议线格式：[`dev-notes/mptp-protocol-20261020-0200.md`](../dev-notes/mptp-protocol-20261020-0200.md)
- 本次重构设计：[`dev-notes/mptp-smux-refactor-20261111-0650.md`](../dev-notes/mptp-smux-refactor-20261111-0650.md)
- 职责边界：[`dev-notes/mptp-smux-20261110-0730.md`](../dev-notes/mptp-smux-20261110-0730.md)
- RPC 框架设计：[`DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)

---

**English summary.** `mptp_smux` is the concrete MPTP binding over `smux_v1`. It owns
sub-stream lifecycle, ring injection, the per-request session state machine, and the
client / server role wrappers. The client exposes per-method sugar over one
`RequestBuilder`; the server exposes `MptpServlet` with a **single** `TrServeHandler` and
`ServingSession`/`ResponseBuilder` helpers. There is **no router** in this crate: routing
belongs to the application or `rpc_core_v1`. Client and server sessions share
`SessionCore`; many sessions can run concurrently on one connection, but the crate never
spawns. The library is `#![no_std]` + `alloc`, with `client` / `server` features. A future
`mptp_quic` should share `mptp_wire` and the planned `mptp_session`, never depend on
`mptp_smux`.
