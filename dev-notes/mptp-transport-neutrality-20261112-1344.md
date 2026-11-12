# `mptp_smux` 到 `mptp_quic`：哪些设计原则可迁移，哪些还不行

- 日期：2026-11-12 13:44
- 状态：**讨论稿**；本文不构成公共 API 决策，也不改动代码
- 背景：demo 已经证明业务参数可以在不改 `mptp_smux` 的情况下扩张；现在要问的是
  **如果存在 `mptp_quic`，这套设计是否还能“零改动”迁移**。
- 相关文档：
  - [`mptp-smux-refactor-20261111-0650.md`](mptp-smux-refactor-20261111-0650.md)
  - [`scene-param-evolution-20261112-1251.md`](scene-param-evolution-20261112-1251.md)
  - [`../mptp_smux/README.md`](../mptp_smux/README.md)

## 0. 结论先行

**设计原则可以迁移；当前代码还不能零改动迁移。**

- 可迁移的是 **MPTP 语义**：一条请求一条子流、前缀 / 定长或分块体 / 后缀流、
  单 handler、405/500 兜底、取消传播、半关闭、不隐藏 spawn、客户端 / 服务端配置分离。
- 不能直接迁移的是 **smux 绑定形状**：`Dock` / `DockBinding`、ring 内存准备、
  `abs_smux` 的连接 / 子流 trait，以及 `mptp_smux` 这个“会话驱动 + smux 绑定”合一的
  crate 边界。
- 因此，用“加一个 `mptp_quic` crate，其他代码不动”作为稳定性判据，当前**还没有达到**。
  更准确的评价是：**MPTP 语义层和 `mptp_wire` 是稳定的，传输绑定层还没有被第二个
  传输实现检验过。**

## 1. 现在的分层实际长什么样

```text
mptp_wire          // 前缀 / 头表 / 定长体 / 分块体 / 后缀：不依赖 smux、dock、ring
  ↑
abs_mptp           // 协议 trait + 会话状态 + handler trait
  ↑   ↑
  │   └── abs_smux // 连接 / dock / 子流 / ring：smux 形状
  ↑
mptp_smux          // MPTP 会话驱动 + abs_smux 绑定 + client / servlet
  ↑
应用（demo）
```

看起来是「协议 / 绑定 / 应用」三层，但有一个关键的反向依赖：

- [`abs_mptp/src/client/config.rs:1`](../abs_mptp/src/client/config.rs#L1) 直接
  `use abs_smux::{...}`；
- [`abs_mptp/src/serving/config.rs:14`](../abs_mptp/src/serving/config.rs#L14) 同样
  直接依赖 `abs_smux`；
- 配置里的 `MuxConn: TrConnection`、`Dock`、`DockBinding`、`ChannelHandle` 都来自
  `abs_smux`，而不是一个传输中立的 mux 抽象。

所以 `abs_mptp` 的“协议层”实际上已经**钉死在 smux 的抽象形状**上。它不是“MPTP 协议
层”，而是“MPTP 协议 + smux 绑定形状”的混合层。

另一个信号：workspace 里已经有一个 [`abs_mptp_smux`](../abs_mptp_smux/src/lib.rs)，
但 workspace 内没有下游依赖它；`mptp_smux` 自己复制了一份相似的 config trait。
这说明“MPTP 与 smux 的分层”曾经尝试过，但还没有收敛成一个真正稳定的 seam。

## 2. 哪些东西可以原样迁移

这些是 **MPTP 语义**，与底层是多流 smux 还是多流 QUIC 无关：

| 原则 | 迁移到 QUIC | 说明 |
| --- | --- | --- |
| 一条请求一条子流 | 是 | QUIC 的双向 stream 天然对应 |
| 前缀 → 体 → 后缀的顺序 | 是 | 由 `mptp_wire` 拥有 |
| 前缀 / 体 / 后缀都是裸字节流 | 是 | 不依赖帧或 dock |
| 单 handler / 单 servlet 处理一条连接 | 是 | handler 只看到 `ServingSession` |
| 405 由 `allowed_methods` 处理、500 兜底 | 是 | servlet 语义 |
| 请求体 / 后缀的取消传播 | 是 | 取消令牌与传输无关 |
| 后缀半关闭、响应收尾 | 是 | QUIC 也有 FIN / 半关闭，只是 API 不同 |
| 框架不隐藏 spawn | 是 | 由调用者决定 executor |
| 客户端 / 服务端配置分离 | 是 | 两端可能独立升级 |
| ring 内存由调用者注入 | **要改** | 这是 smux / abs_buff 的模型，不是 MPTP 语义 |

前 9 条说明：`mptp_smux` 的**会话驱动和 handler 模型**确实站在正确的抽象层次上。
`mptp_wire` 也明确声明自己不依赖 smux / dock / ring（[`mptp_wire/src/lib.rs:11`](../mptp_wire/src/lib.rs#L11)）。

## 3. 哪些东西是 smux 泄漏

### 3.1 `Dock` 是传输概念，不是 MPTP 概念

`abs_smux::conf::TrMuxConfig` 有 `Data` 和 `Dock` 两个关联类型；`Dock` 用来标识
子流的 source / target。`mptp_smux` 的缺省服务垛口是 `0x80`，但 `0x80` 并不在 MPTP
请求前缀里——它只存在于 smux 绑定中：

- `MptpClient::request_to_dock_async` 选择本端要 bind 的 dock；
- `MptpServlet::listen_async` 接收一个 `DockBinding`；
- `abs_mptp` 的 config 也通过 `abs_smux` 暴露 `Dock`。

QUIC 有 stream，但没有 dock。把 MPTP 映射到 QUIC 时，必须回答：

- 一个 QUIC 连接上，`request_to_dock` 的语义怎么表达？stream type？ALPN？应用层头？
- 还是说多 dock 只是 smux 实现细节，MPTP 其实只需要“一个连接一个 handler”？

这个问题不解决，“零改动迁移”就无从谈起。`Dock` 是当前分层里最深的泄漏。

### 3.2 ring 内存准备是 smux / abs_buff 的绑定

`mptp_smux` 的 `TrConfig` 要求 `DefaultPrepFactory`，而 `TrChannelHandle::accept_async`
要求调用者当场交出 `TrPrepareRing` 的 ring（见 `abs_smux/src/chan.rs` 的
`TrPrepareRing` / `TrChannelHandle`）。这适合 smux 的零拷贝 ring 模型，但 QUIC 的
stream I/O 通常是 `AsyncRead` / `AsyncWrite`、`Bytes` 或后端自己的 buffer 管理。

可以用一个 QUIC → ring 的适配层把 QUIC stream 包成 ring，从而复用现有 session
driver；但那是**适配器**，不是“QUIC 原生绑定”。适配器会引入复制、背压语义变化和额外的
生命周期问题。是否接受，取决于是否把“零拷贝 / 调用者注入内存”视为 MPTP 的必需语义。

### 3.3 `mptp_smux` 把“会话驱动”和“绑定”放在一个 crate 里

`mptp_smux` 的 `MptpClient` / `MptpServlet` / `ServingSession` 直接使用
`abs_smux` 的 `DockBinding`、`ChannelTx`、`ChannelRx`。即使把 `abs_smux` trait 原样
实现到 QUIC 上，`mptp_quic` 仍然需要：

- 自己的 config（因为 `TrConfig` 的关联类型来自 `abs_smux`）；
- 自己的 client / servlet 包装（因为 `mptp_smux` 的类型名和模块都绑定 smux）；
- 自己的错误映射和生命周期。

也就是说，**会话驱动可以复用，但 crate 的公共类型不能原样复用**。

### 3.4 半关闭 / reset / 背压的语义不同

- smux 子流有明确的 `TrChannelHalf` / `is_tx_closed` / `is_rx_closed`；
- QUIC 有 FIN、`RESET_STREAM`、`STOP_SENDING`、stream / connection 两级流控；
- smux 的 `max_channel_timeout` / `max_channel_wait_close` 是绑定参数，QUIC 的 idle
  timeout / keep-alive 是连接级参数；
- MPTP 的 `finish` / 读到 EOF 的语义需要映射到不同的 API。

这些不是简单的类型别名，属于传输绑定的适配工作。

## 4. 多流语义本身是否一样

大体一样，但不能假设完全同构：

| MPTP 需要 | smux | QUIC |
| --- | --- | --- |
| 双向子流 | 是 | 是 |
| 独立流控 | 是 | 是（stream + connection） |
| 半关闭 | 是 | 是（FIN） |
| 流 reset | 有 | 有（RESET_STREAM / STOP_SENDING） |
| 地址 / 服务选择 | `Dock` | 无原生 dock |
| 内存模型 | ring / `abs_buff` | 后端 buffer / AsyncRead/Write |
| 连接迁移 / 0-RTT | 无 | 有（MPTP 暂不需要） |

结论：**多流这一层足够相似，MPTP 的“一请求一子流”模型可以迁移；差异集中在寻址、
内存和错误/流控 API，而不是请求/响应模型本身。**

## 5. 什么叫“抽象层次稳定”

建议用这个判据，而不是“原则听起来一样”：

> 新增一个传输实现时，只新增一个 binding crate + 新的 config；
> `mptp_wire`、MPTP 协议 trait、session 驱动、handler trait **零改动**。

按这个判据，当前状态：

- `mptp_wire`：已满足；
- MPTP handler / 会话状态机：大体满足；
- `abs_mptp`：**不满足**，因为它依赖 `abs_smux`；
- `mptp_smux`：**不满足**，因为会话驱动和 smux 绑定没有分开；
- `Dock`：**不满足**，因为它是传输概念；
- ring 注入：**不满足**，因为它是 smux/abs_buff 的内存模型。

## 6. 如果要达到这个目标，建议的分层

```text
mptp_wire                 // 纯字节格式，无传输依赖
  ↑
abs_mptp                  // 协议语义 / handler / session 状态，无传输依赖
  ↑
mptp_core                 // 会话驱动：前缀 → 体 → 后缀 → 收尾，泛型于 abs_mux
  ↑
abs_mux                   // 连接 / 子流 open/accept、half-close、reset、流控、buffer
  ↑            ↑
mptp_smux    mptp_quic    // 薄绑定：config + client/servlet 包装 + 错误映射
```

关键决策：

1. **`Dock` 归谁**：如果它是 MPTP 的寻址概念，应该由 `abs_mptp` 自己定义（甚至
   进 wire）；如果它只是 smux 的服务选择，应该从 MPTP 协议 trait 里移出，变成绑定的
   能力。当前它在 `abs_smux`，却泄漏进了 `abs_mptp`，是最需要先定的事。
2. **内存模型**：`abs_mux` 应该抽象“字节流 + 背压”，而不是强制 ring；ring 是 smux
   的实现优化。若坚持 ring，就必须接受 QUIC 适配层的复制。
3. **连接状态**：上一轮讨论的 `MptpServlet` 连接状态，应该放在 `mptp_core` 的
   `MptpConnection<S>` 里，而不是每个绑定各写一遍。
4. **共享 handler**：handler 的 `handle_async` 目前只拿到 `ServingSession`；如果
   `mptp_core` 要支持“一个 router 服务多连接”，需要显式的每连接上下文参数
   （共享 `&S` + 内部可变性，不能是 `&mut S`，因为 session 并发）。
5. **`abs_mptp_smux` 的去留**：它目前是 workspace 内的未使用层。不要在这个 seam 上
   再叠一层未验证的抽象；要么把它收敛成 `abs_mux` 的 smux 实现，要么删除，避免
   第三个“看起来抽象、实际绑死 smux”的 crate。

## 7. 建议的验证方式

不要直接写 `mptp_quic` 再回头改抽象。先做一个**第二个传输的最小实现**：

- 一个内存 / 本地回环的 `abs_mux` 后端，或者一个极简的 QUIC-like stream adapter；
- 跑现有 MPTP 集成测试（405、body、suffix、cancel、half-close、slow subscriber）；
- 统计需要改动的文件：如果必须改 `abs_mptp` / `mptp_smux` 的核心逻辑，说明抽象还不够；
  如果只新增 binding crate，说明到位。

这个测试比“设计原则看起来一样”更能证明抽象层次稳定。

## 8. 最终判断

- **设计原则**：可以迁移。`mptp_smux` 的一请求一子流、单 handler、前缀/体/后缀、
  取消、半关闭、无隐藏 spawn、客户端/服务端配置分离，都是 MPTP 语义，不依赖 smux。
- **代码零改动**：不行。`abs_mptp` 依赖 `abs_smux`，`mptp_smux` 把会话驱动和绑定
  合在一起，`Dock` 与 ring 内存模型也是 smux 特定的。
- **抽象层次**：业务层和 `mptp_wire` 已经稳定；传输绑定层还没有被第二个实现验证。
  这不是设计失败，而是下一步该做的验证：用 `mptp_quic` 作为“第二个传输”来逼出
  `abs_mux` / `mptp_core` 的边界。

## 9. English summary

The MPTP semantics are transport-neutral: one request per bidirectional stream, prefix /
body / suffix, a single handler per servlet, 405/500 fallbacks, cancellation, half-close,
no hidden spawning, and separate client/server configs all carry over to a QUIC binding.
The current code does not carry over unchanged, however: `abs_mptp` imports `abs_smux`,
`mptp_smux` combines the session driver with the smux binding, and `Dock`, ring-buffer
injection and smux channel traits are smux-shaped. A QUIC binding would at minimum need a
new transport-neutral mux trait, a separate session-driver crate, and a decision about
whether `Dock` is an MPTP addressing concept or a smux implementation detail. The right
stability test is: add a second binding without changing `mptp_wire`, the protocol traits
or the session driver.

## 10. 补充：servlet 是否应该绑定 connection

### 10.1 先纠正一个前提：当前 demo 不是“单 session 串行”

当前实现并不要求一条 connection 同时只有一个活跃 session：

- `serve_connection_async_` 在 accept 循环里对每条子流单独
  `scope.spawn_local(servlet_for_session.serve_session_async(session))`
  （[`server_.rs`](../mptp_smux_v1_demo/src/scene/server_.rs)）；
- `MptpServlet::serve_session_async(&self, ...)` 只借用 `&self`，多个 session 任务共享
  同一个 `Rc<MptpServlet>`；
- 每条 `ServingSession` 自己持有 `SessionCore` 的 tx/rx，body / suffix 进度是 session
  私有的；
- 集成测试 `multiple_subscriptions_one_connection_` 在**同一条连接**上同时持有两个
  `Pull`，并分别收到各自的事件。

所以“冲突域”不是由串行化换来的；当前 demo 的 per-connection 状态（`ConnectionCtx`）
可以被同一连接的多个并发 session 共享，靠的是 `Rc<RefCell<...>>` / `Cell` + 不在 await
期间持有借用，而不是靠“一次只跑一个 session”。

### 10.2 但你的判断仍然成立：绑定关系是约定，不是类型保证

`MptpServlet` 目前只有 handler：

```rust
pub struct MptpServlet<C, H> {
    handler_: H,
    _use_c_: PhantomData<fn() -> C>,
}
```

它不持有 connection / binding，也不持有 per-connection state。`server_.rs` 通过“每个
TCP 连接新建一个 `SceneHandler` + 一个 `MptpServlet`”来保证隔离；`SceneHandler` 捕获
`Rc<ConnectionCtx>`。这意味着：

- 一条连接一个 servlet 是**调用约定**，不是 `MptpServlet` 的类型不变量；
- 如果把同一个 servlet（或同一个 handler）复用到两条连接，handler 里的 per-connection
  状态就会混在一起，冲突域扩大到这些连接，甚至整个 server；
- 更麻烦的是，`handle_async` 只拿到 `method + ServingSession`，没有任何“这条 session
  属于哪条连接”的上下文；如果 handler 不捕获状态，它根本无法区分连接。

这确实是设计上的一个缺口。**servlet 文档说“一个 servlet 只服务一条连接”，但构造函数
没有把它表达出来。**

### 10.3 当前状态归属

| 状态 | 当前归属 | 冲突域 |
| --- | --- | --- |
| 请求体 / 后缀进度、响应状态 | `ServingSession` | 单 session |
| join 身份、own planet、保活、连接级取消 | `ConnectionCtx`（被 handler 捕获） | 单连接（实际）/ 可能被错误扩大（类型上） |
| 目录、行星注册表、订阅 hub | `PlanetRegistry` / `SceneHub`（server 共享） | server |
| 路由 / 方法分派 | `SceneHandler` | 连接（因为 handler 每连接一个） |

前两行是合理的分层；问题在于第二行的 owner 是 handler，而不是 servlet/connection 对象。
第三行是**有意共享**的 server 状态，不能也不应该被降为 per-connection；它靠短借用 +
单线程避免交错，而不是靠隔离。因此“冲突域扩展到整个 server”要分两种：

- 如果你指的是 per-connection 状态被共享，那么这是真实的 API 风险；
- 如果你指的是目录 / hub，那它们本来就是 server 级共享资源，换成 connection-bound
  servlet 也不会消失。

### 10.4 如果让 servlet 绑定 connection，应该绑定什么

建议把“绑定”拆成三件事，而不是只给 `MptpServlet` 加一个 connection 字段：

1. **拥有 binding / listener**：`MptpServlet` 不再接收外部 `&mut DockBinding`，而是在
   构造时持有一个 `DockBinding<C>`（或 connection handle）。这样它不可能被复用到
   另一条连接，`listen_async` 也不需要调用者再传 binding。
2. **拥有泛型连接状态 `S`**：`S` 由绑定/应用提供，`mptp_smux` 不解释它。servlet 暴露
   `state()`，server loop 用它做保活 / 清理；handler 通过新入口拿到 `&S`。
3. **保持 session 并发**：`serve_session_async(&self)` 仍然只借用 `&self`；传给 handler
   的必须是共享 `&S`（`S` 内部用 `RefCell` / `Cell` / 原子），不能是 `&mut S`，否则
   同一连接的多个 session 会互相阻塞，甚至无法同时存在。

示意：

```rust
pub struct MptpServlet<C, H, S = ()> {
    handler_: H,
    state_: S,
    binding_: DockBinding<C>,
    ...
}

impl<C, H, S> MptpServlet<C, H, S> {
    pub fn new(handler: H, state: S, binding: DockBinding<C>) -> Self;
    pub fn state(&self) -> &S { &self.state_ }
}
```

如果不想破坏现有 handler trait，可以分两步走：

- **先加 `MptpConnection<C, H, S>`**：它 own binding + `MptpServlet` + `S`，只把
  `state()` 暴露给 server loop；handler 暂时仍可捕获 `Rc<S>`。这不改变 `handle_async`
  签名，但已经把连接/状态的生命周期收进一个对象。
- **再讨论 `TrServeHandlerWithState`**：给 handler 传 `&S`。这是公共 API 变更，需要
  明确 GAT / 取消 / 并发约束后再做。

### 10.5 结论

- 你说“servlet 应该绑定 connection”**方向是对的**：当前的一连接一 servlet 是构造约定，
  类型没有保证；一旦误用，per-connection 状态会互相污染。
- 但“当前设计成立的前提是单 session 串行”**不成立**：demo 已经支持同一连接上的并发
  session；真正的问题是状态 owner 和类型约束，而不是并发模型。
- 更准确的目标不是“把 connection 塞进 servlet”，而是：**让 connection-scoped 状态
  有明确的 owner、生命周期和类型边界**。`MptpConnection<C,H,S>` 或
  `MptpServlet<C,H,S>` 都可以；关键是 `S` 泛型、共享访问、不把 server 级资源误降为
  connection 级，也不把 handler 变成隐式的连接状态容器。
- 这与 `mptp_quic` 的讨论是同一个问题：传输中立的 core 需要一个连接对象；smux 和
  QUIC 只是提供不同的 binding/connection 类型和状态。先把 connection scope 做对，
  再谈 transport swap，否则每个绑定都会复制一套“隐式连接状态”约定。

## 11. `DockBinding` 在太阳系业务中的角色

先分清两种 accept：

| accept 的对象 | API | 谁来做 |
| --- | --- | --- |
| 新的 TCP / smux connection | `TcpListener::accept` + `accept_conn_async_` | 顶层 server |
| 已有一条 smux connection 上新的 MPTP 请求子流 | `DockBinding::listen_async` → `Listener::income_async` | 该 connection 的 acceptor |

`DockBinding` 不是“接受新连接”的句柄，而是**一条已建立 smux connection 上的某个 dock 端点**。
在 demo 里它只做一件事：

```rust
let mut binding = shared.bind_async(Dock::new(0x80)).await?;
let mut listener = servlet.listen_async(&mut binding, 16).await?;
let session = servlet.accept_async(&mut listener).await?;
```

- 它选择本端服务垛口 `0x80`（[`K_DEFAULT_SERVING_DOCK`](../mptp_smux/src/lib.rs#L85)）；
- 它让这条 connection 能接收发往该 dock 的入向子流；
- 每个 `Call / View / Head / Pull` 都是一条独立子流，由 `ServingSession` 解码；
- 它不参与行星目录、owner、订阅或任何场景业务；handler 和 registry / hub 根本看不到它。

所以 **DockBinding 是纯传输层对象，不属于太阳系业务**。

### 11.1 Servlet 现在为什么“看起来需要”它

`MptpServlet` 只有 `listen_async` 这一步接触 binding：

```rust
pub fn listen_async(&'f self, binding: &'f mut DockBinding<C>, reserve: usize) -> ... {
    binding.listen_async(reserve)
}

pub fn accept_async(&'f self, listener: &'f mut Listener<C>) -> ... { ... }

pub fn serve_session_async(&'f self, session: ServingSession<C>) -> ... { ... }
```

`accept_async` 只接收 `Listener`，`serve_session_async` 只接收 `ServingSession`；servlet 不保存
binding。`listen_async` 只是一个便利包装：调用者本来就可以直接
`binding.listen_async(reserve)` 得到 `Listener`，再把 listener 交给 servlet。

所以你的判断是对的：**servlet 本质上不需要 DockBinding；它需要的是 Listener 或 Session。**
现在的写法把“accept 侧”的职责塞进了 servlet，导致看起来 servlet 借用了它。

### 11.2 两种自洽的设计，不要混在一起

**A. connection 对象 + servlet 分派器**（推荐）

```text
MptpConnection<C, H, S>
  ├─ owns DockBinding<C> / Listener<C>
  ├─ owns per-connection state S
  ├─ accept_async() -> ServingSession
  └─ servlet.serve_session_async(session, &S)
MptpServlet<C, H>
  └─ handler dispatch only; no binding
```

- 顶层 server 只负责 `TcpListener` 接受新 smux connection；
- 每个 connection 对象绑定自己的 dock、持续 accept 新 session、维护 idle / cancel / cleanup；
- servlet 只处理 session，不碰 DockBinding；
- 冲突域 = 该 connection 对象；服务器共享的 registry / hub 仍是 server 级。
- 这是 `mptp_quic` 也能复用的形状：smux / quic 只提供不同的 connection/binding，core 不变。

**B. servlet 本身是 connection 对象**

```text
MptpServlet<C, H, S>
  ├─ owns DockBinding<C> / Listener<C>
  ├─ owns S
  ├─ accept loop
  └─ handler dispatch
```

- 这时 servlet **确实需要** binding，因为它就是 acceptor；
- 顶层 server 只接受新 smux connection，不持有这个 per-connection binding；
- 你上一轮“servlet 绑定 connection”的想法对应这一种。

两种都可行，但当前 `MptpServlet` 是“A 的 dispatcher + B 的 listen 包装”的混合体：
`listen_async` 让它看起来像 B，`accept_async` / `serve_session_async` 又让它像 A。
混合的后果是：connection ownership 和 accept 职责不清晰，per-connection 状态只能由
handler 捕获，冲突域约束靠调用约定。

### 11.3 建议

- 如果目标是“server / connection 负责 accept，servlet 只负责 session”，选 **A**：
  把 `listen_async` 从 `MptpServlet` 移出，或移到 `MptpConnection`；servlet 只保留
  `serve_session_async`，必要时增加 `&S` 参数。
- 如果目标是“servlet 就是一条连接的 server 侧代表”，选 **B**：让 `MptpServlet` 构造时
  拥有 binding / listener / `S`，从此不存在“借用 dock binding”这个说法；server 顶层只
  接受新 connection。
- 无论 A 还是 B，**DockBinding 都不属于太阳系业务**，它只属于 connection/acceptor 层；
  handler 不应该看到它，更不应该把它当作连接身份。

这与上一节的结论一致：真正需要明确的是 connection scope 和 accept 职责的 owner，而不是
继续让 `MptpServlet` 同时扮演 dispatcher 和 acceptor。
