# MPTP — Multiplexed RPC / Resource Protocol

> **MPTP = Message Packing and Transportation Protocol**  
> 一个建立在“支持多 stream / stream multiplexing 的底层连接”之上的 RPC / resource protocol。

MPTP 不试图重新实现 TCP、QUIC 或连接复用，而是假定底层已经提供：

- 一个可靠的 connection；
- connection 上可以创建多个独立 stream；
- 多个 stream 可以并发运行；
- stream 的生命周期相互独立。

在这个前提下，MPTP 定义“如何在一条条独立 stream 上表达资源访问、RPC 调用、以及双向实时数据推送/拉取”。

---

## 1. 定位与目标

MPTP 的目标是成为 **二进制优先的、面向资源与 RPC 的、天然支持流式通信** 的协议。

它同时具备三种能力：

1. **Resource Protocol**：像 HTTP 一样对资源进行增删查改；
2. **RPC Protocol**：通过 `Call` 调用远端功能；
3. **Realtime Streaming Protocol**：通过流复用 + 会话对应，让客户端和服务端都能实时双向推送/拉取数据。

它不是 HTTP 的替代品，而是更适合“长连接 + 多路复用 + 双向流”场景的协议设计。

---

## 2. 设计原则

- **不重新发明传输层**：底层只要求“可靠的、有序的、可并发的多 stream 连接”，例如 QUIC / iroh。
- **二进制优先**：报文头使用 MessagePack 编码，头字段的 key/value 也可以是数字，避免传统 HTTP 文本协议的开销。
- **资源语义清晰**：请求有 `method + path + headers + body`，回复有 `status + headers + body`。
- **流即会话**：一次 RPC/资源访问占用一条 channel（双向 stream 对），该 channel 就是一次会话的上下文。
- **双向实时**：不仅客户端可以请求-回复，服务端也可以在一条已建立的 channel/session 上持续向客户端推送数据；客户端也可以持续向服务端推送。
- **并发隔离**：每条 stream/channel 独立，协议解析错误或取消不应波及其它 channel。

---

## 3. 协议分层设计

MPTP 从下到上分为 5 层：

```text
┌──────────────────────────────────────────────────────────┐
│ L4 会话 / 流式语义层                                       │
│   Push / Pull / Call、suffix stream、会话关联、实时双向流   │
├──────────────────────────────────────────────────────────┤
│ L3 资源访问语义层                                          │
│   7 种 AccessMethod、Status、path 路由                     │
├──────────────────────────────────────────────────────────┤
│ L2 消息 / 编码层                                           │
│   Request / Response、Headers、Body_Size / Body_Type      │
│   MessagePack / rmp-serde 序列化                          │
├──────────────────────────────────────────────────────────┤
│ L1 通道抽象层                                              │
│   TrMuxConn / TrChannel                                   │
│   一个 Channel = 一对方向相反的流                          │
├──────────────────────────────────────────────────────────┤
│ L0 底层多流连接                                            │
│   TCP+多路复用 / QUIC / iroh / 自定义传输                   │
└──────────────────────────────────────────────────────────┘
```

### L0：底层多流连接

MPTP 只依赖底层提供的能力，不在协议内部重新实现：

- 已建立的 connection；
- 在 connection 上创建独立 stream；
- 每个 stream 内部可靠、有序；
- 不同 stream 之间互不阻塞；
- 最好还能提供双方身份信息（`local_id` / `remote_id`）。

典型实现：iroh（QUIC）、raw QUIC、HTTP/3 双向流等。

### L1：通道抽象层（Transport）

代码中对应：

- `TrMuxConn`：表示一个多路复用连接，可以打开新的 `Channel`；
- `TrChannel`：表示一条 channel，是“一对方向相反的流”，可以 `split()` 成 `Tx` 和 `Rx`；
- `Tx` / `Rx` 实现 `TrBuffTryWrite` / `TrBuffTryRead`，提供分段缓冲 IO 抽象。

**一个 Channel 是 MPTP 中一次会话的载体。** 多个 RPC 可以同时在同一个 connection 的不同 channel 上运行。

### L2：消息 / 编码层

对应 `crates/rpc_core/src/messaging.rs`。

- `Request` 由 `method_ + path_ + headers_` 组成；
- `Response` 由 `status_ + headers_` 组成；
- 头部目前使用 `rmp-serde` / MessagePack 序列化，但这是可以替换的，不能写死
- 如果有 body，通过标准头 `Body_Size` 声明长度、`Body_Type` 声明类型；
- body 是跟随在消息头后面的原始二进制内容；
- 在 body 之后，协议还允许同一 stream 上继续追加“suffix stream”数据，用于实时流。

关键点：**头部不是 HTTP 文本**，而是二进制友好的 `String or u16` 联合体。

### L3：资源访问语义层

对应 `crates/rpc_core/src/access_method.rs` 和 `specs.rs`。

MPTP 定义了 7 种资源访问方法：

| 方法 | 数值 | 请求体 | 回复体 | 流式语义 |
|---|---|---|---|---|
| `Head` | 0 | 通常无 | 无 | 查看资源元信息 |
| `View` | 1 | 通常无 | 有，资源内容 | 读取资源本体 |
| `Post` | 2 | 有，上传内容 | 可选 | 创建/上传资源 |
| `Drop` | 4 | 通常无 | 无 | 删除资源 |
| `Push` | 16 | 可有 | 可选 | 客户端必然带附加流，向资源端实时推送 |
| `Pull` | 32 | 可有 | 可有 | 成功后服务端必然带附加流，向客户端实时拉取/订阅 |
| `Call` | 64 | 有，调用参数 | 可有 | 调用远端功能，结果可能带附加流 |

状态码沿用 HTTP 风格的数值语义，例如 `200 Ok`、`201 Created`、`400 BadRequest`、`404 NotFound`、`500 InternalServerError` 等。

### L4：会话 / 流式语义层

这是 MPTP 区别于普通 HTTP RPC 的核心：

- 一条 `Channel` 对应一个会话；同一个 connection 上可以同时存在大量会话；
- 普通请求（`Head / View / Post / Drop`）在回复头/body 结束后即可关闭；
- `Push / Pull / Call` 可以在消息结束后继续在同一个 channel 上传输持续数据流；
- 持续数据流称为 **suffix stream / 附加流**，可以理解成“请求/回复头已经完成，但会话仍在继续”；
- 未来如果需要把多条底层 stream 关联到同一个逻辑会话，可以在标准头区扩展 `Session-Id`、`Stream-Id` 等字段，让流复用与会话对应更加显式。

这种设计使得：

- 客户端可以 **Push** 音视频、日志、遥测等流式数据到服务端；
- 客户端可以 **Pull** 订阅一个资源，服务端持续推送更新；
- 服务端也可以通过一条已经建立的 channel 主动向客户端推送数据，而不需要客户端反复轮询。

---

## 4. 当前代码映射

| 概念 | 代码位置 | 状态 |
|---|---|---|
| 7 种访问方法 | `crates/rpc_core/src/access_method.rs` | 已定义 |
| HeaderKey / HeaderVal / Status / 标准头 | `crates/rpc_core/src/specs.rs` | 已定义 |
| Request / Response / 编解码 | `crates/rpc_core/src/messaging.rs` | 基础已定义，部分 `todo!` |
| 客户端 RequestBuilder / 回复体决策 | `crates/rpc_core/src/client.rs` | 部分实现，含 `todo!` |
| 多流连接 / Channel 抽象 | `crates/rpc_core/src/transport.rs` | 已定义 trait |
| iroh / QUIC 传输实现 | `crates/rpc_transport_iroh/` | 当前为空，待实现/恢复 |
| 可运行 Demo | `crates/rpc_cs_demo/` | 当前为空壳，待实现 |

---

## 5. Demo 目标

这个 Demo 需要直观展示 MPTP 的核心价值：

1. **资源 CRUD**：像 HTTP 一样对资源做 `Head / View / Post / Drop`；
2. **多 stream 并发**：多个请求/会话在同一个 connection 上同时运行，互不阻塞；
3. **双向实时流**：
   - 客户端通过 `Push` 持续向服务端推送数据；
   - 客户端通过 `Pull` 订阅服务端持续推送的数据；
   - 服务端可以主动把事件推送到已订阅的客户端；
4. **会话对应**：每个订阅/推送都有明确的 channel/session 上下文，数据不会串流。

### 建议 Demo 场景

一个简单的 **实时消息/事件总线 Demo**：

- 服务端维护若干资源（`/topic/chat`、`/stream/ticks` 等）；
- 客户端 `Post` 一条消息到 `/topic/chat`；
- 客户端 `Pull /topic/chat`，服务端在有新消息时持续推送给订阅者；
- 客户端 `Push /stream/log`，服务端实时接收并打印/广播；
- 同时开多个 `View` 请求，验证多 stream 并发互不阻塞。

### 建议 CLI

```text
cargo run -p mptp_rpc_cs_demo -- server
cargo run -p mptp_rpc_cs_demo -- client
```

Demo 内部使用 `mptp_rpc_transport_iroh` 作为底层多流传输。

---

## 6. 开发计划（指导 AI 完成 Demo）

### Phase 0：理解现状

- 阅读 `crates/rpc_core/src/transport.rs`、`messaging.rs`、`specs.rs`、`access_method.rs`、`client.rs`；
- 确认当前 `rpc_transport_iroh/src/` 和 `rpc_cs_demo/src/main.rs` 均为空壳；
- 确认 `crates/rpc_core` 中仍存在 `todo!()`，需要按协议语义补齐。

**完成标准**：
1. 能画出上述 5 层分层图，并能说清每个 crate 的职责。
2. 能在 `crates/rpc_core` 中给出每一层的向下层依赖的 API 是什么，对上提供的 API 是什么。这些 API 可能会在后续有修改，但必须有一个阶段性的成果，确保生成的代码功能是合理设计的。

### Phase 1：打通传输层

任务：

1. 在 `crates/rpc_transport_iroh` 中实现 `TrMuxConn` / `TrChannel`；
2. 基于 iroh QUIC 的 `open_bi` / `accept_bi` 得到双向 stream；
3. 将 iroh 的异步流桥接到 `abs_buff` 的 `TrBuffTryRead` / `TrBuffTryWrite`。可以先将 iroh 中的流数据写入到一个 RingBuffer （该依赖已经引入），不要重新发明过多的 abs_buff 中的 trait 所需要的类型，而是使用现有的；
4. 提供 `connect_by_id`、`connect_by_addr`、`accept` 等构造方式（可参考 git 历史中的旧实现）；
5. 抽象出一个根据 header 查找必要的 serializer 和 deserializer 的功能，可以暂时先固定满足几个测试类。
5. 为传输层写一个端到端 roundtrip 测试：打开 channel，写数据，读回并校验。

**完成标准**：

- `cargo test -p mptp_rpc_transport_iroh` 通过；
- 能证明一条 connection 上可同时开多条 channel 并各自读写。

### Phase 2：完善 core 消息与客户端

任务：

1. 补齐 `RequestBuilder` / `HeadersBuilder`，支持 `method + path + headers + body`；
2. 补齐 `Request` / `Response` 的 body 编码/解码路径；
3. 实现 `should_read_response_body` 已定义的回复体决策逻辑，并完成实际 body 读取；
4. 定义或补齐服务端 dispatcher：从 channel 读 `Request`，按 `path` 路由到 handler，写回 `Response`；
5. 确保没有 body 时不会多读一个字节，避免破坏 stream 对齐。

**完成标准**：

- 单元测试覆盖：无 body 请求、有 body 请求、回复体存在/不存在、协议违规检测；
- `cargo test -p mptp_rpc_core` 通过。

### Phase 3：实现资源 CRUD Demo

任务：

1. 在 `rpc_cs_demo` 中实现一个简单的内存资源表；
2. 服务端处理 `Head / View / Post / Drop`；
3. 客户端通过 `RequestBuilder` 发起这些请求并打印结果；
4. 验证 `Post -> View -> Head -> Drop` 的完整资源生命周期。

**完成标准**：

- `cargo run -p mptp_rpc_cs_demo -- server` 与 `cargo run -p mptp_rpc_cs_demo -- client` 能完成一轮 CRUD；
- 多个 `View` 并发执行时不会互相阻塞。

### Phase 4：实现实时 Push / Pull

任务：

1. 服务端为资源提供订阅表：客户端 `Pull` 后，服务端把该 channel 登记为订阅者；
2. 客户端 `Push` 数据到某资源时，服务端读取持续流，并广播给该资源的所有 `Pull` 订阅者；
3. 实现 `Call` 的一个简单示例，例如 `Call /rpc/echo` 或 `Call /rpc/sum`；
4. 验证“客户端 Push 的数据能实时出现在其它 Pull 客户端”的完整链路。

**完成标准**：

- 一个客户端 `Pull /topic/chat` 后，另一个客户端 `Push /topic/chat` 的消息能被实时收到；
- 多个订阅者同时在线时，广播互不串流；
- Push/Pull 会话结束后，服务端能正确清理订阅关系。

### Phase 5：并发与回归验证

任务：

1. 同时运行多个 Pull、Push、View、Call，验证 stream 隔离；
2. 验证取消/断开一个 channel 不会影响其它 channel；
3. 验证二进制 body 和数字头值在真实链路上正确传递；
4. 整理 README 中的示例输出，作为 Demo 的使用说明。

**完成标准**：

- 提供一个可重复运行的 demo 脚本或命令行步骤；
- 输出中能明显看到：并发 RPC 交错执行、实时推送逐条到达、会话独立。

---

## 7. AI 开发注意事项

- **保持分层边界**：传输层只实现 `TrMuxConn` / `TrChannel`，不要把协议语义塞进去。
- **不要重新发明传输**：iroh 已经提供连接和流复用，Demo 只做适配。
- **保持二进制协议**：不要引入 HTTP 文本解析；报文头按现有 `serde` / MessagePack 设计，body 按 `Body_Type` 声明的类型处理。
- **注意 stream 对齐**：读取方必须严格按照 `Body_Size` 决定是否读取 body；协议违规时宁可丢弃当前 channel，也不要污染后续字节。
- **优先参考现有代码注释**：`messaging.rs` 中关于 suffix stream、body 决策的注释是协议意图的第一手资料。
- **每阶段可编译可测试**：先让 transport 独立通过，再做消息层，最后做 Demo，避免一次性大改导致难以定位问题。

---

## 8. 验收标准（Demo 完成定义）

1. `cargo build --workspace` 成功；
2. `cargo test --workspace` 通过（或至少有核心/传输层测试通过）；
3. Demo 可以启动 server 和 client；
4. Demo 展示：
   - 资源 `Head / View / Post / Drop`；
   - 同一个 connection 上多个 stream 并发独立运行；
   - 客户端 → 服务端实时 `Push`；
   - 服务端 → 客户端实时 `Pull` / 订阅推送；
   - 多路实时流不串流、互不阻塞。

---

*这份 README 既是协议设计备忘，也是后续 AI 实现 Demo 的开发计划。*
