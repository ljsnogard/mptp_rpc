# mptp_smux

`mptp_smux` 是 **MPTP（Multiplexed P2P Transportation Protocol）在 `smux_v1` 多流传输层上的具体实现**。

关系类似 **HTTP over TCP**：MPTP 定义一条字节流通道上「一次请求 / 回复长什么样」，`smux_v1`
负责提供可靠、有序、双向、可并行且支持半关闭的子流。`mptp_smux` 把两者接起来。

> **当前状态**：本 crate 仍是 `0.1.0` 的单体实现，客户端与服务端总是同时编译。
> README 中的 `client` / `server` feature 是**重构目标**，尚未落地；文中凡标「目标」的条目
> 都不应当作已实现能力使用。
>
> 前缀线格式已经抽到 [`mptp_wire`](../mptp_wire/README.md)：`mptp_smux` 通过
> `mptp_smux::messaging` 兼容别名继续暴露原来的类型与读写入口。

## 它是什么

- MPTP 的 **smux_v1 传输绑定**：dock 寻址、开子流 / 接子流、ring 内存注入、半关闭与收尾；
- MPTP 前缀与限长 / 分块体格式由 [`mptp_wire`](../mptp_wire/README.md) 拥有；本 crate 通过
  `mptp_smux::messaging` 别名继续导出这些类型与读写入口；
- 按头表搬运**请求体 / 回复体 / 后缀流**的会话对象；
- 客户端与服务端两套角色实现；
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
- 应用路由框架——路径如何匹配、是否分层、是否运行期增删，由应用 / router crate 决定；
- 运行时或服务器框架——不选 tokio / compio / smol，不替调用者 spawn，不管 socket / TLS；
- QUIC 的抽象层——`mptp_quic` 如果出现，是它的**兄弟绑定**，不是它的子类型。

## 分层位置

```text
┌─────────────────────────────────────────────────────────────┐
│ 应用 / RPC：业务类型、序列化、service、路由、handler          │
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

## MPTP 对传输层的要求

MPTP 对传输层的要求：

1. 可靠、有序、双向的字节流；
2. 半关闭：只关写方向，读方向仍可继续；
3. 一条连接上多条可并行的子流；
4. 子流的 open / accept / reset 生命周期。

`smux_v1` 满足这些条件；QUIC 的双向 stream 也满足。MPTP 本身不依赖「消息边界」，所有边界
由前缀与体帧自行界定。

在此之上，`mptp_smux` 还依赖 `smux_v1` 的 **ring 内存注入**：每条子流的两块读写内存由
调用者按配置现场提供，而不是由传输层内部隐式分配。这是 smux 绑定的特殊要求，不是 MPTP
对 QUIC 的要求。

## 与 `mptp_quic` 的共通点

如果将来实现 `mptp_quic`，它和 `mptp_smux` 应该共享：

| 共享内容 | 说明 |
| --- | --- |
| MPTP 线格式 | 请求 / 回复前缀的字节序列、头表编码 |
| 报文语义 | access method、status、`Body_Size` / `Body_Transfer`、后缀流 |
| 会话顺序 | 前缀 → 体 → 后缀 → 半关闭；回复方向同理 |
| 角色模型 | 两端都既可发起也可响应；client / server 是「一次请求的方向」 |
| handler 契约 | `abs_mptp::serving::TrReqHandler` |
| 取消与错误分类 | 取消、传输错误、协议错误、应用拒绝 |

它们**不应**共享：

| 只属于 smux 绑定 | 只属于 QUIC 绑定 |
| --- | --- |
| dock（本端 / 远端） | stream ID |
| `bind_async` / `open_channel_async` / `accept_async` | QUIC stream open / accept |
| `listen_async` / `income_async` | QUIC accept |
| `TrPrepFactory` / `RingBuffs` | QUIC send / recv buffer 模型 |
| smux FIN / RESET | QUIC FIN / RESET_STREAM |

因此：

- `mptp_smux` 与 `mptp_quic` 是兄弟关系；
- 共享的 MPTP 线格式与会话契约**不能依赖 `mptp_smux` 的具体类型**；
- 不要为了「以后可能有 QUIC」在 `mptp_smux` 里造一个抹平所有传输差异的万能 trait。

## 两端对等与 feature 划分（目标）

MPTP 是 P2P 协议：一条连接的两端都可以是对方的客户端和服务端。因此本 crate 同时提供两端。
这里的 **client / server 是一次请求中的角色，不是连接的属性**：同一个进程可以同时持有
`MptpClient` 和 `MptpServer`，分别对外发请求、对内接请求。

重构目标是把两端拆成独立 feature：

| feature | 提供 | 典型使用者 |
| --- | --- | --- |
| `default` | `client` + `server` | 需要两端对等的应用 |
| `client` | `MptpClient` / `MptpSession`、客户端配置、`ClientPrepCtx` | 只发请求的一端 |
| `server` | `MptpServer`、服务端配置、`ServingPrepCtx`、`TrPathRouter`；第一阶段仍包含内置 `Router` | 只接请求的一端 |
| `router`（第二阶段） | 把内置 `serving::Router` 从 `server` 中拆出，携带 `matchit_route` / `tlist` | 需要默认前缀路由的应用 |

重构分两阶段：**先**拆 `client` / `server`，保持 `Router` 仍在 `server` 下以免破坏现有使用者；
**再**把内置 `Router` 拆成独立的 `router` feature（或独立 crate）。`messaging`（前缀 / 体格式与
头表）是两端共享面，不设 feature，始终编译。

下游目标声明方式：

```toml
# 只做客户端
mptp_smux = { path = "../mptp_smux", default-features = false, features = ["client"] }

# 只做服务端（第一阶段仍带内置 Router；第二阶段拆出 router 后才不含）
mptp_smux = { path = "../mptp_smux", default-features = false, features = ["server"] }

# 两端对等（第一阶段仍带内置 Router）
mptp_smux = { features = ["client", "server"] }

# 第二阶段：显式启用内置 Router
mptp_smux = { features = ["client", "server", "router"] }
```

`router` 是否独立成 feature、内置 `Router` 去留，见 `dev-notes/mptp-smux-20261110-0730.md`。

## 它应该做什么

1. **绑定 `smux_v1`**：dock、bind / open / listen / income / accept、子流读写；
2. **管理一次请求的子流生命周期**：请求前缀 → 请求体 → 请求后缀 → 回复前缀 → 回复体 →
   回复后缀 → 半关闭 → 收尾；
3. **注入 ring 内存**：按配置声明的工厂，为每条子流现场生产新的 ring；不缓存、不复用
   已交给上一条子流的内存；
4. **使用并转出前缀与体格式**：具体编解码由 `mptp_wire` 实现；本 crate 只负责把前缀读写、
   体 / 后缀搬运接到子流上，读前缀时不越过体的第一个字节；
5. **按头表搬运体与后缀**：定长 / 分块 / 无体由 `Body_Size` / `Body_Transfer` 唯一决定；
   后缀是裸字节流；
6. **实现客户端会话**：发请求、发体、发后缀、读回复前缀、读回复体 / 后缀；
7. **实现服务端 accept 循环**：接子流、解码前缀、把请求交给 handler、由 handler 负责收尾；
8. **传播取消、区分错误**：每个 await 点接受调用方令牌；区分取消、传输、协议、handler
   错误；
9. **保持两端配置分离**：客户端与服务端可以跑在两个进程、各自升级。

## 它绝对不应该做什么

1. **不实现 RPC 语义**：service trait、方法到函数的映射、RPC 错误体、`#[rpc]` 宏；
2. **不实现业务体编解码**：MessagePack / JSON / 业务类型序列化；
3. **不决定路由策略**：路径匹配、分层、运行期增删是应用 / router crate 的事；
   `TrPathRouter` 只是可选接缝，内置 `Router` 不是 MPTP 的必备部分；
4. **不选择运行时、不 spawn**：tokio / compio / smol、线程与任务模型由应用决定；
5. **不管理 socket / TLS / 连接池 / 重连**；
6. **不把实现默认值冒充协议常量**：`0x80` 默认服务 dock 在当前代码里被称为协议常量，但
   协议综述并未把 dock 写进线格式；它究竟是协议约定还是 smux 绑定的参考默认值，需要先核对
   再决定归属；
7. **不偷偷支持「一条通道多次请求」**：当前 MPTP 没有请求 ID / 多路复用帧，一条通道一次
   请求是协议约束；
8. **不把 QUIC 差异塞进 `mptp_smux`**：共享部分上移，传输差异留在各自绑定。

## 当前实践

| 使用者 | 用到的部分 | 用途 |
| --- | --- | --- |
| `rpc_core_v1` | `MptpClient` / `MptpSession`、`MptpServer`、`Router`、`mptp_wire`、两端配置、ring 工厂 | RPC 客户端代理、服务端 `RpcCallCtx`、interface 分派 |
| `abs_rpc_serv` | `serving::config::TrConfig` 与协议类型转出 | RPC service 的抽象契约 |
| `rpc_svc_macro` | 经 `rpc_core_v1` 间接使用；文档示例使用 `Router` / `RouterCtx` | 生成代码的装配说明 |
| `mptp_smux_v1_demo` | `MptpClient`、`MptpServer`、`messaging`、`TrPrepFactory`、`TrPathRouter` | 真实 TCP 上的一问一答验证；自带 Router |
| `rpc_demo` | `MptpClient`、`MptpServer`、`messaging`、`TrPathRouter` | 手写 queue 版 RPC demo；自带 Router |
| `mptp_cs_smoke`（仓库外） | `MptpClient`、`MptpServer`、`Router`、`RpcRouter` | 真实 UDS + compio 的 RPC 回归 |

这说明上层真正依赖的是：**线格式类型与读写入口、会话对象、客户端连接、服务端 accept
循环、ring 工厂、取消传播、收尾 helper**；不是业务类型或 RPC 语义。

## 当前缺口

- **feature 未拆分**：只想要客户端也会编译服务端与内置 Router；
- **服务端并发**：`serve_listener_async` 在 accept 循环里 inline await handler，长 `Pull`
  或慢 handler 会阻塞后续 accept；缺少 spawner / 每子流任务；
- **优雅关闭**：只有取消令牌，没有 `ServeHandle` / drain / 等待 in-flight；
- **错误源**：多数错误被转成 `String`，丢失底层 source；
- **std 事实**：Cargo `categories` 写着 `no-std`，但本 crate 目前是 std 实现；
  `abs_mptp_smux` 才是 no_std 抽象层；
- **线格式与 RPC 层耦合（已部分解除）**：`rpc_core_v1` 已直接使用 `mptp_wire`；
  `abs_rpc_serv` 仍通过 `mptp_smux` 的 serving 配置父 trait 间接依赖具体传输，
  尚未完全解除；
- **内置 Router 越界**：路由是应用策略，当前却无条件编译进本 crate。

## 最小使用形状

不在这里复制完整配置；可运行示例见：

- [`mptp_smux_v1_demo`](../mptp_smux_v1_demo)：真实 TCP 上的客户端 / 服务端一问一答；
- [`rpc_demo`](../rpc_demo)：手写 RPC 的 UDS 端到端。

概念上的两端装配是：

```text
客户端：
  MptpClient::new(shared_conn)
    → request_async(RequestPrefix)
    → MptpSession::send_req_body_async / recv_resp_prefix_async / recv_resp_body_async

服务端：
  MptpServer::new(router)          # 当前要求 TrPathRouter
    → serve_listener_async(binding, reserve)
    → 解码前缀 → 路由 → handler 写回复并收尾
```

## 相关 crate

| crate | 关系 |
| --- | --- |
| `abs_mptp` | MPTP 协议形状、访问方法 / 状态码 / 标准头、体声明、会话与 handler trait |
| `abs_mptp_smux` | 名义上的 MPTP-over-SMUX 抽象；当前无使用者，与 `mptp_smux` 有重复，归属待定 |
| `abs_smux` / `smux_v1` | 多流传输与连接实现 |
| `abs_buff` | ring / 段读写接口 |
| `rpc_core_v1` / `abs_rpc_serv` | RPC 语义层与服务端抽象 |
| `mptp_wire` | MPTP 前缀、头表、限长 / 分块体与 MessagePack 线格式；本 crate 的兼容别名指向它 |
| `matchit_route` | 内置 `Router` 的路径匹配实现（目标上应可选） |

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

feature 拆分完成后，还应能分别验证：

```console
cargo check -p mptp_smux --no-default-features
cargo check -p mptp_smux --no-default-features --features client
cargo check -p mptp_smux --no-default-features --features server
cargo check -p mptp_smux --all-features
```

## 文档

- 协议线格式：[`dev-notes/mptp-protocol-20261020-0200.md`](../dev-notes/mptp-protocol-20261020-0200.md)
- 职责边界与重构依据：[`dev-notes/mptp-smux-20261110-0730.md`](../dev-notes/mptp-smux-20261110-0730.md)
- RPC 框架设计：[`DESIGN_BLUEPRINT.md`](../DESIGN_BLUEPRINT.md)

---

**English summary.** `mptp_smux` is the concrete MPTP binding over the `smux_v1`
multiplexed transport, analogous to HTTP over TCP. It owns sub-stream lifecycle, ring
memory injection, MPTP prefix wire IO, body/suffix transport and cancellation. It must
not own RPC semantics, business body codecs, routing policy, runtime selection or
connection management. Client and server roles are both needed because MPTP is
peer-to-peer; they are planned as independent Cargo features. A future `mptp_quic`
should share the MPTP wire format and session contracts, not depend on `mptp_smux`.
