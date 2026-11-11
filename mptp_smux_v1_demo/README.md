# `mptp_smux_v1_demo`：Bevy 太阳系场景

`mptp_smux` 的**下游压测 demo**：客户端与服务端之间直接跑 **MPTP over TCP/smux**，
两端都固定为 `abs_art-facade` + compio。它不使用 HTTP / WebSocket 网关，也不使用
tokio。

场景本身很简单：太阳在原点；每个客户端 `join` 后新增一颗行星；摄像机锁定自己的行星；
两个客户端默认互相订阅、互相可见；客户端可以改自己的公转 / 自转速度，也可以按行星
选择性退订 / 重新订阅。

但 demo 的第一目标是**把 `mptp_smux` 的公开 API 用满**：

- `Call /scene/join`、`Call /scene/update`；
- `View /scene/planets` 目录发现；
- `Head /scene/planets` 数量 / 版本；
- 每颗行星一个 `Pull` + **无限 `SuffixStream`**；
- 退订 = 丢弃 `Pull` 会话；重新订阅 = 新开 `Pull` 拿新快照；
- 慢订阅者由 bounded `flume` + `try_send` 移除，不阻塞其他订阅。

传输层（建流、前缀、体边界、后缀半关闭、收尾、取消）全部在 `mptp_smux` 里；本 crate
只写业务层：目录、行星参数、denco、订阅表、Bevy ECS / 渲染。

## 线程模型

| 进程 | 业务线程 | 职责 |
| --- | --- | --- |
| `scene-server` | **1 个 compio 线程** | accept、所有连接、所有 session、目录与行星流 |
| `scene-client`（Bevy） | **Bevy 主线程 + 1 个 compio 网络线程** | 渲染 / ECS / `bevy_egui`；MPTP 连接与所有 `Pull` |
| `scene-smoke` | **1 个 compio 线程** | 无渲染双客户端 e2e |
| 集成测试 | 每个 `#[compio::test]` 一个 compio 运行时 | 真实 TCP + smux |

Bevy 侧：

- `bevy` 关闭 `multi_threaded` 与 `pipelined_rendering`；
- `TaskPoolPlugin` 显式设为 `TaskPoolOptions::with_num_threads(1)`；
- 跨线程只传 `NetCommand` / `NetEvent` 普通数据；
- `BuffRead` / `BuffWrite` 是 `!Send`，只在 compio 线程内构造与使用；
- Bevy / wgpu / 窗口系统内部若仍有驱动线程，属于**引擎 / 驱动线程**，不是业务线程；
  `TaskPoolPlugin` 的三个默认池（IO / async compute / compute）各限 1 线程，`wgpu`
  驱动线程由显卡后端创建；
- 线程验收只统计**业务线程**：`scene-server` 1 个 compio 线程；Bevy 客户端
  Bevy 主线程 + 1 个 compio 网络线程。

## 协议与订阅模型

| 资源 | 方法 | 语义 |
| --- | --- | --- |
| `/scene/join` | `Call` | 当前连接创建自己的行星，返回 `PlanetDescriptor` |
| `/scene/update` | `Call` | 修改自己的公转 / 自转速度，返回 `UpdateAck`；连接即身份 |
| `/scene/unsubscribe` | `Call` | 业务层退订提示（见「API 缺口」），按连接身份移除订阅 |
| `/scene/planets` | `View` | 列出全部行星数据流，不订阅 |
| `/scene/planets` | `Head` | 无体；返回数量 / 目录版本头 |
| `/scene/planets/{id}` | `Pull` | 订阅该行星后缀流 |
| 其他路径 | 任意 | `404` |
| 不允许的方法 | 任意 | `405` + `Allow` |

`PlanetEvent` 有三个业务分支：

```text
Snapshot(PlanetDescriptor)   // 订阅第一帧
Updated(PlanetDescriptor)    // 参数变化
Removed { planet_id, version } // 行星删除，随后流结束
Heartbeat { sequence }       // 空闲心跳，客户端忽略
```

### chunk 语义

- 后缀流线上是**裸字节**：没有长度声明、没有 2 字节块帧；
- chunk 只是 `SuffixStream::write_chunk_async` 的**写入单位**；
- 每个 `PlanetEvent` 用 denco 序列化后写成一个 chunk；
- 一个对象也可以拆成多个 chunk 写入，接收端按 denco 自描述值序列逐值解码；
- 不修改 `mptp_wire` 的线上格式。

### 退订与清理

- 客户端丢弃 `Pull` 的 `Response` / 取消其任务即停止本地读取；
- 当前 `mptp_smux` **没有**关闭回复读半边的公开入口，服务端无法及时观察到单纯丢弃；
  因此 demo 在丢弃前发一条 `Call /scene/unsubscribe`（业务清理提示），服务端立即
  触发该订阅的取消令牌；
- 连接断开由业务级保活判定：客户端每 500ms 发一次 `Head`，服务端 3 秒没有任何请求
  即认定连接已死，删除行星并向订阅者广播 `Removed`；
- 慢订阅者：每颗行星的 hub 用 bounded `flume` + `try_send`，队列满即移除该订阅并触发
  取消，其他订阅继续。

## 目录

| 文件 | 作用 |
| --- | --- |
| `src/scene/protocol_.rs` | denco 业务类型、URI、头键、编解码 |
| `src/scene/state_.rs` | `PlanetRegistry` / `SceneHub` / 连接上下文 |
| `src/scene/handler_.rs` | 单一 `TrServeHandler`：路由 + join / update / View / Head / Pull |
| `src/scene/server_.rs` | 单 compio 线程 accept、每连接 servlet、连接清理与保活看门狗 |
| `src/scene/client_.rs` | 业务客户端：join / View / update / 显式退订 / Pull |
| `src/scene/net_.rs` | Bevy ↔ 网络线程的 `NetCommand` / `NetEvent` 桥 |
| `src/scene/cfg_.rs` | compio 两端 `mptp_smux` 配置与 ring 准备工厂 |
| `src/scene/conn_.rs` | TCP ↔ smux 握手装配 |
| `src/bin/scene-server.rs` | 服务端二进制 |
| `src/bin/scene-smoke.rs` | headless 双客户端 e2e |
| `src/bin/scene-client.rs` | feature-gated Bevy 客户端 |
| `tests/scene_stream_.rs` | 15 项无 Bevy 集成测试矩阵 |
| `tests/common/mod.rs` | `SceneFixture` / `SceneTestClient` |

## 运行

### 服务端

```console
$ just scene-server
scene-server 已就绪：监听 127.0.0.1:7721（单 compio 业务线程）
```

或：

```console
$ cargo run -p mptp_smux_v1_demo --bin scene-server
```

### Bevy 客户端（两个终端各一个）

```console
$ cargo run -p mptp_smux_v1_demo --features bevy-client --bin scene-client -- --name Alice --color 0x00AA55
$ cargo run -p mptp_smux_v1_demo --features bevy-client --bin scene-client -- --name Bob --color 0xAA5500
```

> 视觉验收需要可用的图形环境（X11 / Wayland + GPU 或软件渲染）。无显示环境下
> `cargo check --features bevy-client` 仍可通过，但窗口不会启动。

面板提供：

- 每颗行星一个勾选框（订阅 / 退订）；自己的行星默认订阅且不可退订；
- 自己的公转 / 自转滑杆；
- 摄像机锁定自己的行星；未订阅行星不显示，重新勾选后按 `Snapshot` 恢复。

命令行：

```text
scene-client [--server 127.0.0.1:7721] [--name NAME] [--color 0xRRGGBB]
```

### headless e2e

```console
$ just e2e-scene
```

它在一个 compio 运行时里起真实服务端与两个真实客户端，验证 join、目录、选择性订阅、
退订、重新订阅、更新广播，以及拥有者断开后的 `Removed` 清理。

### 测试

```console
$ cargo test -p mptp_smux_v1_demo          # 15 项集成测试，无 Bevy
$ cargo test -p mptp_smux                  # SuffixStream 单元测试
$ cargo check -p mptp_smux_v1_demo --features bevy-client --bin scene-client
```

## feature / 二进制

```toml
[features]
default = []
bevy-client = ["dep:bevy", "dep:bevy_egui"]

[[bin]]
name = "scene-client"
required-features = ["bevy-client"]
```

`scene-server` / `scene-smoke` 默认可构建，不拉 Bevy。

## 已知 API 缺口

1. **客户端退订不可被服务端及时观察**：丢弃 `Response` 后服务端写端不会立即失败；
   建议给 `mptp_smux` 补一个与 `MptpSession::close_req_async` 对称的
   `Response::close_resp_async`（或等价入口）。本次按纪律只记录、未新增未确认的公共
   API，demo 暂用业务层 `/scene/unsubscribe`。
2. **客户端无限 `Push`** 仍只有有限源语义；本 demo 不需要。
3. 文件级细节与验证记录见
   [`dev-notes/mptp-smux-demo-20261111-1934.md`](../dev-notes/mptp-smux-demo-20261111-1934.md)。

## `mptp_smux` API 使用矩阵

| 需求 | API |
| --- | --- |
| 加入 / 更新 | `MptpClient::invoke` + `body_bytes` + `send_async` |
| 目录 | `MptpClient::view` + `Response::body_async` |
| 数量 / 版本 | `MptpClient::head` + `Response::headers` |
| 订阅 | `MptpClient::pull` + `Response::suffix_async` |
| 持续行星流 | `ResponseBuilder::suffix_stream_async` + `SuffixStream::write_chunk_async` |
| 收尾 | `SuffixStream::finish_async` |
| 405 | `TrServeHandler::allowed_methods` + `MethodSet` |
| 404 | handler 内 `Status::NotFound` |
| 取消 | `.may_cancel_with(token)` |
| 多 session | `accept_async` + compio `spawn_local` |

---

**English summary.** The demo rewrites `mptp_smux_v1_demo` as a Bevy solar-system scene and
doubles as an API stress test for `mptp_smux`. The server runs on one compio thread; the
Bevy client runs on the main thread plus one compio network thread. Clients discover
planets with `View`, subscribe to a chosen subset with per-planet `Pull` streams, and
receive an infinite denco value stream through `SuffixStream::write_chunk_async`. Dropping
a `Pull` unsubscribes; resubscribing starts with a fresh `Snapshot`. Slow subscribers are
removed through a bounded queue without blocking others. All MPTP/smux transport logic stays
inside `mptp_smux`; this crate only owns scene state and rendering. A known API gap (the
server cannot promptly observe a dropped `Response`) is recorded in the dev-note; the demo
uses a business-level `/scene/unsubscribe` hint until a public symmetric close API is
approved.
