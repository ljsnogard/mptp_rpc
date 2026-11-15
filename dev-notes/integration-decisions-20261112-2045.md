# MPTP 服务端框架：集成阶段的设计决策

- 日期：2026-11-12 20:45
- 状态：**决策记录**；本文固定集成阶段（P1 demo 迁移、P3 收尾）的几个待定点，不实施代码
- 前置：框架核心 `mptp_serving` 已完成 P0 + P2 + P3（框架侧），见
  [`mptp-api-plan-20261112-1850.md`](mptp-api-plan-20261112-1850.md) 与
  [`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md)

## 0. 现状一句话

`mptp_serving` 已经落地：分层 `RadixRouter`、`Endpoint` / `EndpointMetadata`、
`TrMiddleware` + `Pipeline`（线性前置）、`MptpContext`（含连接状态 `S`）、
`RequestCancel`、类型化路径参数（`TrFromRouteParam`）。剩下的都是「把框架接进真实传输 /
应用」的集成工作，需要先定下面三个决策。

## 1. 决策 A：请求级取消令牌进 `MptpContext`

`RequestCancel` 已定义（对象安全擦除，`from_token` 要求 `K: Clone + Cancellation: Send + 'static`）。
把它放进 `MptpContext` 需要在 servlet 胶水里 `RequestCancel::from_token(cancel.clone())`。

**约束**：这会让 `RadixRouter` 的 `TrServeHandler` future 的 `may_cancel_with<K>` 额外要求
`K: Clone`（+ `K::Cancellation: Send + 'static`），是**公开 API 约束变化**（会沿
`mptp_smux::MptpServlet` 传播到调用方）。

**决策**：**暂缓**。项目里的令牌（`NonCancellableToken` / `CancelledToken` /
`SceneCancelToken`）都已 `Clone`，约束实际上可满足；但按 `AGENTS.md` 第 1 条，这条公开
约束在实施前先跟团队确认。确认后只需：`MptpContext` 加 `cancel: RequestCancel` 字段 +
servlet 胶水构造并传入。

## 2. 决策 B：`!Send`（compio）传输的 handler 变体

`TrEndpointHandler::handle` 返回 `Box<dyn Future + Send>`，只适用 `Send` 传输（tokio）。
compio 单线程传输的 channel 半边是 `!Send`，无法挂到当前 handler。

**决策**：框架**保持 Send-by-design**（与框架设计 §6.6、router 设计稿的取舍一致）；compio
场景 demo 暂不迁到 `mptp_serving`。将来要支持 `!Send`，两种可选路径（择一，都属后续）：

1. 提供并行的非 `Send` 变体（`TrLocalEndpointHandler` / `TrLocalMiddleware` /
   `LocalRadixRouter`，去掉 future 的 `Send`）——实现简单但整组复制；
2. 把「future 是否 `Send`」抽象成类型参数——能复用，但就是框架设计 §10 明确要避免的
   Tower 式类型体操。

倾向先按路径 1 做最小复刻（compio 场景价值高于抽象纯洁度），但要等 demo 迁移真正需要时再
动手。

### 2.1 补充（2026-11-12）：`Send` 问题不止 compio，且不可在 future 层解

尝试把 `rpc_demo`（tokio，`Send` 传输）的旧 `RpcRouter` 经 `ReqHandlerTarget` 挂到
`RadixRouter` 时**编译失败**：旧 `TrReqHandler` 的 future（`RouterHandleFuture`）不是
`Send`，无法装箱成 `Box<dyn Future + Send>`。

**根因（团队确认）**：`gen_mcf2` 生成的**带高阶类型的 future** 无法被 Rust 编译器穿透解释，
因此整条 `abs_buff` / `abs_denco` future 链上的 `!Send` **在 future 层无解**。

**因此设计要改**：不要要求调用链上每个 future 都 `Send`，而是把 `Send` 边界放到**分派层**：

1. **让 `MptpServlet` 处理请求这一层是 `Send`、可多线程分派**；或
2. **把请求打包成 `Send` 对象，经 `mpmc-chan` 分派给另一条流水线**（`rpc_demo` 现有的
   `RpcJob` + 队列 worker 就是这种形态，不是要消灭的东西）。

落到框架上：

- `TrEndpointHandler::handle` / `TrMiddleware::before` 的 future **去掉 `+ Send`**（handler /
  中间件在单条流水线上跑，不需要 `Send`）；
- `Send` 边界上移到「请求分派」：servlet 或更高层把请求打包成 `Send` 的 job，交给
  `mpmc-chan` 队列 / 多线程 worker；
- `ReqHandlerTarget` 不再要求旧 handler future `Send`，旧 handler 可原样接入。

## 3. 决策 C：`MptpConnection` 状态 owner

把「一连接 = 一个 binding + 一个 servlet + 一份状态」从调用约定升级为类型不变量：

```rust
// 放在 mptp_smux（binding 与 servlet 的宿主）
pub struct MptpConnection<C, H, S = ()> {
    binding_: DockBinding<C>,
    servlet_: MptpServlet<C, H>,
    state_: Arc<S>,          // 跨 session 共享只读；可变性由 S 内部处理
}
```

**决策**：属**绑定层**（`mptp_smux`），不放进 `mptp_serving`。它拥有 `DockBinding`（不可复用
到别的连接）、`MptpServlet`、`Arc<S>`，提供 `serve_async()`（accept/serve 循环）与
`state()`。`mptp_serving` 的 `MptpContext` 已支持 `S`，无需再改。

## 4. demo 迁移顺序

按「先 tokio 后 compio」：

1. **`rpc_demo`（tokio，`Send`）**：可用 `RadixRouter<SrvCfg>` + `ReqHandlerTarget` 包住现有
   两级 Trie / 队列 worker（`RpcRouter`），或逐步把 interface/service 展开成子 router。
   验收 `just e2e-rpc`；
2. **`mptp_smux_v1_demo`（compio，`!Send`）**：等决策 B 的 `!Send` 变体落地后再迁；
3. `mptp_cs_smoke`（仓库外）随 `rpc_demo` 迁移后回归 11 条 RPC。

## 5. 结论

框架侧到此为止；下一阶段的入口是**决策 A 确认后把取消令牌接进 ctx**，以及**决策 C 的
`MptpConnection` 落地 + `rpc_demo` 迁移**。P4（QUIC 作为 `abs_smux` 第二个实现）仍是设计
层，不在本轮集成范围内。

## 6. 英文摘要

The `mptp_serving` framework core (P0 + P2 + P3) is complete. What remains is integration,
and three decisions are recorded here: (A) wiring `RequestCancel` into `MptpContext` adds a
`K: Clone` bound to the servlet glue's `may_cancel_with` (a public-API change, deferred until
confirmed); (B) the framework stays `Send`-by-design, so the compio scene demo needs a future
non-`Send` handler variant before it can migrate; (C) `MptpConnection<C, H, S>` lives in
`mptp_smux` (owns `DockBinding` + `MptpServlet` + `Arc<S>`). Demo migration proceeds
tokio-first (`rpc_demo`), compio-later.
