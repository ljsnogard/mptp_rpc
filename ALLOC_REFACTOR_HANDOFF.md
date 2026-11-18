# MPTP per-request arena 重构交接

- 时间：2026-11-19 01:05（Asia/Shanghai）
- 仓库：`/home/lino/projects/me.noli/old-cantare/mptp_rpc`
- 当前分支：`feat/per-request-arena`
- 状态：**全部完成，workspace 绿**
- 新 green tag：`green/per-request-arena`

> 本文不是 `dev-notes`。它记录本次重构的最终状态、验证结果与实施中的关键决策。

## 1. 总目标（已达成）

把 `mptp_rpc` 的 MPTP 协议栈从「每个 client / servlet 一个粗粒度 arena」改成
**per-request / per-session `BumpListAlloc`**，并把 `Headers` / `RequestPrefix` /
`ResponsePrefix` 的生命周期绑定到真正拥有它们的 client / servlet。

最终形状：

```rust
pub type RequestAlloc<'a> =
    BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>;
```

- 内层 `BumpListAlloc`：本次请求 / 本条会话自己的 arena；
- `Arc`：拥有内层 arena，控制块从 client / servlet 的 root arena 分配，不用 `Global` 控制块；
- `&'a BumpListAlloc`：把句柄生命周期绑定到 root，因此不会活过 client / servlet；
- `BumpListAllocRef`：`bumplist_alloc` 里的薄适配器，转发 `Allocator` / `AllocatorClone`
  与 denco 的 `TrArena` / `TrArenaBytes` / `TrAllocSource`。

**没有重新引入**：

- `mptp_smux::SessionAlloc`（已彻底删除，仓库内只剩迁移前的历史文档）；
- `BumpListArena` / `RequestArena`；
- `Arc<&'a BumpListAlloc>`；
- 为 `Arc<BumpListAlloc, A>` 直接写 `Allocator` impl。

## 2. 提交与回退点

```text
379e31e refactor(downstream): migrate mptp_serving, rpc_core_v1 and demo to RequestAlloc
9bb86b3 feat(mptp_smux): per-session RequestAlloc lifetimes on the serving side
2546af8 docs: add per-request arena refactor handoff
2a37424 fix(abs_mptp): tie response-prefix future output to the session arena
d0d2022 wip(arena): per-request BumpListAllocRef lifetimes (blocked)
0e36724 refactor(mptp_smux): drop unused client/connection-scope arenas
```

| tag | 用途 |
| --- | --- |
| `green/per-request-arena` | 本次重构完成后的 workspace green 点 |
| `green/pre-lifetime-refactor`（`0e36724`） | 重构前的完整 green 点，保留 |
| `rollback/pre-lifetime-refactor`（`3393c51`） | 旧回退点 |
| `wip/pre-lifetime-refactor`（`376bb92`） | 旧半成品 |

## 3. 最终实现要点

### 3.1 `mptp_smux` 客户端（此前已完成）

- `MptpClient` 持有 `Box<BumpListAlloc>` root；`new_request_alloc()` 返回
  `RequestAlloc<'a>`；`request` / `request_prefix` 等入口创建 per-request arena。
- `MptpSession<'a, C>` / `RequestBuilder<'a, C>` / `Response<'a, C>` /
  `RequestSuffixStream<'a, C>` 全程携带 arena 生命周期。
- `TrMptpSession::RecvRespPrefixAsync<'f>` 的输出使用 `RespPrefix<'a, C>` 并加
  `'a: 'f`（commit `2a37424`）。

### 3.2 `mptp_smux` 服务端

- `ServingSession<'a, C>`：`core_` / `location_` / `headers_` / `resp_prefix_` /
  `alloc_` 全部使用 `RequestAlloc<'a>`；`ServingParts<'a, C>` 同样带 `'a`。
- `MptpServlet<C, H>`：持有 `Box<BumpListAlloc>` root；
  `new_request_alloc<'a>(&'a self)` 每个入向子流新建 arena；
  `accept_async<'a, 'l>(&'a self, listener: &'l mut Listener<C>)` 把 **servlet 借用
  `'a`** 与 **listener 借用 `'l`** 分开，因此会话只受 servlet 生命周期约束。
- `ResponseBuilder<'b, 'a, C>` / `ResponseBodyWriter<'b, 'a, C>` /
  `SuffixStream<'b, 'a, C>`：`'b` 是对会话的借用，`'a` 是会话 arena；生成的 future
  全部按 `<'f, 'a, ...>` 展开并在输出里使用 `'a`。
- `TrServeHandler<C>`：

  ```rust
  type HandleAsync<'f, 'a>: TrMayCancel<'f, ...>
  where Self: 'f, C: 'f, 'a: 'f;

  fn handle_async<'f, 'a>(
      &'f self,
      method: AccessMethod,
      session: &'f mut ServingSession<'a, C>,
  ) -> Self::HandleAsync<'f, 'a>
  where 'a: 'f;
  ```

- `abs_mptp::serving::TrReqHandler` 同步拆成 `HandleAsync<'f, 'a>`，头表参数为
  `&'f mut ReqHeaders<'a, C>` + `'a: 'f`。这是 `LegacyHandler` / `ReqHandlerTarget`
  能把**会话 arena 里的头表**交给旧 handler 的必要条件；旧的 `ReqHeaders<'f, C>`
  要求 arena 生命周期等于借用生命周期，无法与长命的会话 arena 共存。

### 3.3 下游

- `mptp_serving`：
  - `MptpContext<'f, 'a, C, S, R>`：`'f` 请求借用、`'a` 会话 arena；
  - `arena_` 直接克隆会话 arena（不再单独建 P16K context arena）；
  - `ArenaFuture<'f, 'a, O>` / `HandlerFuture<'f, 'a>` /
    `MiddlewareFuture<'x, 'a>` / `NextFn<'x, 'f, 'a, ...>` 全部携带 arena 生命周期；
  - `TrEndpointHandler::handle<'f, 'x, 'a>`、`TrMiddleware::call<'x, 'f, 'a>`。
- `rpc_core_v1`：
  - `ClientConfig` / `ServingConfig` 的 `abs_mptp` 关联类型改为带 `'a` 的 GAT，
    统一使用 `mptp_smux::RequestAlloc<'a>`；
  - serving body helper / `typed_response` / `stream::slot` 的实现带上 arena 生命周期；
  - client proxy / typed helper 改用 `MptpClient::new_request_alloc`；
  - `TrSuffixFinish<'a>` 覆盖 `RequestSuffixStream<'a, C>` 与
    `SuffixStream<'b, 'a, C>`。
- `rpc_svc_macro`：生成的 handler 使用三生命周期
  `handle<'f, 'x, 'a>(&'x self, ctx: &'x mut MptpContext<'f, 'a, ...>)`。
- `mptp_smux_v1_demo`：
  - 配置改为 GAT + `RequestAlloc`；端点 / 中间件签名同步；
  - 服务端连接内部用 `FuturesUnordered` 并发驱动 session，而不是
    `spawn_local`（后者要求 `'static`，而会话借用 servlet root）；
  - 客户端后台 Pull 任务拥有 `Rc<MptpClient>`，并在任务**内部**创建会话；
    建流结果经一次性 `flume` 通道回传。这样会话借用只存在于任务状态机内部，
    不会逃逸到 `'static`。

### 3.4 测试

- 所有既有测试保留中英双语文档注释。
- `ring_prepare_` 的 client-only 测试按 feature 门控；新增
  `serving_env_exposes_docks_and_role_`（中英双语注释）覆盖 server-only 构建。

## 4. 以前踩过的坑（保留供参考）

client 侧曾卡在：

```text
error[E0478]: lifetime bound not satisfied
```

根因：生成 future 的输出实际是 `RespPrefix<'a, C>`（会话 arena），关联类型却写成
`'f`。解法：**输出含 `RequestAlloc<'a>` 时，关联类型/方法必须用 `'a`，并加
`'a: 'f`**。服务端迁移时同样适用。

另外，`ServingSession<'a, C>` 的 builder 必须使用两个生命周期：
`'b`（对会话的借用）与 `'a`（会话 arena）；`&mut` 对 arena 生命周期不变，不能把
`ServingSession<'a, C>` 缩短成 `ServingSession<'b, C>`。

## 5. 验证结果（全绿）

```text
cd /home/lino/projects/me.noli/old-cantare/mptp_rpc

timeout 300 cargo test --workspace
# 全部通过（42 个 test result: ok；含 doc-tests）

cargo check --workspace --all-targets
# 通过

cargo clippy --workspace --all-targets
# 通过，无 warning

cargo test -p mptp_smux --no-default-features --features server
# 19 passed + doc-tests 1 passed / 1 ignored

cargo test -p mptp_smux --no-default-features --features client
# 21 passed + doc-tests 1 passed / 1 ignored

python3 ../format_use_imports.py --check mptp_smux mptp_serving rpc_core_v1 \
    rpc_svc_macro mptp_smux_v1_demo abs_mptp
# 全部合规
```

## 6. 后续可选事项

- 本轮公共 API 的 `# Examples` 仍以说明性代码为主，部分为 `ignore`；后续可补可编译
  示例。
- `MethodSet::to_allow_value()` 仍返回全局 `String`；如在意 405 回复的分配，可加
  `to_allow_value_in(alloc)`。
- `mptp_smux_v1_demo` 的 session 并发从 `spawn_local` 改成
  `FuturesUnordered`；单线程 compio 下语义一致，若未来需要真正的多线程 session，
  应把 servlet root 的所有权设计成可跨线程共享的形式再评估。
