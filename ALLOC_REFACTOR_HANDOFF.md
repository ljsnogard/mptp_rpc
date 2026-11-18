# MPTP per-request arena 重构交接

- 时间：2026-11-19 00:37（Asia/Shanghai）
- 仓库：`/home/lino/projects/me.noli/old-cantare/mptp_rpc`
- 当前分支：`feat/per-request-arena`
- 当前 HEAD：`2a37424 fix(abs_mptp): tie response-prefix future output to the session arena`
- 工作区：干净

> 本文不是 `dev-notes`。它只记录当前重构的目标、进度、阻塞点和下一步，供新会话直接读取。

## 1. 总目标

把 `mptp_rpc` 的 MPTP 协议栈从「每个 client / servlet 一个粗粒度 arena」改成
**per-request / per-session `BumpListAlloc`**，并让 `Headers` / `RequestPrefix` /
`ResponsePrefix` 的生命周期绑定到真正拥有它们的 client / session，而不是使用默认
`Global` 或一个独立的 `SessionAlloc`。

最终形状（目标）：

```rust
pub type RequestAlloc<'a> =
    BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>;
```

- 内层 `BumpListAlloc`：本次请求 / 本条会话自己的 arena；
- `Arc`：拥有内层 arena，控制块从 client / servlet 的 root arena 分配，不用 `Global` 控制块；
- `&'a BumpListAlloc`：把句柄生命周期绑定到 root，因此不会活过 client / servlet；
- `BumpListAllocRef`：`bumplist_alloc` 里的薄适配器，转发 `Allocator` / `AllocatorClone`
  与 denco 的 `TrArena` / `TrArenaBytes` / `TrAllocSource`。

**不要再引入**：

- `mptp_smux::SessionAlloc`（已删除的过渡类型）；
- `BumpListArena` / `RequestArena` 之类新 arena（用户已明确否决）；
- `Arc<&'a BumpListAlloc>`（默认 `Global` 的 Arc 控制块，用户已明确否决）；
- 为 `Arc<BumpListAlloc, A>` 直接写 `Allocator` impl（孤儿规则不允许，见 §4）。

## 2. 状态快照

| 项目 | 状态 |
| --- | --- |
| `bumplist_alloc` 推送版本 | `b96b19b Add thin wrapper impl Allocator for any Deref<Target = BumpListAlloc>`，已推送到 gitee；`mptp_rpc/Cargo.toml` 的临时 `[patch]` 已删除，Cargo 使用 `#b96b19be` |
| 最后 workspace green 点 | `0e36724`，tag `green/pre-lifetime-refactor` |
| 回退点 | tag `rollback/pre-lifetime-refactor` → `3393c51`；tag `wip/pre-lifetime-refactor` → `376bb92`（半成品） |
| 当前分支 client feature | **绿**：`cargo test -p mptp_smux --no-default-features --features client` 通过（21 个单测；doc-tests 1 passed / 1 ignored） |
| 当前分支 server feature | **红**：`serving/servlet_.rs` / `serving/session_.rs` 仍在 import 已删除的 `SessionAlloc`；修掉 import 后还有 `ServingSession` 生命周期改动 |
| workspace | **红**：`mptp_serving` / `rpc_core_v1` / demo 仍使用旧的 `SessionAlloc` / 无生命周期接口 |
| 已清理分支 | 所有 `step/*` 分支已删除（均已 merge 进旧 HEAD） |

### 当前分支提交

```text
2a37424 fix(abs_mptp): tie response-prefix future output to the session arena
d0d2022 wip(arena): per-request BumpListAllocRef lifetimes (blocked)
0e36724 refactor(mptp_smux): drop unused client/connection-scope arenas
```

`d0d2022` 的提交信息写着 blocked；`2a37424` 已解决 client 侧的 E0478，所以
**不要因为 `d0d2022` 的 blocked 信息就回退 client 侧**。

## 3. 已完成

### 3.1 `bumplist_alloc`（已推送，不在本仓库）

`src/alloc_ref_.rs` 定义了：

```rust
pub struct BumpListAllocRef<D, A = Global>
where
    D: Deref<Target = BumpListAlloc<A>>,
    A: Allocator,
{ ... }
```

- 为 `BumpListAllocRef<D, A>` 实现 `Allocator` / `AllocatorClone`，转发给 `&*inner`；
- `denco_arena` 下实现 `TrArena` / `TrArenaBytes` / `TrAllocSource`；
- 用户改写的版本使用直接 `Deref<Target = BumpListAlloc<A>>` 约束，不再有
  `TrStableDeref`；
- 已验证 `cargo test --features denco_arena`、clippy、Miri（`alloc_ref_` 测试）。

### 3.2 `abs_mptp`

已改成生命周期 GAT：

- `client::config::TrConfig`：
  ```rust
  pub trait TrConfig: 'static {
      type ReqPrefix<'a>: TrRequestPrefix where Self: 'a;
      type RespPrefix<'a>: TrResponsePrefix where Self: 'a;
      // ...
  }
  ```
- `serving::config::TrConfig`：
  ```rust
  pub trait TrConfig: 'static {
      type ReqHeaders<'a>: TrHeaders where Self: 'a;
      type RespHeaders<'a>: TrHeadersMut where Self: 'a;
      type ReqPrefix<'a>: TrRequestPrefix where Self: 'a;
      type RespPrefix<'a>: TrResponsePrefix where Self: 'a;
  }
  ```
- `TrMptpClient`：
  ```rust
  type Session<'f>: TrMptpSession<'f, Self::Config> where Self: 'f;
  type RequestAsync<'f>: TrMayCancel<'f, MayCancelOutput =
      Result<Self::Session<'f>, Self::ReqErr>> where Self: 'f;
  ```
- `TrMptpSession<'a, C>` 现在带 arena 生命周期 `'a`：
  ```rust
  pub trait TrMptpSession<'a, C>
  where
      C: client_config::TrConfig,
  {
      type RecvRespPrefixAsync<'f>: TrMayCancel<
          'f,
          MayCancelOutput = Result<client_config::RespPrefix<'a, C>, Self::Err>,
      >
      where
          Self: 'f,
          'a: 'f;

      fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f>
      where
          'a: 'f;
  }
  ```
- `serving::handler::TrReqHandler<C>` 的 `headers` 参数已改成
  `&'f mut srv_config::ReqHeaders<'f, C>`。

### 3.3 `mptp_smux`（client feature 已绿）

- `alloc_.rs` 重写为：
  ```rust
  pub type RequestAlloc<'a> =
      BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>;

  pub fn new_request_alloc<'a>(
      root: &'a BumpListAlloc,
      page_size: PageSize,
  ) -> Result<RequestAlloc<'a>, BumpListAllocError>;
  ```
  内层 `BumpListAlloc` 构造时是空的，`Arc::new_in(inner, root)` 是安全的。
- `MptpClient<C>`：
  - 字段 `conn_: SharedConn<C>`、`root_: Box<BumpListAlloc>`、`page_size_: PageSize`；
  - `new_request_alloc<'a>(&'a self) -> RequestAlloc<'a>`；
  - `request<'a>(&'a self, ...) -> RequestBuilder<'a, C>`。
- `RequestBuilder<'a, C, B, S>`：
  - `alloc_: RequestAlloc<'a>`；
  - `prefix_: RequestPrefix<RequestAlloc<'a>>`；
  - `TrBodySend<'s, C>` / `TrSuffixSend<'s, C>` 现在带 session 生命周期 `'s`；
  - `send_async(self) -> SendRequestAsync<'a, 'a, C, B, S>`。
- `MptpSession<'a, C>`：
  - `core_: SessionCore<ChannelTx<C>, ChannelRx<C>, RequestAlloc<'a>>`；
  - `alloc_: RequestAlloc<'a>`；
  - `req_prefix()` 返回 `Option<&RequestPrefix<RequestAlloc<'a>>>`。
- `Response<'a, C>`：
  - `session_: MptpSession<'a, C>`；
  - `prefix_: ResponsePrefix<RequestAlloc<'a>>`。
- `RequestSuffixStream<'a, C>` 现在带 `'a`。
- `client::config::TrConfig` 的 supertrait 用 HRTB 钉死：
  ```rust
  Self: 'static
      + Sized
      + for<'a> TrMptpClientConfig<
          ReqPrefix<'a> = RequestPrefix<RequestAlloc<'a>>,
          RespPrefix<'a> = ResponsePrefix<RequestAlloc<'a>>,
      >
  ```
- `client/config.rs` 新增具体别名 `ReqPrefix<'a>` / `RespPrefix<'a>`；
  `serving/config.rs` 也新增 `ReqPrefix<'a>` / `RespPrefix<'a>` / `ReqHeaders<'a>` /
  `RespHeaders<'a>`。

## 4. 关键陷阱：E0478 的根因与已用解法

client 侧曾卡在：

```text
error[E0478]: lifetime bound not satisfied
   --> mptp_smux/src/client/session.rs:451:11
    |
451 |         = SessionRecvRespPrefixAsync<'f, 'a, 'f, C>
```

试过但**没有解决问题**的方案：

- 在关联类型处写 `impl TrMayCancel<...>`（nightly `impl_trait_in_assoc_type` 已启用）；
- 把 trait 方法改成 RPITIT（`-> impl TrMayCancel<...>`）；
- 给 impl 方法加 `where 'a: 'f`，但 trait 定义没有对应 bound；
- 把 `TrMptpSession` 改成带 `'a` 的 trait（这个本身是对的，但仍需下面这一步）。

真正原因是：

- `TrMptpSession::RecvRespPrefixAsync<'f>` 的 `MayCancelOutput` 原先写的是
  `client_config::RespPrefix<'f, C>`；
- 但解码出来的响应前缀实际分配在**会话 arena `'a`** 里，返回的是
  `RespPrefix<'a, C>`；
- 生成 future 的输出是 `'a`，关联类型却要求 `'f`，于是编译器要求 `'f: 'a`，
  即 E0478。

已用解法（commit `2a37424`）：

```rust
type RecvRespPrefixAsync<'f>: TrMayCancel<
    'f,
    MayCancelOutput = Result<client_config::RespPrefix<'a, C>, Self::Err>,
>
where
    Self: 'f,
    'a: 'f;

fn recv_resp_prefix_async<'f>(&'f mut self) -> Self::RecvRespPrefixAsync<'f>
where
    'a: 'f;
```

**服务端做同样迁移时，务必检查所有 generated future 的输出类型**：
如果输出里含 `RequestAlloc<'a>`，关联类型/方法必须用 `'a`，并用 `'a: 'f`
约束借用；不要写成 `'f`。

## 5. 剩余工作（按顺序）

### 5.1 `mptp_smux` server feature（下一步）

当前错误：

```text
error[E0432]: unresolved import `crate::SessionAlloc`
  --> mptp_smux/src/serving/servlet_.rs:34:5
  --> mptp_smux/src/serving/session_.rs:33:5
```

修掉 import 后，预计需要：

1. `ServingSession<'a, C>`：
   - `core_: SessionCore<..., RequestAlloc<'a>>`
   - `location_: String<RequestAlloc<'a>>`
   - `headers_: Headers<RequestAlloc<'a>>`
   - `resp_prefix_: Option<ResponsePrefix<RequestAlloc<'a>>>`
   - `alloc_: RequestAlloc<'a>`
2. `ServingParts<'a, C>`。
3. `ResponseBuilder` / `ResponseBodyWriter` 需要**两个**生命周期：
   - `'b`：对 `&'b mut ServingSession<'a, C>` 的借用；
   - `'a`：会话 arena。
   - 例如 `ResponseBuilder<'b, 'a, C>`、`ResponseBodyWriter<'b, 'a, C>`；
   - 不要试图把 `ServingSession<'a>` 缩短成 `ServingSession<'b>`：`&mut` 对 T 不变，
     除非整体改设计。
4. `MptpServlet<C, H>`：
   - `root_: Box<BumpListAlloc>` + `page_size_: PageSize`；
   - `new_request_alloc<'a>(&'a self) -> RequestAlloc<'a>`；
   - `accept_async<'a>(&'a self, ...) -> ServingSession<'a, C>`。
5. `TrServeHandler<C>` / `mptp_smux::serving::handler`：
   - `handle_async` 需要同时表达 handler 借用 `'f` 与会话 arena `'a`；
   - 可能写成 `handle_async<'f, 'a>(&'f self, session: &'f mut ServingSession<'a, C>)`
     或给 trait 加 `'a` 参数；先编译驱动，注意 generated future 的输出生命周期。
6. 405 / `ResponsePrefix`：
   - `ResponsePrefix::new(status)` + `headers_mut(session allocator)`；
   - `MethodSet::to_allow_value()` 目前仍返回全局 `String`，后续可换成
     `to_allow_value_in(alloc)`；本轮可以先保留。
7. 测试与文档同步更新；测试必须保留中英双语文档注释。

验证：

```bash
cd /home/lino/projects/me.noli/old-cantare/mptp_rpc
timeout 120 cargo check -p mptp_smux --no-default-features --features server
timeout 180 cargo test -p mptp_smux --no-default-features --features server
timeout 180 cargo test -p mptp_smux
cargo clippy -p mptp_smux --all-targets --all-features
```

### 5.2 下游迁移

当前 workspace 红，主要是：

- `mptp_serving`：
  - `MptpContext` / `ArenaFuture` / router / serve_handler；
  - 请求级 arena 现在应以 `RequestAlloc<'f>`（或带 session arena 的双生命周期）出现；
- `rpc_core_v1`：
  - 客户端 config / proxy / typed 的 `RequestPrefix<SessionAlloc>`、
    `Headers<SessionAlloc>`、`Response<C>`、`RequestSuffixStream<C>` 等；
  - 服务端 `ServingSession<C>` / `ResponsePrefix<SessionAlloc>`；
- `mptp_smux_v1_demo`、`abs_mptp-tests`；
- 全量检查：
  ```bash
  timeout 300 cargo check --workspace
  timeout 300 cargo test --workspace
  cargo clippy --workspace --all-targets
  python3 ../format_use_imports.py --check mptp_wire mptp_smux mptp_serving rpc_core_v1
  ```

### 5.3 收尾

- 确认全仓库不再有 `SessionAlloc` / `BumpListArena` / `Arc<&BumpListAlloc>`；
- 更新 `mptp_wire` / `mptp_smux` 的公开 API 文档与示例；
- 检查 `Cargo.toml` 没有临时 `[patch]`；
- 新的 workspace green 点打 tag（建议 `green/per-request-arena`），保留旧的
  `green/pre-lifetime-refactor`。

## 6. 当前验证命令与结果

```text
cd /home/lino/projects/me.noli/old-cantare/mptp_rpc

cargo test -p mptp_smux --no-default-features --features client
# 21 passed; doc-tests 1 passed, 1 ignored

cargo clippy -p mptp_smux --no-default-features --features client --all-targets
# 通过

cargo check -p mptp_smux --no-default-features --features server
# 当前失败：SessionAlloc unresolved import（下一步的起点）
```

不要运行全仓 `cargo fmt`；只对改动文件执行 `rustfmt --edition 2024 <file>`。

## 7. 新会话提示词（直接复制给新会话）

```text
你在 /home/lino/projects/me.noli/old-cantare/mptp_rpc 继续 MPTP per-request arena 重构。

第一步必须读取：
  /home/lino/projects/me.noli/old-cantare/mptp_rpc/ALLOC_REFACTOR_HANDOFF.md

然后执行：
  git status
  git branch --show-current
  git log --oneline -5
  git tag -l

确认当前分支是 feat/per-request-arena，HEAD 至少包含 2a37424。
当前状态：client feature 已绿；server feature 和 workspace 仍红。
最后完整的 green 点是 tag green/pre-lifetime-refactor（0e36724），不要破坏它。

目标：
1. 继续 §5.1：给 mptp_smux 的 ServingSession / ServingParts / ResponseBuilder /
   ResponseBodyWriter / MptpServlet / TrServeHandler 加 arena 生命周期，删除所有
   SessionAlloc import，实现 per-session RequestAlloc<'a>；
2. 继续 §5.2：迁移 mptp_serving、rpc_core_v1、mptp_smux_v1_demo、abs_mptp-tests；
3. 全量 cargo check/test/clippy 通过后，打新的 green tag。

设计约束（详见文档 §1、§4）：
- 使用 RequestAlloc<'a> = BumpListAllocRef<Arc<BumpListAlloc, &'a BumpListAlloc>>；
- 不要重新引入 SessionAlloc、BumpListArena、Arc<&'a BumpListAlloc>；
- 不要为 Arc<BumpListAlloc, A> 直接实现 Allocator（孤儿规则）；
- generated future 的输出如果含 RequestAlloc<'a>，关联类型/方法输出必须使用 'a，
  并加 'a: 'f；不要写成 'f。client 侧的 E0478 已由 commit 2a37424 解决，可作为参考；
- ServingSession 的 builder 需要两个生命周期：借用 'b 与会话 arena 'a；
- 公开 API 变更已经在本次重构中推进；新增无法从文档推断的设计决策先问用户；
- 测试必须带中英双语文档注释；单元测试用 timeout，全量测试用 timeout 300；
- 只对改动文件运行 rustfmt，不要全仓 cargo fmt；
- 临时文件放 ./.tmp。

建议节奏：
- 先修 --no-default-features --features server，直到 cargo check/test 通过；
- 再修默认 features 与下游；
- 每完成一个可编译里程碑做一次 commit，不要一次性堆到最后；
- 若服务端双生命周期推进困难，先记录具体 E0xxx 错误与最小复现，不要回退 client 侧。
```
