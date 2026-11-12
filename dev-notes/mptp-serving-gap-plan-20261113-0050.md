# mptp_serving 对齐设计目标的实施计划

- 日期：2026-11-13 00:50
- 状态：**已实施**（round 27–31，Phase A–F 全部落地；结果、关键决策与遗留缺口见
  [`mptp-serving-implementation-20261113-0115.md`](mptp-serving-implementation-20261113-0115.md)）。
  本文件保留为计划与验收门槛的历史记录。
- 依据：`dev-notes/mptp-serving-framework-20261112-1845.md`（设计稿）逐条对照当前实现
- 前置状态：`feat/serv-framework` HEAD = round-26 合并后；`cargo check/clippy/test --workspace
  --offline` 全绿，demo 17 项集成测试不回归；本地分支已收敛到 `main` + `feat/serv-framework`

## 0. 已完成（与设计稿一致，无需再动）

- 路由只匹配：`RadixRouter::match_route → Endpoint + RouteParams`，复用 `matchit_route`。
- 端点携带元数据：`Endpoint` + `EndpointMetadata::get::<T>/insert`（`Arc<dyn Any>` downcast，
  即设计稿开放问题 1 的「前者」）。
- 处理器只关心业务：`TrEndpointHandler` 只拿 `MptpContext`。
- 连接作用域状态 `S`：`Arc<S>` 注入、处理器拿 `&S`。
- 请求级取消令牌 `RequestCancel` 进 `MptpContext`。
- `!Send` 结论落地（future 无 `Send`）。
- 内存纪律：handler / middleware future 进请求级 `bumpalo` arena（round-26）；`abs_str::String<A>`、
  `Vec<T, A>`；无 `OnceLock`（开放问题 6 已解）。
- 不做 Depot / Tower / 无分配器容器（设计稿 §1.2、§10）。

## 1. 差距清单（按优先级）

| # | 差距 | 设计稿 | 影响 | 风险 |
|---|---|---|---|---|
| G1 | 中间件是「线性前置」，不是洋葱 | §6.5 | 缺 after / `next` / 改写上下文；无法做「处理器之后」的策略 | **高**（生命周期 + arena） |
| G2 | `MptpContext` 无请求级运行期状态 | §6.4、§6.5 例 | 中间件无法 stash 鉴权结果 / 主体 / 配额 | **高**（公开 API 大改） |
| G3 | 「兜底 500」与观测上报未做 | §7④/⑤ | handler 未回复时无兜底；错误无处上报 | 中 |
| G4 | 同路径多方法端点不支持 | §6.7、§7 | `feeds/{id}` 同时挂 VIEW+PULL 不可行；405 Allow 无法取并集 | 中 |
| G5 | `MptpContext` 缺一级请求/回复 helper | §6.1 | handler 直接 `ctx.session_mut()` 摸 session | 低 |
| G6 | `Endpoint.metadata` 分配器写死 `Global` | §6.8 表 | 元数据集合无法用自定义分配器 | 低 |
| G7 | 元数据继承 / 组未做 | §11.4 | 子 router 无法继承父层默认元数据 | 低 |

## 2. 阶段划分与具体步骤

> 纪律：每个阶段开新分支 `feat/round-NN-*`，`cargo check/clippy/test --workspace --offline`
> 全绿后 `--no-ff` 合并回 `feat/serv-framework`；公开 API 变更先与用户确认（AGENTS.md 规则 1）。

### Phase A — 低风险补齐（G5 + 为 G3 铺路）

**目标**：把「读体 / 读后缀 / 写回复 / 改头」收成 `MptpContext` 一级 API，并引入「是否已回复」
的可观测状态，为 Phase E 的兜底 500 铺路。

**文件**：`mptp_serving/src/context_.rs`、`mptp_serving/src/serve_handler_.rs`，必要时
`mptp_smux/src/serving/session_.rs`（只读查询）。

**步骤**：
1. `MptpContext` 新增委托方法（不改 handler 现有用法，纯增量）：
   - `headers_mut(&mut self) -> &mut Headers`；
   - `response(&mut self, status) -> ResponseBuilder<'_, C>`；
   - `req_body_async(&mut self)` / `req_suffix_async(&mut self)`（委托 `session_mut()`，签名按
     `mptp_smux` 现有读视图）。
2. 查 `mptp_smux::serving::ServingSession` 是否已有「已写回复 / 回复前缀已发」的只读查询；
   若没有，在 `session_.rs` 加一个 `response_started(&self) -> bool`（装配期零成本，只读标志）。
3. 补 `MptpContext::reply_written(&self) -> bool` 委托。
4. 测试：新增单测覆盖 `headers_mut` / `reply_written` 的委托行为；demo 不回归。

**验收**：workspace check/clippy/test 全绿；demo 17 项不回归。

### Phase B — 路由支持同路径多方法端点（G4）

**目标**：让同一个 `pattern` 能挂多个 `Endpoint`（不同 `MethodSet`），匹配时按方法选一个、
405 时 `Allow` 取并集；对应设计稿 §6.7 的 `feeds/{id}` VIEW+PULL。

**文件**：`mptp_serving/src/router_.rs`、`mptp_serving/src/endpoint_.rs`。

**步骤**：
1. 把 `trie_: Trie<Endpoint<C, S>>` 改为 `Trie<EndpointSet<C, S>>`（`EndpointSet` 持
   `Vec<Endpoint<C, S>, A>`，同 path 的端点按插入顺序）。
2. `bind` / `bind_endpoint`：若 path 已存在则 `push` 到该 set，否则 `insert` 新 set。
   `matchit_route::Router::insert` 的重复路径冲突要改成「先 `at` 查、命中则取回再 push」或
   维护一个旁路 `BTreeMap<path, index>`。
3. `match_method_`：在 set 里按 `MethodSet::contains(method)` 选端点；`allowed` = set 内所有
   端点方法并集。
4. `MatchOutcome::Found` 的 `value` 改为返回选中的 `&Endpoint`；`MethodNotAllowed.allowed`
   为并集。
5. 测试：`feeds/{id}` 同时 VIEW + PULL；405 的 Allow 并集断言；demo 不回归。

**风险**：`Trie<EndpointSet>` 的 `EndpointSet` 需要带分配器（`Vec<T, A>`），`A` 从 router 传。

### Phase C — `MptpContext` 请求级运行期状态（G2）

**目标**：让中间件把「本请求已鉴权 / 主体 / 配额」等运行期状态显式放进请求上下文（设计稿
§6.4/§6.5），而不是 Depot 或元数据。

**这是公开 API 大改，必须先与用户确认形状**，两个候选：

- **C1（推荐）**：给 `MptpContext` 加请求级泛型 `R: 'f = ()`，并提供 `state_mut() -> &mut R` /
  `state() -> &R`。`R` 由装配者提供（如 `RequestState { principal: Option<Principal>, .. }`），
  `TrEndpointHandler` / `TrMiddleware` / `RadixRouter` / `Endpoint` 都要加 `R` 泛型参数。
  - 优点：类型安全、无 `dyn Any`、与设计稿「显式字段」一致；
  - 代价：四处泛型参数扩散，示例/main 都要改；可作为后续大 round。
- **C2（备选）**：固定一个 `RequestState` 结构体（先只放 `principal: Option<Arc<dyn Any + Send + Sync>>`
  之类），`MptpContext` 直接持有 `&'f mut RequestState`；避免泛型扩散，但扩展性差。

**建议**：先写一份小设计稿确认 C1/C2，再实施；若时间不够，Phase C 可整体后置。

**步骤（按 C1）**：
1. `MptpContext<'f, C, S, R = ()>` 增加 `request_state_: &'f mut R`；构造 `new_` 增加参数。
2. `TrEndpointHandler<C, S, R = ()>`、`TrMiddleware<C, S, R = ()>`、`Endpoint<C, S, R = ()>`、
   `RadixRouter<C, A, S, R = ()>` 加 `R`。
3. `serve_handler_` 在请求作用域创建 `R`（`R: Default` 或由装配者注入工厂）。
4. 中间件可 `ctx.request_state_mut()` stash；处理器可 `ctx.request_state()` 读。
5. 测试：一个「鉴权中间件写 principal、处理器读 principal」的端到端用例。

### Phase D — 中间件升级为洋葱（G1）

**目标**：把 `before(endpoint, ctx) -> Option<Status>` 升级为可包裹 `next` 的洋葱模型
（设计稿 §6.5）。**最高风险项，建议先做 spike。**

**文件**：`mptp_serving/src/middleware_.rs`、`mptp_serving/src/router_.rs`。

**两个候选形状**：

- **D1（faithful，设计稿原样）**：
  ```rust
  pub trait TrMiddleware<C, S = (), R = ()>: Send + Sync {
      fn call<'f>(
          &'f self,
          endpoint: &'f Endpoint<C, S, R>,
          ctx: &'f mut MptpContext<'f, C, S, R>,
          next: &'f mut dyn FnMut(&'f mut MptpContext<'f, C, S, R>) -> MiddlewareFuture<'f>,
      ) -> MiddlewareFuture<'f>;
  }
  ```
  `Pipeline::run` 用一个「索引 + 递归 next」的闭包链驱动。难点：`next` 是 `&mut dyn FnMut`
  且要跨 `.await` 存活，与 arena future + `'a`/`'f` 拆分叠加，生命周期会很绕。
- **D2（简化，推荐先做）**：给 `TrMiddleware` 增加可选的 `after`：
  ```rust
  trait TrMiddleware {
      fn before<'a,'f>(&'a self, ep: &'a Endpoint, ctx: &'a mut MptpContext<'f,..>) -> MiddlewareFuture<'a>;
      fn after<'a,'f>(&'a self, ep: &'a Endpoint, ctx: &'a mut MptpContext<'f,..>) -> MiddlewareFuture<'a> {
          // 默认 no-op（返回立即 Ok）
      }
  }
  ```
  `Pipeline::run`：正序跑 `before`（任一 `Some(status)` 短路），再调 handler，再**逆序**跑
  `after`。没有 `next` 回调，生命周期简单，能覆盖「前置 + 后置」的大多数需求（日志耗时、
  计时、审计），但中间件拿不到「next 的返回值」、也无法真正包裹。

**建议**：先 D2（低风险、能落地），把 D1 记为「若确实需要包裹语义再评估」。

**步骤（D2）**：
1. `TrMiddleware` 加 `after`（默认 no-op）。
2. `Pipeline::run` 改为「before 正序 → handler → after 逆序」；短路逻辑保持。
3. 测试：一个记录「before/after 顺序」的中间件；demo 不回归。
4. 若后续要做 D1，再单独开 round。

### Phase E — 兜底 500 + 观测上报（G3）

**目标**：设计稿 §7④/⑤「handler 返回 Ok/Err 但未回复 → 兜底 500；错误经观测通道上报」。

**文件**：`mptp_serving/src/serve_handler_.rs`、`mptp_serving/src/router_.rs`，可能
`mptp_serving/src/handler_.rs`（观测 hook）。

**步骤**：
1. `router_handle_async_` / `Pipeline::run` 之后：若 `!ctx.reply_written()`，写
   `Status::InternalServerError` 的无体回复（500）。
2. 观测：给 `RadixRouter` 或 `TrServeHandler` 加一个可选错误 hook
   （`fn on_error(&self, err: &ServeError)` 或 `Arc<dyn Fn>`），默认 no-op；错误不泄漏到线上。
3. 测试：一个「handler 返回 Ok 但不写回复」的端点 → 客户端收到 500；一个「handler 返回 Err」
   → 500 + hook 被调用。

### Phase F — 收尾小项（G6/G7）

- **G6**：`Endpoint<C, S>` 的 `metadata_: EndpointMetadata<A>` 参数化（`A` 从 router 传），或把
  元数据放进 endpoint 自有 `bumpalo` arena（设计稿 §6.8 表）。
- **G7**：元数据继承 / 组（设计稿 §11.4）：`bind_router` 时附加「组默认元数据」，端点可覆盖。
- 设计稿 §6.6 的 `handle<'f>` 单生命周期与当前 `'a`/`'f` 双生命周期的形状差异，属纯命名/形状，
  可与 Phase D 一起评估是否收敛。

## 3. 每个阶段的验收门槛（硬性）

1. `cargo check --workspace --all-targets --offline` 全绿；
2. `cargo clippy --workspace --all-targets --offline -- -D warnings` 全绿；
3. `cargo test --workspace --offline` 全绿；
4. `mptp_smux_v1_demo` 17 项集成测试不回归；
5. 每阶段开新分支，`--no-ff` 合并回 `feat/serv-framework`；
6. 公开 API 变更（`R` 泛型扩散、`TrMiddleware` 形状、`MptpContext` 方法）先确认。

## 4. 风险与回退

- **Phase C/D 是公开 API 大改**：若中途发现生命周期无法自洽，回退到「保持线性 + 不加 R」，
  把改动限制在 Phase A/B/E/F。
- **Phase D 的 `next` 回调**：与 arena future 叠加的生命周期是最大不确定点；先 spike 一个最小
  例子（无 macro、无 demo）验证能过编译器，再推广。
- **Phase B 的 `Trie<EndpointSet>`**：`matchit_route` 是 `no_std` trie，`EndpointSet` 的分配器
  参数要顺着 router 的 `A` 走；若太绕，可先 `Vec<Endpoint>` + `Global`，后续再参数化。

## 5. 新会话接续提示（可直接复制）

> 见 §6 的「新会话提示词」块。

## 6. 新会话提示词

```text
# 接续任务：mptp_serving 对齐设计目标（G1–G7）

## 工作区
- 目录：/home/lino/projects/me.noli/old-cantare/mptp_rpc
- 分支：feat/serv-framework（当前 HEAD = round-26 合并后，check/clippy/test 全绿，demo 17 项集成测试通过）
- 先执行 `git status` / `git log --oneline -8` 核对；当前工作区应为干净。

## 必读
1. dev-notes/mptp-serving-framework-20261112-1845.md（设计稿）
2. dev-notes/mptp-serving-gap-plan-20261113-0050.md（本计划，含差距清单 G1–G7、阶段 A–F、验收门槛）

## 目标（按依赖顺序，可跨会话分阶段）
- Phase A（低风险）：MptpContext 一级 helper + reply_written 状态（G5/G3 铺路）
- Phase B：路由同路径多方法端点（G4）
- Phase C：MptpContext 请求级运行期状态 R（G2，公开 API 大改，先确认形状）
- Phase D：中间件洋葱化（G1，最高风险，建议先 D2「before/after」再评估 D1「next 回调」）
- Phase E：兜底 500 + 观测 hook（G3）
- Phase F：元数据分配器参数化 / 元数据继承（G6/G7）

## 纪律（必须遵守）
1. cargo 命令必须加 `--offline`（~/.cargo/git/db 只读）。
2. 每阶段开新分支 `feat/round-NN-*`，`--no-ff` 合并回 `feat/serv-framework`。
3. 公开 API 变更先与用户确认（AGENTS.md 规则 1）。
4. 验收：`cargo check --workspace --all-targets --offline`、
   `cargo clippy --workspace --all-targets --offline -- -D warnings`、
   `cargo test --workspace --offline` 全绿，demo 17 项不回归。
5. 测试函数中英双语文档注释；私有成员/函数以 `_` 结尾；错误用 `?`；不无理由 unsafe；
   单文件才 `cargo fmt`；`python3 format_use_imports.py --check <改动文件>`。
6. git 提交：`refactor(mptp_serving): ...` / `feat(mptp_serving): ...` + 中文正文；
   合并 commit 用 `merge round-NN-<phase>: ...`。

## 起点建议
先确认 Phase C 的 R 泛型形状（C1 vs C2）与 Phase D 的洋葱形状（D1 vs D2），再动手。
建议先做 Phase A（纯增量、零风险），再做 Phase B，Phase C/D 大改前先 spike。
```

## 7. 现状快照（供接续核对）

- `git log --oneline -6` 应看到 `merge round-26-p4`、`refactor(...) arena-allocate...`、
  `merge round-25-p3`、`refactor(...) delete legacy RPC serving layer (P3)`。
- 本地分支应只有 `main` + `feat/serv-framework`（远程 `github/*` 未动）。
- `mptp_serving/src/` 现有模块：`context_.rs`（MptpContext + arena）、`endpoint_.rs`、
  `handler_.rs`（TrEndpointHandler + HandlerFuture + box_future_）、`metadata_.rs`、
  `middleware_.rs`（线性 before）、`route_param_.rs`、`route_params_.rs`、`router_.rs`
  （RadixRouter）、`serve_handler_.rs`、`req_handler_target_.rs`。
