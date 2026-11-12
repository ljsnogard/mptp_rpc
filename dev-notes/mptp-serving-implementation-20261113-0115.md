# mptp_serving 差距补齐的实施记录（round 27–31，Phase A–F）

- 日期：2026-11-13 01:15
- 状态：**实施记录**；G1–G7 的框架侧已落地，`feat/serv-framework` 合并后
  `cargo check` / `clippy -D warnings` / `test --workspace --offline` 全绿
- 依据：[`mptp-serving-gap-plan-20261113-0050.md`](mptp-serving-gap-plan-20261113-0050.md)
  （差距清单与阶段划分）、[`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md)
  （设计稿）
- 分支：`feat/round-27-a`、`feat/round-28-b`、`feat/round-29-c`、`feat/round-30-d`、
  `feat/round-31-ef`，均 `--no-ff` 合回 `feat/serv-framework`

## 0. 结论

| 差距 | 结果 | 落点 |
| --- | --- | --- |
| G5 一级 helper | 完成 | `MptpContext::{headers_mut, response, req_body_async, req_suffix_async, reply_written}` |
| G4 同路径多方法 | 完成 | `EndpointSet` + `match_method_` 选择闭包 / `Allow` 并集 |
| G2 请求级运行期状态 | 完成（C1，泛型 `R`） | `MptpContext<'f, C, S, R>` + `RadixRouter` 新构造函数 |
| G1 中间件洋葱 | 完成（D1，`next` 回调） | `TrMiddleware::call` + `Pipeline::run_from_` |
| G3 兜底 500 + 观测 | 完成 | `settle_reply_` + `RadixRouter::with_error_hook` |
| G6 元数据分配器 | 完成 | `Endpoint<C, S, R, A>` + `Endpoint::new_in` |
| G7 组元数据继承 | 完成（同层；跨 router 显式） | `RadixRouter::with_group_metadata` + `EndpointMetadata::inherit` |

验收：`mptp_smux_v1_demo` 集成测试从 17 项增加到 28 项（`scene_stream_` 17 项不变），
全部通过。

## 1. Phase A：`MptpContext` 一级 helper（G5 + G3 铺路）

- 五个纯增量委托：`headers_mut`、`response`、`req_body_async`、`req_suffix_async`、
  `reply_written`；handler 不再必须 `session_mut()` 才能读写。
- `reply_written` 直接委托 `ServingSession::sent_response_prefix().is_some()`：它与内部
  `responded_` 同时置位，因此 `mptp_smux` 只追加导出了 `RequestBodyAsync` /
  `RequestSuffixAsync`（让委托方法返回同形的可取消 future），没有新增状态。
- `mptp_serving` 增加 `abs_buff` 依赖，仅用于体 / 后缀读视图的 trait 约束。
- 测试：demo 夹具加 `/test/ctx`、`/test/ctx-body` 与 `CtxProbe`，`tests/ctx_helpers_` 两项。

## 2. Phase B：同路径多方法端点（G4）

- 路径树的值从 `Endpoint` 改为 `EndpointSet<C, S, R, A>`（`Vec<Endpoint, A>`）：
  `bind` / `bind_endpoint` 先 `remove` 同路径集合、追加、再 `insert` 回去。
  `matchit_route` 没有「按路由模式取回值」的读接口，`remove + insert` 是当前代价最小且
  不会影响其它路由的写法。
- `match_method_` 改为「选择闭包 + 允许集合闭包」：生产路径用
  `|set| set.select(method)`，405 用 `|set| set.allowed()`（并集）。纯逻辑单测用
  `Trie<Vec<TestValue>>` 验证选择与并集，不依赖 `C` / `S`。
- `Allowed` 头取并集的集成断言在 `tests/multi_method_`：`/test/multi` 同时挂 VIEW + PULL。
- 同路径同方法**不做冲突检测**（先插入先命中），已在文档写明：装配者应让方法集合互不相交。
- 分配器：当时用 `Arc<A>` 作集合分配器以回避 `A: Clone`；Phase F 统一改回 `A`，并在
  `bind*` / 构造点要求 `A: Clone`（见 §6）。

## 3. Phase C：请求级运行期状态（C1，G2）

- 用户确认采用 **C1**：`MptpContext<'f, C, S = (), R = ()>` 增加
  `request_state_: &'f mut R` 与 `request_state()` / `request_state_mut()`。
- `R` 每请求由 `bump.alloc(R::default())` 在请求级 arena 里新建，随请求整体释放；因此
  `TrServeHandler` 实现要求 `R: Default`，装配者不需要额外工厂。
- `R` 扩散到 `TrEndpointHandler` / `TrMiddleware` / `Pipeline` / `Endpoint` /
  `EndpointSet` / `RadixRouter` / `TrRouteMatcher` / `MatchOutcome`，全部带 `R = ()`
  默认，现有调用点零改动。
- 新增构造函数 `RadixRouter::new_with_request_state` 与
  `new_in_with_state_and_request`；原有 `new` / `new_in` / `new_with_state` /
  `new_in_with_state` 仍固定 `R = ()`，避免 `new(Arc::new(Root))` 出现 `R` 推断歧义。
- `rpc_svc_macro`：生成的 handler impl 改为 `impl<C, S, R>`，`install_into` 改为
  `install_into<C, S, R>`，宏端点可装进任意 `S` / `R` 的 router。
- demo：场景路由改为 `SceneRouter`（`R = SceneRequestState`），新增
  `PrincipalMiddleware`（写主体）与 `/test/state`（回写），`tests/request_state_` 两项。
- 决策：`R` 与元数据分工固定为「元数据 = 装配期声明；`R` = 本请求运行期事实」，与设计稿
  §6.4 一致；没有引入 `dyn Any` 状态袋。

## 4. Phase D：中间件洋葱化（D1，G1）

用户确认采用 **D1（`next` 回调）**，不是简化版 D2。实际形状：

```rust
pub type NextFn<'a, 'f, C, S = (), R = ()> =
    dyn FnMut(&'a mut MptpContext<'f, C, S, R>) -> MiddlewareFuture<'a> + 'a;

fn call<'a, 'f>(
    &'a self,
    endpoint: &'a Endpoint<C, S, R, M>,
    ctx: &'a mut MptpContext<'f, C, S, R>,
    next: &'a mut NextFn<'a, 'f, C, S, R>,
) -> MiddlewareFuture<'a>;
```

关键实现点（也是设计稿里「生命周期会很绕」的具体答案）：

1. **`next` 放进请求级 arena**：栈上局部闭包活不到调用者选定的 `'a`，而
   `bump.alloc(closure)` 返回 `&'f mut Closure`，可以收窄成 `&'a mut dyn FnMut`。
   闭包只捕获 `&self` / `&endpoint` / `usize`，没有 `Drop`，因此不会漏析构。
2. **保留 `'a` / `'f` 拆分**：`'a` 是本次借用，`'f` 是上下文内部借用；合成一个 `'f`
   会让 `ctx` 在 `.await` 之后无法再用。
3. **`Pipeline::run` 用一次 `&mut *ctx` 重借用**跑完整条链，跑完仍可用同一份 `ctx` 写
   `Some(status)` 的短路回复；`run_from_` 在 base case 调终端处理器。
4. 短路语义保留：返回 `Ok(Some(status))` 由管线统一写无体回复；返回 `Ok(None)` 且不调
   `next` 表示中间件自己写完回复。

内存代价：每一层中间件每次调用 `next` 会在 arena 里放一个闭包 + 一个 boxed future，
随中间件层数线性增长、随请求结束整体释放。

## 5. Phase E：兜底 500 + 观测 hook（G3）

- `serve_handler_` 在管线 / 根 handler 之后统一 `settle_reply_`：
  - 处理器返回 `Ok` 但 `!ctx.reply_written()` → 写 `500` 无体回复；
  - 处理器返回 `Err` → 先调 `RadixRouter::with_error_hook` 上报，再按同一规则兜底；
  - 已经写出回复前缀的请求不改写状态码（回复已经开始）。
- 错误只走 hook，不写线上；hook 是 `Arc<dyn Fn(&ServeError) + Send + Sync>`，
  与 `C` / `S` / `R` 无关。
- 测试：`/test/no-reply`、`/test/fail`、`/test/fail-after-reply` 三个端点，
  `tests/fallback_` 三项。

## 6. Phase F：元数据分配器参数化 + 组元数据继承（G6 + G7）

- `Endpoint<C, S = (), R = (), A = Global>`：`metadata_: EndpointMetadata<A>`，
  由 `Endpoint::new_in(..., alloc)` 传入；`Endpoint::new` 仍是 `A = Global` 的糖。
- `MatchOutcome` / `TrRouteMatcher` / `Pipeline` / `TrMiddleware` 追加分配器参数
  （全部默认 `Global`）；router 的 `A` 贯穿路径树、端点集合、元数据集合、中间件链。
- 装配点需要 `A: Clone`：构造函数用同一个分配器建子 router 列表与中间件链，`bind*`
  克隆它给端点集合与端点元数据。**读路径与 serve 路径不要求 `Clone`**。
  `Global` / `bumpalo::Bump` 都是 `Clone`，实际装配不受影响。
- G7：`RadixRouter::with_group_metadata` 把组默认元数据挂在本层，绑定时经
  `EndpointMetadata::inherit` 补进端点；「先插入先命中」保证端点自己的同类型标记优先。
- **G7 的边界**：`bind_router` 的子 router 已被 `dyn TrRouteMatcher` 擦除，父层无法在
  绑定时改写它。当前提供 `RadixRouter::group_metadata()`，由装配者构造子 router 时显式
  传递父层组元数据。若以后要自动继承，需要给 `TrRouteMatcher` 增加元数据注入面。
- 测试：`EndpointMetadata::inherit` 单测（端点优先、只补缺失类型）；demo 场景路由挂
  `SceneGroupTag("scene")`，`PrincipalMiddleware` 从端点元数据读取，`/test/state` 回写，
  `tests/request_state_` 断言组标记。

## 7. 遗留问题（下一轮候选）

1. **`rpc_svc_macro` 的非 `Pull` 端点仍从请求体解码参数**：`View` / `Head` 带路径参数的
   端点（例如 `#[rpc(view = "feeds/{id}")]`）会把路径参数当成 body 参数解码。
   Phase B 的同路径多方法测试因此改用两个手写端点；这是宏侧的独立缺口，建议下一轮先修
   参数来源（路径 / 头 / 体）的判定。
2. **G7 跨 router 自动继承**：见 §6 的边界说明。
3. **短路回复的收尾**：`Pipeline::run` 写 `Some(status)` 的无体回复后没有调用
   `finish_async`，与 Phase A–D 之前的行为一致；收尾仍由 servlet 负责。
4. `Pipeline::run_from_` 的递归层数 = 中间件数量；若未来中间件链很长，可考虑改成显式栈，
   但当前链长只有个位数。
5. `MptpContext::new_` 参数已达 8 个，用 `#[allow(clippy::too_many_arguments)]` 标注；
   若以后再加请求级输入，建议收成内部装配结构体。

## 8. 验收记录

- `cargo check --workspace --all-targets --offline`：通过；
- `cargo clippy --workspace --all-targets --offline -- -D warnings`：通过；
- `cargo test --workspace --offline`：通过；
- demo 集成测试：`scene_stream_` 17 项（不回归）+ `ctx_helpers_` 2 +
  `multi_method_` 2 + `request_state_` 2 + `onion_` 2 + `fallback_` 3 = 28 项。
