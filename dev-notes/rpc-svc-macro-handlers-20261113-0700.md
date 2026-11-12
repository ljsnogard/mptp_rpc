# rpc_svc_macro handler 形态扩展（round 32）与闭包 handler 的可行性结论

- 日期：2026-11-13 07:00
- 状态：**实施记录 + 调研结论**；①② 已落地，③ 给出「不可行 + 替代方案」的结论，待决策
- 分支：`feat/round-32-macro-handlers`（`--no-ff` 合回 `feat/serv-framework`，
  `merge round-32-macro-handlers`）

## 0. 结论

| 项 | 结果 |
| --- | --- |
| ① 端点方法尾参 `TrCancellationToken` | ✅ 已落地（gen_mcf2 形状） |
| ② 自由函数 `#[rpc(method="path")] async fn` | ✅ 已落地 |
| ③ 闭包 `FnHandler` + `bind_handler!(router, "path", \|ctx\| async move {...})` | ❌ 受 Rust 生命周期约束，**无法实现**；已 spike 实证 |

## 1. 已落地（round 32）

### 1.1 尾参取消令牌（unary 端点）

```rust
#[rpc(call = "join")]
async fn join<K>(&self, req: JoinRequest, cancel: K) -> Result<PlanetDescriptor, SceneError>
where
    K: abs_cancel::TrCancellationToken,
{
    let _ = cancel.child_token();
    // ...
}
```

- `K` 必须是**唯一的泛型类型参数**、`cancel` 必须是**最后一个参数**（这正是
  `gen_may_cancel_future` 要求的形状：最后泛型参数是 token、最后函数参数按值接收）。
- 宏复用该令牌，不再注入 `__C`；handler 仍用
  `{Prefix}Async::new(&svc, req).may_cancel_with(ctx.cancel().child_token())` 驱动，
  `cancel` 即请求级子 token。
- 不声明令牌时行为与原来一致（自动注入 `__C`）。

### 1.2 自由函数 handler

```rust
#[rpc(call = "scene/ping")]
async fn ping(req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }
```

- 无 `self`、路径取 `call = "..."` 的完整值（无 interface 前缀）、不生成客户端代理。
- 生成无状态单元 handler（`{Method}Handler`）与 `install_into(router)`（无 `Arc<Self>`）。
- 可与尾参令牌叠加：`async fn f<K>(req, cancel: K) where K: TrCancellationToken`。

### 1.3 实现要点

展开层重构出 `HandlerNames_`（step 名 / `gen_may_cancel_future` 前缀 / handler 结构体名 /
`{Prefix}Async` 名）与 `self_ty: Option<&Type>`，controller 与自由函数共用
`endpoint_step_fn_` / `handler_struct_` / `handler_impl_` / `pull_handler_impl_` 的生成；
`method_params_` 剔除尾参令牌，body / 路径解码不再把它当作业务参数。

## 2. 闭包 handler 的可行性（结论：不可行）

目标形状是 `bind_handler!(router, "path", |ctx| async move { ... })`，即让闭包拿到
`&mut MptpContext` 并返回一个借用它的 future。三种 `FnHandler<F>` 写法都已实际编译验证，
**全部失败**：

| 写法 | 编译器错误 | 根因 |
| --- | --- | --- |
| `F: Fn(&mut Ctx<'_>) -> Fut, Fut: Future` | `E0309: Fut may not live long enough`（要求 `Fut: 'a`） | 闭包返回的 future 借用 `ctx`，但 `Fn::Output` 是固定类型，无法对借用生命周期 `'a` 高阶化 |
| `F: Fn(&mut Ctx<'_>) -> HandlerFuture<'_>` | `E0106: missing lifetime specifier` | 高阶 return type 无法书写 |
| `F: for<'a> Fn(&'a mut Ctx<'a>) -> HandlerFuture<'a>` | `'a must outlive 'f` | 闭包把「借用」和「内部 `'f`」绑成一个生命周期，而 `handle<'a,'f>` 是**两个独立**生命周期 |

根因：`TrEndpointHandler::handle<'a, 'f>(&'a self, ctx: &'a mut MptpContext<'f, ...>)`
为了支撑洋葱中间件（round-26 的 arena future）把「对 ctx 的借用 `'a`」与「ctx 内部会话
借用 `'f`」拆成两个独立生命周期；`&mut` 对 `'f` 不变，因此管线交给终端 handler 的只能是
`&'a mut Ctx<'f>`（`'a ≤ 'f`，且 `'a` 无法延长到 `'f`）。任何「闭包返回借用 ctx 的
future」都必须输出一个依赖 `'a` 的类型，而 Rust 的 `Fn` trait 不允许高阶化的 `Output`。
这是语言层面无解，不是实现技巧问题。

### 可行的替代

1. **自由函数 + controller 方法**（推荐）：自由函数覆盖「无捕获、不写 struct」；有状态
   handler 用 controller 方法（状态放 `Arc<Self>`）。
2. **owned 请求 handler**：让 handler 接收按值拥有的请求数据（而非 `&mut MptpContext`），
   future 不再借用 ctx，`Fn` 即成立（axum 的 owned extractor 路线）。属较大公开 API 重
   设计，需单独决策。

## 3. 遗留决策

③ 的走向待定：接受 1（收口）、上 2（owned 重设计）、或其它折中。

## 4. 验收

- `cargo check --workspace --all-targets --offline`：通过；
- `cargo clippy --workspace --all-targets --offline -- -D warnings`：通过；
- `cargo test --workspace --offline`：通过（新增 4 项行为测试 + 1 项编译期断言；
  demo 不回归）。
