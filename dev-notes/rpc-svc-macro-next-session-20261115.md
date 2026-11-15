# 下一会话提示词：实施 `rpc_svc_macro` 的 Call 请求信封（共识形状）

- 日期：2026-11-15
- 目标仓库：`mptp_rpc`（主）、`mptp_im_demo`（验收）
- 状态：**已完成**（2026-11-15 实施；记录见
  [`rpc-svc-macro-call-20261115-1120.md`](rpc-svc-macro-call-20261115-1120.md)）。
  本文件保留为当轮的验收口径与工作拆分；§2 的文档地图仍适用。
- 基线：
  - `mptp_rpc` `9014bb1`（`TrRpcInterface::PREFIX`、`RpcMethodInfo` /
    `RpcServiceInfo`、`call_endpoint_typed_async`）
  - `mptp_im_demo` `edc0a31`（Call 请求信封的手写原型；14 场景全绿）
  - `denco` `66ac83b`

---

## 0. 开工前：先处理未提交变更

`mptp_im_demo` 当前工作区**不干净**（不是本轮提示词生成的）：

- `crates/im_protocol/src/routes.rs` 被删除；
- `crates/im_protocol/src/lib.rs` 删除了 `pub mod routes` 与 `routes` 的 re-export；
- `crates/im_protocol/src/services.rs` 删除了 `SessionSvc` / `RoomSvc` 的
  `impl TrRpcInterface`。

这看起来是「去掉 routes 模块 / 由宏生成 marker」的在途改动。**先 `git status` +
`git diff` 与用户确认**，不要覆盖；确认后再把它并入或单独提交。下面的基线以
`edc0a31` 为准；如果这些改动保留，`im_protocol::services` 里的 marker impl 需要由宏
（或新的手写位置）补回来。

---

## 1. 任务

把 `rpc_svc_macro` 从「只有 controller / 自由函数 / Pull」扩展到共识形状的 **Call
code-first**：

1. `#[rpc(service = "im/v1/room", api = RoomSvc)] trait RoomService { ... }`：
   生成 marker、`TrRpcInterface::PREFIX`、请求枚举、`__Req_*`、方法级
   `RpcDataTypeInfo` / `RpcServiceInfo`、`ClientProxy` 的 trait impl；
2. `#[rpc(impl_of(RoomSvc))] impl RoomService for RoomServiceImpl { ... }`：
   生成一个 service dispatcher（方法表 + 两阶段/先解枚举再校验）、一个
   `CALL /<prefix>` endpoint、一个 `install_into`；
3. 把 `mptp_im_demo` 的 `im_protocol/src/services.rs`、`im_server/src/services.rs`
   中**宏能生成的部分**换成宏展开，保留业务 impl；
4. 保持 14 个真实 TCP + smux 场景、workspace 测试、clippy、import check 全绿。

**不做**（除非用户另行确认）：

- View 的 bodyless/header 迁移；
- `rpc_serv_ext` 重写；
- Push 的 typed 化、trait 多 namespace/`apis`；
- GUI、`mptp_cs_smoke` 迁移。

---

## 2. 文档地图：哪些是现行设计，哪些是旧形状

### 2.1 `mptp_rpc/dev-notes/rpc-code-first-trait-20261113-1230.md`

这是主设计文档，但**不是整篇都有效**。

| 部分 | 状态 | 说明 |
| --- | --- | --- |
| 开头「决策记录」#1 | **部分失效** | #1「方法 = endpoint」只对**非 Call** 仍有效；Call 已改为 service 路径 + 方法表（#9）。 |
| #2 `associated_type_defaults` | **有效** | 请求 payload 关联类型仍需要。 |
| #3 `rpc_serv_ext` 独立 crate | **有效（但 builder 的 interface/namespace 形状旧）** | 只做装配糖；不要按旧 `interface::<I>()` 实现。 |
| #4 `RpcEndpointInfo` | **失效** | Call 改用 `RpcServiceInfo` + `RpcMethodInfo`；旧的单 operation 类型不再用于 Call。 |
| #5 `impl_of` 命名 | **有效** | |
| #6 no `path`；`namespace/service/operation/pattern` | **部分失效** | namespace 已取消；术语改为 `SERVICE_PREFIX` / `OPERATION` / `PATTERN` / `MOUNT`。 |
| #7 denco `()` | **有效** | |
| #8 per-service marker + 取消 namespace | **有效** | `#[rpc(service = "...", api = X)]`、`client.call::<X>()`。 |
| #9 Call service 路径 + 请求枚举 + 方法表 | **有效** | 与 §12 一起看。 |
| §0–§2（背景 / 历史设计） | **旧** | ASP.NET Core / gRPC 调研可参考；具体 `ApiV1` / body enum / namespace 形状不要照抄。 |
| §3 落点与设计选择 | **现行** | 已按共识重写；3.1–3.10 都以这里为准。 |
| §4 样式代码 | **现行参考** | 4.1–4.7 是目标/参考形状；宏展开部分只保留语义，不要求精确 token。 |
| §5 实施计划 P0–P5 | **旧** | 仍用 namespace/interface marker、per-method Call path、`constcat`、旧 `RpcEndpointInfo`。只可当作工作拆分骨架，逐条按 §3/§4/§12 改写。 |
| §6 公开 API / 风险 | **部分旧** | 6.1 的 8/9 两条有效；其余 constcat / namespace / `NAMESPACE` / interface marker 风险是旧形状。 |
| §7 决策闭环 | **旧** | 不要按它实施。 |
| §8 验证探针 | **旧** | 以 §12.10 / `mptp_im_demo/ACCEPTANCE.md` §6.6 为准。 |
| §9 参考链接 | **有效** | |
| §10 英文摘要 | **旧** | per-method endpoint / namespace 的摘要，不要当现行设计。 |
| §11 per-service marker / 取消 namespace | **现行** | 11.6 的验收表已移到 §12.10。 |
| §12 Call 请求信封 | **现行** | §12.1–§12.11 + `CC-01..CC-20` 是本次宏改造的权威口径。 |

### 2.2 `mptp_im_demo` 文档

| 文档 / 部分 | 状态 | 说明 |
| --- | --- | --- |
| `dev-notes/endpoint-design-20261114.md` | **现行** | §1 三层模型、§2 Call 形状（§2.1）、§3–§7 非 Call 形状、§9/§10 helper/handler 表。 |
| `dev-notes/call-envelope-20261115.md` | **现行实施记录** | 手写原型的 wire 形状、dispatcher 流程、golden、验证命令；宏输出应与它行为一致。 |
| `DESIGN.md` 顶部全局修订注 | **现行** | Call = `CALL /<prefix>` + 请求枚举；namespace 取消。 |
| `DESIGN.md` §4.2 / §4.5 / §4.9 / §4.10 | **现行（§4.5 表格是历史列）** | §4.5 的 `Pattern` 列要读作「service 路径 + 方法表项」；§4.9 的 `RpcEndpointInfo` 已改为 `RpcServiceInfo`/`RpcMethodInfo`。 |
| `DESIGN.md` §4.6 / §4.8 | **旧** | controller 每方法 path 的写法；非 Call 仍可参考，Call 不要照抄。 |
| `ACCEPTANCE.md` §6.6 | **现行验收** | `CC-01..CC-20`；矩阵 1 仍是 `GAP`（宏未实现），不要因为 demo 手写原型通过就标 PASS。 |

### 2.3 代码基线（行为参考）

| 位置 | 作用 |
| --- | --- |
| `mptp_im_demo/crates/im_protocol/src/services.rs` | 手写原型：trait / marker / 请求枚举 / `__Req_*` / info / 方法表 / 客户端 impl。宏输出应生成这些。 |
| `mptp_im_demo/crates/im_server/src/services.rs` | 手写原型：service impl + dispatcher + installer。宏输出应生成 dispatcher/installer；业务 impl 保留。 |
| `mptp_im_demo/crates/im_server/src/ready.rs` | `ReadyAsync`；宏生成的 impl_of 可以复用或内联。 |
| `rpc_core_v1::serving::{RpcMethodInfo, RpcServiceInfo}` | 方法表 / service 元信息；宏应使用它们。 |
| `rpc_core_v1::client::call_endpoint_typed_async` | 客户端 typed unary；宏生成的代理方法应使用它。 |
| `mptp_serving::{Endpoint, MethodSet, TrEndpointHandler}` | service endpoint + dispatcher 的装配面；不需要改库。 |

---

## 3. 目标形状（必须与 §3/§4/§12 一致）

### 3.1 trait 宏

```rust
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService {
    fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>>;

    fn create_room(
        &self,
        name: String,
        topic: Option<String>,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSummary>>;

    fn join_room(
        &self,
        room_id: RoomId,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<RoomSnapshot>>;

    fn send_message(
        &self,
        room_id: RoomId,
        text: &str,
        client_msg_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>>;
}
```

生成：

- `pub struct RoomSvc;`（可见性跟随 trait）；
- `impl TrRpcInterface for RoomSvc { const PREFIX: &'static str = "im/v1/room"; }`；
- `RoomServiceCall` 枚举：0 参 unit、1 参 newtype、≥2 参 newtype 包
  `__Req_<operation>`；变体线上名 = 方法名（`#[denco(rename = "...")]`）；
  枚举 derive `TrEncodable/TrDecodable`，**不** derive `TrHasTypeId`；
- `__Req_*` 结构体：位置字段 `f0..`，derive `TrEncodable/TrDecodable/TrHasTypeId`；
- 每方法 `RpcDataTypeInfo`（request = payload ID，response = 返回类型，accept =
  response，`TypePolicy::Strict`）；
- `RpcMethodInfo` 方法表 + `RpcServiceInfo`；
- `impl<'a, C> RoomService for ClientProxy<'a, C, RoomSvc>`，每方法构造枚举变体并调用
  `call_endpoint_typed_async`；
- 不生成 `SERVICE_NAME`；不生成 namespace；不生成 per-method Call path。

### 3.2 impl_of 宏

```rust
#[rpc(impl_of(RoomSvc))]
impl RoomService for RoomServiceImpl { ... }
```

生成：

- 一个 `Endpoint::new(RoomSvc::PREFIX, MethodSet::CALL, dispatcher)`
  `.with_metadata(RoomServiceInfo)`；
- dispatcher：
  1. 解请求枚举（当前 demo 是 `decode_body_async::<RoomServiceCall>`；
     理想是「先读变体名 → 选 `RpcMethodInfo` → `validate_request_types` → 再解 payload」；
     若 denco 的枚举解码无法安全两阶段，先按 demo 的「整枚举解码 → 校验 → 分派」实现，
     并在文档里记录这个偏差）；
  2. 未知变体 / 畸形 body → 400；
  3. 方法级 `validate_request_types(Call, headers, info)` → 400/406，在 impl 前；
  4. 调 impl，`.may_cancel_with(ctx.cancel().child_token())`；
  5. 成功 → `write_typed_value_async(..., Call, info, &value)`；错误 →
     `RpcError::into_reply_parts()` + `write_status_async`；
- 一个 `install_into(router, Arc<Impl>)`；
- 所有方法共享同一个 `Arc<Impl>`；
- 不生成 per-method endpoint。

### 3.3 多前缀

- 单前缀：`service = "im/v1/room", api = RoomSvc`；
- 同一 trait 多前缀：后续 `apis = [(DiagV1, "im/v1/diag"), (DiagV2, "im/v2/diag")]`；
  本期可只做单前缀，`apis` 作为后续；但**不要**恢复 `namespace` / interface marker。

---

## 4. `rpc_svc_macro` 现有代码与改造点

当前 `rpc_svc_macro`：

- `src/lib.rs`：`#[zyn::attribute] rpc` 只分派 `syn::Item::Impl`（controller）与
  `syn::Item::Fn`（自由函数）；
- `src/analyze_.rs`：`parse_rpc_args_` 只有 `interface` / method-path；
- `src/expand_.rs`：controller / free fn / Pull 展开；
- `tests/controller_macro.rs`：现有 controller 测试。

需要新增：

1. `analyze_`：
   - `parse_rpc_args_` 增加 `Service { prefix: String, api: syn::Ident }`（trait）与
     `ImplOf { api: syn::Path }`（impl）；
   - `analyze_service_`：非泛型 trait、`&self`、无默认实现、返回
     `impl TrMayCancel<MayCancelOutput = RpcResult<T>>`、参数 owned/借用白名单、
     0/1/多参、`cfg` 继承、保留 `__Req_` 前缀；
   - `analyze_impl_of_`：trait impl、非泛型、无 where、方法列表；校验与 trait 一致；
2. `expand_`：
   - `expand_service_`：trait 原文 + marker + `PREFIX` + `__Req_*` 关联类型 / hidden
     结构体 + 请求枚举 + `RpcMethodInfo`/`RpcServiceInfo` + client impl；
   - `expand_impl_of_`：service dispatcher + endpoint + `install_into`；
3. `lib.rs`：增加 `syn::Item::Trait` 与 `syn::Item::Impl`（带 `impl_of`）分派；
4. 测试：
   - 宏展开形状单测；
   - `mptp_im_demo` 的迁移用例；
   - compile-fail：默认实现 / 非 `&self` / 泛型方法 / 不支持借用 / 名字冲突。

实现时优先把 demo 的手写原型作为**行为黄金**：宏展开的结果应能替换
`im_protocol/src/services.rs` 与 `im_server/src/services.rs` 的生成部分。

---

## 5. 验收标准

以 `CC-01..CC-20` 为准（`rpc-code-first-trait` §12.10 与
`mptp_im_demo/ACCEPTANCE.md` §6.6）。关键点：

- `CC-01` 语法 / marker / `PREFIX`；
- `CC-03` `client.call::<RoomSvc>()`；
- `CC-05` Call 只有 `CALL /<prefix>`，没有 per-method path；
- `CC-06` 枚举变体 / payload 形状；
- `CC-07` 方法级类型头校验在 impl 前（400/406）；
- `CC-09` dispatcher + 方法表 + 响应头；
- `CC-10` `Data_Type_Id` = 方法 payload，新增方法不改变已有方法 ID；
- `CC-11` `RpcServiceInfo` / `RpcMethodInfo`；
- `CC-14` 客户端响应校验；
- `CC-16/17` compile-fail；
- `CC-18` 与非 Call 端点混挂；
- `CC-20` 多前缀（可后置）。

另外必须保持：

- `mptp_im_demo` 14 个 `im_testkit` 场景全绿；
- `cargo test --workspace --exclude im_gui`（demo）；
- `cargo test --workspace`（mptp_rpc）；
- 两侧 `cargo clippy --workspace --all-targets -- -D warnings`；
- `python3 ../format_use_imports.py --check ...`；
- 测试函数中英双语文档注释（AGENTS §2）；
- 单测 ≤ 60s、全量 ≤ 5min 超时（AGENTS §12）。

---

## 6. 已知坑 / 决策点

1. **两阶段校验**：文档要求「读变体名 → 校验 → 解 payload」；demo 目前是「整枚举
   解码 → 校验 → 业务分派」。先确认 denco 的 `MpDecoder` 能否安全地先读 variant name
   再读 payload（注意 `pending_` 预读字节）；不能则按 demo 实现并在 note 里记录偏差。
2. **`__Req_*` 可见性 / 命名**：`#[doc(hidden)]` + `#[allow(non_camel_case_types)]`；
   它们要被 `impl_of` 跨 crate 命名，需要 `pub`。
3. **`TrRpcInterface::PREFIX`**：已完成重命名；不要再生成 `PATH` / `NAMESPACE`。
4. **`SERVICE_NAME`**：不再生成；不要保留旧 trait 的 `SERVICE_NAME` / `SERVICES`。
5. **`RpcEndpointInfo`**：旧类型仍存在；Call 用 `RpcServiceInfo`/`RpcMethodInfo`。
   不要在 Call 上挂旧的单 operation 类型。
6. **`constcat`**：Call pattern 是 `service` 字面量，不需要 `constcat`；不要为 Call
   加这个依赖。非 Call controller 仍用自己的路径。
7. **多前缀**：不要恢复 namespace/interface marker；`apis` 是后续语法。
8. **View**：本期不动；它仍用 `RoomListQuery` body。
9. **demo 的 `routes.rs` 在途删除**：先与用户确认；宏生成的 marker 可能取代 routes。
10. **`rpc_serv_ext`**：旧 builder 按 interface/namespace；本期不改，demo 直接调
    installer。
11. **类型 ID**：`__Req_*` 的 golden 已更新为
    `0x0b47_44a2_b193_896f` / `0x0e67_1ff1_e8d7_b8b3`；宏生成的类型名必须与手写原型
    一致（`__Req_create_room` / `__Req_send_message`），否则 golden 会再变。
12. **公开 API 变更**：新增宏语法/生成类型、`RpcServiceInfo`/`RpcMethodInfo` 已是
    新增 API；按 `AGENTS.md` 第 1 条在提案/note 中记录并确认。

---

## 7. 推荐第一步

1. `git -C mptp_rpc status --short` / `git -C mptp_im_demo status --short`；
   与用户确认 demo 的 routes/marker 在途改动；
2. 读 `rpc-code-first-trait` §3、§4、§11、§12 与 `call-envelope-20261115.md`；
3. 先写 `rpc_svc_macro` 的分析层单测（trait/impl_of 合法与非法形状），再写展开；
4. 先让 `im_protocol` 用宏生成（与手写原型逐字对照），再让 `im_server` 用
   `impl_of` 替换 dispatcher/installer；
5. 跑 14 场景；最后删手写 glue，保留业务 impl；
6. 分提交：`rpc_svc_macro` 分析/展开、`rpc_core_v1` 辅助、demo 迁移、文档。

---

## 8. 一句话

**按 `rpc-code-first-trait` §3/§4/§11/§12 的现行形状实现 `rpc_svc_macro` 的 trait /
`impl_of`；§5–§10 与 `DESIGN.md` §4.6/§4.8 是旧形状，只作历史参考；demo 的
`services.rs` 是行为黄金，宏落地后替换其生成部分，14 场景必须保持全绿。**
