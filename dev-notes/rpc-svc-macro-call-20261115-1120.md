# 2026-11-15 实施记录：Call service trait / `impl_of` 宏落地

- 日期：2026-11-15
- 状态：**已完成**（`rpc_svc_macro` 的 code-first Call 形状 + `mptp_im_demo` 迁移）
- 基线：`mptp_rpc` `4b601f9`（交接提示词）/ `9014bb1`（`PREFIX`、`RpcServiceInfo`、
  `call_endpoint_typed_async`）；`mptp_im_demo` `edc0a31`（手写原型；工作区有在途的
  routes/marker 删除，已按用户确认保留并补完）；`denco` `66ac83b`
- 关联：
  [`rpc-svc-macro-next-session-20261115.md`](rpc-svc-macro-next-session-20261115.md)（本轮提示词）、
  [`rpc-code-first-trait-20261113-1230.md`](rpc-code-first-trait-20261113-1230.md) §3 / §4 / §11 / §12、
  [`../../mptp_im_demo/dev-notes/call-envelope-20261115.md`](../../mptp_im_demo/dev-notes/call-envelope-20261115.md)、
  [`../../mptp_im_demo/ACCEPTANCE.md`](../../mptp_im_demo/ACCEPTANCE.md) §6.6

---

## 1. 结果摘要

| 项 | 状态 |
| --- | --- |
| `#[rpc(service = "...", api = X)] trait` | 已实现（marker / `PREFIX` / 隐藏关联类型 / 请求枚举 / 方法级类型头 / 方法表 / `ClientProxy` impl） |
| `#[rpc(impl_of(X))] impl Trait for Impl` | 已实现（方法表 / dispatcher / 一个 `CALL /<prefix>` endpoint / `install_into`） |
| `mptp_im_demo` 协议 crate | `services.rs` 只剩两个 trait；生成物由宏产出 |
| `mptp_im_demo` 服务端 crate | `services.rs` 只剩业务 impl；dispatcher / installer 由宏产出 |
| 14 个真实 TCP + smux 场景 | 全绿 |
| `CC-01..CC-19` | 达成（`CC-07` / `CC-08` 有记录在案的实现偏差，见 §5） |
| `CC-20`（同一 trait 多前缀） | 未实现（`apis` 语法按提示词后置） |

## 2. 宏语法与生成形状

### 2.1 trait 侧

```rust
#[rpc(service = "im/v1/room", api = RoomSvc)]
pub trait RoomService {
    fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>>;
    fn send_message(
        &self,
        room_id: RoomId,
        text: &str,
        client_msg_id: u64,
    ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>>;
}
```

宏在原 trait 上追加隐藏关联类型（带默认值）：

```rust
pub trait RoomService {
    #[doc(hidden)] #[allow(non_camel_case_types)]
    type __Req_list_my_rooms = ();
    #[doc(hidden)] #[allow(non_camel_case_types)]
    type __Req_send_message = self::__Req_send_message;
    #[doc(hidden)]
    type __Call = self::RoomServiceCall;
    // ...用户方法
}
```

同一模块里生成：

- `pub struct RoomSvc;`（可见性跟随 trait）+ `impl TrRpcInterface for RoomSvc { const PREFIX }`；
- 多参方法的 `pub struct __Req_<operation> { pub f0, pub f1, ... }`（`TrDecodable` /
  `TrEncodable` / `TrHasTypeId`）；
- `pub enum RoomServiceCall`（变体线上名 = 方法名，`#[denco(rename)]`；0 参 unit、
  1 参 newtype、≥2 参 `__Req_*` newtype；**不** derive `TrHasTypeId`）；
- 每方法 `pub const ROOM_SVC_<OPERATION>_INFO: RpcDataTypeInfo`、`ROOM_SVC_METHODS`、
  `ROOM_SVC_INFO`；
- `impl<'__rpc_a, __rpc_c> RoomService for ClientProxy<'__rpc_a, __rpc_c, RoomSvc>`，
  每方法构造枚举变体并调 `call_endpoint_typed_async`。

### 2.2 impl 侧

```rust
#[rpc(impl_of(RoomSvc))]
impl RoomService for RoomServiceImpl { /* 业务实现 */ }
```

生成隐藏模块 `__rpc_impl_of_RoomServiceImpl_via_RoomSvc`，内含：

- `const METHODS_` / `const SERVICE_INFO_` / 每方法 `METHOD_<OPERATION>_INFO_`
  （载荷类型通过 `<Impl as Trait>::__Req_<operation>` 跨 crate 命名）；
- `pub struct ServiceDispatcher { svc_: Arc<Impl> }` + `impl TrEndpointHandler`：
  解请求枚举 → 选方法级 `RpcDataTypeInfo` → `validate_request_types` → 调 impl
  （`.may_cancel_with(ctx.cancel().child_token())`）→ `write_typed_value_async` /
  `write_status_async`；
- `pub fn install_into(router, Arc<Impl>)`：绑定
  `Endpoint::new(<X as TrRpcInterface>::PREFIX, MethodSet::CALL, dispatcher)`
  `.with_metadata(SERVICE_INFO_)`。

## 3. 关键决策

1. **隐藏关联类型是跨 crate 命名的唯一稳定通道。**
   `__Req_*` 让 `impl_of` 在服务端 crate 命名「多参载荷 / 0 参 `()` / 1 参标量」而不必知道
   协议模块路径；`__Call` 让 dispatcher 能写 `<Impl as Trait>::__Call`。代价是协议 crate
   必须开 `#![feature(associated_type_defaults)]`（设计稿 §3.5 已接受）。
2. **`__Call` 是设计稿之外的补充。** 原设计只列 `__Req_*`，但 impl_of 还需要请求枚举类型；
   若按「trait 名 + `Call`」推导类型名，就必须要求调用方 `use` 该枚举，或把 marker 写成
   全路径（`impl_of(RoomSvc)` 是裸标识符语法）。加一个 `#[doc(hidden)] type __Call` 后，
   impl_of 完全自洽。
3. **`__Req_*` 结构体必须是 `pub`。** 私有类型不能出现在 `pub trait` 的关联类型默认值里
   （E0446），且 impl_of 需要跨 crate 使用；它们始终 `#[doc(hidden)]`。
4. **payload 结构体的类型 ID 不变。** denco 的指纹包含**模块路径**、类型名与位置字段名；
   迁移后仍是 `im_protocol::services::__Req_create_room { f0: String, f1: Option<String> }`，
   所以 `0x0b47_44a2_b193_896f` / `0x0e67_1ff1_e8d7_b8b3` 与手写原型完全一致
   （`im_protocol` 的 golden 测试未改动，实测通过）。
5. **trait 侧与 impl 侧各自生成方法表。** 服务端不复用协议 crate 的 `ROOM_SVC_METHODS`
   常量，避免跨 crate 常量名耦合；两边都由「同名 trait 方法 + 同一套载荷判定」推导，
   `RpcMethodInfo.operation` 是唯一共享键。
6. **生成常量命名**：`{MARKER}_{OPERATION}_INFO` / `{MARKER}_METHODS` / `{MARKER}_INFO`
   （marker 的大写蛇形），例如 `ROOM_SVC_CREATE_ROOM_INFO`、`ROOM_SVC_METHODS`、
   `ROOM_SVC_INFO`。impl_of 内部常量用尾部 `_`（`METHODS_` / `METHOD_*_INFO_`）。
7. **`impl_of(...)` 只接受简单标识符**：zyn 的 `Args` 解析器把 `impl_of(RoomSvc)` 解析成
   `Flag`，不支持 `impl_of(im_protocol::RoomSvc)`。错误信息要求先用 `use` 引入 marker；
   这与设计稿的示例一致。
8. **取消与错误**：dispatcher 只把方法 future 交给请求级子 token；业务错误走
   `RpcError::into_reply_parts()`，与手写原型一致。
9. **框架新增公开 helper**：
   `rpc_core_v1::serving::write_typed_value_async(session, status, method, info, value)`
   ——按方法级 `RpcDataTypeInfo` 写响应头 + ring/chunked 编码。它替代了 demo 里手写的
   `im_server/src/stream_io.rs`（该文件已删除，`push.rs` 改用框架 helper）。
   公开 API 变更按 `AGENTS.md` §1 记录在本节与 §6。
10. **`cfg` 继承**：trait 方法上的 `#[cfg]` 会复制到关联类型、枚举变体、`__Req_*`、
    方法表项、客户端方法与 dispatcher 分支；`cfg` 在数组元素位置是稳定的（实测），
    所以方法表可以逐项条件编译。
11. **`impl_of` 拒绝覆盖隐藏关联类型**：impl 块里手写 `type __Req_*` / `type __Call`
    会让服务端载荷与协议 crate 的请求枚举不一致；分析层直接报错。

## 4. demo 迁移

| 位置 | 改动 |
| --- | --- |
| `im_protocol/src/services.rs` | 由 335 行手写原型改为两个 `#[rpc(service = ...)]` trait（约 80 行） |
| `im_protocol/src/lib.rs` | 打开 `associated_type_defaults`；恢复 `pub mod routes`；re-export 换成宏生成名 |
| `im_protocol/src/routes.rs` | 保留非 Call 端点常量 / 路径构造；删除 6 个 per-method Call 路径常量 |
| `im_protocol/Cargo.toml` | 新增 `rpc_svc_macro` 依赖 |
| `im_server/src/services.rs` | 由 364 行手写 dispatcher 改为业务 impl + `#[rpc(impl_of(...))]`（约 140 行） |
| `im_server/src/server.rs` | 用 `__rpc_impl_of_<Impl>_via_<Marker>::install_into` 装配 |
| `im_server/src/stream_io.rs` | 删除（被 `rpc_core_v1::serving::write_typed_value_async` 取代） |
| `im_server/src/push.rs` | 改用框架 helper |
| `im_server/Cargo.toml` | 新增 `rpc_svc_macro` 依赖 |

`im_client` / `im_testkit` 的调用点没有改：`client.call::<RoomSvc>()` 与业务方法名保持
不变，这正是 code-first 的目标。

## 5. 记录在案的实现偏差

1. **整枚举解码 → 校验 → 分派（`CC-07` 的“payload 解码前校验”未严格达成）。**
   denco 的枚举解码在 `decode_enum` 内部一次完成「变体名 + 载荷」；要在两者之间插入
   类型头校验，需要自定义 `Visitor` 或新的两阶段解码 API。本轮按提示词允许的路径实现：
   `decode_body_async::<XxxServiceCall>` → 选 `RpcMethodInfo` → `validate_request_types`
   → 调 impl。**业务 handler 仍然在校验之后**（`CC-07` 的判定口径），但坏类型头会在
   payload 解码之后才回 400/406。后续增强：在 `rpc_core_v1::serving` 提供
   「读变体名后暂停」的解码入口，dispatcher 改成两阶段。
2. **未知变体回 400 而不是 404（`CC-08`）。** 未知变体在 denco 解码阶段就是错误，与
   「畸形 body」同路径。设计稿允许「或团队约定状态」；demo 的验收用 400。若要让未知
   变体与畸形 body 区分，同样需要两阶段解码。
3. **没有方法级中间件 hook。** `RpcMethodInfo` 已在 dispatcher 内可用，但框架没有暴露
   「读到变体名后、payload 解码前」的 hook；当前方法级策略只能内联在 dispatcher。
4. **`CC-16` / `CC-17` 用分析层单测覆盖。** 未引入 `trybuild`（离线环境 + 新增依赖）；
   非法形状（默认实现 / 非 `&self` / 泛型方法 / 白名单外借用 / 重复方法）由
   `analyze_` 的单测直接断言 `Err`。`CC-16`（错误 marker 上调别的方法）是 Rust 类型
   系统的自然结果：`ClientProxy<_, RoomSvc>` 没有为 `SessionService` 实现该 trait。
5. **`CC-05` / `CC-13` 用代码形状而非路由表断言证明**：`mptp_serving` 没有公开的
   「按路径查端点」API，无法在测试里枚举路由表。证据是：宏只在 `install_into` 里生成
   **一个** `Endpoint::new(PREFIX, MethodSet::CALL, ...)`（`CC-05`）；`MethodSet::CALL`
   是 service 级方法集，trait 里不存在非 Call 方法（`CC-13`）；`im_protocol/src/routes.rs`
   已删除所有 per-method Call 路径常量。
6. **`CC-14` 复用 `call_endpoint_typed_async` 的既有响应校验**：客户端代理方法都走该
   入口，响应 `Data_Type_Id` 与方法 `response_body` 不一致时返回 406 / 解码错误。该助手
   在 `rpc_core_v1` 侧有单测；本轮没有为 Call 响应错配新增场景（demo 的 406 场景覆盖
   Pull）。
7. **`CC-20` 多前缀未实现**：`apis = [(DiagV1, ...), ...]` 按提示词后置；单前缀路径已
   完全打通。

## 6. 公开 API 变更清单（按 AGENTS §1 记录）

- 宏语法：新增 `#[rpc(service = "...", api = X)]`（trait）与 `#[rpc(impl_of(X))]`
  （trait impl）；`interface` / 自由函数 / 方法级 `#[rpc(...)]` 不变。
- 生成的公开项：marker（`RoomSvc`）、`TrRpcInterface::PREFIX` 实现、`__Req_*`、
  `XxxServiceCall`、`{MARKER}_*_INFO`、`{MARKER}_METHODS`、`{MARKER}_INFO`、
  `__rpc_impl_of_<Impl>_via_<Marker>::{ServiceDispatcher, install_into}`。
- `rpc_core_v1::serving::write_typed_value_async`（并在 `preludes` 转出）。
- 协议 crate 的前置要求：`#![feature(associated_type_defaults)]`、`rpc_core_v1` 的
  `type-id` feature、直接依赖 `abs_denco`。

## 7. 验证

```bash
# mptp_rpc
cargo test  -p rpc_svc_macro --offline                    # 9 单测 + 9 集成 + doctest
cargo test  --workspace --offline                         # 全绿
cargo clippy --workspace --all-targets --offline -- -D warnings

# mptp_im_demo
cargo test  -p im_protocol --lib --offline                # 5 passed；类型 ID golden 未变
cargo test  -p im_testkit --test scenarios --offline      # 14 passed
cargo test  --workspace --exclude im_gui --offline        # 全绿
cargo clippy --workspace --exclude im_gui --all-targets --offline -- -D warnings

# 两侧
python3 ../format_use_imports.py --check ...              # 全部合规
```

## 8. 后续

- `apis` 多前缀（`DiagV1` / `DiagV2`）与对应 `impl_of`；
- 两阶段解码 + 未知变体 404 + 方法级中间件 hook；
- `rpc_serv_ext` 的装配糖按「前缀字符串分组」重写（本期未动）；
- View 的 bodyless/header 迁移（`im/v1/room/list` 仍是 `RoomListQuery` body）。

## 9. 2026-11-15 补记：controller 的 `Drop` / `Head` 与无体回复

多聊天室 IM 实施（`mptp_im_demo/dev-notes/multi-room-im-20261115-1140.md`）发现两处
code-first 之外、但同属宏 / 客户端的能力缺口，已修补：

1. **controller 的 `Drop` / `Head` 端点原来无法工作**：宏统一用 `write_ok_async` /
   `write_status_async` 写回复，而 MPTP 规定 `Head` / `Drop` 的回复不允许带体
   （`mptp_smux::ensure_body_allowed_`），实际表现为 500。宏现在对这两类端点只写状态
   （`response(status).no_body_async()`），错误也不写体；`rpc_svc_macro` 的 controller
   测试新增 `#[rpc(drop = "leave")]` 端点与代理编译检查。
2. **`rpc_core_v1::client::call_endpoint_no_body_async`（新增公开 API）**：
   `Drop` / `Head` 的成功回复没有 body，`call_endpoint_async` 的 `Rep` 解码会失败
   （`()` 的 msgpack nil 字节不存在）。新入口成功只读状态、读 0 字节收尾；非成功仍读
   错误体返回 `RpcError::Remote`。controller 宏代理对 `Drop` / `Head` 改走该入口。
3. **方法属性转发**：trait 方法上的 `#[allow(...)]` / 文档 / `cfg` 现在也出现在生成的
   `ClientProxy` impl 方法上（此前只带 `cfg`，`clippy::ptr_arg` 这类 lint 无法在生成端
   豁免）。

公开 API 变更：`call_endpoint_no_body_async`（新增）；controller 的 `Drop` / `Head`
端点生成的客户端代理改用无体入口（签名不变）。
