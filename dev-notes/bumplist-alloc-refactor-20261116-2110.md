# bumplist_alloc 重构：头值、allocator 泛型与有生命周期的会话分配

- 状态：**进行中**（2026-11-16 21:10 记录第 1 步；后续步骤完成后补充）
- 分支：`feat/bumplist_alloc`（每个步骤单独开分支，完成后 `--no-ff` 合回）
- 正式依赖：`bumplist_alloc = { git = "https://gitee.com/lino_snsalias/bumplist_alloc.rs.git" }`；
  本地 `../bumplist-alloc` 只用于开发与验证。

## 0. 已确认的决策

1. **头值从 `TrNumOrStr` 改为 `TrNumOrObj`**。键仍然是 `TrNumOrStr`；值只有两种形态：
   - `Num`：简单标准头（`Body_Size`、`Body_Transfer`……）；
   - `Obj`：复杂值，承载 MessagePack 编码的字节流，并带逻辑类型 `type_id`。

   `Obj` 的线上表示是 **`mp_denco` 的两元素数组 `[type_id(u32), bin]`**；`type_id` 取
   `abs_denco` 的 `TrHasTypeId` 生成的 `u32`。`abs_mptp` 只描述这个形状，不依赖 denco，
   也不解析 MessagePack。
2. **allocator 参数统一用 `A: Allocator` 泛型，不给 `Global` 默认值**。凡是 `String` /
   `Vec` / 头表等容器都由 `A` 分配；调用方显式选择 allocator。`mptp_wire` 先用
   denco 的 `TrAllocSource` / `TrArena` 把 decoder 的 arena 投影成容器 allocator。
3. **错误类型里的诊断 `String` 不进入本次 allocator 泛型**：错误是冷路径，继续用
   `alloc::string::String`；协议 / 会话 / 请求数据才必须走 allocator。
4. **`mptp_smux` / `mptp_serving` 的堆分配由 session / client / server 持有的
   带生命周期 allocator 提供**，不再使用 `bumpalo::Bump`。
5. **允许以 feature 开关修改 `bumplist-alloc`**，增加 `TrArena` / `TrArenaBytes` 适配，
   让 `MpDecoder::new_in` 可以直接把字符串解到 `abs_str::String<&BumpListAlloc>`。
   修改先在本地仓库提交；在推送到 gitee 之前，`mptp_rpc` 用 `[patch]` 指向本地路径。

## 1. 步骤划分

| 步骤 | 分支 | 目标 | 合并门槛 |
| --- | --- | --- | --- |
| 1 | `step/abs-mptp-trnumorobj` | `abs_mptp` 头值改为 `TrNumOrObj`，修编译错误 | `cargo test -p abs_mptp` 通过 |
| 2 | `step/mptp-wire-allocator` | `mptp_wire` 全量 `A: Allocator` 泛型 + 头值线格式 | `cargo test -p mptp_wire` 通过 |
| 3 | `step/mptp-smux-allocator` | `mptp_smux` 用会话 allocator 替换 `bumpalo::Bump` | `cargo check -p mptp_smux` 通过 |
| 4 | `step/mptp-serving-allocator` | `mptp_serving` 请求级 arena 改用会话 allocator | `cargo check -p mptp_serving` 通过 |

下游 crate（`rpc_core_v1`、`abs_mptp-tests`、demo）在中间步骤可能暂时编译不过；
每个合并提交只保证**所在 crate** 可编译，这是本次重构明确接受的中间状态。

## 2. 第 1 步：`abs_mptp` 的 `TrNumOrObj`

- `TrHeaders::Val` 约束改为
  `TrNumOrObj<NumType: Into<StdHeaderVal>>`（原 WIP 误写成 `Into<StdHeaderKey>`）。
- 新增 `ObjEnvelope::new` / `as_ref` / `into_parts` / `type_id` / `data`，并逐项导出
  `ObjEnvelope` 与 `TrTypedEnvelope`。
- `TrMsgPrefix::body_info` 的默认实现只看数字形态：
  - `Body_Size` 是 `Obj` 时按「没有长度」处理；
  - `Body_Transfer` 只认数字 `0x20`（`Body_Transfer_Chunked`），`Obj` 不算分块。
- 文本长度 / 文本 `"chunked"` 的兼容路径被移除；这属于值形态变更的直接结果。
- `TrHeaders::Val` 不再要求 `Clone`：`body_info` 只借 `as_num_or_obj()`，不复制值。
