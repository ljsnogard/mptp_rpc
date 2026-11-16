# 新会话提示词：mptp_rpc 的 BumpListAlloc / allocator 重构续作

> **状态（2026-11-17）：本提示词已执行完毕。** 最终形状、迁移清单与验证结果见
> `bumplist-alloc-refactor-20261116-2110.md` §8；下面第 3 节的「未完成」列表只作历史
> 记录，已全部完成（`SessionAlloc` 句柄 + 每个所有者自持 arena，`C::Alloc` 已删除）。
>
> 生成时间：2026-11-16 22:43（Asia/Shanghai）
> 当前 `mptp_rpc` 分支：`feat/bumplist_alloc`，HEAD `4f239b5`
> 当前 `bumplist-alloc`：`main` 两个提交，HEAD `1b22f17`

## 0. 直接使用的提示词

把下面整段作为新会话的第一条消息：

---

你在 `/home/lino/projects/me.noli/old-cantare/mptp_rpc` 项目内继续 BumpListAlloc / allocator
重构。请先执行 `git status` 与 `git log --oneline -5`，确认工作区干净、当前分支是
`feat/bumplist_alloc`（HEAD 预期为 `4f239b5` 或其后继）。

### 原始目标（仍然有效）

1. `abs_mptp`：把 `TrHeaders` 的键值约定从 `TrNumOrStr` 改为 `TrNumOrObj`；简单标准头值
   用 `Num`，复杂值用 MessagePack 编码的 `Obj`，线上是两元素数组
   `[type_id(u32), bin]`，`type_id` 取 `abs_denco` 的 `TrHasTypeId`。先修编译错误。
2. `mptp_wire`：所有 `String` / `Vec` / 头表都用带生命周期的 `A: Allocator` 分配，类型依赖
   只假设 `A: Allocator`（无 `Global` 默认值）。
3. `mptp_smux` / `mptp_serving`：任何堆上分配都由 session / client / server 持有的
   带生命周期 allocator 提供；替换原来的 `bumpalo::Bump`。
4. `bumplist_alloc`：以 feature 开关增加 `TrArena` / `TrArenaBytes` 适配；若分配器本身
   不满足需求，先停下来讨论源码修改。
5. 每个步骤单独开分支，所在 crate 可编译后 `--no-ff` 合回 `feat/bumplist_alloc`；不改与
   任务无关的代码；公开 API 变更先讨论；测试保留中英双语文档注释；及时写 `dev-notes`。

### 已经完成（不要重做）

- `abs_mptp`：`TrNumOrObj` / `ObjEnvelope` / `TrTypedEnvelope` 已落地；`body_info` 只认
  数字形态，`Body_Transfer` 只认 `0x20`；文本长度兼容路径已移除。
- `mptp_wire`：
  - `Headers<A: AllocatorClone>`、`HeaderKey<A>`、`HeaderVal<A>`、
    `RequestPrefix<A: AllocatorClone>`、`ResponsePrefix<A: AllocatorClone>` 全部显式带
    `A`，无 `Global` 默认值。
  - **头表内部是 `Arc<BTreeMap<HeaderKey<A>, HeaderVal<A>, A>, A>`**；不要再用排序 `Vec`。
    `BTreeMap` 要求 `A: AllocatorClone`，因此头表/前缀的类型定义带这个 marker。
  - 文本头是 `Obj`：载荷为 MessagePack string，`type_id` 取 `str` 的
    `TrHasTypeId::TYPE_ID_32`；`from_text` / `as_text` / `as_u64` / `set_text` /
    `data_type_id` 仍可用。
  - 增加了 `read_*_in_async` 与 `read_*_with_async`；后者返回拥有 allocator 的前缀。
  - `Body_Size` 的 `Num` 是 `u16`；客户端大定长体自动转分块（`SizedBody` 用
    `LimitedReader` 限住源），服务端 `sized_async` 超限返回 `InvalidBodySize`。
- `mptp_smux`：
  - `client::config::TrConfig` / `serving::config::TrConfig` 新增 `type Alloc`；
    客户端、会话、servlet 各自持有一份。
  - `SessionAlloc` 是 `Arc<BumpListAlloc>` 的 clonable 句柄，实现 `Allocator`、
    `AllocatorClone`、`TrArena`、`TrArenaBytes`、`TrAllocSource`；替代 `bumpalo::Bump`。
  - 构造参数变为 `MptpClient::new(conn, alloc)`、`MptpServlet::new(handler, alloc)`、
    `MptpConnection::new(binding, handler, state, alloc)`；`ServingSession::allocator()`；
    `take_parts` 返回 `ServingParts<C>`。
- `mptp_serving`：
  - 请求级 arena 改为 `&C::Alloc`；新增 `ArenaFuture` 替代 `bumpalo::boxed::Box`；
    `MptpContext::arena()` 返回 `&C::Alloc`；请求级 `R` 用 `Box::new_in` 分配。
  - `rpc_core_v1` dev-dependency 暂时移除；对应的元数据测试用 `cfg(any())` 暂停；
    `examples/im_session.rs` 由 `im-session-example` feature 关闭。
- `bumplist-alloc`：
  - `denco_arena` feature：`TrArena` / `TrArenaBytes`；`BumpListBox<'alloc, T>` 在 Drop 时
    运行 `T` 的析构并调用 `Page::release_page_` 归还块。
  - `Page` 保存指向非泛型 `BumpListCore` 的 owner 指针，最低位兼作页锁；页变空时直接
    调用 `BumpListCore::cleanup_empty_page_`。
  - `BumpListAlloc<A>` 内联 `BumpListCore` 与 `A`；Core 里两个单态化函数用
    `offset_of!` + 整分配器自指针取出 `A`，**没有 Box**。Page 头回到 64 位 32 字节 /
    32 位 20 字节。
  - **地址稳定性**：创建第一个 Page 后 `BumpListAlloc` 不能移动；要移动先 `Box::new`。
  - 本地 `main` 已压成两个提交（`082d299` Initial + `1b22f17` squash）；
    `backup/pre-squash` 保留压缩前历史。Miri strict-provenance 已通过。

### 当前验证状态

- `abs_mptp`：3 单元 + 6 文档测试通过。
- `mptp_wire`：24 单元 + 11 文档测试通过。
- `mptp_smux`：29 单元 + 2（1 ignored）通过；client/server 单独 feature 编译通过。
- `mptp_serving`：14 单元测试通过。
- 四个 crate 的 `cargo clippy --all-targets` 均无警告；导入检查合规。
- `bumplist-alloc`：16 单元 + 7 集成 + 2 并发 + 3 arena 测试通过；Miri 通过。

### 未完成，下一步优先做

1. **迁移下游 crate**（当前 workspace 整体编译不过）：
   - `rpc_core_v1`：补 `Headers<A>` / `RequestPrefix<A>` / `TrConfig::Alloc`；文本头
     `set_text`、`Data_Type_Id`、`Body_Size`、所有 `RequestPrefix::new` /
     `ResponsePrefix::new` 构造点；这是当前第一个阻塞点。
   - `abs_mptp-tests`：`body_info` 的 `MockVal` 改成 `TrNumOrObj`。
   - `mptp_smux_v1_demo`、`mptp_im_demo`：跟随客户端/服务端构造与类型参数变化。
   - 迁移完成后恢复 `mptp_serving` 的 `rpc_core_v1` dev-dependency、元数据测试与
     `im_session` example。
2. **打通正式依赖**：把 `bumplist-alloc` 的 `main` 推送到
   `https://gitee.com/lino_snsalias/bumplist_alloc.rs.git`，然后删除
   `mptp_rpc/Cargo.toml` 底部的临时 `[patch."https://gitee.com/..."]`，重新解析依赖并跑
   workspace 测试。
3. **确认设计取舍**：`mptp_smux` / `mptp_serving` 现在用 `C::Alloc` 关联类型 +
   `SessionAlloc`，而不是给每个会话加 `A` 泛型/生命周期。原因是
   `abs_mptp::client::TrMptpClient::Session` 是不带生命周期的关联类型。若希望改成
   `Session<'f>` GAT 或 `MptpClient<C, A>`，需要单独立项讨论。
4. **不要回退 HeadMap 的 BTreeMap 决定**：之前为了绕开 `AllocatorClone` 曾擅自改成排序
   `Vec`，已被明确要求改回；`HeaderMap` 必须保持 allocator-aware `BTreeMap`。
5. 若要继续优化 `BumpListAlloc`：目前唯一硬约束是“第一个 Page 后不移动分配器”。如果
   要去掉它，需要让 Page owner 指向稳定地址（例如 `Box<Core>` 或由用户 `Box` 分配器），
   必须先讨论，不能擅自加 Box。
6. 重新评估 `Body_Size` 的 `u16` 上限：现在大定长体客户端转分块、服务端报错；如果需要
   更大的定长体，需要改 `TrNumOrObj::Num` 的宽度或协议约定，属于公开 API 变更。
7. 迁移完成后补跨 crate 集成测试，并更新 `dev-notes` 的最终状态。

### 工作方式

- 动手前先 `git status`；有未提交改动先提醒用户。
- 每一步开分支、`--no-ff` 合回；合并提交必须让所在 crate 编译通过。
- 不确定的公开 API/协议约定先问，不要用“先绕过”的实现替代原设计。
- 测试必须有中英双语文档注释；Rust 代码遵循 `AGENTS.md` 的命名、文档、clippy 要求。
- 关键决策记录在 `dev-notes/bumplist-alloc-refactor-20261116-2110.md`。

---

## 1. 相关路径

- `mptp_rpc/mptp_wire/src/headers_.rs`：`Headers<A: AllocatorClone>` + `BTreeMap`。
- `mptp_rpc/mptp_wire/src/prefix_.rs`：前缀 allocator 参数与 `A: AllocatorClone`。
- `mptp_rpc/mptp_wire/src/wire_.rs`：`_in_` / `_with_` 解码入口。
- `mptp_rpc/mptp_smux/src/alloc_.rs`：`SessionAlloc`。
- `mptp_rpc/mptp_smux/src/client/config.rs`、`serving/config.rs`：`type Alloc`。
- `mptp_rpc/mptp_serving/src/arena_.rs`：`ArenaFuture`。
- `bumplist-alloc/src/alloc_.rs`、`page_.rs`、`denco_arena_.rs`。
- `mptp_rpc/dev-notes/bumplist-alloc-refactor-20261116-2110.md`：完整决策记录。
