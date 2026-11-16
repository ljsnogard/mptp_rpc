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

## 3. 第 2 步：`mptp_wire` 的 allocator 泛型

### 3.1 类型形状

- `Headers<A: AllocatorClone>`、`RequestPrefix<A: AllocatorClone>`、
  `ResponsePrefix<A: AllocatorClone>`、`HeaderVal<A: Allocator>` 都显式带 `A`，没有
  `Global` 默认值；`HeaderKey<A>` 是 `U16OrStr<A>` 的别名。
- `location` 用 `abs_str::String<A>`；头键文本用 `abs_str::String<A>`；对象载荷用
  `Vec<u8, A>`。`HeaderVal` / `HeaderKey` 只要求 `A: Allocator`；头表与前缀因为内部
  使用 allocator-aware `BTreeMap`，类型定义要求 `A: AllocatorClone`。
- 头表内部保持 `Arc<BTreeMap<HeaderKey<A>, HeaderVal<A>, A>, A>`：`BTreeMap` 的
  allocator 方法要求 `A: AllocatorClone`。目标 allocator `SessionAlloc`、`Global` 与
  `&'a BumpListAlloc` 都满足这个 marker；`&BumpListAlloc` 由 core 的
  `impl<A: Allocator + ?Sized> AllocatorClone for &A` 提供。
- `Arc::make_mut` 保留 `TrHeadersMut` 的写时复制语义；`Headers::set` / `remove` 与
  `TrHeadersMut` 的实现同样带 `A: AllocatorClone`。

### 3.2 解码入口

- `read_request_prefix_async` / `read_response_prefix_async`：用 `NoArena` 解码，返回
  allocator 为 `Global` 的前缀；行为与旧入口一致（除头值形态外）。
- `read_request_prefix_in_async` / `read_response_prefix_in_async`：接收 `&'alloc Alloc`，
  用 `MpDecoder::new_in` 解码，返回 allocator 为 `&'alloc Alloc` 的前缀；要求
  `Alloc: TrArena<'alloc> + Allocator`。`mptp_smux` 的会话在后续步骤使用这一族。
- 返回前缀的生命周期收窄到 `rx` 的借用 `'f`（生成 future 的 outlives 约束需要），调用方
  只需保证 arena 活过这次读取即可。

### 3.3 头值线格式与语义

- 数字值：MessagePack 整数（`u16`）。
- 对象值：两元素数组 `[type_id(u32), bin]`；`bin` 是 MessagePack binary，类型为
  `abs_mptp::spec::ObjEnvelope<Vec<u8, A>>`。
- 文本值不再作为头值形态：`HeaderVal::from_text` / `as_text` / `as_u64` /
  `Headers::set_text` / `data_type_id` 被移除。`Body_Size` / `Body_Transfer` 等标准头
  一律写数字；`Data_Type_Id`、`Accept` 这类复杂值改由上层用 `ObjEnvelope` 承载。
  这属于 `TrNumOrObj` 协议变更的直接结果，不是 allocator 重构的附带选择。

### 3.4 验证

- `cargo test -p mptp_wire`：24 个单元测试 + 11 个文档测试通过。
- 新增 `request_prefix_decodes_into_supplied_arena_`：用测试 arena 走 `_in_` 解码，
  断言返回前缀的 allocator 是 `&TestArena`，字符串 / 头表都来自该 arena。
- `cargo clippy -p mptp_wire --all-targets` 无警告。

## 4. 第 3 步：`mptp_smux` 的会话分配器

### 4.1 为什么是 `C::Alloc` 关联类型（已被 §8 取代）

`abs_mptp::client::TrMptpClient::Session` 是一个**不带生命周期**的关联类型，因此 session
不能借用 client 的 `&'a BumpListAlloc`——否则必须把该 trait 改成 GAT。为了不改协议层 trait，
客户端 / 服务端配置各增加一个关联类型：

```rust
type Alloc: AllocatorClone + 'static + for<'a> TrArena<'a> + for<'a> TrAllocSource<'a, Self::Alloc>;
```

`MptpClient<C>` / `MptpServlet<C, H>` 持有一份 `C::Alloc`，每个 `MptpSession<C>` /
`ServingSession<C>` 克隆一份。会话类型因此仍然只有 `C` 一个参数，没有生命周期、没有
自引用。前缀 / 头表 / location 的 allocator 类型就是 `C::Alloc`。

### 4.2 `SessionAlloc`（最终形态见 §8）

本 crate 提供 `SessionAlloc`：`Arc<BumpListAlloc>` 的 clonable 句柄，实现
`Allocator`、`AllocatorClone`、`TrArena`、`TrArenaBytes` 与
`TrAllocSource<'a, SessionAlloc>`。所有克隆共享同一个 arena；`Arc` 保证分配块被释放前
底层分配器仍然存活。它替代了 `MptpSession` 里原先未使用的 `bumpalo::Bump`。

`bumplist_alloc` 以 `denco_arena` 特性引入；开发期间 workspace 根的 `[patch]` 指向本地
`../bumplist-alloc`，推送 gitee 后删除该 patch 即可。

### 4.3 `mptp_wire` 的两处配套

- `read_request_prefix_with_async` / `read_response_prefix_with_async`：解码时用
  `arena: &A`，返回**拥有** `A` 克隆的 `RequestPrefix<A>` / `ResponsePrefix<A>`。会话
  因此不需要把 arena 的借用带进类型。
- 文本头恢复为 `Obj`：载荷是 MessagePack string，`type_id` 取
  `abs_denco::type_id::TrHasTypeId for str` 的 `TYPE_ID_32`。
  `HeaderVal::from_text` / `as_text` / `as_u64` 与 `Headers::set_text` / `data_type_id`
  因此仍然可用；`Allow`、`Accept` 这类文本头不需要另造协议。
- `RequestPrefix` / `ResponsePrefix` 增加 `allocator()` 访问器；`Body_Size` 超过
  `u16::MAX` 的定长体自动改成分块声明。

### 4.4 公开 API 变更

- `client::config::TrConfig` / `serving::config::TrConfig` 新增 `type Alloc`。
- `MptpClient::new(conn, alloc)`；`MptpServlet::new(handler, alloc)`；
  `MptpConnection::new(binding, handler, state, alloc)`。
- `ServingSession::allocator()`；`take_parts` 返回 `ServingParts<C>`。
- `K_TMP_ALLOW_HEADER_KEY` 改为 `u16`（键本身与 allocator 无关，避免常量被钉死到某个
  allocator 类型）。
- `MptpSession` / `ServingSession` 的类型参数没有变化；下游只需为配置补 `type Alloc`。

### 4.5 验证

- `cargo test -p mptp_smux`：29 个单元测试 + 2 个文档测试 / 集成测试通过。
- `cargo check -p mptp_smux --no-default-features --features client` 与
  `--features server` 均通过（顺带修正 `take_rx_` 的 `server` cfg：客户端
  `into_resp_suffix_async` 也需要它）。
- `cargo clippy -p mptp_smux --all-targets` 无警告。

## 5. 第 4 步：`mptp_serving` 的请求级 arena

- `MptpContext::arena()` 从 `&'f bumpalo::Bump` 改为 `&'f C::Alloc`；请求级 `R` 也用
  `Box::new_in(R::default(), &alloc)` 分配。
- handler / middleware 的 future 改为 `ArenaFuture<'a, C::Alloc, O>`：
  - 当前 nightly 的 `Pin<Box<dyn Future, &A>>` 要求 `A: StaticAllocator`；`SessionAlloc`
    析构会释放 arena，不能虚假实现该 marker；
  - `ArenaFuture` 内部是普通 `Box<dyn Future, &'a A>`，轮询时把内层 future 固定在其堆地址
    上；盒子析构按同一个 allocator 归还。
- `box_future_` 改为泛型 `C`，返回 `ArenaFuture`；`HandlerFuture` / `MiddlewareFuture` /
  `NextFn` 的签名随之更新。
- `serve_handler_` 的 `location` 从 `Global` 改为会话 allocator；405 的 `Allow` 头同样用
  会话 allocator。
- `rpc_core_v1` 作为 dev-dependency 暂时移除：它的 `Headers` / `RequestPrefix` 还没有
  allocator 参数，编译不过。对应的元数据测试用 `cfg(any())` 暂停，`examples/im_session.rs`
  用 `im-session-example` feature 关闭；下游迁移完成后恢复。
- 验证：`cargo test -p mptp_serving`（14 passed）与
  `cargo clippy -p mptp_serving --all-targets` 通过。

## 6. 大体的两个边界处理

`TrNumOrObj` 的 `Num` 对标准头是 `u16`，因此定长体的 `Body_Size` 不能超过 `u16::MAX`：

- 客户端：`body_bytes` / `body_sized` 在超过上限时改写为分块声明；`SizedBody` 内部用
  `LimitedReader::new(src, size)` 包住源，分块搬运不会读到声明长度之后。
- 服务端：`sized_async` 超过上限时返回
  `ProtocolError::InvalidBodySize`，提示改用分块体，而不是静默写成 `u16::MAX`。

## 7. 修正：`TrArena::Ref<T>` 必须是会 Drop 的 Box

初版 `denco_arena` 把 `Ref<T>` 定义成 `&'alloc mut T`，这只会借出一块内存，**不会**
运行 `T` 的析构函数。按与 bumpalo `BumpBox` 相同的语义，改为 `bumplist_alloc::BumpListBox`：

- 只拥有 `NonNull<T>` 与 `PhantomData<&'alloc T>`；不带 `A` 参数，也不引用
  `BumpListAlloc`；
- `Deref` / `DerefMut` / `TrBoxed` / `TrUnique`；
- `Drop` 先 `drop_in_place`，再从 `BlockHeader` 的 cell 下标反推 `Page`，调用
  `Page::release_page_` 把块归还到页内；零尺寸 `T` 没有 `BlockHeader`，只运行析构；
- `BumpListAlloc<A>` 内联非泛型 `BumpListCore` 与 `A`：Core 持有页链表、页大小、链表
  锁以及建页/还页两个单态化函数指针。Core 保存一个整分配器自指针，函数用 `offset_of!`
  从中取出 `A`；Page 只持有 Core 指针，不再每个 Page 保存函数指针，也不带 `A` 泛型。
  没有 `Box`；Core 的整分配器自指针在每次分配 / 归还 / 析构前刷新。代价是创建第一个 Page 后分配器不能移动；
- `Page::release_page_` 在 `live_ == 0` 时会释放页锁、直接调用
  `BumpListCore::cleanup_empty_page_`，因此**空页的摘链 / 交还底层分配器也能从
  `BumpListBox` 的析构路径完成**，不需要 Box 持有分配器引用；
- owner 指针最低位兼作页锁：Core 至少按指针宽度对齐，最低位为 0，省掉 Page 的独立
  锁字段；Page 头回到 64 位 32 字节 / 32 位 20 字节；
- `SessionAlloc` 的 `TrArena::Ref` 也同步为 `BumpListBox<'alloc, T>`。

这样 arena 里的值既不会泄漏 `Drop`，也不会把块永久占住；新增
`arena_value_runs_drop_` 测试验证析构函数恰好运行一次。

## 8. 最终形状：每个所有者自持 arena 句柄（2026-11-17）

> 本节取代 §4.1 / §4.2 里「用 `C::Alloc` 关联类型 + SessionAlloc 句柄」的中间方案。
> 目标形状由项目负责人在 2026-11-17 明确：`MptpClient` / `MptpContext` / `MptpSession`
> 自身就是内存分配源，前缀与头表统一使用 arena 句柄，不再为每种细化类型配置 allocator。

### 8.1 决策

1. **唯一的 arena 类型是 `SessionAlloc`**：`Arc<BumpListAlloc<Global>>` 的句柄，实现
   `Allocator`、`AllocatorClone`、`TrArena`、`TrArenaBytes` 与 `TrAllocSource`。
   `RequestPrefix` / `ResponsePrefix` / `Headers` / `HeaderKey` / `HeaderVal` 的 `A`
   一律取它。
2. **删除 `client::config::TrConfig::Alloc` 与 `serving::config::TrConfig::Alloc`**。
   两份配置现在把前缀 / 头表类型直接钉成 `...<SessionAlloc>`；下游配置不再回答
   allocator 类型。
3. **每个所有者持有自己的 `SessionAlloc`**：
   - `MptpClient<C>`：客户端作用域 arena（默认 P2K）；`new(conn)` /
     `new_with_page_size(conn, page_size)` / `new_session_alloc()`。
   - `MptpSession<C>`：从请求前缀接管 per-request arena；请求前缀、回复前缀都由它分配，
     会话 drop（最后一个句柄释放）时一起回收。
   - `MptpServlet<C, H>`：连接作用域 arena（默认 P2K）；
     `new(handler)` / `new_with_page_size`。
   - `ServingSession<C>`：每条入向子流在 `accept_async` 时创建自己的 arena；请求前缀、
     头表副本、location 与回复前缀都在这里。
   - `MptpContext<'f, C, S, R>`：请求级 arena（默认 P16K）；location、请求级状态 `R`、
     handler / 中间件 future 都从这里分配。`MptpContext::arena()` 返回**句柄克隆**。
   - `MptpConnection` 不再接收 allocator 参数，servlet 自己创建连接 arena。
4. **不再把 `&'a BumpListAlloc` 放进会被保存的字段**。如果 `ServingSession` 同时拥有
   `bump: BumpListAlloc` 与 `headers: Headers<&BumpListAlloc>`，后者就指向结构体自己的
   字段，属于自引用，安全 Rust 构造不出来。`Arc` 句柄把 arena 地址与持有者地址解耦：
   句柄是 `Clone + 'static` 的，因此可以按值保存 `RequestPrefix<SessionAlloc>` 这类字段，
   会话类型也不需要生命周期参数，`abs_mptp` 的 `Session` 关联类型不必改 GAT。

### 8.2 为什么 `MptpContext::arena()` 返回克隆而不是 `&SessionAlloc`

`HandlerFuture` / `MiddlewareFuture` 需要把 future 装进 arena，同时 future 自己又借用
`&mut MptpContext`。如果 allocator 是 `&ctx.arena`，就会在 `box_future_` 的调用点同时
产生对 ctx 的共享借用与可变借用。返回句柄克隆后，future 自己拥有一个 arena 句柄，
`'a` 只描述 future 捕获的数据，不再与 allocator 的借用冲突；`ArenaFuture` 也因此改为
按值持有 allocator。

### 8.3 页大小与超大块兜底

`BumpListAlloc` 的一个块不能跨页，单块上限是该页的可用容量。实测发现一个 demo 的
handler future 有 4456 字节，超过 P2K 页容量会导致 `Box::new_in` abort。处理：

- 请求级 context arena 默认用 `PageSize::P16K`（handler / 中间件 future 常见多 KB）；
- `mptp_serving::arena_::arena_future_` 先用 `Layout::new::<F>()` 向 arena 探测一次：
  放得下就让 future 进 arena，放不下回退 `Global`，避免整个请求因 future 太大而 abort；
- 会话 / 连接 arena 仍默认 P2K；需要更大的头表或前缀时用 `new_with_page_size` 调大，
  但要记住单块仍不能跨页。

### 8.4 保留的 allocator 配置

`TrRpcClientAllocConfig::{SharedConnAlloc, RingAlloc}` 与 `TrServingAllocConfig` 保留。
它们描述的是共享连接句柄与 smux ring 存储的来源，生命周期与所有权不属于「一次请求的
arena」，与本次统一的 arena 目标不冲突。

### 8.5 迁移清单与验证

- `mptp_wire`：`Headers::new_in` 保留；新增
  `U16OrStr::realloc_in` / `HeaderVal::realloc_in` / `Headers::realloc_in`（跨 arena 复制）、
  `Headers::try_get_str`（按文本键名查询，不构造带 allocator 的键）、
  `Headers::set_text_named`（键和值都由头表自身 arena 分配）。
- `mptp_smux`：两份 `TrConfig` 去掉 `Alloc`；`MptpClient` / `MptpSession` /
  `MptpServlet` / `ServingSession` / `MptpConnection` 按 §8.1 调整；`RequestBuilder` /
  `ResponseBuilder` 增加 `header_text` / `header_text_named`，让调用者不必接触 allocator。
- `mptp_serving`：`MptpContext` 持有 `SessionAlloc`；`ArenaFuture` 按值持有 allocator 并
  带 `Global` 兜底；`HandlerFuture` / `MiddlewareFuture` 去掉 allocator 参数；恢复
  `rpc_core_v1` dev-dependency、元数据测试与 `im_session` 示例。
- `rpc_core_v1`：配置的前缀 / 头表类型固定为 `SessionAlloc`；请求前缀用
  `client.new_session_alloc()` 创建 arena；代理级额外头用 `Global` 构造，合并时
  `realloc_in` 到请求 arena；头表 helper 泛型化到 `A: AllocatorClone`；
  `DataTypeId::from_str` 直接解析十进制 / `0x` 十六进制。
- `mptp_smux_v1_demo`：配置改成 `...<SessionAlloc>`；头键常量改为裸 `u16`；读写头改用
  `header_text_named` / `try_get_str` / `header_u64_`。
- 验证：`cargo check --workspace --all-targets`、`cargo test --workspace`（300s 内）、
  `cargo clippy --workspace --all-targets` 全部通过，`format_use_imports.py --check` 合规。
