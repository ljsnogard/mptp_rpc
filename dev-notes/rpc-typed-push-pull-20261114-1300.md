# 类型化 Pull / Push 与类型头协议（设计 + 实施记录）

- 日期：2026-11-14 13:00（Asia/Shanghai）
- 状态：**已实施**（2026-11-14）；公开 API 变更见 §9，验收状态见 §10，剩余风险见 §11。
- 关联：
  - `mptp_im_demo/dev-notes/pull-push-flux-20261114.md`（前置决策）
  - `mptp_im_demo/ACCEPTANCE.md` G-15 / A-01..A-08
  - `abs_async_iter/src/{flux_,async_iter_}.rs`
  - `mptp_rpc/mptp_wire/src/{headers_,prefix_}.rs`
  - `mptp_rpc/abs_mptp/src/spec/std_header_.rs`
  - `mptp_rpc/mptp_smux/src/{client/method_,client/session,serving/session_}.rs`
  - `mptp_rpc/mptp_serving/src/{endpoint_,metadata_,middleware_,context_}.rs`

---

## 1. 结论摘要

1. **类型化 flux 放 `rpc_core_v1::stream`**；`mptp_wire` / `mptp_smux` 仍然只认字节。
   `Just` / `Slot` 是「一次性、带流状态」的 guard，不持有业务流对象本体。
2. **`TrAsyncIterator::Item` 不改 GAT**。A-01 spike 采用「迭代器持有共享流状态
   （`Rc<RefCell<Option<Stream>>>`）+ guard 持一次性 token + `PhantomData<&'f mut ()>`」
   的方案：`Iter<'f>` 的 `'f` 用来表达 `Item = Just<'f, T>`，不破坏
   `abs_async_iter` 的既有契约。
3. **`Just` 采用「`next_async` 立即解码、`take` 只放行值」**。原因：固定 `Item` 无法让
   guard 真正借走 reader 再还回迭代器而不引入自引用；`next_async` 解出值后把 reader
   放回共享状态，`take` 只提交「本次读成功」，未 `take` 的 drop 把流标记为 abort。
   语义等价：**take 之前不允许下一次 `next_async`，未 take 的 drop 必然中止而不是静默跳过**。
4. **`Slot` 采用「`set_async` 独占 writer → 编码 → 写 chunk → 归还 writer」**；重复 set
   由 `set_async(self)` 的消费语义在类型层禁止；未 set 的 drop 把 writer 归还并标记
   abort，有限 Push 随即半关闭请求后缀。
5. **类型 ID 第一期显式声明**：`TrDataTypeId::DATA_TYPE_ID`，`0` 保留为「无类型」，
   禁止上线。不使用 `core::any::TypeId` / 运行期哈希表。
6. **一个方向只有一个数据通道**：Pull 请求无 body；Push 响应（有限流）可有 body；
   suffix item 类型只在前缀出现一次，同一 suffix 流所有 item 同一个 `T`。
7. **严格优先**：typed 端点默认 `TypePolicy::Strict`；缺失必填类型头 → `400`；
   类型 ID / codec / Accept 不匹配 → `406`。`Status` 没有 415，不发明状态码。
8. **Push 只做有限流**；无限 Push / 全双工不在本任务实现，只保留设计边界说明。
9. **宏集成（A-08）本期不做**：`rpc_svc_macro` 的 trait 形态仍在恢复中，controller
   的 Pull 端点没有 item 类型强制约束；强行给所有 Pull 端点加 `TrDataTypeId` 约束会
   破坏现有端点。标 `GAP`，另立任务。

---

## 2. A-01 生命周期 spike（语言层）

### 2.1 约束

```rust
pub trait TrAsyncIterator {
    type Item: Sized;
    type Err: core::error::Error;
    type NextAsync<'f>: TrMayCancel<'f, MayCancelOutput = Result<Self::Item, Self::Err>>
    where Self: 'f;
    fn next_async(&mut self) -> Self::NextAsync<'_>;
}
```

`Item` 没有生命周期参数，但实现类型可以有。`Iter<'f>` 的 impl 里写
`type Item = Just<'f, T>` 是合法的：`'f` 来自 iterator 类型本身，而不是来自
`next_async(&mut self)` 的匿名借用。

### 2.2 三种候选

| 方案 | 形状 | 结论 |
| --- | --- | --- |
| A. guard 直接持 `&'f mut Reader` | iterator 把 reader 移进 guard，take/drop 时再还回 | 固定 `Item` 下迭代器与 guard 同时借用同一 reader，需要自引用或 unsafe，**否决** |
| B. 改 `TrAsyncIterator::Item` 为 GAT | `type Item<'a>` | 破坏 `abs_async_iter` 全部实现与下游，**本期否决**（需单独立项） |
| C. 共享状态 + 一次性 guard | `Rc<RefCell<Option<Reader>>>` + `PhantomData<&'f mut ()>` | **采用**：无 unsafe、无自引用，编译期阻止 `'static` 逃逸，运行期阻止越过未消费 guard 的推进 |

### 2.3 方案 C 的不变量

`StreamCore<S>`（非公开）：

- `slot_: Option<S>`：当前可用的底层流对象（reader / writer）；
- `guard_: bool`：是否已交出一个未消费的 guard；
- `aborted_: bool`：是否被未消费的 drop 中止；
- `finished_: bool`：正常耗尽。

`next_async`：

1. `aborted_` → `Err(StreamError::Aborted)`；
2. `guard_` → `Err(StreamError::GuardPending)`（**不会**静默替换 / 预取）；
3. 取出 `slot_`，做一次读 / 准备；
4. 读成功：`guard_ = true`，返回 `Just(value)` / `Slot(writer)`；
5. EOF：`finished_ = true`，`Err(StreamError::EndOfStream)`；
6. 解码 / 写失败：`Err(StreamError::Decode/Encode/Transport)`，并置 `finished_`。

`Just::take(self)`：`guard_ = false`，返回值。
`Just::drop` 且未 take：`guard_ = false; aborted_ = true`。
`Slot::set_async(self, value)`：编码 → 写 chunk → 归还 writer → `guard_ = false`。
`Slot::drop` 且未 set：归还 writer，`guard_ = false; aborted_ = true`。

`'f` 的作用：`Just<'f, T>` / `Slot<'f, T, W>` 都带 `PhantomData<&'f mut ()>`，
因此不能存进 `'static`；doc test 用 `compile_fail` 固定这一点。

### 2.4 与前置决策的偏差

前置决策写「`Just` 绑定底层 suffix reader」「take 后底层 stream 才推进解码」。
本方案里 `next_async` 已完成**当前值**的解码，`take` 之后才允许下一次 `next_async`；
也就是说「推进到下一个值」仍发生在下一次 `next_async`，而当前值在 `next_async`
返回时已经读出。这是固定 `Item` 下的最小偏差，换来无 unsafe、无 GAT；验收语义
（未 take 不得继续、drop 必 abort、EOF/解码/取消有明确错误）全部保留。

---

## 3. 类型 ID 契约

### 3.1 形状

```rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataTypeId(u64);

pub trait TrDataTypeId {
    const DATA_TYPE_ID: DataTypeId;
}
```

- `DataTypeId::new(raw: u64) -> Option<Self>`：`0` 返回 `None`；
- 文本形态固定写 `0x` + 小写十六进制（无前导零），解析同时接受十进制与
  `0x` / `0X` 十六进制（复用 `HeaderVal::as_u64` 的宽容度）；
- `Display` / `FromStr` 都走这一套，保证 round-trip。

### 3.2 分配策略（第一期）

**显式写 ID**，不做 derive / 哈希 / 运行期注册表。理由：

- `Data_Type_Id` 是跨进程、跨版本契约，哈希会随 canonical path、编译器版本、
  crate 重命名漂移；
- FNV / xxhash 的碰撞无法在编译期消除，线上没有协商通道来兜底；
- 显式 ID 让「读代码就能知道线上类型」成立，也便于在 README 维护一张表。

区间约定：

| 区间 | 含义 |
| --- | --- |
| `0x0000_0000_0000_0000` | 保留：**无类型 / 缺失**，禁止上线 |
| `0x0000_0000_0000_0001` – `0x7FFF_FFFF_FFFF_FFFF` | 应用显式分配（本期使用） |
| `0x8000_0000_0000_0000` – `0xFFFF_FFFF_FFFF_FFFF` | 预留给未来的生成 / 实验区间，本期不使用 |

### 3.3 版本 / 迁移规则

- **重命名 Rust 类型但线上字段完全不变**：ID 不变（线上契约是数据形状，不是路径）；
- **新增字段且旧解码器仍能忽略**：**换新 ID**（denco 的 struct 解码按字段名/位置，
  不做「未知字段忽略」的通用保证；本期不赌这个行为）；
- **字段重排 / 删除 / 类型变更 / 语义变更**：必须换新 ID，旧 ID 永久退休，不复用；
- 一个 ID 只能对应一个 (codec, 数据形状)；同一形状不同 codec 目前允许共用 ID，
  但必须同时校验 `Body_Type`；
- 表维护在 `rpc_core_v1/README.md`（业务 ID 由业务 crate 自己维护，README 给出规则与
  示例）。

---

## 4. 头表方向语义（方法 × 方向 × 头）

### 4.1 通用规则

- `Body_Type` 只表达 **codec**（MsgPack = `0x01` / `application/x-mptp-msgpack`；
  JSON = `0x02` / `application/json`；raw = `application/octet-stream`），
  **不**携带逻辑类型 ID；
- `Data_Type_Id` 只表达**本方向唯一数据通道**的逻辑类型；
- `Accept` 只表达**对端方向的期望**（codec + 可选 type-id），格式见 §5；
- `Head` / `Drop` 没有 body 与 suffix；出现任何数据头按协议违规处理（本期只做校验
  函数的拒绝，不改 MBTP 状态机）。

### 4.2 方向表

| 方法 / 方向 | `Body_Type` | `Data_Type_Id` | `Accept` |
| --- | --- | --- | --- |
| `Call` 请求 | 请求 body codec（必填，缺省 MsgPack 视为缺失） | 请求 body 类型（必填） | 期望响应 body codec + 类型（可选；写了就校验） |
| `Call` 响应 | 响应 body codec（必填） | 响应 body 类型（必填） | — |
| `Post` 请求 | 请求 body codec | 请求 body 类型 | 期望响应 codec + 类型（可选） |
| `Post` 响应 | 响应 body codec | 响应 body 类型 | — |
| `Pull` 请求 | suffix codec（必填；本期只支持 MsgPack） | **缺省**（Pull 请求无 body） | 期望响应 **suffix item** codec + 类型（必填） |
| `Pull` 响应 | suffix codec（必填） | **suffix item 类型**（必填） | — |
| `Push` 请求 | suffix codec（必填） | **请求 suffix item 类型**（必填） | 期望响应 body codec + 类型（可选；Push 响应本期允许有体） |
| `Push` 响应 | 响应 body codec（有体时必填） | 响应 body 类型（有体时必填） | — |
| `View` / `Head` / `Drop` 请求 | 无 | 无 | 无（只做路由 / 能力探测） |

### 4.3 关键裁决

1. **`Data_Type_Id` 是「本方向唯一数据通道的类型」**：Call 的 body、Pull 的
   response suffix item、Push 的 request suffix item。没有 body 的 Pull 请求不写
   `Data_Type_Id`，它期望的 item 类型由 `Accept` 表达。
2. **同一请求既有 body 又有 suffix 的歧义在本期直接禁止**：`Pull` 请求无 body、
   `Push` 响应可为 body 但请求侧只有 suffix。`Call` + suffix 仍保留给旧路径，但
   typed 客户端不会生成这种组合。
3. **一条 suffix 流只允许一个 `T`**：头只在前缀出现一次；异构 item 必须另立 tagged
   union 协议，本期不做。
4. **`Accept` 携带 64 位 ID**：MIME 参数，见 §5；不接受「只写 codec、靠约定推 ID」。
5. **缺失 / 冲突**：
   - 必填头缺失 → `400 Bad Request`（请求本身不完整）；
   - `Body_Type` 不是已知 codec → `406 Not Acceptable`（`Status` 没有 415）；
   - `Data_Type_Id` 与端点元信息不符 → `406`；
   - `Accept` 的 codec / type-id 与端点可提供的响应不符 → `406`；
   - 头值无法解析（例如 `type-id=zzz`）→ `400`。
6. **兼容**：typed 端点默认 `TypePolicy::Strict`；`Optional` 只放宽「头缺失」，
   不放过「头存在但不匹配」。没有全局静默放宽。

---

## 5. `Accept` 编码

格式（单值，不支持 q 值列表）：

```text
application/x-mptp-msgpack; type-id=0x1234
application/json; type-id=42
application/octet-stream          # codec-only（Pull 请求必须带 type-id）
```

- codec 查表：`application/x-mptp-msgpack` → `BodyCodec::MsgPack`，
  `application/json` → `BodyCodec::Json`，`application/octet-stream` → `BodyCodec::Raw`；
- `type-id` 参数大小写不敏感，值接受 `0x` 十六进制与十进制；
- 序列化顺序固定为 `mime; type-id=0x...`，便于 round-trip 测试；
- 解析失败 / 未知 codec / 重复 `type-id` 参数都返回明确错误，不取「第一个碰巧能解析的」。

`AcceptType { codec_: BodyCodec, type_id_: Option<DataTypeId> }` 同时用于
「客户端期望」与「服务端响应声明」（Pull 响应前缀不写 Accept，服务端把同样信息
写进 `Body_Type` + `Data_Type_Id`）。

---

## 6. `mptp_serving` 端点元信息

### 6.1 新增标记（放 `rpc_core_v1::serving`）

前置的 `RpcEndpointInfo`（`rpc-code-first-trait-20261113-1230.md` 决策 4）目前
**仍不存在**；本任务一并补：

最终形状（已实施）：

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcEndpointInfo {
    pub namespace: &'static str,
    pub service: &'static str,
    pub operation: &'static str,
    pub pattern: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcDataTypeInfo {
    // 字段私有；`_` 后缀是仓库对非 pub 成员的命名纪律。
    body_codec_: BodyCodec,
    request_body_: Option<DataTypeId>,
    response_body_: Option<DataTypeId>,
    request_suffix_: Option<DataTypeId>,
    response_suffix_: Option<DataTypeId>,
    accept_: Option<AcceptType>,
    policy_: TypePolicy,
}

impl RpcDataTypeInfo {
    pub const fn new(body_codec: BodyCodec) -> Self;
    pub const fn with_request_body(mut self, id: DataTypeId) -> Self;
    pub const fn with_response_body(mut self, id: DataTypeId) -> Self;
    pub const fn with_request_suffix(mut self, id: DataTypeId) -> Self;
    pub const fn with_response_suffix(mut self, id: DataTypeId) -> Self;
    pub const fn with_accept(mut self, accept: AcceptType) -> Self;
    pub const fn with_policy(mut self, policy: TypePolicy) -> Self;
    // getters
}
```

理由：`RpcDataTypeInfo` 的字段全部私有，后续新增字段不会破坏调用者；`RpcEndpointInfo`
在 trait 恢复前没有扩展需求，用公开只读字段更直接。

理由：`#[non_exhaustive]` 的 struct literal 在跨 crate 不可构造，而公开字段一旦
发布就不能加；构造器形态让 `TypePolicy` / 新字段可以后加。

### 6.2 校验位置

- `rpc_core_v1::serving::validate_request_types(method, headers, &RpcDataTypeInfo)
  -> Result<(), TypeRejection>`：纯函数，不依赖 `mptp_serving`；
- 中间件实现放上层（`rpc_serv_ext` / 应用）；本期 `rpc_serv_ext` 尚不存在，因此：
  - 提供纯函数 + 单元测试（正确 / 缺失 / 不匹配 / codec 不支持 / 方向错误）；
  - 在 `mptp_serving` 侧只验证「`RpcDataTypeInfo` 能挂进 `EndpointMetadata` 并按类型
    读回」（用本地测试标记，不新增 `mptp_serving → rpc_core_v1` 依赖）；
  - A-07 的「进 handler 前拒绝」在纯函数层 + 文档层通过，运行期中间件接线标
    `PARTIAL`，等 `rpc_serv_ext` 落地。
- **`rpc_core_v1` 不依赖 `mptp_serving`**。

---

## 7. 有限 Push 边界

### 7.1 客户端 Push（request suffix）

现有 `RequestBuilder::send_async` 的顺序是「前缀 → 体 → suffix → close_req →
等回复」，因此：

- **有限 Push**：`Slot` 流正常结束（或调用者显式 `finish`）→ 请求后缀半关闭 →
  读回复 → 返回 `Response<C>`；
- **无限 Push / 全双工**：客户端在 suffix 结束前读不到回复，本期**不实现**；
  只保留本 note 的边界说明。

为了让 `Slot::set_async` 能「立即编码 + 写入」，需要在 `mptp_smux` 增加一个
**拥有会话的请求后缀写端**（现有只有 `send_req_suffix_async(&mut reader)` 这个
「整段搬运」入口，无法逐 chunk 写）：

```rust
pub struct RequestSuffixStream<C> { /* 拥有 MptpSession + tx */ }
impl<C: TrConfig> RequestSuffixStream<C> {
    pub fn new_(session: MptpSession<C>) -> Result<Self, ClientError>;
    pub fn write_chunk_async(&mut self, chunk: &[u8]) -> RequestSuffixWriteAsync<'_, C>;
    pub async fn finish_async(self) -> Result<Response<C>, ClientError>;
}
```

另加一个**拥有回复后缀读端**的类型，避免客户端 Pull 的 `Response` 自引用：

```rust
impl<C: TrConfig> MptpSession<C> {
    /// 排出未读完的回复体后，把**拥有**的接收半边交出去。
    pub async fn into_resp_suffix_async(self) -> Result<ChannelRx<C>, ClientError>;
}
```

两者都只做字节搬运，不引入 denco / `Just` / `Slot`，保持 `mptp_smux` 的分层。
它们是对 `mptp_smux` 的**新增公开 API**，实施前按 `AGENTS.md` 第 1 条登记在
§9；若团队否决，typed flux 仍可用「调用者自己持有 `Response` + `SuffixStream`」
的低层适配形态，只是客户端 Push / 高层 Pull 需要多一步手工绑定。

### 7.2 服务端 Push（request suffix 消费）

handler 先 `ctx.req_suffix_async().await?` 拿到 `RequestSuffixRead`，再交给
`TypedJustFlux::new(reader)`；flux 拥有该视图，逐项解码。请求体与 suffix 的顺序
由 `ServingSession` 保证。suffix 结束时 reader EOF → `StreamError::EndOfStream`。

### 7.3 服务端 Pull（response suffix 生产）

handler 先 `ctx.session_mut().response(Status::Ok).suffix_stream_async().await?`
拿到 `SuffixStream`，再交给 `TypedSlotFlux::new(stream)`；每个 `Slot::set_async`
写一个 denco 值作为 chunk。`finish` → `SuffixStream::finish_async`。

---

## 8. typed flux 形状

```rust
// 读方向
let mut flux = TypedJustFlux::<_, T>::new(reader);
let mut iter = flux.pull_async().await?;
while let Ok(just) = iter.next_async().await {
    let value: T = just.take();          // 同步 take；也可 `take_async()` 兼容目标形状
    // ...
}

// 写方向
let mut flux = TypedSlotFlux::<_, T>::new(writer);
let mut iter = flux.pull_async().await?;
while let Ok(slot) = iter.next_async().await {
    slot.set_async(value).await?;        // denco 直接编码进底层 buff
}
iter.finish_async().await?;              // 有限流：close → 等服务端回复
```

- `TypedJustFlux<R, T>` 要求 `R: TrBuffRead<u8> + TrConsumerState`，
  `T: TrDecodableOwned`；
- `TypedSlotFlux<W, T>` 要求 `W: TrSuffixWrite`（本 crate 定义的小 trait，借出
  `TrBuffWrite` 视图），`T: TrEncodable`；
- `TrSuffixWrite` 的实现：`SuffixStream`（服务端 response suffix）→
  `ResponseSuffixWriter`、`RequestSuffixStream`（客户端 request suffix）→
  `RequestSuffixWriter`，以及测试用切片 sink；
- `Just::take` 为同步；另提供 `take_async` 返回 `Ready<Result<T, StreamError>>`，
  对齐 `mptp_im_demo` 文档里的目标形状；
- 取消：`next_async` 内部对底层 I/O 使用调用方传入的令牌；`TrAsyncIterator` 接口
  没有令牌参数，因此提供 `next_may_cancel_async(&mut self, token)` 作为公开入口，
  trait 方法用 `NonCancellableToken` 委托。调用者仍可对返回的 future 使用
  `may_cancel_with`；future 被 drop 时，未完成的 guard 走 abort 路径。
- 错误类型 `StreamError` 实现 `core::error::Error` + `Display`，覆盖
  `GuardPending` / `Aborted` / `EndOfStream` / `Decode` / `Encode` / `Transport` / `Cancelled`。

---

## 9. 公开 API 变更清单（实施前登记）

| crate | 变更 | 性质 | 理由 |
| --- | --- | --- | --- |
| `rpc_core_v1` | 新增 `stream` 模块（`DataTypeId` / `TrDataTypeId` / `AcceptType` / `BodyCodec` / `TypePolicy` / `Just` / `Slot` / `TypedJustFlux` / `TypedSlotFlux` / `StreamError` / `TrSuffixWrite`） | 新增 | 本任务核心交付 |
| `rpc_core_v1` | 新增 `serving::RpcEndpointInfo` / `RpcDataTypeInfo` / `validate_request_types` / 头表读写 helper | 新增 | 端点元信息 + A-06/A-07 |
| `rpc_core_v1` | `preludes` 转出上述常用项 | 新增转出 | 使用便利，不改变既有语义 |
| `mptp_smux` | 新增 `client::RequestSuffixStream`（拥有会话的请求后缀写端） | 新增 | 有限 Push 无法用现有「整段 reader」入口逐 chunk 写 |
| `mptp_smux` | 新增 `client::RequestSuffixWriter` / `serving::ResponseSuffixWriter`（`TrBuffWrite` 借用视图） | 新增 | 让调用者直接使用底层自带 buff，不自备 `Vec` / 暂存区 |
| `rpc_core_v1` | `TrSuffixWrite` 改为「借出 `TrBuffWrite` 视图」；`Slot::set_async` 直接编码 | 修改（本会话新增 API 的收紧） | 去掉 64 KiB 上限与一次拷贝 |
| `mptp_smux` | 新增 `MptpSession::into_resp_suffix_async`（返回已有公开类型 `ChannelRx<C>`） | 新增 | 客户端 typed Pull 需要拥有 reader，避免 `Response` 自引用 |
| `mptp_serving` | **库不改**；只加 dev-dependency 测试标记可挂进 `EndpointMetadata` | 仅测试 | 元信息标记在 `rpc_core_v1`，中间件后续放上层 |
| `rpc_svc_macro` | **本期不改**（A-08 GAP） | — | trait 形态仍在恢复；强制 Pull item `TrDataTypeId` 会破坏现有端点 |

`mptp_wire` 不改：`Headers::data_type_id` / `set_text` / `set_num` 已经够用。

> 实施状态：上表 `rpc_core_v1` 与 `mptp_smux` 的新增项已在 2026-11-14 会话落地。
> 由于会话指令是「执行本 prompt」，这些新增 API 先按 §1–§8 的基线实现；若团队评审
> 否决 `mptp_smux` 的两个增量，只需退掉 §7.1 的客户端逐 chunk Push 与高层 Pull，
> 低层 `TypedJustFlux` / `TypedSlotFlux` 仍可继续使用。

---

## 10. 待办 / 验收映射（实施后回填）

| 验收 | 本 note 对应实现 | 状态 |
| --- | --- | --- |
| A-01 类型 / 生命周期 | `stream::{Just, TypedJustFlux, Slot, TypedSlotFlux}` + `Just` 的 `compile_fail` doctest | **PASS（语义层）** |
| A-02 Pull Just | `stream::just_` 单测 + `tests/typed_flux.rs` | **PASS** |
| A-03 Push Slot | `stream::slot_` 单测 + `tests/typed_flux.rs` | **PASS（语义层）** |
| A-04 分层 | `Just`/`Slot` 只在 `rpc_core_v1::stream`；`mptp_smux` 新增类型只用字节名 | **PASS** |
| A-05 有限 Push | §7.1 `RequestSuffixStream::finish_async` + mock finish 用例 | **PARTIAL**（真实网络未跑） |
| A-06 类型头协议 | §3–§5 + round-trip / 缺失 / 非法 / 不匹配单测 | **PASS** |
| A-07 元信息校验 | §6 + `validate_request_types` 单测 + `EndpointMetadata` 冒烟 | **PARTIAL**（中间件未接线） |
| A-08 宏集成 | 本期不做 | **GAP** |

---

## 11. 未决 / 风险

1. **A-01 方案 C 是语义近似**：`Just` 不真正借 reader；「不能跨 stream 复用」由
   `Rc` 状态 + 一次性 guard 运行期保证，不是纯编译期保证。若团队要求类型层强制
   「guard 存活期间 reader 不可用」，需要 GAT（破坏性）或 unsafe 自引用，另立任务。
2. **`mptp_smux` 新增两个公开类型**：若否决，需退回「调用者持有 Response +
   SuffixStream」的手工绑定，客户端 Push 本期无法交付。
3. **类型 ID 表没有中心注册表**：本期靠评审 + README 纪律；跨 crate 冲突需要人工
   发现。后续可加 build-time 注册 / CI 检查，但不在本期。
4. **宏没有自动挂 `RpcDataTypeInfo`**：typed 端点需要手写 `.with_metadata(...)`；
   `im_client` 的宏生成 Pull 适配器要等到 A-08。
5. **denco 的 struct 兼容性未验证**：本 note 的版本规则按「换字段就换 ID」最保守
   处理，不依赖 denco 的未来字段忽略能力。
6. **无限 Push / 全双工**：明确不在本期；现有 `send_async` 顺序决定了它不可用。

---

## 12. 术语澄清：「有限 Push」到底指什么（2026-11-14 评审）

### 12.1 不是「item 数量有限」

`Push` 的数据量完全由生产者决定：0 条、3 条、一千万条都可以。库没有、也不应该
规定 item 数量上限。A-05 里的「有限」**不是**「有界长度」，而是**生产者最终会半关闭
请求后缀**这一生命周期事实。

### 12.2 限制来自客户端高层 API 的顺序

`RequestBuilder::send_async` 的顺序是：

```text
请求前缀 → 请求体 → 请求后缀（整段） → close_req → 读回复前缀
```

在请求后缀写完并半关闭之前，这条路径不会去读回复。因此：

- **「有限 Push」= producer-terminated Push**：生产者最终停止写并半关闭请求后缀；
  客户端随后读回复。它可以提前停（drop `Slot` → abort → `finish_async`），也可以写
  任意多条后正常停。
- **「无限 Push / 全双工」**：生产者在请求后缀仍打开时就要读服务端的回复/停止信号。
  当前高层 API 没有「边写边读」的句柄，因此不能表达。

注意：`MptpSession` 的状态机其实**允许**在 `SendingSuffix` 状态
`recv_resp_prefix_async`（见 `client/session.rs` 的匹配分支）。限制在客户端高层
封装，不在会话 / 协议层。要做真正的全双工，需要把「请求后缀写端」与「回复读端」
从同一个句柄里 `split` 出来，而不是新造线协议。

### 12.3 接收方「随时切断」是什么语义

- **接收方提前关闭子流**：发送方的下一次 `write_chunk_async` / `write_async` 收到
  `Closing` / 传输错误；如果发送方还没半关闭，它也读不到一个完整的回复前缀。
  这是**中止 / 失败路径**，不是「接收方回了一个成功响应」。
- **发送方提前停止**：drop 未 set 的 `Slot` → abort；随后 `finish_async` 半关闭并读
  回复。服务端看到的是**提前结束的 suffix**，可以按自己的业务决定回 200 / 4xx。
  这是当前 A-05 覆盖的「提前结束」路径。
- **服务端推送（Pull 响应后缀）**：这是另一条方向，`SuffixStream` 可以长期写；客户端
  随时 drop 读视图即可切断，服务端写入失败。它**本来就不受「有限 Push」限制**。
  A-05 的 finite/infinite 只针对 `AccessMethod::Push`（客户端→服务端）。

### 12.4 建议改称

文档里把 A-05 的措辞改成：

- **producer-terminated Push**（生产者半关闭后读回复）——当前实现；
- **long-lived / full-duplex Push**（写与读并发）——后续独立设计；
- 不再用「有限 Push」暗示 item 数量有界。

---

## 13. 写入端接入 `TrBuffWrite`（已实施，2026-11-14）

### 13.1 现状

| 方向 | 类型 | 现有 trait |
| --- | --- | --- |
| 客户端读回复 suffix | `RespSuffixRead` / `ChannelRx<C>` | 已实现 `TrBuffRead` / `TrBuffTryRead` |
| 服务端读请求 suffix | `RequestSuffixRead` | 已实现 `TrBuffRead` / `TrBuffTryRead` |
| 服务端写回复 suffix | `SuffixStream<'a, C>` | 只有 `write_chunk_async` |
| 客户端写请求 suffix | `RequestSuffixStream<C>` | 只有 `write_chunk_async` |

读侧已经是标准 buff trait；写侧缺 `TrBuffWrite`（以及可选的 `TrBuffFlush`）。

### 13.2 建议形状：借用的 writer view

不直接把 `TrBuffWrite` 实现在**拥有生命周期**的 `RequestSuffixStream` /
`SuffixStream` 上，而是加一个短生命周期的 view：

```rust
pub struct RequestSuffixWriter<'a, C> { tx_: &'a mut ChannelTx<C> }
pub struct ResponseSuffixWriter<'a, C> { tx_: &'a mut ChannelTx<C> }

impl<C: TrConfig> RequestSuffixStream<C> {
    pub fn writer(&mut self) -> RequestSuffixWriter<'_, C>;
}
impl<'a, C: TrConfig> SuffixStream<'a, C> {
    pub fn writer(&mut self) -> ResponseSuffixWriter<'_, C>;
}
```

两个 view 实现 `TrBuffTryWrite<u8>` + `TrBuffWrite<u8>`，直接转发给内部
`ChannelTx`；`Err` 就是 `ChannelTx` 的 `ProducerError<usize>`。好处：

- denco 可以直接 `encode_async(MpEncoder::new(&mut writer))`，不必先编码到 64 KiB
  暂存 `Vec`；`Slot::set_async` 的 64 KiB 上限消失；
- 与 buffex / abs_buff 的其它工具互通，不需要 `rpc_core_v1` 自定义 `TrSuffixWrite`
  才能写；
- view 结束于 `finish_async` 之前，天然没有「finish 后 tx 为 None」的非法状态。

### 13.3 为什么不在 owner 上直接实现

`TrBuffTryWrite::Err` 要求 `TrTaggedError<WriteErrTag>`；`RequestSuffixStream` 在
`finish_async` 之后 tx 已被取走，若仍实现 trait 就必须为「无 tx」构造一个错误。
`ChannelTx<C>` 的 `Err` 类型由 `C` 的关联类型决定，泛型层拿不到可构造的
`Closing` 值；用 panic 或 synthetic error 都不合适。借用 view 把这条非法状态从类型
层面排除。

### 13.4 `TrBuffFlush` 的边界（已实现）

view 已实现 `TrBuffTryFlush` / `TrBuffFlush`；`close` / `close_async` 的语义就是
**只半关闭写侧**（与 `TrBuffWrite` 的层次一致，不隐含读回复）：

- 客户端：关闭请求后缀写端，之后仍要调用 `RequestSuffixStream::finish_async` 读回复；
- 服务端：关闭回复后缀写端，之后仍要调用 `SuffixStream::finish_async` 收尾 / 读尽请求。

`TrBuffFlush::close_async` 的返回类型只有 `Result<(), Err>`，**无法**表达「关闭 +
读回复」，所以 owner 的 `finish_async` 不能省。若两者都实现，需要把 close 做成幂等，
并在文档里明确「close 只半关闭，不完成会话」。

### 13.5 对 `rpc_core_v1::Slot` 的影响（已落地）

`Slot::set_async` 已从「编码到 64 KiB Vec → `write_chunk_async`」改成
「借 writer view → denco 直接编码 → 段提交」。这改变了：

- 对象字节的提交时机（段 drop 时提交，而不是 `write_chunk_async` 返回时）；
- 取消 / 中途 drop 的边界（未提交的段丢弃，writer 仍可继续或 abort）；
- `StreamError::Encode` 与传输错误的分类。

收益已经拿到：**64 KiB 上限消失**，也不再需要一次完整的 `Vec` 拷贝。单元测试与
`tests/typed_flux.rs` 用固定切片 sink 覆盖了直接编码路径。

### 13.6 当前状态 / 仍待确认

已落地：

- `mptp_smux::client::RequestSuffixWriter` / `mptp_smux::serving::ResponseSuffixWriter`
  实现 `TrBuffTryWrite<u8>` / `TrBuffWrite<u8>`，直接转发会话自带的 `ChannelTx`；
- `RequestSuffixStream::writer()` / `SuffixStream::writer()` 借出 view；view 同时实现
  `TrBuffTryWrite` / `TrBuffWrite` / `TrBuffTryFlush` / `TrBuffFlush`，`close` 只半关闭
  写侧，`finish_async` 仍负责读回复 / 收尾；
- `rpc_core_v1::stream::TrSuffixWrite` 改为「借出 `TrBuffFlush` 视图」（`TrBuffFlush`
  蕴含 `TrBuffWrite`）；
- `Slot::set_async` 直接编码，去掉 64 KiB 暂存与 `Vec` 拷贝。

仍待确认 / 后续：

1. `finish_async` 与 `close_async` 保持两个显式阶段，**暂不做**「先 close 再 finish」的
   幂等组合（2026-11-14 决定）。
2. 真实传输上的大对象 / 慢消费者回归（ring 背压），目前只有固定切片 sink 的单测。
