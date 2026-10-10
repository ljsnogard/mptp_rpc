# 体的两种传输机制：落地实现

日期：2026-10-10 21:02
状态：**已实现**。核销 `body-transfer-20261010-2020.md` §4 列出的 8 条偏差，并把 §5 的待定协议问题落成决策。
范围：`mptp_core/src/specs.rs`、`mptp_core/src/messaging/{limit,chunked,body}.rs`、收发路径与 `rpc_demo`

---

## 1. 拍板的协议约定

| # | 问题 | 结论 |
| --- | --- | --- |
| 1 | 模式标记 | 新标准头 `Body_Transfer`（键 `0xA5`），**只定义 `Chunked` 一个值**（`0x20`） |
| 2 | chunk 前缀 | **定长 2 字节大端 `u16`**；`0` 兼作终止块，因此单块内容上限 65535 字节 |
| 3 | 结束标记 | 长度 `0` 的块（照搬 HTTP last-chunk） |
| 4 | 模式由谁定 | **调用方写头**：定长写 `Body_Size`，分块写 `Body_Transfer: Chunked`。不存在「试探体类型推断模式」的路径 |

判别规则（`body_transfer_of`）是**唯一**的边界判据：

```text
Body_Transfer: Chunked 在场 + Body_Size 在场  → 协议违规（两种边界声明互斥）
Body_Transfer: Chunked 在场                    → 分块
Body_Size 在场（0 视为没有体）                 → 定长
两者都不在场 + Body_Type 在场                  → 协议违规（声明了类型却给不出边界）
两者都不在场                                   → 没有体
```

`Body_Transfer` 的取值认不出来时同样报违规，而不是当作「没有声明」——后者会让接收方按
错误的边界切分后续字节。

## 2. 发送体的统一形状：源是一个 `TrBuffRead<u8>`

这是本轮最关键的一条，也是与上一次尝试的分水岭：

```text
send_body_async(src: &mut impl TrBuffRead<u8>, dst: &mut impl TrBuffWrite<u8>, headers, tok)
```

- 协议层**不预量长度**、**不为体分配任何缓存**，只按头里声明的模式把 `src` 的字节搬进 `dst`；
- 定长：`LimitedRead` 把源钉在 `Body_Size` 上，源不足报截断，源有多余不碰；
- 分块：源每让出一段就封一块，**块长就是这一段实际的字节数**（超 65535 自动切开），
  源耗尽后补一个长度 `0` 的块。

「边序列化边发送」因此有了明确形状：把序列化结果写进一块环，再把**环的读半边**当 `src`
交给这里——一段一段地流出去，块长天然等于环让出的那一段，全程没有第二块缓冲。

代价是：体必须在发送时**已经落在某块内存里**。任意 `Serialize` 值只有 push 式的
`try_encode_into`，给不出 `&[u8]`；为此 `TrRpcBody` 增加了 `try_as_bytes`
（`EncodedBody` 返回 `Some`，`Nothing` 返回 `None`，其余默认报
`BodyEncodeError::NotByteAccessible`）。也就是说，**构造请求时就编成 `EncodedBody`**
是「值体」的标准用法；协议层不会替它临时分配缓存（README §7 第 1 条）。

## 3. 新增的三个 IO 层部件

### `messaging::limit` —— 限长读写

`LimitedRead` / `LimitedWrite` 是 `TrBuffRead` / `TrBuffWrite` 的衍生类型：把每次
`try_read` / `try_write` / `read_async` / `write_async` 的 `Demand` 与剩余额度求交，
底层**永远不会**借出超过额度的段。

额度是**精确**结算的：段接口只在段被回收时记账，调用方完全可能只消费一半就放掉段，
所以包装段在 `Drop` 时比较「借出时的剩余量」与「归还时的剩余量」（`iter_slices()` 各片
长度求和，不分配），差值才是真正被消费/写入的字节数。额度用尽表现为「流结束」
（错误标签 `Closing`），对 `AsStdRead` 就是 EOF。

### `messaging::chunked` —— 分块帧

- `ChunkedWrite`：`write_chunk_async(&[u8])` 写「2 字节长度 + 内容」，`finish_async()`
  写终止块；超过上限的内容自动切块。
- `ChunkedRead`：本身就是一个 `TrBuffRead`，**借出的段不跨越块边界**，读到终止块即 EOF，
  于是块边界对 `AsStdRead` / `rmp_serde` 完全透明。同步的 `try_read` 只在当前块内工作，
  跨块返回 `WouldBlock`（读长度前缀是一个等待点）。

### `messaging::body` —— 模式判定、发送搬运、接收视图

- `body_transfer_of`：上表那套判别；请求与回复共用。
- `send_body_async`：段到段的搬运，按模式分派。
- `body_reader(rx, headers) -> BodyReader`：按头给出体读视图（没有体 / 定长 / 分块三态），
  它自身实现 `TrBuffRead`，因此上层可以直接 `AsStdRead` + `serde` 解，一个字节都不多读。

## 4. 实现中值得记下的三个坑

### 4.1 「读一眼」源段不等于「消费」源段

段的消费量只在段被回收（`Drop`）时结算。第一版分块搬运把源段首片的字节**复制**出去写
目标，源段的 offset 一直是 0，于是 `read_async` 反复给出同一段——死循环写同一块内容。

修正：分块搬运必须**段到段**搬（`move_items_from_segm`），搬移本身既填充目标、又推进源。
目标段按 `Demand::exactly(2 + take)` 借出，多一字节都不借，免得把源里属于下一块的字节
一起搬走。

### 4.2 `gen_may_cancel_future` 会删掉提到取消令牌类型的 where 谓词

宏的规则是「用户谓词去掉所有提到取消令牌类型参数的部分」。于是 step 函数里写不出
`TyTok: 'f`，也就无法对借用 `'f` 的底层 `ReadAsync<'f>` 调 `may_cancel_with`
（它要求 `C: 'f`）。

修正：`io_.rs` 提供 `race_cancel_`——把不可取消的底层 future 与令牌的
`cancellation()` 竞速，令牌先响应就返回 `None`（底层 future 被丢弃，等待注册随之注销）。
它只要求 `TyFut: Future` 与 `TyTok: TrCancellationToken`，与生命周期无关。

### 4.3 `source()` 会把 `'static` 约束传染到每一个使用者

`core::error::Error::source` 要求返回 `&(dyn Error + 'static)`，于是
`LimitReadError<E>` 想实现 `source` 就得要 `E: 'static`；而 `E` 是
`TrBuffRead::Err`，这条约束会一路传染到所有包装类型。三个新错误类型
（`LimitReadError` / `LimitWriteError` / `ChunkedReadError` / `BodyReadError`）因此都
**不**实现 `source()`，底层错误的文案由 `Display` 带出。

## 5. §4 那 8 条偏差的核销

| # | 原偏差 | 现在 |
| --- | --- | --- |
| 1 | 写 prefix 前预量体长 | `send_*_async` 拆成「先写前缀，再 `send_body_async`」，全程不预量 |
| 2 | `try_encoded_len` 只有两态 | 传输模式不再由体类型推断，而由头声明；`BodyTransfer` 三态在协议层 |
| 3 | 没有产出 chunk 的接口 | `messaging::chunked` + `send_body_async` 的分块分支 |
| 4 | 标准头没有承载模式 | `StdHeaderKey::Body_Transfer` / `StdHeaderVal::Body_Transfer_Chunked` |
| 5 | 读体只认 `Body_Size` | `body_reader` 按头给三态视图，`read_body_async_` 走它 |
| 6 | 「有类型没长度」一律判违规 | 现在「分块」是合法的一态；只有既无长度又无分块声明才是违规 |
| 7 | 一次性把体解完 | `body_reader` 是视图，先读前缀、再由上层决定要不要读、怎么读 |
| 8 | client 走合并后的入口 | `send_request_prefix_async` / `send_body_async` 两个独立步骤 |

## 6. 验证

- `cargo test -p mptp_core`：37 个单元测试 + 12 个 doctest，覆盖模式判定（四种组合与三类
  违规）、限长读写的额度结算、分块帧的线上字节、跨块往返（70 KiB）、
  「终止块之后的字节不被吃掉」、「定长体多写的字节不被吃掉」、「没有体不读一个字节」、
  长度前缀不完整报错，以及既有的「写报文不为它攒缓冲」分配探针；
- `cargo test -p rpc_demo`：4 个 CLI 用例；
- 真实 TCP socket 上的一问一答跑通两次：一次定长（当前 demo 的默认形态）、一次把两端
  临时改成 `Body_Transfer: Chunked`（请求体与回复体都分块），验证完即回退。

## 7. 尚未做的事

- **消费型流式体**：`TrRpcBody` / `TrRpcRequest` 至今是 `&self`，真正的「边产生边发」需要
  一个 `&mut self`（或内部可变）的产出接口。目前的做法是先把内容写进环，再从环的读半边
  取源——够用，但体本身仍不是流。
- **`rpc_demo` 未展示分块**：CLI 与文案仍是定长一问一答；分块只做过一次临时端到端验证。
- **suffix stream**：终止块之后紧接着就是 suffix stream（体读视图读到终止块即停，
  后续字节留给同一条 channel），但 Push / Pull 的收发路径还没落地。
