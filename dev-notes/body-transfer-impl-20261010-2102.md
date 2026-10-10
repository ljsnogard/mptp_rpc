# 体的两种传输机制：落地实现

日期：2026-10-10 21:02（同日修订：体的形状由「TrBuffRead 源」改为「值 + 编码格式」）
状态：**已实现**。核销 `body-transfer-20261010-2020.md` §4 列出的 8 条偏差，并把 §5 的待定协议问题落成决策。
范围：`mptp_core/src/specs.rs`、`mptp_core/src/codec/`、`mptp_core/src/messaging/{limit,chunked,body,basic}.rs`、收发路径与 `rpc_demo`

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

## 2. 体的形状：业务值与编码格式打包，编码推迟到发送时

这是本轮改动最大的地方，也是与更早两遍尝试的分水岭。

### 2.1 曾经的错法

`RequestBuilder::body<T: Serialize>` 里直接 `rmp_serde::to_vec`，一次犯三个错：

1. **写死 MessagePack**——「怎么序列化」是调用方的选择，不是协议层能替它决定的；
2. **在构造期编码**——body 一大，卡住的就是构造请求的那一行；
3. **落成 `Vec<u8>`**——体从此「必须完整存在」，`try_encoded_len`（先编一遍量长度）
   成了唯一出路，而它要求体可重放。「边序列化边发送」正是死在这一步。

### 2.2 现在的形状

```text
builder：    .body(data)                // 打包成 CodableBody(data, Codec::MsgPack)
             .body_with(data, codec)    // 打包成 CodableBody(data, codec)
             .body_bytes(bytes)         // 已经编好的字节：Vec<u8> 自己就是体
             .body_size(n)              // 显式声明定长（不调用则默认分块）

发送（两条路，按内容的形状分）：
  字节流      send_body_from_reader_async(src: &mut impl TrBuffRead<u8>, dst, headers, tok)
              └ 便利包装 send_content_async(&[u8], …)：`&[u8]` 自己就是 TrBuffRead
              └ 定长 → 源外套 LimitedRead，段到段搬；分块 → 源每让出一段封一块
  业务值      send_body_async(body: &impl TrRpcBody, dst, headers, tok)
              └ 定长 → dst 外套 LimitedWrite；分块 → dst 外套 ChunkedSink（一次 write 一块）
```

- `TrRpcBody` 只回答两件事：**有没有体**、**怎么把内容写进一个 `io::Write`**。它没有
  「先量长度」的方法——那要求体可重放，也堵死边编边发；
- 编码发生在 `try_encode_into`：`rmp_serde::encode::write` 把字节逐段直接编进目标半边，
  **路径上没有中转缓冲**，总长直到发完都不必知道；
- `Codec`（内置枚举）决定 `Body_Type` 头：不指定就用 MessagePack，指定了就用指定的那个
  ——「调用方不明确指定才默认」，默认值不参与其余情况。

### 2.3 为什么是两条路，而不是把其中一条推广成唯一形状

曾经试过两种「只留一条路」的写法，都不成立：

| 方案 | 为什么不成立 |
| --- | --- |
| 只留 `TrBuffRead` 源 | 编码器是 push 的（往 `io::Write` 里写），`TrBuffRead` 是 pull 的（从源里借段）。要让「业务值」变成源，必须先把它编进某块内存——要么分配一块缓冲（违反 README §7 第 1 条），要么让调用方另给一块 ring。两条路都把「边编边发」推回去了。 |
| 只留「编码器写目标」 | 字节已经在别处产生时（`&[u8]`、环的读半边、文件、上游转发来的字节），硬要求它先变成一个「体对象」再被逐段写出，是多余的绕路——而 `&[u8]` 本来就已经是 `TrBuffRead<u8>`。 |

于是两条路各司其职，差别只在**字节是谁产生的**：

- **已经存在** → pull：从源借段、往目标借段、段到段搬（`send_body_from_reader_async`）；
- **正在产生** → push：编码器把字节直接编进目标半边，外面按模式套写口（`send_body_async`）。

两条路都遵守同一条纪律：**协议层不事先知道体有多长**，也都不为体分配缓存。`codec` 模块里
早已写好的那条链路（`TrEncoder` → `EncodeBuffWrite` → 目标半边）由第二条路接上。

## 3. 新增的三个 IO 层部件

### `messaging::limit` —— 限长读写

`LimitedRead` / `LimitedWrite` 是 `TrBuffRead` / `TrBuffWrite` 的衍生类型：把每次
`try_read` / `try_write` / `read_async` / `write_async` 的 `Demand` 与剩余额度求交，
底层**永远不会**借出超过额度的段。

额度是**精确**结算的：段接口只在段被回收时记账，调用方完全可能只消费一半就放掉段，
所以包装段在 `Drop` 时比较「借出时的剩余量」与「归还时的剩余量」（`iter_slices()` 各片
长度求和，不分配），差值才是真正被消费/写入的字节数。额度用尽表现为「流结束」
（错误标签 `Closing`），对 `AsStdRead` 就是 EOF。

定长发送正是靠它兜底：头里声明了 `Body_Size`，而体给不出那个长度（例如它自己也不知道
会编出多少字节）时，限长写口在写第 `n + 1` 个字节的那一刻报错，多出来的字节一个都落不
到流上。

### `messaging::chunked` —— 分块帧

- `ChunkedWrite`：`write_chunk_async(&[u8])` 写「2 字节长度 + 内容」，`finish_async()`
  写终止块；超过上限的内容自动切块；
- `ChunkedSink`：实现 `std::io::Write` 的分块写口，**每一次 `write` 封一个块**，块长就是
  这一次写出的字节数——这正是「编码器边编边写、写多少块就多大」所依赖的语义；
- `ChunkedRead`：本身就是一个 `TrBuffRead`，**借出的段不跨越块边界**，读到终止块即 EOF，
  于是块边界对 `AsStdRead` / `rmp_serde` 完全透明。同步的 `try_read` 只在当前块内工作，
  跨块返回 `WouldBlock`（读长度前缀是一个等待点）。

### `messaging::body` —— 模式判定、发送、接收视图

- `body_transfer_of`：上表那套判别；请求与回复共用；
- `send_body_from_reader_async`：字节流源的搬运入口（定长/分块按头分派），`send_content_async`
  是它在「内容就在内存里」时的便利包装——`&[u8]` 自己就是源，不需要任何中间类型；
- `send_body_async`：业务值体的入口，定长套限长写口、分块套分块写口，返回**线上**字节数；
- `body_reader(rx, headers) -> BodyReader`：按头给出体读视图（没有体 / 定长 / 分块三态），
  它自身实现 `TrBuffRead`，因此上层可以直接 `AsStdRead` + `serde` 解，一个字节都不多读。

## 4. 实现中值得记下的四个坑

### 4.1 解出一个值 ≠ 读完这条体

`rmp_serde::from_read` 只知道**值**在哪里结束，不知道**体**的边界在哪里。分块体的终止块
因此可能留在流上——紧随其后的字节（suffix stream）就会被下一条报文当成自己的开头。

修正：`read_body_async_` 在解出值之后，把体的剩余部分读干净（`io::copy` 到
`io::sink()`，栈上 8 KiB 缓冲、不分配）。读干净的终点由体视图给出：定长到声明长度为止，
分块到终止块为止。

### 4.2 「读一眼」段不等于「消费」段

段接口的消费量只在段被回收时结算。字节流搬运的第一版把源段首片的字节**复制**出去写目标，
源段的 offset 一直是 0，于是 `read_async` 反复给出同一段——死循环写同一块内容。

修正：那条路必须**段到段**搬（`move_items_from_segm`），搬移本身既填充目标、又推进源。
目标段按 `Demand::exactly(2 + take)` 借出，多一字节都不借，免得把源里属于下一块的字节一起
搬走。这条教训对所有「流式源」都成立：**要让源前进，就得真正搬走它的字节**。

### 4.3 `gen_may_cancel_future` 会删掉提到取消令牌类型的 where 谓词

宏的规则是「用户谓词去掉所有提到取消令牌类型参数的部分」。于是 step 函数里写不出
`TyTok: 'f`，也就无法对借用 `'f` 的底层 `ReadAsync<'f>` 调 `may_cancel_with`
（它要求 `C: 'f`）。

修正：`io_.rs` 提供 `race_cancel_`——把不可取消的底层 future 与令牌的 `cancellation()`
竞速，令牌先响应就返回 `None`（底层 future 被丢弃，等待注册随之注销）。它只要求
`TyFut: Future` 与 `TyTok: TrCancellationToken`，与生命周期无关。

### 4.4 `source()` 会把 `'static` 约束传染到每一个使用者

`core::error::Error::source` 要求返回 `&(dyn Error + 'static)`，于是
`LimitReadError<E>` 想实现 `source` 就得要 `E: 'static`；而 `E` 是 `TrBuffRead::Err`，
这条约束会一路传染到所有包装类型。四个新错误类型（`LimitReadError` /
`LimitWriteError` / `ChunkedReadError` / `BodyReadError`）因此都**不**实现 `source()`，
底层错误的文案由 `Display` 带出。

## 5. §4 那 8 条偏差的核销

| # | 原偏差 | 现在 |
| --- | --- | --- |
| 1 | 写 prefix 前预量体长 | `send_*_async` 拆成「先写前缀，再 `send_body_async`」，全程不预量 |
| 2 | `try_encoded_len` 只有两态 | 该接口连同 `try_as_bytes` 一并删除；传输模式由头声明，体不再自报「能不能借出字节」 |
| 3 | 没有产出 chunk 的接口 | `messaging::chunked` + `ChunkedSink`（编码器的每一次写落成一块） |
| 4 | 标准头没有承载模式 | `StdHeaderKey::Body_Transfer` / `StdHeaderVal::Body_Transfer_Chunked` |
| 5 | 读体只认 `Body_Size` | `body_reader` 按头给三态视图，`read_body_async_` 走它 |
| 6 | 「有类型没长度」一律判违规 | 现在「分块」是合法的一态；只有既无长度又无分块声明才是违规 |
| 7 | 一次性把体解完 | `body_reader` 是视图，先读前缀、再由上层决定要不要读、怎么读 |
| 8 | client 走合并后的入口 | `send_request_prefix_async` / `send_body_async` 两个独立步骤 |

## 6. 验证

- `cargo test -p mptp_core`：**44** 个单元测试 + 13 个 doctest。覆盖模式判定（四种组合与
  三类违规）、限长读写的额度结算、分块帧的线上字节、`ChunkedSink` 的「一次写一块」、
  跨块往返（70 KiB）、「终止块之后的字节不被吃掉」、「定长体多写的字节不被吃掉」、
  「没有体不读一个字节」、长度前缀不完整报错、builder 的默认值与冲突检测、
  `CodableBody` 不在构造期编码、字节流源的两条模式（含 70 KiB 跨块），以及
  **「编码 + 写出 32 KiB 的体不产生与体同阶的分配」**
  ——最后这条是「边序列化边发送」能不能成立的硬指标；
- `cargo test -p rpc_demo`：4 个 CLI 用例；
- 真实 TCP socket 上的一问一答跑通两次：一次定长（当前 demo 的默认形态）、一次把两端临时
  改成 `Body_Transfer: Chunked`（请求体与回复体都分块），验证完即回退。

## 7. 尚未做的事

- **`rpc_demo` 未展示分块**：CLI 与文案仍是定长一问一答；分块只做过临时端到端验证。
- **`Codec::Json` 未实现**：枚举与 `Body_Type` 映射都在，编解码分支返回「尚未实现」而不是
  panic，等有人需要时补。
- **suffix stream**：终止块之后紧接着就是 suffix stream（体读视图读到终止块即停，后续
  字节留给同一条 channel），但 Push / Pull 的收发路径还没落地。
- **`codec::CodecRegistry` 仍未接线**：本轮让 `Codec` 直接参与报文构造与 `Body_Type` 的
  选择，但「按 `Data_Type_Id` 查表」那条路还没有使用者。
