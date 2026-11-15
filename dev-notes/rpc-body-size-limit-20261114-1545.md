# 回复体 64 KiB 上限：两个实现 bug 与修复

- 日期：2026-11-14
- 触发：`mptp_im_demo` 的 headless 验收在大回复体（约 95 KiB 的 `RoomSnapshot`）上挂起。
- 结论：**消息本身没有字节大小上限**；两个 64 KiB 现象都来自实现：
  1. `rpc_core_v1::serving` 先编码进 64 KiB `Vec` 再写定长体；
  2. `mptp_wire::chunked::ChunkedEncode` 假设目标段一定连续，环形缓冲回绕时只搬了一段。
- 公开 API：**未改动**（`write_ok_async` / `write_stream_item_async` 签名不变）。

---

## 1. Bug A：`write_ok_async` / `write_stream_item_async` 的 64 KiB 暂存区

### 症状

`im_protocol` 的 `RoomSnapshot` 约 95 KiB 时，`Call im/v1/room/join_room` 服务端返回
`ServeError::Target`（"编码暂存区不足"），客户端拿不到回复；`Pull` 的宏端点同理。

### 根因

`rpc_core_v1/src/serving/body_.rs` 的实现：

```rust
const K_ENCODE_CAP_: usize = 64 * 1024;
let mut storage = vec![0u8; K_ENCODE_CAP_];
value.encode_async(MpEncoder::new(&mut sink))?;   // 超过即失败
session.response(Status::Ok).sized_async(&mut src, len).await?;
```

这是为了「定长体必须先知道总长」而引入的暂存。它把**实现约束**错误地变成了**消息上限**：
denco 的编码器只要求一个 `TrBuffWrite`，底层 buff 再小也能通过异步等待推进；项目里请求体
早已是「64 字节栈上 ring + 并发发送」，从没有整段暂存。

### 修复

两个 helper 改成与请求体同构的流式写法：

- `write_ok_async`：64 字节栈上 SPSC ring；编码端写 ring，`response(...).chunked_async`
  从另一半读走并成帧，二者在同一个 task 内 `futures::future::join` 推进；
- `write_stream_item_async`：同样的 ring，发送端用 `SuffixStream::write_chunks_async`；
- 删除 `K_ENCODE_CAP_` / `encode_value_async_`。

ring 只是并发窗口，**不是上限**。回复体因此从定长（`Body_Size`）变成分块
（`Body_Transfer: Chunked`）；客户端的 `BodyRead` 同时支持两种（`mptp_wire::chunked` /
`limited`），现有调用点不需要改。

`write_status_async` 保持定长：错误文本本来就是内存里的 `String`，`sized_async` 直接
借用它的字节，没有暂存上限。

## 2. Bug B：`ChunkedEncode` 在环形目标上只搬「连续窗口」

### 症状

修掉 Bug A 后，`>64 KiB` 的回复仍失败：

```text
协议错误：写回复体失败：分块搬运：段让出的量不符（预期 64 字节，实际 56 字节）
```

`<= 64 KiB` 一切正常，恰好跨过 64 KiB 就失败——与 demo 的通道 ring 容量（64 KiB）吻合。

### 根因

`mptp_wire/src/chunked.rs` 的 `chunked_write_all_async_` 在写完 2 字节块长后：

```rust
let mut src_child = src_segm.as_segm_ref();
src_child.move_items_to_segm(&mut dst_child)   // 只调用一次
```

`SegmRef::move_items_to_segm` 有一个**固有方法**（`segm_.rs`），它只把目标段的
`buffer_[offset_..]` 这一段连续窗口填满；环形缓冲在回绕处会给出「到尾 + 从头」两段，
固有方法只处理第一段。`ChunkedEncode` 却假设一次调用能搬完 `len`，于是搬了 56/64 就
判 `Unsatisfied`。

为什么只在 `> ring 容量` 时触发：消息小于 ring 时目标窗口一次就够；跨过容量后目标 ring
必然回绕。

### 修复

改成循环：每轮重新借出源 / 目标子段，直到本块的 `len` 字节全部写入：

```rust
let mut moved_into_dst = 0usize;
while moved_into_dst < len {
    let mut src_child = src_segm.as_segm_ref();
    let mut dst_child = dst_segm.as_segm_mut();
    let step = src_child.move_items_to_segm(&mut dst_child);
    if step == 0usize { break; }
    moved_into_dst += step;
}
```

块长仍然在动笔前定下（2 字节前缀不能回填），只是允许分多次填满目标段。

## 3. 证据

- demo 回归：`cargo test -p im_testkit --test scenarios -- big_payload`，使用**原始 64 KiB**
  通道 ring + 64 KiB 传输 ring：
  - `Call room/join_room` 回复 ~95 KiB；
  - `Pull room/events/{id}` 首项 `Snapshot` ~95 KiB；
  - `Pull im/v1/diag/big/2` 两个 80 KiB item（`write_stream_item_async`）。
- 框架测试（`mptp_rpc`）：`mptp_wire` 22 passed、`rpc_core_v1` 28+ passed、
  `rpc_svc_macro` / `mptp_serving` / `mptp_smux_v1_demo` 全绿。

## 4. 后续

- 建议给 `mptp_wire::chunked::ChunkedEncode` 补一个「环形目标 + 超过 ring 容量」的
  框架级回归测试（本轮只在 demo 级覆盖）；
- `Data_Type_Id` 的生成仍是业务手写，见 `mptp_im_demo/ACCEPTANCE.md` G-16
  （与本文件的两个 bug 无直接关系，但同属「实现约束不该泄漏成协议约束」）。
