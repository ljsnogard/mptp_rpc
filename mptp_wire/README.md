# mptp_wire

MPTP 请求 / 回复前缀与头表的**传输无关线格式**。

本 crate 只负责：

- `RequestPrefix` / `ResponsePrefix` 的具体类型；
- `Headers` / `HeaderKey` / `HeaderVal` / `U16OrStr`；
- 按 MPTP 规定的顺序读写 MessagePack 前缀；
- 前缀编解码错误 `MessagingError`。

它不依赖 `smux_v1`、dock、ring 或任何客户端 / 服务端实现；`mptp_smux` 与未来的
`mptp_quic` 都应依赖它，而不是各自实现一份前缀 codec。

## MessagePack 实现选择

运行时使用 **denco 体系**：`abs_denco` 的 trait + `mp_denco` 的 MessagePack
编解码器。

原因：

- 前缀是**多个连续的顶层 MessagePack 值**，不是单个 serde value；对整体结构体派生
  serde 会多写容器头；
- 编解码直接面向 `abs_buff::TrBuffRead` / `TrBuffWrite`，读前缀时不越过体的第一个字节；
- 每个 await 点都可以接取消令牌；`rmp-serde` 的主 API 是同步 `Read`/`Write`；
- `mp_denco` 是 `no_std`，并支持 `abs_denco` 的 allocator / arena 体系。

`rmp-serde` 只作为 dev-dependency 用于字节互操作 / 黄金测试，不进入运行时依赖；这与
denco 项目自身的 `mp_denco/tests/interop_.rs` 做法一致。

详细职责边界见 [`dev-notes/mptp-smux-20261110-0730.md`](../dev-notes/mptp-smux-20261110-0730.md)。
