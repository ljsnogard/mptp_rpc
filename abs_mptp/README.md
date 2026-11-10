# abs_mptp

MPTP（Multiplexed P2P Transportation Protocol）的**协议形状**：

- 访问方法、状态码、标准头与头表契约；
- 请求 / 回复前缀的 trait 与体声明；
- 客户端 / 服务端会话与 handler 的抽象契约。

本 crate 是 `no_std` 的，不依赖具体传输、MessagePack 编码或字节缓冲实现。

具体的线上格式与体传输实现（前缀编解码、限长体、分块体）在 `mptp_wire`：

```text
abs_mptp       协议形状 / trait
mptp_wire      具体线上格式（前缀、头表、限长体、分块体）
mptp_smux      smux_v1 传输绑定
```
