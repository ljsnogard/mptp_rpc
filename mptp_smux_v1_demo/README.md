# `mptp_smux_v1_demo`

`mptp_smux` 的**下游实现**：在真实 TCP socket 上，用 `smux_v1` 作传输、`denco` 作业务体
编码，把 MPTP 的一问一答跑起来。前缀线格式由 `mptp_smux::messaging` 统一拥有，demo 不再
自己实现。它存在的目的只有一个——**验证 `mptp_smux` 的实现符合实际**。

## 它演示什么

```text
client ──dial──▶ server             TCP + smux_v1 握手
  ├─ Call /rpc/echo + EchoRequest ──▶  头里声明 Data_Type_Id = EchoRequest 的标识
  │  ◀── 200 OK + EchoReply            回复同样声明 Data_Type_Id
  ├─ Call /rpc/sum  + SumRequest  ──▶  另一个类型，另一条路径
  │  ◀── 200 OK + SumReply
  └─ Call /rpc/sum  + EchoRequest ──▶  ◀── 406 NotAcceptable（明确拒绝，不猜）
```

第三次调用是**故意错配**：`EchoRequest` 发到 `/rpc/sum`。服务端看到声明的标识不是它期待的
`SumRequest`，于是回 `406` 而**不解码**——这就是「类型靠报文头里的声明确定，而不是靠两端
代码里的默契」这句话的可执行版本。

## 它验证 `mptp_smux` 的哪几件事

| 被验证的东西 | 落在哪 |
| --- | --- |
| 客户端 ring 注入：`DefaultPrepFactory` + 环境 + `accept_async` | `MptpClient::request_to_dock_async` |
| 服务端 ring 注入：**每条**入向子流现场生产一份新内存 | `MptpServer::serve_listener_async` |
| 前缀线格式（由框架统一拥有，不在使用侧） | `mptp_smux::messaging` 的四个读写入口 |
| 会话状态机：前缀 → 体，体边界完全由前缀决定 | `MptpSession::recv_resp_prefix_async` / `recv_resp_body_async` |
| 请求体的写入口（会话中途自己写） | `MptpSession::send_req_body_async` |
| 服务端收尾次序（先半关闭写端、等对端关闭、再放读端） | demo handler 出口的 `close_channel_async`（`MptpServer` 不再代做） |
| 路由：最长前缀匹配 | `mptp_smux::serving::Router` |

## 怎么跑

两个终端：

```console
$ cargo run -p mptp_smux_v1_demo -- server
服务端已就绪：监听 127.0.0.1:7719，dock 1
等待客户端连接……

$ cargo run -p mptp_smux_v1_demo -- client
客户端已拨号：本地地址 127.0.0.1:xxxxx，目标 dock 1
客户端：/rpc/echo 回来 "hello from mptp_smux_v1_demo"（回复声明 0x9b2e6d3d0786e4e0）
客户端：/rpc/sum 回来 42（回复声明 0x88f1b69fa5e3f13c）
客户端：把 EchoRequest 发到 /rpc/sum（类型与路径不匹配），服务端回 406
完成：三次调用都按报文头声明的类型标识分派与校验。
```

自动化（起两个进程、跨进程核对类型标识、退出码判定）：

```console
$ python3 mptp_smux_v1_demo/scripts/e2e_socket.py --rounds 2
第 1 轮通过：三次调用 + 跨进程标识一致 + 406 拒绝
第 2 轮通过：三次调用 + 跨进程标识一致 + 406 拒绝
全部通过（2 轮）。
```

## 目录

| 文件 | 作用 |
| --- | --- |
| `src/body_.rs` | **业务体编解码**：业务值 ⇄ MessagePack 字节；前缀线格式不在这里 |
| `src/tests_.rs` | 下游集成测试：头表、体边界、前缀解码与 demo 的装配函数 |
| `src/cfg_.rs` | 两端配置 + **ring 准备工厂**（环境参与构造、每条子流新内存） |
| `src/protocol_.rs` | 业务类型与它们的稳定类型标识（FNV-1a 哈希类型名） |
| `src/client_.rs` | 客户端：三次调用、写请求体、按类型收回复 |
| `src/server_.rs` | 服务端：路由 + 两个 handler + 处理计数收尾 |
| `src/conn_.rs` | TCP socket ↔ `smux_v1` 复用连接的装配（宿主线程 + 应用线程） |
| `src/cli.rs` | 命令行 |

## 两处值得注意的实现选择

1. **类型标识是类型名的哈希**（与 `mptp_core::codec::hash_str` 同一个 FNV-1a）。两侧共享
   同一份 `impl_type_id_u64!` 声明，就共享了同一套标识，调用点看不到任何手写常量。代价是
   改名即不兼容——真实 RPC 的对应物是 IDL 生成的名字或编号。
2. **长度为 `Body_Size` 头一律写成文本**：便于人读。`mptp_smux` 的默认体边界判定同时接受
   文本与数字两种形态，因此这不是协议要求，而是本 demo 的表示选择。

## 已知的服务端收尾现状

框架的监听循环是**常驻**的（同一条连接上可以持续来子流），而 `smux_v1` 在对端断开时并不会
让 `income_async` 报错，所以本 demo 用「处理计数达到 + 静默 500ms」收尾，另有 5 秒兜底超时。
更该有的形状是框架提供 `serve_listener_async(.., max_channels)`；见 `src/server_.rs` 里的
`TODO(重构)`。
