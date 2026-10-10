# rpc_demo

`mptp_core` 的**最小演示**：在**真实 TCP socket** 上跑一次带请求体、也带回复体的一问一答。

它刻意只做一件事——把「协议层的报文边界」这条路径跑通。资源 CRUD、Push / Pull 那些
场景（README 的 Phase 3 / 4）之后再加，压在同一个 demo 里只会让「哪一层坏了」难判。

---

## 1. 怎么跑

需要 **Nightly 工具链**（`mptp_core` 用到 `#![feature(impl_trait_in_assoc_type)]` 等），
以及同仓的 `smux_v1` 已在 `../../smux_v1` 就位。

两个终端，服务端先起：

```console
$ cargo run -p rpc_demo -- server
服务端已就绪：监听 127.0.0.1:7719，dock 1
等待客户端连接……（在另一个终端跑 `cargo run -p rpc_demo -- client`）

$ cargo run -p rpc_demo -- client
客户端已拨号：本地地址 127.0.0.1:52512，目标 dock 1
服务端回声："hello from rpc_demo"
完成：请求体与回复体都按 Body_Size 完整走了一趟。
```

看到最后两行就说明整条链路都通了。

### 换地址或 dock

```console
$ cargo run -p rpc_demo -- server --listen 0.0.0.0:9000 --dock 7
$ cargo run -p rpc_demo -- client --peer   127.0.0.1:9000 --dock 7
```

`--help` 给出全部参数。客户端拨号自带重试（100 次 × 100 ms），所以先起客户端也不会
立刻失败——但服务端那一行 `已就绪` 仍然要先打出来。

---

## 2. 这一次往返覆盖了哪些路径

| 步骤 | 由谁负责 |
| --- | --- |
| TCP 连接、握手、建出复用连接 | `smux_v1` + `buffex_tokio_adapt` |
| 客户端绑 dock、开子流、交出两块 ring 内存 | `mptp_core::client` |
| 请求前缀（`Call` + `/rpc/echo` + 头）的编码 | `mptp_core::messaging::request` |
| 请求体按头里声明的 `Body_Size` 搬运（段到段，无中转缓冲） | 同上（`send_request_async` + `send_body_async`） |
| 服务端监听、接受子流、交出两块 ring 内存 | `mptp_core::serving` |
| 解码请求前缀、handler 按头给出的体视图直接解出请求体 | `mptp_core::messaging`（`body_reader`） |
| 路由到 `/rpc/echo`、handler 回带体的 `200 OK` | `mptp_core::serving` |
| 回复前缀 + 回复体的写出（先前缀、再按头搬体） | `mptp_core::messaging::response` |
| 客户端解析回复前缀、按协议决策读回复体 | `mptp_core::client` |

**不覆盖**：协议层的单元测试（那些在 `mptp_core` 里，用内存缓冲驱动，不碰 socket）。

---

## 3. 目录结构与各自的职责

```
src/
├── cli.rs      命令行解析（角色 + --listen / --peer / --dock），带单元测试
├── conn_.rs    TCP socket 装配：适配器、握手、建连接；以及「宿主线程」的划分
├── client_.rs  TrClientConfig 的实现，以及一次 Call /rpc/echo
├── server_.rs  TrServingConfig 的实现、EchoHandler、路由装配、服务一条子流
└── main.rs     cargo run 的入口
```

注意两个配置类型是**彼此独立**的：`DemoClientCfg` 实现 `TrClientConfig`，
`DemoServingCfg` 实现 `TrServingConfig`，它们不认识对方。服务端与客户端可能跑在两个
进程里、由两份代码编译，请求 / 响应类型甚至不必来自同一个 crate——这个 demo 里两者
恰好同源，但类型上并没有绑在一起。

---

## 4. 为什么两端都是「宿主线程 + 应用线程」

这是 `conn_.rs` 里唯一一处不那么直观的编排，理由值得单独写下来。

`mptp_core` 的收发走 `abs_buff` 的**段接口**，而那些段由投递在**本线程本地队列**上的
循环供料（tokio 下即 `LocalSet`）；socket 的 IO 又由同一个 runtime 的 IO driver 驱动。
三者若挤在一条 `current_thread` runtime 上：

- 应用侧一旦同步等数据，本地队列就没人驱动，环里的数据永远不来；
- 若改由 IO driver 驱动，socket 的读写又停摆。

所以分成两层：

| 线程 | 持有 | 干什么 |
| --- | --- | --- |
| **宿主线程**（`conn_.rs` 新建） | socket + 适配器 + 连接的五条循环 | `run_until` 一直驱动到进程结束 |
| **应用线程**（`main`） | 只持连接对象 | `bind` / `serve` 或 `request` / `recv` |

socket 全程不跨 runtime：bind / accept / connect 都在宿主线程里完成，地址与连接经
channel 交回应用线程。`MuxConnection` 是 `Send + Sync` 的智能指针，跨线程传递安全。

---

## 5. 改点东西试试

- **换个路径**：改 `server_.rs` 里 `K_ECHO_PATH`（同时改 `client_.rs` 用的那个常量，
  两者都从 `server_` 取，所以只改一处）。路径不匹配时会走 `ServeError::NotFound`。
- **换个方法**：把客户端请求的 `AccessMethod::Call` 换成 `Head` 或 `Drop`，看服务端的
  回复被判为协议违规——`Head` / `Drop` 的回复按协议不带本体内容，而 echo handler 仍然
  回了体，客户端会明确报错而不是猜一个长度读下去。
- **换个体**：改 `main.rs` 的 `K_PAYLOAD`。请求体编成字节串后直接当体（`Vec<u8>` 实现了
  `TrRpcBody`），`Body_Size` 由 `with_sized_body` 顺手写好。
- **改成发业务值**：把请求换成 `RequestBuilder::body(值)`（默认 MessagePack）或
  `body_with(值, codec)`——那时编码推迟到发送时、边编边发，长度不必事先知道，传输模式
  默认就是分块。
- **换成分块传输**：把两端各自头里的 `Body_Size` 换成
  `Body_Transfer: Chunked`（`HeadersBuilder::set(StdHeaderKey::Body_Transfer,
  chunked_transfer_header_val())`），其余代码一行都不用改——收发两侧都只看头来分派。
  协议层对这两种模式的处理已经各有单元测试。
- **看流对齐**：把 echo handler 改成「不回体」，客户端那句 `recv_response_body_async`
  会一个字节都不读——这正是「没有 body 不破坏对齐」的落点。
