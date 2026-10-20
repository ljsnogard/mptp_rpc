# rpc_demo

MPTP RPC 骨架的**手动实现试验**：两个独立进程通过 **UDS** 建立 `smux_v1` 连接，在 MPTP 上
手动实现 interface → service 两级路由、每 service 队列与 worker，并完成 workspace / session
两层的 CRUD。

这个 demo **不使用 `#[rpc]` 宏**。它要回答的是：蓝图里的骨架在真实 UDS + MPTP + smux 上
是否落得下来。

---

## 1. 怎么跑

需要 Nightly 工具链。两个终端，服务端先起：

```console
$ cargo run -p rpc_demo -- server --socket /tmp/mptp_rpc_demo.sock
服务端已就绪：UDS /tmp/mptp_rpc_demo.sock，等待客户端……

$ cargo run -p rpc_demo -- client --socket /tmp/mptp_rpc_demo.sock
客户端已连接：UDS /tmp/mptp_rpc_demo.sock
客户端：创建 workspace
  -> Workspace { id: 1, name: "alpha" }
...
客户端：通过 ApiV2 再列一次 workspace（验证多 interface 路由）
  -> []
```

服务端处理完固定 11 条请求后会打印：

```text
服务端：本轮处理 11 条请求，退出。
```

### 一条命令跑完

```console
$ python3 rpc_demo/scripts/e2e_uds.py
```

脚本会构建、起两个进程、跑一轮并核对两端退出码；`--rounds N` 可重复多轮，
`--skip-build` 复用已编好的二进制，`MPTP_CARGO_OFFLINE=--offline` 可离线构建。

---

## 2. 演示的内容

| 层 | 本 demo 的做法 |
| --- | --- |
| UDS | `tokio::net::UnixListener` / `UnixStream` + `buffex_tokio_adapt` |
| 多流 | `smux_v1` 的 `MuxConnection`，每条 RPC 一条子流 |
| MPTP | `mptp_smux` 的 `MptpClient` / `MptpSession` / `MptpServer` |
| RPC 客户端 | `ClientProxy` 只持有 `&MptpClient`，interface 由零大小 marker 提供 |
| RPC 服务端 | 运行期 `PathTrie`：interface 前缀 → 该 interface 内的 service 前缀 |
| 业务组织 | 每个 service 独立有界队列，worker 线程按 body enum 的方法变体分派 |
| 分派分配 | 服务端分派路径没有 `Pin<Box<dyn Future>>` |

`MptpServer` 现在把 `ChannelTx` / `ChannelRx` **按值**交给 handler；handler 把两个半边移进
队列，worker 线程负责写回复、半关闭 tx、drain rx。收尾顺序仍是：

```text
写回复 → 半关闭 tx → 读完 rx → 释放
```

---

## 3. 协议形状

```text
location = /{interface}/{service}
```

- interface：`api/v1`、`api/v2`；
- service：`workspace`、`session`；
- 方法名**不进 path**，而是 body enum 的变体。

```rust
enum WorkspaceCall {
    create { name: String },
    list,
    get { id: u64 },
    rename { id: u64, name: String },
    delete { id: u64 },
}
```

变体名直接使用函数名，字段名直接使用参数名；服务端解码后按变体调用具体方法。方法分派仍然
是 `match`，因为同一个 service path 下本来就有多个方法；interface / service 两级才走 Trie。

---

## 4. 用户 service trait 如何接上 router

service trait 在 [`src/protocol_.rs`](src/protocol_.rs) 里，是用户编写的第一等 API：

```rust
pub trait WorkspaceService: Send + Sync {
    const SERVICE_PATH: &'static str = K_WORKSPACE_SERVICE;

    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>>;
    // ...
}
```

客户端与服务端分别实现它：

```rust
// 客户端：代理实现 service trait，方法体走 rpc_call。
impl<I: RpcInterface + Send + Sync + 'static> WorkspaceService for ClientProxy<'_, I> {
    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        ClientCallAsync::new(self, Self::SERVICE_PATH, WorkspaceCall::create { name })
    }
}

// 服务端：用户业务实现。
impl WorkspaceService for WorkspaceServiceImpl {
    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>> {
        CreateWorkspaceImplAsync::new(self, name)
    }
}
```

router 的注册不是把裸 sender 硬塞进去，而是通过 worker 的 `route()`：

```rust
let workspace_worker = WorkspaceWorker::new(WorkspaceServiceImpl::new_(store), metrics);
let session_worker = SessionWorker::new(SessionServiceImpl::new_(store), metrics);

router.add_interface(
    ApiV1::PATH,
    vec![workspace_worker.route(), session_worker.route()],
);

workspace_worker.spawn();
session_worker.spawn();
```

`route()` 返回 `(S::SERVICE_PATH, Sender<RpcJob>)`，于是「用户实现 → worker → service path →
router 的 ServiceTrie」是一条显式装配链。

## 5. 文件

| 文件 | 作用 |
| --- | --- |
| `src/protocol_.rs` | interface / service 常量、workspace/session 类型、body enum |
| `src/trie_.rs` | 运行期路径 Trie（段边界、最长前缀） |
| `src/conn_.rs` | UDS + smux_v1 装配 |
| `src/body_.rs` | MessagePack body 编解码与 ring 读写 |
| `src/client_.rs` | `ClientProxy` 与手动 unary 调用 |
| `src/server_.rs` | 两级 Trie router、每 service 队列、worker 线程、内存 store |
| `src/cli.rs` | `server` / `client` 与 `--socket` |

---

## 6. 这个试验验证了什么 / 没验证什么

已验证：

- UDS 上可以建立 `smux_v1` 连接并跑 MPTP 会话；
- `ChannelTx` / `ChannelRx` 可以按值跨线程交给 worker；
- `MptpServer` 不再需要替 handler 收尾；
- 两级 `PathTrie` 能区分 `api/v1` / `api/v2`，且按 service path 分派；
- 每个 service 独立队列 + worker 线程可行；
- 服务端分派路径没有 boxed future；
- 同一条连接上连续 11 条 RPC 能完成 workspace/session 的完整 CRUD。

尚未验证：

- worker 处理错误如何回传给 `MptpServer` / 监控（当前只打印日志并关闭 channel）；
- 优雅关闭、队列满时的背压行为与 worker 数量调优；
- 流式 `Push` / `Pull` / suffix stream；
- job 级取消令牌尚未从 `MptpServer` 传到 worker（worker 目前用 `NonCancellableToken` 驱动 service future）；
- 宏生成版本（本 demo 是手写版本）。
