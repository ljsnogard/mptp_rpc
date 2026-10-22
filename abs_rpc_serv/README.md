# abs_rpc_serv

RPC 服务端侧的**抽象契约**：供 `rpc_svc_macro` 生成的代码与 `rpc_core_v1` 的实现共同依赖，
不绑定具体运行时。

- `config::TrServingConfig` / `config::TrServingAllocConfig`：服务端配置；
- `service::TrRpcCallCtx`：一次调用的上下文；宏生成的 dispatch 只依赖它；
- `service::TrRpcService`：一个 service 实现如何被服务端调度；`#[rpc(service_of(I))]`
  为用户的 impl 类型实现它。

具体类型（`RpcCallCtx`、`RpcRouter`、`Router`）在 `rpc_core_v1` / `mptp_smux`；本 crate
不反向依赖它们。
