# rpc_router_trie

`core_rpc_mptp_smux_v1` 的前缀树 router 实现。

## 职责

本 crate 只回答「一个 location 命中哪个 service」：

- interface 前缀在内部 `PathTrie` 中做最长段匹配；
- 命中的 interface 内部，再用剩余路径匹配 service 前缀；
- 命中的 `ServiceRoute<C>` 交给 router 的使用者。

拒绝回复、`RpcJob` 构造与入队由 core 的
`core_rpc_mptp_smux_v1::serving::RpcRouterHandler` 统一完成，因此本 crate 只依赖 core，
不直接依赖 `mptp_smux` / `abs_mptp`。

## 装配

```rust
use core_rpc_mptp_smux_v1::serving::RpcRouterHandler;
use rpc_router_trie::RpcRouter;

let mut workers = ApiV1::workers::<ServingCfg, _>(service_impl);
let mut router = RpcRouter::<ServingCfg>::new();
router.add_interface(ApiV1::PATH, workers.routes())?;

// 交给 MptpServer 的 handler；worker 仍由使用者选择线程 / 运行时驱动。
let handler = RpcRouterHandler::new(router);
mptp_router.add_target("", handler);
```

`RpcRouter::into_handler()` 是同一件事的简写。

## 匹配规则

- 只按完整路径段匹配，`api/v10` 不会误命中 `api/v1`；
- 首尾 `/`、重复 `/` 在注册与匹配时按同一套规则忽略；
- interface 与 service 都支持最长前缀；
- 同一个 interface 或同一个 interface 下的同一个 service 重复注册会返回
  `RpcRouterError`，且不会改动已有注册表。
