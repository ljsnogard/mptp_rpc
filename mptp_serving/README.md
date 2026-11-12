# mptp_serving

MPTP 服务端框架：**端点、元数据、中间件与分层路由**。

它把「一次请求该交给哪个端点、端点携带什么元数据、中间件按元数据执行什么策略、终端处理器只
写业务」四件事分开；**不**重做会话驱动——一次请求的前缀 / 体 / 后缀顺序、半关闭与收尾仍由
[`mptp_smux`] 承担，本 crate 只负责「在会话之上如何路由与分派」。

## 分层位置

```text
应用 / RPC：service 实现、资源 handler（只写业务）
      ▲
mptp_serving：Endpoint / 元数据 / 中间件 / 分层 router（本 crate）
      ▲
mptp_smux：一次请求的会话驱动 + smux 绑定
      ▲
mptp_wire / abs_mptp / smux_v1
```

## 运行环境

`#![no_std]` + `alloc`。堆上对象带分配器参数（`Vec<T, A>`、`abs_str::String<A>`），或复用
[`matchit_route`] 的 `bumpalo` arena；不引入 `std::sync::OnceLock` / `RwLock` / `Mutex`。

## 当前状态（round 1 / P0）

- [`Endpoint`]：`pattern + MethodSet + 终端 handler`（元数据在 P2 补上）；
- [`TrEndpointHandler`]：对象安全的终端处理器（boxed future）；
- [`RouteParams`]：零拷贝路径参数（参数名借路由表、值借 `location`）；
- [`TrRouteMatcher`] + [`RadixRouter`]：分层匹配（handler 规则 → 子 router → 根 endpoint），
  复用 `matchit_route`，方法糖 `head/view/post/drop/push/pull/call`；
- [`RadixRouter`] 实现 `mptp_smux::serving::TrServeHandler`，可直接交给 `MptpServlet`；
- [`ReqHandlerTarget`]：旧 `abs_mptp::serving::TrReqHandler` 到 [`TrEndpointHandler`] 的
  适配器（仅适用于 `Send` 传输，如 tokio）。

**尚未实施**（后续 round）：运行期增删（`remove_route`，需 no_std 读多写少锁）、
`TrRouter` 注册契约与 `TrRouterExt` 扩展 trait、类型化路径参数（`TrRouteParams` + derive
编译期校验）、端点元数据 + 中间件链（P2）、旧 `rpc_core_v1::serving::Router` 的
`deprecated` 迁移。

## 设计文档

- [`dev-notes/mptp-serving-framework-20261112-1845.md`](../dev-notes/mptp-serving-framework-20261112-1845.md)
- [`dev-notes/mptp-api-plan-20261112-1850.md`](../dev-notes/mptp-api-plan-20261112-1850.md)

[`mptp_smux`]: ../mptp_smux
[`matchit_route`]: ../matchit_route
[`Endpoint`]: crate::Endpoint
[`TrEndpointHandler`]: crate::TrEndpointHandler
[`ReqHandlerTarget`]: crate::ReqHandlerTarget
[`RouteParams`]: crate::RouteParams
[`TrRouteMatcher`]: crate::TrRouteMatcher
[`RadixRouter`]: crate::RadixRouter
