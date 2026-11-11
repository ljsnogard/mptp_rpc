# dev-notes

本目录保留**与语言无关的 MPTP 协议综述**，以及围绕 RPC 框架重构的决策与计划记录：

- [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md)：通信格式（前缀、头表、
  定长 / 分块体、后缀流、会话顺序）与实现模型；
- [`rpc-framework-20261020-0332.md`](rpc-framework-20261020-0332.md)：`core_rpc_mptp_smux_v1`
  的重构计划、Phase 0 结果与待确认问题。
- [`rpc-preludes-20261022-2110.md`](rpc-preludes-20261022-2110.md)：`rpc_core_v1::preludes`
  的导出内容与后续文档补全清单。
- [`router-20261106-0112.md`](router-20261106-0112.md)：分层 router 的设计提案——
  可注入的注册契约 `TrRouter`（**添加规则只有两个入口**：绑定 handler / 绑定子 router）
  与匹配契约 `TrRouteMatcher`；每个 router 以自己为根的局部路由表，父层不枚举子 router
  内部规则；每个 router 自带根 handler（顶层绝对根 / 子 router 相对根，下层未命中不上抛）；
  七种访问方法用类型化方法糖（规则由参数类型 `P::PATTERN` 提供，derive 宏编译期校验
  `{id}` 与 handler 参数结构体字段一致）或 `MethodSet` 绑定；运行期动态增删；请求级
  `RequestCancel`；
  以及「资源 = handle + 绑定、不抽 `TrResource` trait」的资源模型；含 trait 设计与示例
  用法。初稿的两层 router / 全局单表 / level+interface+scope / 静态动态双入口等方案已被
  取代。
- [`mptp-smux-20261110-0730.md`](mptp-smux-20261110-0730.md)：`mptp_smux` 的职责
  边界与 README 依据：MPTP over `smux_v1` 的定位、与未来 `mptp_quic` 的共通点、
  当前各层使用方式、越界与缺口，以及 `client` / `server` feature 的目标划分。
- [`mptp-smux-refactor-20261111-0650.md`](mptp-smux-refactor-20261111-0650.md)：
  `mptp_smux` 的七种 AccessMethod 标准实现、服务端单一 `TrServeHandler` / `MptpServlet`、
  `SessionCore` 共享会话、`no_std` + `client` / `server` feature，以及 Router 迁出本 crate
  的设计与验收清单。
- [`matchit-no_std-20261110-0520.md`](matchit-no_std-20261110-0520.md)：`matchit_route`
  的 no_std / bumpalo 移植：router 自有 arena、`T: 'static`、`Send` / `Sync` 的安全
  论证，以及第 3 步删除 `rpc_router_trie`、统一为一套路径匹配算法的计划。
- [`denco-migration-20261103-0551.md`](denco-migration-20261103-0551.md)：`mptp_smux` /
  `abs_mptp_smux` 与下游 crate 适配新版 denco（默认分支 `9550d95`）与 abs_mm
  dev/0.3.0 的改动、依赖统一与验证结果。

历史设计笔记已移除；协议细节以这份综述为准。后续若需要新增调研或决策记录，仍按下面的命名
规范存放，但不应把综述中的规范内容拆散回多篇笔记。

## 文件命名规范

文件格式一律为 Markdown。

`[主题]-[日期]`，其中日期时间格式为 `YYYYMMDD-hhmm`。

- **模块类文档**：主题取模块名，例如 `connection-20261002-0548.md`；
- **非模块类文档**：主题取文档自身的性质或方向，用小写英文短语，例如
  `outlook-concurrency-20261002-2322.md`。
