# dev-notes

本目录保留**与语言无关的 MPTP 协议综述**，以及围绕 RPC 框架重构的决策与计划记录：

- [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md)：通信格式（前缀、头表、
  定长 / 分块体、后缀流、会话顺序）与实现模型；
- [`rpc-framework-20261020-0332.md`](rpc-framework-20261020-0332.md)：`core_rpc_mptp_smux_v1`
  的重构计划、Phase 0 结果与待确认问题。
- [`rpc-preludes-20261022-2110.md`](rpc-preludes-20261022-2110.md)：`rpc_core_v1::preludes`
  的导出内容与后续文档补全清单。
- [`router-20261106-0112.md`](router-20261106-0112.md)：统一 router 的设计提案——
  可注入的 `TrRouter` 契约（默认实现参考 matchit、功能参考 salvo 的嵌套路由）；层级
  （level）与精确 route 的二级结构；绝对根 handler + 每层相对根 handler 的分层兜底
  （下层未命中不得上抛给绝对根）；运行期动态绑定；请求级 `RequestCancel`；以及
  「资源 = handle + route 绑定、不抽 `TrResource` trait」的资源模型；含 trait 设计与
  示例用法。初稿的两层 router / 静态动态双入口 / 空前缀祖先回退方案已被取代。
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
