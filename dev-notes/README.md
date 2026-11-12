# dev-notes

本目录保留**与语言无关的 MPTP 协议综述**，以及围绕 RPC 框架重构的决策与计划记录：

- [`mptp-protocol-20261020-0200.md`](mptp-protocol-20261020-0200.md)：通信格式（前缀、头表、
  定长 / 分块体、后缀流、会话顺序）与实现模型；
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
- [`mptp-smux-demo-20261111-1934.md`](mptp-smux-demo-20261111-1934.md)：
  `mptp_smux_v1_demo` 重写为 Bevy 太阳系场景的规划与实施记录：客户端 / 服务端直接
  MPTP，`abs_art-facade` + compio，每进程最多两个业务线程，Bevy 客户端 feature-gated，
  `bevy_egui` 滑杆，denco 编解码，每客户端一条 MPTP 连接；订阅模型为 `View` 目录发现 +
  每行星 `Pull` 数据流，支持选择性订阅 / 退订 / 重新订阅；后缀流按 chunk 无限写入。
  阶段 A 的 `SuffixStream` 与 16 项无 Bevy 集成测试已实施；记录了两条 smux 行为缺口
  （丢弃 `Response` 不可观察、对端消失时 `income_async` 不一定报错）及 demo 的业务层
  对策。2026-11-12 追加中文字体、行星纹理、双视角相机、可调光照与 `PlanetParams`
  广播的实施记录。
- [`scene-param-evolution-20261112-1251.md`](scene-param-evolution-20261112-1251.md)：
  以 demo 的需求变化为例，记录 `mptp_smux` 的传输不变量、场景业务契约的不变量，以及
  `PlanetParams` 的可变范围、服务端清洗、开放纹理主题编号和后续需要新 API 的情形。
- [`denco-migration-20261103-0551.md`](denco-migration-20261103-0551.md)：`mptp_smux` /
  `abs_mptp_smux` 与下游 crate 适配新版 denco（默认分支 `9550d95`）与 abs_mm
  dev/0.3.0 的改动、依赖统一与验证结果。
- [`mptp-serving-framework-20261112-1845.md`](mptp-serving-framework-20261112-1845.md)：
  MPTP 服务端框架设计：借鉴 ASP.NET Core 的四层关注点分离（路由只匹配、端点携带元数据、
  中间件按元数据执行策略、处理器只关心业务），用 Salvo 式基于路由规则的路径树组织功能；
  明确不用 Salvo 的 Depot（类型擦除状态袋）与 Tower 的 `Service`/`Layer` 类型体操。
- [`mptp-api-plan-20261112-1850.md`](mptp-api-plan-20261112-1850.md)：
  MPTP/SMUX API 优化计划：客户端行为收敛到「方法糖 + 类型状态 builder + 显式半关闭 /
  取消 / 超时」，服务端升级为「端点 + 元数据 + 中间件 + 处理器」，并把 QUIC 做成
  `abs_smux` 的第二个实现（`abs_mptp` 继续依赖 `abs_smux`，不抽 `abs_mux`）；含 P0–P4
  分阶段与公开 API 变更清单。
- [`integration-decisions-20261112-2045.md`](integration-decisions-20261112-2045.md)：
  集成阶段的设计决策：取消令牌进 `MptpContext` 引入 `K: Clone` 公开约束（暂缓待确认）、
  框架保持 `Send`-by-design（compio 需非 `Send` 变体）、`MptpConnection` 属 `mptp_smux`
  绑定层、demo 迁移按 tokio 先行 / compio 待后。
- [`rpc-svc-macro-mptp-serving-20261112-2130.md`](rpc-svc-macro-mptp-serving-20261112-2130.md)：
  `rpc_svc_macro` 面向 `mptp_serving` 的改造方向（设计稿）：从「service trait + interface
  marker + body enum 分派」改成「每个方法 = 一个 `Endpoint`」的 C# Attribute 形状（方法 +
  路径 + 元数据 + 参数来源），宏直接落在 `RadixRouter` / `Endpoint` / `EndpointMetadata` /
  `TrEndpointHandler` 上，退役 `RpcRouter` / `TrRpcService` / `TrRpcCallCtx`；含三个框架侧
  前置缺口、理想语法、场景 demo 前后对照与 P0–P3 分阶段。

历史设计笔记已移除；协议细节以这份综述为准。后续若需要新增调研或决策记录，仍按下面的命名
规范存放，但不应把综述中的规范内容拆散回多篇笔记。

## 文件命名规范

文件格式一律为 Markdown。

`[主题]-[日期]`，其中日期时间格式为 `YYYYMMDD-hhmm`。

- **模块类文档**：主题取模块名，例如 `connection-20261002-0548.md`；
- **非模块类文档**：主题取文档自身的性质或方向，用小写英文短语，例如
  `outlook-concurrency-20261002-2322.md`。
