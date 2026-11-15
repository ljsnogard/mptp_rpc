# 场景参数演进记录：`mptp_smux` 不变量与业务变化范围

- 日期：2026-11-12 12:51
- 分支：`feat/bevy-scene-demo`
- 相关提交：`5fbac09`（字体 / 纹理 / 相机 / 光照）、`f1e837a`（行星参数广播）
- 相关文档：
  - [`mptp-smux-demo-20261111-1934.md`](mptp-smux-demo-20261111-1934.md)
  - [`../mptp_smux_v1_demo/README.md`](../mptp_smux_v1_demo/README.md)

## 0. 为什么要单独记录

这个 demo 的需求在很短时间内连续变了三次：

1. 只要求界面中文能显示；
2. 要求自转看得见，并实时调背景光 / 太阳光；
3. 要求调轨道半径、行星半径、颜色、纹理主题，而且修改要通过服务器广播给订阅者。

前两次基本只改客户端；第三次第一次真正扩张了**业务 payload**。这正好可以用来观察：

- 哪些东西在需求变化中保持不变，因而可以当作 `mptp_smux` 的契约；
- 哪些东西被刻意设计成可变，因而后续加参数时只需要改一处；
- 哪些变化最终会逼着协议或 API 演进。

本文记录的是**设计判断**，不是实现细节的复述。实现细节以代码与提交为准。

## 1. 提交时间线

| 提交 | 需求 | 业务 payload 变化 | `mptp_smux` 变化 |
| --- | --- | --- | --- |
| `5fbac09` | 中文 UI、行星纹理、双视角相机、可调光照 | 无；光照是客户端本地状态 | 无 |
| `f1e837a` | 轨道 / 行星半径、颜色、纹理主题广播 | `PlanetParams` 扩张；`PlanetDescriptor` 嵌套参数 | 无 |
| `40b8ea7` | 修正纹理 / 材质资产生命周期 | 无 | 无 |
| `c3690d0` | 颜色改为材质 tint，避免拖动时重建纹理 | 无；颜色语义不变 | 无 |
| 本次文档提交 | 记录不变量与变化范围 | 无 | 无 |

`f1e837a` 的 diff 里，`mptp_smux`、`mptp_wire`、`conn_.rs`、`cfg_.rs`、`server_.rs` 都没有
改动。这不是巧合，而是分层正确时的正常结果。

## 2. `mptp_smux` 的不变量

下列性质在三次需求变化中都没有变。它们不是「这个 demo 恰好这样实现」，而是
`mptp_smux` 对业务方的承诺 / 边界：

### 2.1 传输层不解释业务体

- `Call` 的请求体、`View` / `Head` 的回复体、`Pull` 的后缀流，对 `mptp_smux` 都是
  **不透明字节**；它只管 MPTP 前缀、头表、体边界、后缀半关闭与收尾。
- 因此 `PlanetParams` 加字段、换嵌套结构、改主题编号，都不需要改 `mptp_smux`。
- 反过来，业务层也不应该把「参数校验」「主题语义」塞进 `mptp_smux`。

### 2.2 后缀流是裸字节序列

- chunk 是 `SuffixStream::write_chunk_async` 的写入单位，不是帧边界；
- 一个 denco 对象可以拆成多个 chunk，多个对象也可以连续写；
- 接收端只按「自描述值序列」逐值解码；
- 心跳只是一个普通业务值，用来让写失败成为可观察的退订信号。

这条不变量让 `PlanetEvent` 的演进可以只发生在 denco 类型内部。

### 2.3 会话与取消

- 每条 `Pull` 是一个独立响应 / 后缀流，有自己的取消令牌；
- 客户端丢弃 `Response` / 取消任务即停止本地读取；
- 服务端通过写失败、队列断开或业务层退订提示清理订阅；
- `mptp_smux` 目前**没有**对称的「关闭回复读半边」公开入口，这是 demo 记录过的 API
  缺口，而不是本轮要偷偷补的洞。

### 2.4 单线程业务运行时

- 服务端固定一条 compio 业务线程；`BuffRead` / `BuffWrite` 是 `!Send`；
- 业务状态用 `Rc<RefCell<...>>`，不加锁；
- 每颗行星一个 bounded `flume` 队列，广播只 `try_send`，慢订阅者被移除。

最后一条是**应用层策略**，不是 `mptp_smux` 的语义；它随 demo 的压测需求变化，不属于
库的 API。

## 3. 业务契约中保持不变的部分

虽然 `PlanetParams` 扩张了，场景协议仍保持这些形状：

- `/scene/join` 每个连接只允许一次，返回一个完整的 `PlanetDescriptor`；
- `/scene/update` 以连接为身份，只修改自己的行星；未 join 返回 409；
- `/scene/planets` 的 `View` 返回 `PlanetDirectory`，`Head` 只回数量 / 目录版本；
- 每颗行星一条 `Pull`，第一帧必须是 `Snapshot`，之后是 `Updated` / `Removed` /
  `Heartbeat`；
- `Snapshot` / `Updated` 都携带**完整** `PlanetDescriptor`，`version` 每次更新递增；
- 目录版本只在行星加入 / 离开时变化，参数更新不改变目录版本；
- URI、404 / 405、退订清理、慢订阅者策略保持原样。

这些是业务层的「不变量」。它们足够稳定，所以客户端可以用同一套订阅 / 重连 / 重订阅
逻辑处理新增参数。

## 4. 被设计成可变的部分

### 4.1 `PlanetParams` 是唯一的可变参数集合

`f1e837a` 把原来散在 `PlanetDescriptor` / `UpdateRequest` / `UpdateAck` 里的字段收进
一个值对象：

```rust
pub struct PlanetParams {
    pub orbit_speed: f32,
    pub spin_speed: f32,
    pub orbit_radius: f32,
    pub planet_radius: f32,
    pub color: u32,
    pub texture_theme: u32,
}
```

`PlanetDescriptor` 只多一个 `params` 字段；`PlanetEvent` 的枚举形状不变。以后再加一个
「大气厚度」「轴倾角」：

1. 加字段到 `PlanetParams`；
2. 在 `Default` / `sanitized_` 里给缺省值与范围；
3. 客户端 UI 加控件；
4. `mptp_smux` 与事件包装都不动。

这是这次重构最重要的收益：**变化被局部化到一个值对象**。

### 4.2 支持范围与清洗位置

| 参数 | 范围 | 缺省 | 清洗 |
| --- | --- | --- | --- |
| `orbit_speed` | 0 – 3 rad/s | 0.6 | 有限值 clamp |
| `spin_speed` | 0 – 6 rad/s | 1.2 | 有限值 clamp |
| `orbit_radius` | 4 – 30 世界单位 | 6.0（分配时按 id 错开） | 有限值 clamp |
| `planet_radius` | 0.2 – 1.6 世界单位 | 0.8 | 有限值 clamp |
| `color` | 24 位 RGB | join 请求值 | 掩掉高位 |
| `texture_theme` | 开放 `u32` | 0（类地） | **不夹紧**，原样存储 |

两条设计判断：

- **连续量在服务端清洗，而不是相信客户端**。客户端 UI 的范围与服务端一致，但网络
  请求可以来自任意实现；服务端是权威。
- **非有限值回退到缺省值**，而不是拒绝整个更新。这样一次坏输入不会把行星卡死，也
  不需要为错误恢复写额外分支。
- **`orbit_radius` 与 `planet_radius` 的独立范围经过耦合检查**：太阳半径 2.2 +
  行星最大半径 1.6 < 轨道半径下限 4.0，所以任意合法组合都不会让行星埋进太阳。未来
  若放宽任一范围，这个约束必须变成显式的联合校验，而不能继续靠两个独立常量「碰巧」
  成立。

### 4.3 完整状态更新，不是字段补丁

`UpdateRequest { params: PlanetParams }` 提交完整参数，而不是 `Option<f32>` 补丁。
原因：

- `Updated` 事件本来就是完整 `PlanetDescriptor`，服务端与订阅者都按「最新快照覆盖」
  处理，语义一致；
- 新增字段时不需要设计「字段缺失 = 保持原值」的规则；
- 版本号单调递增，订阅者可以直接丢弃旧版本。

代价：

- 每次更新都要发送全部字段；当前 6 个字段很小，可以接受；
- 多客户端并发编辑同一颗行星时，最后写入者覆盖全部字段；当前只有 owner 能更新，
  所以没有冲突；
- 如果未来参数变多、更新频繁，应考虑**补丁 + 版本前置条件**，或按参数分组拆成多个
  资源 / 流。这属于新的 API，而不是在现有 `PlanetParams` 上打补丁。

### 4.4 纹理主题：线上开放、客户端闭集

- 线上 `texture_theme: u32`，服务端不解释语义，只存储 / 广播；
- 客户端 `PlanetTheme` 是闭集（类地 / 海洋 / 沙漠 / 冰封 / 熔岩 / 气态），未知编号
  回退到「类地」；
- 渲染根据主题生成不同程序化纹理；主题变化会重生成纹理并替换材质引用。

这是「开放 wire + 保守 client」的组合：

- 新增主题时，旧客户端至少能显示一个可辨认的默认主题，而不是解码失败；
- 服务端不需要为了主题新增而升级；
- 代价是主题编号没有 schema 约束，错误编号会被静默当成类地；如果主题数量继续增长，
  应该提供 `View /scene/themes` 之类的目录，而不是让客户端硬编码。

### 4.5 更新成本分级：颜色是 tint，主题是纹理

最初的纹理把行星主色烘进像素；颜色选择器连续拖动时，每一帧都会重新生成 512x256
纹理（debug 约 0.16 s/次），实时性不可接受。后来把纹理改成中性反照率图，颜色通过
`StandardMaterial::base_color` 乘上去：

- 颜色变化 = 改一个材质字段，几乎零成本；
- 主题变化 = 重新生成纹理，是低频操作；
- 半径变化 = 改 `Transform.scale`；
- 速度变化 = 只影响每帧轨道计算。

这不是纯性能优化，而是一条参数分类规则：**连续、高频、只影响着色的参数走材质
uniform；离散、改变数据结构的参数才值得重建资产。** 后续增加「大气颜色」「云量」
之类参数时，应先判断它属于哪一类，而不是一律塞进纹理生成。

### 4.5 哪些不是世界状态

这一轮也明确了「什么不该广播」：

- 相机模式、观察距离、离地高度；
- 背景光 / 太阳光强度；
- 字体、窗口布局。

它们是**本地视图状态**，不同客户端可以不同。把它们塞进 `PlanetParams` 会让业务
payload 变成客户端偏好设置的垃圾桶。判断标准：

> 如果另一个客户端看到的世界应该因此不同，它才是世界状态；否则是本地视图状态。

## 5. 代码影响地图

| 层 | 本轮是否改 | 说明 |
| --- | --- | --- |
| `mptp_smux` / `mptp_wire` | 否 | 不透明业务体；后缀流不变 |
| `conn_.rs` / `cfg_.rs` / `server_.rs` | 否 | 传输装配 / 线程模型不变 |
| `protocol_.rs` | 是 | `PlanetParams`、范围、清洗、主题编号 |
| `state_.rs` | 是 | 完整参数替换 + 版本递增 |
| `handler_.rs` | 是 | update 请求 → `PlanetParams`，广播完整 `Updated` |
| `client_.rs` / `net_.rs` | 是 | 命令与请求改为完整参数 |
| `scene-client.rs` / `surface_.rs` | 是 | 滑杆 / 颜色 / 主题、纹理重生成、网格缩放 |
| 测试 | 是 | 16 项集成测试 + 参数清洗 / 主题编号单元测试 |

这张表就是「业务变化被哪一层吸收」的证据：变化全部落在业务层。

## 6. 后续 API 设计的参考

### 6.1 可以直接沿用的模式

- 新增世界状态参数 → 扩展 `PlanetParams`，保持 `PlanetEvent` / `UpdateRequest` 包装；
- 新增本地视图参数 → 只改客户端，不进 wire；
- 新增有限枚举 → 优先「开放 wire 编号 + 客户端回退」，除非必须强一致；
- 所有连续量 → 服务端定义范围并清洗，客户端 UI 复用同一组常量；
- 外观重生成（纹理 / 材质 / 缩放）→ 原地更新，不重建实体，避免自转相位与订阅状态丢失；
- 高频着色参数（颜色等）→ 走材质 uniform，不要重生成纹理；离散结构参数（主题）才重建资产。

### 6.2 需要新 API 的情况

- **部分更新**：参数多到全量发送不划算，或需要「只改一个字段」的并发语义时，引入
  带版本前置条件的 `PlanetPatch`；不要改变现有全量请求的含义。
- **跨版本兼容**：denco 结构化编码不会自动忽略未知字段；如果必须让新旧客户端互操作，
  需要 schema 版本、字段 ID 或自描述 map，而不是继续依赖结构体布局。
- **主题目录**：主题增多后，客户端不应继续硬编码；需要服务端提供主题清单 / 能力协商。
- **高频更新**：滑杆拖动会逐帧提交。当前 bounded 队列 + `try_send` 会把慢订阅者移除；
  若要支持高刷新率场景，应在客户端合并同帧更新，或在服务端做 last-value-wins 合并，
  而不是单纯加大队列。
- **参数间约束**：本轮用独立范围保证「行星不埋进太阳」；一旦参数组合更多，需要显式
  的联合校验 / 约束求解，而不是继续加独立常量。

## 7. 本轮验证

| 检查 | 结果 |
| --- | --- |
| `cargo test -p mptp_smux_v1_demo` | 16 项集成测试 + 2 项参数 / 主题单元测试 + 1 项 doctest 通过 |
| `cargo test -p mptp_smux_v1_demo --features bevy-client` | 7 项 Bevy 侧单元测试通过 |
| `cargo clippy ... --all-targets -- -D warnings` | 通过 |
| `cargo run --bin scene-smoke` | 双客户端 e2e 通过，包含完整参数广播断言 |
| 真实窗口 | 拖动半径、颜色选择器、纹理主题后，本地行星经服务器广播更新 |

## 8. English summary

This note records how the scene demo's requirements evolved across three commits and which
parts stayed invariant. `mptp_smux` never changed: business bodies and suffix streams are
opaque bytes, chunk boundaries carry no semantics, and every planet still uses one
independent `Pull`. The scene protocol kept its event shape (Snapshot / Updated / Removed /
Heartbeat with full descriptors and monotonic versions), while all mutable world state was
grouped into one `PlanetParams` value object. Continuous values are clamped server-side;
texture themes use an open `u32` on the wire with a conservative client-side fallback.
Local view state (camera, lighting, font) is deliberately excluded from the broadcast.
High-frequency shading parameters such as color are applied as a material tint, while only
discrete structural parameters such as the texture theme rebuild an asset.
The note closes with the cases that would require new APIs: partial updates, cross-version
schema compatibility, theme catalogs, high-frequency update coalescing, and joint parameter
constraints.
