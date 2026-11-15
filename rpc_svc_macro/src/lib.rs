//! `rpc_svc_macro`：把「一段服务端业务代码」展开成 MPTP RPC 的端点处理器与客户端代理。
//!
//! 本 crate 只提供一个属性宏 [`rpc`]，但支持**多种组织服务端代码的写法**，开发者按「有无
//! 状态 / 要不要客户端代理 / 是否流式 / 是否手控传输 / 是不是 RPC 业务方法」来选择。完整
//! 取舍见下文[「组织服务端代码的方式」](#组织服务端代码的方式)与[「如何选择」](#如何选择)。
//!
//! 宏据此生成：
//!
//! - 每个 unary 端点一个 `gen_may_cancel_future` step 函数 + 一个 `TrEndpointHandler`；
//! - 每个 `Pull` 端点一个流处理器（解码路径参数 → 调方法取迭代器 → 逐项写后缀流）；
//! - 一个 `install_into`，把这些处理器登记进 `mptp_serving::RadixRouter`；
//! - controller 形态另生成一个类型化客户端代理 `{Self}Rpc`（unary 端点的方法糖）；
//! - Call service trait 形态生成 marker、请求枚举、方法级类型头、`ClientProxy` 的 trait
//!   impl；对应的 `impl_of` 生成 `CALL /<prefix>` service dispatcher 与 installer。
//!
//! # 组织服务端代码的方式
//!
//! 下面六种方式从「最省事」到「最可控」排列。前五种由本宏生成单个端点，第六种用 trait
//! 定义 Call service 的业务形状。
//!
//! ## 1. controller：有状态、多端点、带客户端代理（最常用）
//!
//! 一个固有 impl 块就是一个 service；每个带 `#[rpc(方法 = "路径")]` 的方法是一个端点，
//! 相对路径拼在 `interface` 前缀之后。状态放进 struct，服务端用
//! `install_into(router, Arc::new(svc))` 登记，同一份 `Arc<Self>` 被所有端点共享：
//!
//! ```text
//! #[rpc(interface = "scene")]
//! impl SceneService {
//!     /// Call /scene/join
//!     #[rpc(call = "join")]
//!     async fn join(&self, req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }
//!
//!     /// Pull /scene/planets/{id}（流式）
//!     #[rpc(pull = "planets/{id}")]
//!     fn planets(&self, id: u64) -> impl TrAsyncIterator<Item = PlanetEvent> { ... }
//! }
//! ```
//!
//! ## 2. 自由函数：无状态、单端点、无客户端代理
//!
//! 不需要 `self` 时直接写自由 `async fn`，路径取完整值（没有 `interface` 前缀）：
//!
//! ```text
//! /// Call /scene/ping
//! #[rpc(call = "scene/ping")]
//! async fn ping(req: JoinRequest) -> Result<PlanetDescriptor, SceneError> { ... }
//! ```
//!
//! 生成无状态的单元 handler 与 `install_into(router)`（不接 `Arc<Self>`）。
//!
//! ## 3. 尾参取消令牌：两种写法都能叠加
//!
//! unary 端点可显式接住请求级取消令牌（`gen_may_cancel_future` 的形状）：
//!
//! ```text
//! #[rpc(call = "join")]
//! async fn join<K>(&self, req: JoinRequest, cancel: K) -> Result<PlanetDescriptor, SceneError>
//! where K: abs_cancel::TrCancellationToken { ... }   // 自由函数同理
//! ```
//!
//! `K` 必须是**唯一的泛型类型参数**、`cancel` 必须是**最后一个参数**；不写时宏自动注入
//! `__C`。`cancel` 在运行时即请求级子 token，可直接 `child_token()` / 交给
//! `may_cancel_with`。
//!
//! ## 4. Pull 流端点：后缀流
//!
//! `Pull` 是普通 `fn`，返回 `impl TrAsyncIterator<Item = T, Err = E>`；宏解码路径参数后
//! 逐项把 item 写进回复后缀流。controller 方法与自由函数都支持。
//!
//! ## 5. 手写 `TrEndpointHandler`：完全控制（逃生口）
//!
//! 需要读请求后缀、手改头表、`take_parts` 拿走两半自己收尾等超出本宏范围的能力时，直接
//! 在 `mptp_serving` 层 `impl TrEndpointHandler` 并用 `router.bind_endpoint` 登记——宏生成的
//! 端点与手写端点可以混挂在同一个 router 上。参考 `mptp_smux_v1_demo` 的 `ScenePlanets` /
//! `ScenePull` / `SceneRoot`。
//!
//! ## 6. Call service trait：code-first 定义 RPC 业务形状
//!
//! Call 是 RPC 语义：一个 service 只占一条 `CALL /<prefix>` 路径，方法名放在请求枚举的
//! 变体里。协议 crate 用 trait 声明业务形状，服务端 crate 用 `impl_of` 声明实现：
//!
//! ```text
//! // 协议 crate
//! #[rpc(service = "im/v1/room", api = RoomSvc)]
//! pub trait RoomService {
//!     fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>>;
//!     fn send_message(
//!         &self,
//!         room_id: RoomId,
//!         text: &str,
//!         client_msg_id: u64,
//!     ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>>;
//! }
//!
//! // 服务端 crate
//! #[rpc(impl_of(RoomSvc))]
//! impl RoomService for RoomServiceImpl {
//!     fn list_my_rooms(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<RoomSummary>>> { ... }
//!     fn send_message(
//!         &self,
//!         room_id: RoomId,
//!         text: &str,
//!         client_msg_id: u64,
//!     ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Message>> { ... }
//! }
//! ```
//!
//! trait 宏生成：
//!
//! - 零大小 marker（`RoomSvc`）与 `TrRpcInterface::PREFIX`；
//! - 每方法隐藏关联类型 `__Req_<operation>`（载荷类型）与 `__Call`（请求枚举）；
//! - 多参方法的 `__Req_*` 位置字段结构体（参数名不进线上契约）；
//! - 请求枚举 `RoomServiceCall`：0 参 unit、1 参 newtype、≥2 参 `__Req_*` newtype；
//! - 每方法 `RpcDataTypeInfo`、`RpcServiceInfo` 方法表；
//! - `ClientProxy` 的 trait impl：调用点只写业务方法，不出现 path / operation / 编码。
//!
//! `impl_of` 宏生成一个 `CALL /<prefix>` endpoint、一个方法表与一个 dispatcher：先解请求
//! 枚举，再按方法级 `RpcDataTypeInfo` 校验类型头，最后调业务 impl；业务 impl 原样保留。
//! 装配点用 `__rpc_impl_of_<Impl>_via_<Marker>::install_into(router, Arc::new(impl))`。
//!
//! 约束：
//!
//! - 方法必须 `&self`、无泛型、无默认实现，返回
//!   `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`；
//! - 参数须能转成 owned 载荷：owned 类型，或白名单借用（`&str` / `&[u8]` / `&String` /
//!   `&Vec<u8>` / `&Copy` 标量）；
//! - 协议 crate 需要 `#![feature(associated_type_defaults)]`（隐藏关联类型带默认值），
//!   并打开 `rpc_core_v1` 的 `type-id` feature（方法级 `Data_Type_Id` 来自 denco 指纹）；
//! - 生成的载荷结构体 derive denco，因此调用方需要直接依赖 `abs_denco`。
//!
//! # 如何选择
//!
//! | 需求 | 选哪种 |
//! | --- | --- |
//! | 有状态、多端点、想要类型化客户端代理 | controller（方式 1） |
//! | 无状态、单端点、不关心客户端代理 | 自由函数（方式 2） |
//! | 需要请求级取消 | 在 1 或 2 上加尾参令牌（方式 3） |
//! | 回复是持续流（每行星推送、订阅流） | Pull 端点（方式 4） |
//! | 读请求后缀、手改头表、手控收尾等 | 手写 `TrEndpointHandler`（方式 5） |
//! | RPC 业务方法（一个 service 一条路径 + 方法表） | Call service trait + `impl_of`（方式 6） |
//!
//! # 闭包为什么不支持
//!
//! `mptp_serving` 的 [`TrEndpointHandler`] 为了支撑洋葱中间件，把「对上下文的借用 `'a`」与
//! 「上下文内部借用 `'f`」拆成两个独立生命周期；而 `MptpContext` 内部 `&mut` 对 `'f` 不变，
//! 任何「闭包返回借用 ctx 的 future」都无法满足 `Fn` 的固定输出类型（高阶 return type 在
//! Rust 里不可写）。因此**闭包 / `bind_handler!` 这类写法不做**；用自由函数覆盖「无捕获」，
//! 用 controller 的 `Arc<Self>` 覆盖「有状态」。
//!
//! # 使用约束
//!
//! - controller 是固有 impl 块，暂不支持泛型 impl 或 impl 级 where 子句；
//! - 每个端点方法用 `#[rpc(方法 = "路径")]` 声明访问方法与相对路径，支持
//!   call / view / head / post / drop / push / pull；
//! - unary 端点是 `async fn`，返回 `Result<T, E>`（`E` 实现 `TrReplyError`）；
//! - `Pull` 端点是普通 `fn`，返回 `impl TrAsyncIterator<Item = T, Err = E>`；
//! - 生成代码会调用 `gen_mcf2`，协议 crate 需要打开
//!   `#![feature(impl_trait_in_assoc_type)]`。
//!
//! # `rpc_svc_macro`: serving handlers and a client proxy
//!
//! The macro turns a controller `impl` block (or a free `async fn`) into per-endpoint
//! `TrEndpointHandler`s, a `RadixRouter` registration helper, and — for controllers — a typed
//! client proxy. Unary endpoints may take the request cancel token as a trailing parameter.
//! Closures are deliberately unsupported (see above); free functions and controller methods
//! cover the ergonomic cases, and hand-written `TrEndpointHandler` covers full control.

mod analyze_;
mod expand_;
mod model_;

use zyn::syn::{self, spanned::Spanned};

/// 把一个 RPC 业务形状展开成服务端处理器与客户端代理。
///
/// 支持四种输入：
///
/// - `#[rpc(service = "...", api = X)]` 用于 **trait**：生成 marker、`PREFIX`、请求枚举、
///   方法级类型头与 `ClientProxy` 的 trait impl（code-first Call）；
/// - `#[rpc(impl_of(X))]` 用于 **trait impl**：生成 service dispatcher、方法表、
///   `CALL /<prefix>` endpoint 与 `install_into`；
/// - `#[rpc(interface = "...")]` 用于**固有 impl 块**，每个端点方法用
///   `#[rpc(方法 = "路径")]` 声明访问方法与相对路径（controller）；
/// - `#[rpc(方法 = "full/path")]` 用于自由 `async fn`，路径取完整值、无客户端代理。
///
/// # Examples
///
/// ```
/// #![feature(impl_trait_in_assoc_type)]
///
/// use rpc_svc_macro::rpc;
///
/// pub struct Counter;
///
/// #[rpc(interface = "demo")]
/// impl Counter {
///     /// 拉取 0..n 的 id 序列。
///     #[rpc(pull = "ids/{n}")]
///     fn ids(&self, n: u64) -> impl rpc_core_v1::x_deps::abs_async_iter::TrAsyncIterator<Item = u64> {
///         0..n
///     }
/// }
///
/// fn main() {}
/// ```
///
/// Call service trait / `impl_of` 的完整形状见 crate 文档的「方式 6」。它们要求协议 crate
/// 打开 `#![feature(associated_type_defaults)]`，因此这里不放可运行示例（本 crate 的
/// 集成测试 `tests/service_macro.rs` 覆盖该形状）。
///
/// # Panics
///
/// 宏本身不 panic；输入不合法时通过编译器错误报出。`#[zyn(input)]` 的解析失败由 zyn
/// 转成编译错误。
///
/// # Errors
///
/// 输入不合法时，宏保留原 item 并附加一条指向具体位置的 `compile_error!`，让编译器把
/// 错误指回用户代码。
#[zyn::attribute]
fn rpc(#[zyn(input)] item: syn::Item, args: zyn::Args) -> zyn::TokenStream {
    let result = (|| -> syn::Result<zyn::Output> {
        match &item {
            syn::Item::Trait(item_trait) => {
                let analyze_::RpcArgs_::Service { prefix, api } = analyze_::parse_rpc_args_(&args)?
                else {
                    return Result::Err(syn::Error::new(
                        item_trait.ident.span(),
                        "`#[rpc(service = \"...\", api = X)]` 用于 Call service trait；\
                         controller 用 `#[rpc(interface = \"...\")]` + 固有 impl 块",
                    ));
                };
                let model = analyze_::analyze_service_(item_trait, prefix, api)?;
                expand_::expand_service_(&model)
            }
            syn::Item::Impl(item_impl) => match analyze_::parse_rpc_args_(&args)? {
                analyze_::RpcArgs_::ImplOf { api } => {
                    let model = analyze_::analyze_impl_of_(item_impl, api)?;
                    expand_::expand_impl_of_(&model)
                }
                analyze_::RpcArgs_::Controller { interface } => {
                    let model = analyze_::analyze_controller_(item_impl, interface)?;
                    expand_::expand_controller_(&model)
                }
                analyze_::RpcArgs_::Service { prefix, .. } => Result::Err(syn::Error::new(
                    item_impl.self_ty.span(),
                    format!(
                        "`service = \"{prefix}\"` 用于 trait 定义；trait impl 用 `#[rpc(impl_of(X))]`"
                    ),
                )),
            },
            syn::Item::Fn(item_fn) => {
                let (method, path) = analyze_::parse_free_fn_args_(&args)?;
                let model = analyze_::analyze_free_fn_(item_fn, method, path)?;
                expand_::expand_free_fn_(&model)
            }
            other => Result::Err(syn::Error::new(
                other.span(),
                "`#[rpc]` 只能用于 trait（Call service）、固有 impl 块（controller）、\
                 trait impl 块（`impl_of`）或自由 async 函数",
            )),
        }
    })();

    match result {
        Result::Ok(tokens) => tokens,
        Result::Err(err) => {
            let error = err.to_compile_error();
            zyn::zyn! {
                {{ item }}
                {{ error }}
            }
        }
    }
}
