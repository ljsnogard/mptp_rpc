//! `rpc_svc_macro`：从 service trait / interface marker 生成 MPTP RPC 的双端样板。
//!
//! 一个协议 crate 通常写两段：
//!
//! ```
//! #![feature(impl_trait_in_assoc_type)]
//!
//! use abs_cancel::TrMayCancel;
//! use core_rpc_mptp_smux_v1::RpcResult;
//! use rpc_svc_macro::rpc;
//!
//! #[derive(denco_macro::TrEncodable, denco_macro::TrDecodable)]
//! pub struct GoodsInfo {
//!     pub id: u64,
//! }
//!
//! #[rpc(service = "customer/goods")]
//! pub trait GoodsCustomerService {
//!     fn find<'f>(
//!         &'f self,
//!         name: &'f str,
//!     ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<GoodsInfo>>>;
//! }
//!
//! #[rpc(path = "api/v1", services = [GoodsCustomerService])]
//! pub struct ApiV1;
//!
//! fn main() {}
//! ```
//!
//! # service 宏展开后
//!
//! - trait 里多出 `const SERVICE_PATH: &'static str = "customer/goods";`；
//! - 隐藏模块里的一个 **owned body enum** `Call`：变体名 = 方法名，字段名 = 参数名；
//! - 为统一的 `ClientProxy` 实现该 service trait：每个方法把参数转成 owned 字段，
//!   构造 body enum 变体并调用通用 unary 调用；
//! - 服务端 dispatch：解码 `Call`、`match` 方法变体、调用用户实现并写回复；
//! - 服务端 `Worker<S, C>`：一个 service 一条有界队列；`route()` 交出
//!   `ServiceRoute<C>`，`run_async()` 循环「取 job → dispatch → 收尾」。worker 不 spawn、
//!   不选运行时。
//!
//! # interface 宏展开后
//!
//! - `impl TrRpcInterface for ApiV1`，`PATH = "api/v1"`；
//! - `impl ServiceSelector<C> for ApiV1`，客户端可用 `<ApiV1 as ServiceSelector<C>>::select`
//!   或 `client.call::<ApiV1>()`；
//! - `ApiV1::workers(service_0, ...)`：按 `services` 顺序建立每个 service 的 worker，
//!   返回一个具体类型的 worker 组；
//! - worker 组的 `routes()` 交给 `RpcRouter::add_interface`，`run_async()` 并发驱动全部
//!   worker（内部是 `try_join` 树，任一 worker 出错时整体提前结束）。
//!
//! 服务端装配的形状（`ServingCfg` 是使用者的 `TrServingConfig` 实现）：
//!
//! ```text
//! let mut workers = ApiV1::workers::<ServingCfg, _>(goods_impl);
//! let mut router = RpcRouter::<ServingCfg>::new();
//! router.add_interface(ApiV1::PATH, workers.routes())?;
//! // 由使用者选择线程 / 运行时：
//! spawn(workers.run_async().may_cancel_with(server_token));
//! ```
//!
//! # 使用约束
//!
//! - service trait 必须是非泛型、非 unsafe 的普通 trait；
//! - 方法必须是 `&self` 方法，不能是 `async fn`，返回
//!   `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`；
//! - 参数支持按值类型，以及 `&str` / `&[u8]` / `&String` / `&Vec<u8>` / `&Copy` 标量；
//!   其它 `&T`、`&mut T`、泛型方法与嵌套借用会被拒绝；
//! - **body enum 是 owned 的**：`denco_macro` 当前不支持带生命周期参数的派生类型，
//!   所以客户端与服务端共用一个 owned enum；借用参数在客户端被转成 owned 字段；
//! - 变体顺序由方法声明顺序决定；**插入 / 重排方法会改变线上兼容性**，在稳定协议上
//!   应只在末尾追加方法；
//! - interface marker 必须是单元结构体；`services` 里写 service trait 的路径。trait 与
//!   interface 不在同一模块时请写完整路径（例如 `super::goods::GoodsCustomerService`），
//!   因为 worker 类型名由 service 宏生成在 trait 所在模块里；
//! - `ApiV1::workers` 的配置类型 `C` 通常由后续 `RpcRouter` 的使用推导；推导失败时用
//!   `ApiV1::workers::<ServingCfg, _, _>(...)` 显式指定；
//! - 生成代码会调用 `gen_mcf2`，协议 crate 需要打开
//!   `#![feature(impl_trait_in_assoc_type)]`。worker 的取消监听要求取消令牌实现
//!   `TrCancellationToken`；`NonCancellableToken` 与 `cantok_rs` 的令牌都满足。
//!
//! # `rpc_svc_macro`: generate both sides of an MPTP RPC service
//!
//! The crate implements the service half (trait constant, one owned body enum, client proxy
//! impl, serving dispatch, per-service worker) and the interface half (`TrRpcInterface`,
//! `ServiceSelector`, `ApiV1::workers` and the worker group). Method order is wire-visible;
//! append new methods at the end. Workers never spawn a thread or pick a runtime.

mod analyze_;
mod expand_;
mod model_;

use zyn::syn::{self, spanned::Spanned};

/// 把一个 service trait 或 interface marker 展开成 RPC 双端样板。
///
/// - `#[rpc(service = "...")]` 用于 service trait；
/// - `#[rpc(path = "...", services = [...])]` 用于 unit struct 形式的 interface marker。
///
/// # Examples
///
/// ```
/// #![feature(impl_trait_in_assoc_type)]
///
/// use abs_cancel::TrMayCancel;
/// use core_rpc_mptp_smux_v1::{RpcResult, TrRpcInterface};
/// use rpc_svc_macro::rpc;
///
/// #[derive(denco_macro::TrEncodable, denco_macro::TrDecodable)]
/// pub struct Ping {
///     pub ok: bool,
/// }
///
/// #[rpc(service = "system")]
/// pub trait SystemService {
///     fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Ping>>;
/// }
///
/// #[rpc(path = "api/v1", services = [SystemService])]
/// pub struct ApiV1;
///
/// fn service_path<S: SystemService>() -> &'static str {
///     S::SERVICE_PATH
/// }
///
/// fn interface_path() -> &'static str {
///     ApiV1::PATH
/// }
///
/// fn main() {}
/// ```
///
/// # Panics
///
/// 宏本身不 panic；输入不合法时通过编译器错误报出。`#[zyn(input)]` 的解析失败由 zyn
/// 转成编译错误。
#[zyn::attribute]
fn rpc(#[zyn(input)] item: syn::Item, args: zyn::Args) -> zyn::TokenStream {
    let result = (|| -> syn::Result<zyn::Output> {
        match analyze_::parse_rpc_args_(&args)? {
            analyze_::RpcArgs_::Service { path } => {
                let syn::Item::Trait(item_trait) = &item else {
                    return Result::Err(syn::Error::new(
                        item.span(),
                        "`#[rpc(service = \"...\")]` 只能用于 trait",
                    ));
                };
                let model = analyze_::analyze_service_(item_trait, path)?;
                expand_::expand_service_(&model)
            }
            analyze_::RpcArgs_::Interface { path, services } => {
                let syn::Item::Struct(item_struct) = &item else {
                    return Result::Err(syn::Error::new(
                        item.span(),
                        "`#[rpc(path = \"...\", services = [...])]` 只能用于单元结构体",
                    ));
                };
                let model = analyze_::analyze_interface_(item_struct, path, services)?;
                expand_::expand_interface_(&model)
            }
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
