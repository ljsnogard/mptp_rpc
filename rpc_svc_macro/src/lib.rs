//! `rpc_svc_macro`：从 service trait / interface marker 生成 MPTP RPC 的双端样板。
//!
//! 一个协议 crate 通常写两段：
//!
//! ```
//! #![feature(impl_trait_in_assoc_type)]
//!
//! use abs_cancel::TrMayCancel;
//! use rpc_core_v1::messaging::RpcResult;
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
//! - trait 里多出一个隐藏的默认方法 `__rpc_dispatch_async`，它转到隐藏模块里的 dispatch；
//! - 隐藏模块里的一个 **owned body enum** `Call`：变体名 = 方法名，字段名 = 参数名；
//! - 为统一的 `ClientProxy` 实现该 service trait：每个方法把参数转成 owned 字段，
//!   构造 body enum 变体并调用通用 unary 调用；
//! - 服务端 dispatch：解码 `Call`、`match` 方法变体、调用用户实现并写回复。
//!
//! 宏**不再生成 `Worker`、队列或 `run_async`**；服务端如何组织 service 完全由使用者决定。
//!
//! # interface 宏展开后
//!
//! - `impl TrRpcInterface for ApiV1`，`PATH = "api/v1"`；
//! - `impl ServiceSelector<C> for ApiV1`，客户端可用 `<ApiV1 as ServiceSelector<C>>::select`
//!   或 `client.call::<ApiV1>()`；
//! - 不再生成 worker 组或 `routes()`。
//!
//! # 服务端：用户自己组织
//!
//! 服务端实现先写普通 impl，再用 `#[rpc(service_of(ApiV1))]` 与 interface 关联：
//!
//! ```
//! #![feature(impl_trait_in_assoc_type)]
//!
//! use abs_cancel::{TrCancellationToken, TrMayCancel};
//! use gen_mcf2::gen_may_cancel_future;
//! use rpc_core_v1::messaging::RpcResult;
//! use rpc_svc_macro::rpc;
//!
//! #[rpc(service = "system")]
//! pub trait SystemService {
//!     fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<bool>>;
//! }
//!
//! #[rpc(path = "api/v1", services = [SystemService])]
//! pub struct ApiV1;
//!
//! pub struct SystemImpl;
//!
//! /// 示意用的立即完成 future。
//! #[allow(clippy::needless_lifetimes)]
//! #[gen_may_cancel_future(Ready, pub(crate), new(pub(crate)))]
//! async fn ready_<'f, T, Tok>(value: RpcResult<T>, cancel: Tok) -> RpcResult<T>
//! where
//!     T: 'f,
//!     Tok: TrCancellationToken,
//! {
//!     let _ = cancel;
//!     value
//! }
//!
//! #[rpc(service_of(ApiV1))]
//! impl SystemService for SystemImpl {
//!     fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<bool>> {
//!         ReadyAsync::<'_, '_, bool>::new(Ok(true))
//!     }
//! }
//!
//! fn build<C: rpc_core_v1::serving::TrServingConfig + 'static>() {
//!     use rpc_core_v1::serving::RpcRouter;
//!     use rpc_core_v1::x_deps::mptp_smux::serving::RouterCtx;
//!
//!     let ctx = RouterCtx::<C>::new();
//!     let _router = RpcRouter::<ApiV1>::new(&ctx)
//!         .add_service::<SystemImpl>(|_| SystemImpl);
//! }
//!
//! fn main() {}
//! ```
//!
//! 顶层用 `mptp_smux::serving::Router` 按 interface 前缀登记 `RpcRouter`，再交给
//! `MptpServer`。`RpcRouter::add_service` 只接受实现了 `abs_rpc_serv::service::TrRpcService`
//! 的类型；`#[rpc(service_of(...))]` 正是为用户的 impl 类型补上这条实现。
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
//! - interface marker 必须是单元结构体；`services` 里写 service trait 的路径。`services`
//!   仍会被解析与校验，但不再据此生成 worker；
//! - `service_of(Interface)` 的 impl 块暂不支持泛型参数与 where 子句；一个 impl 类型在
//!   **同一个 interface** 下只能对应一个 service trait（不同 interface 可以各有一条实现）；
//! - 生成代码会调用 `gen_mcf2`，协议 crate 需要打开
//!   `#![feature(impl_trait_in_assoc_type)]`。
//!
//! # `rpc_svc_macro`: generate both sides without organizing the server
//!
//! The service macro generates the trait constant, one owned body enum, the client proxy
//! impl, the serving dispatch and a hidden default dispatch method. The interface macro only
//! generates the interface contract and client selector. Server-side organization is left to
//! the user: `#[rpc(service_of(I))]` adds the `TrRpcService` impl, and
//! `RpcRouter::add_service` registers services explicitly.

mod analyze_;
mod expand_;
mod model_;

use zyn::syn::{self, spanned::Spanned};

/// 把一个 service trait、interface marker 或服务端 impl 块展开成 RPC 样板。
///
/// - `#[rpc(service = "...")]` 用于 service trait；
/// - `#[rpc(path = "...", services = [...])]` 用于 unit struct 形式的 interface marker；
/// - `#[rpc(service_of(ApiV1))]` 用于服务端 trait impl 块，补上 `TrRpcService` 实现。
///
/// # Examples
///
/// ```
/// #![feature(impl_trait_in_assoc_type)]
///
/// use abs_cancel::TrMayCancel;
/// use rpc_core_v1::messaging::{RpcResult, TrRpcInterface};
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
            analyze_::RpcArgs_::ServiceOf { interface } => {
                let syn::Item::Impl(item_impl) = &item else {
                    return Result::Err(syn::Error::new(
                        item.span(),
                        "`#[rpc(service_of(...))]` 只能用于 trait impl 块",
                    ));
                };
                let model = analyze_::analyze_service_of_(item_impl, interface)?;
                expand_::expand_service_of_(&model)
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
