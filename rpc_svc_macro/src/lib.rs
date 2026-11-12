//! `rpc_svc_macro`：从 controller impl 块生成 MPTP RPC 的服务端处理器与客户端代理。
//!
//! 业务 crate 用 `#[rpc(interface = "...")]` 标注一个固有 impl 块，把每个方法声明成独立
//! 端点；宏据此生成：
//!
//! - 每个 unary 端点一个 `gen_may_cancel_future` step 函数 + 一个 `TrEndpointHandler`；
//! - 每个 `Pull` 端点一个流处理器（解码路径参数 → 调方法取迭代器 → 逐项写后缀流）；
//! - 一个 `install_into`，把这些处理器登记进 `mptp_serving::RadixRouter`；
//! - 一个类型化客户端代理 `{Self}Rpc`（unary 端点的方法糖）。
//!
//! ```
//! #![feature(impl_trait_in_assoc_type)]
//!
//! use rpc_svc_macro::rpc;
//!
//! pub struct Counter;
//!
//! #[rpc(interface = "demo")]
//! impl Counter {
//!     /// 拉取 0..n 的 id 序列。
//!     #[rpc(pull = "ids/{n}")]
//!     fn ids(&self, n: u64) -> impl rpc_core_v1::x_deps::abs_async_iter::TrAsyncIterator<Item = u64> {
//!         0..n
//!     }
//! }
//!
//! fn main() {}
//! ```
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
//! # `rpc_svc_macro`: serving handlers and a client proxy from a controller
//!
//! The macro turns a controller `impl` block into per-endpoint `TrEndpointHandler`s, a
//! `RadixRouter` registration helper, and a typed client proxy for the unary endpoints.

mod analyze_;
mod expand_;
mod model_;

use zyn::syn::{self, spanned::Spanned};

/// 把一个 controller 固有 impl 块展开成 RPC 服务端处理器与客户端代理。
///
/// - `#[rpc(interface = "...")]` 用于固有 impl 块；
/// - 每个端点方法用 `#[rpc(方法 = "路径")]` 声明访问方法与相对路径。
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
        match analyze_::parse_rpc_args_(&args)? {
            analyze_::RpcArgs_::Controller { interface } => {
                let syn::Item::Impl(item_impl) = &item else {
                    return Result::Err(syn::Error::new(
                        item.span(),
                        "`#[rpc(interface = \"...\")]` 只能用于固有 impl 块",
                    ));
                };
                let model = analyze_::analyze_controller_(item_impl, interface)?;
                expand_::expand_controller_(&model)
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
