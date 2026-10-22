//! `#[rpc]` 的参数、service trait 与 interface marker 分析。
//!
//! 这一层只回答「能不能生成」以及「生成需要哪些事实」：
//!
//! - service trait 必须是非泛型、非 unsafe 的普通 trait；
//! - 方法必须是 `&self` 接收者，返回
//!   `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`；
//! - 参数只支持顶层借用 `&T` 与按值类型；`&mut T`、泛型方法、嵌套借用会被拒绝；
//! - interface marker 必须是单元结构体，`path` / `services` 必须同时给出且路径合法。
//!
//! 拒绝都带 span，便于编译器把错误指回用户代码。
//!
//! # `analyze_`: attribute, trait and interface analysis
//!
//! This layer decides whether expansion is possible and records every fact it needs. It
//! rejects generic methods, `&mut` parameters, nested borrows, unsupported return types,
//! non-unit interface markers and malformed `path` / `services` with a span pointing back
//! at the user's code.

use zyn::{
    format_ident,
    meta::Arg,
    syn::{
        self, Attribute, Expr, FnArg, GenericArgument, ItemImpl, ItemStruct, ItemTrait, Lifetime,
        Pat, ReturnType, TraitItem, TraitItemFn, Type, TypeParamBound,
        spanned::Spanned,
        visit::{self, Visit},
    },
};

use crate::model_::{
    ArgModel_, ConvertKind_, InterfaceModel_, MethodModel_, ServiceModel_, ServiceOfModel_,
};

/// `#[rpc]` 的两种参数形态。
pub(crate) enum RpcArgs_ {
    /// `#[rpc(service = "...")]`：service trait。
    Service {
        /// service 相对路径。
        path: String,
    },

    /// `#[rpc(path = "...", services = [...])]`：interface marker。
    Interface {
        /// interface 路径前缀。
        path: String,

        /// `services` 列表里的 trait 路径。
        services: Vec<syn::Path>,
    },

    /// `#[rpc(service_of(ApiV1))]`：服务端 impl 块。
    ServiceOf {
        /// 该实现所属的 interface marker 路径。
        interface: syn::Path,
    },
}

/// 解析 `#[rpc]` 属性参数，区分 service 宏与 interface 宏。
pub(crate) fn parse_rpc_args_(args: &zyn::Args) -> syn::Result<RpcArgs_> {
    let mut service: Option<(String, proc_macro2::Span)> = Option::None;
    let mut interface_path: Option<(String, proc_macro2::Span)> = Option::None;
    let mut services: Option<(Vec<syn::Path>, proc_macro2::Span)> = Option::None;
    let mut service_of: Option<(syn::Path, proc_macro2::Span)> = Option::None;

    for arg in args {
        match arg.name().map(ToString::to_string).as_deref() {
            Some("service") => {
                if service.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`service` 只能出现一次"));
                }
                let (value, span) = string_arg_(arg, "service")?;
                validate_service_path_(&value, span)?;
                service = Option::Some((value, span));
            }
            Some("path") => {
                if interface_path.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`path` 只能出现一次"));
                }
                let (value, span) = string_arg_(arg, "path")?;
                validate_interface_path_(&value, span)?;
                interface_path = Option::Some((value, span));
            }
            Some("services") => {
                if services.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`services` 只能出现一次"));
                }
                let list = parse_services_arg_(arg)?;
                services = Option::Some((list, arg_span_(arg)));
            }
            Some("service_of") => {
                if service_of.is_some() {
                    return Result::Err(syn::Error::new(
                        arg_span_(arg),
                        "`service_of` 只能出现一次",
                    ));
                }
                service_of = Option::Some(parse_service_of_arg_(arg)?);
            }
            Some(other) => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    format!(
                        "不认识的 `#[rpc]` 参数 `{other}`；当前只支持 `service = \"...\"`、\
                         `path = \"...\", services = [...]` 或 `service_of(Interface)`"
                    ),
                ));
            }
            None => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    "`#[rpc]` 参数需要写成 `service = \"...\"`、`path = \"...\", services = [...]` \
                     或 `service_of(Interface)`",
                ));
            }
        }
    }

    match (service, interface_path, services, service_of) {
        (Option::Some((path, _)), Option::None, Option::None, Option::None) => {
            Result::Ok(RpcArgs_::Service { path })
        }
        (Option::None, Option::Some((path, _)), Option::Some((services, _)), Option::None) => {
            Result::Ok(RpcArgs_::Interface { path, services })
        }
        (Option::None, Option::None, Option::None, Option::Some((interface, _))) => {
            Result::Ok(RpcArgs_::ServiceOf { interface })
        }
        (Option::Some((_, span)), ..) => Result::Err(syn::Error::new(
            span,
            "`service` 不能与 `path` / `services` / `service_of` 混用：`service` 用于 service trait",
        )),
        (Option::None, Option::Some((_, span)), Option::None, _) => {
            Result::Err(syn::Error::new(span, "interface 宏缺少 `services = [...]`"))
        }
        (Option::None, Option::None, Option::Some((_, span)), _) => {
            Result::Err(syn::Error::new(span, "interface 宏缺少 `path = \"...\"`"))
        }
        (Option::None, Option::Some((_, span)), _, Option::Some(_)) => Result::Err(
            syn::Error::new(span, "`path` / `services` 不能与 `service_of` 混用"),
        ),
        (Option::None, Option::None, Option::None, Option::None) => Result::Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "缺少 `#[rpc]` 参数；service 用 `#[rpc(service = \"...\")]`，interface 用 \
             `#[rpc(path = \"...\", services = [...])]`，服务端 impl 用 \
             `#[rpc(service_of(Interface))]`",
        )),
    }
}

/// 从 `name = "..."` 形式的参数里取出字符串字面量。
fn string_arg_(arg: &Arg, name: &str) -> syn::Result<(String, proc_macro2::Span)> {
    let Some(lit) = arg.as_expr_lit() else {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            format!("`{name}` 需要一个字符串字面量"),
        ));
    };
    let syn::Lit::Str(text) = lit else {
        return Result::Err(syn::Error::new(
            lit.span(),
            format!("`{name}` 的值必须是字符串字面量"),
        ));
    };
    Result::Ok((text.value(), text.span()))
}

/// 从 `services = [Trait, ...]` 里解析出一组 trait 路径。
fn parse_services_arg_(arg: &Arg) -> syn::Result<Vec<syn::Path>> {
    let Arg::Expr(_, expr) = arg else {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`services` 需要写成数组，例如 `services = [GoodsCustomerService]`",
        ));
    };
    let Expr::Array(array) = expr else {
        return Result::Err(syn::Error::new(
            expr.span(),
            "`services` 需要写成数组，例如 `services = [GoodsCustomerService]`",
        ));
    };
    if array.elems.is_empty() {
        return Result::Err(syn::Error::new(
            array.span(),
            "`services` 至少需要列出一个 service trait",
        ));
    }

    let mut paths = Vec::with_capacity(array.elems.len());
    for element in &array.elems {
        let Expr::Path(expr_path) = element else {
            return Result::Err(syn::Error::new(
                element.span(),
                "`services` 的每一项都必须是 trait 路径",
            ));
        };
        if expr_path.qself.is_some() {
            return Result::Err(syn::Error::new(
                expr_path.span(),
                "`services` 不支持限定路径（`<T as Trait>::...`）",
            ));
        }
        if expr_path
            .path
            .segments
            .iter()
            .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
        {
            return Result::Err(syn::Error::new(
                expr_path.span(),
                "`services` 里的 trait 路径不能带泛型参数",
            ));
        }
        if paths
            .iter()
            .any(|existing: &syn::Path| paths_equal_(existing, &expr_path.path))
        {
            return Result::Err(syn::Error::new(
                expr_path.span(),
                "`services` 里同一个 trait 只能出现一次",
            ));
        }
        paths.push(expr_path.path.clone());
    }

    Result::Ok(paths)
}

/// 从 `service_of(Interface)` 里解析 interface marker 路径。
fn parse_service_of_arg_(arg: &Arg) -> syn::Result<(syn::Path, proc_macro2::Span)> {
    let Arg::List(_, args) = arg else {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`service_of` 需要写成 `service_of(ApiV1)`",
        ));
    };
    if args.len() != 1usize {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`service_of` 需要且只能写一个 interface marker",
        ));
    }
    let path: syn::Path = syn::parse2(zyn::quote::quote!(#args))?;
    if path.segments.is_empty() {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`service_of` 里需要一个 interface marker 路径",
        ));
    }
    Result::Ok((path, arg_span_(arg)))
}

/// 两个路径在 token 层是否相同（用于拒绝重复的 service）。
fn paths_equal_(left: &syn::Path, right: &syn::Path) -> bool {
    zyn::quote::quote!(#left).to_string() == zyn::quote::quote!(#right).to_string()
}

/// 取一个 zyn 属性参数的 span。
fn arg_span_(arg: &Arg) -> proc_macro2::Span {
    match arg {
        Arg::Flag(ident) => ident.span(),
        Arg::Expr(ident, _) => ident.span(),
        Arg::List(ident, _) => ident.span(),
        Arg::Lit(lit) => lit.span(),
    }
}

/// 校验 service 路径：非空、无首尾 `/`、无空段、无空白。
fn validate_service_path_(path: &str, span: proc_macro2::Span) -> syn::Result<()> {
    validate_path_(path, span, "service")
}

/// 校验 interface 路径：规则与 service 路径相同。
fn validate_interface_path_(path: &str, span: proc_macro2::Span) -> syn::Result<()> {
    validate_path_(path, span, "interface")
}

/// 校验路径：非空、无首尾 `/`、无空段、无空白。
fn validate_path_(path: &str, span: proc_macro2::Span, kind: &str) -> syn::Result<()> {
    if path.is_empty() {
        return Result::Err(syn::Error::new(span, format!("{kind} 路径不能为空")));
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Result::Err(syn::Error::new(
            span,
            format!("{kind} 路径不能有首尾 `/`；interface 与 service 的拼接由框架负责"),
        ));
    }
    if path.split('/').any(str::is_empty) {
        return Result::Err(syn::Error::new(
            span,
            format!("{kind} 路径不能包含空段（例如 `a//b`）"),
        ));
    }
    if path.chars().any(char::is_whitespace) {
        return Result::Err(syn::Error::new(
            span,
            format!("{kind} 路径不能包含空白字符"),
        ));
    }
    Result::Ok(())
}

/// 分析一个 interface marker，产出展开层需要的完整模型。
pub(crate) fn analyze_interface_(
    item: &ItemStruct,
    path: String,
    services: Vec<syn::Path>,
) -> syn::Result<InterfaceModel_> {
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc(path = ..., services = ...)]` 不支持泛型 interface marker",
        ));
    }
    if let Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc(path = ..., services = ...)]` 不支持 where 子句",
        ));
    }
    if !matches!(item.fields, syn::Fields::Unit) {
        return Result::Err(syn::Error::new(
            item.fields.span(),
            "interface marker 必须是单元结构体，例如 `pub struct ApiV1;`",
        ));
    }
    if services.is_empty() {
        return Result::Err(syn::Error::new(
            item.ident.span(),
            "interface 的 `services` 列表不能为空",
        ));
    }

    // `services = [...]` 目前只用于校验与未来客户端类型约束；服务端装配由用户显式调用
    // `RpcRouter::add_service` 完成，宏不再生成 worker 组或 service 路由。
    let _ = services;

    Result::Ok(InterfaceModel_ {
        struct_ident_: item.ident.clone(),
        struct_item_: item.clone(),
        path_: path,
    })
}

/// 分析一个 service trait，产出展开层需要的完整模型。
pub(crate) fn analyze_service_(
    item: &ItemTrait,
    service_path: String,
) -> syn::Result<ServiceModel_> {
    if item.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            item.unsafety.span(),
            "`#[rpc]` 不支持 unsafe trait",
        ));
    }
    if item.auto_token.is_some() {
        return Result::Err(syn::Error::new(
            item.auto_token.span(),
            "`#[rpc]` 不支持 auto trait",
        ));
    }
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc]` 不支持泛型 service trait；请把泛型参数放到方法上或拆成多个 trait",
        ));
    }
    if let Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc]` 不支持 service trait 级 where 子句；约束请写在方法上",
        ));
    }

    for trait_item in &item.items {
        match trait_item {
            TraitItem::Const(item_const) => {
                if item_const.ident == "SERVICE_PATH" {
                    return Result::Err(syn::Error::new(
                        item_const.ident.span(),
                        "`SERVICE_PATH` 由 `#[rpc(service = \"...\")]` 注入，不要在 trait 里重复声明",
                    ));
                }
                if item_const.default.is_none() {
                    return Result::Err(syn::Error::new(
                        item_const.ident.span(),
                        "service trait 的关联常量必须有默认值；客户端代理不会为它生成实现",
                    ));
                }
            }
            TraitItem::Type(item_type) => {
                if item_type.default.is_none() {
                    return Result::Err(syn::Error::new(
                        item_type.ident.span(),
                        "service trait 的关联类型必须有默认值；客户端代理不会为它生成实现",
                    ));
                }
            }
            TraitItem::Macro(item_macro) => {
                return Result::Err(syn::Error::new(
                    item_macro.span(),
                    "`#[rpc]` 不支持 service trait 里的宏调用",
                ));
            }
            TraitItem::Verbatim(tokens) => {
                return Result::Err(syn::Error::new(
                    tokens.span(),
                    "`#[rpc]` 不支持无法识别的 trait 项",
                ));
            }
            TraitItem::Fn(method) if method.sig.ident.to_string().starts_with("__rpc_") => {
                return Result::Err(syn::Error::new(
                    method.sig.ident.span(),
                    "`__rpc_` 前缀保留给宏生成的 dispatch 方法，请不要在 service trait 里声明",
                ));
            }
            TraitItem::Fn(_) => {}
            _ => {}
        }
    }

    let methods = item
        .items
        .iter()
        .filter_map(|trait_item| match trait_item {
            TraitItem::Fn(method) => Option::Some(method),
            _ => Option::None,
        })
        .map(analyze_method_)
        .collect::<syn::Result<Vec<_>>>()?;

    if methods.is_empty() {
        return Result::Err(syn::Error::new(
            item.ident.span(),
            "service trait 至少需要一个 RPC 方法",
        ));
    }

    let module_ident = format_ident!(
        "__rpc_service_{}",
        item.ident.to_string().trim_start_matches("r#")
    );
    let requires_send_sync = item.supertraits.iter().any(|bound| match bound {
        TypeParamBound::Trait(trait_bound) => trait_bound
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "Send" || segment.ident == "Sync"),
        _ => false,
    });

    let mut trait_item = item.clone();
    let service_path_lit = syn::LitStr::new(&service_path, item.ident.span());
    trait_item.items.insert(
        0,
        syn::parse_quote! {
            /// 本 service 在 interface 下的相对路径，由 `#[rpc(service = "...")]` 注入。
            const SERVICE_PATH: &'static str = #service_path_lit;
        },
    );
    // 提供一个**默认实现**的隐藏方法：`#[rpc(service_of(...))]` 不需要知道隐藏模块路径，
    // 只要通过 service trait 就能转到本 trait 所在模块里的 dispatch。用户 impl 不必实现它。
    let module_ident_for_dispatch = module_ident.clone();
    trait_item.items.insert(
        1,
        syn::parse_quote! {
            #[doc(hidden)]
            #[allow(clippy::needless_lifetimes)]
            fn __rpc_dispatch_async<'__f, __C, __X>(
                &'__f self,
                ctx: &'__f mut __X,
            ) -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
                '__f,
                MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<()>,
            > + '__f
            where
                Self: ::core::marker::Sized + '__f,
                __C: '__f,
                __X: ::rpc_core_v1::x_deps::abs_rpc_serv::service::TrRpcCallCtx<
                    __C,
                    Err = ::rpc_core_v1::messaging::RpcError,
                > + '__f,
            {
                #module_ident_for_dispatch::dispatch_async(self, ctx)
            }
        },
    );

    Result::Ok(ServiceModel_ {
        trait_ident_: item.ident.clone(),
        trait_item_: trait_item,
        module_ident_: module_ident,
        methods_: methods,
        requires_send_sync_: requires_send_sync,
    })
}

/// 分析一个服务端 impl 块。
pub(crate) fn analyze_service_of_(
    item: &ItemImpl,
    interface_path: syn::Path,
) -> syn::Result<ServiceOfModel_> {
    if item.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            item.unsafety.span(),
            "`#[rpc(service_of(...))]` 不支持 unsafe impl",
        ));
    }
    if item.defaultness.is_some() {
        return Result::Err(syn::Error::new(
            item.defaultness.span(),
            "`#[rpc(service_of(...))]` 不支持 default impl",
        ));
    }
    let Some((bang, trait_path, _)) = &item.trait_ else {
        return Result::Err(syn::Error::new(
            item.self_ty.span(),
            "`#[rpc(service_of(...))]` 只能用于 trait impl，例如 \
             `impl GoodsCustomerService for GoodsCustomerServiceImpl`",
        ));
    };
    if bang.is_some() {
        return Result::Err(syn::Error::new(
            bang.span(),
            "`#[rpc(service_of(...))]` 不支持 negative impl（`impl !Trait`）",
        ));
    }
    if trait_path
        .segments
        .iter()
        .any(|segment| !matches!(segment.arguments, syn::PathArguments::None))
    {
        return Result::Err(syn::Error::new(
            trait_path.span(),
            "`#[rpc(service_of(...))]` 不支持带泛型参数的 service trait",
        ));
    }
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc(service_of(...))]` 暂不支持带泛型参数的 impl；请把泛型参数具体化",
        ));
    }
    if let Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc(service_of(...))]` 暂不支持 impl 级 where 子句",
        ));
    }

    Result::Ok(ServiceOfModel_ {
        impl_item_: item.clone(),
        trait_path_: trait_path.clone(),
        self_ty_: (*item.self_ty).clone(),
        interface_path_: interface_path,
    })
}

/// 分析一个方法。
fn analyze_method_(method: &TraitItemFn) -> syn::Result<MethodModel_> {
    let sig = &method.sig;

    if let Some(asyncness) = &sig.asyncness {
        return Result::Err(syn::Error::new(
            asyncness.span(),
            "service 方法不能是 `async fn`；请返回 `impl TrMayCancel<...>`，这样调用者才能主动取消",
        ));
    }
    if let Some(constness) = &sig.constness {
        return Result::Err(syn::Error::new(
            constness.span(),
            "`#[rpc]` 不支持 const 方法",
        ));
    }
    if let Some(unsafety) = &sig.unsafety {
        return Result::Err(syn::Error::new(
            unsafety.span(),
            "`#[rpc]` 不支持 unsafe 方法",
        ));
    }
    if let Some(abi) = &sig.abi {
        return Result::Err(syn::Error::new(abi.span(), "`#[rpc]` 不支持 extern 方法"));
    }

    for param in &sig.generics.params {
        match param {
            syn::GenericParam::Lifetime(_) => {}
            syn::GenericParam::Type(param) => {
                return Result::Err(syn::Error::new(
                    param.ident.span(),
                    "`#[rpc]` 不支持泛型方法；body enum 无法稳定表达泛型参数",
                ));
            }
            syn::GenericParam::Const(param) => {
                return Result::Err(syn::Error::new(
                    param.ident.span(),
                    "`#[rpc]` 不支持 const 泛型方法",
                ));
            }
        }
    }

    let mut receiver_seen = false;
    let mut args = Vec::new();

    for input in &sig.inputs {
        match input {
            FnArg::Receiver(receiver) => {
                if receiver_seen {
                    return Result::Err(syn::Error::new(
                        receiver.span(),
                        "方法只能有一个 `self` 接收者",
                    ));
                }
                receiver_seen = true;
                if receiver.mutability.is_some() {
                    return Result::Err(syn::Error::new(
                        receiver.span(),
                        "service 方法只支持 `&self`；客户端代理以共享引用持有底层客户端",
                    ));
                }
                match &receiver.reference {
                    Option::Some(_) => {}
                    Option::None => match &*receiver.ty {
                        Type::Reference(reference) if reference.mutability.is_none() => {}
                        other => {
                            return Result::Err(syn::Error::new(
                                other.span(),
                                "service 方法只支持 `&self` 接收者",
                            ));
                        }
                    },
                }
            }
            FnArg::Typed(pat_type) => {
                let Pat::Ident(pattern) = &*pat_type.pat else {
                    return Result::Err(syn::Error::new(
                        pat_type.pat.span(),
                        "方法参数必须是简单标识符；不支持解构、`_` 或子模式",
                    ));
                };
                if pattern.by_ref.is_some() || pattern.subpat.is_some() {
                    return Result::Err(syn::Error::new(
                        pattern.span(),
                        "方法参数必须是简单标识符；不支持 `ref` 或子模式",
                    ));
                }
                args.push(analyze_arg_(&pattern.ident, &pat_type.ty)?);
            }
        }
    }

    if !receiver_seen {
        return Result::Err(syn::Error::new(
            sig.ident.span(),
            "service 方法必须是 `&self` 方法；关联函数无法通过客户端代理调用",
        ));
    }

    let return_info = analyze_return_type_(&sig.output)?;

    Result::Ok(MethodModel_ {
        ident_: sig.ident.clone(),
        sig_: sig.clone(),
        attrs_: method.attrs.clone(),
        args_: args,
        output_ty_: return_info.output_ty_,
        may_cancel_lifetime_: return_info.may_cancel_lifetime_,
        extra_bounds_: return_info.extra_bounds_,
    })
}

/// 分析一个参数。
fn analyze_arg_(ident: &syn::Ident, ty: &Type) -> syn::Result<ArgModel_> {
    reject_bound_lifetimes_(ty)?;

    match ty {
        Type::Reference(reference) => {
            if reference.mutability.is_some() {
                return Result::Err(syn::Error::new(
                    reference.span(),
                    "`#[rpc]` 不支持 `&mut` 参数；请求体只能携带可编码的不可变借用或 owned 值",
                ));
            }
            if reference
                .lifetime
                .as_ref()
                .is_some_and(|lifetime| lifetime.ident == "static")
            {
                return Result::Err(syn::Error::new(
                    reference.span(),
                    "`#[rpc]` 不支持 `&'static` 借用参数；服务端无法从 owned body 重建 \
                     `'static` 借用，请改用 owned 参数",
                ));
            }
            let (owned_ty, convert) = borrowed_to_owned_(&reference.elem)?;
            Result::Ok(ArgModel_ {
                ident_: ident.clone(),
                owned_ty_: owned_ty,
                is_ref_: true,
                convert_: convert,
            })
        }
        other => {
            if contains_non_static_lifetime_(other) {
                return Result::Err(syn::Error::new(
                    other.span(),
                    "借用参数必须是顶层 `&T`；body enum 的 owned 版本无法从嵌套生命周期里重建",
                ));
            }
            if contains_reference_(other) {
                return Result::Err(syn::Error::new(
                    other.span(),
                    "借用参数必须是顶层 `&T`；不支持嵌套在泛型 / 元组里的引用",
                ));
            }
            Result::Ok(ArgModel_ {
                ident_: ident.clone(),
                owned_ty_: other.clone(),
                is_ref_: false,
                convert_: ConvertKind_::None,
            })
        }
    }
}

/// 把顶层借用参数转成 body enum 的 owned 字段类型与客户端转换方式。
///
/// `denco_macro` 当前不支持带生命周期参数的派生类型，因此这里只接受能无额外 bound
/// 转成 owned 的借用：`&str` → `String`、`&[u8]` → `Vec<u8>`、`&String` / `&Vec<u8>`
/// clone、以及 `&Copy` 标量解引用。
fn borrowed_to_owned_(inner: &Type) -> syn::Result<(Type, ConvertKind_)> {
    match inner {
        Type::Path(path) if path.qself.is_none() => {
            let Some(segment) = path.path.segments.last() else {
                return Result::Err(syn::Error::new(inner.span(), "无法识别借用参数的类型"));
            };
            let name = segment.ident.to_string();
            match name.as_str() {
                "str" => Result::Ok((
                    syn::parse_quote!(::std::string::String),
                    ConvertKind_::ToString,
                )),
                "String" => Result::Ok((
                    syn::parse_quote!(::std::string::String),
                    ConvertKind_::Clone,
                )),
                "u8" | "u16" | "u32" | "u64" | "u128" | "usize" | "i8" | "i16" | "i32" | "i64"
                | "i128" | "isize" | "bool" | "char" | "f32" | "f64" => {
                    Result::Ok((inner.clone(), ConvertKind_::Deref))
                }
                "Vec" if is_vec_u8_(path) => {
                    Result::Ok((syn::parse_quote!(::std::vec::Vec<u8>), ConvertKind_::Clone))
                }
                _ => Result::Err(unsupported_borrow_(inner)),
            }
        }
        Type::Slice(slice) if is_u8_type_(&slice.elem) => {
            Result::Ok((syn::parse_quote!(::std::vec::Vec<u8>), ConvertKind_::ToVec))
        }
        _ => Result::Err(unsupported_borrow_(inner)),
    }
}

/// 借用参数不在支持列表时的错误。
fn unsupported_borrow_(inner: &Type) -> syn::Error {
    syn::Error::new(
        inner.span(),
        "借用参数目前只支持 `&str`、`&[u8]`、`&String`、`&Vec<u8>` 与 Copy 标量；其它类型请改为 owned 参数（例如 `String` / `Vec<u8>` / 业务结构体）",
    )
}

/// `Vec<u8>` 的类型路径。
fn is_vec_u8_(path: &syn::TypePath) -> bool {
    let Some(segment) = path.path.segments.last() else {
        return false;
    };
    if segment.ident != "Vec" {
        return false;
    }
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return false;
    };
    arguments.args.len() == 1usize
        && matches!(
            &arguments.args[0],
            GenericArgument::Type(Type::Path(inner))
                if inner.qself.is_none() && inner.path.is_ident("u8")
        )
}

/// 类型是否是 `u8`。
fn is_u8_type_(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("u8"))
}

/// 返回类型分析结果。
struct ReturnInfo_ {
    /// `RpcResult<T>` 里的 `T`。
    output_ty_: Type,

    /// `TrMayCancel<'f, ...>` 里的 `'f`。
    may_cancel_lifetime_: Option<Lifetime>,

    /// 除 `TrMayCancel` 之外的其余 bound。
    extra_bounds_: Vec<TypeParamBound>,
}

/// 分析 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`。
fn analyze_return_type_(output: &ReturnType) -> syn::Result<ReturnInfo_> {
    let ReturnType::Type(_, ty) = output else {
        return Result::Err(syn::Error::new(
            output.span(),
            "service 方法必须返回 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`",
        ));
    };
    let Type::ImplTrait(impl_trait) = &**ty else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "service 方法必须返回 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`",
        ));
    };

    let mut may_cancel: Option<(Option<Lifetime>, Type)> = Option::None;
    let mut extra_bounds = Vec::new();

    for bound in &impl_trait.bounds {
        let TypeParamBound::Trait(trait_bound) = bound else {
            extra_bounds.push(bound.clone());
            continue;
        };
        let Some(segment) = trait_bound.path.segments.last() else {
            extra_bounds.push(bound.clone());
            continue;
        };
        if segment.ident != "TrMayCancel" {
            extra_bounds.push(bound.clone());
            continue;
        }
        if may_cancel.is_some() {
            return Result::Err(syn::Error::new(
                segment.ident.span(),
                "`TrMayCancel` 只能出现一次",
            ));
        }

        let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
            return Result::Err(syn::Error::new(
                segment.ident.span(),
                "`TrMayCancel` 需要显式写出生命周期与 `MayCancelOutput`，例如 \
                 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`",
            ));
        };

        let mut lifetime: Option<Lifetime> = Option::None;
        let mut may_cancel_output: Option<Type> = Option::None;
        for argument in &arguments.args {
            match argument {
                GenericArgument::Lifetime(lt) => lifetime = Option::Some(lt.clone()),
                GenericArgument::AssocType(assoc) if assoc.ident == "MayCancelOutput" => {
                    may_cancel_output = Option::Some(assoc.ty.clone());
                }
                _ => {}
            }
        }

        let Some(may_cancel_output) = may_cancel_output else {
            return Result::Err(syn::Error::new(
                segment.ident.span(),
                "`TrMayCancel` 缺少 `MayCancelOutput = RpcResult<T>` 关联类型绑定",
            ));
        };
        let output_ty = success_type_(&may_cancel_output)?;
        may_cancel = Option::Some((lifetime, output_ty));
    }

    let Some((may_cancel_lifetime, output_ty)) = may_cancel else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "service 方法必须返回 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`",
        ));
    };

    Result::Ok(ReturnInfo_ {
        output_ty_: output_ty,
        may_cancel_lifetime_: may_cancel_lifetime,
        extra_bounds_: extra_bounds,
    })
}

/// 从 `RpcResult<T>` / `Result<T, RpcError>` 中取出 `T`。
fn success_type_(ty: &Type) -> syn::Result<Type> {
    let Type::Path(path) = ty else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "`MayCancelOutput` 必须是 `RpcResult<T>` 或 `Result<T, RpcError>`",
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "`MayCancelOutput` 必须是 `RpcResult<T>` 或 `Result<T, RpcError>`",
        ));
    };
    let syn::PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Result::Err(syn::Error::new(
            segment.ident.span(),
            "`MayCancelOutput` 必须是 `RpcResult<T>` 或 `Result<T, RpcError>`",
        ));
    };

    match segment.ident.to_string().as_str() {
        "RpcResult" => {
            if arguments.args.len() != 1usize {
                return Result::Err(syn::Error::new(
                    segment.ident.span(),
                    "`RpcResult` 需要一个成功类型参数",
                ));
            }
            match &arguments.args[0] {
                GenericArgument::Type(output) => Result::Ok(output.clone()),
                other => Result::Err(syn::Error::new(
                    other.span(),
                    "`RpcResult` 的参数必须是类型",
                )),
            }
        }
        "Result" => {
            if arguments.args.len() != 2usize {
                return Result::Err(syn::Error::new(
                    segment.ident.span(),
                    "`Result` 需要成功类型与错误类型两个参数",
                ));
            }
            let GenericArgument::Type(output) = &arguments.args[0] else {
                return Result::Err(syn::Error::new(
                    arguments.args[0].span(),
                    "`Result` 的第一个参数必须是成功类型",
                ));
            };
            let GenericArgument::Type(error) = &arguments.args[1] else {
                return Result::Err(syn::Error::new(
                    arguments.args[1].span(),
                    "`Result` 的第二个参数必须是 `RpcError`",
                ));
            };
            if !type_ends_with_(error, "RpcError") {
                return Result::Err(syn::Error::new(
                    error.span(),
                    "service 方法必须使用框架的 `RpcError`；业务错误请放进 `RpcError::Service`",
                ));
            }
            Result::Ok(output.clone())
        }
        _ => Result::Err(syn::Error::new(
            segment.ident.span(),
            "`MayCancelOutput` 必须是 `RpcResult<T>` 或 `Result<T, RpcError>`",
        )),
    }
}

/// 类型路径的最后一段是否是给定名字。
fn type_ends_with_(ty: &Type, name: &str) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    path.path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == name)
}

/// 拒绝高阶生命周期（`for<'a> ...`）；重写生命周期时无法安全处理它们。
fn reject_bound_lifetimes_(ty: &Type) -> syn::Result<()> {
    struct Detector_ {
        span_: Option<proc_macro2::Span>,
    }

    impl<'ast> Visit<'ast> for Detector_ {
        fn visit_bound_lifetimes(&mut self, bound: &'ast syn::BoundLifetimes) {
            if self.span_.is_none() {
                self.span_ = Option::Some(bound.span());
            }
            visit::visit_bound_lifetimes(self, bound);
        }
    }

    let mut detector = Detector_ {
        span_: Option::None,
    };
    detector.visit_type(ty);
    match detector.span_ {
        Option::Some(span) => Result::Err(syn::Error::new(
            span,
            "`#[rpc]` 不支持高阶生命周期（`for<'a> ...`）",
        )),
        Option::None => Result::Ok(()),
    }
}

/// 类型里是否含有非 `'static` 的生命周期。
fn contains_non_static_lifetime_(ty: &Type) -> bool {
    struct Detector_ {
        found_: bool,
    }

    impl<'ast> Visit<'ast> for Detector_ {
        fn visit_lifetime(&mut self, lifetime: &'ast Lifetime) {
            if lifetime.ident != "static" {
                self.found_ = true;
            }
        }
    }

    let mut detector = Detector_ { found_: false };
    detector.visit_type(ty);
    detector.found_
}

/// 类型里是否含有引用。
fn contains_reference_(ty: &Type) -> bool {
    struct Detector_ {
        found_: bool,
    }

    impl<'ast> Visit<'ast> for Detector_ {
        fn visit_type_reference(&mut self, reference: &'ast syn::TypeReference) {
            self.found_ = true;
            visit::visit_type_reference(self, reference);
        }
    }

    let mut detector = Detector_ { found_: false };
    detector.visit_type(ty);
    detector.found_
}

/// 只保留 `#[cfg]` / `#[cfg_attr]`，用于生成到 enum 变体或 match 分支上的属性。
pub(crate) fn cfg_attrs_(attrs: &[Attribute]) -> Vec<Attribute> {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("cfg") || attr.path().is_ident("cfg_attr"))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests_ {
    use zyn::syn::parse_quote;

    use super::*;

    /// 测试 service 路径校验接受合法路径并拒绝首尾斜杠、空段。
    /// Test that service path validation accepts valid paths and rejects bad ones.
    ///
    /// - 手段：分别校验 `customer/goods`、`/customer/goods`、`a//b` 与空串。
    /// - 判断：只有合法路径返回 `Ok`，其余三种返回 `Err`。
    ///
    /// - Means: validate a good path and three malformed ones.
    /// - Verdict: only the good path is `Ok`; the other three are `Err`.
    #[test]
    fn validates_service_path_() {
        assert!(validate_service_path_("customer/goods", proc_macro2::Span::call_site()).is_ok());
        assert!(validate_service_path_("/customer/goods", proc_macro2::Span::call_site()).is_err());
        assert!(validate_service_path_("a//b", proc_macro2::Span::call_site()).is_err());
        assert!(validate_service_path_("", proc_macro2::Span::call_site()).is_err());
    }

    /// 测试属性参数解析能区分 service 与 interface 两种形态。
    /// Test that attribute parsing distinguishes the service and interface forms.
    ///
    /// - 手段：分别解析 `service = "customer/goods"` 与
    ///   `path = "api/v1", services = [GoodsCustomerService]`。
    /// - 判断：前者得到 `RpcArgs_::Service`，后者得到 `RpcArgs_::Interface` 且 path 正确。
    ///
    /// - Means: parse one service form and one interface form.
    /// - Verdict: the first is `Service`; the second is `Interface` with the expected path.
    #[test]
    fn parses_both_arg_forms_() {
        let args: zyn::Args = zyn::parse!("service = \"customer/goods\"").expect("参数应当可解析");
        assert!(matches!(
            parse_rpc_args_(&args),
            Result::Ok(RpcArgs_::Service { .. })
        ));

        let args: zyn::Args = zyn::parse!("path = \"api/v1\", services = [GoodsCustomerService]")
            .expect("参数应当可解析");
        match parse_rpc_args_(&args) {
            Result::Ok(RpcArgs_::Interface { path, services }) => {
                assert_eq!(path, "api/v1");
                assert_eq!(services.len(), 1usize);
            }
            _ => panic!("应当解析成 interface 形态"),
        }
    }

    /// 测试属性参数解析接受 `service_of(Interface)` 形态。
    /// Test that attribute parsing accepts the `service_of(Interface)` form.
    ///
    /// - 手段：解析 `service_of(ApiV1)`，再解析三个坏形态（空列表、两个 interface、
    ///   写成赋值）。
    /// - 判断：合法形态得到 `RpcArgs_::ServiceOf`；三个坏形态都返回 `Err`。
    ///
    /// - Means: parse a good `service_of(ApiV1)` and three malformed variants.
    /// - Verdict: the good form is `ServiceOf`; all malformed ones are errors.
    #[test]
    fn parses_service_of_arg_() {
        let args: zyn::Args = zyn::parse!("service_of(ApiV1)").expect("参数应当可解析");
        match parse_rpc_args_(&args) {
            Result::Ok(RpcArgs_::ServiceOf { interface }) => {
                assert_eq!(
                    zyn::quote::quote!(#interface).to_string().replace(' ', ""),
                    "ApiV1"
                );
            }
            _ => panic!("应当解析成 service_of 形态"),
        }

        for case in [
            "service_of()",
            "service_of(ApiV1, ApiV2)",
            "service_of = ApiV1",
        ] {
            let args: zyn::Args = zyn::syn::parse_str(case).expect("参数应当可解析");
            assert!(parse_rpc_args_(&args).is_err(), "应当拒绝：{case}");
        }
    }

    /// 测试服务端 impl 分析接受普通 impl，并拒绝泛型 impl。
    /// Test that service-of analysis accepts a plain impl and rejects generic ones.
    ///
    /// - 手段：分析 `impl GoodsService for GoodsImpl {}` 与
    ///   `impl<T> GoodsService for GoodsImpl<T> {}`。
    /// - 判断：前者得到 trait / self type / interface 路径；后者返回 `Err`。
    ///
    /// - Means: analyse a plain impl and a generic impl.
    /// - Verdict: the plain one yields its trait, self type and interface; the generic one
    ///   is rejected.
    #[test]
    fn analyses_service_of_impl_() {
        let item: ItemImpl = parse_quote!(impl GoodsService for GoodsImpl {});
        let model =
            analyze_service_of_(&item, parse_quote!(ApiV1)).expect("普通 impl 应当分析成功");
        let trait_path = &model.trait_path_;
        assert_eq!(
            zyn::quote::quote!(#trait_path).to_string().replace(' ', ""),
            "GoodsService"
        );
        let self_ty = &model.self_ty_;
        assert_eq!(
            zyn::quote::quote!(#self_ty).to_string().replace(' ', ""),
            "GoodsImpl"
        );

        let generic: ItemImpl = parse_quote!(
            impl<T> GoodsService for GoodsImpl<T> {}
        );
        assert!(analyze_service_of_(&generic, parse_quote!(ApiV1)).is_err());
    }

    /// 测试属性参数解析拒绝缺项、混用、非字符串值与坏路径。
    /// Test that attribute parsing rejects missing pieces, mixing forms, bad values and paths.
    ///
    /// - 手段：分别解析只有 `path`、`service` 与 `path` 混用、`service = 42`、
    ///   `service = "a//b"`、空 `services` 与重复 service。
    /// - 判断：七种输入都返回 `Err`。
    ///
    /// - Means: parse seven malformed argument lists.
    /// - Verdict: every one of them is an error.
    #[test]
    fn rejects_invalid_attr_args_() {
        let cases = [
            "path = \"api/v1\"",
            "service = \"customer/goods\", path = \"api/v1\"",
            "service = 42",
            "service = \"a//b\"",
            "path = \"api/v1\", services = []",
            "path = \"api/v1\", services = [Foo, Foo]",
            "service = \"customer/goods\", services = [Foo]",
        ];
        for case in cases {
            let args: zyn::Args = zyn::syn::parse_str(case).expect("参数应当可解析");
            assert!(parse_rpc_args_(&args).is_err(), "应当拒绝：{case}");
        }
    }

    /// 测试 interface 分析保留 marker 与路径，并接受跨模块 service 列表。
    /// Test that interface analysis keeps the marker and path and accepts a cross-module list.
    ///
    /// - 手段：分析 `pub struct ApiV1;` 与 `services = [nested::GoodsService]`。
    /// - 判断：路径是 `api/v1`，结构体名是 `ApiV1`；`services` 不再用于生成 worker 组。
    ///
    /// - Means: analyse a unit marker listing a trait from a nested module.
    /// - Verdict: the path is `api/v1`, the marker name is `ApiV1`, and no worker group is
    ///   derived.
    #[test]
    fn analyses_interface_() {
        let item: ItemStruct = parse_quote!(
            pub struct ApiV1;
        );
        let services: Vec<syn::Path> = vec![parse_quote!(nested::GoodsService)];
        let model = analyze_interface_(&item, "api/v1".to_string(), services)
            .expect("interface 应当分析成功");

        assert_eq!(model.path_, "api/v1");
        assert_eq!(model.struct_ident_.to_string(), "ApiV1");
    }

    /// 测试 interface 分析拒绝泛型、非单元结构体与空 services。
    /// Test that interface analysis rejects generics, non-unit structs and empty services.
    ///
    /// - 手段：分别分析泛型 marker、带字段 marker 与空列表。
    /// - 判断：三者都返回 `Err`。
    ///
    /// - Means: analyse a generic marker, a marker with fields and an empty service list.
    /// - Verdict: all three are errors.
    #[test]
    fn rejects_bad_interface_() {
        let generic: ItemStruct = parse_quote!(
            pub struct ApiV1<T>(T);
        );
        assert!(
            analyze_interface_(
                &generic,
                "api/v1".to_string(),
                vec![parse_quote!(GoodsService)]
            )
            .is_err()
        );

        let fields: ItemStruct = parse_quote!(
            pub struct ApiV1 {
                value: u8,
            }
        );
        assert!(
            analyze_interface_(
                &fields,
                "api/v1".to_string(),
                vec![parse_quote!(GoodsService)]
            )
            .is_err()
        );

        let unit: ItemStruct = parse_quote!(
            pub struct ApiV1;
        );
        assert!(analyze_interface_(&unit, "api/v1".to_string(), Vec::new()).is_err());
    }

    /// 测试返回类型分析能从 `RpcResult<T>` 取出成功类型。
    /// Test that return-type analysis extracts the success type from `RpcResult<T>`.
    ///
    /// - 手段：解析 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<u64>>>`。
    /// - 判断：成功类型是 `Vec<u64>`，取消生命周期是 `'f`。
    ///
    /// - Means: parse an `impl TrMayCancel` return type.
    /// - Verdict: the success type is `Vec<u64>` and the cancel lifetime is `'f`.
    #[test]
    fn extracts_success_type_() {
        let output: ReturnType =
            parse_quote!(-> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<u64>>>);
        let info = analyze_return_type_(&output).expect("返回类型应当分析成功");
        let output_ty = &info.output_ty_;
        assert_eq!(
            zyn::quote::quote!(#output_ty).to_string(),
            zyn::quote::quote!(Vec<u64>).to_string()
        );
        assert_eq!(
            info.may_cancel_lifetime_.map(|lt| lt.to_string()),
            Some("'f".to_string())
        );
    }

    /// 测试 `Result<T, RpcError>` 与自定义错误分别被接受 / 拒绝。
    /// Test that `Result<T, RpcError>` is accepted while a custom error is rejected.
    ///
    /// - 手段：分别分析 `Result<u8, RpcError>` 与 `Result<u8, OtherError>`。
    /// - 判断：前者成功，后者失败。
    ///
    /// - Means: analyze a framework error and a custom error.
    /// - Verdict: the framework error succeeds; the custom one fails.
    #[test]
    fn accepts_only_rpc_error_() {
        let ok: ReturnType =
            parse_quote!(-> impl TrMayCancel<'f, MayCancelOutput = Result<u8, RpcError>>);
        assert!(analyze_return_type_(&ok).is_ok());

        let bad: ReturnType =
            parse_quote!(-> impl TrMayCancel<'f, MayCancelOutput = Result<u8, OtherError>>);
        assert!(analyze_return_type_(&bad).is_err());
    }

    /// 测试借用参数会被转换成 owned 类型并标记为引用。
    /// Test that borrowed parameters are converted to owned types and flagged.
    ///
    /// - 手段：分析 `&str`、`&[u8]`、`&Foo` 与按值 `u64`。
    /// - 判断：前三者分别得到 `String`、`Vec<u8>`、`Foo` 且 `is_ref` 为真；`u64` 不是引用。
    ///
    /// - Means: analyze three borrowed types and one by-value type.
    /// - Verdict: borrowed types become owned and are flagged; `u64` is not a reference.
    #[test]
    fn converts_borrowed_args_() {
        let name = zyn::format_ident!("name");
        let arg = analyze_arg_(&name, &parse_quote!(&str)).expect("&str 应当被接受");
        let owned_ty = &arg.owned_ty_;
        assert_eq!(
            zyn::quote::quote!(#owned_ty).to_string().replace(' ', ""),
            "::std::string::String"
        );
        assert!(arg.is_ref_);

        let bytes = analyze_arg_(&name, &parse_quote!(&[u8])).expect("&[u8] 应当被接受");
        let owned_ty = &bytes.owned_ty_;
        assert_eq!(
            zyn::quote::quote!(#owned_ty).to_string().replace(' ', ""),
            "::std::vec::Vec<u8>"
        );
        assert!(bytes.is_ref_);

        let value = analyze_arg_(&name, &parse_quote!(u64)).expect("u64 应当被接受");
        assert!(!value.is_ref_);

        let bad = analyze_arg_(&name, &parse_quote!(&mut String));
        assert!(bad.is_err(), "&mut 参数应当被拒绝");

        let static_ref = analyze_arg_(&name, &parse_quote!(&'static str));
        assert!(static_ref.is_err(), "&'static 借用应当被拒绝");
    }
}
