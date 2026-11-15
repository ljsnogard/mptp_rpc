//! `#[rpc]` 的参数、controller impl、Call service trait 与 `impl_of` 分析。
//!
//! 这一层只回答「能不能生成」以及「生成需要哪些事实」：
//!
//! - controller 是固有 impl 块，不支持 unsafe / default / trait impl / 泛型 impl /
//!   impl 级 where 子句；
//! - 每个端点方法用 `#[rpc(方法 = "路径")]` 声明访问方法与相对路径，不带该属性的方法
//!   原样保留为辅助方法；
//! - 端点路径非空、无空段、无空白；
//! - Call service trait 只允许 `&self`、无泛型、无默认实现的方法，返回
//!   `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`；参数必须能稳定地转成 owned
//!   载荷（借用按白名单转换，其它借用拒绝）；
//! - `impl_of` 只接受与之形状一致的 trait impl。
//!
//! 拒绝都带 span，便于编译器把错误指回用户代码。
//!
//! # `analyze_`: attribute, controller, service trait and `impl_of` analysis
//!
//! This layer decides whether expansion is possible and records every fact it needs. It
//! rejects controller/impl/service shapes the macro cannot generate deterministically, and
//! points every rejection back at the user's code.

use zyn::{
    meta::Arg,
    syn::{
        self, Attribute, Expr, FnArg, GenericParam, ImplItem, ImplItemFn, ItemFn, ItemImpl,
        ItemTrait, Lit, Meta, Pat, Path, ReturnType, Signature, TraitItem, Type, TypeParamBound,
        WherePredicate,
        spanned::Spanned,
        visit::{self, Visit},
    },
};

use crate::model_::{
    BorrowMode_, CallMethodModel_, CallParam_, ControllerModel_, EndpointMethodModel_,
    EndpointSpec_, FreeFnModel_, ImplOfModel_, MethodKind_, PayloadShape_, ServiceModel_,
    TokenParam_,
};

/// `#[rpc]` 的参数形态。
pub(crate) enum RpcArgs_ {
    /// `#[rpc(interface = "...")]`：controller 固有 impl 块。
    Controller {
        /// interface 路径前缀。
        interface: String,
    },

    /// `#[rpc(service = "...", api = X)]`：Call service trait。
    Service {
        /// 完整服务前缀。
        prefix: String,

        /// 宏生成的零大小 marker 标识符。
        api: syn::Ident,
    },

    /// `#[rpc(impl_of(X))]`：Call service 的 trait impl。
    ImplOf {
        /// 协议 crate 里的 marker 路径。
        api: Path,
    },
}

/// 解析 `#[rpc]` 属性参数。
pub(crate) fn parse_rpc_args_(args: &zyn::Args) -> syn::Result<RpcArgs_> {
    let mut controller_interface: Option<(String, proc_macro2::Span)> = Option::None;
    let mut service_prefix: Option<(String, proc_macro2::Span)> = Option::None;
    let mut api: Option<syn::Ident> = Option::None;
    let mut impl_of: Option<Path> = Option::None;

    for arg in args {
        match arg.name().map(ToString::to_string).as_deref() {
            Some("interface") => {
                if controller_interface.is_some() {
                    return Result::Err(syn::Error::new(
                        arg_span_(arg),
                        "`interface` 只能出现一次",
                    ));
                }
                let (value, span) = string_arg_(arg, "interface")?;
                validate_path_(&value, span, "interface")?;
                controller_interface = Option::Some((value, span));
            }
            Some("service") => {
                if service_prefix.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`service` 只能出现一次"));
                }
                let (value, span) = string_arg_(arg, "service")?;
                validate_path_(&value, span, "service")?;
                service_prefix = Option::Some((value, span));
            }
            Some("api") => {
                if api.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`api` 只能出现一次"));
                }
                api = Option::Some(simple_ident_arg_(arg, "api")?);
            }
            Some("impl_of") => {
                if impl_of.is_some() {
                    return Result::Err(syn::Error::new(arg_span_(arg), "`impl_of` 只能出现一次"));
                }
                impl_of = Option::Some(impl_of_arg_(arg)?);
            }
            Some(other) => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    format!(
                        "不认识的 `#[rpc]` 参数 `{other}`；支持 `interface = \"...\"`（controller）、\
                         `service = \"...\", api = X`（Call service trait）与 `impl_of(X)`（trait impl）"
                    ),
                ));
            }
            None => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    "`#[rpc]` 参数需要写成 `interface = \"...\"` / `service = \"...\", api = X` / \
                     `impl_of(X)`",
                ));
            }
        }
    }

    if let Option::Some(api_path) = impl_of {
        if controller_interface.is_some() || service_prefix.is_some() || api.is_some() {
            return Result::Err(syn::Error::new(
                api_path.span(),
                "`impl_of` 不能与 `interface` / `service` / `api` 同时出现",
            ));
        }
        return Result::Ok(RpcArgs_::ImplOf { api: api_path });
    }

    match (service_prefix, api) {
        (Option::Some((prefix, _)), Option::Some(api)) => {
            if controller_interface.is_some() {
                return Result::Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "`service` / `api` 不能与 `interface` 同时出现",
                ));
            }
            return Result::Ok(RpcArgs_::Service { prefix, api });
        }
        (Option::Some((_, span)), Option::None) => {
            return Result::Err(syn::Error::new(
                span,
                "Call service trait 还需要 `api = X` 指定生成的 marker",
            ));
        }
        (Option::None, Option::Some(ident)) => {
            return Result::Err(syn::Error::new(
                ident.span(),
                "`api` 必须与 `service = \"...\"` 一起使用",
            ));
        }
        (Option::None, Option::None) => {}
    }

    match controller_interface {
        Option::Some((interface, _)) => Result::Ok(RpcArgs_::Controller { interface }),
        Option::None => Result::Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "缺少 `#[rpc]` 参数；controller 用 `#[rpc(interface = \"...\")]`，Call service 用 \
             `#[rpc(service = \"...\", api = X)]`",
        )),
    }
}

/// 取 `name = Ident` 形式的简单标识符参数。
fn simple_ident_arg_(arg: &Arg, name: &str) -> syn::Result<syn::Ident> {
    let Arg::Expr(_, Expr::Path(expr_path)) = arg else {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            format!("`{name}` 需要一个简单标识符，例如 `api = RoomSvc`"),
        ));
    };
    if expr_path.qself.is_none() && expr_path.path.segments.len() == 1usize {
        let segment = &expr_path.path.segments[0];
        if segment.arguments.is_none() {
            return Result::Ok(segment.ident.clone());
        }
    }
    Result::Err(syn::Error::new(
        expr_path.span(),
        format!(
            "`{name}` 需要是简单标识符（marker 由本宏在 trait 所在模块生成）；请在 trait \
             所在模块直接写 `{name} = MarkerName`"
        ),
    ))
}

/// 解析 `impl_of(X)` 里的 marker 路径。
fn impl_of_arg_(arg: &Arg) -> syn::Result<Path> {
    let Arg::List(_, inner) = arg else {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`impl_of` 需要写成 `impl_of(RoomSvc)`",
        ));
    };
    if inner.len() != 1usize {
        return Result::Err(syn::Error::new(
            arg_span_(arg),
            "`impl_of(...)` 只接受一个 marker 标识符，例如 `impl_of(RoomSvc)`",
        ));
    }
    match inner.get_index(0) {
        Option::Some(Arg::Flag(ident)) => Result::Ok(Path::from(ident.clone())),
        Option::Some(other) => Result::Err(syn::Error::new(
            arg_span_(other),
            "`impl_of(...)` 只接受一个 marker 标识符（先用 `use` 引入该类型），例如 \
             `impl_of(RoomSvc)`",
        )),
        Option::None => Result::Err(syn::Error::new(
            arg_span_(arg),
            "`impl_of(...)` 只接受一个 marker 标识符，例如 `impl_of(RoomSvc)`",
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

/// 取一个 zyn 属性参数的 span。
fn arg_span_(arg: &Arg) -> proc_macro2::Span {
    match arg {
        Arg::Flag(ident) => ident.span(),
        Arg::Expr(ident, _) => ident.span(),
        Arg::List(ident, _) => ident.span(),
        Arg::Lit(lit) => lit.span(),
    }
}

/// 校验路径：非空、无首尾 `/`、无空段、无空白。
fn validate_path_(path: &str, span: proc_macro2::Span, kind: &str) -> syn::Result<()> {
    if path.is_empty() {
        return Result::Err(syn::Error::new(span, format!("{kind} 路径不能为空")));
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Result::Err(syn::Error::new(span, format!("{kind} 路径不能有首尾 `/`")));
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

/// 分析一个 controller impl 块。
///
/// 每个带 `#[rpc(call/view/head/... = "path")]` 的方法是一个端点；不带该属性的方法原样
/// 保留（属于 controller 的普通辅助方法）。方法上的 `#[rpc(...)]` 端点声明属性会被剥离，
/// 避免再次被当作独立属性宏展开。
pub(crate) fn analyze_controller_(
    item: &ItemImpl,
    interface: String,
) -> syn::Result<ControllerModel_> {
    if item.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            item.unsafety.span(),
            "`#[rpc(interface = \"...\")]` 不支持 unsafe impl",
        ));
    }
    if item.defaultness.is_some() {
        return Result::Err(syn::Error::new(
            item.defaultness.span(),
            "`#[rpc(interface = \"...\")]` 不支持 default impl",
        ));
    }
    if item.trait_.is_some() {
        return Result::Err(syn::Error::new(
            item.self_ty.span(),
            "`#[rpc(interface = \"...\")]` 只能用于固有 impl（`impl SceneService { ... }`），\
             不支持 trait impl",
        ));
    }
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc(interface = \"...\")]` 暂不支持泛型 impl",
        ));
    }
    if let Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc(interface = \"...\")]` 暂不支持 impl 级 where 子句",
        ));
    }

    let self_ty = (*item.self_ty).clone();

    let mut impl_item = item.clone();
    let mut methods = Vec::new();
    for impl_item_entry in &mut impl_item.items {
        let ImplItem::Fn(method) = impl_item_entry else {
            continue;
        };
        let Some(endpoint) = parse_method_endpoint_(&method.attrs)? else {
            // 非端点方法：原样保留，不进入端点列表。
            continue;
        };
        if method.sig.asyncness.is_none() && !matches!(endpoint.method_, MethodKind_::Pull) {
            return Result::Err(syn::Error::new(
                method.sig.ident.span(),
                "`#[rpc(interface = \"...\")]` 的端点方法必须是 `async fn`（宏会把它重写成 \
                 `gen_may_cancel_future`）；`Pull` 端点用普通 `fn` 返回 \
                 `impl TrAsyncIterator`",
            ));
        }
        // 尾参取消令牌：unary 端点可显式声明 `cancel: K where K: TrCancellationToken`；
        // Pull 端点是普通 fn，不支持取消令牌，仍拒绝任何泛型。
        let token_param = if matches!(endpoint.method_, MethodKind_::Pull) {
            reject_pull_generics_(method)?;
            Option::None
        } else {
            split_token_param_(&method.sig)?
        };
        // 剥离端点声明属性；其余属性（文档、`#[cfg]` 等）保留。
        method.attrs.retain(|attr| !attr.path().is_ident("rpc"));
        methods.push(EndpointMethodModel_ {
            ident_: method.sig.ident.clone(),
            sig_: method.sig.clone(),
            block_: method.block.clone(),
            attrs_: cfg_attrs_(&method.attrs),
            endpoint_: endpoint,
            token_param_: token_param,
        });
    }

    Result::Ok(ControllerModel_ {
        self_ty_: self_ty,
        impl_item_: impl_item,
        path_: interface,
        methods_: methods,
    })
}

/// 解析自由函数的 `#[rpc(method = "full/path")]` 参数。
pub(crate) fn parse_free_fn_args_(args: &zyn::Args) -> syn::Result<(MethodKind_, String)> {
    let mut method = Option::None;
    let mut path = Option::None;
    for arg in args {
        let Option::Some(name) = arg.name() else {
            return Result::Err(syn::Error::new(
                arg_span_(arg),
                "自由函数 `#[rpc]` 参数需要写成 `method = \"full/path\"`",
            ));
        };
        let name_string = name.to_string();
        let Option::Some(kind) = method_kind_from_name_(name) else {
            return Result::Err(syn::Error::new(
                arg_span_(arg),
                format!(
                    "不认识的访问方法 `{name_string}`；支持 call / view / head / post / drop / push / pull"
                ),
            ));
        };
        if method.is_some() {
            return Result::Err(syn::Error::new(arg_span_(arg), "访问方法只能声明一次"));
        }
        let (value, span) = string_arg_(arg, &name_string)?;
        validate_endpoint_path_(&value, span)?;
        method = Option::Some(kind);
        path = Option::Some(value.trim_matches('/').to_string());
    }
    let (Option::Some(method), Option::Some(path)) = (method, path) else {
        return Result::Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "自由函数 `#[rpc]` 需要指定访问方法，例如 `#[rpc(call = \"scene/join\")]`",
        ));
    };
    Result::Ok((method, path))
}

/// 拆分端点方法的尾参取消令牌（`K: TrCancellationToken`）。
///
/// 这是 `gen_mcf2` 所要求的形状：最后一个泛型类型参数是取消令牌、最后一个函数参数按值
/// 接收它。返回 `None` 表示方法没有泛型（宏随后自行注入 `__C`）；返回 `Some` 表示用户
/// 显式声明了令牌，宏复用而不重复注入。其它泛型参数 / 生命周期 / const 泛型一律拒绝。
fn split_token_param_(sig: &Signature) -> syn::Result<Option<TokenParam_>> {
    let generics = &sig.generics;

    let mut type_params = Vec::new();
    for param in &generics.params {
        match param {
            GenericParam::Type(ty) => type_params.push(ty.ident.clone()),
            GenericParam::Lifetime(lt) => {
                return Result::Err(syn::Error::new(
                    lt.span(),
                    "端点方法暂不支持生命周期参数；借用请放到参数类型上（`&str` 之类）",
                ));
            }
            GenericParam::Const(c) => {
                return Result::Err(syn::Error::new(c.span(), "端点方法暂不支持 const 泛型参数"));
            }
        }
    }

    if type_params.is_empty() {
        if let Some(where_clause) = &generics.where_clause {
            return Result::Err(syn::Error::new(
                where_clause.span(),
                "端点方法没有泛型参数，却带了 where 子句；取消令牌请写成 \
                 `async fn f<K>(..., cancel: K) where K: TrCancellationToken`",
            ));
        }
        return Result::Ok(Option::None);
    }

    if type_params.len() != 1usize {
        return Result::Err(syn::Error::new(
            generics.span(),
            "端点方法只支持一个泛型类型参数，且它必须是尾参取消令牌（`K: TrCancellationToken`）",
        ));
    }
    let ty_ident = &type_params[0];

    // 校验 where 子句里 `#ty_ident: ... TrCancellationToken` 这条约束。
    let has_token_bound = generics.where_clause.as_ref().is_some_and(|where_clause| {
        where_clause.predicates.iter().any(|predicate| {
            let WherePredicate::Type(pred_ty) = predicate else {
                return false;
            };
            let Type::Path(pred_path) = &pred_ty.bounded_ty else {
                return false;
            };
            if !pred_path.path.is_ident(ty_ident) {
                return false;
            }
            pred_ty.bounds.iter().any(|bound| match bound {
                TypeParamBound::Trait(trait_bound) => trait_bound
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident == "TrCancellationToken")
                    .unwrap_or(false),
                _ => false,
            })
        })
    });
    if !has_token_bound {
        return Result::Err(syn::Error::new(
            ty_ident.span(),
            "端点方法的泛型参数必须是尾参取消令牌：`where #ty_ident: TrCancellationToken`",
        ));
    }

    // 最后一个（非 self）参数必须是该令牌类型。
    let last_typed = sig
        .inputs
        .iter()
        .filter_map(|input| match input {
            FnArg::Typed(pat_type) => Option::Some(pat_type),
            FnArg::Receiver(_) => Option::None,
        })
        .next_back();
    let Option::Some(last_typed) = last_typed else {
        return Result::Err(syn::Error::new(
            sig.span(),
            "端点方法至少需要一个业务参数；取消令牌 `cancel: K` 要放在最后",
        ));
    };
    let Pat::Ident(pattern) = &*last_typed.pat else {
        return Result::Err(syn::Error::new(
            last_typed.pat.span(),
            "取消令牌参数必须是简单标识符",
        ));
    };
    let Type::Path(last_ty_path) = &*last_typed.ty else {
        return Result::Err(syn::Error::new(
            last_typed.ty.span(),
            "取消令牌参数的类型必须是泛型参数名（`cancel: K`）",
        ));
    };
    if !last_ty_path.path.is_ident(ty_ident) {
        return Result::Err(syn::Error::new(
            last_typed.ty.span(),
            "取消令牌 `cancel: K` 必须是最后一个参数，且类型与泛型参数一致",
        ));
    }

    Result::Ok(Option::Some(TokenParam_ {
        param_ident_: pattern.ident.clone(),
        ty_ident_: ty_ident.clone(),
    }))
}

/// `Pull` 端点方法不允许泛型（它是普通 `fn`，不参与 `gen_mcf2` 的取消令牌协议）。
fn reject_pull_generics_(method: &ImplItemFn) -> syn::Result<()> {
    if !method.sig.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            method.sig.generics.span(),
            "`Pull` 端点方法暂不支持泛型参数",
        ));
    }
    if let Some(where_clause) = &method.sig.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`Pull` 端点方法暂不支持 where 子句",
        ));
    }
    Result::Ok(())
}

/// 分析一个自由函数 handler：`#[rpc(method = "full/path")] async fn ...`。
///
/// 与 controller 不同：没有 `self`、路径取端点声明里的完整值（无 interface 前缀）、
/// 也没有客户端代理。端点方法规则与 controller 一致（`async fn` 或 `Pull` 返回迭代器、
/// 尾参取消令牌）。
pub(crate) fn analyze_free_fn_(
    item: &ItemFn,
    method: MethodKind_,
    path: String,
) -> syn::Result<FreeFnModel_> {
    let endpoint = EndpointSpec_ {
        method_: method,
        path_: path,
    };
    if item.sig.asyncness.is_none() && !matches!(endpoint.method_, MethodKind_::Pull) {
        return Result::Err(syn::Error::new(
            item.sig.ident.span(),
            "自由函数 handler 必须是 `async fn`（宏会把它重写成 `gen_may_cancel_future`）；\
             `Pull` 端点用普通 `fn` 返回 `impl TrAsyncIterator`",
        ));
    }
    let token_param = if matches!(endpoint.method_, MethodKind_::Pull) {
        if !item.sig.generics.params.is_empty() {
            return Result::Err(syn::Error::new(
                item.sig.generics.span(),
                "`Pull` 自由函数暂不支持泛型参数",
            ));
        }
        if let Some(where_clause) = &item.sig.generics.where_clause {
            return Result::Err(syn::Error::new(
                where_clause.span(),
                "`Pull` 自由函数暂不支持 where 子句",
            ));
        }
        Option::None
    } else {
        split_token_param_(&item.sig)?
    };

    let mut fn_item = item.clone();
    fn_item.attrs.retain(|attr| !attr.path().is_ident("rpc"));
    let method = EndpointMethodModel_ {
        ident_: item.sig.ident.clone(),
        sig_: item.sig.clone(),
        block_: (*item.block).clone(),
        attrs_: cfg_attrs_(&fn_item.attrs),
        endpoint_: endpoint,
        token_param_: token_param,
    };
    Result::Ok(FreeFnModel_ {
        fn_item_: fn_item,
        method_: method,
    })
}

/// 分析一个 Call service trait（`#[rpc(service = "...", api = X)] trait`）。
///
/// 生成的枚举变体、方法表项与客户端方法都由这里的方法列表一一对应；因此分析层要求：
/// 方法都是 `&self`、无泛型 / 无默认实现、返回
/// `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`、参数能稳定转成 owned 载荷。
pub(crate) fn analyze_service_(
    item: &ItemTrait,
    prefix: String,
    api: syn::Ident,
) -> syn::Result<ServiceModel_> {
    if item.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            item.unsafety.span(),
            "`#[rpc(service = \"...\", api = X)]` 不支持 unsafe trait",
        ));
    }
    if item.auto_token.is_some() {
        return Result::Err(syn::Error::new(
            item.auto_token.span(),
            "`#[rpc(service = \"...\", api = X)]` 不支持 auto trait",
        ));
    }
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc(service = \"...\", api = X)]` 暂不支持泛型 trait",
        ));
    }
    if let Option::Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc(service = \"...\", api = X)]` 暂不支持 trait 级 where 子句",
        ));
    }

    let mut methods = Vec::new();
    for trait_item in &item.items {
        match trait_item {
            TraitItem::Fn(trait_fn) => {
                if trait_fn.default.is_some() {
                    return Result::Err(syn::Error::new(
                        trait_fn.sig.ident.span(),
                        "Call service trait 的方法不支持默认实现；请把它拆到普通 trait / 固有 \
                         impl（默认实现会让 `impl_of` 的方法表缺项）",
                    ));
                }
                methods.push(analyze_call_method_(&trait_fn.sig, &trait_fn.attrs)?);
            }
            TraitItem::Type(trait_ty) => {
                let name = trait_ty.ident.to_string();
                if name == "__Call" || name.starts_with("__Req_") {
                    return Result::Err(syn::Error::new(
                        trait_ty.ident.span(),
                        "`__Call` / `__Req_*` 是 `#[rpc]` 生成的隐藏关联类型名，不能手写",
                    ));
                }
            }
            _ => {}
        }
    }
    check_call_method_names_(&methods)?;

    let trait_ident = item.ident.clone();
    let call_enum_ident = crate::model_::call_enum_ident_(&trait_ident);
    Result::Ok(ServiceModel_ {
        trait_item_: item.clone(),
        trait_ident_: trait_ident,
        prefix_: prefix,
        api_: api,
        call_enum_ident_: call_enum_ident,
        methods_: methods,
    })
}

/// 分析一个 Call service 的 trait impl（`#[rpc(impl_of(X))] impl Trait for Impl`）。
pub(crate) fn analyze_impl_of_(item: &ItemImpl, api: Path) -> syn::Result<ImplOfModel_> {
    if item.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            item.unsafety.span(),
            "`#[rpc(impl_of(X))]` 不支持 unsafe impl",
        ));
    }
    if item.defaultness.is_some() {
        return Result::Err(syn::Error::new(
            item.defaultness.span(),
            "`#[rpc(impl_of(X))]` 不支持 default impl",
        ));
    }
    let Option::Some((_, trait_path, _)) = &item.trait_ else {
        return Result::Err(syn::Error::new(
            item.self_ty.span(),
            "`#[rpc(impl_of(X))]` 只能用于 trait impl（`impl RoomService for RoomServiceImpl`）",
        ));
    };
    if !item.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            item.generics.span(),
            "`#[rpc(impl_of(X))]` 暂不支持泛型 impl",
        ));
    }
    if let Option::Some(where_clause) = &item.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "`#[rpc(impl_of(X))]` 暂不支持 impl 级 where 子句",
        ));
    }

    let self_ty = (*item.self_ty).clone();
    let Some(self_ident) = simple_type_ident_(&self_ty) else {
        return Result::Err(syn::Error::new(
            self_ty.span(),
            "`#[rpc(impl_of(X))]` 的自类型必须是简单路径类型，例如 `RoomServiceImpl`",
        ));
    };
    let Some(api_ident) = api.segments.last().map(|segment| segment.ident.clone()) else {
        return Result::Err(syn::Error::new(
            api.span(),
            "`impl_of(...)` 需要一个 marker 标识符",
        ));
    };

    let mut methods = Vec::new();
    for impl_item in &item.items {
        match impl_item {
            ImplItem::Fn(method) => {
                methods.push(analyze_call_method_(&method.sig, &method.attrs)?);
            }
            ImplItem::Type(trait_ty) => {
                let name = trait_ty.ident.to_string();
                if name == "__Call" || name.starts_with("__Req_") {
                    return Result::Err(syn::Error::new(
                        trait_ty.ident.span(),
                        "不要覆盖 `#[rpc]` 生成的隐藏关联类型（`__Call` / `__Req_*`）：\
                         服务端的请求枚举与载荷类型必须与协议 crate 一致",
                    ));
                }
            }
            _ => {}
        }
    }
    check_call_method_names_(&methods)?;

    Result::Ok(ImplOfModel_ {
        impl_item_: item.clone(),
        self_ty_: self_ty,
        self_ident_: self_ident,
        trait_path_: trait_path.clone(),
        api_path_: api,
        api_ident_: api_ident,
        methods_: methods,
    })
}

/// 分析一个 Call 方法：`&self`、无泛型、返回 `impl TrMayCancel<... RpcResult<T>>`。
fn analyze_call_method_(sig: &Signature, attrs: &[Attribute]) -> syn::Result<CallMethodModel_> {
    if sig.asyncness.is_some() {
        return Result::Err(syn::Error::new(
            sig.ident.span(),
            "Call service 方法是普通 `fn`，返回 `impl TrMayCancel<...>`；不要写成 `async fn`",
        ));
    }
    if sig.unsafety.is_some() {
        return Result::Err(syn::Error::new(
            sig.unsafety.span(),
            "Call service 方法不支持 unsafe",
        ));
    }
    if sig.abi.is_some() {
        return Result::Err(syn::Error::new(
            sig.abi.span(),
            "Call service 方法不支持 extern ABI",
        ));
    }
    if sig.variadic.is_some() {
        return Result::Err(syn::Error::new(
            sig.variadic.span(),
            "Call service 方法不支持可变参数",
        ));
    }
    if !sig.generics.params.is_empty() {
        return Result::Err(syn::Error::new(
            sig.generics.span(),
            "Call service 方法暂不支持泛型参数",
        ));
    }
    if let Option::Some(where_clause) = &sig.generics.where_clause {
        return Result::Err(syn::Error::new(
            where_clause.span(),
            "Call service 方法暂不支持 where 子句",
        ));
    }
    if attrs.iter().any(|attr| attr.path().is_ident("rpc")) {
        return Result::Err(syn::Error::new(
            sig.ident.span(),
            "Call service trait 的方法不需要 `#[rpc(...)]`；方法名就是 operation",
        ));
    }

    let mut params = Vec::new();
    for input in &sig.inputs {
        match input {
            FnArg::Receiver(receiver) => {
                if receiver.reference.is_none() || receiver.mutability.is_some() {
                    return Result::Err(syn::Error::new(
                        receiver.span(),
                        "Call service 方法的接收者必须是 `&self`（不能是 `self` / `&mut self` / \
                         显式类型 self）",
                    ));
                }
                if receiver.colon_token.is_some() {
                    return Result::Err(syn::Error::new(
                        receiver.span(),
                        "Call service 方法不支持显式类型 self（`self: ...`）",
                    ));
                }
            }
            FnArg::Typed(pat_type) => {
                let Pat::Ident(pattern) = &*pat_type.pat else {
                    return Result::Err(syn::Error::new(
                        pat_type.pat.span(),
                        "Call service 方法的参数必须是简单标识符",
                    ));
                };
                let (owned_ty, borrow_) = owned_payload_ty_(&pat_type.ty)?;
                params.push(CallParam_ {
                    ident_: pattern.ident.clone(),
                    owned_ty_: owned_ty,
                    borrow_,
                });
            }
        }
    }
    if !sig
        .inputs
        .iter()
        .any(|input| matches!(input, FnArg::Receiver(_)))
    {
        return Result::Err(syn::Error::new(
            sig.ident.span(),
            "Call service 方法必须有 `&self` 接收者",
        ));
    }

    let response_ty = response_ty_(&sig.output)?;
    let method_ident = sig.ident.clone();
    let payload = match params.len() {
        0usize => PayloadShape_::Unit,
        1usize => PayloadShape_::Single,
        _ => PayloadShape_::Struct,
    };
    let req_assoc_ident = crate::model_::req_assoc_ident_(&method_ident);
    let variant_ident = crate::model_::variant_ident_(&method_ident);

    Result::Ok(CallMethodModel_ {
        ident_: method_ident,
        sig_: sig.clone(),
        attrs_: attrs.to_vec(),
        cfg_attrs_: cfg_attrs_(attrs),
        params_: params,
        response_ty_: response_ty,
        payload_: payload,
        req_assoc_ident_: req_assoc_ident,
        variant_ident_: variant_ident,
    })
}

/// 检查方法名 / 生成标识符不重复。
fn check_call_method_names_(methods: &[CallMethodModel_]) -> syn::Result<()> {
    for (index, method) in methods.iter().enumerate() {
        for other in &methods[..index] {
            if method.ident_ == other.ident_ {
                return Result::Err(syn::Error::new(
                    method.ident_.span(),
                    format!("Call service trait 里方法名 `{}` 重复", method.ident_),
                ));
            }
            if method.variant_ident_ == other.variant_ident_ {
                return Result::Err(syn::Error::new(
                    method.ident_.span(),
                    format!(
                        "方法 `{}` 与 `{}` 生成的枚举变体都是 `{}`；请改名",
                        method.ident_, other.ident_, method.variant_ident_
                    ),
                ));
            }
        }
    }
    Result::Ok(())
}

/// 把方法参数类型转成线上载荷的 owned 类型。
///
/// 借用只接受白名单：`&str` / `&[u8]` / `&String` / `&Vec<u8>` / `&Copy 标量`；其余借用
/// （含嵌套借用）一律拒绝——线上载荷必须是自包含的 owned 值。
fn owned_payload_ty_(ty: &Type) -> syn::Result<(Type, BorrowMode_)> {
    if let Type::Reference(reference) = ty {
        if reference.mutability.is_some() {
            return Result::Err(syn::Error::new(
                reference.span(),
                "Call service 方法不支持 `&mut` 参数；请改成 owned 类型",
            ));
        }
        if reference.lifetime.is_some() {
            return Result::Err(syn::Error::new(
                reference.span(),
                "Call service 方法不支持带显式生命周期的借用参数",
            ));
        }
        let elem = &*reference.elem;
        if is_path_ident_(elem, "str") {
            return Result::Ok((syn::parse_quote!(String), BorrowMode_::Str));
        }
        if let Type::Slice(slice) = elem
            && is_path_ident_(&slice.elem, "u8")
        {
            return Result::Ok((syn::parse_quote!(::std::vec::Vec<u8>), BorrowMode_::Bytes));
        }
        if is_path_ident_(elem, "String") {
            return Result::Ok((syn::parse_quote!(String), BorrowMode_::Clone));
        }
        if let Type::Path(type_path) = elem
            && let Some(segment) = type_path.path.segments.last()
            && segment.ident == "Vec"
            && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
            && args.args.len() == 1usize
            && matches!(args.args.first(), Option::Some(syn::GenericArgument::Type(inner)) if is_path_ident_(inner, "u8"))
        {
            return Result::Ok((syn::parse_quote!(::std::vec::Vec<u8>), BorrowMode_::Clone));
        }
        if let Type::Path(type_path) = elem
            && type_path.qself.is_none()
            && type_path.path.segments.len() == 1usize
            && is_copy_scalar_(type_path.path.segments[0].ident.to_string().as_str())
        {
            return Result::Ok(((*elem).clone(), BorrowMode_::Copy));
        }
        return Result::Err(syn::Error::new(
            reference.span(),
            "Call service 方法只支持借用 `&str` / `&[u8]` / `&String` / `&Vec<u8>` / `&Copy 标量`；\
             其它借用请改成 owned 类型（或换成 `&str` 之类）",
        ));
    }

    let mut finder = BorrowFinder_ {
        found_: Option::None,
    };
    finder.visit_type(ty);
    if let Option::Some(span) = finder.found_ {
        return Result::Err(syn::Error::new(
            span,
            "Call service 方法的线上载荷必须是 owned 值；请去掉嵌套借用",
        ));
    }
    Result::Ok((ty.clone(), BorrowMode_::Owned))
}

/// `&Copy` 标量白名单。
fn is_copy_scalar_(name: &str) -> bool {
    matches!(
        name,
        "u8" | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "bool"
            | "char"
            | "f32"
            | "f64"
    )
}

/// 类型是不是单段路径标识符（`str` / `String` / `u8`）。
fn is_path_ident_(ty: &Type, name: &str) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path.qself.is_none() && type_path.path.is_ident(name)
}

/// 在类型里查找任意借用。
struct BorrowFinder_ {
    /// 找到的第一个借用 span。
    found_: Option<proc_macro2::Span>,
}

impl Visit<'_> for BorrowFinder_ {
    fn visit_type_reference(&mut self, reference: &syn::TypeReference) {
        if self.found_.is_none() {
            self.found_ = Option::Some(reference.span());
        }
        visit::visit_type_reference(self, reference);
    }
}

/// 从 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>` 里取 `T`。
fn response_ty_(output: &ReturnType) -> syn::Result<Type> {
    let ty: &Type = match output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Result::Err(syn::Error::new(
                output.span(),
                "Call service 方法必须返回 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`",
            ));
        }
    };
    let Type::ImplTrait(impl_trait) = ty else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "Call service 方法必须返回 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`",
        ));
    };
    let mut cancel_bound = Option::None;
    for bound in &impl_trait.bounds {
        match bound {
            TypeParamBound::Trait(trait_bound) => {
                let Some(segment) = trait_bound.path.segments.last() else {
                    continue;
                };
                if segment.ident == "TrMayCancel" {
                    if cancel_bound.is_some() {
                        return Result::Err(syn::Error::new(
                            trait_bound.span(),
                            "`TrMayCancel` 只能出现一次",
                        ));
                    }
                    cancel_bound = Option::Some(trait_bound);
                } else {
                    return Result::Err(syn::Error::new(
                        trait_bound.span(),
                        "Call service 方法的返回类型只支持 `impl TrMayCancel<...>`；\
                         不支持额外的 trait bound（如 `+ Send`）",
                    ));
                }
            }
            other => {
                return Result::Err(syn::Error::new(
                    other.span(),
                    "Call service 方法的返回类型只支持 `impl TrMayCancel<...>`",
                ));
            }
        }
    }
    let Some(cancel_bound) = cancel_bound else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "Call service 方法必须返回 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`",
        ));
    };
    let Some(cancel_segment) = cancel_bound.path.segments.last() else {
        return Result::Err(syn::Error::new(
            cancel_bound.span(),
            "`TrMayCancel` 路径不能为空",
        ));
    };
    let syn::PathArguments::AngleBracketed(args) = &cancel_segment.arguments else {
        return Result::Err(syn::Error::new(
            cancel_bound.span(),
            "`TrMayCancel` 需要写成 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`",
        ));
    };
    for arg in &args.args {
        let syn::GenericArgument::AssocType(assoc) = arg else {
            continue;
        };
        if assoc.ident != "MayCancelOutput" {
            continue;
        }
        let Type::Path(rpc_result_path) = &assoc.ty else {
            break;
        };
        let Some(segment) = rpc_result_path.path.segments.last() else {
            break;
        };
        if segment.ident != "RpcResult" {
            break;
        }
        let syn::PathArguments::AngleBracketed(result_args) = &segment.arguments else {
            break;
        };
        if let Option::Some(syn::GenericArgument::Type(ok_ty)) = result_args.args.first() {
            return Result::Ok(ok_ty.clone());
        }
        break;
    }
    Result::Err(syn::Error::new(
        ty.span(),
        "Call service 方法必须返回 `impl TrMayCancel<'_, MayCancelOutput = RpcResult<T>>`",
    ))
}

/// 取一个简单路径类型的最后一段标识符。
fn simple_type_ident_(ty: &Type) -> Option<syn::Ident> {
    let Type::Path(type_path) = ty else {
        return Option::None;
    };
    if type_path.qself.is_some() {
        return Option::None;
    }
    type_path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.clone())
}

/// 从方法的 `#[rpc(...)]` 属性里解析端点声明；方法没有 `#[rpc]` 时返回 `None`。
fn parse_method_endpoint_(attrs: &[Attribute]) -> syn::Result<Option<EndpointSpec_>> {
    let mut found = Option::None;
    for attr in attrs {
        if !attr.path().is_ident("rpc") {
            continue;
        }
        if found.is_some() {
            return Result::Err(syn::Error::new(
                attr.span(),
                "一个方法只能有一个 `#[rpc(...)]` 端点声明",
            ));
        }
        found = Option::Some(attr);
    }
    let Option::Some(attr) = found else {
        return Result::Ok(Option::None);
    };

    let Meta::List(list) = &attr.meta else {
        return Result::Err(syn::Error::new(
            attr.span(),
            "方法上的 `#[rpc]` 需要写成 `#[rpc(call = \"join\")]` 之类的端点声明",
        ));
    };
    let metas = list
        .parse_args_with(syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated)
        .map_err(|err| syn::Error::new(err.span(), format!("解析方法端点声明失败：{err}")))?;

    let mut method = Option::None;
    let mut path = Option::None;
    for meta in metas {
        let Meta::NameValue(name_value) = meta else {
            return Result::Err(syn::Error::new(
                meta.span(),
                "端点声明的每一项都要写成 `方法名 = \"路径\"`，例如 `call = \"join\"`",
            ));
        };
        let Option::Some(kind_name) = name_value.path.get_ident() else {
            return Result::Err(syn::Error::new(
                name_value.path.span(),
                "访问方法名必须是简单标识符",
            ));
        };
        if method.is_some() {
            return Result::Err(syn::Error::new(kind_name.span(), "访问方法只能声明一次"));
        }
        let Expr::Lit(expr_lit) = name_value.value else {
            return Result::Err(syn::Error::new(
                name_value.value.span(),
                "端点路径需要写成字符串字面量",
            ));
        };
        let Lit::Str(lit_str) = expr_lit.lit else {
            return Result::Err(syn::Error::new(
                expr_lit.lit.span(),
                "端点路径需要写成字符串字面量",
            ));
        };
        let Option::Some(kind) = method_kind_from_name_(kind_name) else {
            return Result::Err(syn::Error::new(
                kind_name.span(),
                format!(
                    "不认识的访问方法 `{kind_name}`；支持 call / view / head / post / drop / push / pull"
                ),
            ));
        };
        let value = lit_str.value();
        validate_endpoint_path_(&value, lit_str.span())?;
        method = Option::Some(kind);
        path = Option::Some(value.trim_matches('/').to_string());
    }

    let (Option::Some(method), Option::Some(path)) = (method, path) else {
        return Result::Err(syn::Error::new(
            attr.span(),
            "方法端点声明需要指定访问方法，例如 `#[rpc(call = \"join\")]`",
        ));
    };
    Result::Ok(Option::Some(EndpointSpec_ {
        method_: method,
        path_: path,
    }))
}

/// 访问方法名 → [`MethodKind_`]。
fn method_kind_from_name_(name: &syn::Ident) -> Option<MethodKind_> {
    match name.to_string().as_str() {
        "call" => Option::Some(MethodKind_::Call),
        "view" => Option::Some(MethodKind_::View),
        "head" => Option::Some(MethodKind_::Head),
        "post" => Option::Some(MethodKind_::Post),
        "drop" => Option::Some(MethodKind_::Drop),
        "push" => Option::Some(MethodKind_::Push),
        "pull" => Option::Some(MethodKind_::Pull),
        _ => Option::None,
    }
}

/// 校验端点路径：非空、无空段、无空白；允许首尾 `/`（分析时统一 `trim_matches('/')`）。
fn validate_endpoint_path_(path: &str, span: proc_macro2::Span) -> syn::Result<()> {
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Result::Err(syn::Error::new(span, "端点路径不能为空"));
    }
    if trimmed.split('/').any(str::is_empty) {
        return Result::Err(syn::Error::new(span, "端点路径不能包含空段（例如 `a//b`）"));
    }
    if path.chars().any(char::is_whitespace) {
        return Result::Err(syn::Error::new(span, "端点路径不能包含空白字符"));
    }
    Result::Ok(())
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

    /// 测试 controller 分析能从固有 impl 抽出端点方法，并跳过非端点辅助方法。
    /// Test that controller analysis extracts endpoint methods and skips helper methods.
    ///
    /// - 手段：构造一个带 `#[rpc(call = "join")]` 端点与一个普通辅助方法的 `impl`。
    /// - 判断：interface 为 `scene`，端点列表只含 `join`（`Call`、路径 `join`）。
    ///
    /// - Means: build an impl with one `#[rpc(call = "join")]` endpoint and one helper.
    /// - Verdict: the interface is `scene` and the endpoint list holds only `join`.
    #[test]
    fn analyzes_controller_() {
        let item: ItemImpl = parse_quote!(
            impl SceneService {
                #[rpc(call = "join")]
                async fn join(&self, req: u64) -> u64 { req }

                fn helper_(&self) {}
            }
        );
        let model =
            analyze_controller_(&item, "scene".to_string()).expect("controller 应当分析成功");

        assert_eq!(model.path_, "scene");
        assert_eq!(model.methods_.len(), 1usize);
        assert_eq!(model.methods_[0].ident_.to_string(), "join");
        assert_eq!(model.methods_[0].endpoint_.method_, MethodKind_::Call);
        assert_eq!(model.methods_[0].endpoint_.path_, "join");
    }

    /// 测试 controller 分析拒绝 trait impl 与泛型 impl。
    /// Test that controller analysis rejects trait impls and generic impls.
    ///
    /// - 手段：分别构造 `impl SomeTrait for SceneService {}` 与 `impl<T> SceneService<T> {}`。
    /// - 判断：两者都返回 `Err`。
    ///
    /// - Means: build a trait impl and a generic impl.
    /// - Verdict: both are rejected.
    #[test]
    fn rejects_bad_controller_impls_() {
        let trait_impl: ItemImpl = parse_quote!(impl SomeTrait for SceneService {});
        assert!(
            analyze_controller_(&trait_impl, "scene".to_string()).is_err(),
            "trait impl 应当被拒绝"
        );

        let generic: ItemImpl = parse_quote!(
            impl<T> SceneService<T> {}
        );
        assert!(
            analyze_controller_(&generic, "scene".to_string()).is_err(),
            "泛型 impl 应当被拒绝"
        );
    }

    /// 测试端点属性解析拒绝未知访问方法与空路径。
    /// Test that endpoint parsing rejects unknown methods and empty paths.
    ///
    /// - 手段：分别构造 `#[rpc(foo = "join")]` 与 `#[rpc(call = "")]`。
    /// - 判断：两者都返回 `Err`。
    ///
    /// - Means: build an unknown method kind and an empty path.
    /// - Verdict: both are rejected.
    #[test]
    fn rejects_bad_endpoint_() {
        let unknown: ItemImpl = parse_quote!(
            impl SceneService {
                #[rpc(foo = "join")]
                async fn join(&self) {}
            }
        );
        assert!(
            analyze_controller_(&unknown, "scene".to_string()).is_err(),
            "未知访问方法应当被拒绝"
        );

        let empty: ItemImpl = parse_quote!(
            impl SceneService {
                #[rpc(call = "")]
                async fn join(&self) {}
            }
        );
        assert!(
            analyze_controller_(&empty, "scene".to_string()).is_err(),
            "空路径应当被拒绝"
        );
    }

    /// 测试访问方法名到 [`MethodKind_`] 的映射覆盖全部七种方法。
    /// Test that the method-name mapping covers all seven access methods.
    ///
    /// - 手段：把七种方法名与一个未知名交给 `method_kind_from_name_`。
    /// - 判断：七种都得到对应变体，未知名得到 `None`。
    ///
    /// - Means: map seven names and one unknown name.
    /// - Verdict: the seven map correctly and the unknown yields `None`.
    #[test]
    fn method_kind_maps_() {
        fn kind(name: &str) -> Option<MethodKind_> {
            method_kind_from_name_(&zyn::format_ident!("{}", name))
        }
        assert_eq!(kind("call"), Option::Some(MethodKind_::Call));
        assert_eq!(kind("view"), Option::Some(MethodKind_::View));
        assert_eq!(kind("head"), Option::Some(MethodKind_::Head));
        assert_eq!(kind("post"), Option::Some(MethodKind_::Post));
        assert_eq!(kind("drop"), Option::Some(MethodKind_::Drop));
        assert_eq!(kind("push"), Option::Some(MethodKind_::Push));
        assert_eq!(kind("pull"), Option::Some(MethodKind_::Pull));
        assert_eq!(kind("foo"), Option::None);
    }

    /// 测试 service trait 分析收集方法、载荷形状与借用转换。
    /// Test that service analysis collects methods, payload shapes and borrow conversion.
    ///
    /// - 手段：解析一个含 0 参 / 1 参 / 多参（带 `&str`）方法的 trait，交给
    ///   `analyze_service_`。
    /// - 判断：前缀、marker、枚举名正确；三个方法的载荷形状分别是 Unit / Single / Struct；
    ///   `&str` 被转换成 owned `String` 并标记为 `BorrowMode_::Str`。
    ///
    /// - Means: parse a trait with 0-arg / 1-arg / multi-arg (with `&str`) methods and hand it
    ///   to `analyze_service_`.
    /// - Verdict: prefix, marker and enum name are right; payload shapes are Unit / Single /
    ///   Struct; `&str` becomes an owned `String` marked `BorrowMode_::Str`.
    #[test]
    fn analyzes_service_trait_() {
        let item: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn list_my_rooms(
                    &self,
                ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<Vec<u64>>>;
                fn join_room(
                    &self,
                    room_id: u64,
                ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>>;
                fn send_message(
                    &self,
                    room_id: u64,
                    text: &str,
                ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<String>>;
            }
        );
        let model = analyze_service_(
            &item,
            "im/v1/room".to_string(),
            zyn::format_ident!("RoomSvc"),
        )
        .expect("service trait 应当分析成功");
        assert_eq!(model.prefix_, "im/v1/room");
        assert_eq!(model.api_.to_string(), "RoomSvc");
        assert_eq!(model.call_enum_ident_.to_string(), "RoomServiceCall");
        assert_eq!(model.methods_.len(), 3usize);
        assert_eq!(model.methods_[0].payload_, PayloadShape_::Unit);
        assert_eq!(model.methods_[1].payload_, PayloadShape_::Single);
        assert_eq!(model.methods_[2].payload_, PayloadShape_::Struct);
        assert_eq!(model.methods_[2].variant_ident_.to_string(), "SendMessage");
        assert_eq!(
            model.methods_[2].req_assoc_ident_.to_string(),
            "__Req_send_message"
        );
        assert_eq!(model.methods_[2].params_[1].borrow_, BorrowMode_::Str);
        assert_eq!(
            model.methods_[2].params_[1].owned_ty_,
            parse_quote!(String),
            "&str 必须转成 owned String"
        );
    }

    /// 测试 service trait 分析拒绝无法稳定生成的形状。
    /// Test that service analysis rejects shapes that cannot be generated deterministically.
    ///
    /// - 手段：分别构造默认实现、非 `&self`、泛型方法、`async fn`、不支持借用与重复方法的
    ///   trait。
    /// - 判断：全部返回 `Err`。
    ///
    /// - Means: build traits with a default body, a non-`&self` receiver, a generic method,
    ///   an `async fn`, an unsupported borrow, and a duplicate method.
    /// - Verdict: every case is rejected.
    #[test]
    fn rejects_bad_service_shapes_() {
        let prefix = "im/v1/room".to_string();
        let api = || zyn::format_ident!("RoomSvc");

        let with_default: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>> {}
            }
        );
        assert!(
            analyze_service_(&with_default, prefix.clone(), api()).is_err(),
            "默认实现应当被拒绝"
        );

        let by_value: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn ping(self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
            }
        );
        assert!(
            analyze_service_(&by_value, prefix.clone(), api()).is_err(),
            "按值 self 应当被拒绝"
        );

        let generic: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn ping<T>(
                    &self,
                    value: T,
                ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
            }
        );
        assert!(
            analyze_service_(&generic, prefix.clone(), api()).is_err(),
            "泛型方法应当被拒绝"
        );

        let async_fn: ItemTrait = parse_quote!(
            pub trait RoomService {
                async fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
            }
        );
        assert!(
            analyze_service_(&async_fn, prefix.clone(), api()).is_err(),
            "async fn 应当被拒绝"
        );

        let bad_borrow: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn ping(
                    &self,
                    value: &Vec<String>,
                ) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
            }
        );
        assert!(
            analyze_service_(&bad_borrow, prefix.clone(), api()).is_err(),
            "白名单外的借用应当被拒绝"
        );

        let duplicate: ItemTrait = parse_quote!(
            pub trait RoomService {
                fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
                fn ping(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<()>>;
            }
        );
        assert!(
            analyze_service_(&duplicate, prefix, api()).is_err(),
            "重复方法名应当被拒绝"
        );
    }

    /// 测试 impl_of 分析收集服务端方法并可跨 crate 命名隐藏关联类型。
    /// Test that `impl_of` analysis collects server methods for hidden associated types.
    ///
    /// - 手段：解析 `impl RoomService for RoomServiceImpl { ... }`，交给 `analyze_impl_of_`。
    /// - 判断：自类型 / 标识符 / trait 路径 / marker 标识符正确，方法数与形状与 trait 一致。
    ///
    /// - Means: parse `impl RoomService for RoomServiceImpl { ... }` and hand it to
    ///   `analyze_impl_of_`.
    /// - Verdict: self type, identifiers, trait path and marker are right; method count and
    ///   shapes match the trait.
    #[test]
    fn analyzes_impl_of_() {
        let item: ItemImpl = parse_quote!(
            impl RoomService for RoomServiceImpl {
                fn join_room(&self, room_id: u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>> {
                    todo!()
                }
            }
        );
        let model = analyze_impl_of_(&item, parse_quote!(RoomSvc)).expect("impl_of 应当分析成功");
        assert_eq!(model.self_ident_.to_string(), "RoomServiceImpl");
        assert_eq!(model.api_ident_.to_string(), "RoomSvc");
        assert_eq!(model.methods_.len(), 1usize);
        assert_eq!(model.methods_[0].payload_, PayloadShape_::Single);
    }

    /// 测试 impl_of 拒绝覆盖宏生成的隐藏关联类型。
    /// Test that `impl_of` rejects overriding the macro-generated hidden associated types.
    ///
    /// - 手段：构造一个显式定义 `type __Req_join_room = u64;` 的 trait impl。
    /// - 判断：返回 `Err`（否则服务端载荷会与协议 crate 的请求枚举不一致）。
    ///
    /// - Means: build a trait impl that redefines `type __Req_join_room = u64;`.
    /// - Verdict: it is rejected, because the server payload would otherwise diverge from
    ///   the protocol crate's request enum.
    #[test]
    fn rejects_hidden_assoc_type_override_() {
        let item: ItemImpl = parse_quote!(
            impl RoomService for RoomServiceImpl {
                type __Req_join_room = u64;

                fn join_room(&self, room_id: u64) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u64>> {
                    todo!()
                }
            }
        );
        assert!(
            analyze_impl_of_(&item, parse_quote!(RoomSvc)).is_err(),
            "覆盖隐藏关联类型应当被拒绝"
        );
    }

    /// 测试 `#[rpc]` 参数解析区分三种形态并拒绝混用。
    /// Test that `#[rpc]` argument parsing distinguishes the three forms and rejects mixing.
    ///
    /// - 手段：分别解析 controller / service / impl_of 三种参数串，再解析一个混合串。
    /// - 判断：前三种得到对应变体，混合串返回 `Err`。
    ///
    /// - Means: parse the controller / service / impl_of argument strings, then a mixed one.
    /// - Verdict: the first three yield the matching variant; the mixed string is rejected.
    #[test]
    fn parses_rpc_arg_forms_() {
        let controller: zyn::Args =
            syn::parse_str("interface = \"scene\"").expect("解析 controller 参数");
        assert!(matches!(
            parse_rpc_args_(&controller),
            Result::Ok(RpcArgs_::Controller { .. })
        ));

        let service: zyn::Args =
            syn::parse_str("service = \"im/v1/room\", api = RoomSvc").expect("解析 service 参数");
        match parse_rpc_args_(&service) {
            Result::Ok(RpcArgs_::Service { prefix, api }) => {
                assert_eq!(prefix, "im/v1/room");
                assert_eq!(api.to_string(), "RoomSvc");
            }
            _ => panic!("service 参数应当解析成 Service 形态"),
        }

        let impl_of: zyn::Args = syn::parse_str("impl_of(RoomSvc)").expect("解析 impl_of 参数");
        assert!(matches!(
            parse_rpc_args_(&impl_of),
            Result::Ok(RpcArgs_::ImplOf { .. })
        ));

        let mixed: zyn::Args =
            syn::parse_str("service = \"im/v1/room\", api = RoomSvc, impl_of(RoomSvc)")
                .expect("解析混合参数");
        assert!(
            parse_rpc_args_(&mixed).is_err(),
            "service 与 impl_of 不能同时出现"
        );
    }
}
