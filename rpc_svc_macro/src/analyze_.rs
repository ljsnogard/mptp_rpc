//! `#[rpc(interface = "...")]` 的参数与 controller impl 块分析。
//!
//! 这一层只回答「能不能生成」以及「生成需要哪些事实」：
//!
//! - controller 是固有 impl 块，不支持 unsafe / default / trait impl / 泛型 impl /
//!   impl 级 where 子句；
//! - 每个端点方法用 `#[rpc(方法 = "路径")]` 声明访问方法与相对路径，不带该属性的方法
//!   原样保留为辅助方法；
//! - 端点路径非空、无空段、无空白。
//!
//! 拒绝都带 span，便于编译器把错误指回用户代码。
//!
//! # `analyze_`: attribute and controller impl analysis
//!
//! This layer decides whether expansion is possible and records every fact it needs. It
//! rejects unsafe/default/trait/generic impls and malformed endpoint declarations with a
//! span pointing back at the user's code.

use zyn::{
    meta::Arg,
    syn::{
        self, Attribute, Expr, FnArg, GenericParam, ImplItem, ImplItemFn, ItemFn, ItemImpl, Lit,
        Meta, Pat, Signature, Type, TypeParamBound, WherePredicate, spanned::Spanned,
    },
};

use crate::model_::{
    ControllerModel_, EndpointMethodModel_, EndpointSpec_, FreeFnModel_, MethodKind_, TokenParam_,
};

/// `#[rpc]` 的参数形态。
pub(crate) enum RpcArgs_ {
    /// `#[rpc(interface = "...")]`：controller 固有 impl 块。
    Controller {
        /// interface 路径前缀。
        interface: String,
    },
}

/// 解析 `#[rpc]` 属性参数。
pub(crate) fn parse_rpc_args_(args: &zyn::Args) -> syn::Result<RpcArgs_> {
    let mut controller_interface: Option<(String, proc_macro2::Span)> = Option::None;

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
                validate_interface_path_(&value, span)?;
                controller_interface = Option::Some((value, span));
            }
            Some(other) => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    format!("不认识的 `#[rpc]` 参数 `{other}`；当前只支持 `interface = \"...\"`"),
                ));
            }
            None => {
                return Result::Err(syn::Error::new(
                    arg_span_(arg),
                    "`#[rpc]` 参数需要写成 `interface = \"...\"`",
                ));
            }
        }
    }

    match controller_interface {
        Option::Some((interface, _)) => Result::Ok(RpcArgs_::Controller { interface }),
        Option::None => Result::Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "缺少 `#[rpc]` 参数；controller 用 `#[rpc(interface = \"...\")]`",
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

/// 校验 interface 路径：非空、无首尾 `/`、无空段、无空白。
fn validate_interface_path_(path: &str, span: proc_macro2::Span) -> syn::Result<()> {
    validate_path_(path, span, "interface")
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
}
