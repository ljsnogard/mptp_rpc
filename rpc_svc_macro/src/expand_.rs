//! `#[rpc(interface = "...")]` controller 的代码展开。
//!
//! controller 展开分两块：
//!
//! 1. 保留原 impl 块（业务方法）；
//! 2. 在隐藏模块里为每个端点方法生成 `gen_may_cancel_future` step 函数、端点处理器、
//!    `install_into` 注册与类型化客户端代理。
//!
//! # `expand_`: controller code generation
//!
//! The macro keeps the user's impl and emits, in a hidden module, a step function and an
//! endpoint handler per endpoint, an `install_into` registration helper, and a typed client
//! proxy for the unary endpoints.

use zyn::{
    format_ident,
    quote::quote,
    syn::{
        self, FnArg, Pat, ReturnType, Type, TypeParamBound,
        spanned::Spanned,
        visit_mut::{self, VisitMut},
    },
};

use crate::model_::{ControllerModel_, EndpointMethodModel_, MethodKind_};

/// 展开一个 controller impl 块。
///
/// 保留原 impl（业务方法），并在隐藏模块里为每个端点方法生成 `gen_may_cancel_future` 的
/// step 函数。端点处理器与 `install_into` 注册属 P1 后续 round，届时在
/// [`controller_module_`] 里补齐。
pub(crate) fn expand_controller_(model: &ControllerModel_) -> syn::Result<zyn::Output> {
    let impl_item = &model.impl_item_;
    let module = controller_module_(model)?;

    Result::Ok(zyn::zyn! {
        {{ impl_item }}
        {{ module }}
    })
}

/// 生成 controller 的隐藏模块：step 函数 + 端点处理器 + `install_into`。
fn controller_module_(model: &ControllerModel_) -> syn::Result<::zyn::TokenStream> {
    let self_ty = &model.self_ty_;
    let self_ident = self_type_ident_(self_ty).ok_or_else(|| {
        syn::Error::new(
            self_ty.span(),
            "controller 自身类型必须是简单路径类型，例如 `SceneService`",
        )
    })?;
    let module_ident = format_ident!("__rpc_controller_{}", self_ident);
    let steps = model
        .methods_
        .iter()
        .filter(|method| !matches!(method.endpoint_.method_, MethodKind_::Pull))
        .map(|method| endpoint_step_fn_(self_ty, method))
        .collect::<syn::Result<Vec<_>>>()?;
    let handlers = model
        .methods_
        .iter()
        .map(|method| {
            let handler_struct = handler_struct_(&self_ident, self_ty, method);
            let handler_impl = handler_impl_(&self_ident, method)?;
            Result::Ok(quote! {
                #handler_struct
                #handler_impl
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let install = install_into_(&self_ident, model);
    let client = client_proxy_(&self_ident, model)?;

    Result::Ok(quote! {
        #[doc(hidden)]
        #[allow(non_snake_case, non_camel_case_types, unused_imports)]
        pub mod #module_ident {
            use super::*;
            use ::mptp_serving::{Endpoint, MethodSet, MptpContext, RadixRouter, TrEndpointHandler};
            use ::mptp_serving::x_deps::mptp_smux::{ServeError, serving::config::TrConfig};
            use ::rpc_core_v1::serving::{TrReplyError, decode_body_async, write_ok_async, write_status_async, write_stream_item_async};
            use ::rpc_core_v1::x_deps::abs_async_iter::TrAsyncIterator;
            use ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel;
            use ::rpc_core_v1::x_deps::abs_mptp::spec::Status;

            #(#steps)*
            #(#handlers)*
            #install
            #client
        }
    })
}

/// 把一个 `async fn` 端点方法重写成一个 `gen_may_cancel_future` step 函数。
///
/// 生成的 step 函数是自由函数（`svc: &'f Self` + 原参数 + `cancel: __C`），方法体内的
/// `self` 被重写成 `svc`。处理器（P1 后续）会用它来驱动业务逻辑。
fn endpoint_step_fn_(
    self_ty: &Type,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    let self_ident = self_type_ident_(self_ty).ok_or_else(|| {
        syn::Error::new(
            self_ty.span(),
            "controller 自身类型必须是简单路径类型，例如 `SceneService`",
        )
    })?;
    let method_ident = &method.ident_;
    let sig = &method.sig_;

    // `gen_may_cancel_future` 的前缀：`{Self}_{Method}` → `{Prefix}Async` / `{Prefix}Future`。
    let prefix = format_ident!("{}_{}", self_ident, method_ident);
    // step 函数名：`{self}_{method}_async_`。
    let step_ident = format_ident!("{}_{}_async_", self_ident, method_ident);

    // 收集非 `self` 参数（保留名字与类型）。
    let mut arg_idents = Vec::new();
    let mut arg_tys = Vec::new();
    for input in &sig.inputs {
        let FnArg::Typed(pat_type) = input else {
            continue; // `&self` 接收者被 `svc` 取代。
        };
        let Pat::Ident(pattern) = &*pat_type.pat else {
            return Result::Err(syn::Error::new(
                pat_type.pat.span(),
                "端点方法参数必须是简单标识符",
            ));
        };
        arg_idents.push(pattern.ident.clone());
        arg_tys.push((*pat_type.ty).clone());
    }

    let output_ty = match &sig.output {
        ReturnType::Type(_, ty) => (**ty).clone(),
        ReturnType::Default => syn::parse2(quote!(())).expect("() 是可解析的单元类型"),
    };

    // 重写方法体：`self` → `svc`。
    let mut block = method.block_.clone();
    SelfToSvc_.visit_block_mut(&mut block);

    Result::Ok(quote! {
        #[::rpc_core_v1::x_deps::gen_mcf2::gen_may_cancel_future(
            #prefix,
            pub(crate),
            new(pub(crate))
        )]
        pub(crate) async fn #step_ident<'f, __C>(
            svc: &'f #self_ty,
            #(#arg_idents: #arg_tys,)*
            cancel: __C,
        ) -> #output_ty
        where
            __C: ::rpc_core_v1::x_deps::abs_cancel::TrCancellationToken,
        {
            #block
        }
    })
}

/// 取一个简单路径类型的最后一段标识符。
fn self_type_ident_(ty: &Type) -> Option<syn::Ident> {
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

/// 一个端点处理器结构体的标识符：`{Self}_{Method}`。
fn handler_ident_(self_ident: &syn::Ident, method_ident: &syn::Ident) -> syn::Ident {
    format_ident!("{}_{}", self_ident, method_ident)
}

/// 一个端点方法对应的 `{Prefix}Async` 类型标识符（`gen_may_cancel_future` 生成）。
fn step_async_ident_(self_ident: &syn::Ident, method_ident: &syn::Ident) -> syn::Ident {
    format_ident!("{}_{}Async", self_ident, method_ident)
}

/// 取出端点方法的非 `self` 参数（名字 + 类型）。
fn method_params_(method: &EndpointMethodModel_) -> syn::Result<Vec<(syn::Ident, Type)>> {
    let mut params = Vec::new();
    for input in &method.sig_.inputs {
        let FnArg::Typed(pat_type) = input else {
            continue; // `&self` 接收者。
        };
        let Pat::Ident(pattern) = &*pat_type.pat else {
            return Result::Err(syn::Error::new(
                pat_type.pat.span(),
                "端点方法参数必须是简单标识符",
            ));
        };
        params.push((pattern.ident.clone(), (*pat_type.ty).clone()));
    }
    Result::Ok(params)
}

/// 生成一个端点处理器结构体：持有 `Arc<Self>`，实现 [`TrEndpointHandler`]。
fn handler_struct_(self_ident: &syn::Ident, self_ty: &Type, method: &EndpointMethodModel_) -> ::zyn::TokenStream {
    let handler_ident = handler_ident_(self_ident, &method.ident_);

    quote! {
        #[doc(hidden)]
        pub struct #handler_ident {
            pub svc_: ::std::sync::Arc<#self_ty>,
        }
    }
}

/// 生成一个端点处理器的 [`TrEndpointHandler`] 实现。
///
/// `handle` 解码请求体（0 或 1 个 body 参数）、调用 step 函数、写回复。业务错误按
/// `Display` 文本写成 `500` 状态（更精细的错误→状态映射属后续 round）。
fn handler_impl_(
    self_ident: &syn::Ident,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    if matches!(method.endpoint_.method_, MethodKind_::Pull) {
        return pull_handler_impl_(self_ident, method);
    }
    let handler_ident = handler_ident_(self_ident, &method.ident_);
    let step_async_ident = step_async_ident_(self_ident, &method.ident_);
    let params = method_params_(method)?;

    let decode_call = match params.len() {
        0usize => quote! {
            let result = #step_async_ident::new(&self.svc_)
                .may_cancel_with(ctx.cancel().child_token())
                .await;
        },
        1usize => {
            let (arg_ident, arg_ty) = &params[0];
            quote! {
                let #arg_ident: #arg_ty = decode_body_async(ctx.session_mut()).await?;
                let result = #step_async_ident::new(&self.svc_, #arg_ident)
                    .may_cancel_with(ctx.cancel().child_token())
                    .await;
            }
        }
        _ => {
            return Result::Err(syn::Error::new(
                method.sig_.span(),
                "端点方法暂只支持 0 或 1 个 body 参数",
            ));
        }
    };

    Result::Ok(quote! {
        #[doc(hidden)]
        impl<C, S> TrEndpointHandler<C, S> for #handler_ident
        where
            C: TrConfig,
        {
            fn handle<'a, 'f>(
                &'a self,
                ctx: &'a mut MptpContext<'f, C, S>,
            ) -> ::core::pin::Pin<
                ::std::boxed::Box<
                    dyn ::core::future::Future<Output = Result<(), ServeError>> + 'a
                >
            > {
                ::std::boxed::Box::pin(async move {
                    #decode_call
                    match result {
                        Ok(value) => {
                            write_ok_async(ctx.session_mut(), &value).await?;
                        }
                        Err(error) => {
                            let status = TrReplyError::status(&error);
                            let message = TrReplyError::message(&error);
                            write_status_async(ctx.session_mut(), status, message).await?;
                        }
                    }
                    Result::Ok(())
                })
            }
        }
    })
}

/// 生成 `install_into`：把每个端点处理器注册进 [`RadixRouter`]。
fn install_into_(self_ident: &syn::Ident, model: &ControllerModel_) -> ::zyn::TokenStream {
    let self_ty = &model.self_ty_;
    let interface = &model.path_;
    let bindings = model.methods_.iter().map(|method| {
        let handler_ident = handler_ident_(self_ident, &method.ident_);
        let path = full_path_lit_(interface, &method.endpoint_.path_);
        let method_set = method_set_expr_(method.endpoint_.method_);
        quote! {
            .bind_endpoint(Endpoint::new(
                #path,
                #method_set,
                ::std::sync::Arc::new(#handler_ident { svc_: svc.clone() }),
            ))?
        }
    });

    quote! {
        #[doc(hidden)]
        pub fn install_into<C>(
            router: RadixRouter<C>,
            svc: ::std::sync::Arc<#self_ty>,
        ) -> Result<RadixRouter<C>, ::mptp_serving::x_deps::matchit_route::InsertError>
        where
            C: TrConfig + 'static,
        {
            let router = router #(#bindings)*;
            Result::Ok(router)
        }
    }
}

/// 生成一个 `Pull` 端点的流处理器：解码路径参数 → 调方法取迭代器 → 逐项写后缀流。
fn pull_handler_impl_(
    self_ident: &syn::Ident,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    let handler_ident = handler_ident_(self_ident, &method.ident_);
    let method_ident = &method.ident_;
    stream_item_ty_(method)?;
    let params = method_params_(method)?;

    let mut decode_stmts = Vec::new();
    let mut call_args = Vec::new();
    for (ident, ty) in &params {
        let key = ident.to_string();
        decode_stmts.push(quote! {
            let #ident: #ty = match ctx.route().get_typed(#key) {
                Option::Some(Result::Ok(value)) => value,
                _ => {
                    ctx.session_mut().response(Status::BadRequest).no_body_async().await?;
                    return Result::Ok(());
                }
            };
        });
        call_args.push(ident.clone());
    }

    Result::Ok(quote! {
        #[doc(hidden)]
        impl<C: TrConfig, S> TrEndpointHandler<C, S> for #handler_ident {
            fn handle<'a, 'f>(
                &'a self,
                ctx: &'a mut MptpContext<'f, C, S>,
            ) -> ::core::pin::Pin<
                ::std::boxed::Box<
                    dyn ::core::future::Future<Output = Result<(), ServeError>> + 'a
                >
            > {
                ::std::boxed::Box::pin(async move {
                    let cancel = ctx.cancel().child_token();
                    #(#decode_stmts)*
                    let mut iter: _ = self.svc_.#method_ident(#(#call_args),*);
                    let mut stream = ctx
                        .session_mut()
                        .response(Status::Ok)
                        .suffix_stream_async()
                        .may_cancel_with(cancel.child_token())
                        .await?;
                    loop {
                        match iter.next_async().may_cancel_with(cancel.child_token()).await {
                            Result::Ok(item) => {
                                write_stream_item_async(&mut stream, &item).await?;
                            }
                            Result::Err(_) => break,
                        }
                    }
                    let _ = stream.finish_async().await;
                    Result::Ok(())
                })
            }
        }
    })
}

/// 从 `impl TrAsyncIterator<Item = T, Err = E>` 返回类型里取 item 类型 `T`。
fn stream_item_ty_(method: &EndpointMethodModel_) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &method.sig_.output else {
        return Result::Err(syn::Error::new(
            method.sig_.ident.span(),
            "`Pull` 端点必须返回 `impl TrAsyncIterator<Item = T, Err = E>`",
        ));
    };
    let Type::ImplTrait(impl_trait) = &**ty else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "`Pull` 端点必须返回 `impl TrAsyncIterator<Item = T, Err = E>`",
        ));
    };
    for bound in &impl_trait.bounds {
        let TypeParamBound::Trait(trait_bound) = bound else {
            continue;
        };
        let Some(segment) = trait_bound.path.segments.last() else {
            continue;
        };
        if segment.ident != "TrAsyncIterator" {
            continue;
        }
        let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
            continue;
        };
        for arg in &args.args {
            let syn::GenericArgument::AssocType(assoc) = arg else {
                continue;
            };
            if assoc.ident == "Item" {
                return Result::Ok(assoc.ty.clone());
            }
        }
    }
    Result::Err(syn::Error::new(
        method.sig_.ident.span(),
        "`Pull` 端点必须返回 `impl TrAsyncIterator<Item = T, Err = E>`",
    ))
}

/// 访问方法种类 → [`MethodSet`] 表达式。
fn method_set_expr_(kind: MethodKind_) -> ::zyn::TokenStream {
    match kind {
        MethodKind_::Call => quote!(MethodSet::CALL),
        MethodKind_::View => quote!(MethodSet::VIEW),
        MethodKind_::Head => quote!(MethodSet::HEAD),
        MethodKind_::Post => quote!(MethodSet::POST),
        MethodKind_::Drop => quote!(MethodSet::DROP),
        MethodKind_::Push => quote!(MethodSet::PUSH),
        MethodKind_::Pull => quote!(MethodSet::PULL),
    }
}

/// 组合 interface 前缀与端点相对路径，返回一个字符串字面量。
fn full_path_lit_(interface: &str, path: &str) -> syn::LitStr {
    let full = if interface.is_empty() {
        path.to_string()
    } else {
        format!("{}/{}", interface, path)
    };
    syn::LitStr::new(&full, proc_macro2::Span::call_site())
}

/// 生成 controller 的客户端代理：`{Self}Rpc<'a, C>` + 构造器 + 各端点方法。
fn client_proxy_(
    self_ident: &syn::Ident,
    model: &ControllerModel_,
) -> syn::Result<::zyn::TokenStream> {
    let proxy_ident = format_ident!("{}Rpc", self_ident);
    let interface = &model.path_;
    let methods = model
        .methods_
        .iter()
        .filter(|method| !matches!(method.endpoint_.method_, MethodKind_::Pull))
        .map(|method| controller_client_method_(self_ident, interface, method))
        .collect::<syn::Result<Vec<_>>>()?;

    Result::Ok(quote! {
        #[doc(hidden)]
        pub struct #proxy_ident<'a, C>
        where
            C: ::rpc_core_v1::client::TrRpcClientConfig,
        {
            client_: &'a ::rpc_core_v1::x_deps::mptp_smux::client::MptpClient<C>,
        }

        #[doc(hidden)]
        pub fn rpc<'a, C>(
            client: &'a ::rpc_core_v1::x_deps::mptp_smux::client::MptpClient<C>,
        ) -> #proxy_ident<'a, C>
        where
            C: ::rpc_core_v1::client::TrRpcClientConfig,
        {
            #proxy_ident { client_: client }
        }

        #[doc(hidden)]
        impl<'a, C> #proxy_ident<'a, C>
        where
            C: ::rpc_core_v1::client::TrRpcClientConfig,
        {
            #(#methods)*
        }
    })
}

/// 生成一个客户端代理方法（当前只支持 1 个 body 参数的 unary 端点）。
fn controller_client_method_(
    _self_ident: &syn::Ident,
    interface: &str,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    let method_ident = &method.ident_;
    let params = method_params_(method)?;
    let (arg_ident, arg_ty) = match params.as_slice() {
        [single] => (&single.0, &single.1),
        _ => {
            return Result::Err(syn::Error::new(
                method.sig_.span(),
                "客户端代理暂只支持 1 个 body 参数的端点",
            ));
        }
    };
    let output_ty = success_ty_(method)?;
    let path = full_path_lit_(interface, &method.endpoint_.path_);
    let access_method = access_method_expr_(method.endpoint_.method_);

    Result::Ok(quote! {
        #[doc(hidden)]
        pub fn #method_ident<'f>(&'f self, #arg_ident: #arg_ty)
            -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
                'f,
                MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<#output_ty>
            > + 'f
        {
            ::rpc_core_v1::client::call_endpoint_async(
                self.client_,
                #access_method,
                #path,
                #arg_ident,
            )
        }
    })
}

/// 从 `Result<T, E>` 返回类型里取成功类型 `T`。
fn success_ty_(method: &EndpointMethodModel_) -> syn::Result<Type> {
    let ReturnType::Type(_, ty) = &method.sig_.output else {
        return Result::Err(syn::Error::new(
            method.sig_.ident.span(),
            "客户端代理要求端点方法返回 `Result<T, E>`",
        ));
    };
    let Type::Path(type_path) = &**ty else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "客户端代理要求端点方法返回 `Result<T, E>`",
        ));
    };
    let Some(segment) = type_path.path.segments.last() else {
        return Result::Err(syn::Error::new(
            ty.span(),
            "客户端代理要求端点方法返回 `Result<T, E>`",
        ));
    };
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Result::Err(syn::Error::new(
            segment.span(),
            "客户端代理要求端点方法返回 `Result<T, E>`",
        ));
    };
    let Some(syn::GenericArgument::Type(ok_ty)) = args.args.first() else {
        return Result::Err(syn::Error::new(
            segment.span(),
            "客户端代理要求端点方法返回 `Result<T, E>`",
        ));
    };
    Result::Ok(ok_ty.clone())
}

/// 访问方法种类 → [`AccessMethod`] 表达式。
fn access_method_expr_(kind: MethodKind_) -> ::zyn::TokenStream {
    match kind {
        MethodKind_::Call => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Call)
        }
        MethodKind_::View => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::View)
        }
        MethodKind_::Head => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Head)
        }
        MethodKind_::Post => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Post)
        }
        MethodKind_::Drop => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Drop)
        }
        MethodKind_::Push => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Push)
        }
        MethodKind_::Pull => {
            quote!(::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Pull)
        }
    }
}

/// 把方法体里的 `self` 重写成 `svc`。
struct SelfToSvc_;

impl VisitMut for SelfToSvc_ {
    fn visit_expr_path_mut(&mut self, expr_path: &mut zyn::syn::ExprPath) {
        if expr_path.qself.is_none() && expr_path.path.is_ident("self") {
            expr_path.path = zyn::syn::parse_quote!(svc);
        }
        visit_mut::visit_expr_path_mut(self, expr_path);
    }
}

