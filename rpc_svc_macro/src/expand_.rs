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
        self, FnArg, LitStr, Pat, ReturnType, Type, TypeParamBound,
        spanned::Spanned,
        visit_mut::{self, VisitMut},
    },
};

use crate::model_::{
    BorrowMode_, CallMethodModel_, ControllerModel_, EndpointMethodModel_, FreeFnModel_,
    ImplOfModel_, MethodKind_, PayloadShape_, ServiceModel_, raw_ident_name_, upper_snake_,
};

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

/// 展开一个自由函数 handler：保留原函数，并在隐藏模块里生成 step 函数、单元处理器与
/// `install_into`。
pub(crate) fn expand_free_fn_(model: &FreeFnModel_) -> syn::Result<zyn::Output> {
    let fn_item = &model.fn_item_;
    let module = free_fn_module_(model)?;
    Result::Ok(zyn::zyn! {
        {{ fn_item }}
        {{ module }}
    })
}

/// 生成自由函数 handler 的隐藏模块。
fn free_fn_module_(model: &FreeFnModel_) -> syn::Result<::zyn::TokenStream> {
    let method = &model.method_;
    let names = free_fn_names_(method);
    let step = endpoint_step_fn_(&names, Option::None, method)?;
    let handler_struct = handler_struct_(&names, Option::None, method);
    let handler_impl = handler_impl_(&names, Option::None, method)?;
    let install = free_install_into_(&names, method);
    let module_ident = format_ident!("__rpc_handler_{}", method.ident_);

    Result::Ok(quote! {
        #[doc(hidden)]
        #[allow(non_snake_case, non_camel_case_types, unused_imports)]
        pub mod #module_ident {
            use super::*;
            use ::mptp_serving::{Endpoint, HandlerFuture, MethodSet, MptpContext, RadixRouter, TrEndpointHandler, box_future_};
            use ::mptp_serving::x_deps::mptp_smux::{ServeError, serving::config::TrConfig};
            use ::rpc_core_v1::serving::{TrReplyError, decode_body_async, write_ok_async, write_status_async, write_stream_item_async};
            use ::rpc_core_v1::x_deps::abs_async_iter::TrAsyncIterator;
            use ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel;
            use ::rpc_core_v1::x_deps::abs_mptp::spec::Status;

            #step
            #handler_struct
            #handler_impl
            #install
        }
    })
}

/// 生成自由函数 handler 的 `install_into`（无 `Arc<Self>` 参数）。
fn free_install_into_(names: &HandlerNames_, method: &EndpointMethodModel_) -> ::zyn::TokenStream {
    let handler_ident = &names.handler_ident;
    let path = syn::LitStr::new(&method.endpoint_.path_, proc_macro2::Span::call_site());
    let method_set = method_set_expr_(method.endpoint_.method_);
    quote! {
        #[doc(hidden)]
        pub fn install_into<C, S, R>(
            router: RadixRouter<C, S, R, ::std::alloc::Global>,
        ) -> Result<
            RadixRouter<C, S, R, ::std::alloc::Global>,
            ::mptp_serving::x_deps::matchit_route::InsertError,
        >
        where
            C: TrConfig + 'static,
            S: Send + Sync + 'static,
            R: 'static,
        {
            let router = router.bind_endpoint(Endpoint::new(
                #path,
                #method_set,
                ::std::sync::Arc::new(#handler_ident),
            ))?;
            Result::Ok(router)
        }
    }
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
        .map(|method| {
            let names = handler_names_(&self_ident, method);
            endpoint_step_fn_(&names, Option::Some(self_ty), method)
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let handlers = model
        .methods_
        .iter()
        .map(|method| {
            let names = handler_names_(&self_ident, method);
            let handler_struct = handler_struct_(&names, Option::Some(self_ty), method);
            let handler_impl = handler_impl_(&names, Option::Some(self_ty), method)?;
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
            use ::mptp_serving::{Endpoint, HandlerFuture, MethodSet, MptpContext, RadixRouter, TrEndpointHandler, box_future_};
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
/// 生成的 step 函数是自由函数（`self_ty = Some` 时带 `svc: &'f Self`，`None` 时是纯自由
/// 函数）。取消令牌分两种情况：用户显式声明 `cancel: K where K: TrCancellationToken` 时
/// 直接复用；否则注入 `cancel: __C`。方法体内的 `self`（仅 controller）被重写成 `svc`。
fn endpoint_step_fn_(
    names: &HandlerNames_,
    self_ty: Option<&Type>,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    let sig = &method.sig_;
    let prefix = &names.prefix;
    let step_ident = &names.step_ident;

    // 收集非 `self`、非令牌参数（保留名字与类型）。
    let mut arg_idents = Vec::new();
    let mut arg_tys = Vec::new();
    let token_ident = method
        .token_param_
        .as_ref()
        .map(|token| &token.param_ident_);
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
        if token_ident.is_some_and(|token| token == &pattern.ident) {
            continue; // 取消令牌单独生成。
        }
        arg_idents.push(pattern.ident.clone());
        arg_tys.push((*pat_type.ty).clone());
    }

    let output_ty = match &sig.output {
        ReturnType::Type(_, ty) => (**ty).clone(),
        ReturnType::Default => syn::parse2(quote!(())).expect("() 是可解析的单元类型"),
    };

    // 取消令牌的「泛型参数 + 形参 + where 子句」。
    let (generics, cancel_arg, where_clause) = match &method.token_param_ {
        Option::Some(token) => {
            let ty = &token.ty_ident_;
            let param = &token.param_ident_;
            (
                quote!('f, #ty),
                quote!(#param: #ty,),
                quote! {
                    where
                        #ty: ::rpc_core_v1::x_deps::abs_cancel::TrCancellationToken,
                },
            )
        }
        Option::None => (
            quote!('f, __C),
            quote!(cancel: __C,),
            quote! {
                where
                    __C: ::rpc_core_v1::x_deps::abs_cancel::TrCancellationToken,
            },
        ),
    };

    // 方法体：controller 才把 `self` 重写成 `svc`。
    let mut block = method.block_.clone();
    if self_ty.is_some() {
        SelfToSvc_.visit_block_mut(&mut block);
    }
    let svc_arg = match self_ty {
        Option::Some(self_ty) => quote!(svc: &'f #self_ty,),
        Option::None => quote!(),
    };

    Result::Ok(quote! {
        #[::rpc_core_v1::x_deps::gen_mcf2::gen_may_cancel_future(
            #prefix,
            pub(crate),
            new(pub(crate))
        )]
        pub(crate) async fn #step_ident<#generics>(
            #svc_arg
            #(#arg_idents: #arg_tys,)*
            #cancel_arg
        ) -> #output_ty
        #where_clause
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

/// 一个端点方法对应的 `{Prefix}Async` 类型标识符（`gen_may_cancel_future` 生成）。
fn step_async_ident_(self_ident: &syn::Ident, method_ident: &syn::Ident) -> syn::Ident {
    format_ident!("{}_{}Async", self_ident, method_ident)
}

/// 一次展开所需的全部命名：step 函数名、`gen_may_cancel_future` 前缀、handler 结构体名、
/// `{Prefix}Async` 类型名。
struct HandlerNames_ {
    /// `gen_may_cancel_future` 前缀（生成 `{Prefix}Async` / `{Prefix}Future`）。
    prefix: syn::Ident,

    /// step 函数名。
    step_ident: syn::Ident,

    /// handler 结构体名。
    handler_ident: syn::Ident,

    /// `{Prefix}Async` 类型名。
    step_async_ident: syn::Ident,
}

/// controller 端点的命名：`{Self}_{Method}`。
fn handler_names_(self_ident: &syn::Ident, method: &EndpointMethodModel_) -> HandlerNames_ {
    let method_ident = &method.ident_;
    HandlerNames_ {
        prefix: format_ident!("{}_{}", self_ident, method_ident),
        step_ident: format_ident!("{}_{}_async_", self_ident, method_ident),
        handler_ident: handler_ident_(self_ident, method_ident),
        step_async_ident: step_async_ident_(self_ident, method_ident),
    }
}

/// 自由函数端点的命名：`{Method}` / `{Method}Handler`。
fn free_fn_names_(method: &EndpointMethodModel_) -> HandlerNames_ {
    let method_ident = &method.ident_;
    HandlerNames_ {
        prefix: method_ident.clone(),
        step_ident: format_ident!("{}_async_", method_ident),
        handler_ident: format_ident!("{}Handler", method_ident),
        step_async_ident: format_ident!("{}Async", method_ident),
    }
}

/// 一个端点处理器结构体的标识符：`{Self}_{Method}`。
fn handler_ident_(self_ident: &syn::Ident, method_ident: &syn::Ident) -> syn::Ident {
    format_ident!("{}_{}", self_ident, method_ident)
}

/// 取出端点方法的业务参数（名字 + 类型），不含 `self` 接收者与尾参取消令牌。
fn method_params_(method: &EndpointMethodModel_) -> syn::Result<Vec<(syn::Ident, Type)>> {
    let mut params = Vec::new();
    let token_ident = method
        .token_param_
        .as_ref()
        .map(|token| &token.param_ident_);
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
        if token_ident.is_some_and(|token| token == &pattern.ident) {
            continue; // 取消令牌不参与 body / 路径解码。
        }
        params.push((pattern.ident.clone(), (*pat_type.ty).clone()));
    }
    Result::Ok(params)
}

/// 生成一个端点处理器结构体：controller 持有 `Arc<Self>`，自由函数是无状态单元结构体。
fn handler_struct_(
    names: &HandlerNames_,
    self_ty: Option<&Type>,
    _method: &EndpointMethodModel_,
) -> ::zyn::TokenStream {
    let handler_ident = &names.handler_ident;
    match self_ty {
        Option::Some(self_ty) => quote! {
            #[doc(hidden)]
            pub struct #handler_ident {
                pub svc_: ::std::sync::Arc<#self_ty>,
            }
        },
        Option::None => quote! {
            #[doc(hidden)]
            pub struct #handler_ident;
        },
    }
}

/// 生成一个端点处理器的 [`TrEndpointHandler`] 实现。
///
/// `handle` 解码请求体（0 或 1 个 body 参数）、调用 step 函数、写回复。业务错误按
/// `TrReplyError` 映射成状态回复。取消令牌由 `may_cancel_with(ctx.cancel().child_token())`
/// 交给 step 函数，业务参数里不出现。
fn handler_impl_(
    names: &HandlerNames_,
    self_ty: Option<&Type>,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    if matches!(method.endpoint_.method_, MethodKind_::Pull) {
        return pull_handler_impl_(names, self_ty, method);
    }
    let handler_ident = &names.handler_ident;
    let step_async_ident = &names.step_async_ident;
    let params = method_params_(method)?;
    if params.len() > 1usize {
        return Result::Err(syn::Error::new(
            method.sig_.span(),
            "端点方法暂只支持 0 或 1 个 body 参数（取消令牌不计入）",
        ));
    }

    // step::new(...) 的实参：controller 在最前面放 `&self.svc_`，自由函数为空。
    let mut call_args = Vec::new();
    if self_ty.is_some() {
        call_args.push(quote!(&self.svc_));
    }
    let mut decode_stmts = Vec::new();
    for (arg_ident, arg_ty) in &params {
        decode_stmts.push(quote! {
            let #arg_ident: #arg_ty = decode_body_async(ctx.session_mut()).await?;
        });
        call_args.push(quote!(#arg_ident));
    }

    // `Head` / `Drop` 的回复不允许带体（协议约束）。这两类端点只写状态、丢弃返回值；
    // 错误也只写状态（错误体写不出去），这是 MPTP 的既定语义。
    let no_body = matches!(
        method.endpoint_.method_,
        MethodKind_::Head | MethodKind_::Drop
    );
    let ok_write = if no_body {
        quote!(ctx.session_mut().response(Status::Ok).no_body_async().await?;)
    } else {
        quote!(write_ok_async(ctx.session_mut(), &value).await?;)
    };
    let err_write = if no_body {
        quote!(ctx.session_mut().response(status).no_body_async().await?;)
    } else {
        quote!(write_status_async(ctx.session_mut(), status, message).await?;)
    };

    Result::Ok(quote! {
        #[doc(hidden)]
        impl<C, S, R> TrEndpointHandler<C, S, R> for #handler_ident
        where
            C: TrConfig,
        {
            fn handle<'a, 'f>(
                &'a self,
                ctx: &'a mut MptpContext<'f, C, S, R>,
            ) -> HandlerFuture<'a> {
                let arena = ctx.arena();
                box_future_(arena, async move {
                    #(#decode_stmts)*
                    let result = #step_async_ident::new(#(#call_args),*)
                        .may_cancel_with(ctx.cancel().child_token())
                        .await;
                    match result {
                        Ok(value) => {
                            let _ = &value;
                            #ok_write
                        }
                        Err(error) => {
                            let status = TrReplyError::status(&error);
                            let message = TrReplyError::message(&error);
                            #err_write
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
        pub fn install_into<C, S, R>(
            router: RadixRouter<C, S, R, ::std::alloc::Global>,
            svc: ::std::sync::Arc<#self_ty>,
        ) -> Result<
            RadixRouter<C, S, R, ::std::alloc::Global>,
            ::mptp_serving::x_deps::matchit_route::InsertError,
        >
        where
            C: TrConfig + 'static,
            S: Send + Sync + 'static,
            R: 'static,
        {
            let router = router #(#bindings)*;
            Result::Ok(router)
        }
    }
}

/// 生成一个 `Pull` 端点的流处理器：解码路径参数 → 调方法取迭代器 → 逐项写后缀流。
fn pull_handler_impl_(
    names: &HandlerNames_,
    self_ty: Option<&Type>,
    method: &EndpointMethodModel_,
) -> syn::Result<::zyn::TokenStream> {
    let handler_ident = &names.handler_ident;
    let method_ident = &method.ident_;
    stream_item_ty_(method)?;
    let params = method_params_(method)?;
    // 自由函数的调用接收者为空；controller 是 `self.svc_.`。
    let receiver = match self_ty {
        Option::Some(_) => quote!(self.svc_.),
        Option::None => quote!(),
    };

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
        impl<C: TrConfig, S, R> TrEndpointHandler<C, S, R> for #handler_ident {
            fn handle<'a, 'f>(
                &'a self,
                ctx: &'a mut MptpContext<'f, C, S, R>,
            ) -> HandlerFuture<'a> {
                let arena = ctx.arena();
                box_future_(arena, async move {
                    let cancel = ctx.cancel().child_token();
                    #(#decode_stmts)*
                    let mut iter: _ = #receiver #method_ident(#(#call_args),*);
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

    // `Head` / `Drop` 的回复没有体：用 no-body 入口，否则客户端会尝试解码一个不存在的
    // 回复体（`RpcResult<()>` 的 `()` 需要 1 个 msgpack nil 字节）。
    let call = if matches!(
        method.endpoint_.method_,
        MethodKind_::Head | MethodKind_::Drop
    ) {
        quote! {
            ::rpc_core_v1::client::call_endpoint_no_body_async(
                self.client_,
                #access_method,
                #path,
                #arg_ident,
            )
        }
    } else {
        quote! {
            ::rpc_core_v1::client::call_endpoint_async(
                self.client_,
                #access_method,
                #path,
                #arg_ident,
            )
        }
    };

    Result::Ok(quote! {
        #[doc(hidden)]
        pub fn #method_ident<'f>(&'f self, #arg_ident: #arg_ty)
            -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
                'f,
                MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<#output_ty>
            > + 'f
        {
            #call
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

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Call service trait：#[rpc(service = "...", api = X)]
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 展开一个 Call service trait。
///
/// 生成内容（都写在 trait 所在模块里）：
///
/// - 原 trait + 每方法 `__Req_<operation>` 关联类型 + `__Call` 关联类型；
/// - 零大小 marker `X` 与 `TrRpcInterface::PREFIX`；
/// - 多参方法的 `__Req_*` 位置字段结构体；
/// - 请求枚举 `XxxServiceCall`（变体线上名 = 方法名）；
/// - 每方法 `RpcDataTypeInfo`、方法表 `RpcServiceInfo`；
/// - `ClientProxy` 的 trait impl（每方法构造枚举变体，调用
///   `call_endpoint_typed_with_headers_async`，把代理级请求头一并带上）。
pub(crate) fn expand_service_(model: &ServiceModel_) -> syn::Result<zyn::Output> {
    let trait_item = service_trait_item_(model);
    let marker = service_marker_(model);
    let payloads = model
        .methods_
        .iter()
        .filter(|method| matches!(method.payload_, PayloadShape_::Struct))
        .map(payload_struct_)
        .collect::<Vec<_>>();
    let call_enum = service_call_enum_(model);
    let infos = model
        .methods_
        .iter()
        .map(|method| service_method_info_(model, method))
        .collect::<Vec<_>>();
    let methods_table = service_methods_table_(model);
    let service_info = service_info_const_(model);
    let client = service_client_impl_(model)?;

    Result::Ok(
        quote! {
            #trait_item
            #marker
            #(#payloads)*
            #call_enum
            #(#infos)*
            #methods_table
            #service_info
            #client
        }
        .into(),
    )
}

/// 原 trait + 隐藏关联类型（`__Req_*` / `__Call`）。
fn service_trait_item_(model: &ServiceModel_) -> syn::ItemTrait {
    let mut item = model.trait_item_.clone();
    for method in &model.methods_ {
        let cfg = &method.cfg_attrs_;
        let req_ident = &method.req_assoc_ident_;
        let default_ty = match method.payload_ {
            PayloadShape_::Unit => quote!(()),
            PayloadShape_::Single => {
                let ty = &method.params_[0].owned_ty_;
                quote!(#ty)
            }
            PayloadShape_::Struct => quote!(self::#req_ident),
        };
        let assoc: syn::TraitItem = syn::parse_quote! {
            #(#cfg)*
            #[doc(hidden)]
            #[allow(non_camel_case_types)]
            type #req_ident = #default_ty;
        };
        item.items.push(assoc);
    }
    let call_enum = &model.call_enum_ident_;
    item.items.push(syn::parse_quote! {
        #[doc(hidden)]
        type __Call = self::#call_enum;
    });
    item
}

/// 零大小 marker + `TrRpcInterface::PREFIX`。
fn service_marker_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let vis = &model.trait_item_.vis;
    let api = &model.api_;
    let prefix = LitStr::new(&model.prefix_, proc_macro2::Span::call_site());
    quote! {
        /// Call service 的客户端挂载点；`client.call::<Self>()` 用它选择服务前缀。
        #[derive(Clone, Copy, Debug)]
        #vis struct #api;

        impl ::rpc_core_v1::messaging::TrRpcInterface for #api {
            /// 完整服务前缀；Call 的 pattern 就是它。
            const PREFIX: &'static str = #prefix;
        }
    }
}

/// 一个多参方法的线上请求载荷结构体（位置字段 `f0..`）。
fn payload_struct_(method: &CallMethodModel_) -> ::zyn::TokenStream {
    let ident = &method.req_assoc_ident_;
    let cfg = &method.cfg_attrs_;
    let fields = method.params_.iter().enumerate().map(|(index, param)| {
        let field = format_ident!("f{}", index);
        let ty = &param.owned_ty_;
        let name = &param.ident_;
        quote! {
            #[doc = concat!("位置字段 `", stringify!(#field), "`，对应参数 `", stringify!(#name), "`。")]
            pub #field: #ty,
        }
    });
    quote! {
        /// 多参方法的线上请求载荷：按位置编码的字段（参数名不进线上契约）。
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        #(#cfg)*
        #[derive(
            ::rpc_core_v1::x_deps::denco_macro::TrDecodable,
            ::rpc_core_v1::x_deps::denco_macro::TrEncodable,
            ::rpc_core_v1::x_deps::denco_macro::TrHasTypeId,
        )]
        pub struct #ident {
            #(#fields)*
        }
    }
}

/// 请求枚举：变体线上名 = trait 方法名。
fn service_call_enum_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let vis = &model.trait_item_.vis;
    let enum_ident = &model.call_enum_ident_;
    let variants = model.methods_.iter().map(|method| {
        let cfg = &method.cfg_attrs_;
        let variant = &method.variant_ident_;
        let operation = LitStr::new(&raw_ident_name_(&method.ident_), method.ident_.span());
        let payload = match method.payload_ {
            PayloadShape_::Unit => quote!(),
            PayloadShape_::Single => {
                let ty = &method.params_[0].owned_ty_;
                quote!((#ty))
            }
            PayloadShape_::Struct => {
                let req = &method.req_assoc_ident_;
                quote!((#req))
            }
        };
        quote! {
            #(#cfg)*
            #[denco(rename = #operation)]
            #variant #payload,
        }
    });
    quote! {
        /// Call service 的请求枚举；变体线上名 = trait 方法名。
        #[doc(hidden)]
        #[derive(
            ::rpc_core_v1::x_deps::denco_macro::TrEncodable,
            ::rpc_core_v1::x_deps::denco_macro::TrDecodable,
        )]
        #vis enum #enum_ident {
            #(#variants)*
        }
    }
}

/// 一个方法的 `RpcDataTypeInfo` 常量。
fn service_method_info_(model: &ServiceModel_, method: &CallMethodModel_) -> ::zyn::TokenStream {
    let const_ident = info_const_ident_(&model.api_, method);
    let payload_ty = service_payload_ty_(method);
    let response_ty = &method.response_ty_;
    let expr = data_type_info_expr_(payload_ty, response_ty);
    let cfg = &method.cfg_attrs_;
    let method_ident = &method.ident_;
    quote! {
        #[doc = concat!("`", stringify!(#method_ident), "` 的方法级类型头。")]
        #(#cfg)*
        pub const #const_ident: ::rpc_core_v1::stream::RpcDataTypeInfo = #expr;
    }
}

/// trait 侧的方法载荷类型表达式。
fn service_payload_ty_(method: &CallMethodModel_) -> ::zyn::TokenStream {
    match method.payload_ {
        PayloadShape_::Unit => quote!(()),
        PayloadShape_::Single => {
            let ty = &method.params_[0].owned_ty_;
            quote!(#ty)
        }
        PayloadShape_::Struct => {
            let req = &method.req_assoc_ident_;
            quote!(#req)
        }
    }
}

/// 方法级 `RpcDataTypeInfo` 表达式。
fn data_type_info_expr_(payload_ty: ::zyn::TokenStream, response_ty: &Type) -> ::zyn::TokenStream {
    quote! {
        ::rpc_core_v1::stream::RpcDataTypeInfo::new(::rpc_core_v1::stream::BodyCodec::MsgPack)
            .with_request_body(
                <#payload_ty as ::rpc_core_v1::stream::TrDataTypeId>::DATA_TYPE_ID,
            )
            .with_response_body(
                <#response_ty as ::rpc_core_v1::stream::TrDataTypeId>::DATA_TYPE_ID,
            )
            .with_accept(::rpc_core_v1::stream::AcceptType::new(
                ::rpc_core_v1::stream::BodyCodec::MsgPack,
                ::core::option::Option::Some(
                    <#response_ty as ::rpc_core_v1::stream::TrDataTypeId>::DATA_TYPE_ID,
                ),
            ))
            .with_policy(::rpc_core_v1::stream::TypePolicy::Strict)
    }
}

/// `{}_METHODS` 方法表常量。
fn service_methods_table_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let const_ident = format_ident!("{}_METHODS", upper_snake_(&model.api_));
    let entries = model.methods_.iter().map(|method| {
        let cfg = &method.cfg_attrs_;
        let operation = LitStr::new(&raw_ident_name_(&method.ident_), method.ident_.span());
        let info = info_const_ident_(&model.api_, method);
        quote! {
            #(#cfg)*
            ::rpc_core_v1::stream::RpcMethodInfo::new(#operation, #info),
        }
    });
    quote! {
        /// Call service 的方法表：operation + 方法级类型头。
        pub const #const_ident: &[::rpc_core_v1::stream::RpcMethodInfo] = &[
            #(#entries)*
        ];
    }
}

/// `{}_INFO` service 级元信息常量。
fn service_info_const_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let const_ident = format_ident!("{}_INFO", upper_snake_(&model.api_));
    let methods = format_ident!("{}_METHODS", upper_snake_(&model.api_));
    let api = &model.api_;
    quote! {
        /// Call service 的 service 级元信息：前缀 + 方法表。
        pub const #const_ident: ::rpc_core_v1::stream::RpcServiceInfo =
            ::rpc_core_v1::stream::RpcServiceInfo::new(
                <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                #methods,
            );
    }
}

/// 方法级类型头常量名：`{MARKER}_{METHOD}_INFO`。
fn info_const_ident_(api: &syn::Ident, method: &CallMethodModel_) -> syn::Ident {
    format_ident!(
        "{}_{}_INFO",
        upper_snake_(api),
        upper_snake_(&method.ident_)
    )
}

/// `ClientProxy` 的 trait impl。
fn service_client_impl_(model: &ServiceModel_) -> syn::Result<::zyn::TokenStream> {
    let trait_ident = &model.trait_ident_;
    let api = &model.api_;
    let methods = model
        .methods_
        .iter()
        .map(|method| service_client_method_(model, method))
        .collect::<Vec<_>>();
    Result::Ok(quote! {
        impl<'__rpc_a, __rpc_c> #trait_ident
            for ::rpc_core_v1::client::ClientProxy<'__rpc_a, __rpc_c, #api>
        where
            __rpc_c: ::rpc_core_v1::client::TrRpcClientConfig,
        {
            #(#methods)*
        }
    })
}

/// 一个客户端代理方法：构造枚举变体 + 方法级类型头。
fn service_client_method_(model: &ServiceModel_, method: &CallMethodModel_) -> ::zyn::TokenStream {
    let sig = &method.sig_;
    // 原方法的文档 / `#[allow(...)]` / `#[cfg]` 一起带到客户端 impl 上：签名相同的 impl
    // 方法会面对同一批 lint（例如 `clippy::ptr_arg`）。
    let attrs = &method.attrs_;
    let enum_ident = &model.call_enum_ident_;
    let api = &model.api_;
    let variant = &method.variant_ident_;
    let response_ty = &method.response_ty_;
    let info = info_const_ident_(&model.api_, method);
    let body = match method.payload_ {
        PayloadShape_::Unit => quote!(#enum_ident::#variant),
        PayloadShape_::Single => {
            let param = &method.params_[0];
            let value = owned_expr_(param);
            quote!(#enum_ident::#variant(#value))
        }
        PayloadShape_::Struct => {
            let req = &method.req_assoc_ident_;
            let fields = method.params_.iter().enumerate().map(|(index, param)| {
                let field = format_ident!("f{}", index);
                let value = owned_expr_(param);
                quote!(#field: #value)
            });
            quote!(#enum_ident::#variant(#req { #(#fields),* }))
        }
    };
    quote! {
        #(#attrs)*
        #sig {
            ::rpc_core_v1::client::call_endpoint_typed_with_headers_async::<__rpc_c, _, #response_ty>(
                self.client(),
                ::rpc_core_v1::x_deps::abs_mptp::spec::access_method::AccessMethod::Call,
                <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                &#info,
                #body,
                self.headers(),
            )
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Call service impl：#[rpc(impl_of(X))]
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 展开一个 Call service 的 trait impl：保留业务 impl，生成 service dispatcher、
/// 方法表、一个 `CALL /<prefix>` endpoint 与 `install_into`。
pub(crate) fn expand_impl_of_(model: &ImplOfModel_) -> syn::Result<zyn::Output> {
    let impl_item = &model.impl_item_;
    let module = impl_of_module_(model);
    Result::Ok(
        quote! {
            #impl_item
            #module
        }
        .into(),
    )
}

/// 生成 impl_of 的隐藏模块：方法表 + dispatcher + `install_into`。
fn impl_of_module_(model: &ImplOfModel_) -> ::zyn::TokenStream {
    let module_ident = format_ident!(
        "__rpc_impl_of_{}_via_{}",
        raw_ident_name_(&model.self_ident_),
        raw_ident_name_(&model.api_ident_)
    );
    let self_ty = &model.self_ty_;
    let trait_path = &model.trait_path_;
    let api = &model.api_path_;
    let call_ty = quote!(<#self_ty as #trait_path>::__Call);
    let infos = model
        .methods_
        .iter()
        .map(|method| impl_of_method_info_(model, method))
        .collect::<Vec<_>>();
    let methods_table = impl_of_methods_table_(model);
    let service_info = impl_of_service_info_(model);
    let info_arms = model.methods_.iter().map(|method| {
        let cfg = &method.cfg_attrs_;
        let variant = &method.variant_ident_;
        let info = impl_of_info_ident_(method);
        let pattern = match method.payload_ {
            PayloadShape_::Unit => quote!(CallTy_::#variant),
            _ => quote!(CallTy_::#variant(_)),
        };
        quote! {
            #(#cfg)*
            #pattern => &#info,
        }
    });
    let dispatch_arms = model.methods_.iter().map(impl_of_dispatch_arm_);
    quote! {
        #[doc(hidden)]
        #[allow(non_snake_case, non_camel_case_types, unused_imports)]
        pub mod #module_ident {
            use super::*;
            use ::mptp_serving::{
                Endpoint, HandlerFuture, MethodSet, MptpContext, RadixRouter, TrEndpointHandler,
                box_future_,
            };
            use ::mptp_serving::x_deps::mptp_smux::{
                ServeError,
                serving::{ServingSession, config::TrConfig},
            };
            use ::rpc_core_v1::messaging::{RpcError, RpcResult};
            use ::rpc_core_v1::serving::{
                decode_body_async, validate_request_types, write_status_async,
                write_typed_value_async,
            };
            use ::rpc_core_v1::stream::RpcDataTypeInfo;
            use ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel;
            use ::rpc_core_v1::x_deps::abs_denco::ser::TrEncodable;
            use ::rpc_core_v1::x_deps::abs_mptp::spec::{Status, access_method::AccessMethod};

            /// 请求枚举的本地别名：模式里不能用限定路径，别名让 `XxxCall::Variant` 可用。
            type CallTy_ = #call_ty;

            /// 一个业务结果写成 typed 200 或错误状态。
            async fn write_result_<C, T>(
                session: &mut ServingSession<C>,
                info: &RpcDataTypeInfo,
                result: RpcResult<T>,
            ) -> Result<(), ServeError>
            where
                C: TrConfig,
                T: TrEncodable,
            {
                match result {
                    Result::Ok(value) => {
                        write_typed_value_async(
                            session,
                            Status::Ok,
                            AccessMethod::Call,
                            info,
                            &value,
                        )
                        .await
                    }
                    Result::Err(error) => {
                        let (status, message) = error.into_reply_parts();
                        write_status_async(session, status, message).await
                    }
                }
            }

            #(#infos)*
            #methods_table
            #service_info

            /// 一个 Call service 的 dispatcher：解请求枚举 → 方法级校验 → 调 impl。
            pub struct ServiceDispatcher {
                /// 业务实现；所有方法共享同一个 `Arc`。
                svc_: ::std::sync::Arc<#self_ty>,
            }

            impl<C, S, R> TrEndpointHandler<C, S, R> for ServiceDispatcher
            where
                C: TrConfig + 'static,
            {
                fn handle<'a, 'f>(
                    &'a self,
                    ctx: &'a mut MptpContext<'f, C, S, R>,
                ) -> HandlerFuture<'a> {
                    let arena = ctx.arena();
                    box_future_(arena, async move {
                        let call: CallTy_ = match decode_body_async(ctx.session_mut()).await {
                            Result::Ok(call) => call,
                            Result::Err(err) => {
                                write_status_async(
                                    ctx.session_mut(),
                                    Status::BadRequest,
                                    err.to_string(),
                                )
                                .await?;
                                return Result::Ok(());
                            }
                        };
                        let info: &'static RpcDataTypeInfo = match &call {
                            #(#info_arms)*
                        };
                        if let Result::Err(rejection) =
                            validate_request_types(AccessMethod::Call, ctx.headers(), info)
                        {
                            write_status_async(
                                ctx.session_mut(),
                                rejection.status(),
                                rejection.to_string(),
                            )
                            .await?;
                            return Result::Ok(());
                        }

                        match call {
                            #(#dispatch_arms)*
                        }
                        Result::Ok(())
                    })
                }
            }

            /// 安装这个 service：`CALL /<prefix>` + 方法表。
            #[doc(hidden)]
            pub fn install_into<C, S, R>(
                router: RadixRouter<C, S, R, ::std::alloc::Global>,
                svc: ::std::sync::Arc<#self_ty>,
            ) -> Result<
                RadixRouter<C, S, R, ::std::alloc::Global>,
                ::mptp_serving::x_deps::matchit_route::InsertError,
            >
            where
                C: TrConfig + 'static,
                S: Send + Sync + 'static,
                R: 'static,
            {
                let endpoint = Endpoint::new(
                    <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                    MethodSet::CALL,
                    ::std::sync::Arc::new(ServiceDispatcher { svc_: svc }),
                )
                .with_metadata(SERVICE_INFO_);
                router.bind_endpoint(endpoint)
            }
        }
    }
}

/// impl_of 侧的方法级类型头常量名：`METHOD_{METHOD}_INFO_`。
fn impl_of_info_ident_(method: &CallMethodModel_) -> syn::Ident {
    format_ident!("METHOD_{}_INFO_", upper_snake_(&method.ident_))
}

/// impl_of 侧的方法载荷类型：跨 crate 通过隐藏关联类型命名。
fn impl_of_payload_ty_(model: &ImplOfModel_, method: &CallMethodModel_) -> ::zyn::TokenStream {
    let self_ty = &model.self_ty_;
    let trait_path = &model.trait_path_;
    let req = &method.req_assoc_ident_;
    quote!(<#self_ty as #trait_path>::#req)
}

/// impl_of 侧一个方法的 `RpcDataTypeInfo` 常量。
fn impl_of_method_info_(model: &ImplOfModel_, method: &CallMethodModel_) -> ::zyn::TokenStream {
    let const_ident = impl_of_info_ident_(method);
    let payload_ty = impl_of_payload_ty_(model, method);
    let response_ty = &method.response_ty_;
    let expr = data_type_info_expr_(payload_ty, response_ty);
    let cfg = &method.cfg_attrs_;
    quote! {
        #(#cfg)*
        const #const_ident: ::rpc_core_v1::stream::RpcDataTypeInfo = #expr;
    }
}

/// impl_of 侧的方法表常量。
fn impl_of_methods_table_(model: &ImplOfModel_) -> ::zyn::TokenStream {
    let entries = model.methods_.iter().map(|method| {
        let cfg = &method.cfg_attrs_;
        let operation = LitStr::new(&raw_ident_name_(&method.ident_), method.ident_.span());
        let info = impl_of_info_ident_(method);
        quote! {
            #(#cfg)*
            ::rpc_core_v1::stream::RpcMethodInfo::new(#operation, #info),
        }
    });
    quote! {
        const METHODS_: &[::rpc_core_v1::stream::RpcMethodInfo] = &[
            #(#entries)*
        ];
    }
}

/// impl_of 侧的 service 级元信息常量。
fn impl_of_service_info_(model: &ImplOfModel_) -> ::zyn::TokenStream {
    let api = &model.api_path_;
    quote! {
        const SERVICE_INFO_: ::rpc_core_v1::stream::RpcServiceInfo =
            ::rpc_core_v1::stream::RpcServiceInfo::new(
                <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                <#api as ::rpc_core_v1::messaging::TrRpcInterface>::PREFIX,
                METHODS_,
            );
    }
}

/// impl_of 侧一个变体的分派分支。
fn impl_of_dispatch_arm_(method: &CallMethodModel_) -> ::zyn::TokenStream {
    let cfg = &method.cfg_attrs_;
    let variant = &method.variant_ident_;
    let method_ident = &method.ident_;
    let info = impl_of_info_ident_(method);
    let (pattern, args) = match method.payload_ {
        PayloadShape_::Unit => (quote!(CallTy_::#variant), quote!()),
        PayloadShape_::Single => {
            let param = &method.params_[0];
            let arg = dispatch_arg_(param, quote!(payload));
            (quote!(CallTy_::#variant(payload)), quote!(#arg))
        }
        PayloadShape_::Struct => {
            let args = method.params_.iter().enumerate().map(|(index, param)| {
                let field = format_ident!("f{}", index);
                dispatch_arg_(param, quote!(req.#field))
            });
            (quote!(CallTy_::#variant(req)), quote!(#(#args),*))
        }
    };
    quote! {
        #(#cfg)*
        #pattern => {
            let result = self
                .svc_
                .#method_ident(#args)
                .may_cancel_with(ctx.cancel().child_token())
                .await;
            write_result_(ctx.session_mut(), &#info, result).await?;
        }
    }
}

/// 客户端侧：owned 参数原样放进载荷，借用参数按白名单转换成 owned。
fn owned_expr_(param: &crate::model_::CallParam_) -> ::zyn::TokenStream {
    let ident = &param.ident_;
    match param.borrow_ {
        BorrowMode_::Owned => quote!(#ident),
        BorrowMode_::Str => quote!(::std::string::ToString::to_string(#ident)),
        BorrowMode_::Bytes => quote!(::std::vec::Vec::from(#ident)),
        BorrowMode_::Clone => quote!(::core::clone::Clone::clone(#ident)),
        BorrowMode_::Copy => quote!(*#ident),
    }
}

/// 服务端侧：借用参数把 owned 载荷借回去，owned 参数按值传。
fn dispatch_arg_(
    param: &crate::model_::CallParam_,
    owned: ::zyn::TokenStream,
) -> ::zyn::TokenStream {
    if param.borrow_.is_borrowed() {
        quote!(&#owned)
    } else {
        owned
    }
}
