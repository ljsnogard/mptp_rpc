//! `#[rpc]` 的代码展开。
//!
//! service 宏的展开分为四块：
//!
//! 1. 在 service trait 里注入 `SERVICE_PATH` 与隐藏的 `__rpc_dispatch_async` 方法；
//! 2. 生成一个 **owned body enum**：变体名 = 方法名，字段名 = 参数名；
//! 3. 生成客户端代理对统一 `ClientProxy` 的 trait 实现；
//! 4. 在隐藏模块里生成服务端 dispatch（`match` body enum → 调用用户实现）。
//!
//! interface 宏只生成 `TrRpcInterface` 与 `ServiceSelector`，不再生成 worker 组。
//!
//! `#[rpc(service_of(ApiV1))]` 生成服务端 impl 块：它保留用户原来的 impl，并为该类型补一个
//! `abs_rpc_serv::service::TrRpcService<ApiV1, C>` 实现；路由与 service 的组织由用户在装配
//! 期自己调用 `RpcRouter::add_service` 完成。
//!
//! `denco_macro` 当前不支持带生命周期参数的派生类型，所以客户端与服务端共用同一个
//! owned body enum；客户端方法在构造变体时把支持的借用参数转成 owned 字段。
//!
//! # `expand_`: code generation for every macro form
//!
//! Service expansion injects `SERVICE_PATH` and a hidden dispatch method, emits one owned body
//! enum, implements the service trait for the unified `ClientProxy`, and generates the serving
//! dispatch inside a hidden module. Interface expansion only emits the interface contract and
//! client selector. `service_of` keeps the user's impl and adds the serving association that
//! `RpcRouter::add_service` consumes.

use zyn::{
    format_ident,
    quote::quote,
    syn::{
        self, FnArg, Lifetime, Pat, ReturnType, Type, TypeParamBound,
        spanned::Spanned,
        visit_mut::{self, VisitMut},
    },
};

use crate::{
    analyze_::cfg_attrs_,
    model_::{
        ArgModel_, ControllerModel_, ConvertKind_, EndpointMethodModel_, InterfaceModel_,
        MethodKind_, MethodModel_, ServiceModel_, ServiceOfModel_,
    },
};

/// 展开一个 service trait。
pub(crate) fn expand_service_(model: &ServiceModel_) -> syn::Result<zyn::Output> {
    let trait_item = &model.trait_item_;
    let module = dispatch_module_(model)?;
    let client_impl = client_impl_(model)?;
    let cfg = cfg_attrs_(&trait_item.attrs);

    Result::Ok(zyn::zyn! {
        {{ trait_item }}
        @for (attr in cfg.iter()) {
            {{ attr }}
        }
        {{ module }}
        @for (attr in cfg.iter()) {
            {{ attr }}
        }
        {{ client_impl }}
    })
}

/// 生成 owned body enum。
fn body_enum_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let variants = model.methods_.iter().map(variant_def_).collect::<Vec<_>>();
    let variants = quote!(#(#variants,)*);

    quote! {
        #[allow(non_camel_case_types)]
        #[derive(
            ::rpc_core_v1::x_deps::denco_macro::TrEncodable,
            ::rpc_core_v1::x_deps::denco_macro::TrDecodable
        )]
        pub enum Call {
            #variants
        }
    }
}

/// 生成一个 enum 变体。
fn variant_def_(method: &MethodModel_) -> ::zyn::TokenStream {
    let ident = &method.ident_;
    let cfg = cfg_attrs_(&method.attrs_);

    if method.args_.is_empty() {
        return quote! {
            #(#cfg)*
            #ident
        };
    }

    let fields = method
        .args_
        .iter()
        .map(|arg| {
            let name = &arg.ident_;
            let ty = &arg.owned_ty_;
            quote!(#name: #ty)
        })
        .collect::<Vec<_>>();
    let fields = quote!(#(#fields),*);

    quote! {
        #(#cfg)*
        #ident { #fields }
    }
}

/// 生成客户端 trait 实现。
fn client_impl_(model: &ServiceModel_) -> syn::Result<::zyn::TokenStream> {
    let trait_ident = &model.trait_ident_;
    let methods = model
        .methods_
        .iter()
        .map(|method| client_method_(model, method))
        .collect::<syn::Result<Vec<_>>>()?;
    let methods = quote!(#(#methods)*);
    let shared_conn_bound = if model.requires_send_sync_ {
        quote! {
            <__C as ::rpc_core_v1::x_deps::abs_mptp::client::config::TrConfig>::SharedConn:
                Sync,
        }
    } else {
        quote!()
    };

    Result::Ok(quote! {
        impl<'__a, __C, __I> #trait_ident
            for ::rpc_core_v1::client::ClientProxy<'__a, __C, __I>
        where
            __C: ::rpc_core_v1::client::TrRpcClientConfig + Send + Sync + 'static,
            #shared_conn_bound
            __I: ::rpc_core_v1::messaging::TrRpcInterface + Send + Sync + 'static,
        {
            #methods
        }
    })
}

/// 生成一个客户端方法。
fn client_method_(model: &ServiceModel_, method: &MethodModel_) -> syn::Result<::zyn::TokenStream> {
    let mut sig = method.sig_.clone();
    let output_ty = return_type_(&method.output_ty_, method)?;
    sig.output = syn::parse2(quote!(-> #output_ty))?;

    let module_ident = &model.module_ident_;
    let trait_ident = &model.trait_ident_;
    let method_ident = &method.ident_;
    let fields = method
        .args_
        .iter()
        .map(|arg| {
            let name = &arg.ident_;
            if matches!(arg.convert_, ConvertKind_::None) {
                // 按值参数直接 move；用字段简写避免 clippy::redundant_field_names。
                quote!(#name)
            } else {
                let expr = convert_expr_(arg);
                quote!(#name: #expr)
            }
        })
        .collect::<Vec<_>>();
    let fields = quote!(#(#fields),*);
    let variant = if method.args_.is_empty() {
        quote!(#module_ident::Call::#method_ident)
    } else {
        quote!(#module_ident::Call::#method_ident { #fields })
    };
    let output_ty = &method.output_ty_;
    let attrs = &method.attrs_;

    Result::Ok(quote! {
        #(#attrs)*
        #sig {
            let request: #module_ident::Call = #variant;
            ::rpc_core_v1::client::call_unary_async::<
                __C,
                __I,
                #module_ident::Call,
                #output_ty,
            >(
                self,
                <Self as #trait_ident>::SERVICE_PATH,
                request,
            )
        }
    })
}

/// 客户端把参数变成 owned body enum 字段的表达式。
fn convert_expr_(arg: &ArgModel_) -> ::zyn::TokenStream {
    let name = &arg.ident_;
    match arg.convert_ {
        ConvertKind_::None => quote!(#name),
        ConvertKind_::ToString => quote!(#name.to_string()),
        ConvertKind_::ToVec => quote!(#name.to_vec()),
        ConvertKind_::Clone => quote!(#name.clone()),
        ConvertKind_::Deref => quote!(*#name),
    }
}

/// 重建客户端方法的返回类型。
///
/// 用户写的是 `impl TrMayCancel<'f, MayCancelOutput = RpcResult<T>>`；这里把它重写成
/// **全限定**的路径，保证生成的 impl 与 trait 使用的是同一个 `TrMayCancel` / `RpcResult`，
/// 同时保留用户额外写的 `+ Send` 之类 bound。
fn return_type_(output_ty: &Type, method: &MethodModel_) -> syn::Result<Type> {
    let lifetime = method
        .may_cancel_lifetime_
        .clone()
        .unwrap_or_else(|| Lifetime::new("'_", proc_macro2::Span::call_site()));
    let extra = &method.extra_bounds_;
    let extra = extra
        .iter()
        .map(|bound: &TypeParamBound| quote!(+ #bound))
        .collect::<Vec<_>>();

    syn::parse2(quote! {
        impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
            #lifetime,
            MayCancelOutput = ::rpc_core_v1::messaging::RpcResult<#output_ty>
        >
        #(#extra)*
    })
}

/// 生成隐藏的 dispatch 模块。
fn dispatch_module_(model: &ServiceModel_) -> syn::Result<::zyn::TokenStream> {
    let module_ident = &model.module_ident_;
    let body_enum = body_enum_(model);
    let step = dispatch_step_(model)?;
    let wrapper = dispatch_wrapper_(model)?;

    Result::Ok(quote! {
        #[doc(hidden)]
        pub mod #module_ident {
            #[allow(unused_imports)]
            use super::*;
            use ::rpc_core_v1::x_deps::abs_cancel;
            use ::rpc_core_v1::x_deps::abs_cancel::{
                TrCancellationToken,
                TrMayCancel,
            };
            use ::rpc_core_v1::x_deps::abs_denco;
            use ::rpc_core_v1::x_deps::gen_mcf2;
            use ::rpc_core_v1::x_deps::abs_rpc_serv::service::TrRpcCallCtx;
            use ::rpc_core_v1::messaging::RpcResult;

            #body_enum
            #step
            #wrapper
        }
    })
}

/// 生成 dispatch 的 step 函数。
fn dispatch_step_(model: &ServiceModel_) -> syn::Result<::zyn::TokenStream> {
    let trait_ident = &model.trait_ident_;
    let arms = model.methods_.iter().map(dispatch_arm_).collect::<Vec<_>>();
    let arms = quote!(#(#arms)*);

    Result::Ok(quote! {
        #[allow(clippy::needless_lifetimes)]
        #[::rpc_core_v1::x_deps::gen_mcf2::gen_may_cancel_future(
            Dispatch,
            pub(crate),
            new(pub(crate))
        )]
        async fn dispatch_async_<'__f, __S, __C, __X, __Tok>(
            service: &'__f __S,
            ctx: &'__f mut __X,
            cancel: __Tok,
        ) -> RpcResult<()>
        where
            __S: #trait_ident + '__f,
            __C: '__f,
            __X: TrRpcCallCtx<__C, Err = ::rpc_core_v1::messaging::RpcError> + '__f,
            __Tok: abs_cancel::TrCancellationToken,
        {
            let call: Call = match ctx
                .decode_call_async()
                .may_cancel_with(cancel.child_token())
                .await
            {
                Ok(call) => call,
                Err(err) => {
                    ctx.reply_error_async(err)
                        .may_cancel_with(cancel.child_token())
                        .await?;
                    return Ok(());
                }
            };

            match call {
                #arms
            }
        }
    })
}

/// 生成一个 dispatch 分支。
fn dispatch_arm_(method: &MethodModel_) -> ::zyn::TokenStream {
    let ident = &method.ident_;
    let cfg = cfg_attrs_(&method.attrs_);

    let pattern = if method.args_.is_empty() {
        quote!(Call::#ident)
    } else {
        let fields = method
            .args_
            .iter()
            .map(|arg| &arg.ident_)
            .collect::<Vec<_>>();
        let fields = quote!(#(#fields),*);
        quote!(Call::#ident { #fields })
    };

    let call_args = method
        .args_
        .iter()
        .map(|arg| {
            let name = &arg.ident_;
            if arg.is_ref_ {
                quote!(&#name)
            } else {
                quote!(#name)
            }
        })
        .collect::<Vec<_>>();
    let call_args = quote!(#(#call_args),*);

    quote! {
        #(#cfg)*
        #pattern => {
            let result = service
                .#ident(#call_args)
                .may_cancel_with(cancel.child_token())
                .await;

            match result {
                Ok(value) => {
                    ctx.reply_ok_async(value)
                        .may_cancel_with(cancel.child_token())
                        .await
                }
                Err(err) => {
                    ctx.reply_error_async(err)
                        .may_cancel_with(cancel.child_token())
                        .await
                }
            }
        }
    }
}

/// 生成 dispatch 的公开包装。
fn dispatch_wrapper_(model: &ServiceModel_) -> syn::Result<::zyn::TokenStream> {
    let trait_ident = &model.trait_ident_;

    Result::Ok(quote! {
        pub fn dispatch_async<'__f, __S, __C, __X>(
            service: &'__f __S,
            ctx: &'__f mut __X,
        ) -> impl TrMayCancel<'__f, MayCancelOutput = RpcResult<()>> + '__f
        where
            __S: #trait_ident + '__f,
            __C: '__f,
            __X: TrRpcCallCtx<__C, Err = ::rpc_core_v1::messaging::RpcError> + '__f,
        {
            DispatchAsync::<'__f, '__f, __S, __C, __X>::new(service, ctx)
        }
    })
}

/// 展开一个 interface marker。
pub(crate) fn expand_interface_(model: &InterfaceModel_) -> syn::Result<zyn::Output> {
    let item = &model.struct_item_;
    let generated = interface_items_(model);

    Result::Ok(zyn::zyn! {
        {{ item }}
        {{ generated }}
    })
}

/// 生成 interface 的契约实现与客户端选择器。
///
/// 服务端组织不再由宏生成：用户用 `RpcRouter::<ApiV1>::new(ctx).add_service::<Impl>(...)`
/// 自己决定装哪些 service、以什么顺序装、是否需要队列。
fn interface_items_(model: &InterfaceModel_) -> ::zyn::TokenStream {
    let struct_ident = &model.struct_ident_;
    let path = &model.path_;

    quote! {
        impl ::rpc_core_v1::messaging::TrRpcInterface for #struct_ident {
            const PATH: &'static str = #path;
        }

        impl<__C> ::rpc_core_v1::client::ServiceSelector<__C> for #struct_ident
        where
            __C: ::rpc_core_v1::client::TrRpcClientConfig,
        {
            type View<'__a>
                = ::rpc_core_v1::client::ClientProxy<'__a, __C, #struct_ident>
            where
                Self: '__a,
                __C: '__a;

            fn select<'__a>(
                client: &'__a ::rpc_core_v1::x_deps::mptp_smux::client::MptpClient<__C>,
            ) -> Self::View<'__a> {
                ::rpc_core_v1::client::ClientProxy::new(client)
            }
        }
    }
}

/// 展开一个服务端 impl 块：保留原 impl，并补上 `TrRpcService` 实现。
pub(crate) fn expand_service_of_(model: &ServiceOfModel_) -> syn::Result<zyn::Output> {
    let impl_item = &model.impl_item_;
    let generated = service_of_impl_(model);

    Result::Ok(zyn::zyn! {
        {{ impl_item }}
        {{ generated }}
    })
}

/// 生成 `abs_rpc_serv::service::TrRpcService` 的实现。
fn service_of_impl_(model: &ServiceOfModel_) -> ::zyn::TokenStream {
    let impl_item = &model.impl_item_;
    let trait_path = &model.trait_path_;
    let self_ty = &model.self_ty_;
    let interface = &model.interface_path_;
    let cfg = cfg_attrs_(&impl_item.attrs);

    quote! {
        #(#cfg)*
        #[automatically_derived]
        impl<__C> ::rpc_core_v1::x_deps::abs_rpc_serv::service::TrRpcService<
            #interface,
            __C,
        > for #self_ty
        where
            #interface: ::rpc_core_v1::messaging::TrRpcInterface,
        {
            const SERVICE_PATH: &'static str =
                <#self_ty as #trait_path>::SERVICE_PATH;

            type Err = ::rpc_core_v1::messaging::RpcError;

            fn dispatch_async<'__f, __X>(
                &'__f self,
                ctx: &'__f mut __X,
            ) -> impl ::rpc_core_v1::x_deps::abs_cancel::TrMayCancel<
                '__f,
                MayCancelOutput = ::core::result::Result<(), Self::Err>,
            > + '__f
            where
                __C: '__f,
                __X: ::rpc_core_v1::x_deps::abs_rpc_serv::service::TrRpcCallCtx<
                    __C,
                    Err = Self::Err,
                > + '__f,
            {
                <#self_ty as #trait_path>::__rpc_dispatch_async::<__C, __X>(self, ctx)
            }
        }
    }
}

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

    Result::Ok(quote! {
        #[doc(hidden)]
        #[allow(non_snake_case, non_camel_case_types, unused_imports)]
        pub mod #module_ident {
            use super::*;
            use ::mptp_serving::{Endpoint, MethodSet, MptpContext, RadixRouter, TrEndpointHandler};
            use ::mptp_serving::x_deps::mptp_smux::{ServeError, serving::config::TrConfig};
            use ::rpc_core_v1::serving::{TrReplyError, decode_body_async, write_ok_async, write_status_async};

            #(#steps)*
            #(#handlers)*
            #install
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

#[cfg(test)]
mod tests_ {
    use zyn::syn::parse_quote;

    use super::*;

    /// 测试返回类型重建会保留额外 bound 并使用全限定路径。
    /// Test that return-type rebuilding keeps extra bounds and uses fully qualified paths.
    ///
    /// - 手段：构造一个带 `+ Send` 的方法模型，重建返回类型。
    /// - 判断：输出 token 同时包含 `impl ::rpc_core_v1` 与 `+ Send`。
    ///
    /// - Means: build a method model with `+ Send` and rebuild its return type.
    /// - Verdict: the tokens contain both the fully qualified `impl` and `+ Send`.
    #[test]
    fn rebuilds_return_type_() {
        let method = MethodModel_ {
            ident_: zyn::format_ident!("find"),
            sig_: parse_quote!(fn find(&self) -> impl TrMayCancel<'_, MayCancelOutput = RpcResult<u8>>),
            attrs_: Vec::new(),
            args_: Vec::new(),
            output_ty_: parse_quote!(u8),
            may_cancel_lifetime_: Option::Some(Lifetime::new("'_", proc_macro2::Span::call_site())),
            extra_bounds_: vec![parse_quote!(Send)],
        };
        let ty = return_type_(&method.output_ty_, &method).expect("返回类型应当重建成功");
        let text = quote!(#ty).to_string().replace(' ', "");
        assert!(text.contains("::rpc_core_v1"), "应使用全限定路径：{text}");
        assert!(text.contains("Send"), "应保留额外 bound：{text}");
    }

    /// 测试借用参数的客户端转换表达式。
    /// Test client-side conversion expressions for borrowed arguments.
    ///
    /// - 手段：分别构造 `&str` / `&[u8]` / `&u64` 的转换种类并生成表达式。
    /// - 判断：得到 `to_string()` / `to_vec()` / 解引用三种表达式。
    ///
    /// - Means: build conversion kinds for `&str`, `&[u8]` and `&u64`.
    /// - Verdict: the generated expressions are `to_string()`, `to_vec()` and a deref.
    #[test]
    fn converts_borrowed_expressions_() {
        let name = zyn::format_ident!("value");

        let text = convert_expr_(&ArgModel_ {
            ident_: name.clone(),
            owned_ty_: parse_quote!(String),
            is_ref_: true,
            convert_: ConvertKind_::ToString,
        });
        assert!(quote!(#text).to_string().contains("to_string"));

        let text = convert_expr_(&ArgModel_ {
            ident_: name.clone(),
            owned_ty_: parse_quote!(Vec<u8>),
            is_ref_: true,
            convert_: ConvertKind_::ToVec,
        });
        assert!(quote!(#text).to_string().contains("to_vec"));

        let text = convert_expr_(&ArgModel_ {
            ident_: name.clone(),
            owned_ty_: parse_quote!(u64),
            is_ref_: true,
            convert_: ConvertKind_::Deref,
        });
        assert!(quote!(#text).to_string().starts_with('*'));

        let text = convert_expr_(&ArgModel_ {
            ident_: name,
            owned_ty_: parse_quote!(::std::string::String),
            is_ref_: true,
            convert_: ConvertKind_::Clone,
        });
        assert!(quote!(#text).to_string().contains("clone"));
    }
}
