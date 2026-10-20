//! `#[rpc]` 的代码展开。
//!
//! service 宏的展开分为五块：
//!
//! 1. 在 service trait 里注入 `SERVICE_PATH`；
//! 2. 生成一个 **owned body enum**：变体名 = 方法名，字段名 = 参数名；
//! 3. 生成客户端代理对统一 `ClientProxy` 的 trait 实现；
//! 4. 在隐藏模块里生成服务端 dispatch（`match` body enum → 调用用户实现）；
//! 5. 在隐藏模块里生成 `Worker`：有界队列、`route()` 与可取消的 `run_async()`。
//!
//! interface 宏的展开生成 `TrRpcInterface`、`ServiceSelector`、`ApiV1::workers(...)` 与
//! 具体类型的 worker 组（`routes()` / `run_async()`）。
//!
//! `denco_macro` 当前不支持带生命周期参数的派生类型，所以客户端与服务端共用同一个
//! owned body enum；客户端方法在构造变体时把支持的借用参数转成 owned 字段。
//!
//! # `expand_`: code generation for both macro forms
//!
//! Service expansion injects `SERVICE_PATH`, emits one owned body enum, implements the
//! service trait for the unified `ClientProxy`, and generates the serving dispatch and worker
//! inside a hidden module. Interface expansion emits the interface contract impls and the
//! concrete worker group. `denco_macro` cannot derive lifetime-parameterized types yet, so
//! both sides share the owned enum and the client converts supported borrowed arguments into
//! owned fields.

use zyn::{
    format_ident,
    quote::quote,
    syn::{self, Lifetime, Type, TypeParamBound},
};

use crate::{
    analyze_::cfg_attrs_,
    model_::{ArgModel_, ConvertKind_, InterfaceModel_, MethodModel_, ServiceModel_},
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
            ::core_rpc_mptp_smux_v1::x_deps::denco_macro::TrEncodable,
            ::core_rpc_mptp_smux_v1::x_deps::denco_macro::TrDecodable
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
            <__C as ::core_rpc_mptp_smux_v1::x_deps::abs_mptp::client::config::TrConfig>::SharedConn:
                Sync,
        }
    } else {
        quote!()
    };

    Result::Ok(quote! {
        impl<'__a, __C, __I> #trait_ident
            for ::core_rpc_mptp_smux_v1::client::ClientProxy<'__a, __C, __I>
        where
            __C: ::core_rpc_mptp_smux_v1::client::TrRpcClientConfig + Send + Sync + 'static,
            #shared_conn_bound
            __I: ::core_rpc_mptp_smux_v1::TrRpcInterface + Send + Sync + 'static,
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
            ::core_rpc_mptp_smux_v1::client::call_unary_async::<
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
        impl ::core_rpc_mptp_smux_v1::x_deps::abs_cancel::TrMayCancel<
            #lifetime,
            MayCancelOutput = ::core_rpc_mptp_smux_v1::RpcResult<#output_ty>
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
    let worker = worker_(model);

    Result::Ok(quote! {
        #[doc(hidden)]
        pub mod #module_ident {
            #[allow(unused_imports)]
            use super::*;
            use ::core_rpc_mptp_smux_v1::x_deps::abs_cancel;
            use ::core_rpc_mptp_smux_v1::x_deps::abs_cancel::{
                TrCancellationToken,
                TrMayCancel,
            };
            use ::core_rpc_mptp_smux_v1::x_deps::abs_denco;
            use ::core_rpc_mptp_smux_v1::x_deps::futures;
            use ::core_rpc_mptp_smux_v1::x_deps::futures::StreamExt as _;
            use ::core_rpc_mptp_smux_v1::x_deps::gen_mcf2;
            use ::core_rpc_mptp_smux_v1::serving::{
                RpcCallCtx,
                RpcJob,
                ServiceRoute,
                TrServingConfig,
            };
            use ::core_rpc_mptp_smux_v1::RpcResult;

            #body_enum
            #step
            #wrapper
            #worker
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
        #[::core_rpc_mptp_smux_v1::x_deps::gen_mcf2::gen_may_cancel_future(
            Dispatch,
            pub(crate),
            new(pub(crate))
        )]
        async fn dispatch_async_<'__a, '__f, __S, __C, __Tok>(
            service: &'__f __S,
            ctx: &'__f mut RpcCallCtx<'__a, __C>,
            cancel: __Tok,
        ) -> RpcResult<()>
        where
            '__a: '__f,
            __S: #trait_ident,
            __C: TrServingConfig,
            __Tok: abs_cancel::TrCancellationToken,
        {
            let call: Call = match ctx
                .decode_call_async()
                .may_cancel_with(cancel.child_token())
                .await
            {
                Ok(call) => call,
                Err(err) => {
                    ctx.reply_status_async(err.status(), err.to_string())
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
                    ctx.reply_status_async(err.status(), err.to_string())
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
        pub fn dispatch_async<'__a, '__f, __S, __C>(
            service: &'__f __S,
            ctx: &'__f mut RpcCallCtx<'__a, __C>,
        ) -> impl TrMayCancel<'__f, MayCancelOutput = RpcResult<()>> + '__f
        where
            '__a: '__f,
            __S: #trait_ident,
            __C: TrServingConfig,
        {
            DispatchAsync::<'__a, '__f, '__f, __S, __C>::new(service, ctx)
        }
    })
}

/// 生成一个 service 的 worker：有界队列、`route()` 与 `run_async()`。
///
/// worker 不 spawn、不选运行时；它只把「取 job → 调用 dispatch → 收尾」这条循环包成一个
/// 可取消 future，由使用者决定放到哪个线程 / 运行时上。
fn worker_(model: &ServiceModel_) -> ::zyn::TokenStream {
    let trait_ident = &model.trait_ident_;

    quote! {
        /// 一个 service 的 worker：持有用户实现与自己的有界队列。
        ///
        /// 用户实现 → `Worker` → `route()` → router 的 service Trie，是一条显式的装配链；
        /// 框架不替使用者 spawn 线程，也不假定运行时。
        pub struct Worker<__S, __C>
        where
            __C: TrServingConfig,
        {
            /// 用户实现。
            service_: __S,

            /// 本 service 的队列接收半边。
            queue_: futures::channel::mpsc::Receiver<RpcJob<__C>>,

            /// 交给 router 的注册项。
            route_: ServiceRoute<__C>,
        }

        impl<__S, __C> Worker<__S, __C>
        where
            __S: #trait_ident,
            __C: TrServingConfig,
        {
            /// 用缺省队列容量建立一个 worker。
            pub fn new(service: __S) -> Self {
                Self::with_capacity(
                    service,
                    ::core_rpc_mptp_smux_v1::serving::K_DEFAULT_SERVICE_QUEUE_CAP,
                )
            }

            /// 用指定队列容量建立一个 worker；容量为 0 时按 1 处理。
            pub fn with_capacity(service: __S, capacity: usize) -> Self {
                let (sender, queue) =
                    futures::channel::mpsc::channel::<RpcJob<__C>>(capacity.max(1usize));
                let route =
                    ServiceRoute::new(<__S as #trait_ident>::SERVICE_PATH, sender);
                Worker {
                    service_: service,
                    queue_: queue,
                    route_: route,
                }
            }

            /// 返回 `(SERVICE_PATH, 队列发送端)` 的注册项，供 router 插入 service Trie。
            pub fn route(&self) -> ServiceRoute<__C> {
                self.route_.clone()
            }

            /// 用户实现。
            pub fn service(&self) -> &__S {
                &self.service_
            }

            /// 驱动 worker 循环，直到队列关闭或取消令牌触发。
            ///
            /// 返回的 future 可以直接 `.await`（不可取消），也可以先
            /// `.may_cancel_with(token)` 再等待。每处理完一个 job 都会检查取消信号；
            /// 等待下一个 job 时同时等待取消信号，因此空闲 worker 也能被及时停止。
            pub fn run_async<'__f>(
                &'__f mut self,
            ) -> impl TrMayCancel<'__f, MayCancelOutput = RpcResult<()>> + '__f
            where
                __S: '__f,
                __C: '__f,
            {
                RunWorkerAsync::<'__f, '__f, __S, __C>::new(self)
            }
        }

        /// `Worker::run_async` 的 step 函数。
        #[allow(clippy::needless_lifetimes)]
        #[gen_mcf2::gen_may_cancel_future(RunWorker, pub(crate), new(pub(crate)))]
        async fn run_worker_async_<'__f, __S, __C, __Tok>(
            worker: &'__f mut Worker<__S, __C>,
            cancel: __Tok,
        ) -> RpcResult<()>
        where
            __S: #trait_ident,
            __C: TrServingConfig,
            __Tok: TrCancellationToken,
        {
            let watcher = cancel.child_token();
            let mut cancelled = ::core::pin::pin!(async move {
                watcher.cancellation().await;
            });
            loop {
                if cancel.is_cancelled() {
                    break;
                }
                let job = {
                    let mut next = ::core::pin::pin!(worker.queue_.next());
                    match futures::future::select(next, cancelled.as_mut()).await {
                        futures::future::Either::Left((
                            ::core::option::Option::Some(job),
                            _,
                        )) => job,
                        futures::future::Either::Left((
                            ::core::option::Option::None,
                            _,
                        )) => break,
                        futures::future::Either::Right(((), _)) => break,
                    }
                };

                let (location, headers, tx, rx) = job.into_parts();
                let mut ctx = RpcCallCtx::<__C>::new(&location, &headers, tx, rx);
                let result = dispatch_async(&worker.service_, &mut ctx)
                    .may_cancel_with(cancel.child_token())
                    .await;
                ctx.close_async().await;
                result?;
            }

            ::core::result::Result::Ok(())
        }
    }
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

/// 生成 interface 的契约实现、`workers(...)` 构造器与 worker 组。
fn interface_items_(model: &InterfaceModel_) -> ::zyn::TokenStream {
    let struct_ident = &model.struct_ident_;
    let path = &model.path_;
    let workers_ident = &model.workers_ident_;
    let run_prefix = format_ident!(
        "__rpc_interface_{}RunWorkers",
        struct_ident.to_string().trim_start_matches("r#")
    );
    let run_async_ident = format_ident!("{}Async", run_prefix);
    let run_fn_ident = format_ident!(
        "__rpc_interface_{}_run_workers_async_",
        struct_ident.to_string().trim_start_matches("r#")
    );

    let service_idents = (0usize..model.services_.len())
        .map(|index| format_ident!("__S{index}"))
        .collect::<Vec<_>>();
    let trait_paths = model
        .services_
        .iter()
        .map(|service| &service.trait_path_)
        .collect::<Vec<_>>();
    let module_paths = model
        .services_
        .iter()
        .map(|service| &service.module_path_)
        .collect::<Vec<_>>();
    let field_idents = (0usize..model.services_.len())
        .map(|index| format_ident!("worker_{index}_"))
        .collect::<Vec<_>>();
    let param_idents = (0usize..model.services_.len())
        .map(|index| format_ident!("service_{index}"))
        .collect::<Vec<_>>();
    let local_idents = (0usize..model.services_.len())
        .map(|index| format_ident!("worker_{index}"))
        .collect::<Vec<_>>();

    let runners = local_idents
        .iter()
        .map(|local| {
            quote! {
                ::core::future::IntoFuture::into_future(
                    #local
                        .run_async()
                        .may_cancel_with(cancel.child_token()),
                )
            }
        })
        .collect::<Vec<_>>();
    let run_tree = join_tree_(&runners);

    quote! {
        impl ::core_rpc_mptp_smux_v1::TrRpcInterface for #struct_ident {
            const PATH: &'static str = #path;
        }

        impl<__C> ::core_rpc_mptp_smux_v1::client::ServiceSelector<__C> for #struct_ident
        where
            __C: ::core_rpc_mptp_smux_v1::client::TrRpcClientConfig,
        {
            type View<'__a>
                = ::core_rpc_mptp_smux_v1::client::ClientProxy<'__a, __C, #struct_ident>
            where
                Self: '__a,
                __C: '__a;

            fn select<'__a>(
                client: &'__a ::core_rpc_mptp_smux_v1::x_deps::mptp_smux::client::MptpClient<__C>,
            ) -> Self::View<'__a> {
                ::core_rpc_mptp_smux_v1::client::ClientProxy::new(client)
            }
        }

        impl #struct_ident {
            /// 为这个 interface 建立每个 service 的队列与 worker 组。
            ///
            /// `C` 是服务端配置；调用点通常由后续的
            /// [`RpcRouter::add_interface`](::core_rpc_mptp_smux_v1::serving::RpcRouter::add_interface)
            /// 推导。若编译器无法推导，可用
            /// `ApiV1::workers::<ServingCfg, _, _>(...)` 显式指定。
            pub fn workers<__C, #(#service_idents),*>(
                #(#param_idents: #service_idents),*
            ) -> #workers_ident<__C, #(#service_idents),*>
            where
                __C: ::core_rpc_mptp_smux_v1::serving::TrServingConfig,
                #(#service_idents: #trait_paths),*
            {
                #workers_ident::new(#(#param_idents),*)
            }
        }

        /// 一个 interface 的 worker 组。
        ///
        /// 它按 service 列表顺序持有每个 service 的 `Worker`；`routes()` 交给 router，
        /// `run_async()` 并发驱动全部 worker。
        #[doc(hidden)]
        pub struct #workers_ident<__C, #(#service_idents),*>
        where
            __C: ::core_rpc_mptp_smux_v1::serving::TrServingConfig,
            #(#service_idents: #trait_paths),*
        {
            #(#field_idents: #module_paths::Worker<#service_idents, __C>),*
        }

        impl<__C, #(#service_idents),*> #workers_ident<__C, #(#service_idents),*>
        where
            __C: ::core_rpc_mptp_smux_v1::serving::TrServingConfig,
            #(#service_idents: #trait_paths),*
        {
            /// 建立全部 service 的 worker；由 `ApiV1::workers` 调用。
            #[allow(dead_code)]
            pub(crate) fn new(
                #(#param_idents: #service_idents),*
            ) -> Self {
                #workers_ident {
                    #(#field_idents: #module_paths::Worker::new(#param_idents)),*
                }
            }

            /// 收集全部 service 的注册项，供
            /// [`RpcRouter::add_interface`](::core_rpc_mptp_smux_v1::serving::RpcRouter::add_interface)
            /// 插入该 interface 的 service Trie。
            pub fn routes(&self) -> ::std::vec::Vec<
                ::core_rpc_mptp_smux_v1::serving::ServiceRoute<__C>,
            > {
                ::std::vec![#(self.#field_idents.route()),*]
            }

            /// 并发驱动全部 service worker；返回的 future 可取消。
            pub fn run_async<'__f>(
                &'__f mut self,
            ) -> impl ::core_rpc_mptp_smux_v1::x_deps::abs_cancel::TrMayCancel<
                '__f,
                MayCancelOutput = ::core_rpc_mptp_smux_v1::RpcResult<()>,
            > + '__f
            where
                __C: '__f,
                #(#service_idents: '__f),*
            {
                #run_async_ident::<'__f, '__f, __C, #(#service_idents),*>::new(self)
            }
        }

        /// `Workers::run_async` 的 step 函数。
        #[allow(clippy::needless_lifetimes)]
        #[::core_rpc_mptp_smux_v1::x_deps::gen_mcf2::gen_may_cancel_future(
            #run_prefix,
            pub(crate),
            new(pub(crate))
        )]
        async fn #run_fn_ident<'__f, __C, #(#service_idents),*, __Tok>(
            workers: &'__f mut #workers_ident<__C, #(#service_idents),*>,
            cancel: __Tok,
        ) -> ::core_rpc_mptp_smux_v1::RpcResult<()>
        where
            __C: ::core_rpc_mptp_smux_v1::serving::TrServingConfig,
            #(#service_idents: #trait_paths),*,
            __Tok: ::core_rpc_mptp_smux_v1::x_deps::abs_cancel::TrCancellationToken,
        {
            let #workers_ident {
                #(#field_idents: #local_idents),*
            } = workers;
            let result = #run_tree.await;
            result?;
            ::core::result::Result::Ok(())
        }
    }
}

/// 把多个 worker future 组合成一棵 `try_join` 树。
///
/// `try_join` 只支持两个 future，因此这里按二分递归组合；任一 worker 出错时提前返回，
/// 其余 worker future 被丢弃（队列随之关闭）。空列表不会出现：interface 分析已拒绝空
/// `services`。
fn join_tree_(runners: &[::zyn::TokenStream]) -> ::zyn::TokenStream {
    match runners {
        [single] => single.clone(),
        [] => quote! {
            async {
                ::core::result::Result::<(), ::core_rpc_mptp_smux_v1::RpcError>::Ok(())
            }
        },
        _ => {
            let middle = runners.len() / 2usize;
            let left = join_tree_(&runners[..middle]);
            let right = join_tree_(&runners[middle..]);
            quote!(::core_rpc_mptp_smux_v1::x_deps::futures::future::try_join(#left, #right))
        }
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
    /// - 判断：输出 token 同时包含 `impl ::core_rpc_mptp_smux_v1` 与 `+ Send`。
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
        assert!(
            text.contains("::core_rpc_mptp_smux_v1"),
            "应使用全限定路径：{text}"
        );
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
