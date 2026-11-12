//! 路由端点：一个 pattern + 方法集合 + 元数据 + 终端处理器。
//!
//! 端点是“将要运行什么”的**描述**，不是执行体。元数据是类型化标记集合，中间件按它执行
//! 横切策略（P2）。
//!
//! # `endpoint_`: a route endpoint
//!
//! An endpoint describes what will run (pattern + method set + metadata + terminal handler);
//! it is not the execution itself. Metadata is a typed marker collection read by middleware.

use alloc::{alloc::Global, sync::Arc, vec::Vec};
use core::{alloc::Allocator, any::Any};

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::{MethodSet, serving::config::TrConfig};

use super::{handler_::TrEndpointHandler, metadata_::EndpointMetadata};

/// 一个路由目标：`pattern` + 方法集合 + 元数据 + 终端处理器。
///
/// `C` 是服务端配置类型；它只出现在 handler 的类型约束里，端点上不存储 `C` 的值。
/// `C: 'static` 是因为 handler 以 `Arc<dyn TrEndpointHandler<C>>`（默认 `'static` 对象）
/// 存储。元数据集合的分配器固定为 [`Global`]（[`EndpointMetadata`] 类型本身可参数化）。
pub struct Endpoint<C, S = ()>
where
    C: TrConfig + 'static,
{
    /// 路径模式（相对所属 router），如 `"feeds/{id}"`；静态字符串，零拷贝。
    pattern_: &'static str,

    /// 本端点响应的方法集合。
    methods_: MethodSet,

    /// 本端点的类型化元数据（P2：中间件按它执行策略）。
    metadata_: EndpointMetadata<Global>,

    /// 终端处理器；类型擦除只发生在装配边界。
    handler_: Arc<dyn TrEndpointHandler<C, S>>,
}

impl<C, S> Endpoint<C, S>
where
    C: TrConfig + 'static,
{
    /// 由「模式 + 方法集合 + 处理器」构造。
    pub fn new(
        pattern: &'static str,
        methods: MethodSet,
        handler: Arc<dyn TrEndpointHandler<C, S>>,
    ) -> Self {
        Endpoint {
            pattern_: pattern,
            methods_: methods,
            metadata_: EndpointMetadata::new_in(Global),
            handler_: handler,
        }
    }

    /// 路径模式。
    pub fn pattern(&self) -> &'static str {
        self.pattern_
    }

    /// 本端点响应的方法集合。
    pub fn methods(&self) -> MethodSet {
        self.methods_
    }

    /// 是否接受某个方法。
    pub fn accepts(&self, method: AccessMethod) -> bool {
        self.methods_.contains(method)
    }

    /// 终端处理器。
    pub fn handler(&self) -> &Arc<dyn TrEndpointHandler<C, S>> {
        &self.handler_
    }

    /// 本端点的元数据。
    pub fn metadata(&self) -> &EndpointMetadata<Global> {
        &self.metadata_
    }

    /// 追加一个类型化元数据标记（消费式构建）。
    pub fn with_metadata<T>(mut self, value: T) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        self.metadata_.insert(value);
        self
    }
}

/// 同一个路径模式上的端点集合：一条路径可以挂多个端点，各自声明不同的方法集合。
///
/// 设计稿 §6.7 的 `feeds/{id}` 同时挂 `VIEW` + `PULL` 就落在这里：匹配时按插入顺序取
/// **第一个**接受该方法的端点；`405` 的 `Allow` 取集合内所有端点方法集合的并集。
/// 集合在装配期构建、请求路径只读。
///
/// `A` 是集合内 `Vec` 的分配器，由 [`RadixRouter`](super::RadixRouter) 传入，与它的子
/// router 列表共用同一个分配器策略。
///
/// 装配约定：**同路径同方法**不做冲突检测，先插入的端点先命中；装配者应当让同一路径上的
/// 方法集合互不相交。
pub(crate) struct EndpointSet<C, S = (), A = Global>
where
    C: TrConfig + 'static,
    A: Allocator,
{
    /// 按插入顺序保存的端点。
    entries_: Vec<Endpoint<C, S>, A>,
}

impl<C, S, A> EndpointSet<C, S, A>
where
    C: TrConfig + 'static,
    A: Allocator,
{
    /// 用指定分配器造一个空集合。
    pub(crate) fn new_in(alloc: A) -> Self {
        EndpointSet {
            entries_: Vec::new_in(alloc),
        }
    }

    /// 追加一个端点（同路径的新端点排在已有端点之后）。
    pub(crate) fn push(&mut self, endpoint: Endpoint<C, S>) {
        self.entries_.push(endpoint);
    }

    /// 取第一个接受 `method` 的端点；都不接受时返回 `None`。
    pub(crate) fn select(&self, method: AccessMethod) -> Option<&Endpoint<C, S>> {
        self.entries_
            .iter()
            .find(|endpoint| endpoint.accepts(method))
    }

    /// 集合内所有端点方法集合的并集（`405` 的 `Allow`）。
    pub(crate) fn allowed(&self) -> MethodSet {
        self.entries_
            .iter()
            .fold(MethodSet::NONE, |allowed, endpoint| {
                allowed.union(endpoint.methods())
            })
    }
}
