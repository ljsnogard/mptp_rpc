//! 路由端点：一个 pattern + 方法集合 + 元数据 + 终端处理器。
//!
//! 端点是“将要运行什么”的**描述**，不是执行体。元数据是类型化标记集合，中间件按它执行
//! 横切策略（P2）。
//!
//! # `endpoint_`: a route endpoint
//!
//! An endpoint describes what will run (pattern + method set + metadata + terminal handler);
//! it is not the execution itself. Metadata is a typed marker collection read by middleware.

use alloc::alloc::Global;
use alloc::sync::Arc;
use core::any::Any;

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::MethodSet;

use super::handler_::TrEndpointHandler;
use super::metadata_::EndpointMetadata;

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
