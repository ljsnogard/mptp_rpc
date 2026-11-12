//! 路由端点：一个 pattern + 方法集合 + 终端处理器。
//!
//! 端点是“将要运行什么”的**描述**，不是执行体。元数据字段在 P2 补上（端点携带类型化元数据，
//! 中间件按元数据执行策略）。
//!
//! # `endpoint_`: a route endpoint
//!
//! An endpoint describes what will run (pattern + method set + terminal handler); it is not
//! the execution itself. Metadata arrives in P2.

use alloc::sync::Arc;

use abs_mptp::spec::access_method::AccessMethod;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::MethodSet;

use super::handler_::TrEndpointHandler;

/// 一个路由目标：`pattern` + 方法集合 + 终端处理器。
///
/// `C` 是服务端配置类型；它只出现在 handler 的类型约束里，端点上不存储 `C` 的值。
/// `C: 'static` 是因为 handler 以 `Arc<dyn TrEndpointHandler<C>>`（默认 `'static` 对象）
/// 存储，需要配置类型自身可静态存活。
pub struct Endpoint<C>
where
    C: TrConfig + 'static,
{
    /// 路径模式（相对所属 router），如 `"feeds/{id}"`；静态字符串，零拷贝。
    pattern_: &'static str,

    /// 本端点响应的方法集合。
    methods_: MethodSet,

    /// 终端处理器；类型擦除只发生在装配边界。
    handler_: Arc<dyn TrEndpointHandler<C>>,
}

impl<C> Endpoint<C>
where
    C: TrConfig + 'static,
{
    /// 由「模式 + 方法集合 + 处理器」构造。
    pub fn new(
        pattern: &'static str,
        methods: MethodSet,
        handler: Arc<dyn TrEndpointHandler<C>>,
    ) -> Self {
        Endpoint {
            pattern_: pattern,
            methods_: methods,
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
    pub fn handler(&self) -> &Arc<dyn TrEndpointHandler<C>> {
        &self.handler_
    }
}
