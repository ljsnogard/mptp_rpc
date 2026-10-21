//! 两级 Trie router：interface 前缀 → 该 interface 内部的 service 前缀。
//!
//! [`RpcRouter`] 只实现 core 的 [`TrRpcRouter`]：它回答「一个 location 命中哪个
//! service」，并持有每个 service 的 [`ServiceRoute`]。把 job 交给 service、未命中时的
//! `404` / `405` 回复与 channel 收尾，都由
//! [`RpcRouterHandler`](rpc_core_v1::serving::RpcRouterHandler) 完成。
//!
//! 两级路径都按**完整路径段**匹配，支持最长前缀；`api/v10` 不会误命中 `api/v1`。
//! 同一个 interface 或同一个 interface 下的同一个 service 重复注册会返回
//! [`RpcRouterError`]，且不会改动已有注册表。
//!
//! # `router_`: the two-level trie router
//!
//! [`RpcRouter`] implements only [`TrRpcRouter`]: it matches an interface prefix and then a
//! service prefix inside it. Job delivery, rejection replies and channel cleanup stay in the
//! core handler adapter.

use rpc_core_v1::serving::{
    RpcRouterHandler, ServiceRoute, TrRpcRouter, TrServingConfig,
};
use thiserror::Error;

use crate::trie_::{PathTrie, split_segments_};

/// interface 级路由项。
struct InterfaceEntry_<C: TrServingConfig> {
    /// 该 interface 下的 service Trie。
    services_: PathTrie<ServiceRoute<C>>,
}

/// 服务端两级 Trie 路由器。
///
/// 一个 server 可以注册多个 interface（`ApiV1`、`ApiV2`……），每个 interface 内部再按
/// service 的 `SERVICE_PATH` 注册。匹配失败不会把 channel 交给 worker；是否写 `404` 由
/// 接入方（通常是
/// [`RpcRouterHandler`](rpc_core_v1::serving::RpcRouterHandler)）决定。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::serving::TrServingConfig;
/// use rpc_router_trie::{RpcRouter, RpcRouterError};
///
/// fn register_interface<C: TrServingConfig>(
///     router: &mut RpcRouter<C>,
/// ) -> Result<(), RpcRouterError> {
///     router.add_interface("api/v1", std::iter::empty())
/// }
/// ```
pub struct RpcRouter<C: TrServingConfig> {
    /// interface 级 Trie。
    interfaces_: PathTrie<InterfaceEntry_<C>>,
}

impl<C: TrServingConfig> RpcRouter<C> {
    /// 造一个空 router。
    pub fn new() -> Self {
        RpcRouter {
            interfaces_: PathTrie::new(),
        }
    }

    /// 注册一个 interface 及其全部 service。
    ///
    /// interface 路径与 service 路径都按完整路径段比较；同一个 interface 或同一个
    /// interface 下的同一个 service 重复注册会返回错误，且不会改动已有注册表。
    ///
    /// # Errors
    ///
    /// - [`RpcRouterError::DuplicateInterface`]：interface 路径已经注册；
    /// - [`RpcRouterError::DuplicateService`]：同一 interface 下的 service 路径重复。
    pub fn add_interface<I>(&mut self, interface: &str, routes: I) -> Result<(), RpcRouterError>
    where
        I: IntoIterator<Item = ServiceRoute<C>>,
    {
        <Self as TrRpcRouter<C>>::add_interface(self, interface, routes)
    }

    /// 解析一个完整 location，返回命中 service 的注册项。
    ///
    /// 先匹配 interface 的最长前缀，再在剩余段上匹配 service 的最长前缀。剩余段之后
    /// 还有内容时仍然命中——service 路径不是请求的终点，真正的方法由 body enum 决定。
    pub fn resolve(&self, location: &str) -> Option<&ServiceRoute<C>> {
        <Self as TrRpcRouter<C>>::resolve(self, location)
    }

    /// 把本 router 包成可直接交给 `MptpServer` 的 handler。
    pub fn into_handler(self) -> RpcRouterHandler<Self, C> {
        RpcRouterHandler::new(self)
    }
}

impl<C: TrServingConfig> Default for RpcRouter<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: TrServingConfig> TrRpcRouter<C> for RpcRouter<C> {
    type Route = ServiceRoute<C>;

    type RegisterErr = RpcRouterError;

    fn add_interface<I>(&mut self, interface: &str, routes: I) -> Result<(), Self::RegisterErr>
    where
        I: IntoIterator<Item = Self::Route>,
    {
        if self.interfaces_.contains(interface) {
            return Result::Err(RpcRouterError::DuplicateInterface(interface.to_string()));
        }

        let services = build_service_trie_(routes.into_iter().map(|route| (route.path(), route)))
            .map_err(|service| RpcRouterError::DuplicateService {
            interface: interface.to_string(),
            service: service.to_string(),
        })?;

        self.interfaces_.insert(
            interface,
            InterfaceEntry_ {
                services_: services,
            },
        );
        Result::Ok(())
    }

    fn resolve(&self, location: &str) -> Option<&Self::Route> {
        let segments = split_segments_(location);
        let (interface, depth) = self.interfaces_.match_longest(&segments)?;
        let (service, _) = interface.services_.match_longest(&segments[depth..])?;
        Option::Some(service)
    }
}

/// 路由器注册过程中的错误。
///
/// # Examples
///
/// ```
/// use rpc_router_trie::RpcRouterError;
///
/// let err = RpcRouterError::DuplicateInterface("api/v1".to_string());
/// assert!(err.to_string().contains("api/v1"));
/// ```
#[derive(Debug, Error)]
pub enum RpcRouterError {
    /// interface 路径已经注册。
    #[error("interface 路径 `{0}` 已注册")]
    DuplicateInterface(String),

    /// 同一 interface 下的 service 路径重复。
    #[error("interface `{interface}` 下的 service 路径 `{service}` 已注册")]
    DuplicateService {
        /// interface 路径。
        interface: String,

        /// 重复的 service 路径。
        service: String,
    },
}

/// 用 service 路径与值建一棵 service Trie；路径重复时返回重复的那条路径。
///
/// 抽成泛型函数是为了能用整数做单元测试：路由表的重复检测与路径规范化不依赖
/// `ServiceRoute<C>` 里的 channel 类型。
fn build_service_trie_<T>(
    routes: impl IntoIterator<Item = (&'static str, T)>,
) -> Result<PathTrie<T>, &'static str> {
    let mut services = PathTrie::new();
    for (path, value) in routes {
        if services.insert(path, value).is_some() {
            return Result::Err(path);
        }
    }
    Result::Ok(services)
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 service Trie 构建会拒绝重复路径并保留规范化后的最长匹配。
    /// Test that service-trie construction rejects duplicates and keeps normalised paths.
    ///
    /// - 手段：分别用 `["/api/v1/", 1]`、`["api", 2], ["api/v1", 3]` 与重复 `"api"`
    ///   构建 Trie。
    /// - 判断：前两者成功且最长匹配正确；重复路径返回 `Err("api")`。
    ///
    /// - Means: build tries from a slashed path, an overlapping pair, and a duplicate pair.
    /// - Verdict: the first two succeed with the right longest match; the duplicate reports
    ///   the offending path.
    #[test]
    fn builds_service_trie_() {
        let trie = build_service_trie_([("/api/v1/", 1u32)]).expect("路径应当合法");
        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&1u32, 2usize)));

        let trie = build_service_trie_([("api", 2u32), ("api/v1", 3u32)])
            .expect("重叠路径按最长匹配合法共存");
        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&3u32, 2usize)));
        assert_eq!(trie.match_longest(&["api", "v2"]), Some((&2u32, 1usize)));

        let duplicate = build_service_trie_([("api", 1u32), ("/api/", 2u32)]);
        assert_eq!(duplicate.err(), Some("/api/"));
    }
}
