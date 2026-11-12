//! 分层路由器的默认实现（round 1 / P0：单层 handler 规则 + 根 endpoint）。
//!
//! 路径树复用 [`matchit_route`]（no_std + bumpalo），本 crate 不新造匹配算法。方法维度由
//! [`MethodSet`] 表达：路径命中但方法不在集合内时返回 [`MatchOutcome::MethodNotAllowed`]；
//! 本层无命中时返回 [`MatchOutcome::Root`]，由根 endpoint 兜底。
//!
//! # `router_`: the layered router (flat, P0)
//!
//! The trie is `matchit_route`; the method dimension is `MethodSet`. Unmatched paths fall
//! back to the root endpoint, and a path hit with a disallowed method reports
//! `MethodNotAllowed`.

use alloc::sync::Arc;

use abs_mptp::spec::access_method::AccessMethod;
use matchit_route::Router as Trie;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::MethodSet;

use super::endpoint_::Endpoint;
use super::handler_::TrEndpointHandler;
use super::route_params_::RouteParams;

/// 一次匹配的结果，泛型于端点值类型 `V`（便于纯逻辑单测）。
///
/// 生产类型见 [`MatchOutcome`]（`V = Endpoint<C>`）。
#[doc(hidden)]
pub enum Matched<'f, V> {
    /// 路径 + 方法都命中。
    Found {
        /// 命中的端点。
        value: &'f V,

        /// 零拷贝路径参数。
        params: RouteParams<'f>,
    },

    /// 路径命中但方法不在该路径允许的集合内。
    MethodNotAllowed {
        /// 该路径上允许的方法集合。
        allowed: MethodSet,
    },

    /// 本层没有命中任何路径，交给根 endpoint。
    Root,
}

/// 服务端路由的匹配结果。
pub type MatchOutcome<'f, C> = Matched<'f, Endpoint<C>>;

/// 一个 router 的匹配契约。
///
/// 对象安全：返回类型是具体类型 [`MatchOutcome`]，因此子 router 可以用
/// [`alloc::sync::Arc<dyn TrRouteMatcher>`](alloc::sync::Arc) 擦除。
pub trait TrRouteMatcher<C>: Send + Sync
where
    C: TrConfig,
{
    /// 在「相对于本 router 根」的 `location` 上匹配。
    fn match_route<'f>(&'f self, method: AccessMethod, location: &'f str) -> MatchOutcome<'f, C>;
}

/// 默认实现：单层 handler 规则 + 根 endpoint。
///
/// `C` 是服务端配置类型。当前是**装配期**构建（消费式 `bind`，无运行期增删）；运行期增删
/// 需要 no_std 读多写少锁，属后续 round。
pub struct RadixRouter<C>
where
    C: TrConfig + 'static,
{
    /// 本层根 endpoint：所有未命中的请求都由它兜底。
    root_: Arc<dyn TrEndpointHandler<C>>,

    /// handler 规则：精确路径模式 → 端点。
    trie_: Trie<Endpoint<C>>,
}

impl<C> RadixRouter<C>
where
    C: TrConfig + 'static,
{
    /// 用根 handler 构造一个空 router。
    pub fn new(root: Arc<dyn TrEndpointHandler<C>>) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
        }
    }

    /// 本层根 handler。
    pub fn root(&self) -> &Arc<dyn TrEndpointHandler<C>> {
        &self.root_
    }

    /// 绑定一条 handler 规则：`pattern` + 方法集合 + 终端处理器。
    ///
    /// 消费式构建；`pattern` 是相对本 router 根的路径模式（支持 `{name}` / `{*rest}`）。
    /// 同路径同方法冲突时返回 [`matchit_route::InsertError`]。
    pub fn bind(
        mut self,
        pattern: &'static str,
        methods: MethodSet,
        handler: impl TrEndpointHandler<C> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        let endpoint = Endpoint::new(pattern, methods, Arc::new(handler));
        let normalized = normalize_(endpoint.pattern());
        self.trie_.insert(normalized, endpoint)?;
        Result::Ok(self)
    }
}

impl<C> TrRouteMatcher<C> for RadixRouter<C>
where
    C: TrConfig + 'static,
{
    fn match_route<'f>(&'f self, method: AccessMethod, location: &'f str) -> MatchOutcome<'f, C> {
        match_method_(&self.trie_, method, location, Endpoint::methods)
    }
}

/// 纯匹配：路径 + 方法 → 值。
///
/// 与 `C` 无关，因此可以在单测里用简单值类型（如 `&'static str`）验证；生产路径由
/// [`RadixRouter::match_route`] 以 `V = Endpoint<C>` 调用。
fn match_method_<'f, V>(
    trie: &'f Trie<V>,
    method: AccessMethod,
    location: &'f str,
    methods_of: impl Fn(&V) -> MethodSet,
) -> Matched<'f, V> {
    match trie.at(normalize_(location)) {
        Result::Ok(matched) => {
            let matchit_route::Match { value, params } = matched;
            let allowed = methods_of(value);
            if allowed.contains(method) {
                Matched::Found {
                    value,
                    params: RouteParams::new_(params),
                }
            } else {
                Matched::MethodNotAllowed { allowed }
            }
        }
        Result::Err(_) => Matched::Root,
    }
}

/// 路径规范化：去掉首尾 `/`，与现有 `rpc_core_v1` 的约定一致。
fn normalize_(path: &str) -> &str {
    path.trim_matches('/')
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试路径 + 方法都命中时返回 `Found` 与零拷贝参数。
    /// Test that a matching path and method return `Found` with zero-copy params.
    ///
    /// - 手段：登记 `feeds/{id}` 并用 `View` 匹配 `feeds/42`。
    /// - 判断：结果是 `Found`，值为 `"feed"`，参数 `id == "42"`。
    ///
    /// - Means: register `feeds/{id}` and match `feeds/42` with `View`.
    /// - Verdict: the result is `Found`, the value is `"feed"` and `id == "42"`.
    #[test]
    fn found_with_params_() {
        let mut trie = Trie::new();
        trie.insert("feeds/{id}", "feed").expect("登记规则");

        let matched = match_method_(&trie, AccessMethod::View, "feeds/42", |_| MethodSet::VIEW);
        match matched {
            Matched::Found { value, params } => {
                assert_eq!(*value, "feed");
                assert_eq!(params.get("id"), Option::Some("42"));
            }
            _ => panic!("应当 Found，实际命中了其它分支"),
        }
    }

    /// 测试路径命中但方法不允许时返回 `MethodNotAllowed` 与允许集合。
    /// Test that a path hit with a disallowed method reports `MethodNotAllowed`.
    ///
    /// - 手段：登记 `echo`（只允许 `View`），用 `Call` 匹配。
    /// - 判断：结果是 `MethodNotAllowed { allowed: VIEW }`。
    ///
    /// - Means: register `echo` for `View` only and match it with `Call`.
    /// - Verdict: the result is `MethodNotAllowed { allowed: VIEW }`.
    #[test]
    fn method_not_allowed_() {
        let mut trie = Trie::new();
        trie.insert("echo", "echo").expect("登记规则");

        let matched = match_method_(&trie, AccessMethod::Call, "echo", |_| MethodSet::VIEW);
        match matched {
            Matched::MethodNotAllowed { allowed } => assert_eq!(allowed, MethodSet::VIEW),
            _ => panic!("应当 MethodNotAllowed，实际命中了其它分支"),
        }
    }

    /// 测试路径未命中时返回 `Root`。
    /// Test that an unmatched path returns `Root`.
    ///
    /// - 手段：空 trie 上匹配任意路径。
    /// - 判断：结果是 `Root`。
    ///
    /// - Means: match any path on an empty trie.
    /// - Verdict: the result is `Root`.
    #[test]
    fn root_fallback_() {
        let trie = Trie::<&'static str>::new();
        let matched = match_method_(&trie, AccessMethod::View, "nowhere", |_| MethodSet::ALL);
        assert!(matches!(matched, Matched::Root));
    }

    /// 测试路径规范化去掉了首尾斜杠。
    /// Test that path normalization trims leading and trailing slashes.
    ///
    /// - 手段：对 `/feeds/` 调用 `normalize_`。
    /// - 判断：结果是 `feeds`。
    ///
    /// - Means: call `normalize_` on `/feeds/`.
    /// - Verdict: the result is `feeds`.
    #[test]
    fn normalize_trims_slashes_() {
        assert_eq!(normalize_("/feeds/"), "feeds");
        assert_eq!(normalize_("feeds"), "feeds");
    }
}
