//! 分层路由器的默认实现（round 1 / P0）。
//!
//! 每个 router 以自己为根：`根 endpoint + handler 规则（精确路径模式 + 方法集合）+ 子
//! router 规则（静态前缀）`。匹配顺序是「本层 handler 规则 → 子 router（递归）→ 本层根
//! endpoint」。路径树复用 [`matchit_route`]（no_std + bumpalo）；方法维度由 [`MethodSet`]
//! 表达。子 router 列表是**装配期构建、只读**的，用带分配器参数的 `Vec<T, A>` 存储。
//!
//! 连接作用域状态 `S`（每连接一份）由 router 以 `Arc<S>` 持有，匹配后经
//! [`MptpContext::state`] 交给处理器 / 中间件（共享借用）。
//!
//! # `router_`: the layered router (P0)
//!
//! Each router owns a local table rooted at itself. Matching tries handler rules, then child
//! routers (recursively), then the root endpoint. The trie is `matchit_route`; the child list
//! is an allocator-parameterised `Vec` built at assembly time. `S` is per-connection state,
//! held by the router as `Arc<S>` and handed to handlers/middleware as a shared `&S`.

use alloc::alloc::Global;
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::alloc::Allocator;

use abs_mptp::spec::access_method::AccessMethod;
use matchit_route::Router as Trie;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::MethodSet;

use super::endpoint_::Endpoint;
use super::handler_::TrEndpointHandler;
use super::middleware_::{Pipeline, TrMiddleware};
use super::route_params_::RouteParams;

/// 一次匹配的结果，泛型于端点值类型 `V`（便于纯逻辑单测）。
///
/// 生产类型见 [`MatchOutcome`]（`V = Endpoint<C, S>`）。
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
pub type MatchOutcome<'f, C, S = ()> = Matched<'f, Endpoint<C, S>>;

/// 一个 router 的匹配契约。
///
/// 对象安全：返回类型是具体类型 [`MatchOutcome`]，因此子 router 可以用
/// [`alloc::sync::Arc<dyn TrRouteMatcher>`](alloc::sync::Arc) 擦除。
pub trait TrRouteMatcher<C, S = ()>: Send + Sync
where
    C: TrConfig,
{
    /// 在「相对于本 router 根」的 `location` 上匹配。
    fn match_route<'f>(&'f self, method: AccessMethod, location: &'f str) -> MatchOutcome<'f, C, S>;
}

/// 默认实现：分层 router。
///
/// `C` 是服务端配置类型，`A` 是子 router 列表的分配器（缺省 [`Global]），`S` 是连接作用域
/// 状态（缺省 `()`）。当前是**装配期**构建（消费式 `bind` / `bind_router`，无运行期增删）；
/// 运行期增删需要 no_std 读多写少锁，属后续 round。
pub struct RadixRouter<C, A = Global, S = ()>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
    S: Send + Sync + 'static,
{
    /// 本层根 endpoint：所有未命中的请求都由它兜底。
    root_: Arc<dyn TrEndpointHandler<C, S>>,

    /// handler 规则：精确路径模式 → 端点。
    trie_: Trie<Endpoint<C, S>>,

    /// 子 router 规则：静态前缀 → 子 router 匹配器。
    #[allow(clippy::type_complexity)]
    routers_: Vec<(&'static str, Arc<dyn TrRouteMatcher<C, S>>), A>,

    /// 请求路径上的中间件链（装配期构建，请求路径只读）。
    pipeline_: Pipeline<C, S>,

    /// 连接作用域状态（每连接一份，跨 session 共享只读）。
    state_: Arc<S>,
}

impl<C> RadixRouter<C, Global, ()>
where
    C: TrConfig + 'static,
{
    /// 用根 handler 构造一个空 router（无连接状态，全局分配器）。
    pub fn new(root: Arc<dyn TrEndpointHandler<C, ()>>) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(Global),
            pipeline_: Pipeline::new(),
            state_: Arc::new(()),
        }
    }
}

impl<C, A> RadixRouter<C, A, ()>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
{
    /// 用根 handler 与显式分配器构造（无连接状态）。
    pub fn new_in(root: Arc<dyn TrEndpointHandler<C, ()>>, alloc: A) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(alloc),
            pipeline_: Pipeline::new(),
            state_: Arc::new(()),
        }
    }
}

impl<C, S> RadixRouter<C, Global, S>
where
    C: TrConfig + 'static,
    S: Send + Sync + 'static,
{
    /// 用根 handler 与连接状态构造（全局分配器）。
    pub fn new_with_state(root: Arc<dyn TrEndpointHandler<C, S>>, state: Arc<S>) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(Global),
            pipeline_: Pipeline::new(),
            state_: state,
        }
    }
}

impl<C, A, S> RadixRouter<C, A, S>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
    S: Send + Sync + 'static,
{
    /// 用根 handler、连接状态与显式分配器构造。
    pub fn new_in_with_state(
        root: Arc<dyn TrEndpointHandler<C, S>>,
        state: Arc<S>,
        alloc: A,
    ) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(alloc),
            pipeline_: Pipeline::new(),
            state_: state,
        }
    }

    /// 本层根 handler。
    pub fn root(&self) -> &Arc<dyn TrEndpointHandler<C, S>> {
        &self.root_
    }

    /// 连接作用域状态（共享借用）。
    pub fn state(&self) -> &S {
        &self.state_
    }

    /// 绑定一条 handler 规则：`pattern` + 方法集合 + 终端处理器。
    ///
    /// 消费式构建；`pattern` 是相对本 router 根的路径模式（支持 `{name}` / `{*rest}`）。
    /// 同路径同方法冲突时返回 [`matchit_route::InsertError`]。
    pub fn bind(
        mut self,
        pattern: &'static str,
        methods: MethodSet,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        let endpoint = Endpoint::new(pattern, methods, Arc::new(handler));
        let normalized = normalize_(endpoint.pattern());
        self.trie_.insert(normalized, endpoint)?;
        Result::Ok(self)
    }

    /// 直接绑定一个已经构造好的端点。
    ///
    /// 与 [`bind`](Self::bind) 不同，这里接收一个已经通过
    /// [`Endpoint::with_metadata`](super::Endpoint::with_metadata) 挂上元数据的
    /// [`Endpoint`]，供「先造端点、追加元数据、再登记进路径树」的装配路径使用（RPC 宏生成
    /// 的端点正是这种形状）。`pattern` / `methods` 取自端点自身。
    pub fn bind_endpoint(
        mut self,
        endpoint: Endpoint<C, S>,
    ) -> Result<Self, matchit_route::InsertError> {
        let normalized = normalize_(endpoint.pattern());
        self.trie_.insert(normalized, endpoint)?;
        Result::Ok(self)
    }

    /// 一个 handler 同时响应多种方法。
    pub fn on(
        self,
        pattern: &'static str,
        methods: MethodSet,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, methods, handler)
    }

    /// `Head` 方法糖。
    pub fn head(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::HEAD, handler)
    }

    /// `View` 方法糖。
    pub fn view(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::VIEW, handler)
    }

    /// `Post` 方法糖。
    pub fn post(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::POST, handler)
    }

    /// `Drop` 方法糖。
    pub fn drop(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::DROP, handler)
    }

    /// `Push` 方法糖。
    pub fn push(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::PUSH, handler)
    }

    /// `Pull` 方法糖。
    pub fn pull(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::PULL, handler)
    }

    /// `Call` 方法糖。
    pub fn call(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::CALL, handler)
    }

    /// 在静态前缀下挂载一个子 router。
    ///
    /// 请求命中该前缀后，剩余路径交给子 router 递归匹配；本 router 不关心子 router 内部
    /// 规则。当前不做「handler 规则与子 router 前缀重叠」的冲突检测，属后续 round。
    pub fn bind_router(
        mut self,
        prefix: &'static str,
        child: Arc<dyn TrRouteMatcher<C, S>>,
    ) -> Self {
        self.routers_.push((prefix, child));
        self
    }

    /// 追加一个中间件（装配顺序即执行顺序）。
    pub fn use_middleware(mut self, middleware: impl TrMiddleware<C, S> + 'static) -> Self {
        self.pipeline_ = self.pipeline_.use_middleware(middleware);
        self
    }

    /// 本 router 的中间件链。
    pub fn pipeline(&self) -> &Pipeline<C, S> {
        &self.pipeline_
    }
}

impl<C, A, S> TrRouteMatcher<C, S> for RadixRouter<C, A, S>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync,
    S: Send + Sync + 'static,
{
    fn match_route<'f>(&'f self, method: AccessMethod, location: &'f str) -> MatchOutcome<'f, C, S> {
        // 1. 本层 handler 规则：路径 + 方法精确命中。
        match match_method_(&self.trie_, method, location, Endpoint::methods) {
            Matched::Root => {}
            other => return other,
        }

        // 2. 子 router：前缀命中后递归。
        for (prefix, child) in &self.routers_ {
            if let Option::Some(remaining) = strip_prefix_(prefix, location) {
                return child.match_route(method, remaining);
            }
        }

        // 3. 本层根 endpoint 兜底。
        Matched::Root
    }
}

/// 纯匹配：路径 + 方法 → 值。
///
/// 与 `C` / `S` 无关，因此可以在单测里用简单值类型（如 `&'static str`）验证；生产路径由
/// [`RadixRouter::match_route`] 以 `V = Endpoint<C, S>` 调用。
fn match_method_<'f, V>(
    trie: &'f Trie<V>,
    method: AccessMethod,
    location: &'f str,
    methods_of: fn(&V) -> MethodSet,
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

/// 剥掉静态前缀，返回剩余路径（在 `/` 段边界上）。
///
/// 与 [`normalize_`] 一致：前后空白斜杠被忽略；前缀必须按完整路径段命中，`/topic` 匹配
/// `/topic/chat` 但不匹配 `/topicx`。
fn strip_prefix_<'a>(prefix: &str, location: &'a str) -> Option<&'a str> {
    let prefix = normalize_(prefix);
    let location = normalize_(location);
    if prefix.is_empty() {
        return Option::Some(location);
    }
    let rest = location.strip_prefix(prefix)?;
    if rest.is_empty() {
        return Option::Some("");
    }
    rest.strip_prefix('/')
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

    /// 测试前缀剥离在完整路径段边界上工作。
    /// Test that prefix stripping works on whole-segment boundaries.
    ///
    /// - 手段：用 `api/v1` 剥离 `api/v1/goods`、`api/v10/goods`、`api/v1`。
    /// - 判断：第一条得到 `goods`，第二条为 `None`，第三条得到空串。
    ///
    /// - Means: strip `api/v1` from a normal path, a look-alike prefix and the exact path.
    /// - Verdict: the first yields `goods`, the second `None`, the third an empty slice.
    #[test]
    fn strip_prefix_on_segment_boundary_() {
        assert_eq!(strip_prefix_("api/v1", "/api/v1/goods"), Option::Some("goods"));
        assert_eq!(strip_prefix_("api/v1", "/api/v10/goods"), Option::None);
        assert_eq!(strip_prefix_("api/v1", "/api/v1"), Option::Some(""));
    }
}
