//! 分层路由器的默认实现（round 1 / P0）。
//!
//! 每个 router 以自己为根：`根 endpoint + handler 规则（精确路径模式 + 同路径端点集合）
//! + 子 router 规则（静态前缀）`。匹配顺序是「本层 handler 规则 → 子 router（递归）→ 本层根
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
use core::any::Any;

use abs_mptp::spec::access_method::AccessMethod;
use matchit_route::Router as Trie;
use mptp_smux::serving::config::TrConfig;
use mptp_smux::{MethodSet, ServeError};

use super::endpoint_::{Endpoint, EndpointSet};
use super::handler_::TrEndpointHandler;
use super::metadata_::EndpointMetadata;
use super::middleware_::{Pipeline, TrMiddleware};
use super::route_params_::RouteParams;
use super::{CancellationSource, RequestCancel};

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

/// 错误观测回调：处理器 / 中间件出错时上报，错误文本不上线。
///
/// 用 `Arc<dyn Fn>` 而不是泛型，是为了让 hook 与 router 的 `C` / `S` / `R` 无关，也不给
/// 装配类型再添一个参数。
pub(crate) type ErrorHook = Arc<dyn Fn(&ServeError) + Send + Sync>;

/// 服务端路由的匹配结果。
///
/// `A` 与 [`RadixRouter`] 的分配器一致：它同时用于端点集合与端点元数据集合。
pub type MatchOutcome<'f, C, S = (), R = (), A = Global> = Matched<'f, Endpoint<C, S, R, A>>;

/// 一个 router 的匹配契约。
///
/// 对象安全：返回类型是具体类型 [`MatchOutcome`]，因此子 router 可以用
/// [`alloc::sync::Arc<dyn TrRouteMatcher>`](alloc::sync::Arc) 擦除。
pub trait TrRouteMatcher<C, S = (), R = (), A = Global>: Send + Sync
where
    C: TrConfig,
    A: Allocator,
{
    /// 在「相对于本 router 根」的 `location` 上匹配。
    fn match_route<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
    ) -> MatchOutcome<'f, C, S, R, A>;
}

/// 默认实现：分层 router。
///
/// `C` 是服务端配置类型，`A` 是子 router 列表与同路径端点集合的分配器（缺省 [`Global`]），
/// `S` 是连接作用域状态（缺省 `()`），`R` 是请求级运行期状态（缺省 `()`）。当前是**装配期**构建（消费式 `bind` / `bind_router`，无运行期增删）；
/// 运行期增删需要 no_std 读多写少锁，属后续 round。
pub struct RadixRouter<C, A = Global, S = (), R = ()>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync + 'static,
    S: Send + Sync + 'static,
    R: 'static,
{
    /// 本层根 endpoint：所有未命中的请求都由它兜底。
    root_: Arc<dyn TrEndpointHandler<C, S, R>>,

    /// handler 规则：精确路径模式 → 同路径端点集合（一个路径可挂多个方法）。
    trie_: Trie<EndpointSet<C, S, R, A>>,

    /// 子 router 规则：静态前缀 → 子 router 匹配器（与父层共用同一个分配器类型）。
    #[allow(clippy::type_complexity)]
    routers_: Vec<(&'static str, Arc<dyn TrRouteMatcher<C, S, R, A>>), A>,

    /// 组默认元数据：绑定时继承给本 router 的每个端点（端点自己的标记优先）。
    ///
    /// 惰性创建：只有调用 [`RadixRouter::with_group_metadata`] 时才需要分配器。
    group_metadata_: Option<EndpointMetadata<A>>,

    /// 请求路径上的中间件链（装配期构建、请求路径只读）。
    pipeline_: Pipeline<C, S, R, A, A>,

    /// 连接作用域状态（每连接一份，跨 session 共享只读）。
    state_: Arc<S>,

    /// 连接级取消源：`MptpContext` 的请求级取消句柄从它派生。
    cancel_source_: Arc<CancellationSource>,

    /// 错误观测回调（装配期设置，请求路径只读；缺省无）。
    error_hook_: Option<ErrorHook>,
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
            cancel_source_: Arc::new(CancellationSource::new()),
            error_hook_: Option::None,
            group_metadata_: Option::None,
        }
    }
}

impl<C, A> RadixRouter<C, A, ()>
where
    C: TrConfig + 'static,
    // `Clone`：同一个分配器要分别交给子 router 列表与中间件链。
    A: Allocator + Send + Sync + Clone + 'static,
{
    /// 用根 handler 与显式分配器构造（无连接状态）。
    pub fn new_in(root: Arc<dyn TrEndpointHandler<C, ()>>, alloc: A) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(alloc.clone()),
            pipeline_: Pipeline::new_in(alloc),
            state_: Arc::new(()),
            cancel_source_: Arc::new(CancellationSource::new()),
            error_hook_: Option::None,
            group_metadata_: Option::None,
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
            cancel_source_: Arc::new(CancellationSource::new()),
            error_hook_: Option::None,
            group_metadata_: Option::None,
        }
    }
}

impl<C, R> RadixRouter<C, Global, (), R>
where
    C: TrConfig + 'static,
    R: Default + 'static,
{
    /// 用根 handler 构造一个带请求级状态类型的 router（无连接状态、全局分配器）。
    ///
    /// 与 [`new`](Self::new) 的区别只有 `R`：本构造函数把请求级状态类型交给类型参数决定，
    /// 每请求由框架 [`Default::default`] 新建一份。
    pub fn new_with_request_state(root: Arc<dyn TrEndpointHandler<C, (), R>>) -> Self {
        RadixRouter::new_in_with_state_and_request(root, Arc::new(()), Global)
    }
}

impl<C, A, S, R> RadixRouter<C, A, S, R>
where
    C: TrConfig + 'static,
    // `Clone`：同一个分配器要分别交给子 router 列表与中间件链。
    A: Allocator + Send + Sync + Clone + 'static,
    S: Send + Sync + 'static,
    R: Default + 'static,
{
    /// 用根 handler、连接状态、请求级状态类型与显式分配器构造。
    ///
    /// 请求级状态 `R` 每请求 `Default::default()` 新建；需要按连接状态初始化时，在中间件里
    /// 写入即可。
    pub fn new_in_with_state_and_request(
        root: Arc<dyn TrEndpointHandler<C, S, R>>,
        state: Arc<S>,
        alloc: A,
    ) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(alloc.clone()),
            pipeline_: Pipeline::new_in(alloc),
            state_: state,
            cancel_source_: Arc::new(CancellationSource::new()),
            error_hook_: Option::None,
            group_metadata_: Option::None,
        }
    }
}

impl<C, A, S> RadixRouter<C, A, S>
where
    C: TrConfig + 'static,
    // `Clone`：同一个分配器要分别交给子 router 列表与中间件链。
    A: Allocator + Send + Sync + Clone + 'static,
    S: Send + Sync + 'static,
{
    /// 用根 handler、连接状态与显式分配器构造（请求级状态为 `()`）。
    pub fn new_in_with_state(
        root: Arc<dyn TrEndpointHandler<C, S>>,
        state: Arc<S>,
        alloc: A,
    ) -> Self {
        RadixRouter {
            root_: root,
            trie_: Trie::new(),
            routers_: Vec::new_in(alloc.clone()),
            pipeline_: Pipeline::new_in(alloc),
            state_: state,
            cancel_source_: Arc::new(CancellationSource::new()),
            error_hook_: Option::None,
            group_metadata_: Option::None,
        }
    }
}

impl<C, A, S, R> RadixRouter<C, A, S, R>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync + 'static,
    S: Send + Sync + 'static,
    R: 'static,
{
    /// 本层根 handler。
    pub fn root(&self) -> &Arc<dyn TrEndpointHandler<C, S, R>> {
        &self.root_
    }

    /// 连接作用域状态（共享借用）。
    pub fn state(&self) -> &S {
        &self.state_
    }

    /// 连接级取消源（只读）。
    pub fn cancel_source(&self) -> &CancellationSource {
        self.cancel_source_.as_ref()
    }

    /// 派生一个请求级取消句柄（借用本 router 的取消源）。
    pub fn cancel_token(&self) -> RequestCancel<'_> {
        self.cancel_source_.child_token()
    }

    /// 在静态前缀下挂载一个子 router。
    ///
    /// 请求命中该前缀后，剩余路径交给子 router 递归匹配；本 router 不关心子 router 内部
    /// 规则。当前不做「handler 规则与子 router 前缀重叠」的冲突检测，属后续 round。
    pub fn bind_router(
        mut self,
        prefix: &'static str,
        child: Arc<dyn TrRouteMatcher<C, S, R, A>>,
    ) -> Self {
        self.routers_.push((prefix, child));
        self
    }

    /// 设置错误观测回调（缺省无）。
    ///
    /// 处理器 / 中间件返回错误时，框架先调用本回调上报，再把「尚未写出回复」的请求兜底成
    /// 500；错误文本**不会**写到线上。回调应自行决定记录 / 计数 / 采样，并且不要 panic
    /// （panic 会打断收尾）。重复调用以最后一次为准。
    pub fn with_error_hook(mut self, hook: impl Fn(&ServeError) + Send + Sync + 'static) -> Self {
        self.error_hook_ = Option::Some(Arc::new(hook));
        self
    }

    /// 错误观测回调（crate 内使用）。
    pub(crate) fn error_hook_(&self) -> Option<&ErrorHook> {
        self.error_hook_.as_ref()
    }

    /// 追加一个中间件（装配顺序即执行顺序）。
    pub fn use_middleware(mut self, middleware: impl TrMiddleware<C, S, R, A> + 'static) -> Self {
        self.pipeline_ = self.pipeline_.use_middleware(middleware);
        self
    }

    /// 本 router 的中间件链。
    pub fn pipeline(&self) -> &Pipeline<C, S, R, A, A> {
        &self.pipeline_
    }
}

impl<C, A, S, R> RadixRouter<C, A, S, R>
where
    C: TrConfig + 'static,
    // `Clone`：同路径追加端点 / 继承组元数据时，要把分配器克隆一份给新集合与端点。
    A: Allocator + Send + Sync + Clone + 'static,
    S: Send + Sync + 'static,
    R: 'static,
{
    /// 设置一条组默认元数据：之后绑定到本 router 的每个端点都会继承它。
    ///
    /// 继承是「只补缺失类型」：端点自己用 [`Endpoint::with_metadata`] 声明的同类型标记
    /// 优先。组元数据在绑定时快照进端点，因此**先设置组元数据、再绑定端点**。
    ///
    /// 子 router 的继承是显式的：用 [`group_metadata`](Self::group_metadata) 取出父层
    /// 组元数据，在构造子 router 时调用本方法（`dyn TrRouteMatcher` 已经把子 router 类型
    /// 擦除，父层无法在 `bind_router` 时改写它）。
    pub fn with_group_metadata<T>(mut self, value: T) -> Self
    where
        T: Any + Send + Sync + 'static,
    {
        match &mut self.group_metadata_ {
            Option::Some(group) => group.insert(value),
            Option::None => {
                let mut group = EndpointMetadata::new_in(self.routers_.allocator().clone());
                group.insert(value);
                self.group_metadata_ = Option::Some(group);
            }
        }
        self
    }

    /// 本 router 的组默认元数据（供装配者传给子 router）。
    pub fn group_metadata(&self) -> Option<&EndpointMetadata<A>> {
        self.group_metadata_.as_ref()
    }

    /// 绑定一条 handler 规则：`pattern` + 方法集合 + 终端处理器。
    ///
    /// 消费式构建；`pattern` 是相对本 router 根的路径模式（支持 `{name}` / `{*rest}`）。
    /// 同一 `pattern` 可以绑定多次：每次追加一个端点，匹配时按插入顺序取第一个接受该方法的
    /// 端点（设计稿 §6.7 的 `feeds/{id}` 同时挂 `VIEW` + `PULL`）。
    /// **同路径同方法不做冲突检测**，先插入的端点先命中；装配者应让同路径的方法集合互不相交。
    pub fn bind(
        self,
        pattern: &'static str,
        methods: MethodSet,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        let alloc = self.routers_.allocator().clone();
        let endpoint = Endpoint::new_in(pattern, methods, Arc::new(handler), alloc);
        self.bind_endpoint(endpoint)
    }

    /// 直接绑定一个已经构造好的端点。
    ///
    /// 与 [`bind`](Self::bind) 不同，这里接收一个已经通过
    /// [`Endpoint::with_metadata`](super::Endpoint::with_metadata) 挂上元数据的
    /// [`Endpoint`]，供「先造端点、追加元数据、再登记进路径树」的装配路径使用（RPC 宏生成
    /// 的端点正是这种形状）。`pattern` / `methods` 取自端点自身。
    ///
    /// 同 `pattern` 的多次绑定会合并到同一个端点集合（见 [`bind`](Self::bind) 的方法选择
    /// 约定），因此一条路径可以同时挂多个不同方法集合的端点。
    pub fn bind_endpoint(
        mut self,
        mut endpoint: Endpoint<C, S, R, A>,
    ) -> Result<Self, matchit_route::InsertError> {
        // 组默认元数据只补端点没有的类型（端点自己的声明优先）。
        if let Option::Some(group) = &self.group_metadata_ {
            endpoint.inherit_metadata(group);
        }
        let normalized = normalize_(endpoint.pattern());
        // 同路径已有端点时取出集合追加；没有则用本 router 的分配器新建集合。
        // 先 `remove` 再 `insert` 是 `matchit_route` 目前唯一的「按路由模式取回值」路径，
        // 移除后马上插回同一路由，因此不会破坏其它路由。
        let mut set = match self.trie_.remove(normalized) {
            Option::Some(set) => set,
            Option::None => EndpointSet::new_in(self.routers_.allocator().clone()),
        };
        set.push(endpoint);
        self.trie_.insert(normalized, set)?;
        Result::Ok(self)
    }

    /// 一个 handler 同时响应多种方法。
    pub fn on(
        self,
        pattern: &'static str,
        methods: MethodSet,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, methods, handler)
    }

    /// `Head` 方法糖。
    pub fn head(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::HEAD, handler)
    }

    /// `View` 方法糖。
    pub fn view(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::VIEW, handler)
    }

    /// `Post` 方法糖。
    pub fn post(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::POST, handler)
    }

    /// `Drop` 方法糖。
    pub fn drop(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::DROP, handler)
    }

    /// `Push` 方法糖。
    pub fn push(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::PUSH, handler)
    }

    /// `Pull` 方法糖。
    pub fn pull(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::PULL, handler)
    }

    /// `Call` 方法糖。
    pub fn call(
        self,
        pattern: &'static str,
        handler: impl TrEndpointHandler<C, S, R> + 'static,
    ) -> Result<Self, matchit_route::InsertError> {
        self.bind(pattern, MethodSet::CALL, handler)
    }
}

impl<C, A, S, R> TrRouteMatcher<C, S, R, A> for RadixRouter<C, A, S, R>
where
    C: TrConfig + 'static,
    A: Allocator + Send + Sync + 'static,
    S: Send + Sync + 'static,
    R: 'static,
{
    fn match_route<'f>(
        &'f self,
        method: AccessMethod,
        location: &'f str,
    ) -> MatchOutcome<'f, C, S, R, A> {
        // 1. 本层 handler 规则：路径 + 方法精确命中（同路径多端点按方法选一个）。
        match match_method_(
            &self.trie_,
            location,
            |set| set.select(method),
            |set| set.allowed(),
        ) {
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
fn match_method_<'f, V, E>(
    trie: &'f Trie<V>,
    location: &'f str,
    select: impl Fn(&'f V) -> Option<&'f E>,
    allowed: impl Fn(&'f V) -> MethodSet,
) -> Matched<'f, E> {
    match trie.at(normalize_(location)) {
        Result::Ok(matched) => {
            let matchit_route::Match { value, params } = matched;
            match select(value) {
                Option::Some(endpoint) => Matched::Found {
                    value: endpoint,
                    params: RouteParams::new_(params),
                },
                Option::None => Matched::MethodNotAllowed {
                    allowed: allowed(value),
                },
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
    /// - 手段：登记 `feeds/{id}`，选择闭包按 `View` 命中并用 `feeds/42` 匹配。
    /// - 判断：结果是 `Found`，值为 `"feed"`，参数 `id == "42"`。
    ///
    /// - Means: register `feeds/{id}`, let the selector accept `View`, match `feeds/42`.
    /// - Verdict: the result is `Found`, the value is `"feed"` and `id == "42"`.
    #[test]
    fn found_with_params_() {
        let mut trie = Trie::new();
        trie.insert("feeds/{id}", "feed").expect("登记规则");

        let matched = match_method_(
            &trie,
            "feeds/42",
            |_: &&'static str| Option::Some(&"feed"),
            |_: &&'static str| MethodSet::VIEW,
        );
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
    /// - 手段：登记 `echo`（允许集合 `View`），选择闭包返回 `None`。
    /// - 判断：结果是 `MethodNotAllowed { allowed: VIEW }`。
    ///
    /// - Means: register `echo` with an allowed set of `View` and let the selector reject.
    /// - Verdict: the result is `MethodNotAllowed { allowed: VIEW }`.
    #[test]
    fn method_not_allowed_() {
        let mut trie = Trie::new();
        trie.insert("echo", "echo").expect("登记规则");

        let matched = match_method_(
            &trie,
            "echo",
            |_: &&'static str| Option::None::<&&'static str>,
            |_: &&'static str| MethodSet::VIEW,
        );
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
        let matched = match_method_(
            &trie,
            "nowhere",
            |_: &&'static str| Option::None::<&&'static str>,
            |_: &&'static str| MethodSet::ALL,
        );
        assert!(matches!(matched, Matched::Root));
    }

    /// 测试用端点值：只关心名字与方法集合，用于纯匹配逻辑测试。
    #[derive(Clone, Copy, Debug)]
    struct TestValue {
        /// 端点名字。
        name_: &'static str,

        /// 本端点接受的方法集合。
        methods_: MethodSet,
    }

    /// 测试同一路径上的多个端点按方法各选其一（设计稿 §6.7 的 VIEW + PULL）。
    /// Test that multiple endpoints on one path are selected by method.
    ///
    /// - 手段：同一条 `feeds/{id}` 上挂 `view`（VIEW）与 `pull`（PULL）两个值，分别用
    ///   `View` / `Pull` 调 `match_method_`，选择闭包按方法集合命中。
    /// - 判断：两次都返回 `Found`，选中值的名字分别是 `view` / `pull`——同路径不再被
    ///   「一个端点」限制。
    ///
    /// - Means: put `view` (VIEW) and `pull` (PULL) on one `feeds/{id}` and match with
    ///   `View` / `Pull`, selecting by the endpoint method set.
    /// - Verdict: both return `Found` with the corresponding value name.
    #[test]
    fn same_path_selects_by_method_() {
        let mut trie = Trie::new();
        trie.insert(
            "feeds/{id}",
            alloc::vec![
                TestValue {
                    name_: "view",
                    methods_: MethodSet::VIEW,
                },
                TestValue {
                    name_: "pull",
                    methods_: MethodSet::PULL,
                },
            ],
        )
        .expect("登记规则");

        let view = match_method_(
            &trie,
            "feeds/42",
            |set| {
                set.iter()
                    .find(|value| value.methods_.contains(AccessMethod::View))
            },
            |set| {
                set.iter()
                    .fold(MethodSet::NONE, |acc, value| acc.union(value.methods_))
            },
        );
        match view {
            Matched::Found { value, params } => {
                assert_eq!(value.name_, "view");
                assert_eq!(params.get("id"), Option::Some("42"));
            }
            _ => panic!("View 应当命中 view 端点"),
        }

        let pull = match_method_(
            &trie,
            "feeds/42",
            |set| {
                set.iter()
                    .find(|value| value.methods_.contains(AccessMethod::Pull))
            },
            |set| {
                set.iter()
                    .fold(MethodSet::NONE, |acc, value| acc.union(value.methods_))
            },
        );
        match pull {
            Matched::Found { value, .. } => assert_eq!(value.name_, "pull"),
            _ => panic!("Pull 应当命中 pull 端点"),
        }
    }

    /// 测试同路径 405 的 `Allow` 是集合内所有端点方法的并集。
    /// Test that the 405 `Allow` set is the union of every endpoint on the path.
    ///
    /// - 手段：同一条 `feeds/{id}` 上挂 VIEW 与 PULL，用 `Call` 匹配，选择闭包全部拒绝。
    /// - 判断：`MethodNotAllowed { allowed }` 等于 `VIEW | PULL`——不是第一个端点的方法。
    ///
    /// - Means: put VIEW and PULL on one path, match with `Call`, and reject in selection.
    /// - Verdict: `allowed` equals `VIEW | PULL`, not just the first endpoint's set.
    #[test]
    fn allowed_is_union_on_405_() {
        let mut trie = Trie::new();
        trie.insert(
            "feeds/{id}",
            alloc::vec![
                TestValue {
                    name_: "view",
                    methods_: MethodSet::VIEW,
                },
                TestValue {
                    name_: "pull",
                    methods_: MethodSet::PULL,
                },
            ],
        )
        .expect("登记规则");

        let matched = match_method_(
            &trie,
            "feeds/42",
            |_: &Vec<TestValue>| Option::None::<&TestValue>,
            |set| {
                set.iter()
                    .fold(MethodSet::NONE, |acc, value| acc.union(value.methods_))
            },
        );
        match matched {
            Matched::MethodNotAllowed { allowed } => {
                assert_eq!(allowed, MethodSet::VIEW.union(MethodSet::PULL));
            }
            _ => panic!("应当 MethodNotAllowed，实际命中了其它分支"),
        }
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
        assert_eq!(
            strip_prefix_("api/v1", "/api/v1/goods"),
            Option::Some("goods")
        );
        assert_eq!(strip_prefix_("api/v1", "/api/v10/goods"), Option::None);
        assert_eq!(strip_prefix_("api/v1", "/api/v1"), Option::Some(""));
    }
}
