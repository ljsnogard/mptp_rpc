//! 类型化路径参数：把 `&str` 解析成业务类型。
//!
//! [`TrFromRouteParam`] 是「路径参数值 → Rust 类型」的解析契约；[`RouteParams::get_typed`]
//! 用它把某个参数按类型取出来，解析失败由处理器自行决定是否写 `400`。它不要求 derive 宏
//! （类型化参数的编译期校验属后续 round）。
//!
//! # `route_param_`: typed route parameters
//!
//! `TrFromRouteParam` is the parsing contract from a route value to a Rust type;
//! `RouteParams::get_typed` uses it to fetch a typed parameter. It does not require a derive
//! macro (compile-time validation of typed params is a later round).

use core::fmt;

/// 单个路径参数值 → Rust 类型。
///
/// 常见整数与 `bool` 由框架实现；用户类型（如 `FeedId(u64)`）可自行实现。
pub trait TrFromRouteParam: Sized {
    /// 从原始字符串解析；失败返回 [`RouteParamError`]。
    fn from_route_param(value: &str) -> Result<Self, RouteParamError>;
}

/// 路径参数解析失败。
///
/// 只带一个静态原因（不复制参数名或值，避免分配）；处理器通常据此写 `400 BadRequest`。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteParamError {
    /// 失败原因（静态文本）。
    reason_: &'static str,
}

impl RouteParamError {
    /// 由静态原因构造。
    pub const fn new(reason: &'static str) -> Self {
        RouteParamError { reason_: reason }
    }

    /// 失败原因。
    pub const fn reason(&self) -> &'static str {
        self.reason_
    }
}

impl fmt::Display for RouteParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "非法路径参数：{}", self.reason_)
    }
}

impl core::error::Error for RouteParamError {}

/// 为整数类型生成 `TrFromRouteParam` 实现。
macro_rules! impl_from_route_param_int_ {
    ($($t:ty),* $(,)?) => {
        $(
            impl TrFromRouteParam for $t {
                fn from_route_param(value: &str) -> Result<Self, RouteParamError> {
                    value
                        .parse::<$t>()
                        .map_err(|_| RouteParamError::new("不是合法整数"))
                }
            }
        )*
    };
}

impl_from_route_param_int_!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize);

impl TrFromRouteParam for bool {
    fn from_route_param(value: &str) -> Result<Self, RouteParamError> {
        value
            .parse::<bool>()
            .map_err(|_| RouteParamError::new("不是合法布尔值"))
    }
}

#[cfg(test)]
mod tests_ {
    use crate::RouteParams;
    use alloc::boxed::Box;
    use matchit_route::Router;

    /// 造一张带 `{id}` 的路由并匹配一条路径，返回 `'static` 参数表（测试里泄漏路由表以
    /// 获得 `'static` 借用，用例短命可接受）。
    fn params_for_(path: &'static str) -> RouteParams<'static> {
        let router: &'static mut Router<()> = Box::leak(Box::new(Router::new()));
        router.insert("feeds/{id}", ()).expect("登记规则");
        let matched = router.at(path).expect("匹配路径");
        RouteParams::new_(matched.params)
    }

    /// 测试类型化参数能解析出正确的类型与值。
    /// Test that typed params parse into the right type and value.
    ///
    /// - 手段：匹配 `feeds/42`，用 `get_typed::<u64>("id")` 取值。
    /// - 判断：得到 `Some(Ok(42u64))`；缺失的键返回 `None`。
    ///
    /// - Means: match `feeds/42` and read `id` as `u64`.
    /// - Verdict: `Some(Ok(42u64))`; a missing key yields `None`.
    #[test]
    fn typed_params_parse_() {
        let params = params_for_("feeds/42");
        assert_eq!(params.get_typed::<u64>("id"), Option::Some(Result::Ok(42u64)));
        assert_eq!(params.get_typed::<u64>("missing"), Option::None);
    }

    /// 测试非法值解析失败而不是默默给默认值。
    /// Test that an invalid value fails parsing instead of silently defaulting.
    ///
    /// - 手段：匹配 `feeds/abc`，用 `get_typed::<u64>("id")` 取值。
    /// - 判断：得到 `Some(Err(...))`，且错误原因是「不是合法整数」。
    ///
    /// - Means: match `feeds/abc` and read `id` as `u64`.
    /// - Verdict: `Some(Err(...))` with reason "not a valid integer".
    #[test]
    fn typed_params_invalid_() {
        let params = params_for_("feeds/abc");
        let result = params.get_typed::<u64>("id").expect("id 应当存在");
        assert!(result.is_err());
        assert_eq!(result.err().map(|e| e.reason()), Option::Some("不是合法整数"));
    }

    /// 测试 `bool` 参数能正确解析。
    /// Test that a `bool` parameter parses correctly.
    ///
    /// - 手段：匹配 `feeds/true`，用 `get_typed::<bool>("id")` 取值。
    /// - 判断：得到 `Some(Ok(true))`。
    ///
    /// - Means: match `feeds/true` and read `id` as `bool`.
    /// - Verdict: `Some(Ok(true))`.
    #[test]
    fn typed_params_bool_() {
        let params = params_for_("feeds/true");
        assert_eq!(params.get_typed::<bool>("id"), Option::Some(Result::Ok(true)));
    }
}
