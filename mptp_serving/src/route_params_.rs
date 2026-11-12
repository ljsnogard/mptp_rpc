//! 路由参数：零拷贝借用 [`matchit_route`] 的 `Params`。
//!
//! 参数名借路由表、参数值借被匹配的 `location`；本类型不复制、不分配，只在一次匹配结果
//! 的生命周期内有效。
//!
//! # `route_params_`: zero-copy route parameters
//!
//! Parameter names borrow the route table and values borrow the matched location; nothing
//! is copied or allocated.

use matchit_route::Params;

/// 一次成功匹配得到的路径参数。
///
/// 内部直接持有 `matchit_route::Params` 的零拷贝视图：`get` / `iter` 返回的 `&str` 都
/// 指向路由表或 `location`，不涉及堆分配。
pub struct RouteParams<'f> {
    /// 零拷贝参数表。
    inner_: Params<'f, 'f>,
}

impl<'f> RouteParams<'f> {
    /// 由 `matchit_route` 的匹配结果构造。
    pub(crate) fn new_(inner: Params<'f, 'f>) -> Self {
        RouteParams { inner_: inner }
    }

    /// 按参数名取值；缺失返回 `None`。
    pub fn get(&self, key: &str) -> Option<&'f str> {
        self.inner_.get(key)
    }

    /// 遍历所有 `(参数名, 参数值)`。
    pub fn iter(&self) -> impl Iterator<Item = (&'f str, &'f str)> + '_ {
        self.inner_.iter()
    }

    /// 参数个数。
    pub fn len(&self) -> usize {
        self.inner_.len()
    }

    /// 是否没有参数。
    pub fn is_empty(&self) -> bool {
        self.inner_.is_empty()
    }
}
