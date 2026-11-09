//! 注册 / 匹配 / 合并错误。

use alloc::{alloc::Global, vec::Vec};
use core::{fmt, ops::Deref};

use abs_str::str_alloc::String;

use crate::{
    escape::{UnescapedRef, UnescapedRoute},
    tree::{Node, denormalize_params},
};

/// 把字节路径复制成全局分配器上的字符串。
///
/// 正常路径一定来自 `&str`，所以这里只需在极端情况下退化为空串。
fn to_global_string(bytes: &[u8]) -> String<Global> {
    match core::str::from_utf8(bytes) {
        Ok(s) => String::from_str_in(s, Global),
        Err(_) => String::from_str_in("", Global),
    }
}

/// 插入路由时可能出现的错误。
#[non_exhaustive]
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum InsertError {
    /// 新路由与已注册路由冲突。
    Conflict {
        /// 已注册的冲突路由。
        with: String<Global>,
    },

    /// 同一个路径段里出现了多个参数。
    InvalidParamSegment,

    /// 参数名非法或花括号不匹配；字面量 `{` / `}` 请写成 `{{` / `}}`。
    InvalidParam,

    /// catch-all 参数只能出现在路径末尾。
    InvalidCatchAll,

    /// 一条路由里的参数超过 26 个，无法用单字母规范化。
    TooManyParams,
}

impl fmt::Display for InsertError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Conflict { with } => {
                write!(
                    f,
                    "Insertion failed due to conflict with previously registered route: {with}"
                )
            }
            Self::InvalidParamSegment => {
                write!(f, "Only one parameter is allowed per path segment")
            }
            Self::InvalidParam => write!(f, "Parameters must be registered with a valid name"),
            Self::InvalidCatchAll => write!(
                f,
                "Catch-all parameters are only allowed at the end of a route"
            ),
            Self::TooManyParams => write!(f, "Too many route parameters (maximum is 26)"),
        }
    }
}

impl core::error::Error for InsertError {}

impl InsertError {
    /// 构造一条“与现有路由冲突”的错误，并尽量还原出完整的冲突路径。
    pub(crate) fn conflict<'a, T>(
        route: &UnescapedRoute<'a>,
        prefix: UnescapedRef<'a, '_>,
        current: &Node<'a, T>,
    ) -> Self {
        let mut route = route.clone();

        // 冲突正好发生在当前节点。
        if prefix.unescaped() == current.prefix.unescaped() {
            denormalize_params(&mut route, &current.remapping);
            return InsertError::Conflict {
                with: to_global_string(route.unescaped()),
            };
        }

        // 去掉不匹配的后缀，再拼上冲突节点的前缀。
        route.truncate(route.len() - prefix.len());

        if !route.ends_with(&current.prefix) {
            route.append(&current.prefix);
        }

        // 拼上第一个冲突子节点的前缀。
        let mut child = current.children.first();
        while let Some(node) = child {
            route.append(&node.prefix);
            child = node.children.first();
        }

        // 还原参数名。
        let mut last = current;
        while let Some(node) = last.children.first() {
            last = node;
        }
        denormalize_params(&mut route, &last.remapping);

        InsertError::Conflict {
            with: to_global_string(route.unescaped()),
        }
    }
}

/// 一次合并失败；可能只合并成功了一部分路由。
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeError(pub(crate) Vec<InsertError>);

impl MergeError {
    /// 取出全部插入错误。
    pub fn into_errors(self) -> Vec<InsertError> {
        self.0
    }
}

impl fmt::Display for MergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for error in self.0.iter() {
            writeln!(f, "{error}")?;
        }

        Ok(())
    }
}

impl core::error::Error for MergeError {}

impl Deref for MergeError {
    type Target = Vec<InsertError>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// 匹配失败。
///
/// ```rust
/// use matchit_route::{MatchError, Router};
///
/// let mut router = Router::new();
/// router.insert("/home", "Welcome!").unwrap();
///
/// assert!(matches!(router.at("/blog"), Err(MatchError::NotFound)));
/// ```
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum MatchError {
    /// 没有任何路由匹配该路径。
    NotFound,
}

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Matching route not found")
    }
}

impl core::error::Error for MatchError {}
