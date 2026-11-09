//! `Router`：独占一块 bump arena 的 URL router。

use alloc::{boxed::Box, vec::Vec};
use core::fmt;

use bumpalo::Bump;

use crate::{InsertError, MatchError, Params, error::MergeError, tree::Node};

/// 创建一块堆上稳定的 arena，并返回指向它的 `'static` 引用。
///
/// # Safety
///
/// `Bump` 放在 `Box` 里，堆地址不会因为 `Box` 或 `Router` 移动而改变；返回的引用只由
/// 调用方与 `Box` 一起持有，且 `Router` 的字段声明顺序保证节点先于 arena 析构。
fn make_arena() -> (Box<Bump>, &'static Bump) {
    let arena = Box::new(Bump::new());
    // Safety: 见函数文档；这里只是把“由返回的 Box 保活”这件事写成 `'static`。
    let bump: &'static Bump = unsafe { &*(&*arena as *const Bump) };
    (arena, bump)
}

/// 一个 URL router。
///
/// 路由树、节点前缀与参数名都放在 router 自有的 bump arena 中；`Router` 被丢弃时，
/// 节点里的值先按 RAII 析构，随后 arena 整体释放。
///
/// # Examples
///
/// ```rust
/// use matchit_route::Router;
///
/// let mut router = Router::new();
/// router.insert("/home", "Welcome!").unwrap();
/// router.insert("/users/{id}", "A User").unwrap();
///
/// let matched = router.at("/users/978").unwrap();
/// assert_eq!(matched.params.get("id"), Some("978"));
/// assert_eq!(*matched.value, "A User");
/// ```
pub struct Router<T: 'static> {
    /// 根节点。
    ///
    /// 字段顺序重要：`root` 必须先于 `arena` 析构，否则节点的 `Drop` 会访问已释放的
    /// arena。
    root: Node<'static, T>,

    /// 路由树独占的 arena。
    arena: Box<Bump>,
}

// Safety: `Router` 独占 arena；移动到另一个线程时，`&Bump` 仍指向同一块堆内存，且不存在
// 其它并发访问者。`Bump` 本身是 `Send` 但不是 `Sync`，这里只实现 `Send`。
unsafe impl<T: Send + 'static> Send for Router<T> {}

// Safety: `&Router` 只提供只读匹配（`at`）与参数遍历，不触碰 arena 的分配状态；
// `insert` / `remove` / `merge` 都要求 `&mut self`。因此多线程共享只读视图是安全的，
// 前提是 `T: Sync`。
unsafe impl<T: Sync + 'static> Sync for Router<T> {}

impl<T: 'static> Default for Router<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: 'static> Router<T> {
    /// 构造一个空 router。
    pub fn new() -> Self {
        let (arena, bump) = make_arena();
        Router {
            root: Node::new_in(bump),
            arena,
        }
    }

    /// 插入一条路由。
    ///
    /// # Errors
    ///
    /// 路由语法错误或与已有路由冲突时返回 [`InsertError`]。
    ///
    /// # Examples
    ///
    /// ```rust
    /// use matchit_route::Router;
    ///
    /// let mut router = Router::new();
    /// router.insert("/home", "Welcome!").unwrap();
    /// router.insert("/users/{id}", "A User").unwrap();
    /// ```
    pub fn insert(&mut self, route: impl AsRef<str>, value: T) -> Result<(), InsertError> {
        self.root.insert(route.as_ref(), value)
    }

    /// 匹配一条路径。
    ///
    /// # Errors
    ///
    /// 没有路由命中时返回 [`MatchError::NotFound`]。
    ///
    /// # Examples
    ///
    /// ```rust
    /// use matchit_route::Router;
    ///
    /// let mut router = Router::new();
    /// router.insert("/home", "Welcome!").unwrap();
    ///
    /// assert_eq!(*router.at("/home").unwrap().value, "Welcome!");
    /// ```
    #[inline]
    pub fn at<'path>(&self, path: &'path str) -> Result<Match<'_, 'path, &T>, MatchError> {
        match self.root.at(path.as_bytes()) {
            Ok((value, params)) => Ok(Match {
                // Safety: `value` 只通过 `&self` 暴露为 `&T`；不存在同时生效的 `&mut T`。
                value: unsafe { &*value.get() },
                params,
            }),
            Err(e) => Err(e),
        }
    }

    /// 匹配一条路径并返回可变值引用。
    ///
    /// # Errors
    ///
    /// 没有路由命中时返回 [`MatchError::NotFound`]。
    ///
    /// # Examples
    ///
    /// ```rust
    /// use matchit_route::Router;
    ///
    /// let mut router = Router::new();
    /// router.insert("/", 1).unwrap();
    ///
    /// *router.at_mut("/").unwrap().value += 1;
    /// assert_eq!(*router.at("/").unwrap().value, 2);
    /// ```
    #[inline]
    pub fn at_mut<'path>(
        &mut self,
        path: &'path str,
    ) -> Result<Match<'_, 'path, &mut T>, MatchError> {
        match self.root.at(path.as_bytes()) {
            Ok((value, params)) => Ok(Match {
                // Safety: `&mut self` 保证此刻不存在其它借用的 `&T` / `&mut T`。
                value: unsafe { &mut *value.get() },
                params,
            }),
            Err(e) => Err(e),
        }
    }

    /// 删除一条路由，返回它原来的值。
    ///
    /// # Examples
    ///
    /// ```rust
    /// use matchit_route::Router;
    ///
    /// let mut router = Router::new();
    /// router.insert("/home", "Welcome!").unwrap();
    ///
    /// assert_eq!(router.remove("/home"), Some("Welcome!"));
    /// assert_eq!(router.remove("/home"), None);
    /// ```
    pub fn remove(&mut self, route: impl AsRef<str>) -> Option<T> {
        self.root.remove(route.as_ref())
    }

    /// 把另一个 router 的所有路由合并进来。
    ///
    /// 允许部分成功：冲突的路由会被跳过并汇总到 [`MergeError`]。
    ///
    /// # Errors
    ///
    /// 至少一条路由冲突时返回 [`MergeError`]。
    ///
    /// # Examples
    ///
    /// ```rust
    /// use matchit_route::Router;
    ///
    /// let mut root = Router::new();
    /// root.insert("/home", "Welcome!").unwrap();
    ///
    /// let mut child = Router::new();
    /// child.insert("/users/{id}", "A User").unwrap();
    ///
    /// root.merge(child).unwrap();
    /// assert!(root.at("/users/1").is_ok());
    /// ```
    pub fn merge(&mut self, other: Self) -> Result<(), MergeError> {
        let mut errors = Vec::new();
        other.for_each(|path, value| {
            if let Err(err) = self.insert(path, value) {
                errors.push(err);
            }
        });

        if errors.is_empty() {
            Ok(())
        } else {
            Err(MergeError(errors))
        }
    }

    /// 遍历所有路由，把原始路径与值交给 visitor；会消费 router。
    pub fn for_each<V: FnMut(&str, T)>(self, visitor: V) {
        let Router { root, arena } = self;
        root.for_each(visitor);
        drop(arena);
    }

    /// 测试 helper：检查节点优先级。
    /// Test helper that ensures route priorities are consistent.
    #[cfg(feature = "__test_helpers")]
    pub fn check_priorities(&self) -> Result<u32, (u32, u32)> {
        self.root.check_priorities()
    }
}

impl<T: Clone + 'static> Clone for Router<T> {
    fn clone(&self) -> Self {
        let (arena, bump) = make_arena();
        Router {
            root: self.root.clone_in(bump),
            arena,
        }
    }
}

impl<T: fmt::Debug + 'static> fmt::Debug for Router<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.root, f)
    }
}

/// 一次成功匹配的结果。
#[derive(Debug)]
pub struct Match<'k, 'v, V> {
    /// 命中的值。
    pub value: V,

    /// 路径参数。
    pub params: Params<'k, 'v>,
}
