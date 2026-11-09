//! 基数树本体：插入、删除、匹配与遍历。
//!
//! 节点、节点前缀、子节点列表和参数名表全部来自 router 自有的 bump arena；只有
//! 匹配时的回溯栈与超过内联容量的参数表使用一次性全局分配。

use alloc::collections::VecDeque;
use alloc::vec::Vec as StdVec;
use core::cell::UnsafeCell;
use core::cmp::min;
use core::fmt;
use core::mem;
use core::ops::Range;

use bumpalo::Bump;
use bumpalo::boxed::Box;
use bumpalo::collections::Vec;

use crate::escape::{UnescapedRef, UnescapedRoute};
use crate::{InsertError, MatchError, Params};

/// 一条路由的参数名表，保留参数在原始规则中的顺序。
type ParamRemapping<'a> = Vec<'a, Vec<'a, u8>>;

/// 基数树节点。
pub(crate) struct Node<'a, T> {
    /// 本节点负责的路径前缀。
    pub(crate) prefix: UnescapedRoute<'a>,

    /// 优先级：子树中已注册值的数量越多，越先被搜索。
    pub(crate) priority: u32,

    /// 是否已经有一个 wildcard 子节点。
    pub(crate) wild_child: bool,

    /// 静态子节点的首字节，用于快速线性查找。
    pub(crate) indices: Vec<'a, u8>,

    /// 节点类型。
    pub(crate) node_type: NodeType,

    /// 子节点。
    pub(crate) children: Vec<'a, Node<'a, T>>,

    /// 本节点登记的值。
    ///
    /// `UnsafeCell` 让 `at` / `at_mut` 共享同一套遍历逻辑：`at` 只从 `&self`
    /// 暴露 `&T`，`at_mut` 才暴露 `&mut T`。
    value: Option<Box<'a, UnsafeCell<T>>>,

    /// 本节点登记值对应的参数名重映射。
    pub(crate) remapping: ParamRemapping<'a>,

    /// 本节点使用的 arena。
    bump: &'a Bump,
}

/// 节点类型。
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
pub(crate) enum NodeType {
    /// 根节点。
    Root,

    /// 参数节点；`suffix` 表示参数后面是否还有静态后缀。
    Param { suffix: bool },

    /// catch-all 参数节点。
    CatchAll,

    /// 静态前缀节点。
    Static,
}

impl<'a, T: 'a> Node<'a, T> {
    /// 在指定 arena 上造一个空节点。
    pub(crate) fn new_in(bump: &'a Bump) -> Self {
        Node {
            prefix: UnescapedRoute::empty(bump),
            priority: 0,
            wild_child: false,
            indices: Vec::new_in(bump),
            node_type: NodeType::Static,
            children: Vec::new_in(bump),
            value: None,
            remapping: Vec::new_in(bump),
            bump,
        }
    }

    /// 插入一条路由。
    pub(crate) fn insert(&mut self, route: &str, val: T) -> Result<(), InsertError> {
        let bump = self.bump;
        let route = UnescapedRoute::from_str(route, bump);
        let (route, remapping) = normalize_params(route)?;
        let mut remaining = route.as_ref();

        self.priority += 1;

        // 空树：直接作为根节点插入。
        if self.prefix.is_empty() && self.children.is_empty() {
            let last = self.insert_route(remaining, val)?;
            last.remapping = remapping;
            self.node_type = NodeType::Root;
            return Ok(());
        }

        let mut node = self;
        'walk: loop {
            // 求当前节点前缀与剩余路由的最长公共前缀。
            let len = min(remaining.len(), node.prefix.len());
            let common_prefix = (0..len)
                .find(|&i| {
                    remaining[i] != node.prefix[i]
                    // Make sure not confuse the start of a wildcard with an escaped `{`.
                        || remaining.is_escaped(i) != node.prefix.is_escaped(i)
                })
                .unwrap_or(len);

            // 当前节点前缀更长：分叉，把不匹配的后缀移到子节点。
            if node.prefix.len() > common_prefix {
                let suffix = node.prefix.as_ref().slice_off(common_prefix).to_owned();
                let child = Node {
                    prefix: suffix,
                    value: node.value.take(),
                    indices: node.indices.clone(),
                    wild_child: node.wild_child,
                    children: mem::replace(&mut node.children, Vec::new_in(bump)),
                    remapping: mem::replace(&mut node.remapping, Vec::new_in(bump)),
                    priority: node.priority - 1,
                    node_type: NodeType::Static,
                    bump,
                };

                node.children = Vec::from_iter_in([child], bump);
                node.indices = Vec::from_iter_in([node.prefix[common_prefix]], bump);
                node.prefix = node.prefix.as_ref().slice_until(common_prefix).to_owned();
                node.wild_child = false;
                continue;
            }

            if remaining.len() == common_prefix {
                // 该节点不能已经有值。
                if node.value.is_some() {
                    return Err(InsertError::conflict(&route, remaining, node));
                }

                node.value = Some(Box::new_in(UnsafeCell::new(val), bump));
                node.remapping = remapping;
                return Ok(());
            }

            // 还有不匹配的后缀，继续向下搜索。
            remaining = remaining.slice_off(common_prefix);
            let next = remaining[0];

            // 参数节点：先匹配已有的静态后缀，或新建一个后缀节点。
            if matches!(node.node_type, NodeType::Param { .. }) {
                let terminator = remaining
                    .iter()
                    .position(|&b| b == b'/')
                    .map(|b| b + 1)
                    .unwrap_or(remaining.len());

                let suffix = remaining.slice_until(terminator);

                for (i, child) in node.children.iter().enumerate() {
                    if *child.prefix == *suffix {
                        node = &mut node.children[i];
                        node.priority += 1;
                        continue 'walk;
                    }
                }

                // 同一个段里出现多个参数，例如 `/{foo}{bar}`。
                if matches!(find_wildcard(suffix), Ok(Some(_))) {
                    return Err(InsertError::InvalidParamSegment);
                }

                let child = node.add_suffix_child(Node {
                    prefix: suffix.to_owned(),
                    node_type: NodeType::Static,
                    priority: 1,
                    ..Node::new_in(bump)
                });
                node.node_type = NodeType::Param { suffix: true };
                node = &mut node.children[child];

                if terminator == remaining.len() {
                    node.value = Some(Box::new_in(UnsafeCell::new(val), bump));
                    node.remapping = remapping;
                    return Ok(());
                }

                remaining = remaining.slice_off(terminator);

                if remaining[0] != b'{' || remaining.is_escaped(0) {
                    let child = node.add_child(Node {
                        node_type: NodeType::Static,
                        priority: 1,
                        ..Node::new_in(bump)
                    });
                    node.indices.push(remaining[0]);
                    node = &mut node.children[child];
                }

                let last = node.insert_route(remaining, val)?;
                last.remapping = remapping;
                return Ok(());
            }

            // 找一个首字节相同的静态子节点。
            for mut i in 0..node.indices.len() {
                if next == node.indices[i] {
                    // 别把 wildcard 的 `{` / `}` 与转义字符混淆。
                    if matches!(next, b'{' | b'}') && !remaining.is_escaped(0) {
                        continue;
                    }

                    i = node.update_child_priority(i);
                    node = &mut node.children[i];
                    continue 'walk;
                }
            }

            // 没找到静态子节点；不是 wildcard 就新建静态子节点。
            if (next != b'{' || remaining.is_escaped(0)) && node.node_type != NodeType::CatchAll {
                node.indices.push(next);
                let child = node.add_child(Node::new_in(bump));
                let child = node.update_child_priority(child);

                let last = node.children[child].insert_route(remaining, val)?;
                last.remapping = remapping;
                return Ok(());
            }

            // 正在插入 wildcard；已有 wildcard 子节点时必须匹配。
            if node.wild_child {
                // wildcard 永远是最后一个子节点。
                let Some(last) = node.children.len().checked_sub(1) else {
                    return Err(InsertError::conflict(&route, remaining, node));
                };
                node = &mut node.children[last];
                node.priority += 1;

                if let Some(wildcard) = remaining.get(..node.prefix.len())
                    && *wildcard != *node.prefix
                {
                    return Err(InsertError::conflict(&route, remaining, node));
                }

                // catch-all 不能再有子节点。
                if node.node_type == NodeType::CatchAll {
                    return Err(InsertError::conflict(&route, remaining, node));
                }

                continue 'walk;
            }

            // 新建 wildcard 节点并插入剩余路由。
            let last = node.insert_route(remaining, val)?;
            last.remapping = remapping;
            return Ok(());
        }
    }

    /// 在当前节点插入剩余路由；遇到 wildcard 时创建参数子节点。
    fn insert_route(
        &mut self,
        mut prefix: UnescapedRef<'a, '_>,
        val: T,
    ) -> Result<&mut Node<'a, T>, InsertError> {
        let bump = self.bump;
        let mut node = self;

        loop {
            let Some(wildcard) = find_wildcard(prefix)? else {
                node.value = Some(Box::new_in(UnsafeCell::new(val), bump));
                node.prefix = prefix.to_owned();
                return Ok(node);
            };

            // 插入 catch-all。
            if prefix[wildcard.clone()][1] == b'*' {
                if wildcard.end != prefix.len() {
                    return Err(InsertError::InvalidCatchAll);
                }

                if wildcard.start > 0 {
                    node.prefix = prefix.slice_until(wildcard.start).to_owned();
                    prefix = prefix.slice_off(wildcard.start);
                }

                let child = node.add_child(Node {
                    prefix: prefix.to_owned(),
                    node_type: NodeType::CatchAll,
                    value: Some(Box::new_in(UnsafeCell::new(val), bump)),
                    priority: 1,
                    ..Node::new_in(bump)
                });
                node.wild_child = true;
                return Ok(&mut node.children[child]);
            }

            // 普通参数。
            if wildcard.start > 0 {
                node.prefix = prefix.slice_until(wildcard.start).to_owned();
                prefix = prefix.slice_off(wildcard.start);
            }

            let terminator = prefix
                .iter()
                .position(|&b| b == b'/')
                .map(|b| b + 1)
                .unwrap_or(prefix.len());

            let wildcard = prefix.slice_until(wildcard.len());
            let suffix = prefix.slice_until(terminator).slice_off(wildcard.len());
            prefix = prefix.slice_off(terminator);

            if matches!(find_wildcard(suffix), Ok(Some(_))) {
                return Err(InsertError::InvalidParamSegment);
            }

            let has_suffix = !matches!(suffix.unescaped(), b"" | b"/");
            let child = node.add_child(Node {
                priority: 1,
                node_type: NodeType::Param { suffix: has_suffix },
                prefix: wildcard.to_owned(),
                ..Node::new_in(bump)
            });

            node.wild_child = true;
            node = &mut node.children[child];

            if !suffix.is_empty() {
                let child = node.add_suffix_child(Node {
                    priority: 1,
                    node_type: NodeType::Static,
                    prefix: suffix.to_owned(),
                    ..Node::new_in(bump)
                });

                node = &mut node.children[child];
            }

            if prefix.is_empty() {
                node.value = Some(Box::new_in(UnsafeCell::new(val), bump));
                return Ok(node);
            }

            if prefix[0] != b'{' || prefix.is_escaped(0) {
                node.indices.push(prefix[0]);
                let child = node.add_child(Node {
                    priority: 1,
                    ..Node::new_in(bump)
                });
                node = &mut node.children[child];
            }
        }
    }

    /// 加一个子节点，wildcard 永远保持在末尾。
    fn add_child(&mut self, child: Node<'a, T>) -> usize {
        let len = self.children.len();

        if self.wild_child && len > 0 {
            self.children.insert(len - 1, child);
            len - 1
        } else {
            self.children.push(child);
            len
        }
    }

    /// 加一个后缀子节点，按前缀长度升序排列。
    fn add_suffix_child(&mut self, child: Node<'a, T>) -> usize {
        let i = self
            .children
            .partition_point(|node| node.prefix.len() >= child.prefix.len());
        self.children.insert(i, child);
        i
    }

    /// 提高子节点优先级并保持排序，返回新索引。
    fn update_child_priority(&mut self, i: usize) -> usize {
        self.children[i].priority += 1;
        let priority = self.children[i].priority;

        let mut updated = i;
        while updated > 0 && self.children[updated - 1].priority < priority {
            self.children.swap(updated - 1, updated);
            updated -= 1;
        }

        if updated != i {
            self.indices[updated..=i].rotate_right(1);
        }

        updated
    }

    /// 删除一条路由，返回它原来的值。
    pub(crate) fn remove(&mut self, route: &str) -> Option<T> {
        let bump = self.bump;
        let route = UnescapedRoute::from_str(route, bump);
        let (route, remapping) = normalize_params(route).ok()?;
        let mut remaining = route.unescaped();

        if remaining == self.prefix.unescaped() {
            let value = self
                .value
                .take()
                .map(|value| Box::into_inner(value).into_inner());

            if self.children.is_empty() {
                *self = Node::new_in(bump);
            }

            return value;
        }

        let mut node = self;
        'walk: loop {
            if remaining.len() <= node.prefix.len() {
                return None;
            }

            let (prefix, rest) = remaining.split_at(node.prefix.len());

            if prefix != node.prefix.unescaped() {
                return None;
            }

            let next = rest[0];
            remaining = rest;

            if matches!(node.node_type, NodeType::Param { .. }) {
                let terminator = remaining
                    .iter()
                    .position(|&b| b == b'/')
                    .map(|b| b + 1)
                    .unwrap_or(remaining.len());

                let suffix = &remaining[..terminator];

                for (i, child) in node.children.iter().enumerate() {
                    if *child.prefix == *suffix {
                        if terminator == remaining.len() {
                            return node.remove_child(i, &remapping);
                        }

                        remaining = &remaining[terminator - child.prefix.len()..];
                        node = &mut node.children[i];
                        continue 'walk;
                    }
                }
            }

            if let Some(i) = node.indices.iter().position(|&c| c == next) {
                if node.children[i].prefix.unescaped() == remaining {
                    return node.remove_child(i, &remapping);
                }

                node = &mut node.children[i];
                continue 'walk;
            }

            if !node.wild_child {
                return None;
            }

            if node.children.last()?.prefix.unescaped() == remaining {
                let last = node.children.len() - 1;
                return node.remove_child(last, &remapping);
            }

            node = node.children.last_mut()?;
        }
    }

    /// 删除指定子节点；参数名必须完全一致。
    fn remove_child(&mut self, i: usize, remapping: &ParamRemapping<'a>) -> Option<T> {
        if self.children[i].remapping != *remapping {
            return None;
        }

        let value = if self.children[i].children.is_empty() {
            let child = self.children.remove(i);

            match child.node_type {
                NodeType::Static if !matches!(self.node_type, NodeType::Param { .. }) => {
                    self.indices.remove(i);
                }

                _ => self.wild_child = false,
            }

            child.value
        } else {
            self.children[i].value.take()
        };

        value.map(|value| Box::into_inner(value).into_inner())
    }

    /// 广度优先遍历整棵树，把每条路由的原始路径与值交给 visitor。
    pub(crate) fn for_each<V: FnMut(&str, T)>(self, mut visitor: V) {
        let mut queue = VecDeque::new();
        let prefix = self.prefix.clone();
        queue.push_back((prefix, self));

        while let Some((mut prefix, mut node)) = queue.pop_front() {
            denormalize_params(&mut prefix, &node.remapping);

            if let Some(value) = node.value.take()
                && let Ok(path) = core::str::from_utf8(prefix.unescaped())
            {
                visitor(path, Box::into_inner(value).into_inner());
            }

            for child in node.children {
                let mut child_prefix = prefix.clone();
                child_prefix.append(&child.prefix);
                queue.push_back((child_prefix, child));
            }
        }
    }

    /// 返回匹配路径的节点值引用与参数表。
    #[inline]
    pub(crate) fn at<'node, 'path>(
        &'node self,
        mut path: &'path [u8],
    ) -> Result<(&'node UnsafeCell<T>, Params<'node, 'path>), MatchError> {
        let mut node = self;
        let mut backtracking = false;
        let mut params = Params::new();
        let mut skipped: StdVec<Skipped<'_, '_, '_, T>> = StdVec::new();

        'backtrack: loop {
            'walk: loop {
                if path.len() <= node.prefix.len() {
                    if *path == *node.prefix
                        && let Some(ref value) = node.value
                    {
                        params.for_each_key_mut(|(i, param)| param.key = &node.remapping[i]);
                        return Ok((&**value, params));
                    }

                    break 'walk;
                }

                let (prefix, rest) = path.split_at(node.prefix.len());

                if *prefix != *node.prefix {
                    break 'walk;
                }

                let previous = path;
                path = rest;

                if !backtracking {
                    let next = path[0];

                    if let Some(i) = node.indices.iter().position(|&c| c == next) {
                        if node.wild_child {
                            skipped.push(Skipped {
                                node,
                                path: previous,
                                params: params.len(),
                            });
                        }

                        node = &node.children[i];
                        continue 'walk;
                    }
                }

                if !node.wild_child {
                    break 'walk;
                }

                let Some(wildcard) = node.children.last() else {
                    break 'walk;
                };
                node = wildcard;
                match node.node_type {
                    NodeType::Param { suffix: false } => {
                        let terminator = match path.iter().position(|&c| c == b'/') {
                            Some(0) => break 'walk,
                            Some(i) => i,
                            None => {
                                let Some(ref value) = node.value else {
                                    break 'walk;
                                };

                                params.push(b"", path);
                                params
                                    .for_each_key_mut(|(i, param)| param.key = &node.remapping[i]);

                                return Ok((&**value, params));
                            }
                        };

                        let (param, rest) = path.split_at(terminator);

                        let [child] = node.children.as_slice() else {
                            break 'walk;
                        };

                        params.push(b"", param);

                        path = rest;
                        node = child;
                        backtracking = false;
                        continue 'walk;
                    }

                    NodeType::Param { suffix: true } => {
                        let slash = path.iter().position(|&c| c == b'/');
                        let terminator = match slash {
                            Some(0) => break 'walk,
                            Some(i) => i + 1,
                            None => path.len(),
                        };

                        for child in node.children.iter() {
                            if child.prefix.len() >= terminator {
                                continue;
                            }

                            let suffix_start = terminator - child.prefix.len();
                            let (param, suffix) = path[..terminator].split_at(suffix_start);

                            if *suffix == *child.prefix {
                                node = child;
                                path = &path[suffix_start..];
                                backtracking = false;
                                params.push(b"", param);
                                continue 'walk;
                            }
                        }

                        let value = match node.value {
                            Some(ref value) if slash.is_none() => value,
                            _ => break 'walk,
                        };

                        params.push(b"", path);

                        params.for_each_key_mut(|(i, param)| param.key = &node.remapping[i]);

                        return Ok((&**value, params));
                    }

                    NodeType::CatchAll => {
                        let value = match node.value {
                            Some(ref value) => value,
                            None => return Err(MatchError::NotFound),
                        };

                        params.for_each_key_mut(|(i, param)| param.key = &node.remapping[i]);

                        let key = &node.prefix[2..node.prefix.len() - 1];
                        params.push(key, path);

                        return Ok((&**value, params));
                    }

                    _ => break 'walk,
                }
            }

            while let Some(skipped) = skipped.pop() {
                if skipped.path.ends_with(path) {
                    path = skipped.path;
                    node = skipped.node;
                    backtracking = true;
                    params.truncate(skipped.params);
                    continue 'backtrack;
                }
            }

            return Err(MatchError::NotFound);
        }
    }

    /// 测试 helper：检查每个节点的优先级是否等于子树中登记值的数量。
    #[cfg(feature = "__test_helpers")]
    pub(crate) fn check_priorities(&self) -> Result<u32, (u32, u32)> {
        let mut priority: u32 = 0;
        for child in &self.children {
            priority += child.check_priorities()?;
        }

        if self.value.is_some() {
            priority += 1;
        }

        if self.priority != priority {
            return Err((self.priority, priority));
        }

        Ok(priority)
    }

    /// 在另一块 arena 上深拷贝本节点。
    pub(crate) fn clone_in(&self, bump: &'a Bump) -> Self
    where
        T: Clone,
    {
        let value = self.value.as_ref().map(|value| {
            // Safety: `&mut T` 只在 `&mut Node` 下暴露，`&self` 期间不存在别名写。
            let value = unsafe { &*value.get() };
            Box::new_in(UnsafeCell::new(value.clone()), bump)
        });

        Node {
            prefix: self.prefix.clone_in(bump),
            priority: self.priority,
            wild_child: self.wild_child,
            indices: Vec::from_iter_in(self.indices.iter().copied(), bump),
            node_type: self.node_type.clone(),
            children: Vec::from_iter_in(
                self.children.iter().map(|child| child.clone_in(bump)),
                bump,
            ),
            value,
            remapping: Vec::from_iter_in(
                self.remapping
                    .iter()
                    .map(|param| Vec::from_iter_in(param.iter().copied(), bump)),
                bump,
            ),
            bump,
        }
    }
}

/// 匹配过程中跳过的一个 wildcard 分支，用于回溯。
struct Skipped<'node, 'a, 'path, T> {
    /// 被跳过的节点。
    node: &'node Node<'a, T>,

    /// 跳到该节点时的路径位置。
    path: &'path [u8],

    /// 当时已收集的参数数量。
    params: usize,
}

impl<'a, T: 'a + fmt::Debug> fmt::Debug for Node<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Safety: `&mut T` 只在 `&mut Node` 下暴露，`&self` 期间不存在别名写。
        let value = unsafe { self.value.as_ref().map(|value| &*value.get()) };

        f.debug_struct("Node")
            .field("value", &value)
            .field("prefix", &self.prefix)
            .field("node_type", &self.node_type)
            .field("children", &self.children)
            .finish()
    }
}

/// 先把参数名规范化成 `{a}`、`{b}`…，并记录原始名字。
///
/// 这样 `/{a}/foo` 与 `/{b}/bar` 可以共享同一棵子树；匹配时再按节点上的 remapping
/// 还原真实参数名。
fn normalize_params<'a>(
    mut path: UnescapedRoute<'a>,
) -> Result<(UnescapedRoute<'a>, ParamRemapping<'a>), InsertError> {
    let bump = path.bump();
    let mut start = 0;
    let mut original = Vec::new_in(bump);

    let mut next = b'a';

    loop {
        let mut wildcard = match find_wildcard(path.as_ref().slice_off(start))? {
            Some(wildcard) => wildcard,
            None => return Ok((path, original)),
        };

        wildcard.start += start;
        wildcard.end += start;

        if wildcard.len() < 2 {
            return Err(InsertError::InvalidParam);
        }

        if path[wildcard.clone()][1] == b'*' {
            start = wildcard.end;
            continue;
        }

        let removed = path.splice(
            wildcard.clone(),
            Vec::from_iter_in([b'{', next, b'}'], bump),
        );

        let mut removed = Vec::from_iter_in(removed.skip(1), bump);
        removed.pop();
        original.push(removed);

        next += 1;
        if next > b'z' {
            return Err(InsertError::TooManyParams);
        }

        start = wildcard.start + 3;
    }
}

/// 把规范化后的参数名还原成原始名字。
pub(crate) fn denormalize_params<'a>(route: &mut UnescapedRoute<'a>, params: &ParamRemapping<'a>) {
    let mut start = 0;
    let mut i = 0;

    loop {
        let Ok(Some(mut wildcard)) = find_wildcard(route.as_ref().slice_off(start)) else {
            return;
        };

        wildcard.start += start;
        wildcard.end += start;

        let Some(param) = params.get(i) else {
            return;
        };
        let mut next = param.clone();

        next.insert(0, b'{');
        next.push(b'}');
        let next_len = next.len();
        let _ = route.splice(wildcard.clone(), next);

        i += 1;
        start = wildcard.start + next_len;
    }
}

/// 查找一个 wildcard 参数，并检查路径里的非法字符。
fn find_wildcard(path: UnescapedRef<'_, '_>) -> Result<Option<Range<usize>>, InsertError> {
    for (start, &c) in path.iter().enumerate() {
        if c == b'}' && !path.is_escaped(start) {
            return Err(InsertError::InvalidParam);
        }

        if c != b'{' || path.is_escaped(start) {
            continue;
        }

        if path.get(start + 1) == Some(&b'}') {
            return Err(InsertError::InvalidParam);
        }

        for (i, &c) in path.iter().enumerate().skip(start + 2) {
            match c {
                b'}' => {
                    if path.is_escaped(i) {
                        continue;
                    }

                    if path.get(i - 1) == Some(&b'*') {
                        return Err(InsertError::InvalidParam);
                    }

                    return Ok(Some(start..i + 1));
                }
                b'*' | b'/' => return Err(InsertError::InvalidParam),
                _ => {}
            }
        }

        return Err(InsertError::InvalidParam);
    }

    Ok(None)
}
