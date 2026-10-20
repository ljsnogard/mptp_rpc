//! 运行期路径 Trie：按完整的路径段匹配，支持最长前缀。
//!
//! 服务端的 interface 分派与 service 分派都建立在它之上；它不是宏生成的 `match`，而是注册时
//! 逐个 `insert` 出来的运行期结构。
//!
//! # `trie_`: a runtime path trie
//!
//! Both server-side dispatch levels (interface, then service) use this trie. Entries are
//! inserted at runtime; no macro-generated `match` arms participate in routing.

use std::collections::BTreeMap;

/// 按路径段匹配的 Trie 节点。
#[derive(Debug)]
struct TrieNode_<T> {
    /// 子段 → 子节点。
    children_: BTreeMap<String, TrieNode_<T>>,

    /// 在本节点结束的注册值。
    value_: Option<T>,
}

impl<T> TrieNode_<T> {
    /// 造一个空节点。
    fn new_() -> Self {
        TrieNode_ {
            children_: BTreeMap::new(),
            value_: Option::None,
        }
    }
}

impl<T> Default for TrieNode_<T> {
    fn default() -> Self {
        Self::new_()
    }
}

/// 路径 Trie。
#[derive(Debug)]
pub(crate) struct PathTrie<T> {
    /// 根节点。
    root_: TrieNode_<T>,
}

impl<T> PathTrie<T> {
    /// 造一棵空 Trie。
    pub(crate) fn new() -> Self {
        PathTrie {
            root_: TrieNode_::new_(),
        }
    }

    /// 插入一条路径；路径已存在时返回旧值。
    pub(crate) fn insert(&mut self, path: &str, value: T) -> Option<T> {
        let mut node = &mut self.root_;
        for segment in split_segments_(path) {
            node = node
                .children_
                .entry(segment.to_string())
                .or_insert_with(TrieNode_::new_);
        }
        node.value_.replace(value)
    }

    /// 在给定路径段上做最长前缀匹配，返回命中的值与消耗的段数。
    ///
    /// 只按完整路径段回退，因此 `api/v10` 不会误命中 `api/v1`。
    pub(crate) fn match_longest<'a>(&'a self, segments: &[&str]) -> Option<(&'a T, usize)> {
        let mut node = &self.root_;
        let mut best = node.value_.as_ref().map(|value| (value, 0usize));

        for (index, segment) in segments.iter().enumerate() {
            let Option::Some(child) = node.children_.get(*segment) else {
                break;
            };
            node = child;
            if let Option::Some(value) = node.value_.as_ref() {
                best = Option::Some((value, index + 1usize));
            }
        }

        best
    }
}

impl<T> Default for PathTrie<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// 把路径拆成非空段；首尾 `/` 与重复 `/` 都被忽略。
pub(crate) fn split_segments_(path: &str) -> Vec<&str> {
    path.split('/')
        .filter(|segment| !segment.is_empty())
        .collect()
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 造一棵包含 interface / service 两级路径的树。
    fn sample_trie_() -> PathTrie<&'static str> {
        let mut trie = PathTrie::new();
        trie.insert("api/v1/workspace", "v1-workspace");
        trie.insert("api/v1/session", "v1-session");
        trie.insert("api/v1/workspace/deep", "v1-workspace-deep");
        trie.insert("api/v2/workspace", "v2-workspace");
        trie.insert("api/v10/workspace", "v10-workspace");
        trie
    }

    /// 测试最长前缀匹配会命中更深的注册项。
    /// Test that longest-prefix matching selects the deeper entry.
    ///
    /// - 手段：匹配 `api/v1/workspace/deep/extra` 与 `api/v1/workspace`。
    /// - 判断：前者命中 `v1-workspace-deep`，后者命中 `v1-workspace`。
    ///
    /// - Means: match a deeper path and the shorter one.
    /// - Verdict: the deeper path wins when it exists; otherwise the shorter value is used.
    #[test]
    fn matches_longest_prefix_() {
        let trie = sample_trie_();

        let (value, depth) = trie
            .match_longest(&split_segments_("/api/v1/workspace/deep/extra"))
            .expect("应当命中更深的前缀");
        assert_eq!(*value, "v1-workspace-deep");
        assert_eq!(depth, 4usize);

        let (value, depth) = trie
            .match_longest(&split_segments_("/api/v1/workspace"))
            .expect("应当命中 workspace");
        assert_eq!(*value, "v1-workspace");
        assert_eq!(depth, 3usize);
    }

    /// 测试 `api/v10` 不会误命中 `api/v1`。
    /// Test that `api/v10` never matches `api/v1`.
    ///
    /// - 手段：匹配 `api/v10/workspace`。
    /// - 判断：命中 `v10-workspace`；若实现退化成裸字符串前缀，会错误命中 `v1-*`。
    ///
    /// - Means: match `api/v10/workspace`.
    /// - Verdict: it resolves to the v10 entry, not to any v1 entry.
    #[test]
    fn does_not_match_partial_segment_() {
        let trie = sample_trie_();

        let (value, _) = trie
            .match_longest(&split_segments_("/api/v10/workspace"))
            .expect("应当命中 v10");
        assert_eq!(*value, "v10-workspace");
    }

    /// 测试重复斜杠与首尾斜杠不会改变匹配结果。
    /// Test that repeated and surrounding slashes do not change the match.
    ///
    /// - 手段：用 `//api//v1///workspace//` 匹配。
    /// - 判断：仍然命中 `v1-workspace`。
    ///
    /// - Means: match a path with repeated and surrounding slashes.
    /// - Verdict: the same `v1-workspace` entry is selected.
    #[test]
    fn ignores_empty_segments_() {
        let trie = sample_trie_();

        let (value, _) = trie
            .match_longest(&split_segments_("//api//v1///workspace//"))
            .expect("空段应当被忽略");
        assert_eq!(*value, "v1-workspace");
    }
}
