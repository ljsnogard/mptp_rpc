//! 运行期路径 Trie：按完整的路径段匹配，支持最长前缀。
//!
//! 服务端的分派有两级：
//!
//! 1. interface 前缀（例如 `api/v1`）；
//! 2. 该 interface 内部的 service 前缀（例如 `customer/goods`）。
//!
//! 两级都使用本模块的 [`PathTrie`]。它不是宏生成的 `match`，而是**注册时**逐个
//! [`PathTrie::insert`] 出来的运行期结构；因此同一个 service 的方法仍然由 body enum 的
//! 变体分派，interface / service 这层路径匹配却不依赖方法数量。
//!
//! # 为什么按「段」而不是按字符串前缀
//!
//! 裸字符串前缀会把 `/api/v10` 错误命中到 `/api/v1`。这里按完整的 `/` 分段后逐段下探，
//! 只有段边界上的注册项才可能命中；空段（首尾 `/`、重复 `/`）在拆分时被忽略，保证注册与
//! 匹配使用同一套规范化规则。
//!
//! # `trie_`: a runtime path trie
//!
//! Both dispatch levels (interface and service) use [`PathTrie`]. It matches only complete
//! path segments, so `/api/v10` never hits an entry registered as `/api/v1`; leading,
//! trailing and repeated slashes are ignored consistently on registration and lookup.

use std::collections::BTreeMap;

/// Trie 的一个节点。
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

/// 按路径段匹配的运行期 Trie。
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
    ///
    /// 路径按 [`split_segments_`] 的规则拆段，因此 `"api/v1"`、`"/api/v1/"` 与
    /// `"api//v1"` 落在同一个节点上。空路径会落在根节点。
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

    /// 路径是否已经注册（按同样的分段规则）。
    pub(crate) fn contains(&self, path: &str) -> bool {
        let mut node = &self.root_;
        for segment in split_segments_(path) {
            let Some(child) = node.children_.get(segment) else {
                return false;
            };
            node = child;
        }
        node.value_.is_some()
    }

    /// 在给定路径段上做最长前缀匹配，返回命中的值与消耗的段数。
    ///
    /// 只按完整路径段回退：调用方给出 `["api", "v1", "goods"]` 时，返回最深的有值节点。
    /// 值的借用期与 `self` 相同，因此调用方可以在命中后继续使用 router。
    pub(crate) fn match_longest<'a>(&'a self, segments: &[&str]) -> Option<(&'a T, usize)> {
        let mut node = &self.root_;
        let mut best = node.value_.as_ref().map(|value| (value, 0usize));

        for (index, segment) in segments.iter().enumerate() {
            let Some(child) = node.children_.get(*segment) else {
                break;
            };
            node = child;
            if let Some(value) = node.value_.as_ref() {
                best = Some((value, index + 1usize));
            }
        }

        best
    }
}

/// 把路径拆成非空段；首尾 `/` 与重复 `/` 都被忽略。
///
/// 该函数是注册与匹配共用的**唯一**规范化入口；不要在别处再写一套折叠逻辑，否则线上
/// 路径与注册路径会悄悄错位。
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
    /// - Means: match a deeper path and a shallower one against the sample trie.
    /// - Verdict: the deeper path selects `v1-workspace-deep`, the shallow one
    ///   `v1-workspace`.
    #[test]
    fn matches_longest_prefix_() {
        let trie = sample_trie_();

        let deep = trie.match_longest(&["api", "v1", "workspace", "deep", "extra"]);
        assert_eq!(deep, Some((&"v1-workspace-deep", 4usize)));

        let shallow = trie.match_longest(&["api", "v1", "workspace"]);
        assert_eq!(shallow, Some((&"v1-workspace", 3usize)));
    }

    /// 测试匹配只在路径段边界上回退，不做裸字符串前缀匹配。
    /// Test that fallback happens only at segment boundaries, never on raw strings.
    ///
    /// - 手段：匹配 `api/v10/workspace` 与 `api/v1x/workspace`。
    /// - 判断：前者命中自己的注册项 `v10-workspace`；后者返回 `None`，不会误命中 `api/v1`。
    ///
    /// - Means: match a sibling segment and a segment that merely starts with `v1`.
    /// - Verdict: the sibling resolves to its own entry; the lookalike resolves to `None`.
    #[test]
    fn does_not_match_partial_segment_() {
        let trie = sample_trie_();

        assert_eq!(
            trie.match_longest(&["api", "v10", "workspace"]),
            Some((&"v10-workspace", 3usize))
        );
        assert_eq!(trie.match_longest(&["api", "v1x", "workspace"]), None);
    }

    /// 测试首尾 `/`、重复 `/` 与空路径都被同一套规则规范化。
    /// Test that leading, trailing and repeated slashes share one normalisation rule.
    ///
    /// - 手段：用带多余斜杠的路径插入，再用不同斜杠变体匹配；最后插入空路径。
    /// - 判断：三种变体命中同一个值；空路径落在根节点并消耗 0 段。
    ///
    /// - Means: insert with extra slashes, look up with other slash variants, then insert
    ///   the empty path.
    /// - Verdict: all variants hit the same value; the empty path is the root and consumes
    ///   zero segments.
    #[test]
    fn normalises_slashes_() {
        let mut trie = PathTrie::new();
        assert_eq!(trie.insert("/api//v1/", 7u32), None);

        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&7u32, 2usize)));
        assert!(trie.contains("api/v1"));
        assert!(trie.contains("/api/v1/"));
        assert!(!trie.contains("api/v2"));

        assert_eq!(trie.insert("", 9u32), None);
        assert_eq!(trie.match_longest(&[]), Some((&9u32, 0usize)));
    }

    /// 测试重复插入同一路径会返回旧值。
    /// Test that inserting the same path again returns the replaced value.
    ///
    /// - 手段：对同一路径先后插入 `1` 与 `2`。
    /// - 判断：第二次返回 `Some(1)`，匹配得到 `2`。
    ///
    /// - Means: insert `1` then `2` at the same path.
    /// - Verdict: the second insert returns `Some(1)` and lookup yields `2`.
    #[test]
    fn insert_returns_replaced_value_() {
        let mut trie = PathTrie::new();
        assert_eq!(trie.insert("api/v1", 1u32), None);
        assert_eq!(trie.insert("api/v1", 2u32), Some(1u32));
        assert_eq!(trie.match_longest(&["api", "v1"]), Some((&2u32, 2usize)));
    }
}
