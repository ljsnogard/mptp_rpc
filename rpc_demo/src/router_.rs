//! 路径前缀路由：把请求的 `location` 映射到一个 handler。
//!
//! 这是原先 `mptp_smux::serving::Router` 的实现，现已下放到 demo：`mptp_smux` 只保留
//! [`TrPathRouter`] 契约，具体匹配策略由使用者提供。本实现按**最长前缀**匹配。
//!
//! `rpc_demo` 只用它做顶层分派：空前缀命中唯一的目标 [`RpcRouter`](crate::server_::RpcRouter)，
//! interface → service 两级匹配仍在 `RpcRouter` 内部完成。
//!
//! # `router_`: the demo's longest-prefix path router
//!
//! This is the former built-in `mptp_smux::serving::Router`, moved into the demo. Here it only
//! performs top-level dispatch to the single [`RpcRouter`](crate::server_::RpcRouter); the
//! interface → service matching stays inside that router.

use std::collections::BTreeMap;

use mptp_smux::serving::TrPathRouter;

/// 按**最长前缀**匹配的路由表。
pub struct Router<T> {
    /// 前缀 → 目标。`BTreeMap` 让「最长匹配」可以按键的长度逐个回退，而不是全表扫描。
    prefix_map_: BTreeMap<String, T>,
}

impl<T> Router<T> {
    /// 造一张空路由表。
    pub const fn new() -> Self {
        Router {
            prefix_map_: BTreeMap::new(),
        }
    }

    /// 登记一个前缀；键已存在时返回被替换掉的目标。
    pub fn add_target(&mut self, prefix: &str, target: T) -> Option<T> {
        self.prefix_map_.insert(prefix.to_string(), target)
    }

    /// 按最长前缀匹配；没有任何前缀命中时返回 `None`。
    ///
    /// 从完整路径开始逐字符回退，因此**不会**把 `/topicx` 错误地匹配到登记为 `/topic` 的目标
    /// 上——回退只发生在分隔处（见 [`Router::try_match`] 的实现注释）。
    pub fn try_match(&self, location: &str) -> Option<&T> {
        // 先试完整路径，再逐级回退到上一级目录。回退时跳过结尾的分隔符，因此
        // `/topic/chat/42`、`/topic/chat`、`/topic` 都会被依次试到，而 `/topicx` 不在其中。
        let mut cursor = location.len();
        loop {
            let candidate = &location[..cursor];
            if let Option::Some(target) = self.prefix_map_.get(candidate) {
                return Option::Some(target);
            }
            if cursor == 0usize {
                return Option::None;
            }
            // 回退到上一个分隔符之前；没有分隔符就轮到空前缀。
            cursor = location[..cursor].rfind('/').unwrap_or_default();
        }
    }

    /// 当前登记了多少条前缀。
    ///
    /// 与旧 `mptp_smux::serving::Router` 保持同一组检查接口；demo 本身只在测试里用它。
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.prefix_map_.len()
    }

    /// 路由表是否为空。
    ///
    /// 与旧 `mptp_smux::serving::Router` 保持同一组检查接口；demo 本身不依赖它。
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.prefix_map_.is_empty()
    }
}

impl<T> TrPathRouter for Router<T> {
    type Handler = T;

    fn match_path<'f>(&'f self, path: &'f str) -> Option<&'f Self::Handler> {
        self.try_match(path)
    }
}

impl<T> Default for Router<T> {
    fn default() -> Self {
        Router::new()
    }
}

#[cfg(test)]
mod tests_ {
    //! 路由表的匹配语义。
    //!
    //! 这里只用整数目标：路由与「目标是什么」无关，用整数能让断言直接落在「命中了哪一条」
    //! 上，而不必构造任何 handler。
    //!
    //! Unit tests for the router's matching semantics, with integer targets so that each
    //! assertion is about *which* entry matched.

    use super::*;

    /// 造一张登记了 `/topic`、`/topic/chat`、`/rpc` 的路由表。
    fn sample_router_() -> Router<u32> {
        let mut router = Router::new();
        router.add_target("/topic", 1u32);
        router.add_target("/topic/chat", 2u32);
        router.add_target("/rpc", 3u32);
        router
    }

    /// 测试路由按**最长前缀**匹配，而不是按登记顺序或最短前缀。
    /// Test that the router matches by longest prefix, not by insertion order.
    ///
    /// - 手段：对 `/topic/chat/42`、`/topic/chat`、`/topic` 三条路径各匹配一次。
    /// - 判断：依次命中 2、2、1——同级下多出来的路径段会落到更具体的那一条上。
    ///
    /// - Means: match `/topic/chat/42`, `/topic/chat` and `/topic` one by one.
    /// - Verdict: they resolve to 2, 2 and 1 respectively, so extra path segments land on the
    ///   more specific entry.
    #[test]
    fn matches_longest_prefix_() {
        let router = sample_router_();

        assert_eq!(router.try_match("/topic/chat/42"), Option::Some(&2u32));
        assert_eq!(router.try_match("/topic/chat"), Option::Some(&2u32));
        assert_eq!(router.try_match("/topic"), Option::Some(&1u32));
    }

    /// 测试前缀回退只发生在**路径分隔处**，不会做字符串前缀匹配。
    /// Test that fallback happens only at path separators, never as a raw string prefix.
    ///
    /// - 手段：匹配 `/topicx`（它以上下文里的 `/topic` 开头，但并不是它的子路径）。
    /// - 判断：返回 `None`；若实现直接做 `starts_with`，这里会错误命中 1。
    ///
    /// - Means: match `/topicx`, which starts with `/topic` as a string but is not one of its
    ///   sub-paths.
    /// - Verdict: `None` — a `starts_with` implementation would wrongly return 1 here.
    #[test]
    fn does_not_match_partial_segment_() {
        let router = sample_router_();

        assert_eq!(router.try_match("/topicx"), Option::None);
        assert_eq!(router.try_match("/topi"), Option::None);
    }

    /// 测试空路由表对任何路径都返回 `None`。
    /// Test that an empty router answers `None` for every path.
    ///
    /// - 手段：用 `Router::<u32>::new()`（未登记任何前缀）匹配三条路径，其中一条是空前缀。
    /// - 判断：三次都是 `None`——「没有目标」不会与「命中空前缀」混淆。
    ///
    /// - Means: match three paths, one of them empty, against a router with no entries.
    /// - Verdict: all three are `None`, so "no target" never looks like "the empty prefix
    ///   matched".
    #[test]
    fn empty_router_matches_nothing_() {
        let router = Router::<u32>::new();

        assert_eq!(router.try_match("/"), Option::None);
        assert_eq!(router.try_match("/anything"), Option::None);
        assert_eq!(router.try_match(""), Option::None);
    }

    /// 测试同一前缀重复登记时返回被替换掉的目标，且表里只留一条。
    /// Test that re-registering a prefix returns the replaced target and keeps one entry.
    ///
    /// - 手段：在示例表上再登记一次 `/rpc`，目标换成 30。
    /// - 判断：返回旧目标 `Some(3)`，随后匹配 `/rpc` 得到 `30`，且表长仍为 3。
    ///
    /// - Means: register `/rpc` again with target 30 on the sample router.
    /// - Verdict: the call yields the old target `Some(3)`, `/rpc` then resolves to 30, and the
    ///   table still holds three entries.
    #[test]
    fn re_register_replaces_target_() {
        let mut router = sample_router_();
        let replaced = router.add_target("/rpc", 30u32);

        assert_eq!(replaced, Option::Some(3u32));
        assert_eq!(router.try_match("/rpc"), Option::Some(&30u32));
        assert_eq!(router.len(), 3usize);
    }

    /// 测试 `TrPathRouter` 适配与 `try_match` 使用同一套最长前缀规则。
    /// Test that the `TrPathRouter` adapter shares the same longest-prefix rule.
    ///
    /// - 手段：对同一张表分别调用 `try_match` 与 `match_path`。
    /// - 判断：两者对 `/topic/chat/42` 返回同一个目标。
    ///
    /// - Means: call both `try_match` and `match_path` on the same table.
    /// - Verdict: both return the same target for `/topic/chat/42`.
    #[test]
    fn match_path_delegates_() {
        let router = sample_router_();

        assert_eq!(
            router.match_path("/topic/chat/42"),
            router.try_match("/topic/chat/42")
        );
    }
}
