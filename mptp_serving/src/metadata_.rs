//! 端点元数据：一串类型化标记，供中间件按类型读取。
//!
//! 元数据是**声明**，不是运行期状态：它描述「这个端点要遵守什么策略」（鉴权、超时、类型
//! 校验、限流……），不携带跨中间件共享的可变状态。运行时状态放在请求上下文（P2 的
//! `MptpContext`），不放回元数据。
//!
//! 存储用带分配器参数的 `Vec<T, A>`；标记本身是 `Arc<dyn Any>`（`Arc` 固定走全局分配器，
//! 与框架设计 §6.8 一致）。
//!
//! # `metadata_`: typed endpoint metadata
//!
//! Metadata is a declaration, not runtime state. It says which policies an endpoint obeys;
//! runtime state lives on the request context. The collection is an allocator-parameterised
//! `Vec`; each marker is an `Arc<dyn Any>`.

use alloc::{alloc::Global, sync::Arc, vec::Vec};
use core::{alloc::Allocator, any::Any};

/// 一个端点上的类型化元数据集合。
///
/// `A` 是元数据集合的分配器（缺省 [`Global`]）。查询按类型进行：`get::<T>()` 用
/// `downcast_ref` 命中第一个同类型的标记，缺失代表「本端点不需要这项策略」。
pub struct EndpointMetadata<A = Global>
where
    A: Allocator,
{
    /// 按插入顺序保存的标记。
    entries_: Vec<Arc<dyn Any + Send + Sync>, A>,
}

impl<A> EndpointMetadata<A>
where
    A: Allocator,
{
    /// 用指定分配器造一个空元数据集合。
    pub fn new_in(alloc: A) -> Self {
        EndpointMetadata {
            entries_: Vec::new_in(alloc),
        }
    }

    /// 追加一个类型化标记。
    ///
    /// 标记必须是 `'static` 且 `Send + Sync`：它随端点存活，跨 session 共享只读。
    pub fn insert<T>(&mut self, value: T)
    where
        T: Any + Send + Sync + 'static,
    {
        self.entries_.push(Arc::new(value));
    }

    /// 继承一组默认标记：只补本集合里**没有的类型**，已有类型保持原值。
    ///
    /// 用于「router 组默认元数据 → 端点」的继承：先插入先命中，因此端点显式声明的标记
    /// 覆盖组默认值。[`EndpointMetadata::get`] 按插入顺序取第一个同类型标记。
    pub fn inherit(&mut self, parent: &EndpointMetadata<A>) {
        for entry in parent.entries_.iter() {
            let type_id = entry.as_ref().type_id();
            let present = self
                .entries_
                .iter()
                .any(|own| own.as_ref().type_id() == type_id);
            if !present {
                self.entries_.push(Arc::clone(entry));
            }
        }
    }

    /// 按类型取标记；缺失返回 `None`。
    pub fn get<T>(&self) -> Option<&T>
    where
        T: Any + Send + Sync + 'static,
    {
        self.entries_
            .iter()
            .find_map(|entry| entry.downcast_ref::<T>())
    }

    /// 是否没有标记。
    pub fn is_empty(&self) -> bool {
        self.entries_.is_empty()
    }

    /// 标记个数。
    pub fn len(&self) -> usize {
        self.entries_.len()
    }
}

impl Default for EndpointMetadata<Global> {
    fn default() -> Self {
        Self::new_in(Global)
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 一个只读声明式标记。
    struct Authorize {
        /// 允许的角色。
        allow_: &'static str,
    }

    /// 测试 `insert` / `get` 按类型命中，缺失返回 `None`。
    /// Test that insert/get round-trip by type and a missing type yields `None`.
    ///
    /// - 手段：插入一个 `Authorize`，再 `get::<Authorize>()` 与 `get::<u32>()`。
    /// - 判断：前者返回插入的值，后者返回 `None`。
    ///
    /// - Means: insert one `Authorize`, then query it and a `u32`.
    /// - Verdict: the former returns the inserted value, the latter `None`.
    #[test]
    fn insert_and_get_by_type_() {
        let mut metadata = EndpointMetadata::default();
        metadata.insert(Authorize { allow_: "admin" });

        assert_eq!(
            metadata.get::<Authorize>().map(|m| m.allow_),
            Option::Some("admin")
        );
        assert!(metadata.get::<u32>().is_none());
        assert_eq!(metadata.len(), 1usize);
    }

    /// 测试继承只补缺失的类型，端点自己的标记优先。
    /// Test that inheritance only fills missing types and keeps the endpoint's own marker.
    ///
    /// - 手段：端点集合先插入 `Authorize { allow: "endpoint" }`，再继承父集合的
    ///   `Authorize { allow: "group" }` 与 `Timeout`；父集合用另一个标记类型。
    /// - 判断：`get::<Authorize>()` 仍是端点的值；父集合独有类型能被继承到；`len` 只增加
    ///   缺失的那一个。
    ///
    /// - Means: insert an endpoint-owned `Authorize`, then inherit a parent `Authorize` and
    ///   a parent-only `GroupTag`.
    /// - Verdict: `Authorize` keeps the endpoint value, the group-only type appears, and
    ///   `len` grows by exactly one.
    #[test]
    fn inherit_keeps_own_marker_() {
        /// 父集合独有的标记。
        #[derive(Debug)]
        struct GroupTag(&'static str);

        let mut endpoint = EndpointMetadata::default();
        endpoint.insert(Authorize { allow_: "endpoint" });

        let mut group = EndpointMetadata::default();
        group.insert(Authorize { allow_: "group" });
        group.insert(GroupTag("scene"));

        endpoint.inherit(&group);

        assert_eq!(
            endpoint.get::<Authorize>().map(|m| m.allow_),
            Option::Some("endpoint")
        );
        assert_eq!(
            endpoint.get::<GroupTag>().map(|m| m.0),
            Option::Some("scene")
        );
        assert_eq!(endpoint.len(), 2usize);
    }

    /// 测试空集合对任何类型都返回 `None`。
    /// Test that an empty collection returns `None` for every type.
    ///
    /// - 手段：对 `EndpointMetadata::default()` 查询一个类型。
    /// - 判断：`get` 为 `None`，且 `is_empty` 为真。
    ///
    /// - Means: query one type against a default collection.
    /// - Verdict: `get` is `None` and `is_empty` is true.
    #[test]
    fn empty_metadata_() {
        let metadata = EndpointMetadata::default();
        assert!(metadata.get::<Authorize>().is_none());
        assert!(metadata.is_empty());
    }
}
