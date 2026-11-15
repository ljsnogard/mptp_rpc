//! 高性能、零拷贝 URL router 的 `no_std` 移植版（源自 `matchit` 0.9.2）。
//!
//! 路由规则在构建完成后保持内存稳定：整棵基数树、节点前缀与参数名都放在 router
//! 自有的 `bumpalo` arena 里；`Router` 被丢弃时，值先析构，arena 随后整体释放。
//! 只有参数列表、回溯栈与错误信息这类**一次性**数据会临时走全局分配器。
//!
//! ```rust
//! use matchit_route::Router;
//!
//! let mut router = Router::new();
//! router.insert("/home", "Welcome!").unwrap();
//! router.insert("/users/{id}", "A User").unwrap();
//!
//! let matched = router.at("/users/978").unwrap();
//! assert_eq!(matched.params.get("id"), Some("978"));
//! assert_eq!(*matched.value, "A User");
//! ```
//!
//! # 参数
//!
//! 支持命名参数 `/{id}` 与 catch-all 参数 `/{*rest}`；同一个路径段内只允许一个命名
//! 参数。`{` / `}` 可以用 `{{` / `}}` 转义为静态字符。
//!
//! ```rust
//! use matchit_route::Router;
//!
//! let mut router = Router::new();
//! router.insert("/images/img{id}.png", true).unwrap();
//!
//! assert_eq!(router.at("/images/img1.png").unwrap().params.get("id"), Some("1"));
//! assert!(router.at("/images/img1.jpg").is_err());
//! ```
//!
//! ```rust
//! use matchit_route::Router;
//!
//! let mut router = Router::new();
//! router.insert("/{*p}", true).unwrap();
//!
//! assert_eq!(router.at("/foo.js").unwrap().params.get("p"), Some("foo.js"));
//! assert_eq!(router.at("/c/bar.css").unwrap().params.get("p"), Some("c/bar.css"));
//! assert!(router.at("/").is_err());
//! ```
//!
//! # 优先级
//!
//! 静态段优先于参数段，参数段优先于 catch-all：
//!
//! ```rust
//! use matchit_route::Router;
//!
//! let mut router = Router::new();
//! router.insert("/", "root").unwrap();
//! router.insert("/about", "about").unwrap();
//! router.insert("/{*filepath}", "catch-all").unwrap();
//!
//! assert_eq!(*router.at("/about").unwrap().value, "about");
//! assert_eq!(*router.at("/other").unwrap().value, "catch-all");
//! ```
//!
//! # 冲突规则（0.9.2）
//!
//! 0.9.2 收紧了前缀参数与后缀参数的冲突规则：静态段可以任意重叠；同一段内，带后缀的
//! 参数之间按最长后缀优先，带前缀的参数之间按最长前缀优先；但“前缀参数 + 后缀参数”
//! 指向同一段时视为歧义，插入返回 [`InsertError::Conflict`]。catch-all 仍与带后缀参数
//! 冲突。完整的规则见 crate 文档的英文说明与上游 `matchit` 0.9.2。
//!
//! # 内存模型
//!
//! - `Router<T>` 自己持有一个 `bumpalo::Bump`，节点用 `bumpalo::boxed::Box` 持有值，
//!   因此 `Drop` 会按 RAII 语义先析构 `T`，再释放 arena；
//! - `Router` 只在 `&mut self` 的注册 / 删除路径上写 arena，匹配路径只读；
//! - 因 arena 分配器不是 `Sync`，但匹配过程不触碰分配器，本 crate 为
//!   `Router<T>: Send`（`T: Send`）与 `Router<T>: Sync`（`T: Sync`）提供了带安全论证的
//!   手动实现。
//!
//! # `matchit_route`: a no_std port of matchit
//!
//! A zero-copy URL router with `matchit` 0.9.2's API and matching algorithm. The routing
//! tree lives in a router-owned bump arena; only transient data (parameter overflow,
//! backtracking stack and error messages) uses the global allocator. Values are kept
//! in `bumpalo::boxed::Box`, so dropping the router runs their destructors before the
//! arena is released.

#![no_std]
#![deny(rust_2018_idioms)]
#![deny(unsafe_op_in_unsafe_fn)]

extern crate alloc;

mod error;
mod escape;
mod params;
mod router;
mod tree;

pub use error::{InsertError, MatchError, MergeError};
pub use params::{Params, ParamsIter};
pub use router::{Match, Router};

/// 依赖重导出，便于下游命名错误信息里的 `String` 或自行使用同一套分配工具。
pub mod x_deps {
    pub use abs_str;
    pub use bumpalo;
}

#[cfg(doctest)]
mod readme {
    #[allow(dead_code)]
    #[doc = include_str!("../README.md")]
    struct Readme;
}
