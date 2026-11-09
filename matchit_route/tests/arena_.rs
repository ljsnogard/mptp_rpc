//! `Router` 自有 arena 的 RAII、移动与线程安全测试。
//!
//! RAII, move and thread-safety tests for the router-owned bump arena.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use matchit_route::{InsertError, Router};

/// 一个在析构时计数的值。
///
/// A value that bumps a counter when dropped.
#[derive(Clone)]
struct DropCounter {
    dropped: Arc<AtomicUsize>,
}

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// 测试 `Router` 被丢弃时会执行值的析构函数。
/// Test that dropping a `Router` runs the destructors of its values.
///
/// - 手段：向 router 插入一个 `DropCounter`，确认丢弃前计数为 0，再丢弃 router。
/// - 判断：丢弃 router 后计数变为 1，说明 bumpalo Box 的 RAII 语义生效。
///
/// - Means: insert a `DropCounter`, check the count is 0, then drop the router.
/// - Verdict: the count becomes 1, proving the bumpalo box runs destructors.
#[test]
fn drop_runs_value_destructors_() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut router = Router::new();
    router
        .insert(
            "/counted",
            DropCounter {
                dropped: dropped.clone(),
            },
        )
        .unwrap();

    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(router);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

/// 测试 `Router` 移动后仍能匹配，arena 地址保持稳定。
/// Test that a moved `Router` still matches, i.e. the arena address is stable.
///
/// - 手段：在函数内构造 router，移动到另一个函数后匹配并读取参数。
/// - 判断：移动后的匹配结果与原路由一致。
///
/// - Means: build a router, move it into another function, then match.
/// - Verdict: the moved router returns the original route and parameter.
#[test]
fn move_keeps_arena_valid_() {
    fn consume(router: Router<&'static str>) -> &'static str {
        let matched = router.at("/users/42").unwrap();
        assert_eq!(matched.params.get("id"), Some("42"));
        matched.value
    }

    let mut router = Router::new();
    router.insert("/users/{id}", "user").unwrap();
    assert_eq!(consume(router), "user");
}

/// 测试 `Router<T>` 在 `T: Send + Sync` 时可以被跨线程共享与移动。
/// Test that `Router<T>` is `Send + Sync` when `T` is.
///
/// - 手段：用泛型断言要求 `Router<u32>` 实现 `Send + Sync`，并把它移入一个线程。
/// - 判断：编译通过，且线程内匹配成功。
///
/// - Means: assert `Router<u32>: Send + Sync`, then move it into a thread.
/// - Verdict: it compiles and the thread matches the route.
#[test]
fn router_is_send_and_sync_() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Router<u32>>();

    let mut router = Router::new();
    router.insert("/ping", 7u32).unwrap();

    let handle = std::thread::spawn(move || *router.at("/ping").unwrap().value);
    assert_eq!(handle.join().unwrap(), 7);
}

/// 测试 `clone` 使用独立 arena，删除克隆不会影响原 router。
/// Test that `clone` owns an independent arena and removals do not affect the original.
///
/// - 手段：克隆 router，在克隆上删除路由并调用 `at_mut` 改值。
/// - 判断：克隆上删除后匹配失败；原 router 仍可匹配且值未被修改。
///
/// - Means: clone the router, remove a route and mutate a value on the clone.
/// - Verdict: the clone no longer matches; the original still matches its value.
#[test]
fn clone_uses_independent_arena_() {
    let mut router = Router::new();
    router.insert("/value", 1u32).unwrap();
    router.insert("/other", 2u32).unwrap();

    let mut cloned = router.clone();
    assert_eq!(cloned.remove("/value"), Some(1));
    assert!(cloned.at("/value").is_err());
    *cloned.at_mut("/other").unwrap().value = 20;

    assert_eq!(*router.at("/value").unwrap().value, 1);
    assert_eq!(*router.at("/other").unwrap().value, 2);
}

/// 测试 `for_each` 消费 router 并按路由返回原始路径与值。
/// Test that `for_each` consumes the router and yields original paths and values.
///
/// - 手段：插入两条带参数的路由，用 `for_each` 收集到 `Vec` 后排序。
/// - 判断：路径已还原成原始参数名，值一一对应。
///
/// - Means: insert two parameterised routes and collect `for_each` output.
/// - Verdict: paths are denormalised to the original names and values match.
#[test]
fn for_each_yields_original_paths_() {
    let mut router = Router::new();
    router.insert("/users/{id}", "user").unwrap();
    router.insert("/posts/{slug}", "post").unwrap();

    let mut found = Vec::new();
    router.for_each(|path, value| found.push((path.to_string(), value)));
    found.sort();

    assert_eq!(
        found,
        vec![
            ("/posts/{slug}".to_string(), "post"),
            ("/users/{id}".to_string(), "user"),
        ]
    );
}

/// 测试冲突错误携带 `abs_str` 的分配器字符串，并能正常显示。
/// Test that conflict errors carry an `abs_str` allocator string and render it.
///
/// - 手段：连续插入两条冲突路由，取出 `InsertError::Conflict` 的 `with` 字段。
/// - 判断：错误格式化后包含已注册路由，且 `with` 的类型是 `abs_str::str_alloc::String`。
///
/// - Means: insert two conflicting routes and inspect `InsertError::Conflict`.
/// - Verdict: the display text contains the registered route and the field is an `abs_str` string.
#[test]
fn conflict_uses_abs_str_() {
    fn assert_abs_string(
        _: &matchit_route::x_deps::abs_str::str_alloc::String<std::alloc::Global>,
    ) {
    }

    let mut router = Router::new();
    router.insert("/users/{id}", 1u32).unwrap();

    let err = router.insert("/users/{name}", 2u32).unwrap_err();
    let InsertError::Conflict { with } = &err else {
        panic!("expected a conflict, got {err:?}");
    };

    assert_abs_string(with);
    assert!(with.contains("/users/{id}"));
    assert!(err.to_string().contains("/users/{id}"));
}
