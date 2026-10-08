//! 最小复现：tokio `LocalSet` 内能否「同步等待一个由本地任务供料的 future」。
//!
//! 运行：`cargo run -p mptp_cs_demo --example local_block_probe`
//!
//! # 为什么问这个问题
//!
//! `abs_buff` 的环是**全被动**的：环里的数据要靠投递在本地队列（tokio 下即
//! `LocalSet`）上的循环搬进来。而 `abs_buff_stdio_adapt` 的 `AsStdRead` / `AsStdWrite`
//! 是**同步**的 `std::io` 适配器——`rmp_serde` 那类库只认 `std::io::Read/Write`，
//! 所以适配器内部必须把「等数据」变成一次同步阻塞。
//!
//! 于是问题收敛成一句话：**在驱动本地队列的那个调用栈里，能不能同步等到队列上的
//! 任务把数据喂好？** 本程序把它拆成三个独立实验，各建一套干净的运行时与队列。

use std::{
    cell::Cell,
    future::Future,
    pin::pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use tokio::task::LocalSet;

/// 单个实验的超时：到点就判定「不推进」，免得整个程序挂死。
const K_PROBE_TIMEOUT: Duration = Duration::from_secs(2);

/// 纯 park 的同步执行器：只 poll 给定 future，未就绪就把线程挂起（带超时兜底）。
///
/// 它**不**进入任何运行时上下文——这正是我们想验证的那条路：如果不借用
/// `Handle::block_on`，单纯 poll「驱动本地队列的 future」能否推进队列。
fn park_on_<F>(future: F, timeout: Duration) -> Option<F::Output>
where
    F: Future,
{
    struct ThreadWaker_(thread::Thread);

    impl Wake for ThreadWaker_ {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let deadline = Instant::now() + timeout;
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker_(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
            return Option::Some(out);
        }
        if Instant::now() >= deadline {
            return Option::None;
        }
        thread::park_timeout(Duration::from_millis(2));
    }
}

/// 跑一段场景所需的环境：多线程运行时 + 一条本地队列（以 `Rc` 交出，便于移进 future）。
fn with_env_<F, Fut>(body: F)
where
    F: FnOnce(Rc<LocalSet>) -> Fut,
    Fut: Future<Output = ()>,
{
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("建 tokio 运行时");
    let local = Rc::new(LocalSet::new());
    let for_body = Rc::clone(&local);
    rt.block_on(local.run_until(async move {
        body(for_body).await;
    }));
}

/// 投递一个「供料任务」：让出若干次之后把标志置位。
///
/// 它模拟 `smux_v1` 的读循环——**必须在本地队列上被驱动**，否则标志永远不会变。
fn spawn_feeder_(local: &LocalSet, flag: Rc<Cell<bool>>) {
    local.spawn_local(async move {
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        flag.set(true);
    });
}

/// 等标志置位的 future：没置位就让出（于是需要队列被持续驱动）。
async fn wait_flag_(flag: Rc<Cell<bool>>) {
    while !flag.get() {
        tokio::task::yield_now().await;
    }
}

/// **对照实验**：直接 `await`。
///
/// 这是唯一被 tokio 明确支持的形态：`await` 会把控制权交回 `run_until`，队列继续
/// 被驱动，供料任务因此能推进。
fn probe_await_inside_() {
    with_env_(|local| async move {
        let flag = Rc::new(Cell::new(false));
        spawn_feeder_(&local, Rc::clone(&flag));
        // 直接 await：控制权交回外层 `run_until`，队列继续被驱动。
        wait_flag_(flag).await;
        println!("  E1 对照（await + 外层 run_until 驱动）：推进了 ✓");
    });
}

/// **实验 A**：在 `run_until` 的调用栈里嵌套一次 `run_until`，并用 park 执行器同步等。
///
/// 若可行，`AsStdRead/Write` 就能原地修好；若不可行，就必须动架构。
fn probe_nested_run_until_() {
    with_env_(|local| async move {
        let flag = Rc::new(Cell::new(false));
        spawn_feeder_(&local, Rc::clone(&flag));
        // 在**同步**函数里嵌套驱动同一条队列，并用 park 执行器同步等待。
        let done = park_on_(
            local.run_until(wait_flag_(Rc::clone(&flag))),
            K_PROBE_TIMEOUT,
        );
        println!(
            "  E2 嵌套 run_until + park：{}",
            match done {
                Option::Some(()) => "推进了 ✓",
                Option::None => "超时未推进 ✗",
            }
        );
    });
}

/// **实验 B**：在 `run_until` 的调用栈里用 `block_in_place`（`abs_art` 的 `block_on`
/// 在 tokio 下的实现）。用 `catch_unwind` 把 panic 记下来，不让它终止程序。
fn probe_block_in_place_() {
    with_env_(|_local| async move {
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(|| 42u32)
        }));
        println!(
            "  E3 run_until 内 block_in_place：{}",
            match caught {
                Result::Ok(v) => format!("未 panic，返回 {v}"),
                Result::Err(_) => "panic ✗".to_string(),
            }
        );
    });
}

fn main() {
    println!("== LocalSet 内同步等待本地任务的可行性探测 ==");
    println!("（每项实验各自建一套多线程运行时 + 本地队列，超时 {}s）", K_PROBE_TIMEOUT.as_secs());

    // E1 必须放在最前：它也是唯一预期成立的形态。
    println!("\n[E1] 对照组：await + 外层 run_until 驱动");
    probe_await_inside_();

    println!("\n[E2] 嵌套 run_until + park（stdio_adapt 想走的那条路）");
    probe_nested_run_until_();

    println!("\n[E3] run_until 内 block_in_place（abs_art 的 block_on 实现）");
    probe_block_in_place_();

    println!("\n== 结论 ==");
    println!("若 E2 超时且 E3 panic，则「在驱动本地队列的调用栈里同步等待」在本架构下");
    println!("无法成立：`AsStdRead/Write` 必须换掉等待方式，或让连接会话离开本地队列。");
}
