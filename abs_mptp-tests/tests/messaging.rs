//! `abs_mptp::messaging` 的集成测试：限长读写与分块帧。
//!
//! 它放在 `tests/` 而不是 `src/`：用例只走**公开 API**，库源码里不该再有 `#[cfg(test)]`。
//!
//! # 为什么限长那几条用例要跑在后端运行时里
//!
//! `abs_mptp` 本体不依赖任何 io 适配 crate，也不替使用者选后端。本仓是**应用**：限长那几条
//! 用例按应用的做法，用 `abs_buff_stdio_adapt` 的 `AsStdRead` / `AsStdWrite` 把段接口暴露成
//! `std::io::Read` / `Write`，而它在数据没到时会同步等待——同步等待要问门面取**当前后端**的
//! 运行时值。分块那几条用例不走 `io`：它们直接在段接口上搬运，因此与后端运行时无关。
//!
//! 「当前后端」是**编译期**决定的（入口是 `abs_art-facade`），而**选择它的地方是本仓的
//! `Cargo.toml`**：`abs_mptp` 本体不带任何 `rt-*` feature，后端选择留在「应用」这一侧。
//! 本仓只点亮 `rt-tokio`，因此这里固定挂 `#[tokio::test]`。
//!
//! 收发半边在用例里用 `&[u8]` / `&mut [u8]` 充当：它们的段接口立即就绪，于是这些用例
//! 只覆盖协议行为，不牵扯真实传输。
//!
//! # Integration tests for `abs_mptp::messaging`
//!
//! These live under `tests/` rather than `src/`: they exercise the **public API** only, and
//! the library source keeps no `#[cfg(test)]` module.
//!
//! `abs_mptp` itself depends on no io adapter crate and never picks a backend for its users.
//! This crate is the *application*: the limited-IO cases follow the application's route and
//! expose the segment interface as `std::io::Read` / `Write` through `abs_buff_stdio_adapt`,
//! whose synchronous waits ask the facade for the active backend's runtime. The chunked cases
//! never touch `io` — they move bytes on the segment interface directly, so they do not care
//! about the runtime. Buffers are plain slices, so only the protocol behaviour is under test —
//! no real transport is involved.

// 测试函数沿用生产 code 的命名习惯：内部函数以 `_` 结尾。
#![allow(non_snake_case)]

use abs_buff::{
    Demand, ReadySegm, TrBuffRead, TrBuffTryRead, TrBuffTryWrite, TrBuffWrite,
    buffer::{TrBuffSegmRef, TrBuffSegmView, TrConsumerState, TrProducerState},
    error::{ReadErrTag, TrTaggedError, WriteErrTag},
    x_deps::{abs_cancel, anylr},
};
use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};
use abs_cancel::{NonCancellableToken, ok_or::OkOr};
use abs_mptp::messaging::{
    chunked::{ChunkedReader, ChunkedReadError, ChunkedEncode, K_MAX_CHUNK_PAYLOAD},
    limited::{LimitedReader, LimitedReaderError, LimitedWriter, LimitedWriterError},
};
use anylr::SomeOf;

/// 在当前后端对应的运行时上下文里定义一个用例。
///
/// 本仓的后端**就是 tokio**（见 `Cargo.toml`：`abs_art-facade` 关掉默认 feature、只点亮
/// `rt-tokio`），因此入口固定为 `#[tokio::test]`，不需要任何 `#[cfg]` 分派。要换后端，
/// 改 `Cargo.toml` 里那一行并同步这里即可。
///
/// Define one case inside the runtime context of the active backend. This crate's backend
/// *is* tokio (see `Cargo.toml`), so the entry attribute is fixed and no `#[cfg]` dispatch
/// is needed.
macro_rules! backend_async_test {
    ($(#[$meta:meta])* fn $name:ident() $body:block) => {
        $(#[$meta])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 1usize)]
        async fn $name() $body
    };
}

/// 睡 `ms` 毫秒，按当前后端挑计时器。
///
/// 竞速的语义差别只在「两条分支谁先就绪」上，因此要用**真实时序**去测，而不是用一个永远
/// 立即就绪的源去逼近。
///
/// Sleep for `ms` milliseconds using the active backend's timer, so race semantics can be
/// exercised with real timing rather than an always-ready source.
async fn sleep_ms_(ms: u64) {
    tokio::time::sleep(core::time::Duration::from_millis(ms)).await;
}

/// 一个「睡够 `ms` 毫秒才给出数据」的异步操作。
///
/// An asynchronous operation that only yields its data after sleeping `ms` milliseconds.
async fn slow_op_(ms: u64) -> &'static str {
    sleep_ms_(ms).await;
    "data"
}

/// 一个「睡够 `ms` 毫秒才触发」的取消令牌。
///
/// 它报告 `can_be_cancelled() == true`，取消 future 睡 `ms` 毫秒后就绪——即真实的
/// 「信号稍后到达」。触发标志用 `Arc<AtomicBool>` 持有，竞速结束后调用方仍能读它，用来
/// 证明竞速**没有**顺手把信号跑出来。
///
/// 取消 future 装进 `Box<dyn Future>`：它只在这个用例的局部作用域里被轮询，
/// `TrCancellationToken` 也不要求关联 future 是 `Send`。
///
/// A cancellation token that fires after `ms` milliseconds — a genuine "signal arrives
/// later". The flag is shared through an `Arc<AtomicBool>` so the caller can still read it
/// after the race. The cancellation future is a boxed local future; the token trait does
/// not require its associated future to be `Send`.
struct TimedToken_ {
    ms_: u64,
    done_: std::sync::Arc<core::sync::atomic::AtomicBool>,
}

impl abs_cancel::TrCancellationToken for TimedToken_ {
    type Cancellation = core::pin::Pin<Box<dyn Future<Output = ()> + 'static>>;
    type ChildToken = TimedToken_;

    fn is_cancelled(&self) -> bool {
        self.done_.load(core::sync::atomic::Ordering::SeqCst)
    }

    fn can_be_cancelled(&self) -> bool {
        true
    }

    fn child_token(&self) -> Self::ChildToken {
        TimedToken_ {
            ms_: self.ms_,
            done_: self.done_.clone(),
        }
    }

    fn cancellation(self) -> Self::Cancellation {
        let done = self.done_;
        let ms = self.ms_;
        Box::pin(async move {
            sleep_ms_(ms).await;
            done.store(true, core::sync::atomic::Ordering::SeqCst);
        })
    }
}

/// 依 `OkOr` 的轮询顺序竞速，并把结果折叠成「数据 / 被取消」两态。
///
/// **生产代码用的就是这条路径**（`read_fut.ok_or(cancel.cancellation())`），因此下面的时序
/// 用例盯的是真实实现，不是仿真。
///
/// Race in `OkOr`'s polling order and fold the outcome into "data / cancelled". This is the
/// very path production code takes, so the timing cases below observe the real
/// implementation rather than a model of it.
async fn raced_<TyFut, TyTok>(fut: TyFut, tok: TyTok) -> &'static str
where
    TyFut: Future<Output = &'static str>,
    TyTok: abs_cancel::TrCancellationToken,
{
    match OkOr::new(fut, tok.cancellation()).await {
        Result::Ok(_) => "data",
        Result::Err(_) => "cancelled",
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 竞速的真实时序 / real timing of the race
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//
// 等待点统一走 `OkOr`（`read_fut.ok_or(cancel.cancellation())`，见 dev-notes §3）。这两条
// 用例用**真实时序**钉住它的行为：**谁先就绪谁赢**。
//
// 需要留意的取舍：`OkOr` 的轮询顺序是「先试底层、再试令牌」，因此当**两端同一轮都就绪**
// （令牌早已取消、底层又恰好立即就绪）时它取数据、不报 `Cancelled`。真实 IO 在数据未到
// 时是 `Pending`，所以那个窗口很窄；代价是「底层当下有数据」时取消不再优先——库内因此
// 不再有 `race_cancel_`，也**没有**用例再去断言「已取消的令牌 + 立即就绪的源 → Cancelled」
// （那正是被删掉的那条断言）。
//
// Both cases pin `OkOr`'s behaviour with genuine timing: whoever becomes ready first wins.
// One trade-off is deliberate and worth noticing: `OkOr` polls the underlying operation
// first, so when *both* are ready within the same poll (token already cancelled and the
// source immediately ready) it yields the data rather than `Cancelled`. Real IO is
// `Pending` until data arrives, so the window is narrow — but the library no longer
// promises cancel-priority over ready data, and no case asserts it any more.

backend_async_test! {
    /// 测试「操作 40ms 完成、取消信号 50ms 才到」时，数据如期交出。
    ///
    /// - 手段：操作睡 40ms 后返回数据，令牌的取消 future 睡 50ms，用生产代码同一条路径
    ///   （`OkOr`）竞速。
    /// - 判断：拿到数据；且令牌的取消信号**从未被触发**——竞速赢家是操作，取消 future
    ///   随竞速结束被丢弃，它的定时器也就没跑到。
    ///
    /// Verifies that when the operation finishes at 40ms and the cancel signal only arrives
    /// at 50ms, the data is handed back.
    /// - Means: race an operation completing after 40ms against a token firing after 50ms,
    ///   through the same `OkOr` path production code uses.
    /// - Criteria: the data comes back and the token never fired — the losing cancellation
    ///   future was dropped before its timer expired.
    fn race_operation_wins_when_it_finishes_first_() {
        use std::sync::Arc;
        use core::sync::atomic::{AtomicBool, Ordering};

        let done = Arc::new(AtomicBool::new(false));
        let token = |done: Arc<AtomicBool>| TimedToken_ { ms_: 50, done_: done };

        let verdict = raced_(slow_op_(40u64), token(done.clone())).await;
        assert_eq!(verdict, "data", "操作先就绪就该拿到数据");
        assert!(!done.load(Ordering::SeqCst), "取消信号不该被触发");
    }
}

backend_async_test! {
    /// 测试「取消信号 40ms 到、操作 50ms 才完成」时以取消收场。
    ///
    /// - 手段：令牌的取消 future 睡 40ms，操作睡 50ms，走生产代码同一条 `OkOr` 路径。
    /// - 判断：以取消收场——操作那一支在 50ms 之前一直 `Pending`，因此「先试底层」也不会
    ///   误取到数据；这正是「信号稍后到达」能被如实响应的证据。
    ///
    /// Verifies that when the cancel signal arrives at 40ms and the operation only finishes
    /// at 50ms, the race ends as cancelled.
    /// - Means: race with the token at 40ms and the operation at 50ms through `OkOr`.
    /// - Criteria: the verdict is "cancelled" — the operation stays `Pending` until 50ms,
    ///   so trying it first cannot accidentally collect data.
    fn race_cancel_wins_when_it_fires_first_() {
        use std::sync::Arc;
        use core::sync::atomic::{AtomicBool, Ordering};

        let done = Arc::new(AtomicBool::new(false));
        let token = |done: Arc<AtomicBool>| TimedToken_ { ms_: 40, done_: done };

        let verdict = raced_(slow_op_(50u64), token(done.clone())).await;
        assert_eq!(verdict, "cancelled", "信号先到就该以取消收场");
        assert!(done.load(Ordering::SeqCst), "取消信号应当被触发");
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 限长读写 / limited IO
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

backend_async_test! {
    /// 测试限长读读到额度就用完，不会多借一个字节。
    ///
    /// - 手段：以 `b"abcdef"` 为源、额度 3 构造 [`LimitedReader`]，用 `AsStdRead` 一次读满
    ///   一个 8 字节缓冲。
    /// - 判断：只读到 `b"abc"`，额度归零（额度用尽在读者眼里就是 EOF），且底层源恰好
    ///   推进了 3 个字节。
    ///
    /// Verifies that a limited read stops exactly at the configured quota.
    /// - Means: wrap `b"abcdef"` with a quota of 3, then drain an 8-byte buffer through
    ///   `AsStdRead`.
    /// - Criteria: only `b"abc"` is read, the quota drops to zero (EOF for the reader),
    ///   and the underlying source advanced by exactly 3 bytes.
    fn limited_read_stops_at_the_limit_() {
        let mut src: &[u8] = b"abcdef";
        {
            let mut limited = LimitedReader::new(&mut src, 3usize);
            let mut read = AsStdRead::new(&mut limited, NonCancellableToken::new());
            let mut buf = [0u8; 8usize];
            let n = std::io::Read::read(&mut read, &mut buf).expect("读不该失败");
            assert_eq!(&buf[..n], b"abc", "只应当读到额度内的 3 个字节");
            assert_eq!(limited.remaining(), 0usize, "额度应当刚好用完");
            assert!(limited.is_exhausted(), "额度归零后应当报告已用尽");
        }
        assert_eq!(src, b"def", "底层源恰好推进了被读走的那 3 个字节");
    }
}

backend_async_test! {
    /// 测试限长写最多只借出额度那么多空间。
    ///
    /// - 手段：以 16 字节缓冲为底、额度 4 构造 [`LimitedWriter`]，用 `AsStdWrite` 尝试写
    ///   10 个字节。
    /// - 判断：只写进去 4 个字节（返回 4），底层缓冲的前 4 个字节是 `b"abcd"`，额度归零。
    ///
    /// Verifies that a limited write never lends more room than the quota.
    /// - Means: wrap a 16-byte buffer with a quota of 4, then try to write 10 bytes
    ///   through `AsStdWrite`.
    /// - Criteria: exactly 4 bytes land in the buffer as `b"abcd"` and the quota is zero.
    fn limited_write_caps_the_amount_() {
        let mut storage = [0u8; 16usize];
        {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut limited = LimitedWriter::new(&mut sink, 4usize);
            let mut write = AsStdWrite::new(&mut limited, NonCancellableToken::new());
            let n = std::io::Write::write(&mut write, b"abcdefghij").expect("写不该失败");
            assert_eq!(n, 4usize, "只应当写进额度内的 4 个字节");
            assert_eq!(limited.remaining(), 0usize, "额度应当刚好用完");
        }
        assert_eq!(&storage[..4], b"abcd", "写进去的应当正好是前 4 个字节");
    }
}

backend_async_test! {
    /// 测试额度按**段真正被消费的量**结算，而不是「借出多少就扣多少」。
    ///
    /// 段接口的消费量只在段被回收时记账，因此包装层必须比较「借出时的剩余量」与
    /// 「归还时的剩余量」。这条用例就是那个不变量的直接回归。
    ///
    /// - 手段：额度 4 的限长读上借出一段（同步 `try_read`，需求 4），只从该段搬走
    ///   2 个字节就放掉；随后再借一次。
    /// - 判断：第一次借到 4 个字节的段、搬 2 个；段回收后剩余额度为 2；第二次只借得到
    ///   2 个字节，且底层源总共只推进了 4 个字节。
    ///
    /// Verifies that the quota settles on what the borrowed segment *actually*
    /// consumed, not on what was lent out.
    /// - Means: borrow a 4-byte segment from a quota-4 reader via the sync `try_read`,
    ///   move only 2 bytes out of it, drop it, then borrow again.
    /// - Criteria: the counter drops to 2 after the first segment is reclaimed, the
    ///   second borrow yields exactly 2 bytes, and the source advanced by 4 in total.
    fn limited_quota_settles_on_actual_consumption_() {
        let mut src: &[u8] = b"abcdef";
        let mut limited = LimitedReader::new(&mut src, 4usize);

        {
            let demand = Demand::no_more_than(4usize);
            let Some(mut segm) = limited.try_read(&demand).pick_left() else {
                panic!("额度充足时应当借得出段");
            };
            // 借到的整段是 4 字节，但只搬走 2 个。
            assert_eq!(segm.least_count(), 4usize, "借出的段应当正好是额度那么多");
            let mut child = segm.as_segm_ref();
            let mut part = [0u8; 2usize];
            let target = unsafe {
                core::slice::from_raw_parts_mut(
                    part.as_mut_ptr().cast::<core::mem::MaybeUninit<u8>>(),
                    part.len(),
                )
            };
            // SAFETY: 搬的是 `u8`（无需 drop 的资源），`target` 在整个搬移期间独占借用。
            let moved = unsafe { child.move_items_to_buff(target) };
            assert_eq!(moved, 2usize, "只搬 2 个字节");
        }
        // 段已经在这里被回收，额度据此结算。
        assert_eq!(limited.remaining(), 2usize, "额度应当只扣掉真正被消费的 2 个字节");

        {
            let demand = Demand::no_more_than(4usize);
            let Some(mut segm) = limited.try_read(&demand).pick_left() else {
                panic!("还剩 2 个字节的额度，应当借得出段");
            };
            assert_eq!(segm.least_count(), 2usize, "第二次只借得到剩余的 2 个字节");
            let mut child = segm.as_segm_ref();
            let mut part = [0u8; 4usize];
            let target = unsafe {
                core::slice::from_raw_parts_mut(
                    part.as_mut_ptr().cast::<core::mem::MaybeUninit<u8>>(),
                    part.len(),
                )
            };
            // SAFETY: 同上一处：位搬移，`u8` 无 drop 需求。
            let moved = unsafe { child.move_items_to_buff(target) };
            assert_eq!(moved, 2usize, "第二次应当借满剩余额度");
        }
        assert_eq!(limited.remaining(), 0usize, "额度用尽");
        assert_eq!(src, b"ef", "底层源总共只推进了被消费的 4 个字节");
    }
}

backend_async_test! {
    /// 测试额度用尽的读口再借段时给出明确的 `Exhausted`，而不是空转。
    ///
    /// - 手段：额度 0 的限长读上同步借段（需求 1），再用取消令牌驱动的异步路径借一次。
    /// - 判断：两条路径都返回 [`LimitedReaderError::Exhausted`]；其错误标签是 `Closing`，
    ///   也就是 `AsStdRead` 眼里的 EOF。
    ///
    /// Verifies that a drained limited reader reports `Exhausted` on both paths instead
    /// of spinning.
    /// - Means: borrow once synchronously (demand 1) from a quota-0 reader, then borrow
    ///   again through the cancellable async path.
    /// - Criteria: both paths report `Exhausted`, whose tag is `Closing` — EOF for
    ///   `AsStdRead`.
    fn limited_reader_reports_exhausted_() {
        let mut src: &[u8] = b"abc";
        let mut limited = LimitedReader::new(&mut src, 0usize);

        let demand = Demand::no_more_than(1usize);
        let err = match limited.try_read(&demand).pick_right() {
            Option::Some(err) => err,
            Option::None => panic!("额度为 0 时同步借段应当报 Exhausted"),
        };
        assert!(matches!(err, LimitedReaderError::Exhausted));
        assert_eq!(err.err_tag(), ReadErrTag::Closing, "额度用尽对读者就是流结束");

        let demand = Demand::no_more_than(1usize);
        let err = match limited.read_async(&demand).await.pick_right() {
            Option::Some(err) => err,
            Option::None => panic!("额度为 0 时异步借段应当报 Exhausted"),
        };
        assert!(matches!(err, LimitedReaderError::Exhausted));
        assert_eq!(limited.remaining(), 0usize, "报 Exhausted 不该消耗额度");
    }
}

backend_async_test! {
    /// 测试额度用尽的写口给出明确的 `Exhausted`，而不是被误当成「暂时没空间」。
    ///
    /// - 手段：以 4 字节缓冲为底、额度 0 构造 [`LimitedWriter`]，同步借段一次，再异步借段
    ///   一次。
    /// - 判断：两次都报 [`LimitedWriterError::Exhausted`]（错误标签 `Closing`）；底层缓冲
    ///   一个字节都没被动过。
    ///
    /// Verifies that a drained limited writer reports `Exhausted` on both paths.
    /// - Means: wrap a 4-byte buffer with a quota of 0 and borrow once synchronously,
    ///   then once asynchronously.
    /// - Criteria: both report [`LimitedWriterError::Exhausted`] with the `Closing` tag,
    ///   and no byte of the underlying buffer is touched.
    fn limited_writer_reports_exhausted_() {
        let mut storage = [0u8; 4usize];
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let mut limited = LimitedWriter::new(&mut sink, 0usize);

        let demand = Demand::no_more_than(2usize);
        let err = match limited.try_write(&demand).pick_right() {
            Option::Some(err) => err,
            Option::None => panic!("额度为 0 时同步借段应当报 Exhausted"),
        };
        assert!(matches!(err, LimitedWriterError::Exhausted));
        assert_eq!(err.err_tag(), WriteErrTag::Closing);

        let demand = Demand::no_more_than(2usize);
        let err = match limited.write_async(&demand).await.pick_right() {
            Option::Some(err) => err,
            Option::None => panic!("额度为 0 时异步借段应当报 Exhausted"),
        };
        assert!(matches!(err, LimitedWriterError::Exhausted));
        assert!(limited.is_exhausted(), "额度仍然是用尽状态");
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 分块帧 / chunk framing
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 从分块读视图里借出一个段，**搬走**它覆盖的字节并返回（段随即归还）。
///
/// 段接口的消费量只在段被回收时结算：只读一眼段里的字节是不会推进读视图的，必须真的把
/// 它们搬出去。这里搬进一个拥有所有权的 `Vec`，于是「借出 → 消费 → 归还」三件事都发生了，
/// 读视图也就能接着读下一块。
///
/// Borrow one segment out of a chunked view, **move** its bytes out and return them (the
/// segment is reclaimed right away). Consumption is only settled when a segment is
/// dropped, so merely looking at the bytes would not advance the view.
async fn next_chunk_<TyRx>(reader: &mut ChunkedReader<'_, TyRx>) -> Option<Vec<u8>>
where
    TyRx: TrBuffRead<u8> + TrConsumerState,
{
    let demand = Demand::no_more_than(16usize);
    let borrowed = reader.read_async(&demand).await;
    // 段借用了 `reader` 内部的需求槽，因此显式限定作用域，让它在函数返回之前就归还。
    let bytes = {
        let mut segm = borrowed.pick_left()?;
        let mut bytes = vec![0u8; segm.least_count()];
        // 与 `AsStdRead` 同一条路径：段的缓冲就是源自己的内存，`u8` 没有 drop 需求，
        // 搬移只推进段自身的偏移。
        let target = unsafe {
            core::slice::from_raw_parts_mut(
                bytes.as_mut_ptr().cast::<core::mem::MaybeUninit<u8>>(),
                bytes.len(),
            )
        };
        // `TrBuffSegmRef::move_items_to_buff` 是安全方法：被搬的是 `u8`（无需 drop 的
        // 资源），目标切片在整个搬移期间独占借用。
        let moved = segm.move_items_to_buff(target);
        bytes.truncate(moved);
        bytes
    };
    Option::Some(bytes)
}

/// 一个「报告的可用空间恒为 `room_`」的写目标：底层就是普通切片，只是把
/// [`TrProducerState`] 报小，用来钉住「块长由目标报告的剩余空间预先定下」。
///
/// A write target that always reports `room_` bytes of available space. The backing store is
/// a plain slice, so the only thing under test is how the reported room shapes the chunks.
struct FixedRoomTx_<'a> {
    inner_: &'a mut [u8],
    room_: usize,
}

impl TrProducerState for FixedRoomTx_<'_> {
    fn producer_state(&self) -> Option<(usize, bool)> {
        Option::Some((self.room_, false))
    }
}

impl<'a> TrBuffTryWrite<u8> for FixedRoomTx_<'a> {
    type SegmMut<'f>
        = <&'a mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <&'a mut [u8] as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        // 字段本身就是那个切片引用，借出的段结算写入量时把它往前推。
        self.inner_.try_write(demand)
    }
}

impl<'a> TrBuffWrite<u8> for FixedRoomTx_<'a> {
    type WriteAsync<'f>
        = ReadySegm<Self::SegmMut<'f>, Self::Err>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        // 底层是内存切片，借段立即就绪。
        ReadySegm::new(TrBuffTryWrite::try_write(self, demand))
    }
}

backend_async_test! {
    /// 测试分块搬运写出的线上字节就是「2 字节大端长度 + 内容」，并以长度 0 的块收尾。
    ///
    /// - 手段：以 `b"abc"` 为源、32 字节缓冲为目标，用 [`ChunkedWriter`] 一次搬完。
    /// - 判断：线上字节恰好是 `00 03 61 62 63 00 00`；返回值（线上字节数）是 7，即
    ///   2 字节前缀 + 3 字节内容 + 2 字节终止块。
    ///
    /// Verifies the chunked wire format: a 2-byte big-endian length followed by the
    /// payload, terminated by a zero-length chunk.
    /// - Means: pump `b"abc"` into a 32-byte buffer with [`ChunkedWriter`].
    /// - Criteria: the wire is exactly `00 03 61 62 63 00 00` and the returned wire length
    ///   is 7 (2 + 3 + 2).
    fn chunked_write_emits_expected_frame_() {
        let mut src: &[u8] = b"abc";
        let mut storage = [0u8; 32usize];
        // 借用期间不能同时读 `storage`，因此先把剩余长度与返回值带出来。
        let (rest, wire) = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedEncode::new(&mut src, &mut sink);
            let wire = writer.write_all_async().await.expect("搬运不该失败");
            (sink.len(), wire)
        };
        let written = storage.len() - rest;
        assert_eq!(wire, written, "返回的线上字节数应与实际写出的字节数一致");
        assert_eq!(
            &storage[..written],
            b"\x00\x03abc\x00\x00",
            "线上字节应当是长度前缀 + 内容 + 长度 0 的终止块"
        );
    }
}

backend_async_test! {
    /// 测试空体只写出终止块。
    ///
    /// - 手段：以空切片为源，用 [`ChunkedWriter`] 搬运。
    /// - 判断：线上只有 `00 00`——没有内容块，终止块就是整条体；返回的线上字节数是 2。
    ///
    /// Verifies that an empty body is nothing but the terminating chunk.
    /// - Means: pump an empty slice through [`ChunkedWriter`].
    /// - Criteria: the wire holds only `00 00` and the returned wire length is 2.
    fn chunked_write_empty_body_is_just_the_terminator_() {
        let mut src: &[u8] = b"";
        let mut storage = [0u8; 8usize];
        let (rest, wire) = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedEncode::new(&mut src, &mut sink);
            let wire = writer.write_all_async().await.expect("搬运不该失败");
            (sink.len(), wire)
        };
        let written = storage.len() - rest;
        assert_eq!(wire, 2usize, "空体只有终止块");
        assert_eq!(&storage[..written], b"\x00\x00", "空体就是长度 0 的块");
    }
}

backend_async_test! {
    /// 测试块长由目标报告的可用空间预先定下。
    ///
    /// - 手段：源是 5 字节，目标是 [`FixedRoomTx_`]（可用空间恒报 4 字节，于是每块最多
    ///   2 字节内容），用 [`ChunkedWriter`] 一次搬完。
    /// - 判断：线上是 `00 02 ab`、`00 02 cd`、`00 01 e`、`00 00`——块长依次为 2、2、1；
    ///   若搬运器不问目标的剩余空间、直接按源现有量封块，这里会得到一块 5 字节的内容。
    ///
    /// Verifies that the chunk size is decided up front from the room the destination
    /// reports.
    /// - Means: pump 5 bytes into `FixedRoomTx_`, which reports a constant room of 4 bytes
    ///   (so at most 2 payload bytes per chunk).
    /// - Criteria: the chunks carry 2, 2 and 1 payload bytes plus the terminator; a pump
    ///   that ignored the reported room would seal one 5-byte chunk instead.
    fn chunked_write_caps_chunk_by_destination_room_() {
        let mut src: &[u8] = b"abcde";
        let mut storage = [0u8; 16usize];
        let rest = {
            let mut tx = FixedRoomTx_ {
                inner_: storage.as_mut_slice(),
                room_: 4usize,
            };
            let mut writer = ChunkedEncode::new(&mut src, &mut tx);
            let wire = writer.write_all_async().await.expect("搬运不该失败");
            assert_eq!(wire, 13usize, "线上 = 三块前缀 + 5 字节内容 + 终止块");
            tx.inner_.len()
        };
        let written = storage.len() - rest;
        assert_eq!(
            &storage[..written],
            b"\x00\x02ab\x00\x02cd\x00\x01e\x00\x00",
            "每一块的内容都不超过目标报出的剩余空间"
        );
    }
}

backend_async_test! {
    /// 测试源是限长读口时，「额度用尽」就是「体到此为止」。
    ///
    /// - 手段：以 6 字节为底层源、额度 4 构造 [`LimitedReader`]，把它当作 [`ChunkedWriter`]
    ///   的源搬进 16 字节缓冲。
    /// - 判断：线上是 `00 04 abcd 00 00`——只搬了额度内的 4 字节并正常收尾；底层源还剩
    ///   `ef`，多出来的字节一个都没有被写出去。
    ///
    /// Verifies that a limited source delimits the body: the quota's end is the body's end.
    /// - Means: wrap `b"abcdef"` with a quota of 4 and pump it into a 16-byte buffer.
    /// - Criteria: the wire is `00 04 abcd 00 00` and the underlying source still holds
    ///   `ef` — not one byte beyond the quota is written.
    fn chunked_write_from_limited_source_stops_at_quota_() {
        let mut under: &[u8] = b"abcdef";
        let mut storage = [0u8; 16usize];
        let rest = {
            let mut src = LimitedReader::new(&mut under, 4usize);
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedEncode::new(&mut src, &mut sink);
            let wire = writer.write_all_async().await.expect("搬运不该失败");
            assert_eq!(wire, 8usize, "线上 = 前缀 2 + 内容 4 + 终止块 2");
            sink.len()
        };
        let written = storage.len() - rest;
        assert_eq!(
            &storage[..written],
            b"\x00\x04abcd\x00\x00",
            "只搬额度内的字节，并补上终止块"
        );
        assert_eq!(under, b"ef", "额度之外的字节一个都不该被搬走");
    }
}

backend_async_test! {
    /// 测试分块读把块边界对调用方隐藏：借出的段不跨越块边界，终止块就是流末。
    ///
    /// - 手段：造出 `00 02 ab 00 03 cde 00 00` 的线上字节，用 [`ChunkedRead`] 反复借段。
    /// - 判断：依次得到 `ab`、`cde` 两段（各自只覆盖一个块）；再读一次得到
    ///   [`ChunkedReadError::Ended`]，其错误标签是 `Closing`（等价于 EOF），且此后同步借段
    ///   同样报 `Ended`。
    ///
    /// Verifies that chunk boundaries are transparent to the consumer: a borrowed
    /// segment never crosses a boundary and the terminating chunk is the end of stream.
    /// - Means: build the wire bytes `00 02 ab 00 03 cde 00 00` and pull segments out of
    ///   [`ChunkedRead`].
    /// - Criteria: the chunks come out as `ab` then `cde`, and once the terminator is
    ///   reached the reader reports `Ended` (tag `Closing`, i.e. EOF).
    fn chunked_read_hides_chunk_boundaries_() {
        let wire: &[u8] = b"\x00\x02ab\x00\x03cde\x00\x00";
        let mut src: &[u8] = wire;
        let mut reader = ChunkedReader::new(&mut src);

        // 第一块：块边界对上层不可见，借出的段正好是这一块的内容 `ab`。
        assert_eq!(
            next_chunk_(&mut reader).await.as_deref(),
            Some(&b"ab"[..]),
            "第一段就是第一个块的内容"
        );
        // 第二块：跨块读取走异步路径，借出的段正好是 `cde`。
        assert_eq!(
            next_chunk_(&mut reader).await.as_deref(),
            Some(&b"cde"[..]),
            "第二段就是第二个块的内容"
        );

        // 终止块：此后再读就是流末。
        let ended = matches!(
            reader
                .read_async(&Demand::no_more_than(16usize))
                .await
                .pick_right(),
            Option::Some(ChunkedReadError::Ended)
        );
        assert!(ended, "读到终止块之后应当报 Ended");
        assert!(reader.is_ended(), "终止块之后应当记住已经结束");
        assert_eq!(reader.chunk_left(), 0usize, "终止块之后块内剩余为 0");

        let err = match reader.try_read(&Demand::no_more_than(16usize)).pick_right() {
            Option::Some(err) => err,
            Option::None => panic!("已经读到终止块，同步借段应当报 Ended"),
        };
        assert_eq!(err.err_tag(), ReadErrTag::Closing, "结束就是 EOF");
    }
}

backend_async_test! {
    /// 测试超过单块上限的内容会被切成多块。
    ///
    /// - 手段：以 [`K_MAX_CHUNK_PAYLOAD`] + 5 字节为源、足够大的缓冲为目标，一次搬完。
    /// - 判断：线上长度 = 内容长度 + 两个块前缀 + 终止块；第一块的前缀是单块上限的 2 字节
    ///   大端表示，第二块的前缀是剩下的 5，末尾是长度 0 的终止块。
    ///
    /// Verifies that payloads longer than the per-chunk limit are split automatically.
    /// - Means: pump `K_MAX_CHUNK_PAYLOAD + 5` bytes into a large enough buffer.
    /// - Criteria: the wire length accounts for two chunk prefixes plus the terminator, the
    ///   first prefix is the per-chunk limit itself, and the trailing 5 bytes belong to the
    ///   second chunk.
    fn chunked_write_splits_oversized_chunk_() {
        let payload = vec![0x5Au8; K_MAX_CHUNK_PAYLOAD + 5usize];
        let mut src: &[u8] = payload.as_slice();
        let mut storage = vec![0u8; payload.len() + 16usize];
        let (rest, wire) = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedEncode::new(&mut src, &mut sink);
            let wire = writer.write_all_async().await.expect("搬运不该失败");
            (sink.len(), wire)
        };
        let written = storage.len() - rest;
        assert_eq!(wire, written, "返回的线上字节数应与实际写出的字节数一致");
        assert_eq!(
            written,
            payload.len() + 2usize + 2usize + 2usize,
            "线上字节 = 内容 + 两个块前缀 + 终止块"
        );
        // 第一块的前缀是单块上限本身的 2 字节大端表示。
        assert_eq!(&storage[..2], &(K_MAX_CHUNK_PAYLOAD as u16).to_be_bytes());
        // 第二块紧跟在第一块内容之后，前缀是剩下的 5 个字节。
        let second = 2usize + K_MAX_CHUNK_PAYLOAD;
        assert_eq!(
            &storage[second..second + 2],
            &[0u8, 5u8],
            "第二个块承载剩下的 5 个字节"
        );
        assert_eq!(
            &storage[written - 2..written],
            &[0u8, 0u8],
            "末尾是长度 0 的终止块"
        );
    }
}

backend_async_test! {
    /// 测试写侧搬出的分块体，能被读侧原样读回。
    ///
    /// - 手段：把 [`K_MAX_CHUNK_PAYLOAD`] + 9 字节的内容用 [`ChunkedWriter`] 搬进一块足够大
    ///   的缓冲，再把线上字节交给 [`ChunkedRead`] 反复借段、逐段搬出。
    /// - 判断：逐段拼起来的字节与源逐字节相同，且最后读到终止块（`is_ended()`）。
    ///
    /// Verifies a round trip: whatever the writer frames, the reader reassembles byte for
    /// byte.
    /// - Means: pump `K_MAX_CHUNK_PAYLOAD + 9` bytes through [`ChunkedWriter`], then pull the
    ///   payload back out of [`ChunkedRead`] segment by segment.
    /// - Criteria: the reassembled bytes equal the source, and the reader finally reports
    ///   `Ended`.
    fn chunked_write_then_read_roundtrip_() {
        let payload: Vec<u8> = (0..K_MAX_CHUNK_PAYLOAD + 9usize)
            .map(|idx| (idx % 251usize) as u8)
            .collect();
        let mut src: &[u8] = payload.as_slice();
        let mut storage = vec![0u8; payload.len() + 16usize];
        let rest = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedEncode::new(&mut src, &mut sink);
            writer.write_all_async().await.expect("搬运不该失败");
            sink.len()
        };
        let written = storage.len() - rest;

        let wire: &[u8] = &storage[..written];
        let mut rx: &[u8] = wire;
        let mut reader = ChunkedReader::new(&mut rx);
        let mut got: Vec<u8> = Vec::new();
        while let Option::Some(part) = next_chunk_(&mut reader).await {
            assert!(!part.is_empty(), "内容段不该是空的");
            got.extend_from_slice(&part);
        }
        assert_eq!(got, payload, "读回的字节应当与源逐字节相同");
        assert!(reader.is_ended(), "读到终止块之后应当记住已经结束");
    }
}
