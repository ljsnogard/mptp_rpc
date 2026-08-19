//! 把 `buffex::RingBuffer` 当作黑箱，验证它在 IrohChannel 使用场景下的行为。
//!
//! 背景：IrohChannel 的每个方向使用一个 RingBuffer，后台 pump 从一端读、
//! 另一端写。之前使用 `spawn_local` 时，并发双 channel 出现其中一个发送泵
//! 没有观察到 `tx.close()` 的迹象。本测试不经过 iroh，直接模拟“写端写入后
//! close，读端应读到全部数据并退出”的行为，并且同时跑多对，检查 RingBuffer
//! 本身是否在并发/关闭语义上有问题。

use std::{mem::MaybeUninit, sync::Arc, time::Duration};

use mptp_rpc_core::x_deps::buffex::{
    ring_buffer::{RingBuffer, RingRx, RingTx},
    x_deps::abs_buff::Demand,
};
use tokio::time::timeout;

type Ring = RingBuffer<Box<[u8]>>;
type Tx = RingTx<Arc<Ring>, Box<[u8]>>;
type Rx = RingRx<Arc<Ring>, Box<[u8]>>;

const RING_CAP: usize = 64 * 1024;
const CHUNK: usize = 32 * 1024;

fn new_pair() -> (Tx, Rx) {
    let ring = Arc::new(
        RingBuffer::try_new(Box::from(vec![0u8; RING_CAP])).expect("ring"),
    );
    RingBuffer::try_split_shared(ring, Arc::strong_count, Arc::weak_count)
        .expect("split")
}

async fn write_all(tx: &mut Tx, data: &[u8]) {
    let mut data = data;
    while !data.is_empty() {
        let mut segm = tx
            .write_async(&Demand::less_than(data.len()))
            .await
            .pick_left()
            .expect("write segm");
        while !segm.is_empty() && !data.is_empty() {
            let mut child = segm.as_segm_mut();
            let take = child.least_count().min(data.len());
            let moved = child.clone_items_from_buff(&data[..take]);
            assert_eq!(moved, take);
            drop(child);
            data = &data[take..];
        }
        drop(segm);
    }
    tx.close();
}

async fn read_all(rx: &mut Rx) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        if rx.ring().data_size() == 0 && rx.ring().is_tx_closed() {
            break;
        }
        let mut segm = rx
            .read_async(&Demand::less_than(CHUNK))
            .await
            .pick_left()
            .expect("read segm");
        let len = segm.least_count();
        let mut tmp: Vec<MaybeUninit<u8>> = (0..len).map(|_| MaybeUninit::uninit()).collect();
        let moved = unsafe { segm.move_items_to_buff(&mut tmp) };
        out.extend(tmp[..moved].iter().map(|m| unsafe { m.assume_init_read() }));
        drop(segm);
    }
    out
}

/// 单对 RingBuffer：写入后 close，读端应完整读回并退出。
#[tokio::test(flavor = "multi_thread")]
async fn single_ring_close_propagates() {
    let (mut tx, mut rx) = new_pair();
    let payload: Vec<u8> = (0..100_000u32).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let result = timeout(Duration::from_secs(5), async {
        let writer = tokio::spawn(async move { write_all(&mut tx, &payload).await });
        let reader = tokio::spawn(async move { read_all(&mut rx).await });
        let (_, got) = tokio::join!(writer, reader);
        got.expect("reader task")
    })
    .await
    .expect("single ring should finish within timeout");

    assert_eq!(result, expected);
}

/// 多对 RingBuffer 并发：模拟 IrohChannel 同时存在多条 channel 的场景。
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_rings_close_propagates() {
    let payloads: Vec<Vec<u8>> = vec![
        (0..50_000u32).map(|i| (i % 13) as u8).collect(),
        (0..80_000u32).map(|i| (i % 7) as u8).collect(),
    ];

    let result = timeout(Duration::from_secs(5), async {
        let mut handles = Vec::new();
        for payload in payloads {
            let (mut tx, mut rx) = new_pair();
            handles.push(tokio::spawn(async move {
                let writer = tokio::spawn(async move { write_all(&mut tx, &payload).await });
                let reader = tokio::spawn(async move { read_all(&mut rx).await });
                let (_, got) = tokio::join!(writer, reader);
                got.expect("reader task")
            }));
        }

        let mut outputs = Vec::new();
        for h in handles {
            outputs.push(h.await.expect("task"));
        }
        outputs
    })
    .await
    .expect("concurrent rings should finish within timeout");

    assert_eq!(result[0], (0..50_000u32).map(|i| (i % 13) as u8).collect::<Vec<_>>());
    assert_eq!(result[1], (0..80_000u32).map(|i| (i % 7) as u8).collect::<Vec<_>>());
}

/// 使用 LocalSet + spawn_local 同时跑多对 RingBuffer，验证关闭传播是否受
/// LocalSet 单线程调度影响。
#[tokio::test(flavor = "current_thread")]
async fn concurrent_rings_close_propagates_in_local_set() {
    let payloads: Vec<Vec<u8>> = vec![
        (0..50_000u32).map(|i| (i % 13) as u8).collect(),
        (0..80_000u32).map(|i| (i % 7) as u8).collect(),
    ];

    let result = timeout(Duration::from_secs(5), async {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let mut handles = Vec::new();
                for payload in payloads {
                    let (mut tx, mut rx) = new_pair();
                    handles.push(local.spawn_local(async move {
                        let writer = tokio::task::spawn_local(async move {
                            write_all(&mut tx, &payload).await;
                        });
                        let reader = tokio::task::spawn_local(async move {
                            read_all(&mut rx).await
                        });
                        let (_, got) = tokio::join!(writer, reader);
                        got.expect("reader task")
                    }));
                }

                let mut outputs = Vec::new();
                for h in handles {
                    outputs.push(h.await.expect("task"));
                }
                outputs
            })
            .await
    })
    .await
    .expect("concurrent rings in LocalSet should finish within timeout");

    assert_eq!(result[0], (0..50_000u32).map(|i| (i % 13) as u8).collect::<Vec<_>>());
    assert_eq!(result[1], (0..80_000u32).map(|i| (i % 7) as u8).collect::<Vec<_>>());
}
