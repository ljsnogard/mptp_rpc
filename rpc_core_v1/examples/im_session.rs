//! 一个 IM 会话中的 `rpc_core_v1` 标准用法。
//!
//! # 场景
//!
//! 这条连接的一端是 IM 客户端 A，另一端是 `im_server`。一条 MPTP 子流就是一次
//! **session**：请求前缀（方法 + 路径 + 类型头）→ 请求体 / 请求后缀 → 服务端处理 →
//! 响应前缀 → 响应体 / 响应后缀。本示例围绕 IM 的 `im/v1`：
//!
//! 1. A `Call im/v1/session/whoami` → 服务端回 `Option<User>`；
//! 2. A `Pull im/v1/room/events/42` → 服务端先写 `RoomEvent::Snapshot`，随后写
//!    `RoomEvent::MessagePosted`；A 每 `take` 一条，服务端才允许看到下一条；
//! 3. A `Push im/v1/room/messages/42`，把 2 条补发消息推给服务端；A 半关闭后服务端
//!    才能回 200；
//! 4. 类型头缺失 / 不匹配时，服务端在进 handler 前回 400 / 406。
//!
//! # 这个示例做什么
//!
//! 它不启动 TCP。`main` 用一段内存字节流模拟「同一个 session 的两端」：服务端一侧
//! 写 typed slot，客户端一侧读 typed just，并打印每一步之后对端应看到的反应。真实
//! 网络路径只是把 `SliceSink` / `&[u8]` 换成 `SuffixStream` / `ChannelRx`，调用形状
//! 不变。
//!
//! 运行：
//!
//! ```text
//! cargo run -p rpc_core_v1 --example im_session
//! ```
//!
#![feature(impl_trait_in_assoc_type)]

//! # `im_session`: standard `rpc_core_v1` usage over one IM session
//!
//! The example simulates both sides of one MPTP session in memory. It shows unary calls,
//! typed Pull/Push fluxes, type headers, finite Push and cancellation; a real transport
//! only swaps the in-memory reader/writer for `mptp_smux` handles.

use abs_async_iter::{TrAsyncIterator, TrFlux};
use abs_buff::{
    Demand, TrBuffFlush, TrBuffTryFlush, TrBuffTryWrite, TrBuffWrite, x_deps::anylr::SomeOf,
};
use abs_cancel::{CancelledToken, TrMayCancel};
use abs_mptp::spec::access_method::AccessMethod;
use denco_macro::{TrDecodable, TrEncodable};
use futures::executor::block_on;
use mptp_smux::Headers;
use rpc_core_v1::{
    client::{ClientProxy, TrRpcClientConfig, call_unary_async},
    messaging::{RpcResult, TrRpcInterface},
    stream::{
        AcceptType, BodyCodec, DataTypeId, RpcDataTypeInfo, StreamError, StreamFuture, TrDataTypeId,
        TrSuffixFinish, TrSuffixWrite, TypedJustFlux, TypedSlotFlux, apply_request_type_headers,
        validate_request_types, write_accept,
    },
    x_deps::{abs_async_iter, abs_buff, abs_cancel, abs_mptp, mptp_smux},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// IM 协议的最小 DTO（与 mptp_im_demo/DESIGN.md §4.4 同名同义，字段做了裁剪）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个用户。
#[derive(Clone, Debug, PartialEq, TrDecodable, TrEncodable)]
struct User {
    /// 服务端分配的用户 ID。
    id: u64,

    /// 显示名。
    display_name: String,
}

impl TrDataTypeId for User {
    const DATA_TYPE_ID: DataTypeId = DataTypeId::new(0x2001u64).unwrap();
}

/// 一条聊天消息。
#[derive(Clone, Debug, PartialEq, TrDecodable, TrEncodable)]
struct ChatMessage {
    /// 房间 ID。
    room_id: u64,

    /// 房间内单调序号（字段名不用 `seq`：denco 派生内部会占用同名局部变量）。
    sequence: u64,

    /// 文本。
    text: String,
}

impl TrDataTypeId for ChatMessage {
    const DATA_TYPE_ID: DataTypeId = DataTypeId::new(0x2002u64).unwrap();
}

/// 房间事件流的一项。
#[derive(Clone, Debug, PartialEq, TrDecodable, TrEncodable)]
enum RoomEvent {
    /// 订阅时的快照。
    Snapshot {
        /// 快照覆盖到的最后序号。
        last_seq: u64,
    },

    /// 一条新消息。
    MessagePosted {
        /// 新消息。
        message: ChatMessage,
    },
}

impl TrDataTypeId for RoomEvent {
    const DATA_TYPE_ID: DataTypeId = DataTypeId::new(0x2003u64).unwrap();
}

/// `im/v1` interface marker（真实项目里由 `#[rpc(namespace = "im/v1", ...)]` 生成）。
struct ImV1;

impl TrRpcInterface for ImV1 {
    const PATH: &'static str = "im/v1";
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 内存写端：真实项目里这里是 `SuffixStream` / `RequestSuffixStream`
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 固定切片上的写端；直接复用底层 `&mut [u8]` 的 `TrBuffWrite`。
struct SliceSink<'a> {
    /// 目标切片。
    buf_: &'a mut [u8],

    /// 初始长度。
    init_: usize,
}

impl<'a> SliceSink<'a> {
    /// 构造。
    fn new(buf: &'a mut [u8]) -> Self {
        let init_ = buf.len();
        SliceSink { buf_: buf, init_ }
    }

    /// 已写入长度。
    fn used(&self) -> usize {
        self.init_ - self.buf_.len()
    }
}

/// [`SliceSink`] 的写视图。
struct SliceSinkView<'b, 'a> {
    /// 指向切片变量的可变引用。
    buf_: &'b mut &'a mut [u8],
}

impl<'b, 'a> TrBuffTryWrite<u8> for SliceSinkView<'b, 'a> {
    type SegmMut<'f>
        = <&'a mut [u8] as TrBuffTryWrite<u8>>::SegmMut<'f>
    where
        Self: 'f;

    type Err = <&'a mut [u8] as TrBuffTryWrite<u8>>::Err;

    fn try_write<'f>(
        &'f mut self,
        demand: &'f Demand<usize>,
    ) -> SomeOf<Self::SegmMut<'f>, Self::Err> {
        <&'a mut [u8] as TrBuffTryWrite<u8>>::try_write(self.buf_, demand)
    }
}

impl<'b, 'a> TrBuffWrite<u8> for SliceSinkView<'b, 'a> {
    type WriteAsync<'f>
        = <&'a mut [u8] as TrBuffWrite<u8>>::WriteAsync<'f>
    where
        Self: 'f;

    fn write_async<'f>(&'f mut self, demand: &'f Demand<usize>) -> Self::WriteAsync<'f> {
        <&'a mut [u8] as TrBuffWrite<u8>>::write_async(self.buf_, demand)
    }
}

impl<'b, 'a> TrBuffTryFlush<u8> for SliceSinkView<'b, 'a> {
    fn try_flush(&mut self) -> Result<(), Self::Err> {
        <&'a mut [u8] as TrBuffTryFlush<u8>>::try_flush(self.buf_)
    }

    fn close(&mut self) -> Result<(), Self::Err> {
        <&'a mut [u8] as TrBuffTryFlush<u8>>::close(self.buf_)
    }
}

impl<'b, 'a> TrBuffFlush<u8> for SliceSinkView<'b, 'a> {
    type FlushAsync<'f>
        = <&'a mut [u8] as TrBuffFlush<u8>>::FlushAsync<'f>
    where
        Self: 'f;

    type CloseAsync<'f>
        = <&'a mut [u8] as TrBuffFlush<u8>>::CloseAsync<'f>
    where
        Self: 'f;

    fn flush_async(&mut self) -> Self::FlushAsync<'_> {
        <&'a mut [u8] as TrBuffFlush<u8>>::flush_async(self.buf_)
    }

    fn close_async(&mut self) -> Self::CloseAsync<'_> {
        <&'a mut [u8] as TrBuffFlush<u8>>::close_async(self.buf_)
    }
}

impl<'a> TrSuffixWrite for SliceSink<'a> {
    type Writer<'b>
        = SliceSinkView<'b, 'a>
    where
        Self: 'b;

    fn writer_mut(&mut self) -> Result<Self::Writer<'_>, StreamError> {
        Ok(SliceSinkView {
            buf_: &mut self.buf_,
        })
    }
}

impl<'a> TrSuffixFinish<'a> for SliceSink<'a> {
    type FinishOutput = usize;

    fn finish_async(self) -> StreamFuture<'a, usize> {
        let used = self.used();
        StreamFuture::new(async move { Ok(used) })
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 1：Pull 房间事件
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 演示 `Pull im/v1/room/events/42`：服务端写 item，客户端逐项 take。
fn pull_room_events_() {
    println!("\n[Pull] A订阅 im/v1/room/events/42");
    let item_id = RoomEvent::DATA_TYPE_ID;
    let info = RpcDataTypeInfo::new(BodyCodec::MsgPack)
        .with_response_suffix(item_id)
        .with_accept(AcceptType::new(BodyCodec::MsgPack, Some(item_id)));

    // 客户端请求前缀：Pull 没有 body，只声明 suffix codec + Accept。
    let mut request_headers = Headers::new();
    apply_request_type_headers(&mut request_headers, AccessMethod::Pull, &info);
    write_accept(
        &mut request_headers,
        AcceptType::new(BodyCodec::MsgPack, Some(item_id)),
    );
    println!("  A → 请求头: Body_Type=MsgPack, Accept=types(RoomEvent)");
    println!(
        "  server 校验请求头: {:?}（进入 handler 前）",
        validate_request_types(AccessMethod::Pull, &request_headers, &info)
    );

    // 服务端：写出 Snapshot + MessagePosted。
    let mut storage = [0u8; 4096usize];
    {
        let sink = SliceSink::new(&mut storage);
        let mut flux = TypedSlotFlux::<_, RoomEvent>::new(sink);
        let mut slots = block_on(flux.pull_async());
        let snapshot = RoomEvent::Snapshot { last_seq: 7u64 };
        let message = RoomEvent::MessagePosted {
            message: ChatMessage {
                room_id: 42u64,
                sequence: 8u64,
                text: "hello from B".to_string(),
            },
        };
        block_on(
            block_on(slots.next_async())
                .expect("槽")
                .set_async(snapshot),
        )
        .expect("写 Snapshot");
        block_on(block_on(slots.next_async()).expect("槽").set_async(message))
            .expect("写 MessagePosted");
        let used = block_on(slots.finish_async()).expect("finish Pull suffix");
        println!("  server → 写出 2 个 RoomEvent，共 {used} 字节；随后半关闭 suffix");
        drop(flux);

        // 客户端：同一个 session 的另一端读取。
        let mut flux = TypedJustFlux::<_, RoomEvent>::new(&storage[..used]);
        let mut items = block_on(flux.pull_async());
        let first = block_on(items.next_async()).expect("第一个 item");
        println!("  A ← {:?}", first.take());
        let second = block_on(items.next_async()).expect("第二个 item");
        println!("  A ← {:?}", second.take());
        println!(
            "  A 再读 → {:?}（服务端已半关闭；客户端看到流结束）",
            block_on(items.next_async()).unwrap_err()
        );
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 2：有限 Push 补发消息
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 演示 `Push im/v1/room/messages/42`：客户端逐槽写，半关闭后服务端读取并回复。
fn push_messages_() {
    println!("\n[Push] A 补发 2 条消息到 im/v1/room/messages/42");
    let info = RpcDataTypeInfo::new(BodyCodec::MsgPack)
        .with_request_suffix(ChatMessage::DATA_TYPE_ID)
        .with_accept(AcceptType::new(BodyCodec::MsgPack, None));
    let mut headers = Headers::new();
    apply_request_type_headers(&mut headers, AccessMethod::Push, &info);
    println!("  A → 请求头: Body_Type=MsgPack, Data_Type_Id=0x2002");
    println!(
        "  server 校验 Push 请求头: {:?}",
        validate_request_types(AccessMethod::Push, &headers, &info)
    );

    let mut storage = [0u8; 4096usize];
    let used = {
        let sink = SliceSink::new(&mut storage);
        let mut flux = TypedSlotFlux::<_, ChatMessage>::new(sink);
        let mut slots = block_on(flux.pull_async());
        for (seq, text) in [(9u64, "backfill 1"), (10u64, "backfill 2")] {
            let msg = ChatMessage {
                room_id: 42u64,
                sequence: seq,
                text: text.to_string(),
            };
            block_on(block_on(slots.next_async()).expect("槽").set_async(msg))
                .expect("写 Push item");
        }
        let used = block_on(slots.finish_async()).expect("finish Push");
        println!("  A → 写完 2 条后 finish：请求后缀半关闭；开始等回复");
        drop(flux);
        used
    };

    // 服务端：同一个 session 的另一端读取 Push suffix。
    let mut flux = TypedJustFlux::<_, ChatMessage>::new(&storage[..used]);
    let mut items = block_on(flux.pull_async());
    let mut count = 0usize;
    loop {
        match block_on(items.next_async()) {
            Ok(just) => {
                println!("  server ← 收到 {:?}", just.take());
                count += 1usize;
            }
            Err(StreamError::Decode(_)) => break,
            Err(err) => panic!("Push suffix 读取失败：{err}"),
        }
    }
    println!("  server → 读尽 {count} 条后回 200；A 的 finish_async 才会返回 Response");
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 3：类型头不匹配 / 取消
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 演示类型头不匹配：服务端在进 handler 前回 406。
fn type_mismatch_() {
    println!("\n[Type] A 用错误的 Data_Type_Id 发 Push");
    let info =
        RpcDataTypeInfo::new(BodyCodec::MsgPack).with_request_suffix(ChatMessage::DATA_TYPE_ID);
    let mut headers = Headers::new();
    rpc_core_v1::stream::write_body_codec(&mut headers, BodyCodec::MsgPack);
    rpc_core_v1::stream::write_data_type_id(
        &mut headers,
        DataTypeId::new(0x9999u64).expect("非零 ID"),
    );
    let rejection = validate_request_types(AccessMethod::Push, &headers, &info).unwrap_err();
    println!(
        "  server → {rejection}（状态 {}）",
        rejection.status().inner()
    );
    println!("  A ← 收到该状态；handler 从未运行");
}

/// 演示取消：调用方在流操作等待期间取消。
fn cancellation_() {
    println!("\n[Cancel] A 取消一个尚未完成的流操作");
    let fut = StreamFuture::new(async { core::future::pending::<Result<(), StreamError>>().await })
        .may_cancel_with(CancelledToken::new());
    match block_on(fut) {
        Err(StreamError::Cancelled) => println!("  A → 立即得到 StreamError::Cancelled"),
        other => panic!("期待 Cancelled，实际 {other:?}"),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 场景 4：unary Call（编译期示例；真实调用需要 MptpClient）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `Call im/v1/session/whoami` 的客户端写法。
///
/// 真实运行时 `proxy` 来自 `client.call::<ImV1>()`；这里只做类型检查，避免示例
/// 依赖一个正在运行的 `im_server`。
#[allow(dead_code)]
async fn whoami_async_<C>(proxy: &ClientProxy<'_, C, ImV1>) -> RpcResult<Option<User>>
where
    C: TrRpcClientConfig + 'static,
{
    call_unary_async(proxy, "session/whoami", ()).await
}

/// `Call im/v1/session/set_display_name` 的请求 / 响应类型头。
///
/// 真实实现把这两个头写进请求前缀与响应前缀；服务端用
/// `validate_request_types` 检查请求，客户端用 `validate_response_types` 检查响应。
fn unary_headers_() {
    println!("\n[Call] A 修改显示名 im/v1/session/set_display_name");
    let request = RpcDataTypeInfo::new(BodyCodec::MsgPack).with_request_body(User::DATA_TYPE_ID);
    let response = RpcDataTypeInfo::new(BodyCodec::MsgPack).with_response_body(User::DATA_TYPE_ID);
    let mut headers = Headers::new();
    apply_request_type_headers(&mut headers, AccessMethod::Call, &request);
    write_accept(
        &mut headers,
        AcceptType::new(BodyCodec::MsgPack, Some(User::DATA_TYPE_ID)),
    );
    println!("  A → Data_Type_Id=0x2001（请求体是 User），Accept=0x2001");
    println!(
        "  server 校验 → {:?}；handler 写 200 + User(0x2001)",
        validate_request_types(AccessMethod::Call, &headers, &request)
    );
    let mut resp_headers = Headers::new();
    rpc_core_v1::stream::apply_response_type_headers(
        &mut resp_headers,
        AccessMethod::Call,
        &response,
    );
    println!(
        "  A 校验响应头 → {:?}",
        rpc_core_v1::stream::validate_response_types(AccessMethod::Call, &resp_headers, &response)
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 入口：打印一条 IM session 中的动作与对端反应。
fn main() {
    println!("rpc_core_v1::im_session —— 一个 IM session 的标准用法");
    println!("（用内存字节流模拟同一 session 的两端，不启动 TCP）");
    pull_room_events_();
    push_messages_();
    type_mismatch_();
    cancellation_();
    unary_headers_();
    println!("\n真实网络路径：把 SliceSink 换成 SuffixStream / RequestSuffixStream，");
    println!("把 &[u8] 换成 ChannelRx / RequestSuffixRead，调用形状不变。");
    println!("Pull / Push 的 writer 都直接使用底层自带的 buff，调用者无需准备 Vec。");
}
