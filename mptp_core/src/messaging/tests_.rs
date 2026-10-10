//! 协议层的单元测试：报文前缀、报文体，以及回复体的读取决策。
//!
//! # 为什么这些用例要跑在后端运行时里
//!
//! 报文 IO 走 `AsStdRead` / `AsStdWrite`，它们把 ring 直接暴露成 `std::io::Read` /
//! `Write`，数据没到时靠 `TrLocalScope::block_on_local` 同步等待——而后者要问门面取
//! **当前后端**的运行时值。这是「直接读写 ring」这条路线的固有限制，调用点必须满足它：
//! 连接交给宿主线程驱动，用例进入后端上下文。绕开它（换成自建缓冲）就等于把 ring 白用。
//!
//! 收发半边在这里用 `&mut [u8]` / `&[u8]` 充当：它们的段接口立即就绪，于是这些用例
//! 只覆盖协议行为，不牵扯真实传输。

use std::io::Write;

use abs_buff::x_deps::abs_cancel;
use abs_cancel::NonCancellableToken;

use abs_buff_stdio_adapt::{AsStdRead, AsStdWrite};

use super::{
    MessageIoError, Nothing, ProtocolViolation, Request, RespPrefix, Response,
    ResponseBodyDecision, TrRpcBody, TrRpcRequest,
    basic::EncodedBody,
    body::{
        BodyTransfer, body_reader, body_transfer_of, chunked_transfer_header_val, send_body_async,
    },
    chunked::ChunkedWrite,
    limit::{LimitedRead, LimitedWrite},
    request::{RequestBuildError, RequestBuilder, recv_request_prefix_async, send_request_async},
    response::{recv_response_body_async, recv_response_prefix_async, send_response_async},
};
use crate::{
    access_method::AccessMethod,
    client::HeadersBuilder,
    specs::{HeaderVal, Status, StdHeaderKey, StdHeaderVal},
};

/// 统计**当前线程**堆分配**字节数**的全局分配器。
///
/// 它只服务于一件事：给「IO 路径上不允许用 `Vec` 之类绕开 ring」这条纪律上回归保护。
///
/// 为什么记字节数而不是次数：底层适配器本身在个别后端下会有固定的小额分配（例如
/// tokio 的 `LocalSet::run_until` 每次会包一层任务），次数对不上但量级很小；而「把整条
/// 报文攒进一块缓冲」这类违规的分配量必然与报文大小同阶。用一条几 KiB 的报文做差分，
/// 两者就区分得干干净净。
///
/// 计数器是 thread-local 的，因此并行跑的其它用例不会干扰；`const` 初始化保证首次访问
/// 计数本身不产生分配，也就不会在分配器里递归。
mod alloc_probe_ {
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        cell::Cell,
    };

    thread_local! {
        static COUNT_: Cell<usize> = const { Cell::new(0) };
    }

    /// 转发到系统分配器，只顺手记一笔账。
    pub(super) struct CountingAlloc;

    // SAFETY: 每个方法都原样转发给 `System`，不改变任何分配语义；`COUNT_` 的读改写
    // 只碰一个 thread-local 的 `Cell<usize>`，不分配、不阻塞、不与分配器重入。
    unsafe impl GlobalAlloc for CountingAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            COUNT_.with(|c| c.set(c.get() + layout.size()));
            unsafe { System.alloc(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            COUNT_.with(|c| c.set(c.get() + new_size));
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    /// 当前线程至今累计分配到的字节数。
    pub(super) fn count_() -> usize {
        COUNT_.with(|c| c.get())
    }

    /// 把当前线程的计数清零。
    pub(super) fn reset_() {
        COUNT_.with(|c| c.set(0usize));
    }
}

#[global_allocator]
static ALLOC_PROBE_: alloc_probe_::CountingAlloc = alloc_probe_::CountingAlloc;

/// 内存缓冲的容量：远大于任何一条被测报文。
const K_BUFFER: usize = 4096usize;

/// 在当前后端对应的运行时上下文里定义一个用例。
///
/// workspace 的默认后端由 feature 并集决定：只有 `rt-compio` 时是 compio，一旦有成员
/// 点亮 `rt-tokio`（demo 就是），全图默认后端变成 tokio。用例本身与后端无关，只有
/// 「怎么进上下文」这一句要跟着换。
#[cfg(feature = "rt-tokio")]
macro_rules! backend_async_test {
    ($(#[$meta:meta])* fn $name:ident() $body:block) => {
        $(#[$meta])*
        #[tokio::test(flavor = "multi_thread", worker_threads = 1usize)]
        async fn $name() $body
    };
}

#[cfg(all(feature = "rt-compio", not(feature = "rt-tokio")))]
macro_rules! backend_async_test {
    ($(#[$meta:meta])* fn $name:ident() $body:block) => {
        $(#[$meta])*
        #[compio::test]
        async fn $name() $body
    };
}

/// 把一次「写报文」操作落进内存缓冲，返回写出的那些字节。
macro_rules! write_to_vec {
    ($op:expr) => {{
        let mut storage = vec![0u8; K_BUFFER];
        let written = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            $op(&mut sink).await.expect("写进内存缓冲不应当失败")
        };
        assert!(written <= storage.len(), "写出的字节不应当超过缓冲容量");
        storage.truncate(written);
        storage
    }};
}

/// 造一个只带 `Body_Size` 的回复前缀。
fn prefix_with_size_(status: Status, size: Option<usize>) -> RespPrefix {
    let size = match size {
        Option::Some(size) => size,
        Option::None => return RespPrefix(status, Option::None),
    };
    let headers = HeadersBuilder::new().set_body_size(size).build();
    RespPrefix(status, Option::Some(headers))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

backend_async_test! {
    /// 测试无 body 的请求能原样往返，且读完前缀后紧跟的字节一个都没被吃掉。
    /// - 手段：构造一条 `View /hello`（体是 [`Nothing`]）的请求，写进内存缓冲，再手工追加
    ///   一个哨兵字节 `0xAB`；随后从这段字节里解出请求前缀。
    /// - 判断：解出的 method / location 与构造时一致；`Body_Size` 头不存在；且**读源切片
    ///   推进到只剩哨兵那一字节**——这正是「没有 body 就不多读一个字节」的直接证据
    ///   （这里没有预读缓冲，多读的字节无处可藏）。
    fn request_prefix_without_body_roundtrips_() {
        let req: Request<Nothing, Nothing> = Request::new(AccessMethod::View, "/hello");
        let mut wire = write_to_vec!(|tx| send_request_async(
            &req,
            tx,
            NonCancellableToken::new()
        ));
        // 前缀之后追加哨兵：它代表「同一条 channel 上的后续数据」。
        wire.push(0xABu8);

        let mut src: &[u8] = wire.as_slice();
        let prefix = recv_request_prefix_async(&mut src, NonCancellableToken::new())
            .await
            .expect("前缀应当解得出来");

        assert_eq!(prefix.0, AccessMethod::View, "method 应当原样往返");
        assert_eq!(prefix.1, "/hello", "location 应当原样往返");
        assert_eq!(
            super::io_::try_get_body_size_(prefix.2.as_ref()).expect("头应当可解"),
            0usize,
            "不该有体"
        );
        assert_eq!(src, &[0xABu8], "哨兵字节不应当被前缀读取吃掉");
    }
}

backend_async_test! {
    /// 测试带 body 的请求能原样往返。
    /// - 手段：构造一条 `Call /rpc/echo`，体是 MessagePack 编出来的字符串；用
    ///   [`Request::with_measured_body`] 自动写好 `Body_Size` 头，写出去再解回来，
    ///   随后按头声明的长度直接解出体。
    /// - 判断：解码出的前缀字段与构造时一致；读出 String 等于原来那个；且**读源被恰好
    ///   耗尽**（体之后没有多余字节）。
    fn request_with_body_roundtrips_() {
        // 发送路径要求体的字节**现成可借出**，因此这里先编成 `EncodedBody`：
        // MessagePack 的 "hello" 是 fixstr 头 1 字节 + 正文 5 字节。
        let req = Request::<EncodedBody, Nothing>::with_measured_body(
            AccessMethod::Call,
            "/rpc/echo",
            EncodedBody::new(rmp_serde::to_vec("hello").expect("编码不该失败")),
        )
        .expect("量长度不该失败");
        let wire = write_to_vec!(|tx| send_request_async(
            &req,
            tx,
            NonCancellableToken::new()
        ));

        let mut src: &[u8] = wire.as_slice();
        let prefix = recv_request_prefix_async(&mut src, NonCancellableToken::new())
            .await
            .expect("前缀应当解得出来");
        assert_eq!(prefix.0, AccessMethod::Call, "method 应当原样往返");
        assert_eq!(prefix.1, "/rpc/echo", "location 应当原样往返");

        let body: Option<String> =
            super::recv_request_body_async(&mut src, prefix.2.as_ref(), NonCancellableToken::new())
                .await
                .expect("体应当解得出来");
        assert_eq!(body, Some("hello".to_string()), "体应当与写出去的一致");
        assert!(src.is_empty(), "读完体之后不应当还有剩余字节");
    }
}

backend_async_test! {
    /// 测试带体的回复能原样往返，客户端侧按前缀读体。
    /// - 手段：构造一条 `Status::Ok` + 体的回复（体长度由 `with_measured_body` 量好），
    ///   写出去后从字节里先解前缀，交给 [`ResponseBodyDecision`] 判边界，再按判决读体。
    /// - 判断：状态码一致；决策给出 `Present(5)`；读出的字符串与写出去的一致。
    fn response_with_body_roundtrips_() {
        let resp = Response::<EncodedBody, Nothing>::with_measured_body(
            Status::Ok,
            EncodedBody::new(rmp_serde::to_vec("resp").expect("编码不该失败")),
        )
        .expect("量长度不该失败");
        let wire = write_to_vec!(|tx| send_response_async(
            &resp,
            tx,
            NonCancellableToken::new()
        ));

        let mut src: &[u8] = wire.as_slice();
        let prefix = recv_response_prefix_async(&mut src, NonCancellableToken::new())
            .await
            .expect("回复前缀应当解得出来");
        assert_eq!(prefix.status(), Status::Ok, "状态码应当原样往返");

        let decision = ResponseBodyDecision::decide(AccessMethod::Call, &prefix)
            .expect("Call 的回复带体是合法的");
        assert_eq!(
            decision,
            ResponseBodyDecision::Present(5usize),
            "MessagePack 的 \"resp\" 是 fixstr 头 1 字节加正文 4 字节"
        );
        let body: Option<String> =
            recv_response_body_async(&mut src, &prefix, NonCancellableToken::new())
                .await
                .expect("体应当解得出来");
        assert_eq!(body, Some("resp".to_string()), "体应当与写出去的一致");
    }
}

backend_async_test! {
    /// 测试「没有回复体」时一个字节都不读，后续数据保持完整。
    /// - 手段：造一条只有 `Status::Ok`、没有任何 body 头的回复，写出去后手工追加哨兵；
    ///   先解前缀，再让决策判一次，然后按其结论（0 字节）走读体路径。
    /// - 判断：决策必须是 `Absent`；读体返回 `None`；且**读源推进到只剩哨兵**。
    fn absent_response_body_consumes_nothing_() {
        let resp = Response::<Nothing, Nothing>::new(Status::Ok);
        let mut wire = write_to_vec!(|tx| send_response_async(
            &resp,
            tx,
            NonCancellableToken::new()
        ));
        wire.push(0xCDu8);

        let mut src: &[u8] = wire.as_slice();
        let prefix = recv_response_prefix_async(&mut src, NonCancellableToken::new())
            .await
            .expect("回复前缀应当解得出来");

        let decision = ResponseBodyDecision::decide(AccessMethod::View, &prefix)
            .expect("两个头都没有，是「没有体」而不是违规");
        assert_eq!(decision, ResponseBodyDecision::Absent, "应当判定为没有体");

        let body: Option<String> =
            recv_response_body_async(&mut src, &prefix, NonCancellableToken::new())
                .await
                .expect("读 0 字节不应当失败");
        assert!(body.is_none(), "没有体时应当读出 None");
        assert_eq!(src, &[0xCDu8], "哨兵字节不应当被读体路径吃掉");
    }
}

backend_async_test! {
    /// 测试体读到一半对端关闭时按「提前结束」报出。
    /// - 手段：造一条体为 8 字节的回复，写出后把线上字节削掉 5 个（前缀仍声明 8 字节），
    ///   再按头声明的长度去读体。
    /// - 判断：返回 `Err(Truncated)`——协议头已经承诺了长度，流却在长度满足前结束，属于
    ///   必须报出来的错误，不能当成功。
    fn truncated_body_is_reported_() {
        let resp = Response::<EncodedBody, Nothing>::with_measured_body(
            Status::Ok,
            EncodedBody::new(rmp_serde::to_vec("abcdefgh").expect("编码不该失败")),
        )
        .expect("量长度不该失败");
        let mut wire = write_to_vec!(|tx| send_response_async(
            &resp,
            tx,
            NonCancellableToken::new()
        ));
        // 削掉 5 个字节：头仍然声明 8 字节的体，而流里只剩 3 个。
        let cut = wire.len() - 5usize;
        wire.truncate(cut);

        let mut src: &[u8] = wire.as_slice();
        let prefix = recv_response_prefix_async(&mut src, NonCancellableToken::new())
            .await
            .expect("前缀仍然解得出来");
        let err: MessageIoError =
            recv_response_body_async::<String, _, _>(&mut src, &prefix, NonCancellableToken::new())
                .await
                .expect_err("体不足时应当报错");
        assert!(
            matches!(err, MessageIoError::Truncated(_)),
            "应当是 Truncated，实际是 {err}"
        );
    }
}

backend_async_test! {
    /// 测试「`Body_Size` 声明得比体实际字节多」时按截断报出。
    /// - 手段：手工把 `Body_Size` 设成 5 而体只有 3 字节，再调用 `send_request_async`。
    /// - 判断：返回 `Err(Truncated)`——定长搬运要求源恰好给得出声明的那么多字节，源提前
    ///   结束就是「对端会按错误的长度切分后续字节」的前兆，必须报出来而不是照发。
    fn send_reports_truncated_when_body_shorter_than_declared_() {
        let req = Request::<EncodedBody, Nothing>::new(AccessMethod::Post, "/upload")
            .with_headers(HeadersBuilder::new().set_body_size(5usize).build())
            .with_body(EncodedBody::new(b"abc".to_vec()));
        let mut storage = vec![0u8; K_BUFFER];
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let err = send_request_async(&req, &mut sink, NonCancellableToken::new())
            .await
            .expect_err("体不足声明长度时应当报截断");
        assert!(
            matches!(err, MessageIoError::Truncated(_)),
            "应当是 Truncated，实际是 {err}"
        );
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 纯逻辑：不碰 IO，因此不需要后端上下文
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试 `Head` 的回复带了体时按协议违规报出，而不是猜一个长度读下去。
/// - 手段：构造一个声明了 `Body_Size` 的回复前缀，用 `AccessMethod::Head` 去决策。
/// - 判断：返回 `Err(BodyNotAllowed)`，且其中的 `declared` 就是头里声明的长度——按自己
///   的理解读下去只会让两端越错越远，所以这里必须失败。
#[test]
fn head_reply_with_body_is_a_violation_() {
    let prefix = prefix_with_size_(Status::Ok, Option::Some(8usize));
    let err =
        ResponseBodyDecision::decide(AccessMethod::Head, &prefix).expect_err("Head 的回复不该带体");
    match err {
        ProtocolViolation::BodyNotAllowed { method, declared } => {
            assert_eq!(method, AccessMethod::Head, "违规的应当是 Head");
            assert_eq!(declared, Option::Some(8usize), "声明的长度应当原样带出来");
        }
        other => panic!("应当是 BodyNotAllowed，实际是 {other}"),
    }
}

/// 测试「只有 `Body_Type` 没有 `Body_Size`」按协议违规报出。
/// - 手段：造一个只设 `Body_Type` 的回复头，拼成前缀后用 `View` 去决策。
/// - 判断：返回 `Err(MissingBodySize)`——没有长度就无法确定边界，此时任何「读一点看看」
///   的做法都会破坏后续字节的对齐。
#[test]
fn body_type_without_size_is_a_violation_() {
    let headers = HeadersBuilder::new()
        .set_body_type(&HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack))
        .build();
    let prefix = RespPrefix(Status::Ok, Option::Some(headers));
    let err = ResponseBodyDecision::decide(AccessMethod::View, &prefix)
        .expect_err("只有类型没有长度应当被判违规");
    assert!(
        matches!(err, ProtocolViolation::MissingBodySize),
        "应当是 MissingBodySize，实际是 {err}"
    );
}

/// 测试只声明 `Body_Size`、没有 `Body_Type` 的回复仍然被正常读取。
/// - 手段：造一个只有 `Body_Size` 的回复前缀，用 `View` 去决策。
/// - 判断：决策给出 `Present(2)` 而不是违规——「有长度、没类型」是合法的原始字节体。
#[test]
fn body_without_type_is_allowed_() {
    let prefix = prefix_with_size_(Status::Ok, Option::Some(2usize));
    let decision =
        ResponseBodyDecision::decide(AccessMethod::View, &prefix).expect("有长度没类型是合法的");
    assert_eq!(
        decision,
        ResponseBodyDecision::Present(2usize),
        "应当读 2 字节"
    );
}

/// 测试 `TrRpcBody` 对「没有体」与「有体」的回答，以及长度预量与实际写出一致。
/// - 手段：对 [`Nothing`] 与字符串分别取长度，并把字符串编进一个 `Vec` 数出实际字节。
/// - 判断：`Nothing` 的长度是 `None`（没有体）；字符串长度是 `Some(3)`，且与真正编出来
///   的字节数相等——预量与实际编码走的是同一个编码器，两者必须对得上。
#[test]
fn body_view_reports_length_() {
    assert!(
        TrRpcBody::try_encoded_len(&Nothing)
            .expect("不该失败")
            .is_none(),
        "Nothing 表示没有体"
    );

    let len = TrRpcBody::try_encoded_len(&"hi")
        .expect("不该失败")
        .expect("字符串是有体的");
    let mut sink = Vec::new();
    TrRpcBody::try_encode_into(&"hi", &mut sink).expect("编码不该失败");
    assert_eq!(len, sink.len(), "预量出来的长度必须与实际写出量一致");
    assert_eq!(sink, b"\xa2hi", "MessagePack 的 \"hi\" 应当是这两个字节");
}

/// 测试 `RequestBuilder` 会把 `Body_Size` 与 `Body_Type` 两个头一并写好。
/// - 手段：用 `RequestBuilder` 的 `body` 入口编一个字符串，再读回请求的头与体长度。
/// - 判断：`Body_Size` 等于编出来的字节数；`Body_Type` 是 `Mime_Body_Type_MsgPack`；
///   `try_body_len` 给出的也是同一个数。
#[test]
fn builder_writes_body_headers_() {
    let req = RequestBuilder::new()
        .method(AccessMethod::Call)
        .path("/rpc/echo")
        .body("ping")
        .build()
        .expect("builder 应当构造成功");

    assert_eq!(
        super::io_::try_get_body_size_(req.headers()).expect("头应当可解"),
        5usize,
        "MessagePack 的 \"ping\" 是 fixstr 头 1 字节加正文 4 字节"
    );
    let body_type = req
        .headers()
        .expect("应当有头")
        .try_get_header(&StdHeaderKey::Body_Type.into())
        .expect("Body_Type 应当写好");
    assert_eq!(
        body_type.try_as_header_val().expect("应当是数字形态"),
        StdHeaderVal::Mime_Body_Type_MsgPack,
        "body 入口写的应当是 MessagePack"
    );
    assert_eq!(
        req.try_body_len().expect("体可编码"),
        Some(5usize),
        "体长度应当与头里的一致"
    );
}

/// 测试 `RequestBuilder` 在缺少 method / path 时明确失败。
/// - 手段：分别构造「只有 path」「只有 method」两个 builder 并 `build`。
/// - 判断：前者报 `MissingMethod`、后者报 `MissingPath`——两者都不该产出一条「猜一个
///   默认值」的请求。
#[test]
fn builder_requires_method_and_path_() {
    let no_method = RequestBuilder::new().path("/hello").build();
    assert!(
        matches!(no_method, Result::Err(RequestBuildError::MissingMethod)),
        "缺少 method 时应当报 MissingMethod"
    );

    let no_path = RequestBuilder::new().method(AccessMethod::View).build();
    assert!(
        matches!(no_path, Result::Err(RequestBuildError::MissingPath)),
        "缺少 path 时应当报 MissingPath"
    );
}

/// 测试 `RequestBuilder` 不容忍「手工设了 Body_Size 又与体不符」。
/// - 手段：先用 `body_bytes` 给 3 字节，再用 `headers` 把 `Body_Size` 覆盖成 9。
/// - 判断：`build` 报 `BodySizeMismatch`，而不是静默把头改成 3——静默改写会把「调用方
///   以为自己在传 9 字节」这类装配错误藏起来。
#[test]
fn builder_rejects_inconsistent_body_size_() {
    let built = RequestBuilder::new()
        .method(AccessMethod::Post)
        .path("/upload")
        .body_bytes(b"abc".to_vec())
        .headers(HeadersBuilder::new().set_body_size(9usize).build())
        .build();
    match built {
        Result::Err(RequestBuildError::BodySizeMismatch { declared, actual }) => {
            assert_eq!(declared, 9usize, "头里写的是 9");
            assert_eq!(actual, 3usize, "体是 3 字节");
        }
        other => panic!("应当是 BodySizeMismatch，实际是 {other:?}"),
    }
}

/// 测试 `HeadersBuilder` 的 `Body_Size` 按大小自动选形态，`Data_Type_Id` 走文本形态。
/// - 手段：分别用小长度（能进 `u16`）与大长度（超出 `u16`）设 `Body_Size`，并用
///   `set_data_type` 写一个 `u64` 标识；随后逐个取回头的值。
/// - 判断：小长度是数字形态且值相等；大长度是文本形态且能解析回原值；`Data_Type_Id`
///   是文本形态，内容等于该标识的十进制写法。
#[test]
fn headers_builder_chooses_value_forms_() {
    let small = HeadersBuilder::new().set_body_size(1024usize).build();
    let small_val = small
        .try_get_header(&StdHeaderKey::Body_Size.into())
        .expect("Body_Size 应当写好");
    assert_eq!(
        small_val
            .try_as_header_val()
            .expect("小长度应当是数字形态")
            .into_inner(),
        1024u16,
        "数字形态的值应当就是 1024"
    );

    let big_size = usize::from(u16::MAX) + 10usize;
    let big = HeadersBuilder::new().set_body_size(big_size).build();
    let big_val = big
        .try_get_header(&StdHeaderKey::Body_Size.into())
        .expect("Body_Size 应当写好");
    assert_eq!(
        big_val.try_as_str().expect("超大长度应当是文本形态"),
        big_size.to_string(),
        "文本形态应当能解析回原值"
    );

    let type_id: u64 = 0x1234_5678_9abc_def0u64;
    let typed = HeadersBuilder::new().set_data_type(type_id).build();
    let id_val = typed
        .try_get_header(&StdHeaderKey::Data_Type_Id.into())
        .expect("Data_Type_Id 应当写好");
    assert_eq!(
        id_val.try_as_str().expect("标识应当是文本形态"),
        type_id.to_string(),
        "标识应当按十进制写进头里"
    );
}

/// 测试 `EncodedBody` 交出的就是那串字节本身，不做二次编码。
/// - 手段：用 3 个字节构造一个 `EncodedBody`，取长度并编进一个 `Vec`。
/// - 判断：长度是 3、编出来的字节与输入逐字节相同——它是「已经编好」的载体，不该再被
///   MessagePack 包一层。
#[test]
fn encoded_body_passes_bytes_through_() {
    let body = EncodedBody::new(vec![1u8, 2, 3]);
    assert_eq!(
        TrRpcBody::try_encoded_len(&body).expect("不该失败"),
        Some(3usize)
    );
    let mut sink = Vec::new();
    TrRpcBody::try_encode_into(&body, &mut sink).expect("编码不该失败");
    assert_eq!(sink, vec![1u8, 2, 3], "应当原样写出字节");
    // 顺带确认 `Write` 的引入没有问题（`try_encode_into` 用的就是它）。
    let _ = sink.write(&[]);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

backend_async_test! {
    /// 测试写出请求的路径**不为报文攒缓冲**。
    /// - 手段：在全局分配器的**本线程**字节计数下，把一条带 32 KiB 体的请求写进内存
    ///   缓冲（体本身与缓冲都在计数开始前就绪）。
    /// - 判断：本次写出的分配总量必须落在 8 KiB 的预算内。预算不是「零」：底层适配器
    ///   在个别后端下每次同步等待会有固定的小额分配（实测 tokio 约 2 KiB），那份开销与
    ///   报文大小无关。而「把整条报文攒进一块缓冲」这类违规的分配量必然与报文同阶
    ///   （≥ 32 KiB），两者差一个数量级，一测就露——这正是 README §7 第 1 条纪律要守住
    ///   的东西。
    fn writing_request_does_not_buffer_message_() {
        /// 分配预算：容纳底层适配器的固定开销，远小于报文大小。
        const K_ALLOC_BUDGET: usize = 8usize * 1024usize;

        let payload = "x".repeat(32usize * 1024usize);
        // 编码发生在计数开始**之前**：这正是新模型要求的——体在发送时已经是可借出的
        // 字节，协议层不再为它临时分配任何缓存。
        let encoded = rmp_serde::to_vec(&payload).expect("编码不该失败");
        let req = Request::<EncodedBody, Nothing>::with_measured_body(
            AccessMethod::Call,
            "/rpc/echo",
            EncodedBody::new(encoded),
        )
        .expect("量长度不该失败");
        let mut storage = vec![0u8; K_BUFFER * 16usize];
        let mut sink: &mut [u8] = storage.as_mut_slice();

        alloc_probe_::reset_();
        send_request_async(&req, &mut sink, NonCancellableToken::new())
            .await
            .expect("写应当成功");
        let allocated = alloc_probe_::count_();
        assert!(
            allocated < K_ALLOC_BUDGET,
            "写出 32 KiB 的请求不该为报文攒缓冲（实际分配 {allocated} 字节，预算 {K_ALLOC_BUDGET}）"
        );
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 体的传输模式判定（纯逻辑，不碰 IO）
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 造一组「声明了分块传输」的报文头。
fn chunked_headers_() -> crate::specs::Headers {
    HeadersBuilder::new()
        .set(StdHeaderKey::Body_Transfer, chunked_transfer_header_val())
        .build()
}

/// 测试模式判定只看「哪些头在场」，三种声明各归各位。
/// - 手段：分别造「没有头」「只有 Body_Size」「只有分块声明」三组头，交给
///   [`body_transfer_of`]。
/// - 判断：三者分别是 `Absent` / `Sized(n)` / `Chunked`；`Body_Size` 为 0 等价于没有体
///   ——边界一旦为 0，收发两侧都不该多碰一个字节。
#[test]
fn body_transfer_decision_matches_declarations_() {
    assert_eq!(
        body_transfer_of(Option::None).expect("没有头不是违规"),
        BodyTransfer::Absent,
        "没有任何声明就是没有体"
    );

    let sized = HeadersBuilder::new().set_body_size(7usize).build();
    assert_eq!(
        body_transfer_of(Option::Some(&sized)).expect("只声明长度是合法的"),
        BodyTransfer::Sized(7usize),
        "有 Body_Size 就是定长"
    );

    let zero = HeadersBuilder::new().set_body_size(0usize).build();
    assert_eq!(
        body_transfer_of(Option::Some(&zero)).expect("长度 0 不是违规"),
        BodyTransfer::Absent,
        "长度为 0 与「没有体」是同一件事"
    );

    let chunked = chunked_headers_();
    assert_eq!(
        body_transfer_of(Option::Some(&chunked)).expect("只声明分块是合法的"),
        BodyTransfer::Chunked,
        "只有分块声明就是分块"
    );
}

/// 测试「定长」与「分块」两种声明同时在场时按协议违规报出。
/// - 手段：一组头里同时写下 `Body_Size` 与 `Body_Transfer: Chunked`。
/// - 判断：返回 `Err(ConflictingTransfer)`，并把 `Body_Size` 声明的长度带出来——两种边界
///   互斥，接收方无从判定该按哪种切分后续字节，宁可失败也不能猜。
#[test]
fn conflicting_transfer_declarations_are_rejected_() {
    let headers = HeadersBuilder::new()
        .set_body_size(9usize)
        .set(StdHeaderKey::Body_Transfer, chunked_transfer_header_val())
        .build();
    let err = body_transfer_of(Option::Some(&headers)).expect_err("两种声明同时在场应当违规");
    match err {
        ProtocolViolation::ConflictingTransfer { declared } => {
            assert_eq!(declared, 9usize, "声明的长度应当原样带出来");
        }
        other => panic!("应当是 ConflictingTransfer，实际是 {other}"),
    }
}

/// 测试无法解读的 `Body_Transfer` 取值按违规报出，而不是当作「没有声明」。
/// - 手段：把 `Body_Transfer` 写成一个协议未定义的数字值。
/// - 判断：返回 `Err(UnknownBodyTransfer)`；若静默按「没有声明」处理，对端就会按错误的
///   边界切分后续字节。
#[test]
fn unknown_transfer_value_is_rejected_() {
    let headers = HeadersBuilder::new()
        .set(StdHeaderKey::Body_Transfer, HeaderVal::from_u16(0x7fff))
        .build();
    let err = body_transfer_of(Option::Some(&headers)).expect_err("未知取值应当违规");
    assert!(
        matches!(err, ProtocolViolation::UnknownBodyTransfer(_)),
        "应当是 UnknownBodyTransfer，实际是 {err}"
    );
}

/// 测试分块声明的回复被决策判为 `Chunked`。
/// - 手段：造一个只带分块声明的回复前缀，用 `AccessMethod::Call` 去决策。
/// - 判断：得到 `ResponseBodyDecision::Chunked`——分块体没有确定总长，`body_size()` 因此
///   返回 `None`，调用方必须按块读到终止块为止。
#[test]
fn chunked_reply_is_decided_as_chunked_() {
    let prefix = RespPrefix(Status::Ok, Option::Some(chunked_headers_()));
    let decision = ResponseBodyDecision::decide(AccessMethod::Call, &prefix)
        .expect("Call 的回复带分块体是合法的");
    assert_eq!(decision, ResponseBodyDecision::Chunked, "应当判为分块");
    assert_eq!(decision.body_size(), Option::None, "分块体没有确定总长");
}

/// 测试 `Head` 的回复声明了分块体时同样按协议违规报出。
/// - 手段：造一个只带分块声明的回复前缀，用 `AccessMethod::Head` 去决策。
/// - 判断：返回 `Err(BodyNotAllowed)`，且 `declared` 为 `None`（分块体没有总长可报）。
#[test]
fn head_reply_with_chunked_body_is_a_violation_() {
    let prefix = RespPrefix(Status::Ok, Option::Some(chunked_headers_()));
    let err = ResponseBodyDecision::decide(AccessMethod::Head, &prefix)
        .expect_err("Head 的回复不该带体");
    match err {
        ProtocolViolation::BodyNotAllowed { method, declared } => {
            assert_eq!(method, AccessMethod::Head, "违规的应当是 Head");
            assert_eq!(declared, Option::None, "分块体没有总长");
        }
        other => panic!("应当是 BodyNotAllowed，实际是 {other}"),
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 限长读写
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

backend_async_test! {
    /// 测试限长读读到额度就用完，不会多借一个字节。
    /// - 手段：以 `b"abcdef"` 为源、额度 3 构造 [`LimitedRead`]，用 `AsStdRead` 一次读满
    ///   一个 8 字节缓冲。
    /// - 判断：只读到 `b"abc"`，且额度归零；额度用尽在读者眼里就是 EOF。
    fn limited_read_stops_at_the_limit_() {
        let mut src: &[u8] = b"abcdef";
        {
            let mut limited = LimitedRead::new(&mut src, 3usize);
            let mut read = AsStdRead::new(&mut limited, NonCancellableToken::new());
            let mut buf = [0u8; 8usize];
            let n = std::io::Read::read(&mut read, &mut buf).expect("读不该失败");
            assert_eq!(&buf[..n], b"abc", "只应当读到额度内的 3 个字节");
            assert_eq!(limited.remaining(), 0usize, "额度应当刚好用完");
        }
        assert_eq!(src, b"def", "底层源恰好推进了被读走的那 3 个字节");
    }
}

backend_async_test! {
    /// 测试限长写最多只借出额度那么多空间。
    /// - 手段：以 16 字节缓冲为底、额度 4 构造 [`LimitedWrite`]，用 `AsStdWrite` 尝试写
    ///   10 个字节。
    /// - 判断：只写进去 4 个字节（返回 4），底层缓冲的前 4 个字节是 `b"abcd"`；额度用尽
    ///   之后对生产者就是「没有空间」。
    fn limited_write_caps_the_amount_() {
        let mut storage = [0u8; 16usize];
        let mut sink: &mut [u8] = storage.as_mut_slice();
        {
            let mut limited = LimitedWrite::new(&mut sink, 4usize);
            let mut write = AsStdWrite::new(&mut limited, NonCancellableToken::new());
            let n = std::io::Write::write(&mut write, b"abcdefghij").expect("写不该失败");
            assert_eq!(n, 4usize, "只应当写进额度内的 4 个字节");
            assert_eq!(limited.remaining(), 0usize, "额度应当刚好用完");
        }
        assert_eq!(&storage[..4], b"abcd", "写进去的应当正好是前 4 个字节");
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 分块帧
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

backend_async_test! {
    /// 测试分块写的线上字节就是「2 字节大端长度 + 内容」，并以长度 0 的块收尾。
    /// - 手段：用 [`ChunkedWrite`] 写一块 `b"abc"`，再写终止块。
    /// - 判断：线上字节恰好是 `00 03 61 62 63 00 00`——长度是定长 2 字节、且就在内容前面，
    ///   写侧因此不需要回填，读侧也不需要跑一遍解码器才知道块有多长。
    fn chunked_write_emits_expected_frame_() {
        let mut storage = [0u8; 32usize];
        // 借用期间不能同时读 `storage`，因此先把剩余长度带出来。
        let rest = {
            let mut sink: &mut [u8] = storage.as_mut_slice();
            let mut writer = ChunkedWrite::new(&mut sink);
            let n = writer
                .write_chunk_async(b"abc")
                .await
                .expect("写块不该失败");
            assert_eq!(n, 3usize, "返回的是内容字节数，不含前缀");
            writer.finish_async().await.expect("写终止块不该失败");
            sink.len()
        };
        let written = storage.len() - rest;
        assert_eq!(
            &storage[..written],
            b"\x00\x03abc\x00\x00",
            "线上字节应当是长度前缀 + 内容 + 长度 0 的终止块"
        );
    }
}

backend_async_test! {
    /// 测试分块体可以跨多个块完整往返（内容比单块上限还大）。
    /// - 手段：造 70000 字节的体（超过 65535 的单块上限，必然被切成多块），声明分块后
    ///   用 [`send_body_async`] 写进内存缓冲，再用 [`body_reader`] 把它读回来。
    /// - 判断：读回的字节与原体逐字节相同；且线上长度 = 体长度 + 每块 2 字节前缀 +
    ///   终止块 2 字节——前缀记的是**各块实际的字节数**，不是固定值。
    fn chunked_body_roundtrips_across_chunks_() {
        const LEN: usize = 70_000usize;
        let payload: Vec<u8> = (0..LEN).map(|i| (i % 251usize) as u8).collect();
        let headers = chunked_headers_();

        let mut wire = vec![0u8; LEN + 64usize];
        // 借用期间不能同时读 `wire`，因此先把剩余长度带出来。
        let rest = {
            let mut src: &[u8] = payload.as_slice();
            let mut dst: &mut [u8] = wire.as_mut_slice();
            let written = send_body_async(&mut src, &mut dst, Option::Some(&headers), NonCancellableToken::new())
                .await
                .expect("分块发送不该失败");
            assert_eq!(written, LEN, "返回的是体字节数");
            dst.len()
        };
        let body_len = wire.len() - rest;
        // 70000 = 65535 + 4465，两块；线上 = 70000 + 2*2(前缀) + 2(终止块)。
        assert_eq!(body_len, LEN + 3usize * 2usize, "线上长度应当含块前缀与终止块");

        let mut rx: &[u8] = &wire[..body_len];
        let mut reader = body_reader(&mut rx, Option::Some(&headers)).expect("头声明合法");
        let mut out = Vec::new();
        {
            let mut read = AsStdRead::new(&mut reader, NonCancellableToken::new());
            std::io::Read::read_to_end(&mut read, &mut out).expect("读回不该失败");
        }
        assert_eq!(out, payload, "分块往返后内容应当逐字节相同");
        assert!(rx.is_empty(), "读完之后线上的体字节应当恰好耗尽");
    }
}

backend_async_test! {
    /// 测试分块体读完终止块之后，同一条流上的后续字节一个都不会被吃掉。
    /// - 手段：写出一个分块体，末尾手工追加一个哨兵字节，再由体视图读完。
    /// - 判断：内容与写出去的一致，且读源恰好剩下哨兵——终止块就是体的边界，后面的字节
    ///   属于 suffix stream（Push / Pull 的数据从那里开始）。
    fn chunked_body_leaves_suffix_untouched_() {
        let headers = chunked_headers_();
        let mut wire = vec![0u8; 64usize];
        // 借用期间不能同时读 `wire`，因此先把剩余长度带出来。
        let rest = {
            let mut src: &[u8] = b"hello";
            let mut dst: &mut [u8] = wire.as_mut_slice();
            send_body_async(&mut src, &mut dst, Option::Some(&headers), NonCancellableToken::new())
                .await
                .expect("分块发送不该失败");
            dst.len()
        };
        let used = wire.len() - rest;
        let mut with_sentinel = wire[..used].to_vec();
        with_sentinel.push(0xABu8);

        let mut rx: &[u8] = with_sentinel.as_slice();
        let mut reader = body_reader(&mut rx, Option::Some(&headers)).expect("头声明合法");
        let mut out = Vec::new();
        {
            let mut read = AsStdRead::new(&mut reader, NonCancellableToken::new());
            std::io::Read::read_to_end(&mut read, &mut out).expect("读回不该失败");
        }
        assert_eq!(out, b"hello", "体内容应当原样读回");
        assert_eq!(rx, &[0xABu8], "终止块之后的字节不应当被体读取吃掉");
    }
}

backend_async_test! {
    /// 测试定长体读完之后，多写的字节不会被这条报文吃掉。
    /// - 手段：声明 `Body_Size` 为 5，实际写出 8 个字节，末尾再追加一个哨兵，然后按头读体。
    /// - 判断：只读出声明的 5 个字节，读源剩下「多写的 3 字节 + 哨兵」——边界由头决定，
    ///   不由「读到哪算哪」决定。
    fn sized_body_stops_at_declared_length_() {
        let headers = HeadersBuilder::new().set_body_size(5usize).build();
        let mut wire = b"abcdefgh".to_vec();
        wire.push(0xCDu8);

        let mut rx: &[u8] = wire.as_slice();
        let mut reader = body_reader(&mut rx, Option::Some(&headers)).expect("头声明合法");
        let mut out = Vec::new();
        {
            let mut read = AsStdRead::new(&mut reader, NonCancellableToken::new());
            std::io::Read::read_to_end(&mut read, &mut out).expect("读回不该失败");
        }
        assert_eq!(out, b"abcde", "只应当读出声明的 5 个字节");
        assert_eq!(rx, b"fgh\xcd", "多写的字节与哨兵都应当留在流上");
    }
}

backend_async_test! {
    /// 测试「没有体」的视图一个字节都不读。
    /// - 手段：源里放一个哨兵，用「没有声明任何体」的头构造体视图并尝试读。
    /// - 判断：一个字节都读不到，且哨兵原封不动——「没有体」是独立于「体有多长」的结论。
    fn absent_body_view_consumes_nothing_() {
        let mut rx: &[u8] = &[0xABu8];
        let mut reader = body_reader(&mut rx, Option::None).expect("没有头不是违规");
        assert_eq!(reader.transfer(), BodyTransfer::Absent, "应当是「没有体」");
        let mut out = Vec::new();
        {
            let mut read = AsStdRead::new(&mut reader, NonCancellableToken::new());
            std::io::Read::read_to_end(&mut read, &mut out).expect("读不该失败");
        }
        assert!(out.is_empty(), "没有体时不该读出任何字节");
        assert_eq!(rx, &[0xABu8], "哨兵不应当被碰到");
    }
}

backend_async_test! {
    /// 测试分块帧的长度前缀不完整时按帧非法报出。
    /// - 手段：只给 1 个字节（长度前缀要 2 字节），声明分块后尝试读。
    /// - 判断：得到一个 io 错误——帧读不满就不能猜长度，更不能把它当成「读到头了」。
    fn incomplete_chunk_header_is_reported_() {
        let headers = chunked_headers_();
        let mut rx: &[u8] = &[0x00u8];
        let mut reader = body_reader(&mut rx, Option::Some(&headers)).expect("头声明合法");
        let mut read = AsStdRead::new(&mut reader, NonCancellableToken::new());
        let mut buf = [0u8; 4usize];
        let err = std::io::Read::read(&mut read, &mut buf).expect_err("长度前缀不完整应当报错");
        assert!(
            err.to_string().contains("分块"),
            "错误文案应当指向分块帧，实际是 {err}"
        );
    }
}

backend_async_test! {
    /// 测试定长发送只搬声明的字节数，源里多出来的一个都不碰。
    /// - 手段：声明 `Body_Size` 为 5，源给 8 个字节，用 [`send_body_async`] 写进内存缓冲。
    /// - 判断：线上只落了 5 个字节；源恰好推进到剩下的 `b"fgh"`——多出来的字节留在源上，
    ///   既不会被这条报文发出去，也不会被丢掉。
    fn sized_send_moves_only_declared_bytes_() {
        let headers = HeadersBuilder::new().set_body_size(5usize).build();
        let mut payload: &[u8] = b"abcdefgh";
        let mut wire = vec![0u8; 32usize];
        let rest = {
            let mut dst: &mut [u8] = wire.as_mut_slice();
            let moved = send_body_async(
                &mut payload,
                &mut dst,
                Option::Some(&headers),
                NonCancellableToken::new(),
            )
            .await
            .expect("定长发送不该失败");
            assert_eq!(moved, 5usize, "返回的应当是声明长度");
            dst.len()
        };
        let used = wire.len() - rest;
        assert_eq!(used, 5usize, "线上只应当落下声明的 5 个字节");
        assert_eq!(&wire[..used], b"abcde", "内容应当是源的前 5 个字节");
        assert_eq!(payload, b"fgh", "源里多出来的字节应当留在源上");
    }
}
