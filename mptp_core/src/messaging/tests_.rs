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

use super::{
    MessageIoError, Nothing, ProtocolViolation, Request, RespPrefix, Response,
    ResponseBodyDecision, TrRpcBody, TrRpcRequest,
    basic::EncodedBody,
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
        let req = Request::<String, Nothing>::with_measured_body(
            AccessMethod::Call,
            "/rpc/echo",
            "hello".to_string(),
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
        let resp = Response::<String, Nothing>::with_measured_body(
            Status::Ok,
            "resp".to_string(),
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
        let resp = Response::<String, Nothing>::with_measured_body(
            Status::Ok,
            "abcdefgh".to_string(),
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
    /// 测试发送端拒绝写出「`Body_Size` 与实际体长度不符」的报文。
    /// - 手段：手工把 `Body_Size` 设成比实际体多，再调用 `send_request_async`。
    /// - 判断：返回 `Err(BodySizeMismatch { declared, actual })`，两个数就是头声明与实际
    ///   字节数；报文**不会被写出去**，因此接收方不可能按错误长度切分字节。
    fn send_rejects_body_size_mismatch_() {
        let req = Request::<EncodedBody, Nothing>::new(AccessMethod::Post, "/upload")
            .with_headers(HeadersBuilder::new().set_body_size(5usize).build())
            .with_body(EncodedBody::new(b"abc".to_vec()));
        let mut storage = vec![0u8; K_BUFFER];
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let err = send_request_async(&req, &mut sink, NonCancellableToken::new())
            .await
            .expect_err("长度不符时应当拒绝写出");
        match err {
            MessageIoError::BodySizeMismatch { declared, actual } => {
                assert_eq!(declared, 5usize, "头声明的是 5");
                assert_eq!(actual, 3usize, "实际体是 3");
            }
            other => panic!("应当是 BodySizeMismatch，实际是 {other}"),
        }
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
            assert_eq!(declared, 8usize, "声明的长度应当原样带出来");
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
        let req = Request::<String, Nothing>::with_measured_body(
            AccessMethod::Call,
            "/rpc/echo",
            payload,
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
