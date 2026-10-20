//! 下游集成测试：demo 如何使用 `mptp_smux::messaging` 的头表、体边界与前缀读写。
//!
//! 这些用例原先位于 demo 自己的 `wire_` 模块——那时前缀与头表都由 demo 实现。现在前缀线格式
//! 属于 `mptp_smux::messaging`，测试因此改为**从下游视角**验证同一批性质：
//!
//! - 体边界仍由 `Body_Size` / `Body_Transfer` 唯一决定；
//! - 头表的查询、遍历与修改契约仍然成立；
//! - 未知方法位明确失败，状态码作为开放数值不被折叠；
//! - demo 的请求 / 回复装配函数确实写下了标准头。
//!
//! # `tests_`: downstream integration tests for the messaging API
//!
//! These tests used to live in the demo's own `wire_` module. The prefix wire format now
//! belongs to `mptp_smux::messaging`, so the same properties are checked from the downstream
//! side: body boundaries, the header-map contracts, loud failure on unknown methods, open
//! status values, and the standard headers written by the demo's builders.

use core::{
    borrow::Borrow,
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mptp::{
    messaging::{BodySize, TrMsgPrefix},
    spec::{
        NumOrStr, Status, StdHeaderKey, StdHeaderVal, TrHeaders, TrHeadersMut,
        access_method::AccessMethod,
    },
};
use mptp_smux::messaging::{
    HeaderEntry, HeaderVal, Headers, MessagingError, RequestPrefix, read_request_prefix_async,
    read_response_prefix_async,
};

use crate::protocol_::{
    EchoReply, EchoRequest, K_ECHO_PATH, request_with_, response_empty_, response_with_,
    type_id_of_,
};

/// 立即完成的测试执行器：这些用例只操作内存中的字节，会一直 `Ready`。
fn block_on_<TyFut>(future: TyFut) -> TyFut::Output
where
    TyFut: Future,
{
    let mut cx = Context::from_waker(Waker::noop());
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => core::hint::spin_loop(),
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 体边界
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试体边界判定把三种头声明各归各位。
/// Test that the body-boundary decision maps each header declaration to its own case.
///
/// - 手段：造三条请求前缀——没有 `Body_Size`、`Body_Size: "12"`、`Body_Transfer: Chunked`
///   ——各调用一次 `body_info`。
/// - 判断：依次得到 `NilBody` / `FixedLength(Num(12))` / `ChunkedData`；三者互不混淆，
///   因此「有没有体、怎么切」不会被读成同一件事。
///
/// - Means: build three request prefixes (no `Body_Size`, `Body_Size: "12"`, and
///   `Body_Transfer: Chunked`) and call `body_info` on each.
/// - Verdict: they yield `NilBody`, `FixedLength(Num(12))` and `ChunkedData` respectively, so
///   "is there a body" and "how is it framed" never blur together.
#[test]
fn body_info_maps_declarations_() {
    let plain = RequestPrefix::new(AccessMethod::View, "/rpc/echo");
    assert_eq!(plain.body_info(), BodySize::NilBody);

    let mut sized = plain.clone();
    sized.headers_mut().set_text(StdHeaderKey::Body_Size, "12");
    assert_eq!(
        sized.body_info(),
        BodySize::FixedLength(NumOrStr::Num(12usize))
    );

    let mut chunked = plain;
    chunked.headers_mut().set_num(
        StdHeaderKey::Body_Transfer,
        StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );
    assert_eq!(chunked.body_info(), BodySize::ChunkedData);
}

/// 测试两个体声明同时在场时按 `abs_mptp` 的文档给出**唯一**解读。
/// Test that two simultaneous body declarations resolve to exactly one documented reading.
///
/// - 手段：一张同时写了 `Body_Size: "8"` 与 `Body_Transfer: Chunked` 的头表，配成请求前缀。
/// - 判断：`body_info` 为 `FixedLength(Num(8))`。两个声明同时存在属于协议违规；`abs_mptp`
///   的默认实现**优先采用 `Body_Size`**，这里钉住这条顺序。旧 demo 自己的实现曾优先分块，
///   那条策略随 `wire_` 一起删除——测试意图仍是「不能靠猜，必须可预期」。
///
/// - Means: one header map carrying both `Body_Size: "8"` and `Body_Transfer: Chunked`,
///   wrapped in a request prefix.
/// - Verdict: `body_info` is `FixedLength(Num(8))`. Both declarations at once is a protocol
///   violation, and `abs_mptp`'s default explicitly prefers `Body_Size`; the old demo used to
///   prefer chunked, but that policy was removed with `wire_`. The intent is unchanged:
///   resolution must be deterministic, never guessed.
#[test]
fn both_body_declarations_resolve_deterministically_() {
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, "8");
    headers.set_num(
        StdHeaderKey::Body_Transfer,
        StdHeaderVal::Body_Transfer_Chunked.into_inner(),
    );
    let prefix =
        RequestPrefix::with_headers(AccessMethod::View, "/rpc/echo", Option::Some(headers));

    assert_eq!(
        prefix.body_info(),
        BodySize::FixedLength(NumOrStr::Num(8usize))
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 头表
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试头表在 `TrHeaders` 契约下按键取值、按值遍历。
/// Test that the header map answers lookups and iteration through `TrHeaders`.
///
/// - 手段：写入一个数字头与一个文本头，先用 `try_get_header` 按键取，再用
///   `into_entries` 全量遍历。
/// - 判断：两次取到的值与写入的一致；遍历得到两条，且 `Borrow` 出来的键值对得上。
///   这一条同时验证了「条目必须同时能借成键与值」这个 `abs_mptp` 的要求确实成立。
///
/// - Means: store one numeric and one text header, look them up by key through
///   `try_get_header`, then walk everything through `into_entries`.
/// - Verdict: both lookups match what was stored, the walk yields two entries, and each
///   borrowed key/value pair lines up — which also shows the "an entry must borrow as both
///   key and value" requirement of `abs_mptp` really holds.
#[test]
fn headers_answer_tr_headers_() {
    let mut headers = Headers::new();
    headers.set_num(StdHeaderKey::Body_Type, 1u16);
    headers.set_text(StdHeaderKey::Data_Type_Id, "0xabc");

    let by_key = headers
        .try_get_header(StdHeaderKey::Body_Type)
        .expect("写进去的头应当取得回来");
    assert_eq!(by_key, &HeaderVal::Num(1u16));

    let text = headers
        .try_get_header(StdHeaderKey::Data_Type_Id)
        .expect("文本头也应当取得回来");
    assert_eq!(text, &HeaderVal::Text("0xabc".to_string()));

    let all: Vec<HeaderEntry> = headers.into_entries().collect();
    assert_eq!(all.len(), 2usize);
    let borrowed: &StdHeaderKey = all[0].borrow();
    assert!(matches!(
        *borrowed,
        StdHeaderKey::Body_Type | StdHeaderKey::Data_Type_Id
    ));
    // 条目对**值**同样可 `Borrow`——`abs_mptp` 要求两者都能借出。
    let value: &HeaderVal = all[0].borrow();
    assert!(matches!(value, HeaderVal::Num(1u16) | HeaderVal::Text(_)));
}

/// 测试头表的**固有**写入口（唯一持有者那条路）真的改得动。
/// Test that the inherent write entry of the header map really mutates it.
///
/// - 手段：用 `set_num` / `set_text` 往一张刚造出来的表里写两个头，再读回来。
/// - 判断：读到的值与写入的一致。这两个方法收 `&mut self`，因此调用方持有唯一句柄，
///   写时复制不会把它分出去——这正是 handler 手里那个 `&mut Headers` 的情形。
///
/// - Means: write two headers into a fresh map through `set_num` / `set_text` and read
///   them back.
/// - Verdict: the values match. Both methods take `&mut self`, so the caller holds the only
///   handle and copy-on-write cannot split it — exactly the situation of the `&mut Headers`
///   a handler receives.
#[test]
fn headers_inherent_writes_land_() {
    let mut headers = Headers::new();
    headers.set_num(StdHeaderKey::Body_Type, 7u16);
    headers.set_text(StdHeaderKey::Body_Size, "12");

    assert_eq!(
        headers.try_get_val(StdHeaderKey::Body_Type),
        Option::Some(&HeaderVal::Num(7u16))
    );
    assert_eq!(headers.body_size(), Option::Some(12usize));
}

/// 测试 `TrHeadersMut` 三个入口在空表上给出的回报。
/// Test what the three `TrHeadersMut` entries report on an empty map.
///
/// - 手段：对一张空表分别调 `try_add_header`（期待成功）、`remove_header`、
///   `add_or_set_header`。
/// - 判断：加成功；删与覆盖式加都返回 `None`（空表里既没有可删的，也没有可替换的）。
///
/// # 这条测试**不**断言「改完之后表里有东西」
///
/// `TrHeadersMut` 的三个方法都按值收 `self` 却只交出「旧值 / 新条目」，修改只能发生在
/// **共享状态**里才留得住。`Headers` 是写时复制句柄（`Arc<BTreeMap>`）：调用方若还握着
/// 另一个句柄，写就落在临时副本上。这不是本 demo 的用法（框架构造完头表就交给 handler
/// 独占），但它是契约字面语义与实现之间的一道缝。
///
/// - Means: call `try_add_header` (expected to succeed), `remove_header` and
///   `add_or_set_header` on an empty map.
/// - Verdict: the add succeeds; the removal and the upsert both report `None`, an empty map
///   having neither something to remove nor something to replace. Deliberately **not**
///   asserted: that the map holds the new entry afterwards — see the note above.
#[test]
fn headers_report_tr_headers_mut_on_empty_() {
    let headers = Headers::new();

    let added = headers
        .clone()
        .try_add_header(StdHeaderKey::Accept, |_| HeaderVal::Text("a".to_string()));
    assert!(added.is_ok(), "空表上添加应当成功");

    assert_eq!(
        headers.clone().remove_header(StdHeaderKey::Accept),
        Option::None
    );
    assert_eq!(
        headers
            .clone()
            .add_or_set_header(StdHeaderKey::Accept, HeaderVal::Text("b".to_string())),
        Option::None
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 线上取值
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试未知的方法位**明确失败**，不被折叠成某个已知方法。
/// Test that an unknown method bit fails loudly instead of folding into a known method.
///
/// - 手段：把 `0x7f`（不在 7 个方法位里）作为方法位，后跟一个空位置与 `nil` 头，喂给
///   `mptp_smux::messaging` 的请求前缀解码。
/// - 判断：返回 `Err(MessagingError::Unknown)`。线上契约不认识就不能猜——把 `0x7f` 折成
///   `Head` 会让服务端把一次未知访问当成元信息查询。
///
/// - Means: feed `0x7f` (not one of the seven method bits), followed by an empty location
///   and nil headers, to `mptp_smux::messaging`'s request-prefix decoder.
/// - Verdict: `Err(MessagingError::Unknown)`. An unrecognised wire contract is never guessed:
///   folding `0x7f` into `Head` would turn an unknown access into a metadata read.
#[test]
fn unknown_method_fails_loudly_() {
    let bytes = [0x7fu8, 0xa0u8, 0xc0u8];
    let mut input: &[u8] = &bytes;
    let result = block_on_(
        read_request_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
    );

    assert!(
        matches!(result, Result::Err(MessagingError::Unknown(_))),
        "未知方法位应当报 Unknown，实际是 {result:?}"
    );
}

/// 测试状态码是**开放数值**：未知状态原样保留，不折叠成相近的已知状态。
/// Test that status codes are open numeric values: unknown ones are preserved, never folded.
///
/// - 手段：直接解码 `cd 02 57 c0`——`u16` 599 的 MessagePack 表示加上 `nil` 头。
/// - 判断：得到 `Status::new(599)`。旧实现把未知状态当成错误；现在状态在线上只是一个
///   `u16`，保留原值才能让客户端看到服务端真正回了什么。
///
/// - Means: decode `cd 02 57 c0` — the MessagePack form of `u16` 599 followed by nil
///   headers.
/// - Verdict: `Status::new(599)`. The old implementation rejected unknown statuses; a status
///   is just a `u16` on the wire, and preserving it lets the client see what the server
///   actually answered.
#[test]
fn status_is_open_not_folded_() {
    let bytes = [0xcdu8, 0x02u8, 0x57u8, 0xc0u8];
    let mut input: &[u8] = &bytes;
    let decoded = block_on_(
        read_response_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
    )
    .expect("开放状态码应当可解");

    assert_eq!(decoded.status(), Status::new(599u16));
    assert_eq!(decoded.headers(), Option::None);
    assert!(input.is_empty());
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// demo 的装配函数
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 测试带体的请求装配写下 `Body_Size` / `Body_Type` / `Data_Type_Id` 三个标准头。
/// Test that the sized request builder writes the three standard body headers.
///
/// - 手段：对 `EchoRequest` 调用 `request_with_`，检查返回的前缀与体。
/// - 判断：方法与路径原样保留；体边界是「长度等于实际体字节数」的定长；头里的长度、MIME
///   类型与类型标识都与本端已知值一致。任何一项缺失都会让服务端读错体或拒绝类型。
///
/// - Means: call `request_with_` on an `EchoRequest` and inspect the returned prefix and
///   body.
/// - Verdict: method and path are preserved; the boundary is a fixed length equal to the
///   actual body bytes; the size, MIME type and type identifier in the headers all match the
///   local values. A missing header would make the server read the wrong body or reject the
///   type.
#[test]
fn request_builder_sets_standard_headers_() {
    let value = EchoRequest {
        text: "hello".to_string(),
    };
    let (prefix, body) = block_on_(request_with_(AccessMethod::Call, K_ECHO_PATH, &value))
        .expect("请求装配应当成功");

    assert_eq!(prefix.method(), AccessMethod::Call);
    assert_eq!(prefix.location(), K_ECHO_PATH);
    assert_eq!(
        prefix.body_info(),
        BodySize::FixedLength(NumOrStr::Num(body.len()))
    );

    let headers = prefix.headers().expect("请求应当带标准头");
    assert_eq!(headers.body_size(), Option::Some(body.len()));
    assert_eq!(
        headers.try_get_val(StdHeaderKey::Body_Type),
        Option::Some(&HeaderVal::Num(
            StdHeaderVal::Mime_Body_Type_MsgPack.into_inner()
        ))
    );
    assert_eq!(
        headers.data_type_id(),
        Option::Some(type_id_of_::<EchoRequest>())
    );
}

/// 测试带体的回复装配写下与请求同一套标准头，且空回复不声明体。
/// Test that the sized response builder writes the same standard headers, while an empty
/// reply declares no body.
///
/// - 手段：分别对 `EchoReply` 调用 `response_with_`、对 `NotAcceptable` 调用
///   `response_empty_`。
/// - 判断：带体回复的体边界等于实际字节数、类型标识等于 `EchoReply` 的标识；空回复的
///   状态正确、体为空且 `body_info` 为 `NilBody`。这保证 406 这类拒绝不会被误读成「有体
///   但没读到」。
///
/// - Means: call `response_with_` on an `EchoReply`, and `response_empty_` on
///   `NotAcceptable`.
/// - Verdict: the sized reply's boundary equals its byte count and its identifier equals
///   `EchoReply`'s; the empty reply carries the right status, an empty body, and
///   `body_info == NilBody`. This keeps a 406 from looking like "a body that was not read".
#[test]
fn response_builder_sets_standard_headers_() {
    let value = EchoReply {
        text: "hello".to_string(),
    };
    let (prefix, body) = block_on_(response_with_(Status::Ok, &value)).expect("回复装配应当成功");

    assert_eq!(prefix.status(), Status::Ok);
    assert_eq!(
        prefix.body_info(),
        BodySize::FixedLength(NumOrStr::Num(body.len()))
    );
    let headers = prefix.headers().expect("带体回复应当带标准头");
    assert_eq!(
        headers.data_type_id(),
        Option::Some(type_id_of_::<EchoReply>())
    );

    let (empty, empty_body) = response_empty_(Status::NotAcceptable);
    assert_eq!(empty.status(), Status::NotAcceptable);
    assert!(empty_body.is_empty());
    assert_eq!(empty.body_info(), BodySize::NilBody);
}
