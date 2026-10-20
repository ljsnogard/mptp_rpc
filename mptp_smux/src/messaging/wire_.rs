//! 前缀的线格式：**四个入口，一套格式**。
//!
//! | 方向 | 线上顺序 | 入口 |
//! | --- | --- | --- |
//! | 客户端发请求 | `access_method → location → headers` | [`write_request_prefix_async`] |
//! | 服务端收请求 | 同上 | [`read_request_prefix_async`] |
//! | 服务端发回复 | `status → headers` | [`write_response_prefix_async`] |
//! | 客户端收回复 | 同上 | [`read_response_prefix_async`] |
//!
//! 每个值都是一个**连续的 MessagePack 值**，不额外包一层数组或结构体：把三个值装进元组会
//! 多写一个数组头，直接改变线上字节。头表本身是「数字键 → 值」的映射，没有头时写 `nil`。
//!
//! # 只读前缀，不碰体
//!
//! 读入口把 `MpDecoder` 直接架在接收半边上，要多少借多少；解完最后一个头就返回，报文体的
//! 第一个字节仍在环里。分块体尤其依赖这一点——多读一个字节就会被当成块长。
//!
//! # 编码不预攒整体
//!
//! 写入口把 `MpEncoder` 直接架在发送半边上：标签、长度与标量在栈上编好后按段写入，字符串
//! 直接引用调用方数据。因此不需要「先编到 `Vec`、再整块写出去」的中转缓冲，也没有固定
//! 暂存区上限。
//!
//! # `wire_`: the prefix wire format
//!
//! Four entry points, one format. Requests are `method → location → headers`; responses are
//! `status → headers`, each as a **separate** MessagePack value. Decoding reads exactly the
//! prefix and leaves the first body byte in the ring; encoding writes through the ring
//! directly, with no gather buffer and no fixed scratch cap.

use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{de::TrDecodable, ser::TrEncodable};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use gen_mcf2::gen_may_cancel_future;
use mp_denco::{MpDecoder, MpEncoder};

use super::{
    error_::MessagingError,
    headers_::Headers,
    prefix_::{RequestPrefix, ResponsePrefix},
};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 线上取值 ↔ 本端取值
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把线上的方法位还原成本端的方法值。
///
/// [`AccessMethod`] 是 `#[repr(u8)]`，但没有「从数值构造」的公开入口，因此这里列一遍位值
/// ——它本来就是**线上契约**的一部分。未知位返回 `None`，由调用者报成
/// [`MessagingError::Unknown`]。
const fn method_from_u8_(code: u8) -> Option<AccessMethod> {
    let method = match code {
        0 => AccessMethod::Head,
        0b0000_0001 => AccessMethod::View,
        0b0000_0010 => AccessMethod::Post,
        0b0000_0100 => AccessMethod::Drop,
        0b0001_0000 => AccessMethod::Push,
        0b0010_0000 => AccessMethod::Pull,
        0b0100_0000 => AccessMethod::Call,
        _ => return Option::None,
    };
    Option::Some(method)
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 请求前缀
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把请求前缀按 `access_method → location → headers` 写出去。
///
/// 返回的 future 在 `.may_cancel_with(token)` 之后才携带取消令牌；等待写段期间收到取消
/// 信号时返回 [`MessagingError::Encode`]。
///
/// # Errors
///
/// 编码失败、写子流失败，或等待期间被取消时返回 [`MessagingError::Encode`]。
///
/// # Examples
///
/// ```ignore
/// use abs_cancel::NonCancellableToken;
/// use abs_mptp::spec::access_method::AccessMethod;
/// use mptp_smux::messaging::{write_request_prefix_async, RequestPrefix};
///
/// let prefix = RequestPrefix::new(AccessMethod::Call, "/rpc/echo");
/// write_request_prefix_async(&prefix, &mut tx)
///     .may_cancel_with(NonCancellableToken::new())
///     .await?;
/// ```
pub fn write_request_prefix_async<'f, W>(
    prefix: &'f RequestPrefix,
    tx: &'f mut W,
) -> WriteRequestPrefixAsync<'f, 'f, W>
where
    W: 'f + TrBuffWrite<u8>,
{
    WriteRequestPrefixAsync::new(prefix, tx)
}

/// [`write_request_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteRequestPrefix, pub, new(pub(crate)))]
async fn write_request_prefix_async_<'f, W, TyTok>(
    prefix: &'f RequestPrefix,
    tx: &'f mut W,
    cancel: TyTok,
) -> Result<(), MessagingError>
where
    W: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    // 三个值**依次编进同一个写口**：线上格式就是「三个连续的 MessagePack 值」。
    (prefix.method_ as u8)
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Encode(err.to_string()))?;
    prefix
        .location_
        .as_str()
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Encode(err.to_string()))?;
    prefix
        .headers_
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| MessagingError::Encode(err.to_string()))?;
    Result::Ok(())
}

/// 从接收半边解出一个请求前缀。
///
/// 只读前缀所需的字节：报文体的第一个字节仍在环里。未知方法位报
/// [`MessagingError::Unknown`]，不猜一个近似值。
///
/// # Errors
///
/// 报文不是合法编码、方法位不可识别、对端提前关闭，或读子流失败时返回错误。
///
/// # Examples
///
/// ```ignore
/// use abs_cancel::NonCancellableToken;
/// use mptp_smux::messaging::read_request_prefix_async;
///
/// let prefix = read_request_prefix_async(&mut rx)
///     .may_cancel_with(NonCancellableToken::new())
///     .await?;
/// ```
pub fn read_request_prefix_async<'f, R>(rx: &'f mut R) -> ReadRequestPrefixAsync<'f, 'f, R>
where
    R: 'f + TrBuffRead<u8>,
{
    ReadRequestPrefixAsync::new(rx)
}

/// [`read_request_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadRequestPrefix, pub, new(pub(crate)))]
async fn read_request_prefix_async_<'f, R, TyTok>(
    rx: &'f mut R,
    cancel: TyTok,
) -> Result<RequestPrefix, MessagingError>
where
    R: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let code = u8::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Decode(err.to_string()))?;
    let method_ = method_from_u8_(code)
        .ok_or_else(|| MessagingError::Unknown(format!("不认识的方法位 {code:#04x}")))?;
    let location_ = String::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Decode(err.to_string()))?;
    let headers_ = Option::<Headers>::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| MessagingError::Decode(err.to_string()))?;
    Result::Ok(RequestPrefix {
        method_,
        location_,
        headers_,
    })
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 回复前缀
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把回复前缀按 `status → headers` 写出去。
///
/// 返回的 future 在 `.may_cancel_with(token)` 之后才携带取消令牌。
///
/// # Errors
///
/// 编码失败、写子流失败，或等待期间被取消时返回 [`MessagingError::Encode`]。
///
/// # Examples
///
/// ```ignore
/// use abs_cancel::NonCancellableToken;
/// use abs_mptp::spec::Status;
/// use mptp_smux::messaging::{write_response_prefix_async, ResponsePrefix};
///
/// let prefix = ResponsePrefix::new(Status::Ok);
/// write_response_prefix_async(&prefix, &mut tx)
///     .may_cancel_with(NonCancellableToken::new())
///     .await?;
/// ```
pub fn write_response_prefix_async<'f, W>(
    prefix: &'f ResponsePrefix,
    tx: &'f mut W,
) -> WriteResponsePrefixAsync<'f, 'f, W>
where
    W: 'f + TrBuffWrite<u8>,
{
    WriteResponsePrefixAsync::new(prefix, tx)
}

/// [`write_response_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteResponsePrefix, pub, new(pub(crate)))]
async fn write_response_prefix_async_<'f, W, TyTok>(
    prefix: &'f ResponsePrefix,
    tx: &'f mut W,
    cancel: TyTok,
) -> Result<(), MessagingError>
where
    W: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    prefix
        .status_
        .inner()
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Encode(err.to_string()))?;
    prefix
        .headers_
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| MessagingError::Encode(err.to_string()))?;
    Result::Ok(())
}

/// 从接收半边解出一个回复前缀。
///
/// 只读前缀所需的字节；状态码是开放数值（任意 `u16` 都由 [`Status::new`] 表示），因此未知
/// 状态不会失败。
///
/// # Errors
///
/// 报文不是合法编码、对端提前关闭，或读子流失败时返回 [`MessagingError::Decode`]。
///
/// # Examples
///
/// ```ignore
/// use abs_cancel::NonCancellableToken;
/// use mptp_smux::messaging::read_response_prefix_async;
///
/// let prefix = read_response_prefix_async(&mut rx)
///     .may_cancel_with(NonCancellableToken::new())
///     .await?;
/// ```
pub fn read_response_prefix_async<'f, R>(rx: &'f mut R) -> ReadResponsePrefixAsync<'f, 'f, R>
where
    R: 'f + TrBuffRead<u8>,
{
    ReadResponsePrefixAsync::new(rx)
}

/// [`read_response_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadResponsePrefix, pub, new(pub(crate)))]
async fn read_response_prefix_async_<'f, R, TyTok>(
    rx: &'f mut R,
    cancel: TyTok,
) -> Result<ResponsePrefix, MessagingError>
where
    R: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    let code = u16::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| MessagingError::Decode(err.to_string()))?;
    let status_ = Status::new(code);
    let headers_ = Option::<Headers>::decode_async(MpDecoder::new(&mut *rx))
        .may_cancel_with(cancel)
        .await
        .map_err(|err| MessagingError::Decode(err.to_string()))?;
    Result::Ok(ResponsePrefix { status_, headers_ })
}

#[cfg(test)]
mod tests_ {
    //! 四个线格式入口的往返、字节顺序与「不碰体」性质。
    //!
    //! 这里用 `&mut [u8]` / `&[u8]` 当段接口的实现：编码与解码都是纯内存操作，不需要连接、
    //! 也不需要运行时。真实 socket 往返由 demo 的 e2e 脚本覆盖。
    //!
    //! Round-trip, wire order and the "never touch the body" property of the four entry
    //! points. `&mut [u8]` / `&[u8]` stand in for the segment interface: everything is pure
    //! in-memory work, so neither a connection nor a runtime is needed.

    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use abs_cancel::NonCancellableToken;
    use abs_mptp::spec::{StdHeaderKey, StdHeaderVal};

    use super::*;
    use crate::messaging::HeaderVal;

    /// 立即完成的测试执行器：所有操作都是纯内存的，会一直 `Ready`。
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

    /// 测试请求前缀的线上字节就是「方法 → 位置 → 头」三个连续值。
    /// Test that a request prefix is exactly three consecutive values in wire order.
    ///
    /// - 手段：以 `View` + `"/x"` + 无头编码，随后读出被写走的字节。
    /// - 判断：字节为 `01 a2 2f 78 c0`——方法位、fixstr 位置、`nil` 头，没有任何额外数组头。
    ///   若实现把三个值包成元组，第二条断言会看到数组标记而失败。
    ///
    /// - Means: encode `View` + `"/x"` + no headers and read back the bytes written.
    /// - Verdict: `01 a2 2f 78 c0` — method bit, fixstr location, nil headers, with no extra
    ///   array header. Wrapping the three values in a tuple would leave an array marker here.
    #[test]
    fn request_prefix_wire_order_() {
        let prefix = RequestPrefix::new(AccessMethod::View, "/x");
        let mut storage = [0u8; 32usize];
        let cap = storage.len();
        let mut sink: &mut [u8] = storage.as_mut_slice();

        block_on_(
            write_request_prefix_async(&prefix, &mut sink)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("请求前缀编码应当成功");

        let written = cap - sink.len();
        assert_eq!(&storage[..written], &[0x01, 0xa2, b'/', b'x', 0xc0]);
    }

    /// 测试回复前缀的线上字节就是「状态 → 头」两个连续值。
    /// Test that a response prefix is exactly two consecutive values in wire order.
    ///
    /// - 手段：以 `Status::Ok` + 无头编码，读出字节。
    /// - 判断：字节为 `cc c8 c0`——`mp_denco` 把 200 写成最短的 `u8` 形态，随后是 `nil` 头；
    ///   没有数组头，也没有把状态换成另一种容器。MessagePack 的整数宽度不进入语义，解码侧
    ///   只要按 `u16` 读就能接受任意宽度。
    ///
    /// - Means: encode `Status::Ok` + no headers and read back the bytes.
    /// - Verdict: `cc c8 c0` — `mp_denco` writes 200 in the shortest `u8` form followed by nil
    ///   headers, with neither an array header nor another container. Integer width carries no
    ///   semantics in MessagePack, and decoding as `u16` accepts every width.
    #[test]
    fn response_prefix_wire_order_() {
        let prefix = ResponsePrefix::new(Status::Ok);
        let mut storage = [0u8; 16usize];
        let cap = storage.len();
        let mut sink: &mut [u8] = storage.as_mut_slice();

        block_on_(
            write_response_prefix_async(&prefix, &mut sink)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("回复前缀编码应当成功");

        let written = cap - sink.len();
        assert_eq!(&storage[..written], &[0xcc, 0xc8, 0xc0]);
    }

    /// 测试读请求前缀时**不会**读走报文体的第一个字节。
    /// Test that reading a request prefix never consumes the first body byte.
    ///
    /// - 手段：把一段合法的请求前缀与两个体字节写在同一个切片里，只调用前缀解码，再检查
    ///   剩下的输入。
    /// - 判断：解出的前缀与原值相等，且输入恰好剩下 `[0xAA, 0xBB]`；多读一个字节就会让这条
    ///   断言失败（分块体会因此把数据字节当成块长）。
    ///
    /// - Means: put a valid request prefix and two body bytes in one slice, decode only the
    ///   prefix, then inspect what is left.
    /// - Verdict: the decoded prefix equals the original and the input is exactly
    ///   `[0xAA, 0xBB]`; reading one byte too many would fail this and desynchronise a chunked
    ///   body.
    #[test]
    fn request_prefix_leaves_body_untouched_() {
        let prefix = RequestPrefix::with_headers(
            AccessMethod::Call,
            "/rpc/echo",
            Option::Some(Headers::new()),
        );
        let mut storage = [0u8; 128usize];
        let cap = storage.len();
        let mut sink: &mut [u8] = storage.as_mut_slice();
        block_on_(
            write_request_prefix_async(&prefix, &mut sink)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("请求前缀编码应当成功");
        let written = cap - sink.len();

        let body = [0xaau8, 0xbbu8];
        storage[written..written + body.len()].copy_from_slice(&body);

        let mut input: &[u8] = &storage[..written + body.len()];
        let decoded = block_on_(
            read_request_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
        )
        .expect("请求前缀解码应当成功");

        assert_eq!(decoded, prefix);
        assert_eq!(input, &body);
    }

    /// 测试头值直接写成 MessagePack 整数 / 字符串，不带 `"Num"` / `"Text"` 变体标签。
    /// Test that header values are bare MessagePack integers / strings, without variant tags.
    ///
    /// - 手段：构造 `{0xA1: Text("12"), 0xA2: Num(1)}` 的回复前缀并编码。
    /// - 判断：字节为
    ///   `cc c8 82 cc a1 a2 31 32 cc a2 01`——状态 200、头表两项，两个值分别是裸字符串与
    ///   裸整数。若仍走外部标签，会看到 `"Text"` / `"Num"` 的字符串片段。
    ///
    /// - Means: build a response prefix carrying `{0xA1: Text("12"), 0xA2: Num(1)}` and
    ///   encode it.
    /// - Verdict: the bytes are `cc c8 82 cc a1 a2 31 32 cc a2 01` — status 200, two header
    ///   entries, one bare string and one bare integer. An externally tagged representation
    ///   would contain the literal `"Text"` / `"Num"` strings.
    #[test]
    fn response_prefix_headers_are_bare_values_() {
        let mut headers = Headers::new();
        headers.set_text(StdHeaderKey::Body_Size, "12");
        headers.set_num(
            StdHeaderKey::Body_Type,
            StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
        );
        let prefix = ResponsePrefix::with_headers(Status::Ok, Option::Some(headers));

        let mut storage = [0u8; 64usize];
        let cap = storage.len();
        let mut sink: &mut [u8] = storage.as_mut_slice();
        block_on_(
            write_response_prefix_async(&prefix, &mut sink)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("回复前缀编码应当成功");
        let written = cap - sink.len();

        assert_eq!(
            &storage[..written],
            &[
                0xcc, 0xc8, // status 200
                0x82, // headers: 2 entries
                0xcc, 0xa1, 0xa2, b'1', b'2', // Body_Size: "12"
                0xcc, 0xa2, 0x01, // Body_Type: 1
            ]
        );
    }

    /// 测试头值解码接受「正数用有符号整数编码」的等价表示。
    /// Test that header-value decoding accepts positive values encoded as signed integers.
    ///
    /// - 手段：直接喂入 `cc c8 81 cc a1 d1 00 01`——状态 200，头表一项，值用 `int16` 表示 1。
    /// - 判断：解出的 `Body_Size` 是 `Num(1)`。MessagePack 的整数宽度与符号类型不进入语义，
    ///   只要数值落在 `u16` 范围内就应当接受。
    ///
    /// - Means: feed `cc c8 81 cc a1 d1 00 01` — status 200, one header entry whose value is
    ///   the `int16` form of 1.
    /// - Verdict: the decoded `Body_Size` is `Num(1)`. MessagePack integer width and
    ///   signedness carry no semantics; any value within `u16` must be accepted.
    #[test]
    fn header_value_accepts_signed_positive_() {
        let bytes = [
            0xccu8, 0xc8u8, 0x81u8, 0xccu8, 0xa1u8, 0xd1u8, 0x00u8, 0x01u8,
        ];
        let mut input: &[u8] = &bytes;
        let decoded = block_on_(
            read_response_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
        )
        .expect("有符号正整数的头值应当可解");

        assert_eq!(
            decoded
                .headers()
                .expect("应该有头表")
                .try_get_val(StdHeaderKey::Body_Size),
            Option::Some(&HeaderVal::Num(1u16))
        );
        assert!(input.is_empty());
    }

    /// 测试带头表的回复前缀能完整往返，且头值形态不变。
    /// Test that a response prefix with headers round-trips with header forms intact.
    ///
    /// - 手段：构造带 `Body_Size` 文本头、`Body_Type` 数字头与自定义数字键的表，编码后解码。
    /// - 判断：状态、整张头表与自定义键都相等；文本仍是文本、数字仍是数字，没有被归一成
    ///   同一种形态。
    ///
    /// - Means: build a map with a textual `Body_Size`, a numeric `Body_Type` and a custom
    ///   numeric key, encode it, then decode it back.
    /// - Verdict: status, the whole map and the custom key all compare equal; text stays text
    ///   and numbers stay numbers.
    #[test]
    fn response_prefix_headers_roundtrip_() {
        let mut headers = Headers::new();
        headers.set_text(StdHeaderKey::Body_Size, "12");
        headers.set_num(
            StdHeaderKey::Body_Type,
            StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
        );
        headers.set_num(StdHeaderKey::new(0x01), 7u16);

        let prefix = ResponsePrefix::with_headers(Status::Created, Option::Some(headers));
        let mut storage = [0u8; 128usize];
        let cap = storage.len();
        let mut sink: &mut [u8] = storage.as_mut_slice();
        block_on_(
            write_response_prefix_async(&prefix, &mut sink)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("回复前缀编码应当成功");
        let written = cap - sink.len();

        let mut input: &[u8] = &storage[..written];
        let decoded = block_on_(
            read_response_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
        )
        .expect("回复前缀解码应当成功");

        assert_eq!(decoded, prefix);
        assert_eq!(decoded.status(), Status::Created);
        assert_eq!(
            decoded
                .headers()
                .expect("应该有头表")
                .try_get_val(StdHeaderKey::new(0x01)),
            Option::Some(&HeaderVal::Num(7u16))
        );
    }

    /// 测试任意 `u16` 状态码都能往返（状态是开放数值，不是封闭枚举）。
    /// Test that every `u16` status round-trips: status is an open numeric value.
    ///
    /// - 手段：取 `0`、`599`、`65535` 三个不在常用集合里的状态编码再解码。
    /// - 判断：解出的状态数值与原值完全相等。若实现把未知状态折叠成某个近似值，这条断言
    ///   会立刻失败。
    ///
    /// - Means: encode and decode the statuses `0`, `599` and `65535`, none of which belongs
    ///   to the common set.
    /// - Verdict: each decoded status equals the original number; folding an unknown status
    ///   into a nearby value would fail here.
    #[test]
    fn response_status_is_open_() {
        for code in [0u16, 599u16, u16::MAX] {
            let prefix = ResponsePrefix::new(Status::new(code));
            let mut storage = [0u8; 16usize];
            let cap = storage.len();
            let mut sink: &mut [u8] = storage.as_mut_slice();
            block_on_(
                write_response_prefix_async(&prefix, &mut sink)
                    .may_cancel_with(NonCancellableToken::new()),
            )
            .expect("回复前缀编码应当成功");
            let written = cap - sink.len();

            let mut input: &[u8] = &storage[..written];
            let decoded = block_on_(
                read_response_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
            )
            .expect("回复前缀解码应当成功");
            assert_eq!(decoded.status(), Status::new(code));
        }
    }

    /// 测试解码器接受「固定 u16 宽度」的状态码，与旧 `rmp-serde` 写出的字节互通。
    /// Test that the decoder accepts a fixed-width `u16` status, interoperating with old
    /// `rmp-serde` bytes.
    ///
    /// - 手段：直接喂入 `cd 00 c8 c0`——`rmp-serde` 对 `u16` 200 的固定宽度表示加上 `nil` 头。
    /// - 判断：解出 `Status::Ok` 且没有头表。`mp_denco` 自己写的是最短形态 `cc c8`，但
    ///   MessagePack 的整数宽度不进入语义；这条用例保证新旧实现仍能对话。
    ///
    /// - Means: feed `cd 00 c8 c0` — `rmp-serde`'s fixed-width form of `u16` 200 followed by
    ///   nil headers.
    /// - Verdict: `Status::Ok` with no header map. `mp_denco` itself writes the shortest form
    ///   `cc c8`, but integer width carries no semantics; this pins old/new interoperability.
    #[test]
    fn response_prefix_accepts_fixed_width_status_() {
        let bytes = [0xcdu8, 0x00u8, 0xc8u8, 0xc0u8];
        let mut input: &[u8] = &bytes;
        let decoded = block_on_(
            read_response_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
        )
        .expect("固定宽度的状态码应当可解");

        assert_eq!(decoded.status(), Status::Ok);
        assert_eq!(decoded.headers(), Option::None);
        assert!(input.is_empty());
    }

    /// 测试未知方法位**明确失败**，不被折叠成某个已知方法。
    /// Test that an unknown method bit fails loudly instead of folding into a known method.
    ///
    /// - 手段：把 `0x7f` 作为方法位（后面跟一个空位置与 `nil` 头）喂给请求前缀解码。
    /// - 判断：返回 `Err(MessagingError::Unknown)`。线上契约不认识就不能猜——把 `0x7f`
    ///   折成 `Head` 会让服务端把一次未知访问当成元信息查询。
    ///
    /// - Means: feed `0x7f` as the method bit (followed by an empty location and nil headers)
    ///   to the request-prefix decoder.
    /// - Verdict: `Err(MessagingError::Unknown)`. An unrecognised wire contract is never
    ///   guessed: folding `0x7f` into `Head` would turn an unknown access into a metadata read.
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

    /// 测试「没有头」与「空头表」在线上是两种不同表示，解码后仍可区分。
    /// Test that "no headers" and "an empty map" are distinct on the wire and after decoding.
    ///
    /// - 手段：分别编码无头的请求与带头表（空表）的请求，再解码。
    /// - 判断：前者 `headers()` 为 `None`，后者为 `Some` 且 `is_empty` 为真。把空表折成
    ///   `nil` 会让「有没有头表」这条协议区分消失。
    ///
    /// - Means: encode one request without headers and one carrying an empty map, then decode
    ///   both.
    /// - Verdict: the first reports `None`, the second `Some` with `is_empty()` true; folding
    ///   an empty map into nil would erase the protocol-level distinction.
    #[test]
    fn absent_and_empty_headers_stay_distinct_() {
        let absent = RequestPrefix::new(AccessMethod::Head, "/x");
        let empty =
            RequestPrefix::with_headers(AccessMethod::Head, "/x", Option::Some(Headers::new()));

        let decode = |prefix: &RequestPrefix| {
            let mut storage = [0u8; 64usize];
            let cap = storage.len();
            let mut sink: &mut [u8] = storage.as_mut_slice();
            block_on_(
                write_request_prefix_async(prefix, &mut sink)
                    .may_cancel_with(NonCancellableToken::new()),
            )
            .expect("请求前缀编码应当成功");
            let written = cap - sink.len();

            let mut input: &[u8] = &storage[..written];
            block_on_(
                read_request_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
            )
            .expect("请求前缀解码应当成功")
        };

        assert_eq!(decode(&absent).headers(), Option::None);
        let decoded_empty = decode(&empty);
        let headers = decoded_empty.headers().expect("空头表仍是 Some");
        assert!(headers.is_empty());
    }
}
