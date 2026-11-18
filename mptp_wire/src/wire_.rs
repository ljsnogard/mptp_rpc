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
//! 多写一个数组头，直接改变线上字节。头表本身是「数字或字符串键 → 数字或 `[type_id, bin]`
//! 值」的映射，没有头时写 `nil`。
//!
//! # 两个解码入口族
//!
//! - [`read_request_prefix_async`] / [`read_response_prefix_async`] 用 [`NoArena`] 解码，
//!   返回 allocator 为 [`Global`] 的前缀；
//! - [`read_request_prefix_in_async`] / [`read_response_prefix_in_async`] 接收一个
//!   `&Alloc` arena，把 `location`、头键与头值都分配在同一个 arena 里，返回 allocator 为
//!   `&'alloc Alloc` 的前缀。后者要求 `Alloc: TrArena<'alloc> + Allocator`。
//!
//! # 只读前缀，不碰体
//!
//! 读入口把 [`MpDecoder`] 直接架在接收半边上，要多少借多少；解完最后一个头就返回，报文体的
//! 第一个字节仍在环里。
//!
//! # `wire_`: the prefix wire format
//!
//! Four entry points, one format. Requests are `method → location → headers`; responses are
//! `status → headers`, each as a **separate** MessagePack value. The `_in_` variants decode
//! into a caller-supplied arena; the plain variants decode into `Global`.

use core::alloc::{Allocator, AllocatorClone};

use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{TrAllocSource, TrArena, TrDecodable},
    ser::TrEncodable,
};
use abs_mptp::spec::{Status, access_method::AccessMethod};
use abs_str::str_alloc::String as AllocString;
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
/// 返回的 future 在 `.may_cancel_with(token)` 之后才携带取消令牌。
///
/// # Errors
///
/// 编码失败、写子流失败，或等待期间被取消时返回 [`MessagingError::Encode`]。
pub fn write_request_prefix_async<'f, A, W>(
    alloc: &'f A,
    prefix: &'f RequestPrefix<A>,
    tx: &'f mut W,
) -> WriteRequestPrefixAsync<'f, 'f, A, W>
where
    A: AllocatorClone,
    W: 'f + TrBuffWrite<u8>,
{
    WriteRequestPrefixAsync::new(alloc, prefix, tx)
}

/// [`write_request_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteRequestPrefix, pub, new(pub(crate)))]
async fn write_request_prefix_async_<'f, A, W, K>(
    alloc: &'f A,
    prefix: &'f RequestPrefix<A>,
    tx: &'f mut W,
    cancel: K,
) -> Result<(), MessagingError>
where
    A: AllocatorClone,
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    // 三个值**依次编进同一个写口**：线上格式就是「三个连续的 MessagePack 值」。
    (prefix.method_ as u8)
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Encode)?;
    prefix
        .location()
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Encode)?;
    prefix
        .headers_
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel)
        .await
        .map_err(MessagingError::Encode)?;
    Result::Ok(())
}


/// [`read_request_prefix_in_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadRequestPrefixIn, pub, new(pub(crate)))]
async fn read_request_prefix_in_async_<'f, 'alloc, R, A, K>(
    rx: &'f mut R,
    arena: &'alloc A,
    cancel: K,
) -> Result<RequestPrefix<&'alloc A>, MessagingError>
where
    R: TrBuffRead<u8>,
    A: TrArena<'alloc> + Allocator + 'alloc,
    K: TrCancellationToken,
{
    let code = u8::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let method_ = method_from_u8_(code).ok_or(MessagingError::UnknownMethod { code })?;
    let location_ = AllocString::<&'alloc A>::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let headers_ =
        Option::<Headers<&'alloc A>>::decode_async(MpDecoder::new_in(&mut *rx, arena))
            .may_cancel_with(cancel)
            .await
            .map_err(MessagingError::Decode)?;
    Result::Ok(RequestPrefix::from_parts_(
        method_, location_, headers_,
    ))
}

/// 从接收半边解出一个请求前缀，所有堆内存都由 `arena` 分配，但返回**拥有** allocator
/// `A` 的前缀。
///
/// 与 [`read_request_prefix_in_async`] 的差别：这里的前缀把 `arena` 克隆一份存进
/// `RequestPrefix<A>`，因此不需要把 arena 的生命周期带进会话类型；适合 session / client
/// 持有可克隆 allocator 的场景。
///
/// # Errors
///
/// 同 [`read_request_prefix_async`]。
pub fn read_request_prefix_with_async<'f, 'alloc, R, A>(
    rx: &'f mut R,
    arena: &'alloc A,
) -> ReadRequestPrefixWithAsync<'f, 'alloc, 'f, R, A>
where
    R: 'f + TrBuffRead<u8>,
    A: TrArena<'alloc> + AllocatorClone + TrAllocSource<'alloc, A> + 'alloc + 'f,
{
    ReadRequestPrefixWithAsync::new(rx, arena)
}

/// [`read_request_prefix_with_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadRequestPrefixWith, pub, new(pub(crate)))]
async fn read_request_prefix_with_async_<'f, 'alloc, R, A, TyTok>(
    rx: &'f mut R,
    arena: &'alloc A,
    cancel: TyTok,
) -> Result<RequestPrefix<A>, MessagingError>
where
    R: TrBuffRead<u8>,
    A: TrArena<'alloc> + AllocatorClone + TrAllocSource<'alloc, A> + 'alloc + 'f,
    'alloc: 'f,
    TyTok: TrCancellationToken,
{
    let code = u8::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let method_ = method_from_u8_(code).ok_or(MessagingError::UnknownMethod { code })?;
    let location_ = AllocString::<A>::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let headers_ = Option::<Headers<A>>::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel)
        .await
        .map_err(MessagingError::Decode)?;
    Result::Ok(RequestPrefix::from_parts_(
        method_,
        location_,
        headers_,
    ))
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 回复前缀
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把回复前缀按 `status → headers` 写出去。
///
/// # Errors
///
/// 编码失败、写子流失败，或等待期间被取消时返回 [`MessagingError::Encode`]。
pub fn write_response_prefix_async<'f, A, W>(
    prefix: &'f ResponsePrefix<A>,
    tx: &'f mut W,
) -> WriteResponsePrefixAsync<'f, 'f, A, W>
where
    A: AllocatorClone,
    W: 'f + TrBuffWrite<u8>,
{
    WriteResponsePrefixAsync::new(prefix, tx)
}

/// [`write_response_prefix_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(WriteResponsePrefix, pub, new(pub(crate)))]
async fn write_response_prefix_async_<'f, A, W, K>(
    prefix: &'f ResponsePrefix<A>,
    tx: &'f mut W,
    cancel: K,
) -> Result<(), MessagingError>
where
    A: AllocatorClone,
    W: TrBuffWrite<u8>,
    K: TrCancellationToken,
{
    prefix
        .status_
        .inner()
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Encode)?;
    prefix
        .headers_
        .encode_async(MpEncoder::new(&mut *tx))
        .may_cancel_with(cancel)
        .await
        .map_err(MessagingError::Encode)?;
    Result::Ok(())
}

/// [`read_response_prefix_in_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadResponsePrefixIn, pub, new(pub(crate)))]
async fn read_response_prefix_in_async_<'f, 'alloc, R, A, K>(
    rx: &'f mut R,
    arena: &'alloc A,
    cancel: K,
) -> Result<ResponsePrefix<&'alloc A>, MessagingError>
where
    R: TrBuffRead<u8>,
    A: TrArena<'alloc> + Allocator + 'alloc,
    K: TrCancellationToken,
{
    let code = u16::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let status = Status::new(code);
    let headers =
        Option::<Headers<&'alloc A>>::decode_async(MpDecoder::new_in(&mut *rx, arena))
            .may_cancel_with(cancel)
            .await
            .map_err(MessagingError::Decode)?;
    Result::Ok(ResponsePrefix::from_parts_(status, headers))
}

/// 从接收半边解出一个回复前缀，所有堆内存都由 `arena` 分配，但返回**拥有** allocator
/// `A` 的前缀。
///
/// # Errors
///
/// 同 [`read_response_prefix_async`]。
pub fn read_response_prefix_with_async<'f, 'alloc, R, A>(
    rx: &'f mut R,
    arena: &'alloc A,
) -> ReadResponsePrefixWithAsync<'f, 'alloc, 'f, R, A>
where
    R: 'f + TrBuffRead<u8>,
    A: TrArena<'alloc> + AllocatorClone + TrAllocSource<'alloc, A> + 'alloc + 'f,
{
    ReadResponsePrefixWithAsync::new(rx, arena)
}

/// [`read_response_prefix_with_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ReadResponsePrefixWith, pub, new(pub(crate)))]
async fn read_response_prefix_with_async_<'f, 'alloc, R, A, K>(
    rx: &'f mut R,
    arena: &'alloc A,
    cancel: K,
) -> Result<ResponsePrefix<A>, MessagingError>
where
    R: TrBuffRead<u8>,
    A: TrArena<'alloc> + AllocatorClone + TrAllocSource<'alloc, A> + 'alloc + 'f,
    'alloc: 'f,
    K: TrCancellationToken,
{
    let code = u16::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(MessagingError::Decode)?;
    let status_ = Status::new(code);
    let headers_ = Option::<Headers<A>>::decode_async(MpDecoder::new_in(&mut *rx, arena))
        .may_cancel_with(cancel)
        .await
        .map_err(MessagingError::Decode)?;
    Result::Ok(ResponsePrefix::from_parts_(
        status_,
        headers_,
    ))
}

#[cfg(test)]
mod tests_ {
    //! 四个线格式入口的往返、字节顺序与「不碰体」性质。
    //!
    //! Round-trip, wire order and the "never touch the body" property of the four entry
    //! points.

    use alloc::alloc::Global;
    use core::{
        future::Future,
        pin::pin,
        task::{Context, Poll, Waker},
    };

    use abs_cancel::NonCancellableToken;
    use abs_mptp::spec::{StdHeaderKey, StdHeaderVal};

    use super::*;
    use crate::{HeaderKey, HeaderVal};

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

    /// 测试用的 arena：分配走 `Global`，`TrArena` 的 `Ref<T>` 是泄漏出来的 `&mut T`。
    ///
    /// 只在测试里使用；它模拟 `BumpListAlloc` 那种「只借出、不回收」的 arena，用来验证
    /// `_in_` 解码路径确实把字符串与头表分配进调用方给的 arena。
    struct TestArena;

    // SAFETY: 直接转发给 `Global`；deallocate 的契约由 `Global` 保证。
    unsafe impl Allocator for TestArena {
        fn allocate(
            &self,
            layout: core::alloc::Layout,
        ) -> Result<core::ptr::NonNull<[u8]>, core::alloc::AllocError> {
            Global.allocate(layout)
        }

        unsafe fn deallocate(&self, ptr: core::ptr::NonNull<u8>, layout: core::alloc::Layout) {
            // SAFETY: 调用方保证 ptr/layout 来自本分配器，而本分配器转发给 `Global`。
            unsafe { Global.deallocate(ptr, layout) }
        }
    }

    impl<'alloc> TrArena<'alloc> for TestArena {
        type Error = core::convert::Infallible;

        type Ref<T: 'alloc>
            = &'alloc mut T
        where
            Self: 'alloc;

        type AllocValueAsync<'a, T>
            = core::future::Ready<Result<&'alloc mut T, core::convert::Infallible>>
        where
            'alloc: 'a,
            T: 'alloc + 'a,
            Self: 'alloc + 'a;

        fn alloc_value_async<'a, T>(&'alloc self, value: T) -> Self::AllocValueAsync<'a, T>
        where
            'alloc: 'a,
            T: 'alloc + 'a,
            Self: 'alloc + 'a,
        {
            core::future::ready(Result::Ok(alloc::boxed::Box::leak(alloc::boxed::Box::new(
                value,
            ))))
        }
    }

    /// 测试 `_in_` 解码把字符串与头表分配进调用方给的 arena。
    /// Test that `_in_` decoding allocates strings and the header map in the supplied arena.
    ///
    /// - 手段：编码一个带数字头与对象头的请求前缀，再用 `TestArena` 作为 arena 调
    ///   `read_request_prefix_in_async`。
    /// - 判断：解出的方法 / 位置 / 头值与原值一致；返回类型的前缀 allocator 是
    ///   `&TestArena`，证明所有堆内存都来自该 arena 而不是 `Global`。
    ///
    /// - Means: encode a request prefix with numeric and object headers, then decode it with
    ///   `read_request_prefix_in_async` using `TestArena`.
    /// - Verdict: method / location / header values match, and the returned prefix's
    ///   allocator is `&TestArena`, proving the heap memory came from that arena.
    #[test]
    fn request_prefix_decodes_into_supplied_arena_() {
        let mut headers = Headers::new_in(Global);
        headers.set_num(StdHeaderKey::Body_Size, 12);
        headers.set(
            HeaderKey::from(StdHeaderKey::Cookie),
            HeaderVal::from_bytes(0x1234, b"v", Global),
        );
        let prefix = RequestPrefix::with_headers(
            AccessMethod::Call,
            "/arena",
            Option::Some(headers),
            Global,
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

        let arena = TestArena;
        let mut input: &[u8] = &storage[..written];
        let decoded: RequestPrefix<&TestArena> = block_on_(
            read_request_prefix_in_async(&mut input, &arena)
                .may_cancel_with(NonCancellableToken::new()),
        )
        .expect("arena 解码应当成功");

        assert_eq!(decoded.method(), AccessMethod::Call);
        assert_eq!(decoded.location(), "/arena");
        assert!(input.is_empty());
        let headers = decoded.headers().expect("应该有头表");
        assert_eq!(headers.body_size(), Option::Some(12usize));
        assert_eq!(
            headers
                .try_get_val(StdHeaderKey::Cookie)
                .and_then(HeaderVal::data),
            Option::Some(&b"v"[..])
        );
    }

    /// 测试请求前缀的线上字节就是「方法 → 位置 → 头」三个连续值。
    /// Test that a request prefix is exactly three consecutive values in wire order.
    ///
    /// - 手段：以 `View` + `"/x"` + 无头编码，随后读出被写走的字节。
    /// - 判断：字节为 `01 a2 2f 78 c0`。
    ///
    /// - Means: encode `View` + `"/x"` + no headers and read back the bytes written.
    /// - Verdict: `01 a2 2f 78 c0`.
    #[test]
    fn request_prefix_wire_order_() {
        let prefix = RequestPrefix::new(AccessMethod::View, "/x", Global);
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
    /// - 判断：字节为 `cc c8 c0`。
    ///
    /// - Means: encode `Status::Ok` + no headers and read back the bytes.
    /// - Verdict: `cc c8 c0`.
    #[test]
    fn response_prefix_wire_order_() {
        let prefix = ResponsePrefix::new(Status::Ok, Global);
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
    /// - 手段：把一段合法的请求前缀与两个体字节写在同一个切片里，只调用前缀解码。
    /// - 判断：解出的前缀与原值相等，且输入恰好剩下 `[0xAA, 0xBB]`。
    ///
    /// - Means: put a valid request prefix and two body bytes in one slice, decode only the
    ///   prefix.
    /// - Verdict: the decoded prefix equals the original and the input is exactly
    ///   `[0xAA, 0xBB]`.
    #[test]
    fn request_prefix_leaves_body_untouched_() {
        let prefix = RequestPrefix::with_headers(
            AccessMethod::Call,
            "/rpc/echo",
            Option::Some(Headers::new_in(Global)),
            Global,
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

    /// 测试头值是「裸整数」或「`[type_id, bin]` 数组」，不带变体标签。
    /// Test that header values are bare integers or `[type_id, bin]` arrays.
    ///
    /// - 手段：构造带 `Body_Size: 12`、`Body_Type: 1` 与 `Cookie: [0x1234, bin("v")]`
    ///   的回复前缀并编码。
    /// - 判断：字节里没有 `"Num"` / `"Obj"` 之类的变体标签；数字直接写整数，对象写两元素
    ///   数组。
    ///
    /// - Means: encode a response prefix carrying `Body_Size: 12`, `Body_Type: 1` and
    ///   `Cookie: [0x1234, bin("v")]`.
    /// - Verdict: the bytes contain no `"Num"` / `"Obj"` variant tags; numbers are bare
    ///   integers and objects are two-element arrays.
    #[test]
    fn response_prefix_obj_value_wire_form_() {
        let mut headers = Headers::new_in(Global);
        headers.set_num(StdHeaderKey::Body_Size, 12);
        headers.set_num(
            StdHeaderKey::Body_Type,
            StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
        );
        headers.set(
            HeaderKey::from(StdHeaderKey::Cookie),
            HeaderVal::from_bytes(0x1234, b"v", Global),
        );
        let prefix = ResponsePrefix::with_headers(Status::Ok, Option::Some(headers), Global);

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
                0x83, // headers: 3 entries
                0xcc, 0xa1, 0x0c, // Body_Size: 12
                0xcc, 0xa2, 0x01, // Body_Type: 1
                0xcc, 0xb3, // Cookie key
                0x92, 0xcd, 0x12, 0x34, // [type_id = 0x1234,
                0xc4, 0x01, b'v', // bin("v")]
            ]
        );
    }

    /// 测试头值解码接受「正数用有符号整数编码」的等价表示。
    /// Test that header-value decoding accepts positive values encoded as signed integers.
    ///
    /// - 手段：直接喂入 `cc c8 81 cc a1 d1 00 01`。
    /// - 判断：解出的 `Body_Size` 是 `Num(1)`。
    ///
    /// - Means: feed `cc c8 81 cc a1 d1 00 01`.
    /// - Verdict: the decoded `Body_Size` is `Num(1)`.
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
            Option::Some(&HeaderVal::from_num(1u16))
        );
        assert!(input.is_empty());
    }

    /// 测试带头表的回复前缀能完整往返。
    /// Test that a response prefix with headers round-trips.
    ///
    /// - 手段：构造带数字头、对象头与自定义数字键的表，编码后解码。
    /// - 判断：状态、整张头表与自定义键都相等。
    ///
    /// - Means: build a map with numeric headers, an object header and a custom numeric key,
    ///   encode it, then decode it back.
    /// - Verdict: status, the whole map and the custom key all compare equal.
    #[test]
    fn response_prefix_headers_roundtrip_() {
        let mut headers = Headers::new_in(Global);
        headers.set_num(StdHeaderKey::Body_Size, 12);
        headers.set_num(
            StdHeaderKey::Body_Type,
            StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
        );
        headers.set(
            HeaderKey::from(StdHeaderKey::Cookie),
            HeaderVal::from_bytes(0xabc, b"payload", Global),
        );
        headers.set_num(StdHeaderKey::new(0x01), 7u16);

        let prefix = ResponsePrefix::with_headers(Status::Created, Option::Some(headers), Global);
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
            Option::Some(&HeaderVal::from_num(7u16))
        );
    }

    /// 测试字符串头键能随回复前缀完整往返。
    /// Test that a string header key round-trips through a response prefix.
    ///
    /// - 手段：构造一张同时带标准数字头与 `HeaderKey::from_text("x-custom")` 的表。
    /// - 判断：整张头表相等，且字符串键能按原键取回。
    ///
    /// - Means: build a map with a standard numeric header and
    ///   `HeaderKey::from_text("x-custom")`.
    /// - Verdict: the whole map compares equal and the string key resolves again.
    #[test]
    fn string_header_key_roundtrips_() {
        let key = HeaderKey::from_text("x-custom", Global);
        let mut headers = Headers::new_in(Global);
        headers.set_num(StdHeaderKey::Body_Size, 12);
        headers.set(key.clone(), HeaderVal::from_bytes(0x1234, b"v", Global));

        let prefix = ResponsePrefix::with_headers(Status::Ok, Option::Some(headers), Global);
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
        assert_eq!(
            decoded.headers().expect("应该有头表").try_get(key),
            Option::Some(&HeaderVal::from_bytes(0x1234, b"v", Global))
        );
    }

    /// 测试任意 `u16` 状态码都能往返。
    /// Test that every `u16` status round-trips.
    ///
    /// - 手段：取 `0`、`599`、`65535` 三个不在常用集合里的状态编码再解码。
    /// - 判断：解出的状态数值与原值完全相等。
    ///
    /// - Means: encode and decode the statuses `0`, `599` and `65535`.
    /// - Verdict: each decoded status equals the original number.
    #[test]
    fn response_status_is_open_() {
        for code in [0u16, 599u16, u16::MAX] {
            let prefix = ResponsePrefix::new(Status::new(code), Global);
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

    /// 测试解码器接受「固定 u16 宽度」的状态码。
    /// Test that the decoder accepts a fixed-width `u16` status.
    ///
    /// - 手段：直接喂入 `cd 00 c8 c0`。
    /// - 判断：解出 `Status::Ok` 且没有头表。
    ///
    /// - Means: feed `cd 00 c8 c0`.
    /// - Verdict: `Status::Ok` with no header map.
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
    /// - 判断：返回 `Err(MessagingError::UnknownMethod)`。
    ///
    /// - Means: feed `0x7f` as the method bit (followed by an empty location and nil
    ///   headers).
    /// - Verdict: `Err(MessagingError::UnknownMethod)`.
    #[test]
    fn unknown_method_fails_loudly_() {
        let bytes = [0x7fu8, 0xa0u8, 0xc0u8];
        let mut input: &[u8] = &bytes;
        let result = block_on_(
            read_request_prefix_async(&mut input).may_cancel_with(NonCancellableToken::new()),
        );

        assert!(
            matches!(result, Result::Err(MessagingError::UnknownMethod { .. })),
            "未知方法位应当报 Unknown，实际是 {result:?}"
        );
    }

    /// 测试「没有头」与「空头表」在线上是两种不同表示，解码后仍可区分。
    /// Test that "no headers" and "an empty map" stay distinct on the wire and after
    /// decoding.
    ///
    /// - 手段：分别编码无头的请求与带头表（空表）的请求，再解码。
    /// - 判断：前者 `headers()` 为 `None`，后者为 `Some` 且 `is_empty` 为真。
    ///
    /// - Means: encode one request without headers and one carrying an empty map, then
    ///   decode both.
    /// - Verdict: the first reports `None`, the second `Some` with `is_empty()` true.
    #[test]
    fn absent_and_empty_headers_stay_distinct_() {
        let absent = RequestPrefix::new(AccessMethod::Head, "/x", Global);
        let empty = RequestPrefix::with_headers(
            AccessMethod::Head,
            "/x",
            Option::Some(Headers::new_in(Global)),
            Global,
        );

        let decode = |prefix: &RequestPrefix<Global>| {
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
