//! 头键的「u16 或字符串」表示：`abs_mptp::spec::NumOrStr` 的 u16 特化复刻。
//!
//! `U16OrStr<A>` 把数字形态固定为 `u16`，字符串形态固定为 `abs_str::String<A>`；后者由
//! 调用方给出的 `A: Allocator` 分配，因此本类型自身只要求 `A: Allocator`，不需要
//! `Global`，也不需要 `Default`。它直接实现 `abs_mptp::spec::TrNumOrStr`，因此可以作为
//! [`TrHeaders::Key`](abs_mptp::spec::TrHeaders::Key) 使用。
//!
//! 头键的 MessagePack 表示为「数字写整数、字符串写字符串」，不写变体标签；这里的
//! [`TrEncodable`] / [`TrDecodable`] 手写实现负责保持这个线上形状。解码时字符串经
//! [`TrAllocSource`] 从 decoder 的 arena 投影出的 allocator 分配。
//!
//! # `num_or_str_`: the u16-specialised numeric-or-string key
//!
//! `U16OrStr<A>` is the u16-specialised replica of `abs_mptp::spec::NumOrStr`. Its textual
//! form is `abs_str::String<A>`, so the type itself only requires `A: Allocator`. Numeric
//! keys are written as MessagePack integers and string keys as MessagePack strings, without
//! variant tags.

use core::{
    alloc::Allocator,
    cmp::Ordering,
    fmt, future,
    hash::{Hash, Hasher},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{
        Error as DencoError, TrAllocSource, TrArena, TrDecodable, TrDecoder, Unexpected, Visitor,
    },
    ser::{TrEncodable, TrEncoder},
};
use abs_mptp::spec::{NumOrStr as AbsNumOrStr, StdHeaderKey, TrNumOrStr as AbsTrNumOrStr};
use abs_str::str_alloc::String as AllocString;
use gen_mcf2::gen_may_cancel_future;

/// 一个头键：要么是协议规定的 `u16` 数字，要么是使用者给出的字符串。
///
/// 这是 `abs_mptp::spec::NumOrStr` 的 u16 特化复刻：数字形态固定为 `u16`，字符串形态
/// 固定为 `abs_str::String<A>`。字符串由 `A` 分配，因此类型本身只要求 `A: Allocator`；
/// 构造字符串键与解码时还要求 `A: Clone` / `A: TrAllocSource`。
///
/// # Examples
///
/// ```
/// use mptp_wire::U16OrStr;
/// use std::alloc::Global;
///
/// let numeric: U16OrStr<Global> = U16OrStr::U16(0xA1);
/// let textual = U16OrStr::from_text("x-custom", Global);
///
/// assert!(matches!(numeric, U16OrStr::U16(0xA1)));
/// assert!(matches!(textual, U16OrStr::Str(_)));
/// ```
///
/// A header key: either the protocol's `u16` number or a string allocated by `A`.
pub enum U16OrStr<A: Allocator> {
    /// 数字形态。
    U16(u16),

    /// 字符串形态。
    Str(AllocString<A>),
}

impl<A: Allocator + Clone> Clone for U16OrStr<A> {
    fn clone(&self) -> Self {
        match self {
            U16OrStr::U16(num) => U16OrStr::U16(*num),
            U16OrStr::Str(text) => U16OrStr::Str(text.clone()),
        }
    }
}

impl<A: Allocator> PartialEq for U16OrStr<A> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (U16OrStr::U16(left), U16OrStr::U16(right)) => left == right,
            (U16OrStr::Str(left), U16OrStr::Str(right)) => **left == **right,
            _ => false,
        }
    }
}

impl<A: Allocator> Eq for U16OrStr<A> {}

impl<A: Allocator> PartialOrd for U16OrStr<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl<A: Allocator> Ord for U16OrStr<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (U16OrStr::U16(left), U16OrStr::U16(right)) => left.cmp(right),
            (U16OrStr::U16(_), U16OrStr::Str(_)) => Ordering::Less,
            (U16OrStr::Str(_), U16OrStr::U16(_)) => Ordering::Greater,
            (U16OrStr::Str(left), U16OrStr::Str(right)) => (**left).cmp(&**right),
        }
    }
}

impl<A: Allocator> Hash for U16OrStr<A> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            U16OrStr::U16(num) => {
                0u8.hash(state);
                num.hash(state);
            }
            U16OrStr::Str(text) => {
                1u8.hash(state);
                (**text).hash(state);
            }
        }
    }
}

impl<A: Allocator> fmt::Debug for U16OrStr<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            U16OrStr::U16(num) => formatter.debug_tuple("U16").field(num).finish(),
            U16OrStr::Str(text) => formatter.debug_tuple("Str").field(&(&**text)).finish(),
        }
    }
}

impl<A: Allocator> U16OrStr<A> {
    /// 由裸 `u16` 构造数字形态（不校验它是否是标准键）。
    ///
    /// Build the numeric form from a bare `u16` without checking its standard meaning.
    pub const fn from_num(num: u16) -> Self {
        U16OrStr::U16(num)
    }

    /// 由标准头键构造数字形态。
    ///
    /// Build the numeric form from a standard header key.
    pub fn from_header_key(key: StdHeaderKey) -> Self {
        U16OrStr::U16(u16::from(key))
    }

    /// 借用内部表示（`HeaderKey` 与 `U16OrStr` 是同一个类型，保留此入口以兼容旧调用）。
    ///
    /// Borrow the inner representation; `HeaderKey` and `U16OrStr` are the same type, so
    /// this is kept for compatibility with old call sites.
    pub const fn as_u16_or_str(&self) -> &Self {
        self
    }

    /// 取出内部表示（`HeaderKey` 与 `U16OrStr` 是同一个类型）。
    ///
    /// Take the inner representation; `HeaderKey` and `U16OrStr` are the same type.
    pub fn into_inner(self) -> Self {
        self
    }

    /// 把本键复制到另一个 allocator 上；数字形态不需要分配，字符串形态重新分配。
    ///
    /// Copy this key onto another allocator. Numeric keys need no allocation; textual keys
    /// are re-allocated by `alloc`.
    ///
    /// # Examples
    ///
    /// ```
    /// use mptp_wire::U16OrStr;
    /// use std::alloc::Global;
    ///
    /// let key = U16OrStr::<Global>::from_text("x-custom", Global);
    /// let same = key.realloc_in(&Global);
    /// assert_eq!(same, U16OrStr::from_text("x-custom", &Global));
    /// ```
    pub fn realloc_in<B: Allocator + Clone>(&self, alloc: B) -> U16OrStr<B> {
        match self {
            U16OrStr::U16(num) => U16OrStr::U16(*num),
            U16OrStr::Str(text) => U16OrStr::from_text(text, alloc),
        }
    }
}

impl<A: Allocator + Clone> U16OrStr<A> {
    /// 由一段文本与 allocator 构造字符串形态。
    ///
    /// Build the textual form from a string slice and an allocator instance.
    pub fn from_text(text: &str, alloc: A) -> Self {
        U16OrStr::Str(AllocString::from_str_in(text, alloc))
    }
}

impl<A: Allocator> AbsTrNumOrStr for U16OrStr<A> {
    type NumType = u16;

    type StrType = AllocString<A>;

    fn into(self) -> AbsNumOrStr<Self::NumType, Self::StrType> {
        match self {
            U16OrStr::U16(num) => AbsNumOrStr::Num(num),
            U16OrStr::Str(text) => AbsNumOrStr::Str(text),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self {
            U16OrStr::U16(num) => Result::Ok(num),
            U16OrStr::Str(text) => Result::Err(text),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self {
            U16OrStr::Str(text) => Result::Ok(text),
            U16OrStr::U16(num) => Result::Err(num),
        }
    }
}

impl<A: Allocator> From<u16> for U16OrStr<A> {
    fn from(value: u16) -> Self {
        U16OrStr::U16(value)
    }
}

impl<A: Allocator> From<StdHeaderKey> for U16OrStr<A> {
    fn from(value: StdHeaderKey) -> Self {
        U16OrStr::U16(u16::from(value))
    }
}

impl<A: Allocator> TrEncodable for U16OrStr<A> {
    type EncodeAsync<'a, E>
        = EncodeKeyAsync<'a, 'a, A, E>
    where
        Self: 'a,
        E: TrEncoder + 'a,
        E::Ok: 'a,
        E::Error: 'a;

    fn encode_async<'a, E>(&'a self, encoder: E) -> Self::EncodeAsync<'a, E>
    where
        Self: 'a,
        E: TrEncoder + 'a,
        E::Ok: 'a,
        E::Error: 'a,
    {
        EncodeKeyAsync::new(self, encoder)
    }
}

/// [`U16OrStr`] 编码的 step 函数：数字写整数，字符串写字符串，不写变体标签。
#[gen_may_cancel_future(EncodeKey, pub, new(pub(crate)))]
async fn encode_key_async_<'a, A, E, C>(
    key: &'a U16OrStr<A>,
    encoder: E,
    cancel: C,
) -> Result<E::Ok, E::Error>
where
    A: Allocator + 'a,
    E: TrEncoder + 'a,
    E::Ok: 'a,
    E::Error: 'a,
    C: TrCancellationToken,
{
    match key {
        U16OrStr::U16(num) => encoder.encode_u16(*num).may_cancel_with(cancel).await,
        U16OrStr::Str(text) => encoder.encode_str(text).may_cancel_with(cancel).await,
    }
}

/// [`U16OrStr`] 的访问者：按 MessagePack 线类型区分整数与字符串。
struct KeyVisitor<A: Allocator> {
    /// 字符串形态使用的 allocator；与 decoder 的 arena 投影一致。
    alloc_: A,
}

impl<'alloc, Alloc, A> Visitor<'alloc, Alloc> for KeyVisitor<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: Allocator + Clone + 'alloc,
{
    type Value = U16OrStr<A>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a non-negative integer or a string")
    }

    fn visit_u64<'a, E>(
        self,
        value: u64,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
    {
        let result = u16::try_from(value).map(U16OrStr::U16).map_err(|_| {
            <E as DencoError>::invalid_value(
                Unexpected::Unsigned(value),
                &"a non-negative integer or a string",
            )
        });
        future::ready(result)
    }

    fn visit_i64<'a, E>(
        self,
        value: i64,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
    {
        let result = u16::try_from(value).map(U16OrStr::U16).map_err(|_| {
            <E as DencoError>::invalid_value(
                Unexpected::Signed(value),
                &"a non-negative integer or a string",
            )
        });
        future::ready(result)
    }

    fn visit_str<'a, E>(
        self,
        value: &'a str,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
    {
        future::ready(Result::Ok(U16OrStr::from_text(value, self.alloc_)))
    }

    fn visit_string<'a, E>(
        self,
        value: alloc::string::String,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
        'alloc: 'a,
    {
        future::ready(Result::Ok(U16OrStr::from_text(value.as_str(), self.alloc_)))
    }
}

impl<'alloc, Alloc, A> TrDecodable<'alloc, Alloc> for U16OrStr<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: Allocator + Clone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type DecodeAsync<'a, D>
        = DecodeKeyAsync<'alloc, 'a, 'a, Alloc, A, D>
    where
        'alloc: 'a,
        D: TrDecoder<'alloc, Alloc> + 'a,
        D::Error: 'a,
        Self: 'a;

    fn decode_async<'a, D>(de: D) -> Self::DecodeAsync<'a, D>
    where
        'alloc: 'a,
        D: TrDecoder<'alloc, Alloc> + 'a,
        D::Error: 'a,
        Self: 'a,
    {
        let alloc = A::alloc_from(de.allocator());
        DecodeKeyAsync::new(de, alloc)
    }
}

/// [`U16OrStr`] 解码的 step 函数：交给 `decode_any`，由 [`KeyVisitor`] 分派。
#[gen_may_cancel_future(DecodeKey, pub, new(pub(crate)))]
async fn decode_key_async_<'alloc, 'a, Alloc, A, D, C>(
    de: D,
    alloc: A,
    cancel: C,
) -> Result<U16OrStr<A>, D::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: Allocator + Clone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    D: TrDecoder<'alloc, Alloc> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    de.decode_any(KeyVisitor { alloc_: alloc })
        .may_cancel_with(cancel)
        .await
}

#[cfg(test)]
mod tests_ {
    use alloc::alloc::Global;

    use super::*;

    /// 测试 `U16OrStr` 实现 `abs_mptp` 的 `TrNumOrStr` 契约。
    /// Test that `U16OrStr` implements `abs_mptp`'s `TrNumOrStr` contract.
    ///
    /// - 手段：把 `U16(0xA1)` 与 `Str("x")` 分别转成 `abs_mptp` 的 `NumOrStr` 再匹配。
    /// - 判断：数字键仍是数字，字符串键仍是字符串。
    ///
    /// - Means: convert `U16(0xA1)` and `Str("x")` into `abs_mptp`'s `NumOrStr` and match.
    /// - Verdict: the numeric key stays numeric and the string key stays textual.
    #[test]
    fn bridges_abs_mptp_trait_() {
        let num: U16OrStr<Global> = U16OrStr::U16(0xA1u16);
        assert!(matches!(
            AbsTrNumOrStr::into(num),
            AbsNumOrStr::Num(0xA1u16)
        ));

        let text = U16OrStr::from_text("x", Global);
        assert!(matches!(AbsTrNumOrStr::into(text), AbsNumOrStr::Str(_)));
    }

    /// 测试标准键可以直接转换成 `U16OrStr` 的数字形态。
    /// Test that a standard key converts directly into `U16OrStr`'s numeric form.
    ///
    /// - 手段：用 `From<StdHeaderKey>` 构造 `U16OrStr<Global>`。
    /// - 判断：得到 `U16(u16::from(StdHeaderKey::Body_Size))`。
    ///
    /// - Means: build a `U16OrStr<Global>` through `From<StdHeaderKey>`.
    /// - Verdict: it yields `U16(u16::from(StdHeaderKey::Body_Size))`.
    #[test]
    fn converts_std_key_() {
        let key = U16OrStr::<Global>::from(StdHeaderKey::Body_Size);
        assert_eq!(key, U16OrStr::U16(u16::from(StdHeaderKey::Body_Size)));
    }

    /// 测试字符串形态确实使用传入的 allocator，而不是全局分配。
    /// Test that the textual form uses the supplied allocator rather than the global one.
    ///
    /// - 手段：用 `Global` 构造一个文本键并比较内容。
    /// - 判断：文本内容完整保留；类型本身只要求 `A: Allocator`，不需要 `Default`。
    ///
    /// - Means: build a textual key with `Global` and compare its contents.
    /// - Verdict: the text is preserved and the type only requires `A: Allocator`, not
    ///   `Default`.
    #[test]
    fn text_uses_supplied_allocator_() {
        let key = U16OrStr::from_text("x-custom", Global);
        match key {
            U16OrStr::Str(text) => assert_eq!(&*text, "x-custom"),
            U16OrStr::U16(_) => panic!("应当构造出字符串形态"),
        }
    }
}
