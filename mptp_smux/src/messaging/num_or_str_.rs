//! 头键的「u16 或字符串」表示：`abs_mptp::spec::NumOrStr` 的 u16 特化复刻。
//!
//! `U16OrStr<S>` 把数字形态固定为 `u16`，字符串形态由使用者选择；它直接实现
//! `abs_mptp::spec::TrNumOrStr`，因此可以直接作为 [`TrHeaders::Key`] 使用。头表还需要
//! 自己的 MessagePack 表示，所以这里同时手写 [`TrEncodable`] / [`TrDecodable`]：数字写整数、
//! 字符串写字符串，不写变体标签。
//!
//! # `num_or_str_`: the u16-specialised numeric-or-string key
//!
//! `U16OrStr<S>` is the u16-specialised replica of `abs_mptp::spec::NumOrStr`. It implements
//! `abs_mptp::spec::TrNumOrStr` directly, so it can serve as `TrHeaders::Key`; denco's encode
//! and decode traits are implemented manually because the header map owns its own wire format.

use core::{borrow::Borrow, fmt, future, marker::PhantomData};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{Error as DencoError, TrDecodable, TrDecoder, Unexpected, Visitor},
    ser::{TrEncodable, TrEncoder},
};
use abs_mptp::spec::{NumOrStr as AbsNumOrStr, StdHeaderKey, TrNumOrStr as AbsTrNumOrStr};
use gen_mcf2::gen_may_cancel_future;

use crate::x_deps::{abs_cancel, abs_denco, abs_mptp, gen_mcf2};

/// 一个头键：要么是协议规定的 `u16` 数字，要么是使用者选择的字符串。
///
/// 这是 `abs_mptp::spec::NumOrStr` 的 u16 特化复刻：数字形态固定为 `u16`，字符串形态
/// 由 `S` 选择。头表实现要求 `S: Borrow<str> + Ord + Clone`，解码还要求 `S: From<String>`。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::U16OrStr;
///
/// let numeric: U16OrStr<String> = U16OrStr::U16(0xA1);
/// let textual: U16OrStr<String> = U16OrStr::Str("x-custom".to_string());
///
/// assert!(matches!(numeric, U16OrStr::U16(0xA1)));
/// assert!(matches!(textual, U16OrStr::Str(_)));
/// ```
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum U16OrStr<S>
where
    S: Borrow<str>,
{
    /// 数字形态。
    U16(u16),

    /// 字符串形态。
    Str(S),
}

impl<S> AbsTrNumOrStr for U16OrStr<S>
where
    S: Borrow<str>,
{
    type NumType = u16;

    type StrType = S;

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

impl<S> From<u16> for U16OrStr<S>
where
    S: Borrow<str>,
{
    fn from(value: u16) -> Self {
        U16OrStr::U16(value)
    }
}

impl<S> From<StdHeaderKey> for U16OrStr<S>
where
    S: Borrow<str>,
{
    fn from(value: StdHeaderKey) -> Self {
        U16OrStr::U16(u16::from(value))
    }
}

impl From<String> for U16OrStr<String> {
    fn from(value: String) -> Self {
        U16OrStr::Str(value)
    }
}

impl From<&str> for U16OrStr<String> {
    fn from(value: &str) -> Self {
        U16OrStr::Str(value.to_string())
    }
}

impl<S> TrEncodable for U16OrStr<S>
where
    S: Borrow<str>,
{
    type EncodeAsync<'a, E>
        = EncodeKeyAsync<'a, 'a, S, E>
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
async fn encode_key_async_<'a, S, E, C>(
    key: &'a U16OrStr<S>,
    encoder: E,
    cancel: C,
) -> Result<E::Ok, E::Error>
where
    S: Borrow<str> + 'a,
    E: TrEncoder + 'a,
    E::Ok: 'a,
    E::Error: 'a,
    C: TrCancellationToken,
{
    match key {
        U16OrStr::U16(num) => encoder.encode_u16(*num).may_cancel_with(cancel).await,
        U16OrStr::Str(text) => {
            encoder
                .encode_str(text.borrow())
                .may_cancel_with(cancel)
                .await
        }
    }
}

/// [`U16OrStr`] 的访问者：按 MessagePack 线类型区分整数与字符串。
struct KeyVisitor<S>(PhantomData<S>);

impl<'de, S> Visitor<'de> for KeyVisitor<S>
where
    S: Borrow<str> + From<String> + 'de,
{
    type Value = U16OrStr<S>;

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
        let result = u16::try_from(value)
            .map(U16OrStr::U16)
            .map_err(|_| <E as DencoError>::invalid_value(Unexpected::Unsigned(value), &self));
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
        let result = u16::try_from(value)
            .map(U16OrStr::U16)
            .map_err(|_| <E as DencoError>::invalid_value(Unexpected::Signed(value), &self));
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
        future::ready(Result::Ok(U16OrStr::Str(S::from(value.to_string()))))
    }

    fn visit_string<'a, E>(
        self,
        value: String,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
    {
        future::ready(Result::Ok(U16OrStr::Str(S::from(value))))
    }
}

impl<'de, S> TrDecodable<'de> for U16OrStr<S>
where
    S: Borrow<str> + From<String> + 'de,
{
    type DecodeAsync<'a, D>
        = DecodeKeyAsync<'de, 'a, 'a, S, D>
    where
        'de: 'a,
        D: TrDecoder<'de> + 'a,
        D::Error: 'a,
        Self: 'a;

    fn decode_async<'a, D>(decoder: D) -> Self::DecodeAsync<'a, D>
    where
        'de: 'a,
        D: TrDecoder<'de> + 'a,
        D::Error: 'a,
        Self: 'a,
    {
        DecodeKeyAsync::new(decoder)
    }
}

/// [`U16OrStr`] 解码的 step 函数：交给 `decode_any`，由 `KeyVisitor` 分派。
#[gen_may_cancel_future(DecodeKey, pub, new(pub(crate)))]
async fn decode_key_async_<'de, 'a, S, D, C>(decoder: D, cancel: C) -> Result<U16OrStr<S>, D::Error>
where
    'de: 'a,
    S: Borrow<str> + From<String> + 'de + 'a,
    D: TrDecoder<'de> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    decoder
        .decode_any(KeyVisitor::<S>(PhantomData))
        .may_cancel_with(cancel)
        .await
}

#[cfg(test)]
mod tests_ {
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
        let num: U16OrStr<String> = U16OrStr::U16(0xA1u16);
        assert!(matches!(
            AbsTrNumOrStr::into(num),
            AbsNumOrStr::Num(0xA1u16)
        ));

        let text: U16OrStr<String> = U16OrStr::Str("x".to_string());
        assert!(matches!(AbsTrNumOrStr::into(text), AbsNumOrStr::Str(_)));
    }

    /// 测试标准键可以直接转换成 `U16OrStr` 的数字形态。
    /// Test that a standard key converts directly into `U16OrStr`'s numeric form.
    ///
    /// - 手段：用 `From<StdHeaderKey>` 构造 `U16OrStr<String>`。
    /// - 判断：得到 `U16(u16::from(StdHeaderKey::Body_Size))`。
    ///
    /// - Means: build a `U16OrStr<String>` through `From<StdHeaderKey>`.
    /// - Verdict: it yields `U16(u16::from(StdHeaderKey::Body_Size))`.
    #[test]
    fn converts_std_key_() {
        let key = U16OrStr::<String>::from(StdHeaderKey::Body_Size);
        assert_eq!(key, U16OrStr::U16(u16::from(StdHeaderKey::Body_Size)));
    }
}
