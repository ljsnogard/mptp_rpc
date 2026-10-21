//! MPTP 头表：标准键值、可变契约与 MessagePack 表示。
//!
//! 头表是前缀里**唯一的开放部分**：方法与状态是固定取值，`location` 是一段文本，而头表允许
//! 两端在标准键之外携带自定义键。本模块把这张表实现成一个便宜的句柄
//! （`Arc<BTreeMap<HeaderKey<S>, HeaderVal<S>>>`），因此前缀解码一次之后，handler 拿到的
//! 克隆与框架手里的头表共享同一份存储。
//!
//! # 键与值
//!
//! 键与值都是本 crate 复刻的 [`U16OrStr`] 的透明封装：[`HeaderKey`] 与 [`HeaderVal`]。
//! 数字形态是协议标准键值（`u16`），字符串形态由使用者在 `S` 上选择（`String` /
//! `Arc<str>` / `Box<str>`……），缺省 `S = String`。这与
//! `abs_mptp::spec::TrHeaders` 对键和值的要求一致，同时保留了 denco 编解码能力。
//!
//! # 线上形态
//!
//! 它被编码成 MessagePack 的「数字或字符串键 → 数字或字符串值」映射。键与值都直接写成
//! 整数或字符串，不写变体标签（与旧格式逐字节兼容）。
//!
//! # 入口约定
//!
//! - 标准键的便捷入口 [`Headers::try_get_val`] / [`Headers::set_num`] /
//!   [`Headers::set_text`] / [`Headers::remove`] 收 [`StdHeaderKey`]。
//! - `TrHeaders::try_get_header` 与 `TrHeadersMut` 的键参数是 [`HeaderKey`]；
//!   标准键可用 `HeaderKey::from(key)`，或改用 `try_get_val`。
//! - [`Headers::iter`] 返回 [`HeaderKey`] 键；只需要数字键时用 [`Headers::iter_std`]。
//! - [`HeaderEntry::key`] 返回 [`HeaderKey`]；需要标准键时用
//!   [`HeaderEntry::key_std`]。
//!
//! # `headers_`: the MPTP header map
//!
//! The header map is the only open-ended part of a prefix. It is a cheap handle over
//! `Arc<BTreeMap<HeaderKey<S>, HeaderVal<S>>>`, encoded as a MessagePack map whose keys and
//! values are written as plain integers or strings. The string representation `S` defaults
//! to `String`; both [`HeaderKey`] and [`HeaderVal`] are transparent wrappers around
//! `U16OrStr<S>`.

use core::{borrow::Borrow, fmt, marker::PhantomData};
use std::{collections::BTreeMap, sync::Arc};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{MapAccess, MapAccessExt, TrDecodable, TrDecoder, Visitor},
    ser::{TrEncodable, TrEncodeMap, TrEncoder},
};
use abs_mptp::spec::{
    NumOrStr as AbsNumOrStr, StdHeaderKey, StdHeaderVal, TrHeaders, TrHeadersMut,
    TrNumOrStr as AbsTrNumOrStr,
};
use denco_macro::{TrDecodable, TrEncodable};
use gen_mcf2::gen_may_cancel_future;

use super::num_or_str_::U16OrStr;

/// 一个头值：协议标准头只有「一个数字」或「一段文本」两种形态。
///
/// 它是 [`U16OrStr<S>`](U16OrStr) 的透明封装：数字形态用 `u16`，字符串形态由 `S`
/// 选择，因此与键共享同一套「数字或字符串」表示。`Data_Type_Id` 这类 64 位标识按
/// 文本携带（[`HeaderVal::as_u64`] 同时认十进制与十六进制）。
///
/// # 线上表示
///
/// 直接写成 MessagePack 的整数或字符串，**不写**变体标签；派生被标记为
/// `#[denco(transparent)]`，编解码完全转发给 [`U16OrStr`] 的手写实现。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::HeaderVal;
///
/// let num = HeaderVal::<String>::from_num(7u16);
/// let text = HeaderVal::<String>::from_text("0xabc");
///
/// assert_eq!(num.as_num(), Some(7u16));
/// assert_eq!(text.as_u64(), Some(0xabcu64));
/// ```
///
/// A header value: either a `u16` number or a string. It is a transparent wrapper
/// around `U16OrStr<S>`, so values share the numeric-or-string representation with keys.
#[derive(Clone, Debug, Eq, PartialEq, TrDecodable, TrEncodable)]
#[denco(transparent)]
pub struct HeaderVal<S = String>(U16OrStr<S>)
where
    S: Borrow<str>;

/// 一个头键：与 [`HeaderVal`] 同构的「数字或字符串」表示。
///
/// 数字形态就是协议标准键，字符串形态用于使用者自定义键；两者在线上都不带变体标签。
/// 头表的键类型就是本类型；它与 [`HeaderVal`] 一起让 [`HeaderEntry`] 的键值字段都有
/// 强类型。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::{HeaderKey, U16OrStr};
///
/// let key = HeaderKey::<String>::from_num(0xA1);
/// assert_eq!(key.into_inner(), U16OrStr::U16(0xA1));
/// ```
///
/// A header key with the same numeric-or-string shape as [`HeaderVal`]. Numeric keys
/// are protocol standard keys; string keys are user-defined.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, TrDecodable, TrEncodable)]
#[denco(transparent)]
pub struct HeaderKey<S = String>(U16OrStr<S>)
where
    S: Borrow<str>;

impl<S> HeaderVal<S>
where
    S: Borrow<str>,
{
    /// 由数字形态构造一个头值。
    ///
    /// Build a numeric header value.
    pub const fn from_header_val(v: StdHeaderVal) -> Self {
        HeaderVal(U16OrStr::U16(v.into_inner()))
    }

    /// 由裸 `u16` 构造一个数字头值（不校验它是否是标准值）。
    ///
    /// Build a numeric header value from a bare `u16` without checking its standard meaning.
    pub const fn from_num(num: u16) -> Self {
        HeaderVal(U16OrStr::U16(num))
    }

    /// 由文本构造一个头值；字符串形态由 `S: From<String>` 决定。
    ///
    /// Build a textual header value; the string representation comes from `S`.
    pub fn from_text(text: impl Into<String>) -> Self
    where
        S: From<String>,
    {
        HeaderVal(U16OrStr::Str(S::from(text.into())))
    }

    /// 取出内部的 [`U16OrStr<S>`](U16OrStr)。
    ///
    /// Unwrap the inner `U16OrStr<S>`.
    pub fn into_inner(self) -> U16OrStr<S> {
        self.0
    }

    /// 借用内部表示。
    ///
    /// Borrow the inner representation.
    pub const fn as_u16_or_str(&self) -> &U16OrStr<S> {
        &self.0
    }

    /// 尽力按数字解读；文本形态会被解析成 `u16`。
    ///
    /// # Examples
    ///
    /// ```
    /// use mptp_smux::messaging::HeaderVal;
    ///
    /// assert_eq!(HeaderVal::<String>::from_text("42").as_num(), Some(42u16));
    /// assert_eq!(HeaderVal::<String>::from_text("not a number").as_num(), None);
    /// ```
    ///
    /// Interpret the value as a number, parsing textual values as `u16`.
    pub fn as_num(&self) -> Option<u16> {
        match &self.0 {
            U16OrStr::U16(num) => Option::Some(*num),
            U16OrStr::Str(text) => text.borrow().trim().parse::<u16>().ok(),
        }
    }

    /// 尽力按 `u64` 解读（`Data_Type_Id` 是 64 位，装不进 `u16`）。
    ///
    /// 十六进制（带或不带 `0x`）与十进制都认。
    ///
    /// # Examples
    ///
    /// ```
    /// use mptp_smux::messaging::HeaderVal;
    ///
    /// assert_eq!(HeaderVal::<String>::from_text("0x10").as_u64(), Some(16u64));
    /// assert_eq!(HeaderVal::<String>::from_text("16").as_u64(), Some(16u64));
    /// ```
    ///
    /// Interpret the value as `u64`, accepting decimal and `0x`-prefixed hexadecimal.
    pub fn as_u64(&self) -> Option<u64> {
        match &self.0 {
            U16OrStr::U16(num) => Option::Some(u64::from(*num)),
            U16OrStr::Str(text) => {
                let text = text.borrow().trim();
                let hex = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X"));
                match hex {
                    Option::Some(hex) => u64::from_str_radix(hex, 16u32).ok(),
                    Option::None => text.parse::<u64>().ok(),
                }
            }
        }
    }

    /// 文本形态的借用；数字形态返回 `None`。
    ///
    /// # Examples
    ///
    /// ```
    /// use mptp_smux::messaging::HeaderVal;
    ///
    /// assert_eq!(HeaderVal::<String>::from_text("abc").as_text(), Some("abc"));
    /// assert_eq!(HeaderVal::<String>::from_num(1u16).as_text(), None);
    /// ```
    ///
    /// Borrow the textual form; numeric values return `None`.
    pub fn as_text(&self) -> Option<&str> {
        match &self.0 {
            U16OrStr::U16(_) => Option::None,
            U16OrStr::Str(text) => Option::Some(text.borrow()),
        }
    }
}

impl<S> HeaderKey<S>
where
    S: Borrow<str>,
{
    /// 由数字形态构造一个头键。
    ///
    /// Build a numeric header key.
    pub fn from_header_key(k: StdHeaderKey) -> Self {
        HeaderKey(U16OrStr::U16(k.into_inner()))
    }

    /// 由裸 `u16` 构造一个数字头键（不校验它是否是标准键）。
    ///
    /// Build a numeric header key from a bare `u16` without checking its standard meaning.
    pub const fn from_num(num: u16) -> Self {
        HeaderKey(U16OrStr::U16(num))
    }

    /// 由文本构造一个头键；字符串形态由 `S: From<String>` 决定。
    ///
    /// Build a textual header key; the string representation comes from `S`.
    pub fn from_text(text: impl Into<String>) -> Self
    where
        S: From<String>,
    {
        HeaderKey(U16OrStr::Str(S::from(text.into())))
    }

    /// 取出内部的 [`U16OrStr<S>`](U16OrStr)。
    ///
    /// Unwrap the inner `U16OrStr<S>`.
    pub fn into_inner(self) -> U16OrStr<S> {
        self.0
    }

    /// 借用内部表示。
    ///
    /// Borrow the inner representation.
    pub const fn as_u16_or_str(&self) -> &U16OrStr<S> {
        &self.0
    }
}

impl<S> From<StdHeaderVal> for HeaderVal<S>
where
    S: Borrow<str>,
{
    fn from(value: StdHeaderVal) -> Self {
        HeaderVal::from_header_val(value)
    }
}

impl<S> From<String> for HeaderVal<S>
where
    S: Borrow<str> + From<String>,
{
    fn from(value: String) -> Self {
        HeaderVal::from_text(value)
    }
}

impl<S> From<&str> for HeaderVal<S>
where
    S: Borrow<str> + From<String>,
{
    fn from(value: &str) -> Self {
        HeaderVal::from_text(value)
    }
}

impl<S> From<String> for HeaderKey<S>
where
    S: Borrow<str> + From<String>,
{
    fn from(value: String) -> Self {
        HeaderKey::from_text(value)
    }
}

impl<S> From<&str> for HeaderKey<S>
where
    S: Borrow<str> + From<String>,
{
    fn from(value: &str) -> Self {
        HeaderKey::from_text(value)
    }
}

impl<S> From<StdHeaderKey> for HeaderKey<S>
where
    S: Borrow<str>,
{
    fn from(value: StdHeaderKey) -> Self {
        HeaderKey::from_num(u16::from(value))
    }
}

impl<S> From<U16OrStr<S>> for HeaderVal<S>
where
    S: Borrow<str>,
{
    fn from(value: U16OrStr<S>) -> Self {
        HeaderVal(value)
    }
}

impl<S> From<HeaderVal<S>> for U16OrStr<S>
where
    S: Borrow<str>,
{
    fn from(value: HeaderVal<S>) -> Self {
        value.0
    }
}

impl<S> From<U16OrStr<S>> for HeaderKey<S>
where
    S: Borrow<str>,
{
    fn from(value: U16OrStr<S>) -> Self {
        HeaderKey(value)
    }
}

impl<S> From<HeaderKey<S>> for U16OrStr<S>
where
    S: Borrow<str>,
{
    fn from(value: HeaderKey<S>) -> Self {
        value.0
    }
}

impl<S> AbsTrNumOrStr for HeaderKey<S>
where
    S: Borrow<str>,
{
    type NumType = u16;

    type StrType = S;

    fn into(self) -> AbsNumOrStr<Self::NumType, Self::StrType> {
        match self.0 {
            U16OrStr::U16(num) => AbsNumOrStr::Num(num),
            U16OrStr::Str(text) => AbsNumOrStr::Str(text),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self.0 {
            U16OrStr::U16(num) => Result::Ok(num),
            U16OrStr::Str(text) => Result::Err(text),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self.0 {
            U16OrStr::Str(text) => Result::Ok(text),
            U16OrStr::U16(num) => Result::Err(num),
        }
    }
}

impl<S> AbsTrNumOrStr for HeaderVal<S>
where
    S: Borrow<str>,
{
    type NumType = u16;

    type StrType = S;

    fn into(self) -> AbsNumOrStr<Self::NumType, Self::StrType> {
        match self.0 {
            U16OrStr::U16(num) => AbsNumOrStr::Num(num),
            U16OrStr::Str(text) => AbsNumOrStr::Str(text),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self.0 {
            U16OrStr::U16(num) => Result::Ok(num),
            U16OrStr::Str(text) => Result::Err(text),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self.0 {
            U16OrStr::Str(text) => Result::Ok(text),
            U16OrStr::U16(num) => Result::Err(num),
        }
    }
}

/// 头表里的一条：**同时**能借成键与值。
///
/// `abs_mptp` 的 [`TrHeaders`] 要求条目对键与值都可 `Borrow`，因此不能用元组。
/// 键与值分别是 [`HeaderKey<S>`] 与 [`HeaderVal<S>`]；[`HeaderEntry::new`] 保留按
/// [`StdHeaderKey`] 构造的入口。
///
/// # Examples
///
/// ```
/// use core::borrow::Borrow;
///
/// use abs_mptp::spec::StdHeaderKey;
/// use mptp_smux::messaging::{HeaderEntry, HeaderKey, HeaderVal};
///
/// let entry = HeaderEntry::new(StdHeaderKey::Body_Type, HeaderVal::from_num(1u16));
/// let key: &HeaderKey<String> = entry.borrow();
/// let val: &HeaderVal = entry.borrow();
///
/// assert_eq!(*key, HeaderKey::from(StdHeaderKey::Body_Type));
/// assert_eq!(*val, HeaderVal::from_num(1u16));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEntry<S = String>
where
    S: Borrow<str>,
{
    /// 头键。
    key_: HeaderKey<S>,

    /// 头值。
    val_: HeaderVal<S>,
}

impl<S> HeaderEntry<S>
where
    S: Borrow<str>,
{
    /// 由新键类型与值构造一条头。
    pub const fn from_kv(key: HeaderKey<S>, val: HeaderVal<S>) -> Self {
        HeaderEntry {
            key_: key,
            val_: val,
        }
    }

    /// 本条头的键。
    pub const fn key(&self) -> &HeaderKey<S> {
        &self.key_
    }

    /// 本条头的值。
    pub const fn value(&self) -> &HeaderVal<S> {
        &self.val_
    }
}

impl<S> HeaderEntry<S>
where
    S: Borrow<str>,
{
    /// 由标准键与值构造一条头；字符串形态仍由 `S` 决定。
    pub fn new(key: StdHeaderKey, val: HeaderVal<S>) -> Self {
        HeaderEntry {
            key_: HeaderKey::from(key),
            val_: val,
        }
    }

    /// 若键是数字形态，还原成 [`StdHeaderKey`]；字符串键返回 `None`。
    pub fn key_std(&self) -> Option<StdHeaderKey> {
        match self.key_.as_u16_or_str() {
            U16OrStr::U16(code) => Option::Some(StdHeaderKey::new(*code)),
            U16OrStr::Str(_) => Option::None,
        }
    }
}

impl<S> Borrow<HeaderKey<S>> for HeaderEntry<S>
where
    S: Borrow<str>,
{
    fn borrow(&self) -> &HeaderKey<S> {
        &self.key_
    }
}

impl<S> Borrow<HeaderVal<S>> for HeaderEntry<S>
where
    S: Borrow<str>,
{
    fn borrow(&self) -> &HeaderVal<S> {
        &self.val_
    }
}

/// MPTP 头表：`HeaderKey<S>` → [`HeaderVal<S>`]。
///
/// # 为什么内部是 `Arc<BTreeMap>`
///
/// `abs_mptp` 的 [`TrHeadersMut`] 把所有修改入口都定义成按值收 `self`，却只交出旧值；修改
/// 只可能发生在**共享状态**里，否则就丢了。因此这一档实现是一个句柄，而不是一个独占的 map。
///
/// `Arc<BTreeMap>` + `Arc::make_mut` 是它的写时复制版本：唯一持有者（handler 手里那个
/// `&mut Headers` 就是）时修改**就地生效**；多持有者时修改落在临时副本上。框架只在把请求
/// 交给 handler 之前克隆一次头表，之后的修改路径不共享句柄。
///
/// # 字符串形态
///
/// `S` 缺省为 `String`；也可以选 `Arc<str>`、`Box<str>` 等。头表的实现要求
/// `S: Borrow<str> + Ord + Clone`，解码还要求 `S: From<String>`。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::{HeaderKey, HeaderVal, Headers};
///
/// let mut headers = Headers::new();
/// headers.set_text(abs_mptp::spec::StdHeaderKey::Body_Size, "12");
/// headers.set_num(abs_mptp::spec::StdHeaderKey::Body_Type, 1u16);
///
/// let custom = HeaderKey::<String>::from_text("x-custom");
/// headers.set(custom.clone(), HeaderVal::from_num(7u16));
///
/// assert_eq!(headers.body_size(), Some(12usize));
/// assert_eq!(headers.try_get(custom), Some(&HeaderVal::from_num(7u16)));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Headers<S = String>(Arc<BTreeMap<HeaderKey<S>, HeaderVal<S>>>)
where
    S: Borrow<str> + Ord;

impl<S> Headers<S>
where
    S: Borrow<str> + Ord,
{
    /// 造一张空头表（字符串形态由调用点的类型决定）。
    ///
    /// 缺省 `String` 形态通常直接用 [`Headers::new`]；自定义 `S` 时写
    /// `Headers::<Arc<str>>::empty()`。
    pub fn empty() -> Self {
        Headers(Arc::new(BTreeMap::new()))
    }

    /// 头表里有多少条。
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// 头表是否为空。
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 按标准键查一个值。
    pub fn try_get_val(&self, key: StdHeaderKey) -> Option<&HeaderVal<S>> {
        self.try_get(HeaderKey::from(key))
    }

    /// 按任意头键查一个值。
    pub fn try_get<K>(&self, key: K) -> Option<&HeaderVal<S>>
    where
        K: Borrow<HeaderKey<S>>,
    {
        self.0.get(key.borrow())
    }

    /// 遍历全部条目。
    pub fn iter(&self) -> impl Iterator<Item = (&HeaderKey<S>, &HeaderVal<S>)> + '_ {
        self.0.iter()
    }

    /// 只遍历数字键，并还原成本端的 [`StdHeaderKey`]。
    pub fn iter_std(&self) -> impl Iterator<Item = (StdHeaderKey, &HeaderVal<S>)> + '_ {
        self.0
            .iter()
            .filter_map(|(key, val)| match key.as_u16_or_str() {
                U16OrStr::U16(code) => Option::Some((StdHeaderKey::new(*code), val)),
                U16OrStr::Str(_) => Option::None,
            })
    }

    /// `Body_Size` 头声明的长度。
    pub fn body_size(&self) -> Option<usize> {
        let val = self.try_get_val(StdHeaderKey::Body_Size)?;
        match val.as_u64() {
            Option::Some(num) => usize::try_from(num).ok(),
            Option::None => Option::None,
        }
    }

    /// 是否声明了 `Body_Transfer: Chunked`。
    ///
    /// 与 `abs_mptp` 的默认 [`body_info`](abs_mptp::messaging::TrMsgPrefix::body_info) 同一个
    /// 判据：数字 `0x20` 或文本 `"chunked"`（忽略大小写）都算分块。
    pub fn is_chunked(&self) -> bool {
        let Option::Some(val) = self.try_get_val(StdHeaderKey::Body_Transfer) else {
            return false;
        };
        match val.as_u16_or_str() {
            U16OrStr::U16(num) => StdHeaderVal::from(*num) == StdHeaderVal::Body_Transfer_Chunked,
            U16OrStr::Str(text) => text.borrow().trim().eq_ignore_ascii_case("chunked"),
        }
    }

    /// `Data_Type_Id` 头声明的类型标识。
    pub fn data_type_id(&self) -> Option<u64> {
        self.try_get_val(StdHeaderKey::Data_Type_Id)?.as_u64()
    }
}

impl<S> Headers<S>
where
    S: Borrow<str> + Ord + Clone,
{
    /// 按标准键写一个数字值（覆盖已有值）。
    pub fn set_num(&mut self, key: StdHeaderKey, num: u16) {
        self.set(HeaderKey::from(key), HeaderVal::from_num(num));
    }

    /// 按标准键写一个文本值（覆盖已有值）。
    pub fn set_text(&mut self, key: StdHeaderKey, text: impl Into<String>)
    where
        S: From<String>,
    {
        self.set(HeaderKey::from(key), HeaderVal::from_text(text));
    }

    /// 按任意头键写一个值（覆盖已有值）。
    pub fn set<K>(&mut self, key: K, val: HeaderVal<S>)
    where
        K: Into<HeaderKey<S>>,
    {
        Arc::make_mut(&mut self.0).insert(key.into(), val);
    }

    /// 删除一个标准键，返回被删掉的值。
    pub fn remove(&mut self, key: StdHeaderKey) -> Option<HeaderVal<S>> {
        self.remove_key(HeaderKey::from(key))
    }

    /// 删除一个任意头键，返回被删掉的值。
    pub fn remove_key<K>(&mut self, key: K) -> Option<HeaderVal<S>>
    where
        K: Borrow<HeaderKey<S>>,
    {
        Arc::make_mut(&mut self.0).remove(key.borrow())
    }
}

impl Headers<String> {
    /// 造一张空头表（缺省 `String` 字符串形态）。
    pub fn new() -> Self {
        Headers::empty()
    }
}

impl Default for Headers<String> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> TrHeaders for Headers<S>
where
    S: Borrow<str> + Ord + Clone,
{
    type Key = HeaderKey<S>;

    type Val = HeaderVal<S>;

    type Entry = HeaderEntry<S>;

    type EntriesIter = std::vec::IntoIter<HeaderEntry<S>>;

    fn try_get_std_header(&self, key: StdHeaderKey) -> Option<&Self::Val> {
        self.try_get_val(key)
    }

    fn try_get_header<K>(&self, key: K) -> Option<&Self::Val>
    where
        K: Borrow<Self::Key>,
    {
        self.0.get(key.borrow())
    }

    fn into_entries(self) -> Self::EntriesIter {
        self.0
            .iter()
            .map(|(key, val_)| HeaderEntry::from_kv(key.clone(), val_.clone()))
            .collect::<Vec<HeaderEntry<S>>>()
            .into_iter()
    }
}

impl<S> TrHeadersMut for Headers<S>
where
    S: Borrow<str> + Ord + Clone,
{
    fn try_add_header<K>(
        mut self,
        key: K,
        factory: impl FnOnce(K) -> Self::Val,
    ) -> Result<Self::Entry, K>
    where
        K: Borrow<Self::Key>,
    {
        let key_ = key.borrow().clone();
        if self.0.contains_key(&key_) {
            return Result::Err(key);
        }
        let val_ = factory(key);
        Arc::make_mut(&mut self.0).insert(key_.clone(), val_.clone());
        Result::Ok(HeaderEntry::from_kv(key_, val_))
    }

    fn add_or_set_header<K, V>(mut self, key: K, val: V) -> Option<(Self::Key, Self::Val)>
    where
        K: Into<Self::Key>,
        V: Into<Self::Val>,
    {
        let key_ = key.into();
        Arc::make_mut(&mut self.0)
            .insert(key_.clone(), val.into())
            .map(|old| (key_, old))
    }

    fn remove_header(mut self, key: impl Borrow<Self::Key>) -> Option<(Self::Key, Self::Val)> {
        Arc::make_mut(&mut self.0).remove_entry(key.borrow())
    }
}

impl<S> TrEncodable for Headers<S>
where
    S: Borrow<str> + Ord,
{
    type EncodeAsync<'a, E>
        = EncodeHeadersAsync<'a, 'a, S, E>
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
        EncodeHeadersAsync::new(self, encoder)
    }
}

/// [`Headers`] 编码的 step 函数：键交给 [`HeaderKey<S>`]，值交给 [`HeaderVal<S>`]。
#[gen_may_cancel_future(EncodeHeaders, pub, new(pub(crate)))]
async fn encode_headers_async_<'a, S, E, C>(
    headers: &'a Headers<S>,
    encoder: E,
    cancel: C,
) -> Result<E::Ok, E::Error>
where
    S: Borrow<str> + Ord + 'a,
    E: TrEncoder + 'a,
    E::Ok: 'a,
    E::Error: 'a,
    C: TrCancellationToken,
{
    let mut map = encoder
        .encode_map(Option::Some(headers.0.len()))
        .may_cancel_with(cancel.child_token())
        .await?;
    for (key, val) in headers.0.iter() {
        map.encode_key(key)
            .may_cancel_with(cancel.child_token())
            .await?;
        map.encode_value(val)
            .may_cancel_with(cancel.child_token())
            .await?;
    }
    map.end().may_cancel_with(cancel).await
}

/// [`Headers`] 的访问者：遍历 MessagePack 映射，键交给 [`HeaderKey<S>`]，值交给 [`HeaderVal<S>`]。
struct HeadersVisitor<S> {
    /// `S` 只出现在键类型里。
    _marker_: PhantomData<S>,
}

impl<'de, S> Visitor<'de> for HeadersVisitor<S>
where
    S: Borrow<str> + Ord + From<String> + 'de,
{
    type Value = Headers<S>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map with numeric or string header keys")
    }

    fn visit_map<'a, A>(
        self,
        map: A,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, A::Error>>
    where
        'de: 'a,
        Self: 'a,
        Self::Value: 'a,
        A: MapAccess<'de> + 'a,
        A::Error: 'a,
    {
        DecodeHeadersMapAsync::<'de, 'a, 'a, S, A>::new(map)
    }
}

/// [`Headers`] 解码的 step 函数：读出整张映射，再交给 [`Headers`]。
#[gen_may_cancel_future(DecodeHeadersMap, pub, new(pub(crate)))]
async fn decode_headers_map_async_<'de, 'a, S, A, C>(
    mut map: A,
    cancel: C,
) -> Result<Headers<S>, A::Error>
where
    'de: 'a,
    S: Borrow<str> + Ord + From<String> + 'a + 'de,
    A: MapAccess<'de> + 'a,
    A::Error: 'a,
    C: TrCancellationToken,
{
    let mut values = BTreeMap::new();
    while let Option::Some((key, val)) = map
        .next_entry::<HeaderKey<S>, HeaderVal<S>>()
        .may_cancel_with(cancel.child_token())
        .await?
    {
        values.insert(key, val);
    }
    Result::Ok(Headers(Arc::new(values)))
}

impl<'de, S> TrDecodable<'de> for Headers<S>
where
    S: Borrow<str> + Ord + From<String> + 'de,
{
    type DecodeAsync<'a, D>
        = DecodeHeadersAsync<'de, 'a, 'a, S, D>
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
        DecodeHeadersAsync::new(decoder)
    }
}

/// [`Headers`] 解码的 step 函数：交给 `decode_map`，由 [`HeadersVisitor`] 遍历。
#[gen_may_cancel_future(DecodeHeaders, pub, new(pub(crate)))]
async fn decode_headers_async_<'de, 'a, S, D, C>(
    decoder: D,
    cancel: C,
) -> Result<Headers<S>, D::Error>
where
    'de: 'a,
    S: Borrow<str> + Ord + From<String> + 'de + 'a,
    D: TrDecoder<'de> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    decoder
        .decode_map(HeadersVisitor::<S> {
            _marker_: PhantomData,
        })
        .may_cancel_with(cancel)
        .await
}

#[cfg(test)]
mod tests_ {
    //! 头表的纯本地点判定：取值、增删与两个 `abs_mptp` 契约。
    //!
    //! 这里不碰 ring、不建连接：头表就是一 map，所有性质都能在内存里钉住。
    //!
    //! Local tests for the header map: accessors, mutation and the two `abs_mptp`
    //! contracts. No ring or connection is involved.

    use super::*;

    /// 造一个标准数字键。
    fn std_key_(key: StdHeaderKey) -> HeaderKey<String> {
        HeaderKey::from_num(u16::from(key))
    }

    /// 测试标准头访问器把数字、文本与体声明各归各位。
    /// Test that the standard-header accessors map numeric, textual and body declarations.
    ///
    /// - 手段：写入 `Body_Size: "12"`、`Body_Type: 1`，再写入十六进制的 `Data_Type_Id`，
    ///   分别调用 `body_size` / `try_get_val` / `data_type_id`。
    /// - 判断：长度是 12，`Body_Type` 是数字 1，类型标识按十六进制解析成 `0xabc`；三者不会
    ///   被读成同一个值。
    ///
    /// - Means: store `Body_Size: "12"`, `Body_Type: 1` and a hexadecimal `Data_Type_Id`,
    ///   then call `body_size` / `try_get_val` / `data_type_id`.
    /// - Verdict: the size is 12, `Body_Type` is numeric 1, and the identifier parses as
    ///   `0xabc`; the three never blur together.
    #[test]
    fn header_accessors_map_values_() {
        let mut headers = Headers::new();
        headers.set_text(StdHeaderKey::Body_Size, "12");
        headers.set_num(StdHeaderKey::Body_Type, 1u16);
        headers.set_text(StdHeaderKey::Data_Type_Id, "0xabc");

        assert_eq!(headers.body_size(), Option::Some(12usize));
        assert_eq!(
            headers.try_get_val(StdHeaderKey::Body_Type),
            Option::Some(&HeaderVal::from_num(1u16))
        );
        assert_eq!(headers.data_type_id(), Option::Some(0xabcu64));

        // 旧入口 `set(StdHeaderKey, HeaderVal)` 仍通过 `Into<U16OrStr>` 工作。
        headers.set(StdHeaderKey::Accept, HeaderVal::from_text("a"));
        assert_eq!(
            headers.try_get_val(StdHeaderKey::Accept),
            Option::Some(&HeaderVal::from_text("a"))
        );
    }

    /// 测试分块声明与长度声明可分别读出，且分块判定的两种取值形态都认。
    /// Test that the chunked declaration is readable on its own in both value forms.
    ///
    /// - 手段：分别写数字 `0x20` 与文本 `"chunked"` 的 `Body_Transfer`，再写一张同时带
    ///   `Body_Size` 的表。
    /// - 判断：数字与文本两种声明都让 `is_chunked` 为真；带长度的表 `body_size` 为
    ///   `Some(8)`，且两者互不吞并——「有没有长度」与「是否分块」是两件事。
    ///
    /// - Means: store `Body_Transfer` as numeric `0x20` and as textual `"chunked"`, then a
    ///   map that also carries `Body_Size`.
    /// - Verdict: both forms report chunked, the sized map reports `Some(8)`, and neither
    ///   declaration swallows the other.
    #[test]
    fn chunked_and_sized_are_independent_() {
        let mut chunked = Headers::new();
        chunked.set_num(
            StdHeaderKey::Body_Transfer,
            StdHeaderVal::Body_Transfer_Chunked.into_inner(),
        );
        assert!(chunked.is_chunked());
        assert_eq!(chunked.body_size(), Option::None);

        let mut textual = Headers::new();
        textual.set_text(StdHeaderKey::Body_Transfer, "chunked");
        assert!(textual.is_chunked());

        let mut both = chunked.clone();
        both.set_text(StdHeaderKey::Body_Size, "8");
        assert!(both.is_chunked());
        assert_eq!(both.body_size(), Option::Some(8usize));
    }

    /// 测试自定义数字键与标准键共存，且删除返回旧值。
    /// Test that custom numeric keys coexist with standard keys and removal returns the old value.
    ///
    /// - 手段：用 `StdHeaderKey::new(0x01)` 写一条自定义头，再删除它。
    /// - 判断：`try_get_val` 读得到，删除返回原值，表随之为空；标准键区段不受影响。
    ///
    /// - Means: write a custom header under `StdHeaderKey::new(0x01)` and remove it.
    /// - Verdict: the lookup finds it, removal hands the old value back, and the map is empty
    ///   afterwards.
    #[test]
    fn custom_numeric_key_roundtrips_() {
        let key = StdHeaderKey::new(0x01);
        let mut headers = Headers::new();
        headers.set_num(key, 0x1234u16);

        assert_eq!(
            headers.try_get_val(key),
            Option::Some(&HeaderVal::from_num(0x1234u16))
        );
        assert_eq!(
            headers.remove(key),
            Option::Some(HeaderVal::from_num(0x1234u16))
        );
        assert!(headers.is_empty());
    }

    /// 测试字符串键可写入、读取、遍历与删除。
    /// Test that a string key can be written, read, iterated and removed.
    ///
    /// - 手段：用 `HeaderKey::from_text` 写一条自定义头，分别调用 `try_get` / `iter` /
    ///   `remove_key`。
    /// - 判断：读到的值一致，遍历只看到这一条，删除后表为空。
    ///
    /// - Means: write a custom header through `HeaderKey::from_text`, then call `try_get` /
    ///   `iter` / `remove_key`.
    /// - Verdict: the value matches, iteration sees exactly that entry, and the map is empty
    ///   after removal.
    #[test]
    fn string_key_roundtrips_() {
        let key = HeaderKey::<String>::from_text("x-custom");
        let mut headers = Headers::new();
        headers.set(key.clone(), HeaderVal::from_num(7u16));

        assert_eq!(
            headers.try_get(&key),
            Option::Some(&HeaderVal::from_num(7u16))
        );
        assert_eq!(headers.len(), 1usize);
        assert_eq!(headers.iter().count(), 1usize);
        assert_eq!(headers.iter_std().count(), 0usize);
        assert_eq!(
            headers.remove_key(&key),
            Option::Some(HeaderVal::from_num(7u16))
        );
        assert!(headers.is_empty());
    }

    /// 测试字符串实现可以由使用者选择（这里用 `Arc<str>`）。
    /// Test that the string representation is user-selectable (using `Arc<str>` here).
    ///
    /// - 手段：构造 `Headers<Arc<str>>`，用 `HeaderKey::from_text` 写入一个键并读回。
    /// - 判断：值与键都正确，证明 `S` 不是硬编码的 `String`。
    ///
    /// - Means: build `Headers<Arc<str>>`, write a key through `HeaderKey::from_text`, and
    ///   read it back.
    /// - Verdict: both key and value round-trip, so `S` is not hard-coded to `String`.
    #[test]
    fn custom_string_impl_works_() {
        let key = HeaderKey::<Arc<str>>::from_text("x-custom");
        let mut headers: Headers<Arc<str>> = Headers::empty();
        headers.set(key.clone(), HeaderVal::from_text("v"));

        assert_eq!(
            headers.try_get(key),
            Option::Some(&HeaderVal::from_text("v"))
        );
    }

    /// 测试 `TrHeaders` 的按键查询与全量遍历都成立。
    /// Test that `TrHeaders` lookups and full iteration both work.
    ///
    /// - 手段：写入一个数字头与一个文本头，先 `try_get_header`，再 `into_entries`。
    /// - 判断：两次查询命中写入的值；遍历得到两条，且每条对键与值都能 `Borrow`。
    ///   这同时验证了 `abs_mptp` 对条目「同时借出键与值」的要求。
    ///
    /// - Means: store one numeric and one textual header, look them up through
    ///   `try_get_header`, then walk everything through `into_entries`.
    /// - Verdict: both lookups hit, the walk yields two entries, and each entry borrows as
    ///   both key and value — the exact requirement `abs_mptp` places on an entry.
    #[test]
    fn tr_headers_answers_lookups_and_entries_() {
        let mut headers = Headers::new();
        headers.set_num(StdHeaderKey::Body_Type, 1u16);
        headers.set_text(StdHeaderKey::Data_Type_Id, "0xabc");

        assert_eq!(
            headers.try_get_header(std_key_(StdHeaderKey::Body_Type)),
            Option::Some(&HeaderVal::from_num(1u16))
        );

        let all: Vec<HeaderEntry> = headers.into_entries().collect();
        assert_eq!(all.len(), 2usize);
        let borrowed_key: &HeaderKey<String> = all[0].borrow();
        let borrowed_val: &HeaderVal = all[0].borrow();
        assert!(matches!(
            borrowed_key.as_u16_or_str(),
            U16OrStr::U16(code)
                if *code == u16::from(StdHeaderKey::Body_Type)
                    || *code == u16::from(StdHeaderKey::Data_Type_Id)
        ));
        assert!(borrowed_val.as_num().is_some() || borrowed_val.as_text().is_some());
    }

    /// 测试 `TrHeadersMut` 在空表与非空表上的回报符合契约。
    /// Test that `TrHeadersMut` reports what the contract says on empty and filled maps.
    ///
    /// - 手段：在空表上 `try_add_header`、`remove_header`、`add_or_set_header`；再在已有键上
    ///   重复添加一次。
    /// - 判断：空表的添加成功、删除与覆盖式加都返回 `None`；重复添加返回 `Err` 并交回冲突
    ///   键，且表长不变。修改发生在句柄内部，调用方拿到的仍是同一份存储。
    ///
    /// - Means: call `try_add_header`, `remove_header` and `add_or_set_header` on an empty
    ///   map, then try to add an existing key again.
    /// - Verdict: the first add succeeds, removal and upsert report `None`, and the duplicate
    ///   add returns `Err` with the conflicting key while the length stays put.
    #[test]
    fn tr_headers_mut_reports_contract_() {
        let accept = std_key_(StdHeaderKey::Accept);
        let headers = Headers::new();

        let added = headers
            .clone()
            .try_add_header(accept.clone(), |_| HeaderVal::from_text("a"));
        assert!(added.is_ok(), "空表上添加应当成功");
        assert_eq!(headers.clone().remove_header(accept.clone()), Option::None);
        assert_eq!(
            headers
                .clone()
                .add_or_set_header(accept.clone(), HeaderVal::from_text("b")),
            Option::None
        );

        let mut non_empty = Headers::new();
        non_empty.set_text(StdHeaderKey::Accept, "a");
        let duplicate = non_empty
            .clone()
            .try_add_header(accept.clone(), |_| HeaderVal::from_text("b"));
        assert!(
            matches!(duplicate, Result::Err(key) if key == accept),
            "已有键上重复添加必须交回冲突键"
        );
        assert_eq!(non_empty.len(), 1usize);
    }
}
