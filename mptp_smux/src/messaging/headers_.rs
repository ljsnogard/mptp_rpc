//! MPTP 头表：标准键值、可变契约与 MessagePack 表示。
//!
//! 头表是前缀里**唯一的开放部分**：方法与状态是固定取值，`location` 是一段文本，而头表允许
//! 两端在标准键之外携带自定义键。本模块把这张表实现成一个便宜的句柄
//! （`Arc<BTreeMap<U16OrStr<S>, HeaderVal>>`），因此前缀解码一次之后，handler 拿到的
//! 克隆与框架手里的头表共享同一份存储。
//!
//! # 键类型
//!
//! 键是本 crate 复刻的 [`U16OrStr`]：数字形态是协议标准键（`u16`），字符串形态由使用者在
//! `S` 上选择（`String` / `Arc<str>` / `Box<str>`……），缺省 `S = String`。这与
//! `abs_mptp::spec::TrHeaders::Key` 的要求一致，但为本 crate 的类型保留了 denco 编解码能力。
//!
//! # 线上形态
//!
//! 它被编码成 MessagePack 的「数字或字符串键 → 值」映射。数字键仍写成整数（与旧格式逐字节
//! 兼容），字符串键写成字符串；值暂时仍由 [`HeaderVal`] 表达。
//!
//! # 兼容性
//!
//! - [`HeaderVal`] 暂时保留为值类型；后续会替换为 [`U16OrStr`] 并移除。
//! - 标准键的便捷入口 [`Headers::try_get_val`] / [`Headers::set_num`] /
//!   [`Headers::set_text`] / [`Headers::remove`] 仍收 [`StdHeaderKey`]。
//! - `TrHeaders::try_get_header` 与 `TrHeadersMut` 的键参数现在是 [`U16OrStr`]；
//!   标准键可用 `U16OrStr::U16(u16::from(key))`，或改用 `try_get_val`。
//! - [`Headers::iter`] 返回新键；只需要数字键时用 [`Headers::iter_std`]。
//! - [`HeaderEntry::key`] 返回新键的借用；需要标准键时用
//!   [`HeaderEntry::key_std`]（仅 `HeaderEntry<String>`）。
//!
//! # `headers_`: the MPTP header map
//!
//! The header map is the only open-ended part of a prefix. It is a cheap handle over
//! `Arc<BTreeMap<U16OrStr<S>, HeaderVal>>`, encoded as a MessagePack map whose keys may
//! be integers or strings. The string representation `S` defaults to `String`; `HeaderVal`
//! stays as the value type until the follow-up migration replaces it with `U16OrStr`.

use core::{borrow::Borrow, fmt, future, marker::PhantomData};
use std::{collections::BTreeMap, sync::Arc};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{
        Error as DencoError, MapAccess, MapAccessExt, TrDecodable, TrDecoder, Unexpected, Visitor,
    },
    ser::{TrEncodable, TrEncodeMap, TrEncoder},
};
use abs_mptp::spec::{
    NumOrStr as AbsNumOrStr, StdHeaderKey, StdHeaderVal, TrHeaders, TrHeadersMut,
    TrNumOrStr as AbsTrNumOrStr,
};
use gen_mcf2::gen_may_cancel_future;

use super::num_or_str_::U16OrStr;

/// 一个头值：协议标准头只有「一个数字」或「一段文本」两种形态。
///
/// 数字形态用 `u16`：标准键与大多数标准值都在这个范围里；`Data_Type_Id` 这类 64 位标识按
/// 文本携带（[`HeaderVal::as_u64`] 同时认十进制与十六进制）。
///
/// **兼容期类型**：头表的键已经改为 [`U16OrStr<S>`](U16OrStr)，值暂时保留本枚举；
/// 后续会把它替换为 `U16OrStr<S>` 并移除 `HeaderVal`。
///
/// # 线上表示
///
/// 两种形态直接写成 MessagePack 的整数或字符串，**不写** `"Num"` / `"Text"` 之类的变体
/// 标签。MessagePack 的整数标记与字符串标记本来就不同，接收方按标记分派即可；额外的标签
/// 只会增加字节，不提供任何信息。
///
/// # Examples
///
/// ```
/// use mptp_smux::messaging::HeaderVal;
///
/// let num = HeaderVal::Num(7u16);
/// let text = HeaderVal::Text("0xabc".to_string());
///
/// assert_eq!(num.as_num(), Some(7u16));
/// assert_eq!(text.as_u64(), Some(0xabcu64));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeaderVal {
    /// 数字形态。
    Num(u16),

    /// 文本形态。
    Text(String),
}

impl TrEncodable for HeaderVal {
    type EncodeAsync<'a, S>
        = EncodeHeaderValAsync<'a, 'a, S>
    where
        Self: 'a,
        S: TrEncoder + 'a,
        S::Ok: 'a,
        S::Error: 'a;

    fn encode_async<'a, S>(&'a self, encoder: S) -> Self::EncodeAsync<'a, S>
    where
        Self: 'a,
        S: TrEncoder + 'a,
        S::Ok: 'a,
        S::Error: 'a,
    {
        EncodeHeaderValAsync::new(self, encoder)
    }
}

/// [`HeaderVal`] 编码的 step 函数：数字写整数，文本写字符串，不写变体标签。
#[gen_may_cancel_future(EncodeHeaderVal, pub, new(pub(crate)))]
async fn encode_header_val_async_<'a, S, C>(
    value: &'a HeaderVal,
    encoder: S,
    cancel: C,
) -> Result<S::Ok, S::Error>
where
    S: TrEncoder + 'a,
    S::Ok: 'a,
    S::Error: 'a,
    C: TrCancellationToken,
{
    match value {
        HeaderVal::Num(num) => encoder.encode_u16(*num).may_cancel_with(cancel).await,
        HeaderVal::Text(text) => {
            encoder
                .encode_str(text.as_str())
                .may_cancel_with(cancel)
                .await
        }
    }
}

/// [`HeaderVal`] 的访问者：按 MessagePack 线类型区分整数与字符串。
struct HeaderValVisitor;

impl<'de> Visitor<'de> for HeaderValVisitor {
    type Value = HeaderVal;

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
            .map(HeaderVal::Num)
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
            .map(HeaderVal::Num)
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
        future::ready(Result::Ok(HeaderVal::Text(value.to_string())))
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
        future::ready(Result::Ok(HeaderVal::Text(value)))
    }
}

/// [`HeaderVal`] 解码的 step 函数：交给 `decode_any`，由 `HeaderValVisitor` 分派。
#[gen_may_cancel_future(DecodeHeaderVal, pub, new(pub(crate)))]
async fn decode_header_val_async_<'de, 'a, D, C>(
    decoder: D,
    cancel: C,
) -> Result<HeaderVal, D::Error>
where
    'de: 'a,
    D: TrDecoder<'de> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    decoder
        .decode_any(HeaderValVisitor)
        .may_cancel_with(cancel)
        .await
}

impl<'de> TrDecodable<'de> for HeaderVal {
    type DecodeAsync<'a, D>
        = DecodeHeaderValAsync<'de, 'a, 'a, D>
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
        DecodeHeaderValAsync::new(decoder)
    }
}

impl HeaderVal {
    /// 尽力按数字解读；文本形态会被解析成 `u16`。
    ///
    /// # Examples
    ///
    /// ```
    /// use mptp_smux::messaging::HeaderVal;
    ///
    /// assert_eq!(HeaderVal::Text("42".to_string()).as_num(), Some(42u16));
    /// assert_eq!(HeaderVal::Text("not a number".to_string()).as_num(), None);
    /// ```
    pub fn as_num(&self) -> Option<u16> {
        match self {
            HeaderVal::Num(num) => Option::Some(*num),
            HeaderVal::Text(text) => text.trim().parse::<u16>().ok(),
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
    /// assert_eq!(HeaderVal::Text("0x10".to_string()).as_u64(), Some(16u64));
    /// assert_eq!(HeaderVal::Text("16".to_string()).as_u64(), Some(16u64));
    /// ```
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            HeaderVal::Num(num) => Option::Some(u64::from(*num)),
            HeaderVal::Text(text) => {
                let text = text.trim();
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
    /// assert_eq!(HeaderVal::Text("abc".to_string()).as_text(), Some("abc"));
    /// assert_eq!(HeaderVal::Num(1u16).as_text(), None);
    /// ```
    pub fn as_text(&self) -> Option<&str> {
        match self {
            HeaderVal::Num(_) => Option::None,
            HeaderVal::Text(text) => Option::Some(text.as_str()),
        }
    }
}

impl From<u16> for HeaderVal {
    fn from(num: u16) -> Self {
        HeaderVal::Num(num)
    }
}

impl From<String> for HeaderVal {
    fn from(text: String) -> Self {
        HeaderVal::Text(text)
    }
}

impl From<&str> for HeaderVal {
    fn from(text: &str) -> Self {
        HeaderVal::Text(text.to_string())
    }
}

impl AbsTrNumOrStr for HeaderVal {
    type NumType = u16;

    type StrType = String;

    fn into(self) -> AbsNumOrStr<Self::NumType, Self::StrType> {
        match self {
            HeaderVal::Num(num) => AbsNumOrStr::Num(num),
            HeaderVal::Text(text) => AbsNumOrStr::Str(text),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self {
            HeaderVal::Num(num) => Result::Ok(num),
            HeaderVal::Text(text) => Result::Err(text),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self {
            HeaderVal::Text(text) => Result::Ok(text),
            HeaderVal::Num(num) => Result::Err(num),
        }
    }
}

impl From<HeaderVal> for U16OrStr<String> {
    fn from(value: HeaderVal) -> Self {
        match value {
            HeaderVal::Num(num) => U16OrStr::U16(num),
            HeaderVal::Text(text) => U16OrStr::Str(text),
        }
    }
}

impl From<U16OrStr<String>> for HeaderVal {
    fn from(value: U16OrStr<String>) -> Self {
        match value {
            U16OrStr::U16(num) => HeaderVal::Num(num),
            U16OrStr::Str(text) => HeaderVal::Text(text),
        }
    }
}

/// 头表里的一条：**同时**能借成键与值。
///
/// `abs_mptp` 的 [`TrHeaders`] 要求条目对键与值都可 `Borrow`，因此不能用元组。
/// 键的类型是 [`U16OrStr<S>`](U16OrStr)；[`HeaderEntry::new`] 保留按
/// [`StdHeaderKey`] 构造的兼容入口（字符串形态固定为 `String`）。
///
/// # Examples
///
/// ```
/// use core::borrow::Borrow;
///
/// use abs_mptp::spec::StdHeaderKey;
/// use mptp_smux::messaging::{HeaderEntry, HeaderVal, U16OrStr};
///
/// let entry = HeaderEntry::new(StdHeaderKey::Body_Type, HeaderVal::Num(1u16));
/// let key: &U16OrStr<String> = entry.borrow();
/// let val: &HeaderVal = entry.borrow();
///
/// assert_eq!(*key, U16OrStr::U16(u16::from(StdHeaderKey::Body_Type)));
/// assert_eq!(*val, HeaderVal::Num(1u16));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeaderEntry<S = String>
where
    S: Borrow<str>,
{
    /// 头键。
    key_: U16OrStr<S>,

    /// 头值。
    val_: HeaderVal,
}

impl<S> HeaderEntry<S>
where
    S: Borrow<str>,
{
    /// 由新键类型与值构造一条头。
    pub const fn from_key(key: U16OrStr<S>, val: HeaderVal) -> Self {
        HeaderEntry {
            key_: key,
            val_: val,
        }
    }

    /// 本条头的键。
    pub const fn key(&self) -> &U16OrStr<S> {
        &self.key_
    }

    /// 本条头的值。
    pub const fn value(&self) -> &HeaderVal {
        &self.val_
    }
}

impl HeaderEntry<String> {
    /// 由标准键与值构造一条头（字符串形态固定为 `String`）。
    pub fn new(key: StdHeaderKey, val: HeaderVal) -> Self {
        HeaderEntry {
            key_: U16OrStr::U16(u16::from(key)),
            val_: val,
        }
    }

    /// 若键是数字形态，还原成 [`StdHeaderKey`]；字符串键返回 `None`。
    pub fn key_std(&self) -> Option<StdHeaderKey> {
        match self.key_ {
            U16OrStr::U16(code) => Option::Some(StdHeaderKey::new(code)),
            U16OrStr::Str(_) => Option::None,
        }
    }
}

impl<S> Borrow<U16OrStr<S>> for HeaderEntry<S>
where
    S: Borrow<str>,
{
    fn borrow(&self) -> &U16OrStr<S> {
        &self.key_
    }
}

impl<S> Borrow<HeaderVal> for HeaderEntry<S>
where
    S: Borrow<str>,
{
    fn borrow(&self) -> &HeaderVal {
        &self.val_
    }
}

/// MPTP 头表：`U16OrStr<S>` → [`HeaderVal`]。
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
/// use mptp_smux::messaging::{HeaderVal, Headers, U16OrStr};
///
/// let mut headers = Headers::new();
/// headers.set_text(abs_mptp::spec::StdHeaderKey::Body_Size, "12");
/// headers.set_num(abs_mptp::spec::StdHeaderKey::Body_Type, 1u16);
///
/// let custom = U16OrStr::Str("x-custom".to_string());
/// headers.set(custom.clone(), HeaderVal::Num(7u16));
///
/// assert_eq!(headers.body_size(), Some(12usize));
/// assert_eq!(headers.try_get(custom), Some(&HeaderVal::Num(7u16)));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Headers<S = String>(Arc<BTreeMap<U16OrStr<S>, HeaderVal>>)
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
    pub fn try_get_val(&self, key: StdHeaderKey) -> Option<&HeaderVal> {
        self.try_get(U16OrStr::U16(u16::from(key)))
    }

    /// 按任意头键查一个值。
    pub fn try_get<K>(&self, key: K) -> Option<&HeaderVal>
    where
        K: Borrow<U16OrStr<S>>,
    {
        self.0.get(key.borrow())
    }

    /// 遍历全部条目。
    pub fn iter(&self) -> impl Iterator<Item = (&U16OrStr<S>, &HeaderVal)> + '_ {
        self.0.iter()
    }

    /// 只遍历数字键，并还原成本端的 [`StdHeaderKey`]。
    pub fn iter_std(&self) -> impl Iterator<Item = (StdHeaderKey, &HeaderVal)> + '_ {
        self.0.iter().filter_map(|(key, val)| match key {
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
        match AbsTrNumOrStr::into(val.clone()) {
            AbsNumOrStr::Num(num) => StdHeaderVal::from(num) == StdHeaderVal::Body_Transfer_Chunked,
            AbsNumOrStr::Str(text) => text.trim().eq_ignore_ascii_case("chunked"),
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
        self.set(U16OrStr::U16(u16::from(key)), HeaderVal::Num(num));
    }

    /// 按标准键写一个文本值（覆盖已有值）。
    pub fn set_text(&mut self, key: StdHeaderKey, text: impl Into<String>) {
        self.set(U16OrStr::U16(u16::from(key)), HeaderVal::Text(text.into()));
    }

    /// 按任意头键写一个值（覆盖已有值）。
    pub fn set<K>(&mut self, key: K, val: HeaderVal)
    where
        K: Into<U16OrStr<S>>,
    {
        Arc::make_mut(&mut self.0).insert(key.into(), val);
    }

    /// 删除一个标准键，返回被删掉的值。
    pub fn remove(&mut self, key: StdHeaderKey) -> Option<HeaderVal> {
        self.remove_key(U16OrStr::U16(u16::from(key)))
    }

    /// 删除一个任意头键，返回被删掉的值。
    pub fn remove_key<K>(&mut self, key: K) -> Option<HeaderVal>
    where
        K: Borrow<U16OrStr<S>>,
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
    type Key = U16OrStr<S>;

    type Val = HeaderVal;

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
            .map(|(key, val_)| HeaderEntry {
                key_: key.clone(),
                val_: val_.clone(),
            })
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
        Result::Ok(HeaderEntry { key_, val_ })
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

/// [`Headers`] 编码的 step 函数：键按 [`U16OrStr`] 的线类型写，值交给 [`HeaderVal`]。
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

/// [`Headers`] 的访问者：遍历 MessagePack 映射，键交给 [`U16OrStr`]，值交给 [`HeaderVal`]。
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
        .next_entry::<U16OrStr<S>, HeaderVal>()
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
    fn std_key_(key: StdHeaderKey) -> U16OrStr<String> {
        U16OrStr::U16(u16::from(key))
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
            Option::Some(&HeaderVal::Num(1u16))
        );
        assert_eq!(headers.data_type_id(), Option::Some(0xabcu64));

        // 旧入口 `set(StdHeaderKey, HeaderVal)` 仍通过 `Into<U16OrStr>` 工作。
        headers.set(StdHeaderKey::Accept, HeaderVal::Text("a".to_string()));
        assert_eq!(
            headers.try_get_val(StdHeaderKey::Accept),
            Option::Some(&HeaderVal::Text("a".to_string()))
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
            Option::Some(&HeaderVal::Num(0x1234u16))
        );
        assert_eq!(headers.remove(key), Option::Some(HeaderVal::Num(0x1234u16)));
        assert!(headers.is_empty());
    }

    /// 测试字符串键可写入、读取、遍历与删除。
    /// Test that a string key can be written, read, iterated and removed.
    ///
    /// - 手段：用 `U16OrStr::Str` 写一条自定义头，分别调用 `try_get` / `iter` / `remove_key`。
    /// - 判断：读到的值一致，遍历只看到这一条，删除后表为空。
    ///
    /// - Means: write a custom header under `U16OrStr::Str`, then call `try_get` / `iter` /
    ///   `remove_key`.
    /// - Verdict: the value matches, iteration sees exactly that entry, and the map is empty
    ///   after removal.
    #[test]
    fn string_key_roundtrips_() {
        let key: U16OrStr<String> = U16OrStr::Str("x-custom".to_string());
        let mut headers = Headers::new();
        headers.set(key.clone(), HeaderVal::Num(7u16));

        assert_eq!(headers.try_get(&key), Option::Some(&HeaderVal::Num(7u16)));
        assert_eq!(headers.len(), 1usize);
        assert_eq!(headers.iter().count(), 1usize);
        assert_eq!(headers.iter_std().count(), 0usize);
        assert_eq!(headers.remove_key(&key), Option::Some(HeaderVal::Num(7u16)));
        assert!(headers.is_empty());
    }

    /// 测试字符串实现可以由使用者选择（这里用 `Arc<str>`）。
    /// Test that the string representation is user-selectable (using `Arc<str>` here).
    ///
    /// - 手段：构造 `Headers<Arc<str>>`，用 `U16OrStr::Str` 写入一个键并读回。
    /// - 判断：值与键都正确，证明 `S` 不是硬编码的 `String`。
    ///
    /// - Means: build `Headers<Arc<str>>`, write a key through `U16OrStr::Str`, and read it
    ///   back.
    /// - Verdict: both key and value round-trip, so `S` is not hard-coded to `String`.
    #[test]
    fn custom_string_impl_works_() {
        let key: U16OrStr<Arc<str>> = U16OrStr::Str(Arc::<str>::from("x-custom"));
        let mut headers: Headers<Arc<str>> = Headers::empty();
        headers.set(key.clone(), HeaderVal::Text("v".to_string()));

        assert_eq!(
            headers.try_get(key),
            Option::Some(&HeaderVal::Text("v".to_string()))
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
            Option::Some(&HeaderVal::Num(1u16))
        );

        let all: Vec<HeaderEntry> = headers.into_entries().collect();
        assert_eq!(all.len(), 2usize);
        let borrowed_key: &U16OrStr<String> = all[0].borrow();
        let borrowed_val: &HeaderVal = all[0].borrow();
        assert!(matches!(
            *borrowed_key,
            U16OrStr::U16(code)
                if code == u16::from(StdHeaderKey::Body_Type)
                    || code == u16::from(StdHeaderKey::Data_Type_Id)
        ));
        assert!(matches!(
            *borrowed_val,
            HeaderVal::Num(1u16) | HeaderVal::Text(_)
        ));
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
            .try_add_header(accept.clone(), |_| HeaderVal::Text("a".to_string()));
        assert!(added.is_ok(), "空表上添加应当成功");
        assert_eq!(headers.clone().remove_header(accept.clone()), Option::None);
        assert_eq!(
            headers
                .clone()
                .add_or_set_header(accept.clone(), HeaderVal::Text("b".to_string())),
            Option::None
        );

        let mut non_empty = Headers::new();
        non_empty.set_text(StdHeaderKey::Accept, "a");
        let duplicate = non_empty
            .clone()
            .try_add_header(accept.clone(), |_| HeaderVal::Text("b".to_string()));
        assert!(
            matches!(duplicate, Result::Err(key) if key == accept),
            "已有键上重复添加必须交回冲突键"
        );
        assert_eq!(non_empty.len(), 1usize);
    }
}
