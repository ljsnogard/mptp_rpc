//! MPTP 头表：标准键值、可变契约与 MessagePack 表示。
//!
//! 头表是前缀里**唯一的开放部分**：方法与状态是固定取值，`location` 是一段文本，而头表允许
//! 两端在标准键之外携带自定义键。本模块把这张表实现成一个便宜的句柄
//! （`Arc<HeaderMap<A>, A>`），因此前缀解码一次之后，handler
//! 拿到的克隆与框架手里的头表共享同一份存储；`A` 是调用方选择的 [`Allocator`]。
//!
//! # 键与值
//!
//! 键仍然是「数字或字符串」：[`HeaderKey`] 是 [`U16OrStr<A>`] 的别名，数字形态是协议
//! 标准键值（`u16`），字符串形态是 `abs_str::String<A>`。值改用 [`HeaderVal`]，它只有
//! 两种形态：
//!
//! - [`HeaderVal::from_num`]：简单标准头值（`Body_Size`、`Body_Transfer`……）；
//! - [`HeaderVal::from_obj`]：复杂值，承载 `abs_mptp::spec::ObjEnvelope` 的
//!   MessagePack 字节流与逻辑类型 `type_id`。
//!
//! 键与值的字符串 / 字节容器都由 `A` 分配；类型自身只要求 `A: Allocator`，构造与解码
//! 另需 `A: Clone` / `A: TrAllocSource`。
//!
//! # 线上形态
//!
//! 头表被编码成 MessagePack 的映射。键直接写整数或字符串；值写整数，
//! 或写一个两元素数组 `[type_id(u32), bin]`。不写变体标签。
//!
//! # `headers_`: the MPTP header map
//!
//! The header map is a cheap handle over an allocator-aware sorted `Vec` map. Keys stay
//! numeric-or-textual; values are either numbers or a two-element MessagePack array
//! `[type_id(u32), bin]`. Every string / byte container is allocated by `A: Allocator`.

use alloc::{
    sync::Arc,
    vec::{IntoIter, Vec},
};
use core::{
    alloc::{Allocator, AllocatorClone},
    borrow::Borrow,
    cmp::Ordering,
    fmt, future,
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use abs_denco::{
    de::{
        Error as DencoError, MapAccess, MapAccessExt, SeqAccess, SeqAccessExt, TrAllocSource,
        TrArena, TrDecodable, TrDecoder, Unexpected, Visitor,
    },
    ser::{TrEncodable, TrEncodeMap, TrEncodeSeq, TrEncoder},
};
use abs_mptp::spec::{
    NumOrObj as AbsNumOrObj, ObjEnvelope, StdHeaderKey, StdHeaderVal, TrHeaders, TrHeadersMut,
    TrNumOrObj,
};
use gen_mcf2::gen_may_cancel_future;

use super::num_or_str_::U16OrStr;

/// 按 `HeaderKey` 排序的条目表，使用 `Vec<T, A>` 而不是 `BTreeMap`。
///
/// `BTreeMap` 的 allocator 方法在 nightly 上要求 `A: AllocatorClone`（一个需要显式实现的
/// marker trait，`A: Allocator + Clone` 并不自动满足它）。为了让本 crate 的类型只要求
/// `A: Allocator`，头表用「排序 `Vec` + 二分查找」实现；头数量很小，插入 / 删除的线性搬移
/// 可以接受。
struct HeaderMap<A: Allocator>(Vec<(HeaderKey<A>, HeaderVal<A>), A>);

impl<A: Allocator> HeaderMap<A> {
    /// 用 `alloc` 造一张空表。
    fn new_in_(alloc: A) -> Self {
        HeaderMap(Vec::new_in(alloc))
    }

    /// 头表里有多少条。
    fn len_(&self) -> usize {
        self.0.len()
    }

    /// 头表是否为空。
    fn is_empty_(&self) -> bool {
        self.0.is_empty()
    }

    /// 二分查找键的位置：命中返回 `Ok`，否则返回可插入位置 `Err`。
    fn position_(&self, key: &HeaderKey<A>) -> Result<usize, usize> {
        self.0.binary_search_by(|(candidate, _)| candidate.cmp(key))
    }

    /// 按键取值。
    fn get_(&self, key: &HeaderKey<A>) -> Option<&HeaderVal<A>> {
        self.position_(key).ok().map(|pos| &self.0[pos].1)
    }

    /// 键是否存在。
    fn contains_key_(&self, key: &HeaderKey<A>) -> bool {
        self.position_(key).is_ok()
    }

    /// 插入或替换，返回被替换的旧值。
    fn insert_(&mut self, key: HeaderKey<A>, val: HeaderVal<A>) -> Option<HeaderVal<A>> {
        match self.position_(&key) {
            Result::Ok(pos) => Option::Some(core::mem::replace(&mut self.0[pos].1, val)),
            Result::Err(pos) => {
                self.0.insert(pos, (key, val));
                Option::None
            }
        }
    }

    /// 删除并返回整条。
    fn remove_entry_(&mut self, key: &HeaderKey<A>) -> Option<(HeaderKey<A>, HeaderVal<A>)> {
        match self.position_(key) {
            Result::Ok(pos) => Option::Some(self.0.remove(pos)),
            Result::Err(_) => Option::None,
        }
    }

    /// 遍历全部条目。
    fn iter_(&self) -> impl Iterator<Item = (&HeaderKey<A>, &HeaderVal<A>)> + '_ {
        self.0.iter().map(|(key, val)| (key, val))
    }
}

impl<A: Allocator + Clone> Clone for HeaderMap<A> {
    fn clone(&self) -> Self {
        HeaderMap(self.0.clone())
    }
}

impl<A: Allocator> PartialEq for HeaderMap<A> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<A: Allocator> Eq for HeaderMap<A> {}

/// 一个头键：数字形态是协议标准键，字符串形态由 [`Allocator`] 分配。
///
/// 它等价于 [`U16OrStr<A>`]；保留这个名字是为了让 [`HeaderEntry`] 的键字段有强类型，
/// 并保持原有调用点可读。
///
/// A header key: a numeric protocol key or a string allocated by `A`. It is the same type
/// as [`U16OrStr<A>`].
pub type HeaderKey<A> = U16OrStr<A>;

/// 文本头值的逻辑类型标识：与 `str` / `String` 的 denco 结构化指纹一致。
///
/// 文本仍写成 `Obj`，载荷是 **MessagePack string**（不是裸 UTF-8）；这样 `Allow`、
/// `Accept` 这类标准文本头仍然可以往返，同时保持「值是 Num 或带 type_id 的 MessagePack
/// 字节流」这一条协议形状。
pub const K_TEXT_HEADER_TYPE_ID: u32 =
    <str as abs_denco::type_id::TrHasTypeId>::TYPE_ID_32;

/// 把一段文本写成 MessagePack string 追加到 `out`。
fn push_msgpack_str_<A: Allocator>(out: &mut Vec<u8, A>, text: &str) {
    let len = text.len();
    if len <= 31usize {
        out.push(0xA0u8 | (len as u8));
    } else if let Result::Ok(len) = u8::try_from(len) {
        out.push(0xD9u8);
        out.push(len);
    } else if let Result::Ok(len) = u16::try_from(len) {
        out.push(0xDAu8);
        out.extend_from_slice(&len.to_be_bytes());
    } else {
        out.push(0xDBu8);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(text.as_bytes());
}

/// 解析一段 MessagePack string；不是单个合法 string 时返回 `None`。
fn decode_msgpack_str_(bytes: &[u8]) -> Option<&str> {
    let (len, offset) = match *bytes.first()? {
        code @ 0xA0u8..=0xBFu8 => (usize::from(code & 0x1Fu8), 1usize),
        0xD9u8 => (usize::from(*bytes.get(1usize)?), 2usize),
        0xDAu8 => (
            usize::from(u16::from_be_bytes([
                *bytes.get(1usize)?,
                *bytes.get(2usize)?,
            ])),
            3usize,
        ),
        0xDBu8 => (
            usize::try_from(u32::from_be_bytes([
                *bytes.get(1usize)?,
                *bytes.get(2usize)?,
                *bytes.get(3usize)?,
                *bytes.get(4usize)?,
            ]))
            .ok()?,
            5usize,
        ),
        _ => return Option::None,
    };
    let end = offset.checked_add(len)?;
    let text = core::str::from_utf8(bytes.get(offset..end)?).ok()?;
    if end == bytes.len() {
        Option::Some(text)
    } else {
        Option::None
    }
}

/// 一个头值：`u16` 数字，或带逻辑类型标识的 MessagePack 字节流。
///
/// 简单标准头（`Body_Size`、`Body_Transfer`……）用 [`HeaderVal::from_num`]；复杂值用
/// [`HeaderVal::from_obj`]，其字节流由 `A` 分配。
///
/// # Examples
///
/// ```
/// use mptp_wire::HeaderVal;
/// use std::alloc::Global;
///
/// let num = HeaderVal::<Global>::from_num(7u16);
/// assert_eq!(num.as_num(), Some(7u16));
///
/// let obj = HeaderVal::from_bytes(0xabc, b"msgpack", Global);
/// assert_eq!(obj.type_id(), Some(0xabc));
/// assert_eq!(obj.data(), Some(&b"msgpack"[..]));
/// ```
///
/// A header value: a `u16` number or a type-tagged MessagePack byte stream allocated by
/// `A`.
pub struct HeaderVal<A: Allocator>(U16OrObj<A>);

/// [`HeaderVal`] 的内部形态。
enum U16OrObj<A: Allocator> {
    /// 数字形态。
    Num(u16),

    /// 对象形态：逻辑类型 + MessagePack 字节流。
    Obj(ObjEnvelope<Vec<u8, A>>),
}

impl<A: Allocator + Clone> Clone for HeaderVal<A> {
    fn clone(&self) -> Self {
        match &self.0 {
            U16OrObj::Num(num) => HeaderVal(U16OrObj::Num(*num)),
            U16OrObj::Obj(obj) => HeaderVal(U16OrObj::Obj(obj.clone())),
        }
    }
}

impl<A: Allocator> fmt::Debug for HeaderVal<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            U16OrObj::Num(num) => formatter.debug_tuple("Num").field(num).finish(),
            U16OrObj::Obj(obj) => formatter
                .debug_struct("Obj")
                .field("type_id", &obj.type_id())
                .field("data", &obj.data())
                .finish(),
        }
    }
}

impl<A: Allocator> PartialEq for HeaderVal<A> {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (U16OrObj::Num(left), U16OrObj::Num(right)) => left == right,
            (U16OrObj::Obj(left), U16OrObj::Obj(right)) => {
                left.type_id() == right.type_id() && left.data() == right.data()
            }
            _ => false,
        }
    }
}

impl<A: Allocator> Eq for HeaderVal<A> {}

impl<A: Allocator> PartialOrd for HeaderVal<A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl<A: Allocator> Ord for HeaderVal<A> {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (U16OrObj::Num(left), U16OrObj::Num(right)) => left.cmp(right),
            (U16OrObj::Num(_), U16OrObj::Obj(_)) => Ordering::Less,
            (U16OrObj::Obj(_), U16OrObj::Num(_)) => Ordering::Greater,
            (U16OrObj::Obj(left), U16OrObj::Obj(right)) => {
                (left.type_id(), left.data()).cmp(&(right.type_id(), right.data()))
            }
        }
    }
}

impl<A: Allocator> HeaderVal<A> {
    /// 由标准头的固定取值构造数字形态。
    ///
    /// Build the numeric form from a standard-header value.
    pub const fn from_header_val(value: StdHeaderVal) -> Self {
        HeaderVal(U16OrObj::Num(value.into_inner()))
    }

    /// 由裸 `u16` 构造数字形态（不校验它是否是标准值）。
    ///
    /// Build the numeric form from a bare `u16` without checking its standard meaning.
    pub const fn from_num(num: u16) -> Self {
        HeaderVal(U16OrObj::Num(num))
    }

    /// 由 `type_id` 与已分配的 MessagePack 字节流构造对象形态。
    ///
    /// Build the object form from a logical `type_id` and an already allocated MessagePack
    /// byte stream.
    pub fn from_obj(type_id: u32, data: Vec<u8, A>) -> Self {
        HeaderVal(U16OrObj::Obj(ObjEnvelope::new(type_id, data)))
    }

    /// 把一段字节拷进 `alloc` 并构造对象形态。
    ///
    /// Copy a byte slice into `alloc` and build the object form.
    pub fn from_bytes(type_id: u32, data: &[u8], alloc: A) -> Self
    where
        A: Clone,
    {
        let mut owned = Vec::with_capacity_in(data.len(), alloc);
        owned.extend_from_slice(data);
        HeaderVal::from_obj(type_id, owned)
    }

    /// 由一段文本构造对象形态；载荷是 MessagePack string，逻辑类型是 `str` 的指纹。
    ///
    /// Build the object form from text; the payload is a MessagePack string tagged with the
    /// `str` type fingerprint.
    pub fn from_text(text: impl AsRef<str>, alloc: A) -> Self
    where
        A: Clone,
    {
        let text = text.as_ref();
        let mut data = Vec::with_capacity_in(text.len().saturating_add(5usize), alloc);
        push_msgpack_str_(&mut data, text);
        HeaderVal::from_obj(K_TEXT_HEADER_TYPE_ID, data)
    }

    /// 文本形态的借用；不是文本对象时返回 `None`。
    ///
    /// Borrow the textual form; returns `None` for non-text objects and numbers.
    pub fn as_text(&self) -> Option<&str> {
        if self.type_id()? != K_TEXT_HEADER_TYPE_ID {
            return Option::None;
        }
        decode_msgpack_str_(self.data()?)
    }

    /// 尽力按 `u64` 解读：数字形态直接转换；文本形态同时认十进制与 `0x` 十六进制。
    ///
    /// Interpret the value as a `u64`: numbers convert directly, text accepts decimal and
    /// `0x`-prefixed hexadecimal.
    pub fn as_u64(&self) -> Option<u64> {
        if let Option::Some(num) = self.as_num() {
            return Option::Some(u64::from(num));
        }
        let text = self.as_text()?.trim();
        let hex = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X"));
        match hex {
            Option::Some(hex) => u64::from_str_radix(hex, 16u32).ok(),
            Option::None => text.parse::<u64>().ok(),
        }
    }

    /// 数字形态的取值；对象形态返回 `None`。
    ///
    /// The numeric value, or `None` for the object form.
    pub const fn as_num(&self) -> Option<u16> {
        match &self.0 {
            U16OrObj::Num(num) => Option::Some(*num),
            U16OrObj::Obj(_) => Option::None,
        }
    }

    /// 对象形态的借用；数字形态返回 `None`。
    ///
    /// Borrow the object form, or `None` for a numeric value.
    pub fn as_obj(&self) -> Option<&ObjEnvelope<Vec<u8, A>>> {
        match &self.0 {
            U16OrObj::Num(_) => Option::None,
            U16OrObj::Obj(obj) => Option::Some(obj),
        }
    }

    /// 对象形态的逻辑类型标识；数字形态返回 `None`。
    ///
    /// The logical type identifier of the object form, or `None` for a number.
    pub fn type_id(&self) -> Option<u32> {
        self.as_obj().map(ObjEnvelope::type_id)
    }

    /// 对象形态的 MessagePack 字节流；数字形态返回 `None`。
    ///
    /// The MessagePack byte stream of the object form, or `None` for a number.
    pub fn data(&self) -> Option<&[u8]> {
        self.as_obj().map(ObjEnvelope::data)
    }
}

impl<A: Allocator> From<StdHeaderVal> for HeaderVal<A> {
    fn from(value: StdHeaderVal) -> Self {
        HeaderVal::from_header_val(value)
    }
}

impl<A: Allocator> TrNumOrObj for HeaderVal<A> {
    type NumType = u16;

    type ObjType = ObjEnvelope<Vec<u8, A>>;

    fn as_num_or_obj(&self) -> AbsNumOrObj<Self::NumType, &[u8]> {
        match &self.0 {
            U16OrObj::Num(num) => AbsNumOrObj::Num(*num),
            U16OrObj::Obj(obj) => AbsNumOrObj::Obj(obj.as_ref()),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::ObjType> {
        match self.0 {
            U16OrObj::Num(num) => Result::Ok(num),
            U16OrObj::Obj(obj) => Result::Err(obj),
        }
    }

    fn try_match_obj(self) -> Result<Self::ObjType, Self::NumType> {
        match self.0 {
            U16OrObj::Obj(obj) => Result::Ok(obj),
            U16OrObj::Num(num) => Result::Err(num),
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 头值的 MessagePack 编解码
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// MessagePack `bin` 段的编码适配器。
///
/// `[T]` 的 `TrEncodable` 实现写的是「整数序列」；对象载荷要的是一段 binary，因此用这个
/// 薄适配器把 `encoder.encode_bytes` 暴露成序列元素。
struct BinRef<'b>(&'b [u8]);

impl<'b> TrEncodable for BinRef<'b> {
    type EncodeAsync<'a, E>
        = E::EncodeBytesAsync<'a, 'b>
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
        encoder.encode_bytes(self.0)
    }
}

impl<A: Allocator> TrEncodable for HeaderVal<A> {
    type EncodeAsync<'a, E>
        = EncodeHeaderValAsync<'a, 'a, A, E>
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
        EncodeHeaderValAsync::new(self, encoder)
    }
}

/// [`HeaderVal`] 编码的 step 函数：数字写整数，对象写 `[type_id, bin]`。
#[gen_may_cancel_future(EncodeHeaderVal, pub, new(pub(crate)))]
async fn encode_header_val_async_<'a, A, E, C>(
    value: &'a HeaderVal<A>,
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
    match &value.0 {
        U16OrObj::Num(num) => encoder.encode_u16(*num).may_cancel_with(cancel).await,
        U16OrObj::Obj(obj) => {
            let mut seq = encoder
                .encode_seq(Option::Some(2usize))
                .may_cancel_with(cancel.child_token())
                .await?;
            let type_id = obj.type_id();
            seq.encode_element(&type_id)
                .may_cancel_with(cancel.child_token())
                .await?;
            let bin = BinRef(obj.data());
            seq.encode_element(&bin)
                .may_cancel_with(cancel.child_token())
                .await?;
            seq.end().may_cancel_with(cancel).await
        }
    }
}

/// 从 `decoder` 读出的一段、由 `A` 分配的字节。
pub(crate) struct AllocBytes<A: Allocator>(Vec<u8, A>);

impl<A: Allocator> AllocBytes<A> {
    fn into_vec(self) -> Vec<u8, A> {
        self.0
    }
}

/// `bin` 段的访问者：把借来的字节拷进 `A`。
struct BytesVisitor<A: Allocator> {
    alloc_: A,
}

impl<'alloc, Alloc, A> Visitor<'alloc, Alloc> for BytesVisitor<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + 'alloc,
{
    type Value = AllocBytes<A>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a MessagePack binary value")
    }

    fn visit_bytes<'a, E>(
        self,
        value: &'a [u8],
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
    {
        let mut owned = Vec::with_capacity_in(value.len(), self.alloc_);
        owned.extend_from_slice(value);
        future::ready(Result::Ok(AllocBytes(owned)))
    }

    fn visit_byte_buf<'a, E>(
        self,
        value: alloc::vec::Vec<u8>,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, E>>
    where
        Self: 'a,
        Self::Value: 'a,
        E: DencoError + 'a,
        'alloc: 'a,
    {
        let mut owned = Vec::with_capacity_in(value.len(), self.alloc_);
        owned.extend_from_slice(value.as_slice());
        future::ready(Result::Ok(AllocBytes(owned)))
    }
}

impl<'alloc, Alloc, A> TrDecodable<'alloc, Alloc> for AllocBytes<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type DecodeAsync<'a, D>
        = DecodeAllocBytesAsync<'alloc, 'a, 'a, Alloc, A, D>
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
        DecodeAllocBytesAsync::new(de, alloc)
    }
}

/// [`AllocBytes`] 解码的 step 函数。
#[gen_may_cancel_future(DecodeAllocBytes, pub(crate), new(pub(crate)))]
async fn decode_alloc_bytes_async_<'alloc, 'a, Alloc, A, D, C>(
    de: D,
    alloc: A,
    cancel: C,
) -> Result<AllocBytes<A>, D::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    D: TrDecoder<'alloc, Alloc> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    de.decode_bytes(BytesVisitor { alloc_: alloc })
        .may_cancel_with(cancel)
        .await
}

/// [`HeaderVal`] 的访问者：数字或两元素数组。
struct ValVisitor<A: Allocator> {
    alloc_: A,
}

impl<'alloc, Alloc, A> Visitor<'alloc, Alloc> for ValVisitor<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type Value = HeaderVal<A>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a non-negative integer or a [type_id, bin] array")
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
        let result = u16::try_from(value).map(HeaderVal::from_num).map_err(|_| {
            <E as DencoError>::invalid_value(
                Unexpected::Unsigned(value),
                &"a non-negative integer or a [type_id, bin] array",
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
        let result = u16::try_from(value).map(HeaderVal::from_num).map_err(|_| {
            <E as DencoError>::invalid_value(
                Unexpected::Signed(value),
                &"a non-negative integer or a [type_id, bin] array",
            )
        });
        future::ready(result)
    }

    fn visit_seq<'a, S>(
        self,
        seq: S,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, S::Error>>
    where
        'alloc: 'a,
        Self: 'a,
        Self::Value: 'a,
        S: SeqAccess<'alloc, Alloc> + 'a,
        S::Error: 'a,
    {
        DecodeHeaderObjAsync::new(seq, self.alloc_)
    }
}

/// [`ValVisitor::visit_seq`] 的 step 函数：读 `[type_id, bin]`。
#[gen_may_cancel_future(DecodeHeaderObj, pub(crate), new(pub(crate)))]
async fn decode_header_obj_async_<'alloc, 'a, Alloc, A, S, C>(
    mut seq: S,
    alloc: A,
    cancel: C,
) -> Result<HeaderVal<A>, S::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    S: SeqAccess<'alloc, Alloc> + 'a,
    S::Error: 'a,
    C: TrCancellationToken,
{
    let _ = alloc;
    let type_id = seq
        .next_element::<u32>()
        .may_cancel_with(cancel.child_token())
        .await?
        .ok_or_else(|| {
            <S::Error as DencoError>::invalid_length(0usize, &"a [type_id, bin] array")
        })?;
    let data = seq
        .next_element::<AllocBytes<A>>()
        .may_cancel_with(cancel.child_token())
        .await?
        .ok_or_else(|| {
            <S::Error as DencoError>::invalid_length(1usize, &"a [type_id, bin] array")
        })?;
    if seq
        .next_element::<()>()
        .may_cancel_with(cancel.child_token())
        .await?
        .is_some()
    {
        return Result::Err(<S::Error as DencoError>::invalid_length(
            3usize,
            &"a [type_id, bin] array",
        ));
    }
    Result::Ok(HeaderVal::from_obj(type_id, data.into_vec()))
}

impl<'alloc, Alloc, A> TrDecodable<'alloc, Alloc> for HeaderVal<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type DecodeAsync<'a, D>
        = DecodeHeaderValAsync<'alloc, 'a, 'a, Alloc, A, D>
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
        DecodeHeaderValAsync::new(de, alloc)
    }
}

/// [`HeaderVal`] 解码的 step 函数：交给 `decode_any`，由 [`ValVisitor`] 分派。
#[gen_may_cancel_future(DecodeHeaderVal, pub, new(pub(crate)))]
async fn decode_header_val_async_<'alloc, 'a, Alloc, A, D, C>(
    de: D,
    alloc: A,
    cancel: C,
) -> Result<HeaderVal<A>, D::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    D: TrDecoder<'alloc, Alloc> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    de.decode_any(ValVisitor { alloc_: alloc })
        .may_cancel_with(cancel)
        .await
}

/// 头表里的一条：**同时**能借成键与值。
#[derive(PartialEq, Eq)]
pub struct HeaderEntry<A: Allocator> {
    /// 头键。
    key_: HeaderKey<A>,

    /// 头值。
    val_: HeaderVal<A>,
}

impl<A: Allocator + Clone> Clone for HeaderEntry<A> {
    fn clone(&self) -> Self {
        HeaderEntry {
            key_: self.key_.clone(),
            val_: self.val_.clone(),
        }
    }
}

impl<A: Allocator> fmt::Debug for HeaderEntry<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HeaderEntry")
            .field("key", &self.key_)
            .field("val", &self.val_)
            .finish()
    }
}

impl<A: Allocator> HeaderEntry<A> {
    /// 由新键类型与值构造一条头。
    pub const fn from_kv(key: HeaderKey<A>, val: HeaderVal<A>) -> Self {
        HeaderEntry {
            key_: key,
            val_: val,
        }
    }

    /// 本条头的键。
    pub const fn key(&self) -> &HeaderKey<A> {
        &self.key_
    }

    /// 本条头的值。
    pub const fn value(&self) -> &HeaderVal<A> {
        &self.val_
    }

    /// 由标准键与值构造一条头。
    pub fn new(key: StdHeaderKey, val: HeaderVal<A>) -> Self {
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

impl<A: Allocator> Borrow<HeaderKey<A>> for HeaderEntry<A> {
    fn borrow(&self) -> &HeaderKey<A> {
        &self.key_
    }
}

impl<A: Allocator> Borrow<HeaderVal<A>> for HeaderEntry<A> {
    fn borrow(&self) -> &HeaderVal<A> {
        &self.val_
    }
}

/// MPTP 头表：`HeaderKey<A>` → [`HeaderVal<A>`]。
///
/// # 为什么内部是 `Arc<HeaderMap<A>, A>`
///
/// `abs_mptp` 的 [`TrHeadersMut`] 把所有修改入口都定义成按值收 `self`，却只交出旧值；修改
/// 只可能发生在**共享状态**里，否则就丢了。因此这一档实现是一个句柄，而不是一个独占的 map。
///
/// `Arc::make_mut` 是它的写时复制版本：唯一持有者时修改**就地生效**；多持有者时修改落在
/// 临时副本上。框架只在把请求交给 handler 之前克隆一次头表，之后的修改路径不共享句柄。
/// map 与 `Arc` 自己都用 `A` 分配。
///
/// # Examples
///
/// ```
/// use mptp_wire::{HeaderKey, HeaderVal, Headers};
/// use std::alloc::Global;
///
/// let mut headers = Headers::new(Global);
/// headers.set_num(abs_mptp::spec::StdHeaderKey::Body_Size, 12);
/// headers.set_num(abs_mptp::spec::StdHeaderKey::Body_Type, 1);
///
/// let custom = HeaderKey::from_text("x-custom", Global);
/// headers.set(custom.clone(), HeaderVal::from_num(7));
///
/// assert_eq!(headers.body_size(), Some(12usize));
/// assert_eq!(headers.try_get(custom), Some(&HeaderVal::from_num(7)));
/// ```
pub struct Headers<A: Allocator> {
    map_: Arc<HeaderMap<A>, A>,
}

impl<A: AllocatorClone> Clone for Headers<A> {
    fn clone(&self) -> Self {
        Headers {
            map_: self.map_.clone(),
        }
    }
}

impl<A: Allocator> fmt::Debug for Headers<A> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_map().entries(self.map_.iter_()).finish()
    }
}

impl<A: Allocator> PartialEq for Headers<A> {
    fn eq(&self, other: &Self) -> bool {
        *self.map_ == *other.map_
    }
}

impl<A: Allocator> Eq for Headers<A> {}

impl<A: Allocator + Clone> Headers<A> {
    /// 造一张空头表；map 与 `Arc` 都由 `alloc` 分配。
    ///
    /// Build an empty header map; both the map and the `Arc` are allocated by `alloc`.
    pub fn new(alloc: A) -> Self {
        let map = HeaderMap::new_in_(alloc.clone());
        Headers {
            map_: Arc::new_in(map, alloc),
        }
    }

    /// [`Headers::new`] 的别名，保留旧名字。
    ///
    /// An alias of [`Headers::new`], kept for the old name.
    pub fn empty(alloc: A) -> Self {
        Self::new(alloc)
    }
}

impl<A: AllocatorClone> Headers<A> {
    /// 按标准键写一个文本值（覆盖已有值）；文本写成带 `str` 类型标识的 MessagePack 载荷。
    ///
    /// Write a textual value; the payload is a MessagePack string tagged with the `str`
    /// type fingerprint.
    pub fn set_text(&mut self, key: StdHeaderKey, text: impl AsRef<str>) {
        let alloc = Arc::allocator(&self.map_).clone();
        self.set(key, HeaderVal::from_text(text, alloc));
    }

    /// 按标准键写一个数字值（覆盖已有值）。
    pub fn set_num(&mut self, key: StdHeaderKey, num: u16) {
        self.set(HeaderKey::from(key), HeaderVal::from_num(num));
    }

    /// 按任意头键写一个值（覆盖已有值）。
    pub fn set<K>(&mut self, key: K, val: HeaderVal<A>)
    where
        K: Into<HeaderKey<A>>,
    {
        Arc::make_mut(&mut self.map_).insert_(key.into(), val);
    }

    /// 删除一个标准键，返回被删掉的值。
    pub fn remove(&mut self, key: StdHeaderKey) -> Option<HeaderVal<A>> {
        self.remove_key(HeaderKey::from(key))
    }

    /// 删除一个任意头键，返回被删掉的值。
    pub fn remove_key<K>(&mut self, key: K) -> Option<HeaderVal<A>>
    where
        K: Borrow<HeaderKey<A>>,
    {
        Arc::make_mut(&mut self.map_)
            .remove_entry_(key.borrow())
            .map(|(_, val)| val)
    }
}

impl<A: Allocator> Headers<A> {
    /// 头表里有多少条。
    pub fn len(&self) -> usize {
        self.map_.len_()
    }

    /// 头表是否为空。
    pub fn is_empty(&self) -> bool {
        self.map_.is_empty_()
    }

    /// 按标准键查一个值。
    pub fn try_get_val(&self, key: StdHeaderKey) -> Option<&HeaderVal<A>> {
        self.try_get(HeaderKey::from(key))
    }

    /// 按任意头键查一个值。
    pub fn try_get<K>(&self, key: K) -> Option<&HeaderVal<A>>
    where
        K: Borrow<HeaderKey<A>>,
    {
        self.map_.get_(key.borrow())
    }

    /// 遍历全部条目。
    pub fn iter(&self) -> impl Iterator<Item = (&HeaderKey<A>, &HeaderVal<A>)> + '_ {
        self.map_.iter_()
    }

    /// 只遍历数字键，并还原成本端的 [`StdHeaderKey`]。
    pub fn iter_std(&self) -> impl Iterator<Item = (StdHeaderKey, &HeaderVal<A>)> + '_ {
        self.map_
            .iter_()
            .filter_map(|(key, val)| match key.as_u16_or_str() {
                U16OrStr::U16(code) => Option::Some((StdHeaderKey::new(*code), val)),
                U16OrStr::Str(_) => Option::None,
            })
    }

    /// `Body_Size` 头声明的长度；只有数字形态才是长度声明。
    pub fn body_size(&self) -> Option<usize> {
        self.try_get_val(StdHeaderKey::Body_Size)?
            .as_num()
            .map(usize::from)
    }

    /// `Data_Type_Id` 头声明的类型标识。
    pub fn data_type_id(&self) -> Option<u64> {
        self.try_get_val(StdHeaderKey::Data_Type_Id)?.as_u64()
    }

    /// 是否声明了 `Body_Transfer: Chunked`。
    ///
    /// 与 `abs_mptp` 的默认 [`body_info`](abs_mptp::messaging::TrMsgPrefix::body_info) 同一个
    /// 判据：只有数字 `0x20` 算分块；对象形态不是分块声明。
    pub fn is_chunked(&self) -> bool {
        let Option::Some(val) = self.try_get_val(StdHeaderKey::Body_Transfer) else {
            return false;
        };
        val.as_num()
            .map(StdHeaderVal::from)
            .is_some_and(|code| code == StdHeaderVal::Body_Transfer_Chunked)
    }
}

impl<A: Allocator + Clone> TrHeaders for Headers<A> {
    type Key = HeaderKey<A>;

    type Val = HeaderVal<A>;

    type Entry = HeaderEntry<A>;

    type EntriesIter = IntoIter<HeaderEntry<A>, A>;

    fn try_get_std_header(&self, key: StdHeaderKey) -> Option<&Self::Val> {
        self.try_get_val(key)
    }

    fn try_get_header<K>(&self, key: K) -> Option<&Self::Val>
    where
        K: Borrow<Self::Key>,
    {
        self.map_.get_(key.borrow())
    }

    fn into_entries(self) -> Self::EntriesIter {
        let alloc = Arc::allocator(&self.map_).clone();
        let mut entries = Vec::with_capacity_in(self.map_.len_(), alloc);
        for (key, val) in self.map_.iter_() {
            entries.push(HeaderEntry::from_kv(key.clone(), val.clone()));
        }
        entries.into_iter()
    }
}

impl<A: AllocatorClone> TrHeadersMut for Headers<A> {
    fn try_add_header<K>(
        mut self,
        key: K,
        factory: impl FnOnce(K) -> Self::Val,
    ) -> Result<Self::Entry, K>
    where
        K: Borrow<Self::Key>,
    {
        let key_ = key.borrow().clone();
        if self.map_.contains_key_(&key_) {
            return Result::Err(key);
        }
        let val_ = factory(key);
        Arc::make_mut(&mut self.map_).insert_(key_.clone(), val_.clone());
        Result::Ok(HeaderEntry::from_kv(key_, val_))
    }

    fn add_or_set_header<K, V>(mut self, key: K, val: V) -> Option<(Self::Key, Self::Val)>
    where
        K: Into<Self::Key>,
        V: Into<Self::Val>,
    {
        let key_ = key.into();
        Arc::make_mut(&mut self.map_)
            .insert_(key_.clone(), val.into())
            .map(|old| (key_, old))
    }

    fn remove_header(mut self, key: impl Borrow<Self::Key>) -> Option<(Self::Key, Self::Val)> {
        Arc::make_mut(&mut self.map_).remove_entry_(key.borrow())
    }
}

impl<A: Allocator> TrEncodable for Headers<A> {
    type EncodeAsync<'a, E>
        = EncodeHeadersAsync<'a, 'a, A, E>
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

/// [`Headers`] 编码的 step 函数：键交给 [`HeaderKey`]，值交给 [`HeaderVal`]。
#[gen_may_cancel_future(EncodeHeaders, pub, new(pub(crate)))]
async fn encode_headers_async_<'a, A, E, C>(
    headers: &'a Headers<A>,
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
    let mut map = encoder
        .encode_map(Option::Some(headers.map_.len_()))
        .may_cancel_with(cancel.child_token())
        .await?;
    for (key, val) in headers.map_.iter_() {
        map.encode_key(key)
            .may_cancel_with(cancel.child_token())
            .await?;
        map.encode_value(val)
            .may_cancel_with(cancel.child_token())
            .await?;
    }
    map.end().may_cancel_with(cancel).await
}

/// [`Headers`] 的访问者：遍历 MessagePack 映射，键交给 [`HeaderKey`]，值交给 [`HeaderVal`]。
struct HeadersVisitor<A: Allocator> {
    alloc_: A,
}

impl<'alloc, Alloc, A> Visitor<'alloc, Alloc> for HeadersVisitor<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type Value = Headers<A>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map with numeric or string header keys")
    }

    fn visit_map<'a, M>(
        self,
        map: M,
    ) -> impl TrMayCancel<'a, MayCancelOutput = Result<Self::Value, M::Error>>
    where
        'alloc: 'a,
        Self: 'a,
        Self::Value: 'a,
        M: MapAccess<'alloc, Alloc> + 'a,
        M::Error: 'a,
    {
        DecodeHeadersMapAsync::new(map, self.alloc_)
    }
}

/// [`Headers`] 解码的 step 函数：读出整张映射，再交给 [`Headers`]。
#[gen_may_cancel_future(DecodeHeadersMap, pub(crate), new(pub(crate)))]
async fn decode_headers_map_async_<'alloc, 'a, Alloc, A, M, C>(
    mut map: M,
    alloc: A,
    cancel: C,
) -> Result<Headers<A>, M::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    M: MapAccess<'alloc, Alloc> + 'a,
    M::Error: 'a,
    C: TrCancellationToken,
{
    let mut values = HeaderMap::new_in_(alloc.clone());
    while let Option::Some((key, val)) = map
        .next_entry::<HeaderKey<A>, HeaderVal<A>>()
        .may_cancel_with(cancel.child_token())
        .await?
    {
        values.insert_(key, val);
    }
    Result::Ok(Headers {
        map_: Arc::new_in(values, alloc),
    })
}

impl<'alloc, Alloc, A> TrDecodable<'alloc, Alloc> for Headers<A>
where
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc,
{
    type DecodeAsync<'a, D>
        = DecodeHeadersAsync<'alloc, 'a, 'a, Alloc, A, D>
    where
        'alloc: 'a,
        D: TrDecoder<'alloc, Alloc> + 'a,
        D::Error: 'a,
        Self: 'a;

    fn decode_async<'a, D>(decoder: D) -> Self::DecodeAsync<'a, D>
    where
        'alloc: 'a,
        D: TrDecoder<'alloc, Alloc> + 'a,
        D::Error: 'a,
        Self: 'a,
    {
        let alloc = A::alloc_from(decoder.allocator());
        DecodeHeadersAsync::new(decoder, alloc)
    }
}

/// [`Headers`] 解码的 step 函数：交给 `decode_map`，由 [`HeadersVisitor`] 遍历。
#[gen_may_cancel_future(DecodeHeaders, pub, new(pub(crate)))]
async fn decode_headers_async_<'alloc, 'a, Alloc, A, D, C>(
    decoder: D,
    alloc: A,
    cancel: C,
) -> Result<Headers<A>, D::Error>
where
    'alloc: 'a,
    Alloc: TrArena<'alloc> + 'alloc,
    A: AllocatorClone + TrAllocSource<'alloc, Alloc> + 'alloc + 'a,
    D: TrDecoder<'alloc, Alloc> + 'a,
    D::Error: 'a,
    C: TrCancellationToken,
{
    decoder
        .decode_map(HeadersVisitor { alloc_: alloc })
        .may_cancel_with(cancel)
        .await
}

#[cfg(test)]
mod tests_ {
    //! 头表的纯本地点判定：取值、增删与两个 `abs_mptp` 契约。
    //!
    //! Local tests for the header map: accessors, mutation and the two `abs_mptp`
    //! contracts.

    use alloc::alloc::Global;

    use super::*;

    /// 造一个标准数字键。
    fn std_key_(key: StdHeaderKey) -> HeaderKey<Global> {
        HeaderKey::from_num(u16::from(key))
    }

    /// 测试标准头访问器把数字与体声明各归各位。
    /// Test that the standard-header accessors map numeric and body declarations.
    ///
    /// - 手段：写入 `Body_Size: 12`、`Body_Type: 1`，分别调用 `body_size` /
    ///   `try_get_val` / `is_chunked`。
    /// - 判断：长度是 12，`Body_Type` 是数字 1，且没有分块声明。
    ///
    /// - Means: store `Body_Size: 12` and `Body_Type: 1`, then call `body_size` /
    ///   `try_get_val` / `is_chunked`.
    /// - Verdict: the size is 12, `Body_Type` is numeric 1, and there is no chunked
    ///   declaration.
    #[test]
    fn header_accessors_map_values_() {
        let mut headers = Headers::new(Global);
        headers.set_num(StdHeaderKey::Body_Size, 12);
        headers.set_num(StdHeaderKey::Body_Type, 1u16);

        assert_eq!(headers.body_size(), Option::Some(12usize));
        assert_eq!(
            headers.try_get_val(StdHeaderKey::Body_Type),
            Option::Some(&HeaderVal::from_num(1u16))
        );
        assert!(!headers.is_chunked());
    }

    /// 测试分块声明与长度声明可分别读出。
    /// Test that the chunked and sized declarations are independent.
    ///
    /// - 手段：写数字 `0x20` 的 `Body_Transfer`，再写一张同时带 `Body_Size` 的表。
    /// - 判断：分块表 `is_chunked` 为真且 `body_size` 为 `None`；同时带长度时两者都成立。
    ///
    /// - Means: store numeric `0x20` for `Body_Transfer`, then a map that also carries
    ///   `Body_Size`.
    /// - Verdict: the chunked map reports chunked with no size; adding a size keeps both
    ///   declarations readable.
    #[test]
    fn chunked_and_sized_are_independent_() {
        let mut chunked = Headers::new(Global);
        chunked.set_num(
            StdHeaderKey::Body_Transfer,
            StdHeaderVal::Body_Transfer_Chunked.into_inner(),
        );
        assert!(chunked.is_chunked());
        assert_eq!(chunked.body_size(), Option::None);

        let mut both = chunked.clone();
        both.set_num(StdHeaderKey::Body_Size, 8);
        assert!(both.is_chunked());
        assert_eq!(both.body_size(), Option::Some(8usize));
    }

    /// 测试自定义数字键与标准键共存，且删除返回旧值。
    /// Test that custom numeric keys coexist with standard keys and removal returns the old
    /// value.
    ///
    /// - 手段：用 `StdHeaderKey::new(0x01)` 写一条自定义头，再删除它。
    /// - 判断：`try_get_val` 读得到，删除返回原值，表随之为空。
    ///
    /// - Means: write a custom header under `StdHeaderKey::new(0x01)` and remove it.
    /// - Verdict: the lookup finds it, removal hands the old value back, and the map is empty
    ///   afterwards.
    #[test]
    fn custom_numeric_key_roundtrips_() {
        let key = StdHeaderKey::new(0x01);
        let mut headers = Headers::new(Global);
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
        let key = HeaderKey::from_text("x-custom", Global);
        let mut headers = Headers::new(Global);
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

    /// 测试对象形态的头值保留 `type_id` 与字节流。
    /// Test that an object header value keeps its `type_id` and byte stream.
    ///
    /// - 手段：用 `HeaderVal::from_bytes` 写入 `type_id = 0x1234` 与 `b"msgpack"`，再读回。
    /// - 判断：`as_num` 为 `None`，`type_id` 与 `data` 与写入值一致。
    ///
    /// - Means: store `type_id = 0x1234` with `b"msgpack"` through `HeaderVal::from_bytes`,
    ///   then read it back.
    /// - Verdict: `as_num` is `None`, while `type_id` and `data` match the inputs.
    #[test]
    fn obj_value_roundtrips_() {
        let mut headers = Headers::new(Global);
        headers.set(
            HeaderKey::from(StdHeaderKey::Cookie),
            HeaderVal::from_bytes(0x1234, b"msgpack", Global),
        );

        let value = headers
            .try_get_val(StdHeaderKey::Cookie)
            .expect("对象头应当存在");
        assert_eq!(value.as_num(), Option::None);
        assert_eq!(value.type_id(), Option::Some(0x1234));
        assert_eq!(value.data(), Option::Some(&b"msgpack"[..]));
    }

    /// 测试 `TrHeaders` 的按键查询与全量遍历都成立。
    /// Test that `TrHeaders` lookups and full iteration both work.
    ///
    /// - 手段：写入一个数字头与一个对象头，先 `try_get_header`，再 `into_entries`。
    /// - 判断：两次查询命中写入的值；遍历得到两条，且每条对键与值都能 `Borrow`。
    ///
    /// - Means: store one numeric and one object header, look them up through
    ///   `try_get_header`, then walk everything through `into_entries`.
    /// - Verdict: both lookups hit, the walk yields two entries, and each entry borrows as
    ///   both key and value.
    #[test]
    fn tr_headers_answers_lookups_and_entries_() {
        let mut headers = Headers::new(Global);
        headers.set_num(StdHeaderKey::Body_Type, 1u16);
        headers.set(
            HeaderKey::from(StdHeaderKey::Cookie),
            HeaderVal::from_bytes(0xabc, b"v", Global),
        );

        assert_eq!(
            headers.try_get_header(std_key_(StdHeaderKey::Body_Type)),
            Option::Some(&HeaderVal::from_num(1u16))
        );

        let all: Vec<HeaderEntry<Global>> = headers.into_entries().collect();
        assert_eq!(all.len(), 2usize);
        let borrowed_key: &HeaderKey<Global> = all[0].borrow();
        let borrowed_val: &HeaderVal<Global> = all[0].borrow();
        assert!(matches!(
            borrowed_key.as_u16_or_str(),
            U16OrStr::U16(code)
                if *code == u16::from(StdHeaderKey::Body_Type)
                    || *code == u16::from(StdHeaderKey::Cookie)
        ));
        assert!(borrowed_val.as_num().is_some() || borrowed_val.type_id().is_some());
    }

    /// 测试 `TrHeadersMut` 在空表与非空表上的回报符合契约。
    /// Test that `TrHeadersMut` reports what the contract says on empty and filled maps.
    ///
    /// - 手段：在空表上 `try_add_header`、`remove_header`、`add_or_set_header`；再在已有键上
    ///   重复添加一次。
    /// - 判断：空表的添加成功、删除与覆盖式加都返回 `None`；重复添加返回 `Err` 并交回冲突
    ///   键，且表长不变。
    ///
    /// - Means: call `try_add_header`, `remove_header` and `add_or_set_header` on an empty
    ///   map, then try to add an existing key again.
    /// - Verdict: the first add succeeds, removal and upsert report `None`, and the duplicate
    ///   add returns `Err` with the conflicting key while the length stays put.
    #[test]
    fn tr_headers_mut_reports_contract_() {
        let accept = std_key_(StdHeaderKey::Accept);
        let headers = Headers::new(Global);

        let added = headers
            .clone()
            .try_add_header(accept.clone(), |_| HeaderVal::from_num(1));
        assert!(added.is_ok(), "空表上添加应当成功");
        assert_eq!(headers.clone().remove_header(accept.clone()), Option::None);
        assert_eq!(
            headers
                .clone()
                .add_or_set_header(accept.clone(), HeaderVal::from_num(2)),
            Option::None
        );

        let mut non_empty = Headers::new(Global);
        non_empty.set_num(StdHeaderKey::Accept, 1);
        let duplicate = non_empty
            .clone()
            .try_add_header(accept.clone(), |_| HeaderVal::from_num(2));
        assert!(
            matches!(duplicate, Result::Err(key) if key == accept),
            "已有键上重复添加必须交回冲突键"
        );
        assert_eq!(non_empty.len(), 1usize);
    }
}
