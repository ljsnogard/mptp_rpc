//! 标准头：两端都必须认识的键与值，以及头表本身的契约。
//!
//! 键值按区段划分，用户自定义与协议保留互不干扰：
//!
//! - `0x00`–`0x9F`：用户自定义；
//! - `0xA0`–`0xAF`：两端通用的标准头（`Body_Size`、`Body_Transfer`……）；
//! - `0xB0`–`0xCF`：客户端常用的标准请求头；
//! - `0xD0`–`0xFF`：服务端常用的标准响应头。
//!
//! # `std_header_`: standard headers
//!
//! The keys and values both ends must understand, plus the contract of the header map.
//! Ranges keep user extensions and protocol reservations apart: `0x00`–`0x9F` is
//! user-defined, `0xA0`–`0xAF` is the shared standard set, `0xB0`–`0xCF` holds client
//! request headers and `0xD0`–`0xFF` holds server response headers.

use core::borrow::Borrow;

use crate::x_deps::funty;

/// MPTP 的头表。
///
/// 它是**值语义**的：查找只借出条目，`into_entries` 交出全部条目，因此实现者可以是一个
/// 便宜的句柄（例如 `Arc<...>` 的包装），不必在这里做深拷贝。
///
/// The MPTP header map. It is value-semantic: lookups lend an entry and `into_entries`
/// hands over all entries, so an implementation may be a cheap handle rather than an
/// owned map.
pub trait TrHeaders {
    /// 头的键类型。
    ///
    /// 它可以是数字键、字符串键或自定义键；按标准头查询走
    /// [`TrHeaders::try_get_std_header`]，不要求键类型能由 [`StdHeaderKey`] 构造。
    ///
    /// The header key type. It may be numeric, string-based, or otherwise user-defined;
    /// standard-header lookups go through [`TrHeaders::try_get_std_header`], so the key
    /// type need not be constructible from [`StdHeaderKey`].
    type Key: TrNumOrStr<NumType: Into<StdHeaderKey>>;

    /// 头的值类型。
    ///
    /// 简单标准头的值是数字；复杂值用 [`TrTypedEnvelope`] 承载 MessagePack 编码的字节流。
    ///
    /// The header value type. Simple standard-header values are numbers; complex values
    /// travel as a MessagePack-encoded byte stream inside a [`TrTypedEnvelope`].
    type Val: TrNumOrObj<NumType: Into<StdHeaderVal>>;

    /// 一次查询 / 遍历交出的条目类型：它同时能借出键与值。
    ///
    /// The entry type handed out per lookup or iteration; it must borrow both key and
    /// value.
    type Entry: Borrow<Self::Key> + Borrow<Self::Val>;

    /// 全部条目的迭代器。
    ///
    /// The iterator over all entries.
    type EntriesIter: IntoIterator<Item = Self::Entry>;

    /// 按标准头键查询；键不存在时返回 `None`。
    ///
    /// 标准头属于协议保留键，与用户自定义键共存于同一张表。该查询独立于
    /// [`TrHeaders::try_get_header`]，因为后者的 `Key` 类型不一定能表达标准键。
    /// 实现者只需把 [`StdHeaderKey`] 映射到自己的键空间；无法表达标准键的实现
    /// 返回 `None`。
    ///
    /// Look a standard header value up; `None` when absent. Standard headers are protocol
    /// reservations that coexist with user-defined keys. This lookup is separate from
    /// [`TrHeaders::try_get_header`] because the `Key` type need not be able to express a
    /// standard key. Implementations map [`StdHeaderKey`] into their key space; those that
    /// cannot express standard keys return `None`.
    fn try_get_std_header(&self, key: StdHeaderKey) -> Option<&Self::Val>;

    /// 按键取一个值；键不存在时返回 `None`。
    ///
    /// Look one value up by key; `None` when the key is absent.
    fn try_get_header<K>(&self, key: K) -> Option<&Self::Val>
    where
        K: Borrow<Self::Key>;

    /// 交出全部条目。
    ///
    /// Hand out all entries.
    fn into_entries(self) -> Self::EntriesIter;
}

/// 可变的 MPTP 头表：在 [`TrHeaders`] 之上补出增、改、删。
///
/// A mutable MPTP header map: [`TrHeaders`] plus insert / replace / remove.
pub trait TrHeadersMut
where
    Self: TrHeaders,
{
    /// 如果没有键冲突，返回 Ok 以及成功添加的值，否则返回 Err 和冲突键对应的值
    ///
    /// Insert without overwriting: `Ok` carries the stored entry, `Err` carries the key
    /// that was already present.
    fn try_add_header<K>(
        self,
        key: K,
        factory: impl FnOnce(K) -> Self::Val,
    ) -> Result<Self::Entry, K>
    where
        K: Borrow<Self::Key>;

    /// 添加或者替换键值对，返回被替代的 Key 和 val （如果存在）
    ///
    /// Insert or replace, returning the replaced key/value pair when there was one.
    fn add_or_set_header<K, V>(self, key: K, val: V) -> Option<(Self::Key, Self::Val)>
    where
        K: Into<Self::Key>,
        V: Into<Self::Val>;

    /// 删除一个键，返回被删掉的键值对（如果存在）。
    ///
    /// Remove a key, returning the removed pair when there was one.
    fn remove_header(self, key: impl Borrow<Self::Key>) -> Option<(Self::Key, Self::Val)>;
}

/// 两端都必须认识的标准头键。
///
/// 取值区段见模块文档；`0x00`–`0x9F` 留给用户自定义，因此这里列出的键可以与用户自定义
/// 键共存而不冲突。
///
/// The standard header keys both ends must understand. See the module docs for the key
/// ranges; `0x00`–`0x9F` is reserved for user extensions, so these keys never collide
/// with them.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StdHeaderKey(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl StdHeaderKey {
    //-------------------------------------------------------------------------
    // 两端端常用的标准响应头 0x00-0x9F 是用户自定义的，0xA0 后是协议保留的
    //-------------------------------------------------------------------------

    /// 保留
    pub const Reserved    : StdHeaderKey = StdHeaderKey::new(0xA0);

    /// 请求体或者回复体中的数据长度，值类型应该是能转成数字的字符串
    pub const Body_Size   : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x01);

    /// 请求体或者回复体中的数据 MIME 类型
    pub const Body_Type   : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x02);

    /// 进一步指明数据期待的反序列化类型，值类型应该是一个字符串，常用 UUID 或者 Hex
    pub const Data_Type_Id: StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x03);

    /// 指明本报文创建的日期和时间，值类型应该是关于日期的字符串
    pub const Created_At  : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x04);

    /// 报文体的**传输方式**。取值见 [`StdHeaderVal::Body_Transfer_Chunked`]。
    ///
    /// 它与 `Body_Size` 是**互斥**的两种声明，接收方靠「哪个头在场」唯一地判断体是
    /// 定长还是分块，不靠猜：
    ///
    /// - `Body_Size` 在场 = 定长，体恰好那么多字节；
    /// - 本头取 `Chunked` = 分块，总长不写在头里，由块自带的长度前缀给出；
    /// - 两者都不在场 = 本条报文没有体；
    /// - 两者同时在场 = 协议违规（无法判定该按哪种方式切分后续字节）。
    pub const Body_Transfer: StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x05);

    //-------------------------------------------------------------------------
    // 客户端常用的标准请求头 0xB0 - 0xCF 保留了 32 个位置供扩展
    //-------------------------------------------------------------------------

    /// 标识客户端的应用程序类型、操作系统、浏览器版本等信息。
    pub const User_Agent    : StdHeaderKey = StdHeaderKey::new(0xB0);

    /// 告知服务器客户端能够处理的内容类型（MIME 类型），例如 application/json。
    pub const Accept        : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x01);

    /// 携带身份认证凭证（如 JWT Token、Basic 认证）
    pub const Authorization : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x02);

    /// 携带服务器之前通过 Set-Cookie 指令存储在客户端的状态信息
    pub const Cookie        : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x03);

    pub const Forwarded     : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x04);

    //-------------------------------------------------------------------------
    // 服务端常用的标准响应头，0xD0 - 0xFF 保留了 48 个位置供扩展
    //-------------------------------------------------------------------------

    /// 标识服务器软件的名称和版本信息
    pub const Server       : StdHeaderKey = StdHeaderKey::new(0xD0);

    /// 用于重定向，告知客户端新的资源地址，值类型应该是路径或者 URI
    pub const Location     : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x01);

    /// 指明所请求资源的最后修改时间，值类型应该是关于日期的字符串
    pub const Last_Modified: StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x02);

    /// 服务器通过此字段向客户端设置 Cookie
    pub const Set_Cookie   : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x03);

    /// 资源的唯一标识符（实体标签），用于缓存验证
    pub const ETag         : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x04);

    /// 控制缓存行为（如缓存时长、是否可缓存），如：`Cache-Control: max-age=3600`
    pub const Cache_Ctrl   : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x05);

    /// 指明响应内容的过期日期和时间，如：`Expires: Wed, 21 Oct 2023 07:28:00 GMT`
    pub const Expires      : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x06);
}

impl StdHeaderKey {
    /// 按数值构造一个头键；使用自定义键时走这里。
    ///
    /// Build a header key from its numeric code; custom keys go through here.
    pub const fn new(code: u16) -> Self {
        StdHeaderKey(code)
    }

    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl From<u16> for StdHeaderKey {
    fn from(value: u16) -> Self {
        StdHeaderKey::new(value)
    }
}

impl From<StdHeaderKey> for u16 {
    fn from(value: StdHeaderKey) -> Self {
        value.0
    }
}

/// 标准头的值：只在协议自己定义的那几个头上使用。
///
/// 大多数头的值都是自由文本（资源位置、MIME 类型、日期……），只有协议赋予固定含义的
/// 少数取值才需要这里的枚举值——例如 [`StdHeaderVal::Body_Transfer_Chunked`]。
///
/// The standard header values, used only on the headers whose meaning the protocol fixes.
/// Most headers carry free text; only a few take one of these enumerated values, such as
/// [`StdHeaderVal::Body_Transfer_Chunked`].
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StdHeaderVal(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl StdHeaderVal {

    /// `Body_Type` 的标准取值：体是 MessagePack。
    ///
    /// The standard `Body_Type` value: the body is MessagePack.
    pub const Mime_Body_Type_MsgPack: StdHeaderVal = StdHeaderVal(0x01);

    /// `Body_Type` 的标准取值：体是 JSON。
    ///
    /// The standard `Body_Type` value: the body is JSON.
    pub const Mime_Body_Type_Json   : StdHeaderVal = StdHeaderVal(0x02);

    /// 表明客户端的主要目的是调用 RPC
    ///
    /// Signals that the client's main purpose is invoking RPC.
    pub const User_Rpc_Client       : StdHeaderVal = StdHeaderVal(0x10);

    /// 表明客户端的主要目的是代理
    ///
    /// Signals that the client's main purpose is proxying.
    pub const User_Proxy_Client     : StdHeaderVal = StdHeaderVal(0x11);

    /// `Body_Transfer` 的标准取值：报文体按**分块**方式传输。
    ///
    /// 每个块自带 2 字节大端长度前缀，以长度 `0` 的块结束；总长不写在 `Body_Size` 里。
    ///
    /// The standard `Body_Transfer` value: the body is transferred in **chunks**. Each
    /// chunk carries its own 2-byte big-endian length prefix and a zero-length chunk ends
    /// the body; the total length is not declared in `Body_Size`.
    pub const Body_Transfer_Chunked  : StdHeaderVal = StdHeaderVal(0x20);
}

impl StdHeaderVal {
    /// 取出数值形态。
    ///
    /// The numeric representation.
    pub const fn into_inner(self) -> u16 {
        self.0
    }
}

impl From<u16> for StdHeaderVal {
    fn from(value: u16) -> Self {
        StdHeaderVal(value)
    }
}

/// 头键的「数字或字符串」表示。
///
/// 键保留字符串形态：用户自定义的键仍然可以写成文本；值则改用 [`TrNumOrObj`]。
///
/// # Examples
///
/// ```
/// use core::borrow::Borrow;
///
/// use abs_mptp::spec::{NumOrStr, StdHeaderKey, TrNumOrStr};
///
/// let key = StdHeaderKey::Body_Size;
/// assert!(matches!(key.try_match_num(), Ok(code) if code == 0xA1));
/// assert!(key.try_match_str().is_err());
/// ```
///
/// The numeric-or-string representation of a header key. Keys may still be textual;
/// values use [`TrNumOrObj`] instead.
pub trait TrNumOrStr {
    /// 数字形态的标量类型。
    type NumType: funty::Unsigned;

    /// 字符串形态的类型。
    type StrType: Borrow<str>;

    /// 转成两种形态的枚举。
    fn into(self) -> NumOrStr<Self::NumType, Self::StrType>;

    /// 按数字取用；字符串形态原样返回。
    fn try_match_num(self) -> Result<Self::NumType, Self::StrType>;

    /// 按字符串取用；数字形态原样返回。
    fn try_match_str(self) -> Result<Self::StrType, Self::NumType>;
}

/// 头值的「数字或对象」表示。
///
/// 简单的标准头取值（`Body_Size`、`Body_Transfer`……）用数字；复杂取值（例如 Cookie）
/// 不再写成文本，而是按 MessagePack 编码成字节流，并随字节流携带逻辑类型的 `type_id`。
/// 后者由 [`TrTypedEnvelope`] 描述，`type_id` 取 `abs_denco` 的 `TrHasTypeId` 生成值。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::{NumOrObj, ObjEnvelope, TrNumOrObj, TrTypedEnvelope};
///
/// let num: NumOrObj<u16, &[u8]> = NumOrObj::Num(7);
/// assert_eq!(num.as_num_or_obj().try_match_num().ok(), Some(7u16));
///
/// let obj: NumOrObj<u16, &[u8]> = NumOrObj::Obj(ObjEnvelope::new(0x1234, &[0xC0]));
/// assert_eq!(obj.as_num_or_obj().try_match_obj().ok().map(|e| e.type_id()), Some(0x1234));
/// ```
///
/// The numeric-or-object representation of a header value. Simple standard-header values
/// are numbers; complex values are MessagePack byte streams tagged with a logical
/// `type_id`.
pub trait TrNumOrObj {
    /// 数字形态的标量类型。
    type NumType: funty::Unsigned;

    /// 对象形态的封装类型。
    type ObjType: TrTypedEnvelope;

    /// 借成 `NumOrObj`；对象形态只借出其字节流，不复制存储。
    fn as_num_or_obj(&self) -> NumOrObj<Self::NumType, &[u8]>;

    /// 按数字取用；对象形态原样返回。
    fn try_match_num(self) -> Result<Self::NumType, Self::ObjType>;

    /// 按对象取用；数字形态原样返回。
    fn try_match_obj(self) -> Result<Self::ObjType, Self::NumType>;
}

/// 一段带逻辑类型标识的 MessagePack 字节流。
///
/// `type_id` 是 `abs_denco` 的 `TrHasTypeId` 生成的 `u32` 指纹，`data` 是该类型的
/// MessagePack 编码结果。头表把它们写成一个两元素数组 `[type_id, bin]`；本 trait 只描述
/// 解出来的逻辑形状，不解析 MessagePack 本身。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::{ObjEnvelope, TrTypedEnvelope};
///
/// let envelope = ObjEnvelope::new(42, &[0x81, 0xA1, 0x78, 0x01][..]);
/// assert_eq!(envelope.type_id(), 42);
/// assert_eq!(envelope.data(), &[0x81, 0xA1, 0x78, 0x01][..]);
/// ```
///
/// A MessagePack byte stream tagged with a logical type identifier.
pub trait TrTypedEnvelope {
    /// 逻辑类型的 `u32` 指纹。
    fn type_id(&self) -> u32;

    /// MessagePack 编码后的字节流。
    fn data(&self) -> &[u8];
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NumOrStr<N, S>
where
    N: funty::Unsigned,
    S: Borrow<str>,
{
    Num(N),
    Str(S),
}

impl TrNumOrStr for StdHeaderKey {
    type NumType = u16;
    type StrType = &'static str;

    fn into(self) -> NumOrStr<Self::NumType, Self::StrType> {
        NumOrStr::Num(self.0)
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        Result::Ok(self.0)
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        Result::Err(self.0)
    }
}

impl TrNumOrStr for StdHeaderVal {
    type NumType = u16;
    type StrType = &'static str;

    fn into(self) -> NumOrStr<Self::NumType, Self::StrType> {
        NumOrStr::Num(self.0)
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        Result::Ok(self.0)
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        Result::Err(self.0)
    }
}

impl<N, S> TrNumOrStr for NumOrStr<N, S>
where
    N: funty::Unsigned,
    S: Borrow<str>,
{
    type NumType = N;
    type StrType = S;

    fn into(self) -> NumOrStr<Self::NumType, Self::StrType> {
        self
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self {
            Self::Num(n) => Result::Ok(n),
            Self::Str(s) => Result::Err(s),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self {
            Self::Str(s) => Result::Ok(s),
            Self::Num(n) => Result::Err(n),
        }
    }
}

/// 头值的两种形态：数字，或带类型标识的 MessagePack 字节流。
///
/// `B` 是字节流的存储形态（`&[u8]` / `Vec<u8>` / 带分配器的容器均可），因此同一枚举既
/// 能表示借用的解码结果，也能表示拥有的构造结果。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::{NumOrObj, ObjEnvelope};
///
/// let numeric: NumOrObj<u16, &[u8]> = NumOrObj::Num(0x20);
/// let object: NumOrObj<u16, &[u8]> =
///     NumOrObj::Obj(ObjEnvelope::new(7, &[0xC0][..]));
///
/// assert!(matches!(numeric, NumOrObj::Num(0x20)));
/// assert!(matches!(object, NumOrObj::Obj(_)));
/// ```
///
/// The two header-value forms: a number, or a type-tagged MessagePack byte stream.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NumOrObj<N, B>
where
    N: funty::Unsigned,
    B: Borrow<[u8]>,
{
    /// 数字形态。
    Num(N),

    /// 对象形态。
    Obj(ObjEnvelope<B>),
}

impl<N, B> TrNumOrObj for NumOrObj<N, B>
where
    N: funty::Unsigned,
    B: Borrow<[u8]>,
{
    type NumType = N;
    type ObjType = ObjEnvelope<B>;

    fn as_num_or_obj(&self) -> NumOrObj<Self::NumType, &[u8]> {
        match self {
            NumOrObj::Num(n) => NumOrObj::Num(*n),
            NumOrObj::Obj(o) => NumOrObj::Obj(o.as_ref()),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::ObjType> {
        match self {
            NumOrObj::Num(n) => Result::Ok(n),
            NumOrObj::Obj(e) => Result::Err(e),
        }
    }

    fn try_match_obj(self) -> Result<Self::ObjType, Self::NumType> {
        match self {
            NumOrObj::Obj(o) => Result::Ok(o),
            NumOrObj::Num(n) => Result::Err(n),
        }
    }
}

/// 一个带逻辑类型标识的 MessagePack 字节流。
///
/// 它是 [`NumOrObj::Obj`] 的载荷，只保存 `type_id` 与原始字节；MessagePack 的解释由拿到
/// 它的上层（知道 `type_id` 对应哪个 Rust 类型的一方）完成。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::{ObjEnvelope, TrTypedEnvelope};
///
/// let envelope = ObjEnvelope::new(9, &[0x01][..]);
/// let borrowed: ObjEnvelope<&[u8]> = envelope.as_ref();
/// assert_eq!(borrowed.type_id(), 9);
/// assert_eq!(borrowed.data(), &[0x01][..]);
/// ```
///
/// A MessagePack byte stream tagged with a logical type identifier.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ObjEnvelope<B>
where
    B: Borrow<[u8]>,
{
    /// 逻辑类型的 `u32` 指纹（`abs_denco` 的 `TrHasTypeId` 生成）。
    type_: u32,

    /// MessagePack 编码后的字节流。
    data_: B,
}

impl<B> ObjEnvelope<B>
where
    B: Borrow<[u8]>,
{
    /// 由 `type_id` 与 MessagePack 字节流构造。
    ///
    /// Build an envelope from a logical `type_id` and a MessagePack byte stream.
    pub const fn new(type_id: u32, data: B) -> Self {
        ObjEnvelope {
            type_: type_id,
            data_: data,
        }
    }

    /// 借用同一段字节流，产生 [`ObjEnvelope<&[u8]>`]。
    ///
    /// Borrow the same byte stream as an [`ObjEnvelope<&[u8]>`].
    pub fn as_ref(&self) -> ObjEnvelope<&[u8]> {
        ObjEnvelope {
            type_: self.type_,
            data_: self.data_.borrow(),
        }
    }

    /// 拆出 `type_id` 与字节流存储。
    ///
    /// Split the envelope into its logical `type_id` and byte-stream storage.
    pub fn into_parts(self) -> (u32, B) {
        (self.type_, self.data_)
    }

    /// 逻辑类型的 `u32` 指纹。
    ///
    /// The logical `u32` type fingerprint.
    pub const fn type_id(&self) -> u32 {
        self.type_
    }

    /// MessagePack 编码后的字节流。
    ///
    /// The MessagePack-encoded byte stream.
    pub fn data(&self) -> &[u8] {
        self.data_.borrow()
    }
}

impl<B> TrTypedEnvelope for ObjEnvelope<B>
where
    B: Borrow<[u8]>,
{
    fn type_id(&self) -> u32 {
        self.type_
    }

    fn data(&self) -> &[u8] {
        self.data_.borrow()
    }
}

#[cfg(test)]
mod tests_ {
    //! 头值「数字或对象」表示的本地单元测试。
    //!
    //! Local unit tests for the numeric-or-object header-value representation.

    use super::*;

    /// 测试 `NumOrObj` 的两种形态分别匹配。
    /// Test that `NumOrObj` matches each of its two forms on its own.
    ///
    /// - 手段：分别构造数字形态与对象形态，调用 `try_match_num` / `try_match_obj`。
    /// - 判断：数字形态的 `try_match_num` 返回 `Ok(0x20)` 且 `try_match_obj` 返回 `Err`；
    ///   对象形态相反，并保留 `type_id` 与字节流。
    ///
    /// - Means: build one numeric and one object value, then call `try_match_num` /
    ///   `try_match_obj`.
    /// - Verdict: the numeric form yields `Ok(0x20)` from `try_match_num` and `Err` from
    ///   `try_match_obj`; the object form does the opposite and keeps its `type_id` and
    ///   bytes.
    #[test]
    fn num_or_obj_matches_each_form_() {
        let num: NumOrObj<u16, &[u8]> = NumOrObj::Num(0x20);
        assert_eq!(num.try_match_num().ok(), Option::Some(0x20));
        assert!(num.try_match_obj().is_err());

        let obj: NumOrObj<u16, &[u8]> = NumOrObj::Obj(ObjEnvelope::new(7, &[0xC0]));
        let envelope = obj
            .try_match_obj()
            .expect("对象形态应当匹配到 ObjEnvelope");
        assert_eq!(envelope.type_id(), 7);
        assert_eq!(envelope.data(), &[0xC0]);
        assert!(obj.try_match_num().is_err());
    }

    /// 测试 `as_num_or_obj` 借出对象字节流而不是复制。
    /// Test that `as_num_or_obj` lends the object bytes instead of copying them.
    ///
    /// - 手段：用栈上数组构造对象形态，调用 `as_num_or_obj` 并解构。
    /// - 判断：借出的 `type_id` 与 `data` 与构造时一致，`data` 指向同一段字节。
    ///
    /// - Means: build the object form from a stack array and destructure the result of
    ///   `as_num_or_obj`.
    /// - Verdict: the borrowed `type_id` and `data` match the constructor inputs, and `data`
    ///   points at the same byte sequence.
    #[test]
    fn as_num_or_obj_borrows_obj_data_() {
        let bytes = [0x81, 0xA1, 0x78, 0x01];
        let obj: NumOrObj<u16, &[u8]> = NumOrObj::Obj(ObjEnvelope::new(42, &bytes[..]));
        match obj.as_num_or_obj() {
            NumOrObj::Obj(envelope) => {
                assert_eq!(envelope.type_id(), 42);
                assert_eq!(envelope.data(), &bytes[..]);
            }
            NumOrObj::Num(_) => panic!("对象形态不应借成数字"),
        }
    }

    /// 测试 `ObjEnvelope` 的构造、借用视图与拆分。
    /// Test `ObjEnvelope` construction, borrowed view and splitting.
    ///
    /// - 手段：构造后先取 `as_ref`，再把原值 `into_parts`。
    /// - 判断：借用视图与原值的 `type_id` / `data` 一致；拆分得到同一个 `type_id` 与
    ///   字节流。
    ///
    /// - Means: construct an envelope, take `as_ref`, then consume the original with
    ///   `into_parts`.
    /// - Verdict: the borrowed view reports the same `type_id` / `data`; splitting yields
    ///   the same `type_id` and byte stream.
    #[test]
    fn obj_envelope_borrows_and_splits_() {
        let envelope = ObjEnvelope::new(5, &[1, 2][..]);
        let borrowed = envelope.as_ref();
        assert_eq!(borrowed.type_id(), 5);
        assert_eq!(borrowed.data(), &[1, 2][..]);

        let (type_id, data) = envelope.into_parts();
        assert_eq!(type_id, 5);
        assert_eq!(data, &[1, 2][..]);
    }
}
