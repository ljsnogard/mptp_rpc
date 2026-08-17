use core::{borrow::Borrow, cmp::Ordering, fmt::Debug, iter::IntoIterator};
use std::collections::btree_map::{self, BTreeMap};

use serde::{Deserialize, Serialize};

use abs_buff::x_deps::funty;
use buffex::x_deps::abs_buff;

type HeaderStrType = String;

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// StrOrU16
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[derive(Clone, Debug, Deserialize, Serialize)]
enum StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
    Str(S),
    Num(N),
}

impl<S, N> StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
    /// 取出其中的字符串形态（`StrOrNum::Str`）。
    ///
    /// 意图：客户端在读取回复体时，需要把 `Body_Size` 之类的数字型头值
    /// 取出来解析；`StrOrNum` 的两种形态（字符串 / 数字）都要有访问途径，
    /// 否则 `HeaderVal` 内部对调用方完全不透明，无法据此决定回复体长度。
    pub fn try_as_str(&self) -> Result<&str, N> {
        match self {
            StrOrNum::Str(s) => Result::Ok(s.borrow()),
            StrOrNum::Num(n) => Result::Err(*n),
        }
    }

    /// 取出其中的数字形态（`StrOrNum::Num`）。
    pub fn try_as_u16(&self) -> Result<N, &str> {
        match &self {
            StrOrNum::Num(n) => Result::Ok(*n),
            StrOrNum::Str(s) => Result::Err(s.borrow()),
        }
    }
}

impl<S, N> PartialEq for StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
    fn eq(&self, other: &Self) -> bool {
        match (&self, &other) {
            (StrOrNum::Num(this), StrOrNum::Num(that)) => N::eq(this, that),
            (StrOrNum::Str(this), StrOrNum::Str(that)) => str::eq(this.borrow(), that.borrow()),
            _ => false,
        }
    }
}

impl<S, N> Eq for StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
}

impl<S, N> PartialOrd for StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl<S, N> Ord for StrOrNum<S, N>
where
    S: Borrow<str> + Clone + Debug,
    N: funty::Unsigned,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self, &other) {
            (StrOrNum::Num(this), StrOrNum::Num(that)) => this.cmp(that),
            (StrOrNum::Str(this), StrOrNum::Str(that)) => str::cmp(this.borrow(), that.borrow()),
            (StrOrNum::Num(_), StrOrNum::Str(_)) => Ordering::Less,
            (StrOrNum::Str(_), StrOrNum::Num(_)) => Ordering::Greater,
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// StdHeaderKey
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// The standard key in header that both client and the server should
/// understand.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct StdHeaderKey(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl StdHeaderKey {
    //-------------------------------------------------------------------------
    // 两端端常用的标准响应头 0x00-0x9F 是用户自定义的，0xA0 后是协议保留的
    //-------------------------------------------------------------------------

    /// 保留
    pub const Reserved  : StdHeaderKey = StdHeaderKey::new(0xA0);

    /// 请求体或者回复体中的数据长度，值类型应该是能转成数字的字符串
    pub const Body_Size : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x01);

    /// 请求体或者回复体中的数据 MIME 类型
    pub const Body_Type : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x02);

    /// 指明本报文创建的日期和时间，值类型应该是关于日期的字符串
    pub const Created_At: StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x03);

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
    pub const fn new(code: u16) -> Self {
        StdHeaderKey(code)
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct StdHeaderVal(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl StdHeaderVal {

    pub const Mime_Body_Type_MsgPack: StdHeaderVal = StdHeaderVal(0x01);
    pub const Mime_Body_Type_Json   : StdHeaderVal = StdHeaderVal(0x02);

    pub const User_Rpc_Client: StdHeaderVal = StdHeaderVal(0x10);
}

impl StdHeaderVal {
    pub const fn into_inner(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct Status(u16);

#[rustfmt::skip]
#[allow(non_upper_case_globals)]
impl Status {
    pub const Ok     : Status = Status(200);
    pub const Created: Status = Status(201);

    /// The HTTP 301 Moved Permanently redirection response status code indicates that the requested
    /// resource has been permanently moved to the URL in the Location header.
    pub const MovedPermanently: Status = Status(301);

    /// The HTTP 302 Found redirection response status code indicates that the requested resource
    /// has been temporarily moved to the URL in the Location header.
    pub const Found: Status = Status(302);

    /// The HTTP 303 See Other redirection response status code indicates that the browser should
    /// redirect to the URL in the Location header instead of rendering the requested resource.
    ///
    /// This response code is often sent back as a result of PUT or POST methods so the client may
    /// retrieve a confirmation, or view a representation of a real-world object (see HTTP range-14).
    /// The method to retrieve the redirected resource is always GET.
    pub const SeeOther: Status = Status(303);

    pub const BadRequest  : Status = Status(400);
    pub const Unauthorized: Status = Status(401);
    pub const Forbidden   : Status = Status(403);
    pub const NotFound    : Status = Status(404);

    /// The access method is known by the server but is not supported by the target resource.
    /// For example, an API may not allow DROP a resource, or the TRACE method entirely.
    pub const MethodNotAllowed: Status = Status(405);

    /// This response is sent when the server, after performing server-driven content negotiation,
    /// doesn't find any content that conforms to the criteria given by the user agent.
    pub const NotAcceptable: Status = Status(406);

    pub const InternalServerError: Status = Status(500);
    pub const NotImplemented     : Status = Status(501);
    pub const BadGateway         : Status = Status(502);
    pub const ServiceUnavailable : Status = Status(503);
    pub const GatewayTimeout     : Status = Status(504);
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//  HeaderKey, HeaderVal, wrapping StrOrNum
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HeaderKey(StrOrNum<HeaderStrType, u16>);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HeaderVal(StrOrNum<HeaderStrType, u16>);

impl From<StdHeaderKey> for HeaderKey {
    fn from(value: StdHeaderKey) -> Self {
        HeaderKey(StrOrNum::Num(value.0))
    }
}

impl From<StdHeaderVal> for HeaderVal {
    fn from(value: StdHeaderVal) -> Self {
        HeaderVal(StrOrNum::Num(value.0))
    }
}

impl HeaderVal {
    /// 取出其中的字符串形态（`StrOrNum::Str`）。
    ///
    /// 意图：客户端在读取回复体时，需要把 `Body_Size` 之类的数字型头值
    /// 取出来解析；`StrOrNum` 的两种形态（字符串 / 数字）都要有访问途径，
    /// 否则 `HeaderVal` 内部对调用方完全不透明，无法据此决定回复体长度。
    /// 取出其中的字符串形态（`StrOrNum::Str`）。
    pub fn try_as_str(&self) -> Result<&str, StdHeaderVal> {
        self.0.try_as_str().map_err(Self::to_header_val)
    }

    /// 取出其中的数字形态（`StrOrNum::Num`）。
    pub fn try_as_header_val(&self) -> Result<StdHeaderVal, &str> {
        self.0.try_as_u16().map(Self::to_header_val)
    }

    fn to_header_val(v: u16) -> StdHeaderVal {
        StdHeaderVal(v)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
//  HeaderKey, HeaderVal, boilderplate
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

impl PartialEq for HeaderKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for HeaderKey {}

impl PartialOrd for HeaderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl Ord for HeaderKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialEq for HeaderVal {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for HeaderVal {}

impl PartialOrd for HeaderVal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl Ord for HeaderVal {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Headers
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// A map storing `HeaderKey` as key and some kind of string as value.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Headers {
    /// The btree map to store some header entries. Do not expose it even any
    /// type-specific information. Keep it opaque to users.
    map_: Box<BTreeMap<HeaderKey, HeaderVal>>,
}

impl Headers {
    pub fn new() -> Self {
        Headers {
            map_: Box::new(BTreeMap::new()),
        }
    }

    pub fn try_get_header<'a>(&'a self, key: &HeaderKey) -> Option<&'a HeaderVal> {
        self.map_.get(key)
    }

    pub fn iter_headers<'f>(&'f self) -> impl IntoIterator<Item = (&'f HeaderKey, &'f HeaderVal)> {
        self.map_.iter()
    }

    /// 如果没有键冲突，返回 Ok 以及成功添加的值，否则返回 Err 和冲突键对应的值
    pub fn try_add_header<'f>(
        &'f mut self,
        key: &'f HeaderKey,
        factory: impl FnOnce(&HeaderKey) -> HeaderVal,
    ) -> Result<&'f mut HeaderVal, &'f HeaderKey> {
        let entry = self.map_.entry(key.clone());
        match entry {
            btree_map::Entry::Occupied(_) => {
                // 返回已有键的引用（生命周期与 self 一致）
                Err(key)
            }
            btree_map::Entry::Vacant(vacant) => {
                // insert 返回 &mut V，其生命周期与 self 的可变借用相同
                Ok(vacant.insert(factory(key)))
            }
        }
    }

    /// 添加或者替换键值对
    pub fn add_or_set_header<'f>(
        &'f mut self,
        key: &'f HeaderKey,
        val: &'f HeaderVal,
    ) -> Option<HeaderVal> {
        self.map_.insert(key.clone(), val.clone())
    }

    pub fn remove_header<'f>(&'f mut self, key: &'f HeaderKey) -> Option<(HeaderKey, HeaderVal)> {
        self.map_.remove_entry(key)
    }
}
