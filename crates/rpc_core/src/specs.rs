use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::btree_map::{self, BTreeMap},
    fmt::Debug,
    iter::IntoIterator,
    str::FromStr,
};

use serde::{Serialize, Deserialize};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum AccessMethod {
    /// 查看资源的元信息
    Head = 0,

    /// 查看资源本体内容
    View = 0b_0000_0001,

    /// 上传资源
    Post = 0b_0000_0010,

    /// 删除资源
    Drop = 0b_0000_0100,

    /// 向资源端推送数据
    Push = 0b_0001_0000,

    /// 从资源端拉取数据
    Pull = 0b_0010_0000,

    /// 调用功能，参数由请求头来传递
    Call = 0b_0100_0000,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
pub struct StdHeaderKey(u16);

#[allow(non_upper_case_globals)]
impl StdHeaderKey {
    //------------------------------------------------------------------------
    // 两端端常用的标准响应头
    //------------------------------------------------------------------------

    /// 保留
    pub const Reserved      : StdHeaderKey = StdHeaderKey::new(0x00);

    /// 请求体或者回复体中的数据长度，值类型应该是能转成数字的字符串
    pub const Content_Length: StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x01);

    /// 请求体或者回复体中的数据 MIME 类型
    pub const Content_Type  : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x02);

    /// 指明报文创建的日期和时间，值类型应该是关于日期的字符串
    pub const Date          : StdHeaderKey = StdHeaderKey::new(Self::Reserved.0 + 0x03);

    //------------------------------------------------------------------------
    // 客户端常用的标准请求头
    //------------------------------------------------------------------------

    /// 标识客户端的应用程序类型、操作系统、浏览器版本等信息。
    pub const User_Agent    : StdHeaderKey = StdHeaderKey::new(0x20);

    /// 告知服务器客户端能够处理的内容类型（MIME 类型），例如 application/json。
    pub const Accept        : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x01);

    /// 携带身份认证凭证（如 JWT Token、Basic 认证）
    pub const Authorization : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x02);

    /// 携带服务器之前通过 Set-Cookie 指令存储在客户端的状态信息
    pub const Cookie        : StdHeaderKey = StdHeaderKey::new(Self::User_Agent.0 + 0x03);

    //------------------------------------------------------------------------
    // 服务端常用的标准响应头
    //------------------------------------------------------------------------

    /// 标识服务器软件的名称和版本信息
    pub const Server        : StdHeaderKey = StdHeaderKey::new(0x40);

    /// 用于重定向，告知客户端新的资源地址，值类型应该是路径或者 URI
    pub const Location      : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x01);

    /// 指明所请求资源的最后修改时间，值类型应该是关于日期的字符串
    pub const Last_Modified : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x02);

    /// 服务器通过此字段向客户端设置 Cookie
    pub const Set_Cookie    : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x03);

    /// 资源的唯一标识符（实体标签），用于缓存验证
    pub const ETag          : StdHeaderKey = StdHeaderKey::new(Self::Server.0 + 0x04);
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

type HeaderStrType = String;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum HeaderKey {
    Std(StdHeaderKey),
    Str(HeaderStrType),
}

impl PartialEq for HeaderKey {
    fn eq(&self, other: &Self) -> bool {
        match (&self, &other) {
            (HeaderKey::Std(this), HeaderKey::Std(that)) => StdHeaderKey::eq(this, that),
            (HeaderKey::Str(this), HeaderKey::Str(that)) => str::eq(this.as_str(), that.as_str()),
            _ => false,
        }
    }
}

impl Eq for HeaderKey { }

impl PartialOrd for HeaderKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Option::Some(self.cmp(other))
    }
}

impl Ord for HeaderKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self, &other) {
            (HeaderKey::Std(this), HeaderKey::Std(that)) => this.cmp(that),
            (HeaderKey::Str(this), HeaderKey::Str(that)) => str::cmp(this.borrow(), that.borrow()),
            (HeaderKey::Std(_), HeaderKey::Str(_)) => Ordering::Less,
            (HeaderKey::Str(_), HeaderKey::Std(_)) => Ordering::Greater,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Headers {
    /// The btree map to store some header entries. Do not expose it even any
    /// type-specific information. Keep it opaque to users.
    map_: BTreeMap<HeaderKey, HeaderStrType>,
}

impl Headers {
    pub fn new() -> Self {
        Headers { map_: BTreeMap::new() }
    }

    pub fn try_get_header<'a>(&'a self, key: &HeaderKey) -> Option<&'a str> {
        self.map_
            .get(key)
            .map(|s| s.as_str())
    }

    pub fn iter_headers<'f>(&'f self) -> impl IntoIterator<Item = (&'f HeaderKey, &'f str)> {
        self.map_
            .iter()
            .map(|t| (t.0, t.1.as_str()))
    }

    /// 如果没有键冲突，返回 Ok 以及成功添加的值，否则返回 Err 和冲突键对应的值
    pub fn try_add_header<'f>(
        &'f mut self,
        key: &'f HeaderKey,
        factory: impl FnOnce(&HeaderKey) -> HeaderStrType,
    ) -> Result<&'f mut HeaderStrType, &'f HeaderKey> {
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
        val: &'f str,
    ) -> Option<impl Borrow<str>> {
        self.map_
            .insert(key.clone(), HeaderStrType::from_str(val).unwrap())
    }

    pub fn remove_header<'f>(
        &'f mut self,
        key: &'f HeaderKey,
    ) -> Option<(HeaderKey, impl Borrow<str>)> {
        self.map_.remove_entry(key)
    }
}


#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct Status(u16);

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
