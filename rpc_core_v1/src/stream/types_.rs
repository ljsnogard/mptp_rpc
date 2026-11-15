//! 类型标识、`Accept` 编码、端点类型元信息与请求头校验。
//!
//! 本模块把 MPTP 的三个标准头连成一套**可校验**的类型协议：
//!
//! - `Body_Type`：数据通道的 codec（MessagePack / JSON / 裸字节）；
//! - `Data_Type_Id`：本方向唯一数据通道的 **u64 稳定类型 ID**；
//! - `Accept`：对端方向的期望（codec + 可选 type-id）。
//!
//! 方向语义、状态码边界与版本规则见
//! `mptp_rpc/dev-notes/rpc-typed-push-pull-20261114-1300.md` §3–§6。
//!
//! # `types_`: type ids, `Accept` and endpoint type metadata
//!
//! This module ties three MPTP standard headers into a checkable type protocol:
//! `Body_Type` (codec), `Data_Type_Id` (a stable u64 id for the single data channel of
//! the direction) and `Accept` (the peer's expected codec + type id).

use alloc::{
    format,
    string::{String, ToString},
};
use core::{fmt, str::FromStr};

use abs_mptp::spec::{Status, StdHeaderKey, access_method::AccessMethod};
use mptp_wire::Headers;

use crate::x_deps::{abs_mptp, mptp_wire};

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 类型 ID
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 一个跨进程、跨版本稳定的 RPC 数据类型标识。
///
/// 取值 `0` 保留给「无类型 / 缺失」，不允许上线；因此本类型只能通过
/// [`DataTypeId::new`] / [`DataTypeId::from_nonzero_u64`] 构造出非零值。文本形态固定写
/// `0x` + 小写十六进制，解析同时接受十进制与 `0x` / `0X` 十六进制。
///
/// `type-id` feature 开启时，业务类型不应再手写 [`TrDataTypeId`]：denco 的
/// `#[derive(TrHasTypeId)]` 给出结构化指纹，blanket impl 自动把它映射成
/// `TYPE_ID_64`。手写只用于固定旧 ID（兼容）或打断递归。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::DataTypeId;
///
/// let id = DataTypeId::new(0x1234).expect("非零 ID");
/// assert_eq!(id.to_string(), "0x1234");
/// assert_eq!("4660".parse::<DataTypeId>().unwrap(), id);
/// assert!(DataTypeId::new(0).is_none());
/// assert_eq!(DataTypeId::from_nonzero_u64(0x1234).into_inner(), 0x1234);
/// ```
///
/// 零值在 `const` 上下文里直接编译失败：
///
/// ```compile_fail
/// use rpc_core_v1::stream::DataTypeId;
///
/// const BAD: DataTypeId = DataTypeId::from_nonzero_u64(0);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DataTypeId(u64);

impl DataTypeId {
    /// 由非零原始值构造；`0` 返回 `None`。
    pub const fn new(raw: u64) -> Option<Self> {
        if raw == 0u64 {
            Option::None
        } else {
            Option::Some(DataTypeId(raw))
        }
    }

    /// 由非零原始值构造；`0` 在 `const` 求值时直接 panic。
    ///
    /// 面向 `const` 消费方（例如 blanket impl 里的
    /// `<T as TrHasTypeId>::TYPE_ID_64`）：这些位置没有 `Option` 可返回，而 `0` 是
    /// 线上非法值，必须在编译期拒绝。
    ///
    /// # Panics
    ///
    /// `raw == 0` 时 panic；发生在 `const` 求值期即编译错误。
    pub const fn from_nonzero_u64(raw: u64) -> Self {
        if raw == 0u64 {
            panic!("Data_Type_Id 不能为 0");
        }
        DataTypeId(raw)
    }

    /// 取原始 `u64`。
    pub const fn into_inner(self) -> u64 {
        self.0
    }
}

impl fmt::Display for DataTypeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:x}", self.0)
    }
}

impl FromStr for DataTypeId {
    type Err = TypeHeaderError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let text = text.trim();
        let raw = mptp_wire::HeaderVal::<String>::from_text(text)
            .as_u64()
            .ok_or_else(|| TypeHeaderError::MalformedDataTypeId(text.to_string()))?;
        DataTypeId::new(raw).ok_or_else(|| TypeHeaderError::MalformedDataTypeId(text.to_string()))
    }
}

/// 一个类型在线上使用的稳定 ID 契约。
///
/// 这是 MPTP 线协议的**窄 facade**：只要求一个非零 [`DataTypeId`]，不关心 ID 怎么生成。
///
/// `type-id` feature 开启时，本 crate 提供
/// `impl<T: abs_denco::type_id::TrHasTypeId> TrDataTypeId for T`，直接从 denco 的
/// **结构化指纹** `TYPE_ID_64` 取值。业务类型应写：
///
/// ```text
/// #[derive(denco_macro::TrEncodable, denco_macro::TrDecodable, denco_macro::TrHasTypeId)]
/// pub struct User { /* ... */ }
/// ```
///
/// 不要再手写 [`TrDataTypeId`]：
///
/// - 需要固定旧 ID（线上兼容）或打断递归时，改为手写
///   `impl abs_denco::type_id::TrHasTypeId`，三个宽度都显式给值（可用
///   `abs_denco::type_id::path_id_64`），这样 ID 仍只有一处定义；
/// - 只有 **关闭** `type-id` feature、或类型不实现 `TrHasTypeId` 时，手写
///   `TrDataTypeId` 才仍然可用。
///
/// # 版本契约
///
/// ID 由 denco 的 `TYPE_ID_VERSION` + `HASH_ALGORITHM` + 定义处 `module_path!()` 决定；
/// 跨进程两端必须使用同一 denco rev / feature / hash。字段名、字段类型、字段顺序、
/// 变体名或载荷变化都会改变 ID。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::{DataTypeId, TrDataTypeId};
///
/// struct LegacyType;
///
/// impl TrDataTypeId for LegacyType {
///     const DATA_TYPE_ID: DataTypeId = DataTypeId::from_nonzero_u64(0x1001);
/// }
///
/// assert_eq!(LegacyType::DATA_TYPE_ID.to_string(), "0x1001");
/// ```
pub trait TrDataTypeId {
    /// 本类型的稳定线上 ID（非零）。
    const DATA_TYPE_ID: DataTypeId;
}

/// 由 denco 的结构化类型指纹自动派生 `Data_Type_Id`。
///
/// 只使用 `TYPE_ID_64`：MPTP 的 `Data_Type_Id` 是 `u64`，128 / 32 位不参与线上头。
/// `TrHasTypeId` 的实现者保证三个宽度都非零，因此这里用
/// [`DataTypeId::from_nonzero_u64`] 做 `const` 构造。
///
/// 同一个类型不要同时 derive `TrHasTypeId` 并手写 `TrDataTypeId`：两者会冲突。
#[cfg(feature = "type-id")]
impl<T> TrDataTypeId for T
where
    T: crate::x_deps::abs_denco::type_id::TrHasTypeId,
{
    const DATA_TYPE_ID: DataTypeId = DataTypeId::from_nonzero_u64(
        <T as crate::x_deps::abs_denco::type_id::TrHasTypeId>::TYPE_ID_64,
    );
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// codec
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 数据通道的编码格式（`Body_Type` 的语义）。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BodyCodec {
    /// MessagePack；标准数字取值 `0x01`，MIME 为 `application/x-mptp-msgpack`。
    MsgPack,

    /// JSON；标准数字取值 `0x02`，MIME 为 `application/json`。
    Json,

    /// 裸字节；没有标准数字取值，只用 MIME `application/octet-stream`。
    Raw,
}

impl BodyCodec {
    /// 协议标准数字取值；`Raw` 没有标准数字。
    pub const fn standard_code(self) -> Option<u16> {
        match self {
            BodyCodec::MsgPack => Option::Some(0x01),
            BodyCodec::Json => Option::Some(0x02),
            BodyCodec::Raw => Option::None,
        }
    }

    /// 规范 MIME 文本。
    pub const fn mime(self) -> &'static str {
        match self {
            BodyCodec::MsgPack => "application/x-mptp-msgpack",
            BodyCodec::Json => "application/json",
            BodyCodec::Raw => "application/octet-stream",
        }
    }

    /// 由标准数字取值还原 codec。
    pub const fn from_standard_code(code: u16) -> Option<Self> {
        match code {
            0x01 => Option::Some(BodyCodec::MsgPack),
            0x02 => Option::Some(BodyCodec::Json),
            _ => Option::None,
        }
    }

    /// 由 MIME 文本还原 codec（忽略大小写与首尾空白）。
    pub fn from_mime(text: &str) -> Option<Self> {
        let text = text.trim();
        if text.eq_ignore_ascii_case(BodyCodec::MsgPack.mime()) {
            Option::Some(BodyCodec::MsgPack)
        } else if text.eq_ignore_ascii_case(BodyCodec::Json.mime()) {
            Option::Some(BodyCodec::Json)
        } else if text.eq_ignore_ascii_case(BodyCodec::Raw.mime()) {
            Option::Some(BodyCodec::Raw)
        } else {
            Option::None
        }
    }
}

impl fmt::Display for BodyCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.mime())
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// Accept
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// `Accept` 头表达的一项目标：响应方向（body 或 suffix）的 codec 与可选类型 ID。
///
/// 文本形态：
///
/// ```text
/// application/x-mptp-msgpack; type-id=0x1234
/// application/json; type-id=42
/// application/octet-stream
/// ```
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::{AcceptType, BodyCodec, DataTypeId};
///
/// let accept = AcceptType::new(
///     BodyCodec::MsgPack,
///     Some(DataTypeId::new(0xabc).unwrap()),
/// );
/// assert_eq!(accept.to_string(), "application/x-mptp-msgpack; type-id=0xabc");
/// assert_eq!(AcceptType::parse(&accept.to_string()).unwrap(), accept);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AcceptType {
    /// 期望的 codec。
    codec_: BodyCodec,

    /// 期望的类型 ID；Pull 请求必须给出。
    type_id_: Option<DataTypeId>,
}

impl AcceptType {
    /// 由「codec + 可选类型 ID」构造。
    pub const fn new(codec: BodyCodec, type_id: Option<DataTypeId>) -> Self {
        AcceptType {
            codec_: codec,
            type_id_: type_id,
        }
    }

    /// 只表达 codec。
    pub const fn codec_only(codec: BodyCodec) -> Self {
        AcceptType::new(codec, Option::None)
    }

    /// 期望的 codec。
    pub const fn codec(self) -> BodyCodec {
        self.codec_
    }

    /// 期望的类型 ID。
    pub const fn type_id(self) -> Option<DataTypeId> {
        self.type_id_
    }

    /// 解析 `Accept` 文本。
    ///
    /// 只接受单个值，不支持 q 值列表；未知参数与重复 `type-id` 都明确报错。
    pub fn parse(text: &str) -> Result<Self, TypeHeaderError> {
        let mut parts = text.split(';');
        let mime = parts.next().unwrap_or("").trim();
        if mime.is_empty() {
            return Result::Err(TypeHeaderError::MalformedAccept(text.to_string()));
        }
        let codec = BodyCodec::from_mime(mime)
            .ok_or_else(|| TypeHeaderError::UnknownBodyCodec(mime.to_string()))?;

        let mut type_id = Option::None;
        for param in parts {
            let param = param.trim();
            if param.is_empty() {
                continue;
            }
            let Some((name, value)) = param.split_once('=') else {
                return Result::Err(TypeHeaderError::MalformedAccept(text.to_string()));
            };
            if !name.trim().eq_ignore_ascii_case("type-id") {
                return Result::Err(TypeHeaderError::MalformedAccept(format!(
                    "未知的 Accept 参数 `{}`",
                    name.trim()
                )));
            }
            if type_id.is_some() {
                return Result::Err(TypeHeaderError::DuplicateAcceptTypeId);
            }
            type_id = Option::Some(value.trim().parse::<DataTypeId>()?);
        }

        Result::Ok(AcceptType {
            codec_: codec,
            type_id_: type_id,
        })
    }
}

impl fmt::Display for AcceptType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.type_id_ {
            Option::Some(id) => write!(f, "{}; type-id={id}", self.codec_.mime()),
            Option::None => f.write_str(self.codec_.mime()),
        }
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 错误
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 类型头 / `Accept` 文本的解析错误。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeHeaderError {
    /// `Data_Type_Id` 不是非零的十进制 / 十六进制整数。
    MalformedDataTypeId(String),

    /// `Body_Type` / `Accept` 的 MIME 不是已知 codec。
    UnknownBodyCodec(String),

    /// `Accept` 文本不合法（空值、未知参数、缺 `=` 等）。
    MalformedAccept(String),

    /// `Accept` 里出现了重复的 `type-id` 参数。
    DuplicateAcceptTypeId,

    /// `Accept` 缺少 Pull 请求所必需的 `type-id`。
    MissingAcceptTypeId,
}

impl fmt::Display for TypeHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeHeaderError::MalformedDataTypeId(text) => {
                write!(f, "非法的 Data_Type_Id：`{text}`")
            }
            TypeHeaderError::UnknownBodyCodec(text) => {
                write!(f, "未知的 Body_Type / MIME：`{text}`")
            }
            TypeHeaderError::MalformedAccept(text) => write!(f, "非法的 Accept：`{text}`"),
            TypeHeaderError::DuplicateAcceptTypeId => write!(f, "Accept 里重复出现 type-id"),
            TypeHeaderError::MissingAcceptTypeId => {
                write!(f, "Accept 缺少 type-id，无法确定期望的数据类型")
            }
        }
    }
}

impl core::error::Error for TypeHeaderError {}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 头表读写
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 读取 `Body_Type`；头缺失返回 `None`。
pub fn read_body_codec(headers: &Headers) -> Result<Option<BodyCodec>, TypeHeaderError> {
    let Some(val) = headers.try_get_val(StdHeaderKey::Body_Type) else {
        return Result::Ok(Option::None);
    };
    if let Option::Some(num) = val.as_u64() {
        let code =
            u16::try_from(num).map_err(|_| TypeHeaderError::UnknownBodyCodec(num.to_string()))?;
        let codec = BodyCodec::from_standard_code(code)
            .ok_or_else(|| TypeHeaderError::UnknownBodyCodec(num.to_string()))?;
        return Result::Ok(Option::Some(codec));
    }
    let text = val
        .as_text()
        .ok_or_else(|| TypeHeaderError::UnknownBodyCodec(String::from("<非文本值>")))?;
    let codec = BodyCodec::from_mime(text)
        .ok_or_else(|| TypeHeaderError::UnknownBodyCodec(text.to_string()))?;
    Result::Ok(Option::Some(codec))
}

/// 写 `Body_Type`：有标准数字用数字，否则写 MIME。
pub fn write_body_codec(headers: &mut Headers, codec: BodyCodec) {
    match codec.standard_code() {
        Option::Some(code) => headers.set_num(StdHeaderKey::Body_Type, code),
        Option::None => headers.set_text(StdHeaderKey::Body_Type, codec.mime()),
    }
}

/// 读取 `Data_Type_Id`；头缺失返回 `None`，头存在但值非法返回错误。
pub fn read_data_type_id(headers: &Headers) -> Result<Option<DataTypeId>, TypeHeaderError> {
    let Some(val) = headers.try_get_val(StdHeaderKey::Data_Type_Id) else {
        return Result::Ok(Option::None);
    };
    let raw = val.as_u64().ok_or_else(|| {
        let text = val.as_text().unwrap_or("<非文本值>").to_string();
        TypeHeaderError::MalformedDataTypeId(text)
    })?;
    let id = DataTypeId::new(raw)
        .ok_or_else(|| TypeHeaderError::MalformedDataTypeId(raw.to_string()))?;
    Result::Ok(Option::Some(id))
}

/// 写 `Data_Type_Id`（固定 `0x` 十六进制文本）。
pub fn write_data_type_id(headers: &mut Headers, id: DataTypeId) {
    headers.set_text(StdHeaderKey::Data_Type_Id, id.to_string());
}

/// 读取 `Accept`；头缺失返回 `None`。
pub fn read_accept(headers: &Headers) -> Result<Option<AcceptType>, TypeHeaderError> {
    let Some(val) = headers.try_get_val(StdHeaderKey::Accept) else {
        return Result::Ok(Option::None);
    };
    let text = val
        .as_text()
        .ok_or_else(|| TypeHeaderError::MalformedAccept(String::from("<非文本值>")))?;
    Result::Ok(Option::Some(AcceptType::parse(text)?))
}

/// 写 `Accept`。
pub fn write_accept(headers: &mut Headers, accept: AcceptType) {
    headers.set_text(StdHeaderKey::Accept, accept.to_string());
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 端点元信息
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 端点的结构化身份元信息（`namespace/service/operation/pattern`）。
///
/// 由宏或手写装配挂在 `mptp_serving::Endpoint::with_metadata` 上；中间件按三个粒度
/// （operation / service / namespace）读它，不依赖字符串解析 `pattern`。
///
/// 注意：本 crate 不依赖 `mptp_serving`，因此这里只定义标记类型。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcEndpointInfo {
    /// 契约命名空间（不含 router 挂载点）。
    pub namespace: &'static str,

    /// service 段。
    pub service: &'static str,

    /// operation 段（trait 方法名 / controller 方法名）。
    pub operation: &'static str,

    /// 相对 router 根的 pattern。
    pub pattern: &'static str,
}

/// 严格 / 兼容两种类型头策略。
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum TypePolicy {
    /// 必填头缺失即拒绝；头存在但不匹配也拒绝。
    #[default]
    Strict,

    /// 头缺失时放行；头存在但不匹配仍然拒绝。
    ///
    /// 只用于旧客户端 / 测试的显式兼容模式，不能默认打开。
    Optional,
}

/// 一个端点的类型元信息：请求 / 响应 body 与 suffix 的类型、codec、期望 Accept。
///
/// 字段私有，用构造器逐项点出；这样后续新增字段不会破坏外部代码。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::{
///     AcceptType, BodyCodec, DataTypeId, RpcDataTypeInfo, TypePolicy,
/// };
///
/// let info = RpcDataTypeInfo::new(BodyCodec::MsgPack)
///     .with_request_body(DataTypeId::new(0x10).unwrap())
///     .with_response_body(DataTypeId::new(0x11).unwrap())
///     .with_accept(AcceptType::new(
///         BodyCodec::MsgPack,
///         Some(DataTypeId::new(0x11).unwrap()),
///     ))
///     .with_policy(TypePolicy::Strict);
///
/// assert_eq!(info.body_codec(), BodyCodec::MsgPack);
/// assert_eq!(info.policy(), TypePolicy::Strict);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcDataTypeInfo {
    /// 数据通道 codec。
    body_codec_: BodyCodec,

    /// 请求 body 类型（无请求体为 `None`）。
    request_body_: Option<DataTypeId>,

    /// 响应 body 类型（无响应体为 `None`）。
    response_body_: Option<DataTypeId>,

    /// 请求 suffix item 类型（Push 用）。
    request_suffix_: Option<DataTypeId>,

    /// 响应 suffix item 类型（Pull 用）。
    response_suffix_: Option<DataTypeId>,

    /// 端点期望客户端发来的 `Accept`（Call / Pull / Push）。
    accept_: Option<AcceptType>,

    /// 校验策略。
    policy_: TypePolicy,
}

impl RpcDataTypeInfo {
    /// 由 codec 构造一份全空的严格元信息。
    pub const fn new(body_codec: BodyCodec) -> Self {
        RpcDataTypeInfo {
            body_codec_: body_codec,
            request_body_: Option::None,
            response_body_: Option::None,
            request_suffix_: Option::None,
            response_suffix_: Option::None,
            accept_: Option::None,
            policy_: TypePolicy::Strict,
        }
    }

    /// 设置请求 body 类型。
    pub const fn with_request_body(mut self, id: DataTypeId) -> Self {
        self.request_body_ = Option::Some(id);
        self
    }

    /// 设置响应 body 类型。
    pub const fn with_response_body(mut self, id: DataTypeId) -> Self {
        self.response_body_ = Option::Some(id);
        self
    }

    /// 设置请求 suffix item 类型。
    pub const fn with_request_suffix(mut self, id: DataTypeId) -> Self {
        self.request_suffix_ = Option::Some(id);
        self
    }

    /// 设置响应 suffix item 类型。
    pub const fn with_response_suffix(mut self, id: DataTypeId) -> Self {
        self.response_suffix_ = Option::Some(id);
        self
    }

    /// 设置端点期望的 `Accept`。
    pub const fn with_accept(mut self, accept: AcceptType) -> Self {
        self.accept_ = Option::Some(accept);
        self
    }

    /// 设置校验策略。
    pub const fn with_policy(mut self, policy: TypePolicy) -> Self {
        self.policy_ = policy;
        self
    }

    /// 数据通道 codec。
    pub const fn body_codec(&self) -> BodyCodec {
        self.body_codec_
    }

    /// 请求 body 类型。
    pub const fn request_body(&self) -> Option<DataTypeId> {
        self.request_body_
    }

    /// 响应 body 类型。
    pub const fn response_body(&self) -> Option<DataTypeId> {
        self.response_body_
    }

    /// 请求 suffix item 类型。
    pub const fn request_suffix(&self) -> Option<DataTypeId> {
        self.request_suffix_
    }

    /// 响应 suffix item 类型。
    pub const fn response_suffix(&self) -> Option<DataTypeId> {
        self.response_suffix_
    }

    /// 端点期望的 `Accept`。
    pub const fn accept(&self) -> Option<AcceptType> {
        self.accept_
    }

    /// 校验策略。
    pub const fn policy(&self) -> TypePolicy {
        self.policy_
    }
}

/// 一个 service 方法在类型头协议里的元信息。
///
/// Call 的 service 级 endpoint 把方法放在请求枚举里；每个方法有自己的
/// `request_body` / `response_body` / `accept` / `policy`。运行时先读变体名，选中本
/// 结构，再调用 [`validate_request_types`]。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::{
///     BodyCodec, DataTypeId, RpcDataTypeInfo, RpcMethodInfo, TypePolicy,
/// };
///
/// let data = RpcDataTypeInfo::new(BodyCodec::MsgPack)
///     .with_request_body(DataTypeId::from_nonzero_u64(0x10))
///     .with_response_body(DataTypeId::from_nonzero_u64(0x11))
///     .with_policy(TypePolicy::Strict);
/// let method = RpcMethodInfo::new("create", data);
/// assert_eq!(method.operation(), "create");
/// assert_eq!(method.data().body_codec(), BodyCodec::MsgPack);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcMethodInfo {
    /// 方法名（wire 上的 operation，即请求枚举变体名）。
    operation_: &'static str,

    /// 方法级类型元信息。
    data_: RpcDataTypeInfo,
}

impl RpcMethodInfo {
    /// 由方法名与类型元信息构造。
    pub const fn new(operation: &'static str, data: RpcDataTypeInfo) -> Self {
        RpcMethodInfo {
            operation_: operation,
            data_: data,
        }
    }

    /// 方法名（请求枚举变体名）。
    pub const fn operation(&self) -> &'static str {
        self.operation_
    }

    /// 方法级类型元信息。
    pub const fn data(&self) -> &RpcDataTypeInfo {
        &self.data_
    }
}

/// 一个 service 级 endpoint 的方法表。
///
/// 这是 Call 请求枚举模式的端点元信息：一个 `CALL /<prefix>` endpoint 对应 N 个方法；
/// 每个方法在 [`methods`](Self::methods) 里有一项。非 Call endpoint 可以不用本类型，
/// 继续用 [`RpcDataTypeInfo`]。
///
/// # Examples
///
/// ```
/// use rpc_core_v1::stream::{
///     BodyCodec, DataTypeId, RpcDataTypeInfo, RpcMethodInfo, RpcServiceInfo,
/// };
///
/// const METHODS: &[RpcMethodInfo] = &[RpcMethodInfo::new(
///     "create",
///     RpcDataTypeInfo::new(BodyCodec::MsgPack)
///         .with_request_body(DataTypeId::from_nonzero_u64(0x10))
///         .with_response_body(DataTypeId::from_nonzero_u64(0x11)),
/// )];
///
/// let info = RpcServiceInfo::new("im/v1/room", "im/v1/room", METHODS);
/// assert_eq!(info.service(), "im/v1/room");
/// assert_eq!(info.methods().len(), 1);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RpcServiceInfo {
    /// 完整服务前缀，如 `im/v1/room`。
    service_: &'static str,

    /// 相对 router 根的 pattern（Call service 与 `service` 相同）。
    pattern_: &'static str,

    /// 方法表。
    methods_: &'static [RpcMethodInfo],
}

impl RpcServiceInfo {
    /// 由 service / pattern / 方法表构造。
    pub const fn new(
        service: &'static str,
        pattern: &'static str,
        methods: &'static [RpcMethodInfo],
    ) -> Self {
        RpcServiceInfo {
            service_: service,
            pattern_: pattern,
            methods_: methods,
        }
    }

    /// 完整服务前缀。
    pub const fn service(&self) -> &'static str {
        self.service_
    }

    /// 相对 router 根的 pattern。
    pub const fn pattern(&self) -> &'static str {
        self.pattern_
    }

    /// 方法表。
    pub const fn methods(&self) -> &'static [RpcMethodInfo] {
        self.methods_
    }

    /// 按 operation 查方法。
    pub fn method(&self, operation: &str) -> Option<&'static RpcMethodInfo> {
        self.methods_
            .iter()
            .find(|method| method.operation() == operation)
    }
}

/// 请求头与端点元信息不匹配的具体原因。
///
/// [`TypeRejection::status`] 给出应当回给对端的状态码：缺失 / 畸形头是 `400`，
/// 类型 / codec / Accept 不匹配是 `406`。`Status` 没有 415，本模块不发明状态码。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeRejection {
    /// 必填头缺失。
    MissingHeader {
        /// 缺失的标准头名称。
        header: &'static str,
    },

    /// 头值无法解析。
    MalformedHeader {
        /// 头的名称。
        header: &'static str,

        /// 解析错误文本。
        detail: String,
    },

    /// 本方向不应该出现的数据头出现了（例如 Pull 请求的 `Data_Type_Id`）。
    UnexpectedDataHeader {
        /// 多出来的头名称。
        header: &'static str,
    },

    /// `Body_Type` 与端点 codec 不一致，或 codec 不受支持。
    CodecMismatch {
        /// 端点声明 / 期望的 codec。
        expected: BodyCodec,

        /// 请求头里的 codec。
        actual: BodyCodec,
    },

    /// `Data_Type_Id` 与端点声明的类型不一致。
    DataTypeMismatch {
        /// 端点声明的类型。
        expected: DataTypeId,

        /// 请求头里的类型。
        actual: DataTypeId,
    },

    /// `Accept` 与端点能提供的响应不一致。
    AcceptMismatch {
        /// 端点能提供的响应。
        expected: AcceptType,

        /// 客户端期望的响应。
        actual: AcceptType,
    },
}

impl TypeRejection {
    /// 应当回给对端的状态码。
    pub const fn status(&self) -> Status {
        match self {
            TypeRejection::MissingHeader { .. }
            | TypeRejection::MalformedHeader { .. }
            | TypeRejection::UnexpectedDataHeader { .. } => Status::BadRequest,
            TypeRejection::CodecMismatch { .. }
            | TypeRejection::DataTypeMismatch { .. }
            | TypeRejection::AcceptMismatch { .. } => Status::NotAcceptable,
        }
    }
}

impl fmt::Display for TypeRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeRejection::MissingHeader { header } => write!(f, "缺少必填类型头 `{header}`"),
            TypeRejection::MalformedHeader { header, detail } => {
                write!(f, "类型头 `{header}` 无法解析：{detail}")
            }
            TypeRejection::UnexpectedDataHeader { header } => {
                write!(f, "本方向不应出现数据头 `{header}`")
            }
            TypeRejection::CodecMismatch { expected, actual } => {
                write!(f, "codec 不匹配：期望 {expected}，实际 {actual}")
            }
            TypeRejection::DataTypeMismatch { expected, actual } => {
                write!(f, "类型 ID 不匹配：期望 {expected}，实际 {actual}")
            }
            TypeRejection::AcceptMismatch { expected, actual } => {
                write!(f, "Accept 不匹配：端点提供 {expected}，客户端期望 {actual}")
            }
        }
    }
}

impl core::error::Error for TypeRejection {}

/// 按方法校验一次请求的类型头。
///
/// 方向表与状态码边界见模块文档；本函数只读头、不改头，也不进入 handler。
///
/// # Errors
///
/// 必填头缺失 / 畸形返回 `400` 对应变体；codec、`Data_Type_Id`、`Accept` 不匹配返回
/// `406` 对应变体。`TypePolicy::Optional` 只放宽「头缺失」。
pub fn validate_request_types(
    method: AccessMethod,
    headers: &Headers,
    info: &RpcDataTypeInfo,
) -> Result<(), TypeRejection> {
    let strict = info.policy() == TypePolicy::Strict;
    match method {
        AccessMethod::Call | AccessMethod::Post => {
            validate_body_direction_(headers, info, strict, info.request_body(), "请求 body")?;
            validate_optional_accept_(headers, info)?;
            Result::Ok(())
        }
        AccessMethod::Pull => {
            // 第一期：Pull 请求没有 body；Data_Type_Id 只表达 suffix item 类型，
            // 它出现在请求方向就是方向错误。
            if read_data_type_id(headers)
                .map_err(malformed_data_type_id_)?
                .is_some()
            {
                return Result::Err(TypeRejection::UnexpectedDataHeader {
                    header: "Data_Type_Id",
                });
            }
            validate_codec_(headers, info, strict, true)?;

            let Some(expected) = info.accept() else {
                if strict {
                    return Result::Err(TypeRejection::MissingHeader { header: "Accept" });
                }
                return Result::Ok(());
            };
            match read_accept(headers).map_err(malformed_accept_)? {
                Option::Some(actual) => compare_accept_(expected, actual),
                Option::None if strict => {
                    Result::Err(TypeRejection::MissingHeader { header: "Accept" })
                }
                Option::None => Result::Ok(()),
            }
        }
        AccessMethod::Push => {
            validate_codec_(headers, info, strict, true)?;
            let expected = info.request_suffix();
            match (
                read_data_type_id(headers).map_err(malformed_data_type_id_)?,
                expected,
            ) {
                (Option::Some(actual), Option::Some(expected)) if actual != expected => {
                    Result::Err(TypeRejection::DataTypeMismatch { expected, actual })
                }
                (Option::None, Option::Some(_)) if strict => {
                    Result::Err(TypeRejection::MissingHeader {
                        header: "Data_Type_Id",
                    })
                }
                (Option::Some(_), Option::None) => {
                    Result::Err(TypeRejection::UnexpectedDataHeader {
                        header: "Data_Type_Id",
                    })
                }
                _ => Result::Ok(()),
            }?;
            validate_optional_accept_(headers, info)
        }
        AccessMethod::Head | AccessMethod::View | AccessMethod::Drop => {
            if strict
                && read_data_type_id(headers)
                    .map_err(malformed_data_type_id_)?
                    .is_some()
            {
                return Result::Err(TypeRejection::UnexpectedDataHeader {
                    header: "Data_Type_Id",
                });
            }
            Result::Ok(())
        }
    }
}

/// 按方法校验一次**响应**的类型头（客户端 / 中间件用）。
///
/// 与 [`validate_request_types`] 对称：Pull 响应校验 suffix codec + item 类型；
/// Call / Post / View / Push 响应校验 body codec + body 类型；Head / Drop 不允许数据头。
///
/// # Errors
///
/// 缺失 / 畸形头返回 `400` 对应变体；codec 或类型 ID 不匹配返回 `406` 对应变体。
pub fn validate_response_types(
    method: AccessMethod,
    headers: &Headers,
    info: &RpcDataTypeInfo,
) -> Result<(), TypeRejection> {
    let strict = info.policy() == TypePolicy::Strict;
    match method {
        AccessMethod::Pull => {
            validate_codec_(headers, info, strict, true)?;
            let expected = info
                .response_suffix()
                .or_else(|| info.accept().and_then(|accept| accept.type_id()));
            match (read_data_type_id(headers).map_err(malformed_data_type_id_)?, expected) {
                (Option::Some(actual), Option::Some(expected)) if actual != expected => {
                    Result::Err(TypeRejection::DataTypeMismatch { expected, actual })
                }
                (Option::None, Option::Some(_)) if strict => {
                    Result::Err(TypeRejection::MissingHeader {
                        header: "Data_Type_Id",
                    })
                }
                (Option::Some(_), Option::None) => Result::Err(
                    TypeRejection::UnexpectedDataHeader { header: "Data_Type_Id" },
                ),
                _ => Result::Ok(()),
            }
        }
        AccessMethod::Call
        | AccessMethod::Post
        | AccessMethod::View
        | AccessMethod::Push => validate_body_direction_(
            headers,
            info,
            strict,
            info.response_body(),
            "响应 body",
        ),
        AccessMethod::Head | AccessMethod::Drop => {
            if strict
                && (read_body_codec(headers).map_err(malformed_codec_)?.is_some()
                    || read_data_type_id(headers)
                        .map_err(malformed_data_type_id_)?
                        .is_some())
            {
                return Result::Err(TypeRejection::UnexpectedDataHeader {
                    header: "Body_Type / Data_Type_Id",
                });
            }
            Result::Ok(())
        }
    }
}

/// 校验一个「有 body」方向的 codec 与 `Data_Type_Id`。
fn validate_body_direction_(
    headers: &Headers,
    info: &RpcDataTypeInfo,
    strict: bool,
    expected_id: Option<DataTypeId>,
    _what: &'static str,
) -> Result<(), TypeRejection> {
    match expected_id {
        Option::Some(expected) => {
            validate_codec_(headers, info, strict, true)?;
            match read_data_type_id(headers).map_err(malformed_data_type_id_)? {
                Option::Some(actual) if actual != expected => {
                    Result::Err(TypeRejection::DataTypeMismatch { expected, actual })
                }
                Option::None if strict => Result::Err(TypeRejection::MissingHeader {
                    header: "Data_Type_Id",
                }),
                _ => Result::Ok(()),
            }
        }
        Option::None => {
            if strict
                && (read_body_codec(headers)
                    .map_err(malformed_codec_)?
                    .is_some()
                    || read_data_type_id(headers)
                        .map_err(malformed_data_type_id_)?
                        .is_some())
            {
                return Result::Err(TypeRejection::UnexpectedDataHeader {
                    header: "Body_Type / Data_Type_Id",
                });
            }
            Result::Ok(())
        }
    }
}

/// 校验 `Body_Type`。
fn validate_codec_(
    headers: &Headers,
    info: &RpcDataTypeInfo,
    strict: bool,
    required: bool,
) -> Result<(), TypeRejection> {
    match read_body_codec(headers).map_err(malformed_codec_)? {
        Option::Some(actual) if actual != info.body_codec() => {
            Result::Err(TypeRejection::CodecMismatch {
                expected: info.body_codec(),
                actual,
            })
        }
        Option::Some(_) => Result::Ok(()),
        Option::None if strict && required => Result::Err(TypeRejection::MissingHeader {
            header: "Body_Type",
        }),
        Option::None => Result::Ok(()),
    }
}

/// 校验可选的 `Accept`（Call / Push 的响应期望）。
fn validate_optional_accept_(
    headers: &Headers,
    info: &RpcDataTypeInfo,
) -> Result<(), TypeRejection> {
    let Some(expected) = info.accept() else {
        return Result::Ok(());
    };
    match read_accept(headers).map_err(malformed_accept_)? {
        Option::Some(actual) => compare_accept_(expected, actual),
        Option::None => Result::Ok(()),
    }
}

/// 比较 `Accept`：codec 必须一致；期望有 type-id 时实际也必须有且相等。
fn compare_accept_(expected: AcceptType, actual: AcceptType) -> Result<(), TypeRejection> {
    if expected.codec() != actual.codec() {
        return Result::Err(TypeRejection::AcceptMismatch { expected, actual });
    }
    match (expected.type_id(), actual.type_id()) {
        (Option::Some(expected_id), Option::Some(actual_id)) if expected_id != actual_id => {
            Result::Err(TypeRejection::AcceptMismatch { expected, actual })
        }
        (Option::Some(_), Option::None) => {
            Result::Err(TypeRejection::AcceptMismatch { expected, actual })
        }
        _ => Result::Ok(()),
    }
}

/// `Data_Type_Id` 解析错误 → 拒绝原因。
fn malformed_data_type_id_(err: TypeHeaderError) -> TypeRejection {
    TypeRejection::MalformedHeader {
        header: "Data_Type_Id",
        detail: err.to_string(),
    }
}

/// `Body_Type` 解析错误 → 拒绝原因。
fn malformed_codec_(err: TypeHeaderError) -> TypeRejection {
    TypeRejection::MalformedHeader {
        header: "Body_Type",
        detail: err.to_string(),
    }
}

/// `Accept` 解析错误 → 拒绝原因。
fn malformed_accept_(err: TypeHeaderError) -> TypeRejection {
    TypeRejection::MalformedHeader {
        header: "Accept",
        detail: err.to_string(),
    }
}

/// 把元信息写进请求头（客户端 / 服务端共用的「本方向」写法）。
///
/// 只写方向表允许的头；不会写对端方向的 `Accept`（由调用者显式写）。
pub fn apply_request_type_headers(
    headers: &mut Headers,
    method: AccessMethod,
    info: &RpcDataTypeInfo,
) {
    match method {
        AccessMethod::Call | AccessMethod::Post => {
            if let Option::Some(id) = info.request_body() {
                write_body_codec(headers, info.body_codec());
                write_data_type_id(headers, id);
            }
        }
        AccessMethod::Pull => {
            // Pull 请求没有 body；`Body_Type` 描述的是响应 suffix 的 codec。
            write_body_codec(headers, info.body_codec());
        }
        AccessMethod::Push => {
            write_body_codec(headers, info.body_codec());
            if let Option::Some(id) = info.request_suffix() {
                write_data_type_id(headers, id);
            }
        }
        AccessMethod::Head | AccessMethod::View | AccessMethod::Drop => {}
    }
}

/// 把元信息写进响应头（服务端「本方向」的写法）。
pub fn apply_response_type_headers(
    headers: &mut Headers,
    method: AccessMethod,
    info: &RpcDataTypeInfo,
) {
    match method {
        AccessMethod::Call | AccessMethod::Post | AccessMethod::Push | AccessMethod::View => {
            if let Option::Some(id) = info.response_body() {
                write_body_codec(headers, info.body_codec());
                write_data_type_id(headers, id);
            }
        }
        AccessMethod::Pull => {
            // Pull 响应一定是 suffix 流：codec + item 类型都必须写。
            write_body_codec(headers, info.body_codec());
            if let Option::Some(id) = info.response_suffix() {
                write_data_type_id(headers, id);
            }
        }
        AccessMethod::Head | AccessMethod::Drop => {}
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 `DataTypeId` 的十六进制 / 十进制往返与零值拒绝。
    /// Test the hex/decimal round-trip of `DataTypeId` and its zero rejection.
    ///
    /// - 手段：构造 `0x1234`，检查 `Display`、十进制解析、十六进制解析。
    /// - 判断：三者相等；`DataTypeId::new(0)` 为 `None`。
    ///
    /// - Means: build `0x1234`, then check display, decimal parse and hex parse.
    /// - Verdict: all three agree; `DataTypeId::new(0)` is `None`.
    #[test]
    fn data_type_id_roundtrip_() {
        let id = DataTypeId::new(0x1234u64).expect("非零 ID");
        assert_eq!(id.to_string(), "0x1234");
        assert_eq!("0x1234".parse::<DataTypeId>(), Result::Ok(id));
        assert_eq!("4660".parse::<DataTypeId>(), Result::Ok(id));
        assert_eq!(DataTypeId::new(0u64), Option::None);
        assert!("0".parse::<DataTypeId>().is_err());
        assert!("not-a-number".parse::<DataTypeId>().is_err());
    }

    /// 测试 `AcceptType` 的解析 / 格式化往返与错误边界。
    /// Test `AcceptType` parse/format round-trips and its error boundaries.
    ///
    /// - 手段：分别构造带 type-id、不带 type-id 的 Accept，格式化后再解析；再解析
    ///   未知 MIME、重复 type-id、未知参数。
    /// - 判断：两个往返都相等；三个非法输入都返回错误。
    ///
    /// - Means: round-trip Accept with and without a type-id; then parse an unknown MIME,
    ///   a duplicate `type-id` and an unknown parameter.
    /// - Verdict: both round-trips match; all three malformed inputs fail.
    #[test]
    fn accept_type_roundtrip_() {
        let with_id = AcceptType::new(
            BodyCodec::MsgPack,
            Option::Some(DataTypeId::new(0xabcu64).unwrap()),
        );
        assert_eq!(
            with_id.to_string(),
            "application/x-mptp-msgpack; type-id=0xabc"
        );
        assert_eq!(AcceptType::parse(&with_id.to_string()), Result::Ok(with_id));

        let codec_only = AcceptType::codec_only(BodyCodec::Json);
        assert_eq!(codec_only.to_string(), "application/json");
        assert_eq!(
            AcceptType::parse(&codec_only.to_string()),
            Result::Ok(codec_only)
        );

        assert!(AcceptType::parse("text/plain").is_err());
        assert!(AcceptType::parse("application/json; type-id=1; type-id=2").is_err());
        assert!(AcceptType::parse("application/json; foo=1").is_err());
        assert!(AcceptType::parse("application/json; type-id=0").is_err());
    }

    /// 测试三个标准头的读写往返。
    /// Test the read/write round-trip of the three standard headers.
    ///
    /// - 手段：往 `Headers` 写 MsgPack、`0x55`、Accept，再逐项读回。
    /// - 判断：读回值与原值完全相等；缺失头读出 `None`。
    ///
    /// - Means: write MsgPack, `0x55` and an Accept into a `Headers`, then read each back.
    /// - Verdict: every read matches; absent headers read as `None`.
    #[test]
    fn headers_roundtrip_() {
        let mut headers = Headers::new();
        assert_eq!(read_body_codec(&headers), Result::Ok(Option::None));
        assert_eq!(read_data_type_id(&headers), Result::Ok(Option::None));
        assert_eq!(read_accept(&headers), Result::Ok(Option::None));

        write_body_codec(&mut headers, BodyCodec::MsgPack);
        let id = DataTypeId::new(0x55u64).unwrap();
        write_data_type_id(&mut headers, id);
        let accept = AcceptType::new(BodyCodec::MsgPack, Option::Some(id));
        write_accept(&mut headers, accept);

        assert_eq!(
            read_body_codec(&headers),
            Result::Ok(Option::Some(BodyCodec::MsgPack))
        );
        assert_eq!(read_data_type_id(&headers), Result::Ok(Option::Some(id)));
        assert_eq!(read_accept(&headers), Result::Ok(Option::Some(accept)));
        assert_eq!(
            headers
                .try_get_val(StdHeaderKey::Data_Type_Id)
                .and_then(|v| v.as_text()),
            Option::Some("0x55")
        );
    }

    /// 测试 Pull 请求的严格校验：正确头通过、缺 Accept 为 400、类型不匹配为 406。
    /// Test strict Pull request validation: correct headers pass, missing Accept is 400,
    /// mismatched ids are 406.
    ///
    /// - 手段：构造响应 suffix 类型为 `0x11` 的 Pull 元信息，分别写正确 / 缺失 /
    ///   错误 type-id 的 Accept。
    /// - 判断：正确时 `Ok`；缺失时 `MissingHeader` 且状态 400；错误时
    ///   `AcceptMismatch` 且状态 406。
    ///
    /// - Means: build Pull info with response suffix `0x11`, then write correct / missing /
    ///   wrong `Accept` headers.
    /// - Verdict: correct is `Ok`; missing is `MissingHeader` (400); wrong is
    ///   `AcceptMismatch` (406).
    #[test]
    fn validate_pull_request_() {
        let id = DataTypeId::new(0x11u64).unwrap();
        let info = RpcDataTypeInfo::new(BodyCodec::MsgPack)
            .with_response_suffix(id)
            .with_accept(AcceptType::new(BodyCodec::MsgPack, Option::Some(id)));

        let mut ok = Headers::new();
        write_body_codec(&mut ok, BodyCodec::MsgPack);
        write_accept(
            &mut ok,
            AcceptType::new(BodyCodec::MsgPack, Option::Some(id)),
        );
        assert_eq!(
            validate_request_types(AccessMethod::Pull, &ok, &info),
            Result::Ok(())
        );

        let mut missing = Headers::new();
        write_body_codec(&mut missing, BodyCodec::MsgPack);
        assert_eq!(
            validate_request_types(AccessMethod::Pull, &missing, &info),
            Result::Err(TypeRejection::MissingHeader { header: "Accept" })
        );
        assert_eq!(
            validate_request_types(AccessMethod::Pull, &missing, &info)
                .unwrap_err()
                .status(),
            Status::BadRequest
        );

        let mut wrong = Headers::new();
        write_body_codec(&mut wrong, BodyCodec::MsgPack);
        write_accept(
            &mut wrong,
            AcceptType::new(
                BodyCodec::MsgPack,
                Option::Some(DataTypeId::new(0x12u64).unwrap()),
            ),
        );
        assert!(matches!(
            validate_request_types(AccessMethod::Pull, &wrong, &info),
            Result::Err(TypeRejection::AcceptMismatch { .. })
        ));
        assert_eq!(
            validate_request_types(AccessMethod::Pull, &wrong, &info)
                .unwrap_err()
                .status(),
            Status::NotAcceptable
        );
    }

    /// 测试 Push 请求的类型校验与 `TypePolicy::Optional` 的放宽边界。
    /// Test Push request validation and the exact relaxations of `TypePolicy::Optional`.
    ///
    /// - 手段：Push 元信息声明请求 suffix `0x21`；先给正确头，再给缺 `Data_Type_Id`，
    ///   再给错误 `Data_Type_Id`，最后用 `Optional` 重试缺头与错误头。
    /// - 判断：正确 `Ok`；缺头 400；错误头 406；`Optional` 下缺头 `Ok`、错误头仍是 406。
    ///
    /// - Means: Push info declares request suffix `0x21`; try correct, missing id, wrong id,
    ///   then repeat missing/wrong under `Optional`.
    /// - Verdict: correct passes; missing is 400; wrong is 406; under `Optional` missing
    ///   passes but wrong still fails with 406.
    #[test]
    fn validate_push_request_and_policy_() {
        let id = DataTypeId::new(0x21u64).unwrap();
        let strict = RpcDataTypeInfo::new(BodyCodec::MsgPack).with_request_suffix(id);

        let mut ok = Headers::new();
        write_body_codec(&mut ok, BodyCodec::MsgPack);
        write_data_type_id(&mut ok, id);
        assert_eq!(
            validate_request_types(AccessMethod::Push, &ok, &strict),
            Result::Ok(())
        );

        let mut missing = Headers::new();
        write_body_codec(&mut missing, BodyCodec::MsgPack);
        assert_eq!(
            validate_request_types(AccessMethod::Push, &missing, &strict),
            Result::Err(TypeRejection::MissingHeader {
                header: "Data_Type_Id"
            })
        );

        let mut wrong = Headers::new();
        write_body_codec(&mut wrong, BodyCodec::MsgPack);
        write_data_type_id(&mut wrong, DataTypeId::new(0x22u64).unwrap());
        assert!(matches!(
            validate_request_types(AccessMethod::Push, &wrong, &strict),
            Result::Err(TypeRejection::DataTypeMismatch { .. })
        ));

        let optional = strict.with_policy(TypePolicy::Optional);
        assert_eq!(
            validate_request_types(AccessMethod::Push, &missing, &optional),
            Result::Ok(())
        );
        assert!(matches!(
            validate_request_types(AccessMethod::Push, &wrong, &optional),
            Result::Err(TypeRejection::DataTypeMismatch { .. })
        ));
    }

    /// 测试 Call 请求的 codec 不匹配返回 406，且无 body 方向的多余类型头返回 400。
    /// Test that a Call codec mismatch returns 406 and an extra type header on a body-less
    /// direction returns 400.
    ///
    /// - 手段：Call 元信息声明请求 body `0x31` + MsgPack；写 JSON 的 `Body_Type` 与同一
    ///   类型 ID；再对无 body 的 Call 只写 `Data_Type_Id`。
    /// - 判断：前者为 `CodecMismatch` / 406；后者为 `UnexpectedDataHeader` / 400。
    ///
    /// - Means: declare request body `0x31` + MsgPack; write JSON `Body_Type` with the same
    ///   id; then write only a `Data_Type_Id` for a body-less Call.
    /// - Verdict: the former is `CodecMismatch` / 406; the latter is
    ///   `UnexpectedDataHeader` / 400.
    #[test]
    fn validate_call_direction_() {
        let id = DataTypeId::new(0x31u64).unwrap();
        let info = RpcDataTypeInfo::new(BodyCodec::MsgPack).with_request_body(id);
        let mut headers = Headers::new();
        write_body_codec(&mut headers, BodyCodec::Json);
        write_data_type_id(&mut headers, id);
        let rejection = validate_request_types(AccessMethod::Call, &headers, &info).unwrap_err();
        assert!(matches!(rejection, TypeRejection::CodecMismatch { .. }));
        assert_eq!(rejection.status(), Status::NotAcceptable);

        let bodyless = RpcDataTypeInfo::new(BodyCodec::MsgPack);
        let mut headers = Headers::new();
        write_data_type_id(&mut headers, id);
        let rejection =
            validate_request_types(AccessMethod::Call, &headers, &bodyless).unwrap_err();
        assert!(matches!(
            rejection,
            TypeRejection::UnexpectedDataHeader { .. }
        ));
        assert_eq!(rejection.status(), Status::BadRequest);
    }
    /// 测试响应方向的类型校验：Pull 正确通过、缺 ID 为 400、item 类型不符为 406。
    /// Test response-direction validation: a correct Pull passes, a missing id is 400, and
    /// a wrong item type is 406.
    ///
    /// - 手段：构造 Pull 元信息（suffix item `0x41`），分别写正确、缺 `Data_Type_Id`、
    ///   错误 `Data_Type_Id` 的响应头。
    /// - 判断：正确 `Ok`；缺失为 `MissingHeader` / 400；错误为 `DataTypeMismatch` / 406。
    ///
    /// - Means: build Pull info with suffix item `0x41`, then write correct / missing /
    ///   wrong `Data_Type_Id` response headers.
    /// - Verdict: correct is `Ok`; missing is `MissingHeader` / 400; wrong is
    ///   `DataTypeMismatch` / 406.
    #[test]
    fn validate_pull_response_() {
        let id = DataTypeId::new(0x41u64).unwrap();
        let info = RpcDataTypeInfo::new(BodyCodec::MsgPack).with_response_suffix(id);

        let mut ok = Headers::new();
        write_body_codec(&mut ok, BodyCodec::MsgPack);
        write_data_type_id(&mut ok, id);
        assert_eq!(
            validate_response_types(AccessMethod::Pull, &ok, &info),
            Result::Ok(())
        );

        let mut missing = Headers::new();
        write_body_codec(&mut missing, BodyCodec::MsgPack);
        let rejection =
            validate_response_types(AccessMethod::Pull, &missing, &info).unwrap_err();
        assert!(matches!(rejection, TypeRejection::MissingHeader { .. }));
        assert_eq!(rejection.status(), Status::BadRequest);

        let mut wrong = Headers::new();
        write_body_codec(&mut wrong, BodyCodec::MsgPack);
        write_data_type_id(&mut wrong, DataTypeId::new(0x42u64).unwrap());
        let rejection = validate_response_types(AccessMethod::Pull, &wrong, &info).unwrap_err();
        assert!(matches!(rejection, TypeRejection::DataTypeMismatch { .. }));
        assert_eq!(rejection.status(), Status::NotAcceptable);
    }

    /// 测试 `DataTypeId::from_nonzero_u64` 在运行期保留非零值。
    /// Test that `DataTypeId::from_nonzero_u64` keeps a non-zero value at run time.
    ///
    /// - 手段：用 `0x1234` 构造并读回。
    /// - 判断：原值返回；零值路径由下面的 `should_panic` 与 `compile_fail` doctest 固定。
    ///
    /// - Means: build from `0x1234` and read it back.
    /// - Verdict: the value round-trips; the zero path is fixed by the `should_panic` test
    ///   below and by a `compile_fail` doctest.
    #[test]
    fn data_type_id_from_nonzero_u64_() {
        assert_eq!(DataTypeId::from_nonzero_u64(0x1234u64).into_inner(), 0x1234);
    }

    /// 测试 `DataTypeId::from_nonzero_u64(0)` 在运行期 panic。
    /// Test that `DataTypeId::from_nonzero_u64(0)` panics at run time.
    ///
    /// - 手段：直接以 `0` 构造，交给测试框架捕获 panic。
    /// - 判断：panic 消息包含「Data_Type_Id 不能为 0」。
    ///
    /// - Means: construct from `0` and let the test harness catch the panic.
    /// - Verdict: the panic message contains "Data_Type_Id 不能为 0".
    #[test]
    #[should_panic(expected = "Data_Type_Id 不能为 0")]
    fn data_type_id_from_nonzero_u64_rejects_zero_() {
        let _ = DataTypeId::from_nonzero_u64(0u64);
    }

    /// 测试手写 `TrDataTypeId` 路径仍然可用（feature 关闭时的旧行为）。
    /// Test that the hand-written `TrDataTypeId` path still works (the feature-off behavior).
    ///
    /// - 手段：为一个**没有** `TrHasTypeId` 的本地类型手写 impl，读常量。
    /// - 判断：常量等于手写的 `0x4321`；blanket impl 不会与它冲突。
    ///
    /// - Means: hand-write the impl for a local type that has no `TrHasTypeId`, then read it.
    /// - Verdict: the constant equals the hand-written `0x4321`; the blanket impl does not
    ///   conflict with it.
    #[test]
    fn manual_tr_data_type_id_still_works_() {
        struct ManualProbe;

        impl TrDataTypeId for ManualProbe {
            const DATA_TYPE_ID: DataTypeId = DataTypeId::from_nonzero_u64(0x4321u64);
        }

        assert_eq!(ManualProbe::DATA_TYPE_ID.into_inner(), 0x4321);
    }

    /// 测试 `type-id` 开启时 blanket impl 直接取 denco 的 `TYPE_ID_64`。
    /// Test that with `type-id` the blanket impl uses denco's `TYPE_ID_64` directly.
    ///
    /// - 手段：本地 `Probe` derive `TrHasTypeId`，比较 facade 常量与 `TYPE_ID_64`。
    /// - 判断：两者相等且非零；注册表把两个类型判为不冲突。
    ///
    /// - Means: derive `TrHasTypeId` for a local `Probe` and compare the facade constant
    ///   with `TYPE_ID_64`.
    /// - Verdict: they are equal and non-zero; the registry reports no conflict for two types.
    #[cfg(feature = "type-id")]
    #[test]
    fn blanket_impl_uses_type_id_64_() {
        #[derive(denco_macro::TrHasTypeId)]
        #[expect(dead_code, reason = "字段只参与类型指纹计算，测试不读取其值")]
        struct Probe {
            id: u64,
            name: String,
        }

        assert_eq!(
            Probe::DATA_TYPE_ID.into_inner(),
            <Probe as crate::x_deps::abs_denco::type_id::TrHasTypeId>::TYPE_ID_64
        );
        assert_ne!(Probe::DATA_TYPE_ID.into_inner(), 0);

        // 通过 `stream` 的 facade 使用注册表宏，验证转出路径也被编译。
        let entries = crate::stream::denco_type_ids![Probe, u64];
        assert_eq!(crate::stream::check_unique(entries, 64), Result::Ok(()));
    }
}
