//! demo 的**线上契约**：业务类型与它们的类型标识。
//!
//! # 类型标识不在这里手写
//!
//! 标识是**类型名**经 FNV-1a 算出的 64 位值（与 `mptp_core::codec::hash_str` 同一个算法）。
//! 两侧共享同一份声明，就共享了同一套标识——不需要谁去分配，也不需要在调用点硬写数字。
//!
//! 于是「这段字节是什么类型」是**类型自己的属性**：发送方设体时把标识写进 `Data_Type_Id`
//! 头，接收方据此校验再解码。demo 里因此看不到任何手写的标识常量。
//!
//! 代价是把契约绑在类型名上：改名即不兼容（真实 RPC 里的对应物是 IDL 生成的名字或编号）。
//!
//! # `protocol_`: the wire contract of this demo
//!
//! Type identifiers are the FNV-1a hash of the type name, exactly as in
//! `mptp_core::codec::hash_str`. Both ends share this one declaration, so they share the
//! identifiers: no allocation, no hand-written constants at the call sites.

use abs_denco::ser::TrEncodable;
use abs_mptp::spec::{Status, StdHeaderKey, StdHeaderVal, access_method::AccessMethod};
use anyhow::{Result, anyhow};
use mptp_smux::messaging::{Headers, RequestPrefix, ResponsePrefix};

use crate::body_::encode_to_vec_async_;

/// 回声路径。
pub const K_ECHO_PATH: &str = "/rpc/echo";

/// 求和路径。
pub const K_SUM_PATH: &str = "/rpc/sum";

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 类型标识
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// FNV-1a：与 `mptp_core::codec::hash_str` 逐字相同的算法。
pub const fn hash_str(s: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    let mut i = 0usize;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x00000100000001B3u64);
        i += 1;
    }
    hash
}

/// 把「类型名 → 稳定标识」写成一份声明。
macro_rules! impl_type_id_u64 {
    ($($t:ty),* $(,)?) => {
        $(
            impl TypeId64 for $t {
                const TYPE_ID: u64 = $crate::protocol_::hash_str(stringify!($t));
            }
        )*
    };
}

/// 「本类型在线上叫什么」的契约：一个 64 位标识。
pub trait TypeId64 {
    /// 本类型的稳定标识。
    const TYPE_ID: u64;
}

/// 取某个类型的线上标识。
pub fn type_id_of_<TyData>() -> u64
where
    TyData: TypeId64,
{
    TyData::TYPE_ID
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 载体类型
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 回声请求：把 `text` 原样带回来。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct EchoRequest {
    pub text: String,
}

/// 回声回复。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct EchoReply {
    pub text: String,
}

/// 求和请求。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct SumRequest {
    pub a: i64,
    pub b: i64,
}

/// 求和回复。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct SumReply {
    pub total: i64,
}

impl_type_id_u64!(EchoRequest, EchoReply, SumRequest, SumReply);

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// 报文装配
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 把类型标识写成头里的十六进制文本（`Data_Type_Id` 的取值形态）。
fn type_id_text_<TyData>() -> String
where
    TyData: TypeId64,
{
    format!("{:#x}", type_id_of_::<TyData>())
}

/// 造一条**带定长体**的请求：前缀 + 已编好的字节。
///
/// 体一律按 MessagePack 编码、按 `Body_Size` 声明定长。长度写成**文本**形态，便于人读；
/// `mptp_smux` 的默认体边界判定同时接受文本与数字两种形态。
///
/// # Errors
///
/// 体编码失败（含超出暂存区容量）时返回错误。
pub async fn request_with_<TyData>(
    method: AccessMethod,
    path: &str,
    value: &TyData,
) -> Result<(RequestPrefix, Vec<u8>)>
where
    TyData: TrEncodable + TypeId64,
{
    let body = encode_to_vec_async_(value).await?;
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    headers.set_text(StdHeaderKey::Data_Type_Id, type_id_text_::<TyData>());
    Result::Ok((
        RequestPrefix::with_headers(method, path, Option::Some(headers)),
        body,
    ))
}

/// 造一条**带定长体**的回复：前缀 + 已编好的字节。
///
/// # Errors
///
/// 体编码失败（含超出暂存区容量）时返回错误。
pub async fn response_with_<TyData>(
    status: Status,
    value: &TyData,
) -> Result<(ResponsePrefix, Vec<u8>)>
where
    TyData: TrEncodable + TypeId64,
{
    let body = encode_to_vec_async_(value).await?;
    let mut headers = Headers::new();
    headers.set_text(StdHeaderKey::Body_Size, body.len().to_string());
    headers.set_num(
        StdHeaderKey::Body_Type,
        StdHeaderVal::Mime_Body_Type_MsgPack.into_inner(),
    );
    headers.set_text(StdHeaderKey::Data_Type_Id, type_id_text_::<TyData>());
    Result::Ok((
        ResponsePrefix::with_headers(status, Option::Some(headers)),
        body,
    ))
}

/// 造一条**不带体**的回复（例如「你声明的类型不是我要的」）。
pub fn response_empty_(status: Status) -> (ResponsePrefix, Vec<u8>) {
    (ResponsePrefix::new(status), Vec::new())
}

/// 校验头里声明的类型标识确实是本端期待的类型。
///
/// # Errors
///
/// 没有声明、声明的不是合法数值、或与期待不符时返回错误——三种情况都明确失败，不猜。
pub fn expect_declared_<TyExpect>(headers: Option<&Headers>, what: &str) -> Result<()>
where
    TyExpect: TypeId64,
{
    let expected = type_id_of_::<TyExpect>();
    let Option::Some(headers) = headers else {
        return Err(anyhow!("{what} 没有头表，也就没有 Data_Type_Id"));
    };
    match headers.data_type_id() {
        Option::Some(actual) if actual == expected => Result::Ok(()),
        Option::Some(actual) => Err(anyhow!(
            "{what} 声明 {actual:#x}，本端期待 {expected:#x}——两端契约不一致"
        )),
        Option::None => Err(anyhow!(
            "{what} 没有声明 Data_Type_Id，无法确认它是什么类型"
        )),
    }
}

#[cfg(test)]
mod tests_ {
    //! 类型标识的两个性质：**稳定**与**因类型而异**。
    //!
    //! 标识是跨进程契约，但它的两条性质在本进程内就能钉住：同一个名字必须永远算出同一个
    //! 值（否则两端对不上），不同名字必须算出不同的值（否则类型校验形同虚设）。
    //!
    //! Two properties of the type identifier are checkable in-process: the same name must
    //! always hash to the same value, and different names must hash to different values.

    use super::*;

    /// 测试类型标识对同一份声明是稳定的（多次求值同一个数）。
    /// Test that the identifier is stable for one declaration.
    ///
    /// - 手段：对 `EchoRequest` 连续取三次标识，并与直接哈希它的类型名做比较。
    /// - 判断：四个值全等——`TYPE_ID` 是常量折叠出来的，不是每次运行随机生成。
    ///
    /// - Means: read the identifier of `EchoRequest` three times and compare it against
    ///   hashing the type name directly.
    /// - Verdict: all four values are equal — `TYPE_ID` is a constant fold, not something
    ///   generated per run.
    #[test]
    fn type_id_is_stable_() {
        let first = type_id_of_::<EchoRequest>();
        assert_eq!(first, type_id_of_::<EchoRequest>());
        assert_eq!(first, type_id_of_::<EchoRequest>());
        assert_eq!(first, hash_str(stringify!(EchoRequest)));
    }

    /// 测试四个业务类型的标识两两不同。
    /// Test that the four business types all hash to distinct identifiers.
    ///
    /// - 手段：把四个标识收进一个集合。
    /// - 判断：集合大小为 4。若有两个类型撞了标识，「按标识校验类型」就退化成「按标识
    ///   猜类型」，那正是这套机制要防的事。
    ///
    /// - Means: collect the four identifiers into a set.
    /// - Verdict: the set holds four elements. A collision would turn "verify the type by its
    ///   identifier" into "guess the type by its identifier", which is exactly what this
    ///   mechanism exists to prevent.
    #[test]
    fn type_ids_are_distinct_() {
        let mut ids = std::collections::BTreeSet::new();
        ids.insert(type_id_of_::<EchoRequest>());
        ids.insert(type_id_of_::<EchoReply>());
        ids.insert(type_id_of_::<SumRequest>());
        ids.insert(type_id_of_::<SumReply>());
        assert_eq!(ids.len(), 4usize);
    }
}
