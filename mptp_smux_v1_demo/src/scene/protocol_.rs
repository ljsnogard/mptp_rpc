//! 场景的业务协议：目录、行星描述、事件与 denco 编解码。
//!
//! 这里**只**定义线上业务契约，不接触 smux / MPTP 的会话逻辑：
//!
//! - URI 与头键常量；
//! - denco 类型（`PlanetDescriptor` / `PlanetEvent` / `PlanetDirectory` 等）；
//! - 把业务值编码成字节、从读端逐值解码。
//!
//! 后缀流是**裸字节**：每个事件用 denco 序列化成一个自描述值，接收端按值逐个解码。
//! 一个值可以拆成多个 chunk 写入，接收端看到的字节序列不变。
//!
//! # `protocol_`: the scene's wire contract
//!
//! Only the business contract lives here: URIs, header keys, denco types and codecs.
//! The suffix stream is raw bytes; every event is one self-describing denco value.

use std::sync::Arc;

use abs_buff::TrBuffRead;
use abs_cancel::NonCancellableToken;
use abs_denco::{de::TrDecodableOwned, ser::TrEncodable};
use mptp_wire::HeaderKey;
use mp_denco::{MpDecoder, MpEncoder};

/// 一颗行星的稳定标识。
pub type PlanetId = u64;

/// 一条连接的稳定标识。
pub type ClientId = u64;

/// 加入场景：当前连接创建自己的行星。
pub const K_SCENE_JOIN: &str = "/scene/join";

/// 修改自己的公转 / 自转速度。
pub const K_SCENE_UPDATE: &str = "/scene/update";

/// 行星目录：`View` 列出全部，`Head` 只返回数量与目录版本。
pub const K_SCENE_PLANETS: &str = "/scene/planets";

/// 显式退订提示：客户端丢弃 Pull 前的业务清理消息。
///
/// 当前 `mptp_smux` 没有「关闭回复读半边」的公开入口，服务端无法及时观察到客户端
/// 单纯丢弃 `Response`；这条消息是应用层的清理提示，见 dev-notes 的「API 缺口」。
pub const K_SCENE_UNSUBSCRIBE: &str = "/scene/unsubscribe";

/// 单颗行星数据流的 URI 前缀。
pub const K_PLANET_URI_PREFIX: &str = "/scene/planets/";

/// `Head /scene/planets` 的行星数量头（临时自定义键）。
pub const K_HDR_PLANET_COUNT: HeaderKey = HeaderKey::from_num(0x0090);

/// `Head /scene/planets` 的目录版本头（临时自定义键）。
pub const K_HDR_DIRECTORY_VERSION: HeaderKey = HeaderKey::from_num(0x0091);

/// 行星数量头的文本名。
pub const K_HDR_PLANET_COUNT_NAME: &str = "x-planet-count";

/// 目录版本头的文本名。
pub const K_HDR_DIRECTORY_VERSION_NAME: &str = "x-directory-version";

/// 一条行星数据流的 URI。
pub fn planet_uri_(planet_id: PlanetId) -> String {
    format!("{K_PLANET_URI_PREFIX}{planet_id}")
}

/// 从 URI 解析行星标识；不是行星流路径时返回 `None`。
pub fn parse_planet_uri_(uri: &str) -> Option<PlanetId> {
    uri.strip_prefix(K_PLANET_URI_PREFIX)?.parse().ok()
}

/// 行星的完整状态：目录项与快照 / 更新事件共用。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct PlanetDescriptor {
    /// 行星标识。
    pub id: PlanetId,

    /// 拥有者（创建它的连接）。
    pub owner: ClientId,

    /// 显示名。
    pub name: String,

    /// 展示颜色（客户端自己解释为 RGB）。
    pub color: u32,

    /// 该行星数据流的 URI。
    pub stream_uri: String,

    /// 公转速度（弧度 / 秒）。
    pub orbit_speed: f32,

    /// 自转速度（弧度 / 秒）。
    pub spin_speed: f32,

    /// 该行星状态的版本，每次更新递增。
    pub version: u64,
}

/// 目录：当前所有行星数据流，不建立订阅。
#[derive(Clone, Debug, Default, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct PlanetDirectory {
    /// 目录版本，加入 / 离开时递增。
    pub directory_version: u64,

    /// 全部行星。
    pub planets: Vec<PlanetDescriptor>,
}

/// `Call /scene/join` 的请求体。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct JoinRequest {
    /// 期望的显示名；空串表示由服务端生成。
    pub name: String,

    /// 期望的颜色。
    pub color: u32,
}

/// `Call /scene/update` 的请求体；连接即身份，不带 client id。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UpdateRequest {
    /// 新的公转速度。
    pub orbit_speed: f32,

    /// 新的自转速度。
    pub spin_speed: f32,
}

/// `Call /scene/update` 的回复体。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UpdateAck {
    /// 被更新的行星。
    pub planet_id: PlanetId,

    /// 更新后的状态版本。
    pub version: u64,

    /// 更新后的公转速度。
    pub orbit_speed: f32,

    /// 更新后的自转速度。
    pub spin_speed: f32,
}

/// `Call /scene/unsubscribe` 的请求体。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UnsubscribeRequest {
    /// 要退订的行星。
    pub planet_id: PlanetId,
}

/// `Call /scene/unsubscribe` 的回复体。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UnsubscribeAck {
    /// 本次移除的订阅数量。
    pub removed: u32,
}

/// 一条行星数据流上的事件。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub enum PlanetEvent {
    /// 订阅起点：当前完整状态。
    Snapshot(PlanetDescriptor),

    /// 参数变化后的完整状态。
    Updated(PlanetDescriptor),

    /// 行星被删除（拥有者离开）；随后流结束。
    Removed {
        /// 被删除的行星。
        planet_id: PlanetId,

        /// 目录版本。
        version: u64,
    },

    /// 空闲心跳；客户端忽略即可。
    ///
    /// 它的作用是让服务端在**没有业务更新**时也能通过「写失败」发现客户端已经丢弃
    /// `Pull` 会话，从而及时移除订阅者。心跳不是对象边界，也不是长度帧。
    Heartbeat {
        /// 服务端自增序号。
        sequence: u64,
    },
}

/// 编解码错误。
#[derive(Debug, thiserror::Error)]
pub enum SceneCodecError {
    /// 编码失败（含暂存区不足）。
    #[error("编码场景值失败：{0}")]
    Encode(String),

    /// 解码失败（含对端提前关闭）。
    #[error("解码场景值失败：{0}")]
    Decode(String),
}

/// 编码暂存区的容量：场景事件都很小，超出时应明确失败而不是悄悄截断。
const K_ENCODE_CAP_: usize = 64usize * 1024usize;

/// 把业务值编码成一段独立字节。
///
/// # Errors
///
/// 编码失败或超出暂存区容量时返回 [`SceneCodecError::Encode`]。
pub async fn encode_to_vec_async_<T>(value: &T) -> Result<Vec<u8>, SceneCodecError>
where
    T: ?Sized + TrEncodable,
{
    let mut storage = vec![0u8; K_ENCODE_CAP_];
    let written = {
        let mut sink: &mut [u8] = storage.as_mut_slice();
        let before = sink.len();
        match value
            .encode_async(MpEncoder::new(&mut sink))
            .may_cancel_with(NonCancellableToken::new())
            .await
        {
            Ok(()) => before - sink.len(),
            Err(err) => {
                return Err(SceneCodecError::Encode(format!(
                    "编码暂存区 {K_ENCODE_CAP_} 字节不足或编码失败：{err}"
                )));
            }
        }
    };
    storage.truncate(written);
    Ok(storage)
}

/// 从读端解出**一个**自描述值。
///
/// 后缀流是连续值序列：每次调用只消费一个值所需的字节，剩余字节留在读端，供下一次
/// 调用继续解码。chunk 怎么切分不影响结果。
///
/// # Errors
///
/// 读端结束、字节不合法或值不完整时返回 [`SceneCodecError::Decode`]。
pub async fn decode_value_async_<T, R>(rx: &mut R) -> Result<T, SceneCodecError>
where
    T: TrDecodableOwned,
    R: TrBuffRead<u8>,
{
    T::decode_async(MpDecoder::new(rx))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| SceneCodecError::Decode(err.to_string()))
}

/// 从一整段字节里解出一个值。
///
/// # Errors
///
/// 字节不合法或值不完整时返回 [`SceneCodecError::Decode`]。
pub async fn decode_slice_async_<T>(bytes: &[u8]) -> Result<T, SceneCodecError>
where
    T: TrDecodableOwned,
{
    let mut slice: &[u8] = bytes;
    decode_value_async_::<T, _>(&mut slice).await
}

/// 把一个事件编码成可广播的不可变字节。
///
/// # Errors
///
/// 编码失败时返回 [`SceneCodecError::Encode`]。
pub async fn encode_event_async_(event: &PlanetEvent) -> Result<Arc<[u8]>, SceneCodecError> {
    let bytes = encode_to_vec_async_(event).await?;
    Ok(Arc::from(bytes.into_boxed_slice()))
}

/// 把头值读成 `u64`（文本形态解析十进制，数字形态直接转换）。
pub fn header_u64_(headers: &mptp_wire::Headers, key: HeaderKey) -> Option<u64> {
    let value = headers.try_get(key)?;
    value
        .as_text()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .or_else(|| value.as_num().map(u64::from))
}
