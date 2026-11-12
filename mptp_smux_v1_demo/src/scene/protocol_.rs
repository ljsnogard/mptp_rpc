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
use mp_denco::{MpDecoder, MpEncoder};
use mptp_wire::HeaderKey;

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

/// 公转速度下限（弧度 / 秒）。
pub const K_ORBIT_SPEED_MIN: f32 = 0.0f32;

/// 公转速度上限（弧度 / 秒）。
pub const K_ORBIT_SPEED_MAX: f32 = 3.0f32;

/// 自转速度下限（弧度 / 秒）。
pub const K_SPIN_SPEED_MIN: f32 = 0.0f32;

/// 自转速度上限（弧度 / 秒）。
pub const K_SPIN_SPEED_MAX: f32 = 6.0f32;

/// 轨道半径下限（世界单位）。太阳半径 2.2 + 行星最大半径 1.6 仍小于 4.0。
pub const K_ORBIT_RADIUS_MIN: f32 = 4.0f32;

/// 轨道半径上限（世界单位）。
pub const K_ORBIT_RADIUS_MAX: f32 = 30.0f32;

/// 行星自身半径下限（世界单位）。
pub const K_PLANET_RADIUS_MIN: f32 = 0.2f32;

/// 行星自身半径上限（世界单位）；与太阳半径相加仍小于轨道半径下限。
pub const K_PLANET_RADIUS_MAX: f32 = 1.6f32;

/// 缺省公转速度（弧度 / 秒）。
pub const K_DEFAULT_ORBIT_SPEED: f32 = 0.6f32;

/// 缺省自转速度（弧度 / 秒）。
pub const K_DEFAULT_SPIN_SPEED: f32 = 1.2f32;

/// 缺省轨道半径（世界单位）；实际分配时还会按行星标识错开。
pub const K_DEFAULT_ORBIT_RADIUS: f32 = 6.0f32;

/// 缺省行星半径（世界单位）。
pub const K_DEFAULT_PLANET_RADIUS: f32 = 0.8f32;

/// 行星表面纹理主题。
///
/// **线上是开放的 `u32`**（见 [`PlanetParams::texture_theme`]），这里只列出当前客户端
/// 认识的主题。未知编号由客户端回退到 [`PlanetTheme::Terran`]，这样服务端 / 旧客户端
/// 不会因为新增主题而无法解码。主题只影响客户端渲染，服务端只负责原样存储和广播。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PlanetTheme {
    /// 类地：海洋、大陆、云带与极冠。
    #[default]
    Terran,

    /// 海洋：更高的海平面、更多云。
    Ocean,

    /// 沙漠：干燥地表与沙丘条带。
    Desert,

    /// 冰封：高反照率的冰原与裂纹。
    Ice,

    /// 熔岩：暗色岩壳与高温裂缝。
    Lava,

    /// 气态：明显的纬向条带。
    Gas,
}

impl PlanetTheme {
    /// 当前客户端支持的全部主题，顺序用于 UI 选择器。
    pub const ALL: [PlanetTheme; 6] = [
        PlanetTheme::Terran,
        PlanetTheme::Ocean,
        PlanetTheme::Desert,
        PlanetTheme::Ice,
        PlanetTheme::Lava,
        PlanetTheme::Gas,
    ];

    /// 转成线上编号。
    pub const fn wire_(self) -> u32 {
        match self {
            PlanetTheme::Terran => 0u32,
            PlanetTheme::Ocean => 1u32,
            PlanetTheme::Desert => 2u32,
            PlanetTheme::Ice => 3u32,
            PlanetTheme::Lava => 4u32,
            PlanetTheme::Gas => 5u32,
        }
    }

    /// 从线上编号解析；未知编号回退到 [`PlanetTheme::Terran`]。
    pub const fn from_wire_(wire: u32) -> PlanetTheme {
        match wire {
            1u32 => PlanetTheme::Ocean,
            2u32 => PlanetTheme::Desert,
            3u32 => PlanetTheme::Ice,
            4u32 => PlanetTheme::Lava,
            5u32 => PlanetTheme::Gas,
            _ => PlanetTheme::Terran,
        }
    }
}

/// 行星的可变业务参数：拥有者可以实时修改，随 [`PlanetDescriptor`] 一起广播。
///
/// 把可变参数集中在一个值对象里，是为了让后续新增旋钮只改这里，而不必同时改
/// `PlanetDescriptor`、`UpdateRequest`、`UpdateAck` 与每个调用点。新增字段时：
///
/// - 服务端 `PlanetParams::default` / [`Self::sanitized_`] 给出缺省值与支持范围；
/// - 客户端 UI 增加对应控件；
/// - `mptp_smux` 的传输层完全不需要改。
#[derive(Clone, Copy, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct PlanetParams {
    /// 公转速度（弧度 / 秒）。
    pub orbit_speed: f32,

    /// 自转速度（弧度 / 秒）。
    pub spin_speed: f32,

    /// 轨道半径（世界单位，行星中心到太阳中心的距离）。
    pub orbit_radius: f32,

    /// 行星自身半径（世界单位）。
    pub planet_radius: f32,

    /// 展示颜色，低 24 位是 RGB。
    pub color: u32,

    /// 表面纹理主题；线上是开放编号，见 [`PlanetTheme`]。
    pub texture_theme: u32,
}

impl Default for PlanetParams {
    fn default() -> Self {
        PlanetParams {
            orbit_speed: K_DEFAULT_ORBIT_SPEED,
            spin_speed: K_DEFAULT_SPIN_SPEED,
            orbit_radius: K_DEFAULT_ORBIT_RADIUS,
            planet_radius: K_DEFAULT_PLANET_RADIUS,
            color: 0u32,
            texture_theme: PlanetTheme::Terran.wire_(),
        }
    }
}

impl PlanetParams {
    /// 把连续量夹紧到服务端支持的范围内，并清洗非有限值与颜色高位。
    ///
    /// 离散量（纹理主题）不做夹紧：线上编号是开放集合，服务端不解释主题语义。
    ///
    /// # Examples
    ///
    /// ```
    /// # use mptp_smux_v1_demo::scene::PlanetParams;
    /// let params = PlanetParams {
    ///     orbit_radius: 999.0f32,
    ///     planet_radius: -1.0f32,
    ///     ..Default::default()
    /// }.sanitized_();
    /// assert!(params.orbit_radius <= 30.0f32);
    /// assert_eq!(params.planet_radius, 0.2f32);
    /// ```
    pub fn sanitized_(self) -> PlanetParams {
        PlanetParams {
            orbit_speed: clamp_finite_(
                self.orbit_speed,
                K_ORBIT_SPEED_MIN,
                K_ORBIT_SPEED_MAX,
                K_DEFAULT_ORBIT_SPEED,
            ),
            spin_speed: clamp_finite_(
                self.spin_speed,
                K_SPIN_SPEED_MIN,
                K_SPIN_SPEED_MAX,
                K_DEFAULT_SPIN_SPEED,
            ),
            orbit_radius: clamp_finite_(
                self.orbit_radius,
                K_ORBIT_RADIUS_MIN,
                K_ORBIT_RADIUS_MAX,
                K_DEFAULT_ORBIT_RADIUS,
            ),
            planet_radius: clamp_finite_(
                self.planet_radius,
                K_PLANET_RADIUS_MIN,
                K_PLANET_RADIUS_MAX,
                K_DEFAULT_PLANET_RADIUS,
            ),
            color: self.color & 0x00FF_FFFFu32,
            texture_theme: self.texture_theme,
        }
    }
}

/// 非有限值回退到 `fallback`，有限值夹紧到 `min..=max`。
fn clamp_finite_(value: f32, min: f32, max: f32, fallback: f32) -> f32 {
    if value.is_finite() {
        value.clamp(min, max)
    } else {
        fallback
    }
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

    /// 该行星数据流的 URI。
    pub stream_uri: String,

    /// 可变业务参数。
    pub params: PlanetParams,

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
///
/// 请求携带**完整**的 [`PlanetParams`]，不是字段补丁。新增参数时只需扩展
/// `PlanetParams` 的默认值与 UI，不需要改 `PlanetEvent` / `UpdateRequest` /
/// `UpdateAck` 的包装结构；但 denco 是结构化编码，旧客户端仍需重新构建才能编码新字段，
/// 不承诺跨版本的旧请求体兼容。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UpdateRequest {
    /// 新的完整参数。
    pub params: PlanetParams,
}

/// `Call /scene/update` 的回复体。
#[derive(Clone, Debug, PartialEq, denco_macro::TrDecodable, denco_macro::TrEncodable)]
pub struct UpdateAck {
    /// 被更新的行星。
    pub planet_id: PlanetId,

    /// 更新后的状态版本。
    pub version: u64,

    /// 服务端清洗后实际生效的完整参数。
    pub params: PlanetParams,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试参数清洗会把连续量夹到支持范围，并对非有限值回退到默认值。
    /// Test that sanitizing clamps continuous values and falls back for non-finite ones.
    ///
    /// - 手段：构造一个速度 / 半径越界或非有限、颜色带高位的 `PlanetParams`。
    /// - 判断：速度与半径落在公开范围内；`NaN` / 无穷回退到缺省值；颜色只剩低 24 位；
    ///   纹理主题作为开放编号不被改写。
    ///
    /// - Means: build a `PlanetParams` whose speeds / radii are out of range or non-finite,
    ///   with extra high bits in the color.
    /// - Verdict: speeds and radii stay within the public range, non-finite values fall back
    ///   to defaults, color keeps only its low 24 bits, and the open theme id is untouched.
    #[test]
    fn sanitized_params_respect_supported_ranges_() {
        let wild = PlanetParams {
            orbit_speed: f32::NAN,
            spin_speed: 99.0f32,
            orbit_radius: -5.0f32,
            planet_radius: f32::INFINITY,
            color: 0xAB12_3456u32,
            texture_theme: 99u32,
        };
        let clean = wild.sanitized_();

        assert_eq!(clean.orbit_speed, K_DEFAULT_ORBIT_SPEED);
        assert_eq!(clean.spin_speed, K_SPIN_SPEED_MAX);
        assert_eq!(clean.orbit_radius, K_ORBIT_RADIUS_MIN);
        assert_eq!(clean.planet_radius, K_DEFAULT_PLANET_RADIUS);
        assert_eq!(clean.color, 0x0012_3456u32);
        assert_eq!(clean.texture_theme, 99u32);
    }

    /// 测试纹理主题的线上编号往返，并验证未知编号回退到类地主题。
    /// Test the texture-theme wire round trip and the unknown-id fallback.
    ///
    /// - 手段：遍历 `PlanetTheme::ALL` 做 `from_wire_(wire_())`，再解析一个未知编号。
    /// - 判断：已知主题全部还原；未知编号得到 `Terran`。
    ///
    /// - Means: round-trip every theme in `PlanetTheme::ALL` and parse one unknown id.
    /// - Verdict: known themes round-trip and an unknown id yields `Terran`.
    #[test]
    fn theme_wire_roundtrip_() {
        for theme in PlanetTheme::ALL {
            assert_eq!(PlanetTheme::from_wire_(theme.wire_()), theme);
        }
        assert_eq!(
            PlanetTheme::from_wire_(999u32),
            PlanetTheme::Terran,
            "未知主题应回退到类地主题"
        );
    }
}
