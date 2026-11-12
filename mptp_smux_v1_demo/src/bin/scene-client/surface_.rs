//! 行星表面程序化纹理与拍摄几何。
//!
//! demo 不携带图片资产，行星表面纹理在启动 / 新行星出现时按行星标识生成：
//! 多倍频值噪声形成大陆与海洋，再叠加极冠、云带和细微颗粒。纹理是等距柱状
//! （equirectangular）投影，正好适配 Bevy `Sphere::mesh().uv(..)` 的 UV；经度方向
//! 的噪声采样首尾环绕，避免接缝。
//!
//! 自转只有在表面有可辨认特征时才看得出来，所以 `planet_surface_image` 与
//! `surface_camera_transform_` 一起解决「行星看不出在转」的问题。
//!
//! # Procedural planet surface
//!
//! Textures are generated from a deterministic multi-octave value noise (continents,
//! oceans, polar caps, cloud bands) so planet rotation is visible. The longitude axis
//! wraps, matching the UV sphere's seam.

use bevy::{
    asset::RenderAssetUsages,
    image::Image,
    math::ops,
    prelude::{Transform, Vec3},
    render::render_resource::{Extent3d, TextureDimension, TextureFormat},
};

/// 纹理宽度（经度方向）。
const K_TEXTURE_WIDTH: u32 = 512;

/// 纹理高度（纬度方向）。
const K_TEXTURE_HEIGHT: u32 = 256;

/// 地表视角在最低高度时的最小俯角（弧度）。
const K_SURFACE_MIN_PITCH: f32 = 0.08f32;

/// 地表视角的最大俯角（接近正下方，弧度）。
const K_SURFACE_MAX_PITCH: f32 = 1.30f32;

/// 为某颗行星生成表面纹理。
///
/// `planet_id` 用于确定噪声种子；`base` 是行星主色的 sRGB 分量。
pub(crate) fn planet_surface_image(planet_id: u64, base: [f32; 3]) -> Image {
    let seed = (planet_id as u32)
        .wrapping_mul(0x9E37_79B9)
        .wrapping_add(0x5A17_3C2D);
    surface_image_(seed, base, K_TEXTURE_WIDTH, K_TEXTURE_HEIGHT)
}

/// 从行星表面的某一点朝切向看出去，构造「地表视角」相机变换。
///
/// 相机挂在行星的局部坐标系里（观察点是北纬约 35° 的球面锚点，沿局部 `+X` 切向
/// 并向下俯视），因此行星自转时相机会跟着转，太阳与其他行星会扫过天空。
/// `altitude` 以行星半径为单位。
pub(crate) fn surface_camera_transform_(planet: &Transform, altitude: f32) -> Transform {
    let altitude = altitude.max(0.0f32);
    let radius = 1.0f32 + altitude;
    let anchor = surface_anchor_();
    let eye_local = anchor * radius;
    // 高度越高，地平线越低；俯角跟着地平线下沉角走，保证地面始终在画面里。
    let horizon_dip = (1.0f32 / radius).acos();
    let pitch = (horizon_dip * 0.92f32).clamp(K_SURFACE_MIN_PITCH, K_SURFACE_MAX_PITCH);
    let look_local = Vec3::X * pitch.cos() - anchor * pitch.sin();
    let eye = planet.transform_point(eye_local);
    let ahead = planet.transform_point(eye_local + look_local);
    let up = (planet.rotation * anchor).normalize_or_zero();
    let up = if up == Vec3::ZERO { Vec3::Y } else { up };
    Transform::from_translation(eye).looking_at(ahead, up)
}

/// 地表观察点：北纬约 35°，避开纹理的极冠区域，保证脚下有大陆 / 海洋细节。
pub(crate) fn surface_anchor_() -> Vec3 {
    Vec3::new(0.0f32, 0.574f32, 0.819f32).normalize()
}

/// 固定方向、固定距离的轨道跟随相机。
pub(crate) fn orbit_camera_transform_(planet: &Transform, distance: f32) -> Transform {
    let target = planet.translation;
    let direction = Vec3::new(0.0f32, 0.55f32, 1.0f32).normalize();
    Transform::from_translation(target + direction * distance.max(0.001f32))
        .looking_at(target, Vec3::Y)
}

/// 生成指定尺寸的表面纹理（测试用小尺寸复用同一算法）。
fn surface_image_(seed: u32, base: [f32; 3], width: u32, height: u32) -> Image {
    let mut data = Vec::with_capacity((width * height * 4) as usize);
    for y in 0..height {
        let v = (y as f32 + 0.5f32) / height as f32;
        let latitude = (v - 0.5f32).abs() * 2.0f32;
        for x in 0..width {
            let u = (x as f32 + 0.5f32) / width as f32;
            let elevation = fbm_(seed, u, v);
            let land = smoothstep_(0.46f32, 0.56f32, elevation);

            // 海洋偏深蓝，陆地用行星主色提亮，形成明显反差。
            let ocean = [0.035f32, 0.085f32, 0.20f32];
            let ground = [
                base[0] * 0.78f32 + 0.14f32,
                base[1] * 0.78f32 + 0.14f32,
                base[2] * 0.78f32 + 0.14f32,
            ];
            let mut rgb = [
                lerp_(ocean[0], ground[0], land),
                lerp_(ocean[1], ground[1], land),
                lerp_(ocean[2], ground[2], land),
            ];

            // 陆地内部按高度做明暗分层。
            let relief = ((elevation - 0.52f32).max(0.0f32) * 1.8f32).min(0.35f32);
            rgb = [
                (rgb[0] * (1.0f32 + relief)).clamp(0.0f32, 1.0f32),
                (rgb[1] * (1.0f32 + relief)).clamp(0.0f32, 1.0f32),
                (rgb[2] * (1.0f32 + relief)).clamp(0.0f32, 1.0f32),
            ];

            // 云带：另一层噪声的高值区，稍微偏白。
            let cloud = smoothstep_(
                0.60f32,
                0.72f32,
                fbm_(
                    seed ^ 0x00A5_5A5A,
                    (u * 2.0f32).fract(),
                    (v * 1.0f32 + 0.17f32).fract(),
                ),
            ) * 0.55f32;
            rgb = [
                lerp_(rgb[0], 0.93f32, cloud),
                lerp_(rgb[1], 0.95f32, cloud),
                lerp_(rgb[2], 0.97f32, cloud),
            ];

            // 极冠。
            let pole = smoothstep_(0.80f32, 0.97f32, latitude);
            rgb = [
                lerp_(rgb[0], 0.96f32, pole),
                lerp_(rgb[1], 0.97f32, pole),
                lerp_(rgb[2], 0.98f32, pole),
            ];

            // 细微颗粒，避免色块过于平整。
            let grain = 0.96f32 + 0.08f32 * hash01_(seed ^ 0x5EED_1234, x as i32, y as i32);
            data.extend_from_slice(&[
                to_u8_(rgb[0] * grain),
                to_u8_(rgb[1] * grain),
                to_u8_(rgb[2] * grain),
                255u8,
            ]);
        }
    }

    Image::new(
        Extent3d {
            width,
            height,
            depth_or_array_layers: 1u32,
        },
        TextureDimension::D2,
        data,
        TextureFormat::Rgba8UnormSrgb,
        RenderAssetUsages::default(),
    )
}

/// 多倍频值噪声，返回 `0.0..=1.0`。
fn fbm_(seed: u32, u: f32, v: f32) -> f32 {
    let mut sum = 0.0f32;
    let mut total = 0.0f32;
    let mut amplitude = 0.5f32;
    let mut cells_x = 6i32;
    let mut cells_y = 3i32;
    for octave in 0..5u32 {
        let octave_seed = seed.wrapping_add(octave.wrapping_mul(0x9E37_79B9));
        sum += amplitude * value_noise_(octave_seed, u, v, cells_x, cells_y);
        total += amplitude;
        amplitude *= 0.5f32;
        cells_x = cells_x.saturating_mul(2);
        cells_y = cells_y.saturating_mul(2);
    }
    (sum / total).clamp(0.0f32, 1.0f32)
}

/// 经度方向环绕、纬度方向夹紧的二维值噪声。
fn value_noise_(seed: u32, u: f32, v: f32, cells_x: i32, cells_y: i32) -> f32 {
    let x = u * cells_x as f32;
    let y = v * cells_y as f32 - 0.5f32;
    let x0 = x.floor() as i32;
    let y0 = y.floor() as i32;
    let tx = smoothstep_(0.0f32, 1.0f32, x - x.floor());
    let ty = smoothstep_(0.0f32, 1.0f32, y - y.floor());

    let x0 = x0.rem_euclid(cells_x);
    let x1 = (x0 + 1).rem_euclid(cells_x);
    let y0 = y0.clamp(0, cells_y - 1);
    let y1 = (y0 + 1).clamp(0, cells_y - 1);

    let top = lerp_(hash01_(seed, x0, y0), hash01_(seed, x1, y0), tx);
    let bottom = lerp_(hash01_(seed, x0, y1), hash01_(seed, x1, y1), tx);
    lerp_(top, bottom, ty)
}

/// 整数哈希到 `0.0..=1.0`。
fn hash01_(seed: u32, x: i32, y: i32) -> f32 {
    let mut h = seed;
    h ^= (x as u32).wrapping_mul(0x9E37_79B1);
    h = h.rotate_left(13);
    h ^= (y as u32).wrapping_mul(0x85EB_CA6B);
    h ^= h >> 16;
    h = h.wrapping_mul(0x7FEB_352D);
    h ^= h >> 15;
    h = h.wrapping_mul(0x846C_A68B);
    h ^= h >> 16;
    (h >> 8) as f32 / (u32::MAX >> 8) as f32
}

/// `edge0..edge1` 的平滑插值，返回 `0.0..=1.0`。
fn smoothstep_(edge0: f32, edge1: f32, value: f32) -> f32 {
    let t = ((value - edge0) / (edge1 - edge0)).clamp(0.0f32, 1.0f32);
    t * t * (3.0f32 - 2.0f32 * t)
}

/// 线性插值。
fn lerp_(from: f32, to: f32, t: f32) -> f32 {
    from + (to - from) * t
}

/// 把 `0.0..=1.0` 的 sRGB 分量量化到字节。
fn to_u8_(value: f32) -> u8 {
    ops::round(value.clamp(0.0f32, 1.0f32) * 255.0f32) as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试表面纹理生成是确定性的，并且尺寸 / 通道数符合 UV 球纹理要求。
    /// Test that surface texture generation is deterministic and has the expected size / channels.
    ///
    /// - 手段：用同一个种子和主色调用两次 32x16 的 `surface_image_`。
    /// - 判断：两张图的像素字节完全相同，长度都是 `32 * 16 * 4`，尺寸字段一致。
    ///
    /// - Means: call the 32x16 `surface_image_` twice with the same seed and base color.
    /// - Verdict: the pixel bytes are identical and both are `32 * 16 * 4` bytes with matching extents.
    #[test]
    fn surface_image_is_deterministic_() {
        let first = surface_image_(0x1234_5678u32, [0.2f32, 0.8f32, 0.3f32], 32u32, 16u32);
        let second = surface_image_(0x1234_5678u32, [0.2f32, 0.8f32, 0.3f32], 32u32, 16u32);

        let first_data = first.data.as_ref().expect("纹理应包含像素数据");
        let second_data = second.data.as_ref().expect("纹理应包含像素数据");
        assert_eq!(first_data, second_data);
        assert_eq!(first_data.len(), 32 * 16 * 4);
        assert_eq!(first.texture_descriptor.size.width, 32u32);
        assert_eq!(first.texture_descriptor.size.height, 16u32);
    }

    /// 测试表面纹理不是纯色，否则行星自转在视觉上仍然不可见。
    /// Test that the surface texture is not a flat color; otherwise rotation still looks static.
    ///
    /// - 手段：用固定种子生成 64x32 纹理，统计不同 RGB 值的数量。
    /// - 判断：不同颜色数远大于 1，说明大陆 / 海洋 / 云带确实产生了可辨认的表面特征。
    ///
    /// - Means: generate a 64x32 texture with a fixed seed and count distinct RGB triples.
    /// - Verdict: the number of distinct colors is far greater than one, proving that continents /
    ///   oceans / clouds produce visible surface features.
    #[test]
    fn surface_image_has_visible_features_() {
        let image = surface_image_(0xDEAD_BEEFu32, [0.7f32, 0.3f32, 0.2f32], 64u32, 32u32);
        let data = image.data.as_ref().expect("纹理应包含像素数据");
        let mut colors = std::collections::BTreeSet::new();
        for pixel in data.chunks_exact(4usize) {
            colors.insert([pixel[0], pixel[1], pixel[2]]);
        }
        assert!(
            colors.len() > 64usize,
            "表面颜色种类过少（{}），自转可能仍然看不出来",
            colors.len()
        );
    }

    /// 测试噪声在经度方向首尾相接，避免球面 UV 接缝处出现一条明显断线。
    /// Test that the noise wraps in longitude, avoiding a visible seam on the UV sphere.
    ///
    /// - 手段：分别采样 `u = 0` 与 `u = 1` 的同一个 8x4 网格值噪声。
    /// - 判断：两个采样值几乎相等（差值小于 1e-5）。
    ///
    /// - Means: sample the same 8x4 value noise at `u = 0` and `u = 1`.
    /// - Verdict: the two samples are nearly equal (difference below 1e-5).
    #[test]
    fn noise_wraps_at_longitude_seam_() {
        let left = value_noise_(0x1122_3344u32, 0.0f32, 0.37f32, 8i32, 4i32);
        let right = value_noise_(0x1122_3344u32, 1.0f32, 0.37f32, 8i32, 4i32);
        assert!(
            (left - right).abs() < 1.0e-5f32,
            "经度接缝不连续：{left} vs {right}"
        );
    }
}
