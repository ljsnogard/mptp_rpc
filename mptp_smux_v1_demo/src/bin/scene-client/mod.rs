//! `scene-client` 的私有子模块聚合。
//!
//! 这里只做导出控制，不存放实际逻辑；逐项列出对二进制入口可见的条目。

mod font_;
mod surface_;

pub(crate) use font_::install_cjk_font;
#[cfg(test)]
pub(crate) use surface_::surface_anchor_;
pub(crate) use surface_::{
    orbit_camera_transform_, planet_surface_image, surface_camera_transform_,
};
