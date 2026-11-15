//! 中文字体装载。
//!
//! egui 自带字体只有 Latin / emoji，不含 CJK 字形，界面上的中文会渲染成方块。
//! 本模块按固定优先级找到一个支持中文的 TTF / OTF，并以 `Lowest` 优先级追加到
//! egui `Proportional` 与 `Monospace` 的字体回退链末尾：Latin 仍用默认字体，只有
//! 默认字体缺字时才回退到中文字体。
//!
//! 查找顺序：
//!
//! 1. 随 demo 提供的 `assets/fonts/MapleMono-NF-CN-Regular.ttf`；
//! 2. 可执行文件旁的 `assets/fonts/MapleMono-NF-CN-Regular.ttf`；
//! 3. 用户字体目录里的 MapleMono / 常见系统 CJK 字体（作为兜底）。
//!
//! # CJK font loading
//!
//! egui's built-in fonts have no CJK glyphs. This module locates a CJK font (preferring
//! the bundled MapleMono), then appends it as the lowest-priority fallback for both the
//! proportional and monospace families.

use std::path::{Path, PathBuf};

use bevy_egui::egui::{
    self, FontData, FontFamily,
    epaint::text::{FontInsert, FontPriority, InsertFontFamily},
};

/// 随 demo 打包的中文字体文件名。
const K_BUNDLED_FONT_FILE: &str = "MapleMono-NF-CN-Regular.ttf";

/// egui 内部登记字体时使用的名字。
const K_FONT_NAME: &str = "MapleMonoNF-CN";

/// 把找到的中文字体注入 egui 上下文。
///
/// 返回实际使用的字体路径；失败时返回一条可直接展示给用户的说明。
pub(crate) fn install_cjk_font(ctx: &egui::Context) -> Result<String, String> {
    let path = locate_cjk_font_()?;
    let bytes =
        std::fs::read(&path).map_err(|err| format!("读取字体 {} 失败：{err}", path.display()))?;
    if !looks_like_font_(&bytes) {
        return Err(format!("{} 不是可识别的 TTF / OTF 字体", path.display()));
    }

    ctx.add_font(FontInsert::new(
        K_FONT_NAME,
        FontData::from_owned(bytes),
        vec![
            InsertFontFamily {
                family: FontFamily::Proportional,
                priority: FontPriority::Lowest,
            },
            InsertFontFamily {
                family: FontFamily::Monospace,
                priority: FontPriority::Lowest,
            },
        ],
    ));

    Ok(path.display().to_string())
}

/// 按优先级返回第一个存在的候选字体路径。
fn locate_cjk_font_() -> Result<PathBuf, String> {
    let candidates = candidate_font_paths_();
    let mut tried = Vec::with_capacity(candidates.len());
    for path in candidates {
        if path.is_file() {
            return Ok(path);
        }
        tried.push(path.display().to_string());
    }
    Err(format!(
        "未找到中文字体；请把 MapleMono-NF-CN-Regular.ttf 放到 assets/fonts/。已尝试：{}",
        tried.join("、")
    ))
}

/// 候选字体路径，从最可控到最兜底。
fn candidate_font_paths_() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    // `cargo run` 时以编译期 manifest 目录为准；发布后在可执行文件旁找。
    paths.push(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("assets/fonts")
            .join(K_BUNDLED_FONT_FILE),
    );
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        paths.push(dir.join("assets/fonts").join(K_BUNDLED_FONT_FILE));
        paths.push(dir.join("../assets/fonts").join(K_BUNDLED_FONT_FILE));
    }

    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        paths.push(
            home.join(".local/share/fonts/Maple Mono NF CN")
                .join(K_BUNDLED_FONT_FILE),
        );
        paths.push(home.join(".local/share/fonts").join(K_BUNDLED_FONT_FILE));
    }

    // 常见 Linux 发行版的 CJK 字体（MapleMono 未安装时的兜底）。
    for path in [
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-microhei.ttc",
        "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",
    ] {
        paths.push(PathBuf::from(path));
    }

    paths
}

/// 用文件头魔数做一次廉价校验，避免把损坏文件塞进 egui 后在建图集时 panic。
fn looks_like_font_(bytes: &[u8]) -> bool {
    bytes.len() >= 4
        && (bytes.starts_with(&[0x00, 0x01, 0x00, 0x00])
            || bytes.starts_with(b"OTTO")
            || bytes.starts_with(b"ttcf")
            || bytes.starts_with(b"true"))
}
