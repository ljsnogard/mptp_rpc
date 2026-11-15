//! `scene-client`：Bevy 3D 太阳系场景（feature-gated）。
//!
//! 线程模型：**Bevy 主线程 + 1 个 compio 网络线程**。Bevy 主线程只做 ECS / 渲染 /
//! `bevy_egui`；网络线程独占 `MptpClient`、所有 `Pull` 与 compio 运行时。跨线程只传
//! [`NetCommand`] / [`NetEvent`] 普通数据。
//!
//! 场景规则：
//!
//! - 太阳在原点；
//! - 每个客户端 join 后出现一颗行星，服务端只发参数、不发位置；
//! - 摄像机锁定自己的行星；
//! - 默认订阅目录里的全部行星，`bevy_egui` 勾选框可退订 / 重新订阅；
//! - 滑杆修改自己的公转 / 自转速度；
//! - 未订阅行星不显示，重新订阅后按 `Snapshot` 恢复。
//!
//! # `scene-client`: the feature-gated Bevy solar-system client
//!
//! The Bevy main thread renders and runs egui; one compio network thread owns the MPTP
//! client and every pull. Only plain data crosses the boundary. The server sends
//! parameters, never positions; the camera follows the local planet.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    process::ExitCode,
};

use bevy::{
    app::TaskPoolPlugin,
    input::mouse::{AccumulatedMouseScroll, MouseScrollPixelsPerLine},
    prelude::*,
    window::WindowResolution,
};
use bevy_egui::{EguiContexts, EguiPlugin, EguiPrimaryContextPass, egui};
use mptp_smux_v1_demo::scene::{
    PlanetDescriptor, PlanetDirectory, PlanetId, PlanetParams, PlanetTheme,
    net_::{NetCommand, NetEvent, NetHandle, spawn_net_thread_},
    protocol_::{
        K_ORBIT_RADIUS_MAX, K_ORBIT_RADIUS_MIN, K_ORBIT_SPEED_MAX, K_ORBIT_SPEED_MIN,
        K_PLANET_RADIUS_MAX, K_PLANET_RADIUS_MIN, K_SPIN_SPEED_MAX, K_SPIN_SPEED_MIN,
    },
};

#[path = "scene-client/mod.rs"]
mod client_view;

use client_view::{
    install_cjk_font, orbit_camera_transform_, planet_surface_image, surface_camera_transform_,
};

/// 缺省服务端地址。
const K_DEFAULT_SERVER: &str = "127.0.0.1:7721";

/// 场景状态资源。
#[derive(Resource, Default)]
struct SceneState {
    /// 自己的行星。
    my_planet: Option<PlanetDescriptor>,

    /// 最近一次目录。
    directory: PlanetDirectory,

    /// 已知行星状态（目录 + 快照 + 更新）。
    planets: BTreeMap<PlanetId, PlanetDescriptor>,

    /// 已订阅的行星。
    subscriptions: BTreeSet<PlanetId>,

    /// 是否已经执行过「默认订阅全部」。
    auto_subscribed: bool,

    /// 自己行星的最新可变参数（速度、半径、颜色、纹理主题）。
    my_params: PlanetParams,

    /// 最近一次网络错误 / 连接状态。
    status: String,
}

/// 观察模式。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ViewMode {
    /// 固定方向跟随行星，观察整颗行星。
    #[default]
    Orbit,

    /// 把相机固定在行星地表，随自转一起转。
    Surface,
}

/// 轨道视角的最近 / 最远距离（世界单位）。
const K_ORBIT_DISTANCE_MIN: f32 = 1.8f32;
const K_ORBIT_DISTANCE_MAX: f32 = 45.0f32;

/// 地表视角的离地高度范围（以行星半径为单位）。
const K_SURFACE_ALTITUDE_MIN: f32 = 0.015f32;
const K_SURFACE_ALTITUDE_MAX: f32 = 3.0f32;

/// 相机机位：轨道跟随 / 地表随转，以及各自的缩放量。
#[derive(Resource)]
struct CameraRig {
    /// 当前观察模式。
    mode_: ViewMode,

    /// 轨道视角到行星中心的距离。
    orbit_distance_: f32,

    /// 地表视角的离地高度（行星半径的倍数）。
    surface_altitude_: f32,
}

impl Default for CameraRig {
    fn default() -> Self {
        CameraRig {
            mode_: ViewMode::Orbit,
            orbit_distance_: 7.0f32,
            surface_altitude_: 0.25f32,
        }
    }
}

/// egui 是否正在占用鼠标；占用时滚轮交给面板而不是相机。
#[derive(Resource, Default)]
struct UiPointerCapture {
    /// 上一帧 egui 是否请求指针输入。
    wants_pointer_: bool,
}

/// 中文字体的装载状态，用于在面板里显示实际使用的字体。
#[derive(Resource, Default)]
struct FontState {
    /// 是否已经尝试过装载。
    installed_: bool,

    /// 给用户看的装载结果。
    report_: String,
}

/// 光照参数：背景光（环境光）与太阳点光，均可由面板滑杆调整。
#[derive(Resource)]
struct LightSettings {
    /// 背景环境光亮度。
    ambient_brightness_: f32,

    /// 太阳点光强度（流明）。
    sun_intensity_: f32,
}

impl Default for LightSettings {
    fn default() -> Self {
        LightSettings {
            // 背景光刻意压低：太阳是场景里唯一的直接光源，明暗交界才明显。
            ambient_brightness_: 30.0f32,
            sun_intensity_: 6_000_000.0f32,
        }
    }
}

/// 网络句柄资源。
#[derive(Resource)]
struct NetResource(NetHandle);

/// 太阳点光标记，供面板实时调整强度。
#[derive(Component)]
struct SunLight;

/// 行星可视化标记，同时缓存当前外观，避免每帧重建纹理。
#[derive(Component)]
struct PlanetVisual {
    /// 行星标识。
    id: PlanetId,

    /// 生成当前纹理时使用的颜色。
    color_: u32,

    /// 生成当前纹理时使用的主题。
    theme_: PlanetTheme,

    /// 当前网格缩放（行星半径）。
    planet_radius_: f32,

    /// 当前表面纹理句柄；颜色 / 主题变化时替换并释放旧纹理。
    texture_: Handle<Image>,
}

/// 共享网格资源。
#[derive(Resource)]
struct PlanetAssets {
    /// 单位球（行星用）。
    unit_sphere: Handle<Mesh>,
}

fn main() -> ExitCode {
    let (addr, name, color) = match parse_args_(std::env::args().skip(1usize)) {
        Ok(values) => values,
        Err(message) => {
            println!("{message}");
            return ExitCode::from(2u8);
        }
    };

    let net = match spawn_net_thread_(addr, name, color) {
        Ok(net) => net,
        Err(err) => {
            eprintln!("启动网络线程失败：{err:#}");
            return ExitCode::FAILURE;
        }
    };

    App::new()
        .add_plugins(
            DefaultPlugins
                .set(WindowPlugin {
                    primary_window: Some(Window {
                        title: String::from("mptp_smux 太阳系场景"),
                        resolution: WindowResolution::new(1280u32, 800u32),
                        ..default()
                    }),
                    ..default()
                })
                // 单线程 task pool：Bevy 内部不再为业务多开工作线程。
                .set(TaskPoolPlugin {
                    task_pool_options: bevy::app::TaskPoolOptions::with_num_threads(1usize),
                }),
        )
        .add_plugins(EguiPlugin::default())
        .insert_resource(NetResource(net))
        .init_resource::<SceneState>()
        .init_resource::<CameraRig>()
        .init_resource::<UiPointerCapture>()
        .init_resource::<FontState>()
        .init_resource::<LightSettings>()
        .add_systems(Startup, setup_scene_)
        .add_systems(
            Update,
            (
                apply_light_settings_,
                drain_net_events_,
                sync_planet_entities_,
                switch_view_mode_,
                zoom_camera_,
                update_orbits_,
                follow_my_planet_,
            )
                .chain(),
        )
        .add_systems(EguiPrimaryContextPass, scene_ui_)
        .run();

    ExitCode::SUCCESS
}

/// 启动时：摄像机、太阳、灯光、共享网格。
fn setup_scene_(
    mut commands: Commands,
    settings: Res<LightSettings>,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.spawn((
        Camera3d::default(),
        // 地表视角会贴到球面上，近裁剪面必须足够小。
        Projection::Perspective(PerspectiveProjection {
            near: 0.01f32,
            far: 2_000.0f32,
            ..default()
        }),
        // 背景环境光：只负责让夜面不至于全黑，强度由面板滑杆控制。
        AmbientLight {
            color: Color::srgb(0.55f32, 0.65f32, 0.90f32),
            brightness: settings.ambient_brightness_,
            ..default()
        },
        Transform::from_xyz(0.0f32, 10.0f32, 16.0f32).looking_at(Vec3::ZERO, Vec3::Y),
    ));

    // 太阳是场景里唯一的直接光源：点光源放在太阳球心，不再添加固定方向的平行光，
    // 否则行星的明暗交界会被一个与自转无关的固定角度照亮，看不出自转。
    commands.spawn((
        SunLight,
        PointLight {
            intensity: settings.sun_intensity_,
            range: 200.0f32,
            ..default()
        },
        Transform::default(),
    ));

    let unit_sphere = meshes.add(Sphere::new(1.0f32).mesh().uv(48u32, 32u32));
    let sun_sphere = meshes.add(Sphere::new(2.2f32).mesh().uv(32u32, 20u32));
    commands.insert_resource(PlanetAssets {
        unit_sphere: unit_sphere.clone(),
    });

    commands.spawn((
        Mesh3d(sun_sphere),
        MeshMaterial3d(materials.add(StandardMaterial {
            base_color: Color::srgb(1.0f32, 0.78f32, 0.18f32),
            emissive: LinearRgba::rgb(6.0f32, 4.4f32, 1.0f32),
            ..default()
        })),
        Transform::default(),
    ));
}

/// 把面板中的光照参数同步到相机的环境光与太阳点光。
fn apply_light_settings_(
    settings: Res<LightSettings>,
    mut ambient: Query<&mut AmbientLight, With<Camera3d>>,
    mut suns: Query<&mut PointLight, With<SunLight>>,
) {
    for mut light in &mut ambient {
        light.brightness = settings.ambient_brightness_;
    }
    for mut light in &mut suns {
        light.intensity = settings.sun_intensity_;
    }
}

/// 排空网络事件，更新场景状态。
fn drain_net_events_(mut state: ResMut<SceneState>, net: Res<NetResource>) {
    while let Ok(event) = net.0.event_rx_.try_recv() {
        match event {
            NetEvent::Joined { descriptor } => {
                state.my_params = descriptor.params;
                state.my_planet = Some(descriptor.clone());
                state.planets.insert(descriptor.id, descriptor.clone());
                if state.subscriptions.insert(descriptor.id) {
                    let _ = net.0.cmd_tx_.try_send(NetCommand::Subscribe {
                        planet_id: descriptor.id,
                        uri: descriptor.stream_uri.clone(),
                    });
                }
                state.status = format!("已加入：行星 #{}", descriptor.id);
            }
            NetEvent::Directory { directory } => {
                state.directory = directory;
                for planet in state.directory.planets.clone() {
                    state.planets.entry(planet.id).or_insert(planet);
                }
                // 默认订阅全部行星（包括自己），满足「两个客户端互相可见」。
                if !state.auto_subscribed && state.my_planet.is_some() {
                    state.auto_subscribed = true;
                    for planet in state.directory.planets.clone() {
                        if state.subscriptions.insert(planet.id) {
                            let _ = net.0.cmd_tx_.try_send(NetCommand::Subscribe {
                                planet_id: planet.id,
                                uri: planet.stream_uri.clone(),
                            });
                        }
                    }
                }
            }
            NetEvent::PlanetEvent { planet_id, event } => match event {
                mptp_smux_v1_demo::scene::PlanetEvent::Snapshot(planet) => {
                    let is_mine = state
                        .my_planet
                        .as_ref()
                        .is_some_and(|mine| mine.id == planet_id);
                    state.planets.insert(planet_id, planet.clone());
                    if is_mine {
                        state.my_params = planet.params;
                        state.my_planet = Some(planet);
                    }
                }
                mptp_smux_v1_demo::scene::PlanetEvent::Updated(planet) => {
                    let is_mine = state
                        .my_planet
                        .as_ref()
                        .is_some_and(|mine| mine.id == planet_id);
                    state.planets.insert(planet_id, planet.clone());
                    // 自己拖动滑杆时本地值才是最新意图；不要用服务端回显覆盖它，
                    // 否则滑杆会在网络往返期间回跳。服务端清洗范围与 UI 范围一致。
                    if is_mine {
                        state.my_planet = Some(planet);
                    }
                }
                mptp_smux_v1_demo::scene::PlanetEvent::Removed { planet_id, .. } => {
                    state.planets.remove(&planet_id);
                    state.subscriptions.remove(&planet_id);
                    if state
                        .my_planet
                        .as_ref()
                        .is_some_and(|mine| mine.id == planet_id)
                    {
                        state.my_planet = None;
                        state.status = String::from("自己的行星已被删除");
                    }
                }
                mptp_smux_v1_demo::scene::PlanetEvent::Heartbeat { .. } => {}
            },
            NetEvent::SubscriptionClosed { planet_id, reason } => {
                state.subscriptions.remove(&planet_id);
                state.status = format!("订阅 #{planet_id} 结束：{reason}");
            }
            NetEvent::Error(message) => {
                state.status = message;
            }
            NetEvent::Disconnected => {
                state.status = String::from("网络线程已断开");
                state.subscriptions.clear();
            }
        }
    }
}

/// 让可视化实体与「已订阅且已知」的行星保持一致；外观变化时原地更新材质与缩放。
fn sync_planet_entities_(
    mut commands: Commands,
    state: Res<SceneState>,
    assets: Res<PlanetAssets>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut images: ResMut<Assets<Image>>,
    mut visuals: Query<(
        Entity,
        &mut PlanetVisual,
        &mut Transform,
        &MeshMaterial3d<StandardMaterial>,
    )>,
) {
    let mut existing = BTreeSet::new();
    for (entity, mut visual, mut transform, material_handle) in &mut visuals {
        let visible =
            state.subscriptions.contains(&visual.id) && state.planets.contains_key(&visual.id);
        if !visible {
            commands.entity(entity).despawn();
            continue;
        }
        existing.insert(visual.id);

        let Some(planet) = state.planets.get(&visual.id) else {
            continue;
        };
        let theme = PlanetTheme::from_wire_(planet.params.texture_theme);
        let color_changed = visual.color_ != planet.params.color;
        let theme_changed = visual.theme_ != theme;
        if color_changed || theme_changed {
            // 颜色只改材质 tint；只有主题变化才重生成中性反照率纹理。
            let new_texture = theme_changed.then(|| {
                let texture = images.add(planet_surface_image(planet.id, theme));
                let old_texture = std::mem::replace(&mut visual.texture_, texture.clone());
                let _ = images.remove(&old_texture);
                texture
            });
            if let Some(mut material) = materials.get_mut(&material_handle.0) {
                if color_changed {
                    material.base_color = color_of_(planet.params.color);
                }
                if let Some(texture) = new_texture {
                    material.base_color_texture = Some(texture);
                    material.emissive = theme_emissive_(theme);
                }
            }
            visual.color_ = planet.params.color;
            visual.theme_ = theme;
        }
        if visual.planet_radius_ != planet.params.planet_radius {
            transform.scale = Vec3::splat(planet.params.planet_radius);
            visual.planet_radius_ = planet.params.planet_radius;
        }
    }

    for (id, planet) in &state.planets {
        if !state.subscriptions.contains(id) || existing.contains(id) {
            continue;
        }
        let theme = PlanetTheme::from_wire_(planet.params.texture_theme);
        let texture = images.add(planet_surface_image(planet.id, theme));
        commands.spawn((
            Mesh3d(assets.unit_sphere.clone()),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: color_of_(planet.params.color),
                base_color_texture: Some(texture.clone()),
                emissive: theme_emissive_(theme),
                perceptual_roughness: 0.88f32,
                metallic: 0.0f32,
                // 太阳点光很亮，岩石行星不该有大片镜面高光。
                reflectance: 0.05f32,
                ..default()
            })),
            Transform::from_xyz(planet.params.orbit_radius, 0.0f32, 0.0f32)
                .with_scale(Vec3::splat(planet.params.planet_radius)),
            PlanetVisual {
                id: *id,
                color_: planet.params.color,
                theme_: theme,
                planet_radius_: planet.params.planet_radius,
                texture_: texture,
            },
        ));
    }
}

/// 不同纹理主题的基础自发光；熔岩主题在夜面保留一点余烬光。
fn theme_emissive_(theme: PlanetTheme) -> LinearRgba {
    match theme {
        PlanetTheme::Lava => LinearRgba::rgb(0.45f32, 0.08f32, 0.01f32),
        _ => LinearRgba::rgb(0.04f32, 0.04f32, 0.045f32),
    }
}

/// 按行星参数本地计算公转 / 自转；服务端只发参数。
fn update_orbits_(
    time: Res<Time>,
    state: Res<SceneState>,
    mut visuals: Query<(&PlanetVisual, &mut Transform)>,
) {
    let now = time.elapsed_secs();
    for (visual, mut transform) in &mut visuals {
        let Some(planet) = state.planets.get(&visual.id) else {
            continue;
        };
        let radius = planet.params.orbit_radius;
        let angle = planet.params.orbit_speed * now + (planet.id as f32) * 0.7f32;
        transform.translation = Vec3::new(angle.cos() * radius, 0.0f32, angle.sin() * radius);
        transform.rotate_local_y(planet.params.spin_speed * time.delta_secs());
    }
}

/// 键盘 `1` / `2` 快速切换观察模式。
fn switch_view_mode_(keys: Res<ButtonInput<KeyCode>>, mut rig: ResMut<CameraRig>) {
    if keys.just_pressed(KeyCode::Digit1) {
        rig.mode_ = ViewMode::Orbit;
    }
    if keys.just_pressed(KeyCode::Digit2) {
        rig.mode_ = ViewMode::Surface;
    }
}

/// 滚轮缩放：轨道模式改变观察距离，地表模式改变离地高度。
///
/// 指针停在 egui 面板上时不动相机，否则滚轮会同时滚动面板和缩放场景。
fn zoom_camera_(
    scroll: Res<AccumulatedMouseScroll>,
    pixels_per_line: Res<MouseScrollPixelsPerLine>,
    ui_pointer: Res<UiPointerCapture>,
    mut rig: ResMut<CameraRig>,
) {
    if ui_pointer.wants_pointer_ {
        return;
    }
    let lines = scroll.to_lines(&pixels_per_line).delta.y;
    if lines == 0.0f32 {
        return;
    }

    apply_scroll_zoom_(&mut rig, lines);
}

/// 按滚轮行数调整当前模式的缩放量，并夹紧到允许区间。
fn apply_scroll_zoom_(rig: &mut CameraRig, lines: f32) {
    let factor = (-lines * 0.16f32).exp();
    match rig.mode_ {
        ViewMode::Orbit => {
            rig.orbit_distance_ =
                (rig.orbit_distance_ * factor).clamp(K_ORBIT_DISTANCE_MIN, K_ORBIT_DISTANCE_MAX);
        }
        ViewMode::Surface => {
            rig.surface_altitude_ = (rig.surface_altitude_ * factor)
                .clamp(K_SURFACE_ALTITUDE_MIN, K_SURFACE_ALTITUDE_MAX);
        }
    }
}

/// 摄像机锁定自己的行星：轨道跟随或贴在地表随自转一起转。
fn follow_my_planet_(
    state: Res<SceneState>,
    rig: Res<CameraRig>,
    visuals: Query<(&PlanetVisual, &Transform), Without<Camera3d>>,
    mut cameras: Query<&mut Transform, With<Camera3d>>,
) {
    let Some(mine) = state.my_planet.as_ref() else {
        return;
    };
    let Some((_, planet_transform)) = visuals.iter().find(|(visual, _)| visual.id == mine.id)
    else {
        return;
    };
    let camera_transform = match rig.mode_ {
        ViewMode::Orbit => orbit_camera_transform_(planet_transform, rig.orbit_distance_),
        ViewMode::Surface => surface_camera_transform_(planet_transform, rig.surface_altitude_),
    };
    for mut camera in &mut cameras {
        *camera = camera_transform;
    }
}

/// `bevy_egui` 面板：字体状态、速度滑杆、观察视角、行星订阅勾选框。
fn scene_ui_(
    mut contexts: EguiContexts,
    mut state: ResMut<SceneState>,
    mut rig: ResMut<CameraRig>,
    mut lights: ResMut<LightSettings>,
    mut fonts: ResMut<FontState>,
    mut ui_pointer: ResMut<UiPointerCapture>,
    net: Res<NetResource>,
) -> Result {
    let Ok(context) = contexts.ctx_mut() else {
        return Ok(());
    };

    if !fonts.installed_ {
        fonts.installed_ = true;
        fonts.report_ = match install_cjk_font(context) {
            Ok(path) => {
                println!("已加载中文字体：{path}");
                let name = std::path::Path::new(&path)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or(path.as_str());
                format!("中文字体：{name}")
            }
            Err(message) => format!("中文字体加载失败：{message}"),
        };
    }
    // 用上一帧的指针状态判断滚轮是否应该交给 egui。
    ui_pointer.wants_pointer_ = context.egui_wants_pointer_input();

    egui::Window::new("太阳系场景").show(context, |ui| {
        ui.label(&state.status);
        if !fonts.report_.is_empty() {
            ui.small(&fonts.report_);
        }
        ui.separator();

        let mut params = state.my_params;
        let mut params_changed = false;
        ui.add_enabled_ui(state.my_planet.is_some(), |ui| {
            ui.heading("行星参数");
            params_changed |= ui
                .add(
                    egui::Slider::new(
                        &mut params.orbit_speed,
                        K_ORBIT_SPEED_MIN..=K_ORBIT_SPEED_MAX,
                    )
                    .text("公转速度"),
                )
                .changed();
            params_changed |= ui
                .add(
                    egui::Slider::new(&mut params.spin_speed, K_SPIN_SPEED_MIN..=K_SPIN_SPEED_MAX)
                        .text("自转速度"),
                )
                .changed();
            params_changed |= ui
                .add(
                    egui::Slider::new(
                        &mut params.orbit_radius,
                        K_ORBIT_RADIUS_MIN..=K_ORBIT_RADIUS_MAX,
                    )
                    .text("轨道半径"),
                )
                .changed();
            params_changed |= ui
                .add(
                    egui::Slider::new(
                        &mut params.planet_radius,
                        K_PLANET_RADIUS_MIN..=K_PLANET_RADIUS_MAX,
                    )
                    .text("行星半径"),
                )
                .changed();

            let mut rgb = [
                ((params.color >> 16) & 0xFFu32) as u8,
                ((params.color >> 8) & 0xFFu32) as u8,
                (params.color & 0xFFu32) as u8,
            ];
            ui.horizontal(|ui| {
                ui.label("行星颜色");
                if egui::color_picker::color_edit_button_srgb(ui, &mut rgb).changed() {
                    params.color =
                        (u32::from(rgb[0]) << 16) | (u32::from(rgb[1]) << 8) | u32::from(rgb[2]);
                    params_changed = true;
                }
            });

            let mut theme = PlanetTheme::from_wire_(params.texture_theme);
            let mut theme_changed = false;
            egui::ComboBox::from_label("纹理主题")
                .selected_text(theme_label_(theme))
                .show_ui(ui, |ui| {
                    for candidate in PlanetTheme::ALL {
                        if ui
                            .selectable_value(&mut theme, candidate, theme_label_(candidate))
                            .clicked()
                        {
                            theme_changed = true;
                        }
                    }
                });
            if theme_changed {
                params.texture_theme = theme.wire_();
                params_changed = true;
            }

            ui.small("修改会通过服务器广播给所有订阅者");
        });
        if params_changed {
            state.my_params = params;
            let _ = net.0.cmd_tx_.try_send(NetCommand::UpdateSelf { params });
        }

        ui.separator();
        ui.heading("观察视角");
        ui.horizontal(|ui| {
            ui.selectable_value(&mut rig.mode_, ViewMode::Orbit, "轨道跟随");
            ui.selectable_value(&mut rig.mode_, ViewMode::Surface, "地表随转");
        });
        match rig.mode_ {
            ViewMode::Orbit => {
                ui.add(
                    egui::Slider::new(
                        &mut rig.orbit_distance_,
                        K_ORBIT_DISTANCE_MIN..=K_ORBIT_DISTANCE_MAX,
                    )
                    .logarithmic(true)
                    .text("观察距离"),
                );
                ui.small("滚轮拉远 / 拉近；按 2 切到地表视角");
            }
            ViewMode::Surface => {
                ui.add(
                    egui::Slider::new(
                        &mut rig.surface_altitude_,
                        K_SURFACE_ALTITUDE_MIN..=K_SURFACE_ALTITUDE_MAX,
                    )
                    .logarithmic(true)
                    .text("离地高度（行星半径）"),
                );
                ui.small("相机随行星自转；按 1 回到轨道视角");
            }
        }

        ui.separator();
        ui.horizontal(|ui| {
            ui.heading("光照");
            if ui.small_button("恢复默认").clicked() {
                *lights = LightSettings::default();
            }
        });
        ui.add(
            egui::Slider::new(&mut lights.ambient_brightness_, 0.0f32..=800.0f32)
                .text("背景光强度"),
        );
        let mut sun_wan = lights.sun_intensity_ / 10_000.0f32;
        if ui
            .add(
                egui::Slider::new(&mut sun_wan, 10.0f32..=4_000.0f32)
                    .logarithmic(true)
                    .text("太阳光强度（万流明）"),
            )
            .changed()
        {
            lights.sun_intensity_ = sun_wan * 10_000.0f32;
        }
        ui.small("背景光调低、太阳光调高，行星明暗交界与自转更明显");

        ui.separator();
        ui.heading("行星订阅");
        let my_id = state.my_planet.as_ref().map(|planet| planet.id);
        let mut toggles = Vec::new();
        for planet in state.directory.planets.clone() {
            let is_self = my_id == Some(planet.id);
            let mut checked = is_self || state.subscriptions.contains(&planet.id);
            let label = format!("#{} {}（owner {}）", planet.id, planet.name, planet.owner);
            let response = ui.add_enabled(!is_self, egui::Checkbox::new(&mut checked, label));
            if response.changed() && !is_self {
                toggles.push((planet.id, planet.stream_uri.clone(), checked));
            }
        }

        for (planet_id, uri, checked) in toggles {
            if checked {
                if let Some(planet) = state
                    .directory
                    .planets
                    .iter()
                    .find(|planet| planet.id == planet_id)
                    .cloned()
                {
                    state.planets.insert(planet_id, planet);
                }
                state.subscriptions.insert(planet_id);
                let _ = net
                    .0
                    .cmd_tx_
                    .try_send(NetCommand::Subscribe { planet_id, uri });
            } else {
                state.subscriptions.remove(&planet_id);
                state.planets.remove(&planet_id);
                let _ = net
                    .0
                    .cmd_tx_
                    .try_send(NetCommand::Unsubscribe { planet_id });
            }
        }
    });
    Ok(())
}

/// 纹理主题的中文界面名称。
fn theme_label_(theme: PlanetTheme) -> &'static str {
    match theme {
        PlanetTheme::Terran => "类地",
        PlanetTheme::Ocean => "海洋",
        PlanetTheme::Desert => "沙漠",
        PlanetTheme::Ice => "冰封",
        PlanetTheme::Lava => "熔岩",
        PlanetTheme::Gas => "气态",
    }
}

/// 把服务端的 `u32` 颜色解释成 RGB。
fn color_of_(color: u32) -> Color {
    let r = ((color >> 16) & 0xFFu32) as f32 / 255.0f32;
    let g = ((color >> 8) & 0xFFu32) as f32 / 255.0f32;
    let b = (color & 0xFFu32) as f32 / 255.0f32;
    Color::srgb(r.max(0.2f32), g.max(0.2f32), b.max(0.2f32))
}

/// 解析命令行：`scene-client [--server 127.0.0.1:7721] [--name NAME] [--color 0xRRGGBB]`。
fn parse_args_(
    mut args: impl Iterator<Item = String>,
) -> Result<(SocketAddr, String, u32), String> {
    let mut server: SocketAddr = K_DEFAULT_SERVER.parse().expect("缺省服务器地址必须合法");
    let mut name = String::from("client");
    let mut color = 0x0000_AA55u32;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                return Err(String::from(
                    "用法：scene-client [--server 127.0.0.1:7721] [--name NAME] [--color 0xRRGGBB]",
                ));
            }
            "--server" => {
                let value = args
                    .next()
                    .ok_or_else(|| String::from("`--server` 缺少地址"))?;
                server = value
                    .parse()
                    .map_err(|err| format!("`--server` 不是合法地址（{value}）：{err}"))?;
            }
            "--name" => {
                name = args.next().ok_or_else(|| String::from("`--name` 缺少值"))?;
            }
            "--color" => {
                let value = args
                    .next()
                    .ok_or_else(|| String::from("`--color` 缺少值"))?;
                let text = value.trim_start_matches("0x");
                color = u32::from_str_radix(text, 16u32)
                    .map_err(|err| format!("`--color` 不是十六进制（{value}）：{err}"))?;
            }
            other => return Err(format!("不认识的参数：{other}")),
        }
    }
    Ok((server, name, color))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试轨道视角相机保持在请求的距离上并始终朝向行星中心。
    /// Test that the orbit camera stays at the requested distance and looks at the planet center.
    ///
    /// - 手段：构造一个带平移 / 旋转 / 缩放的行星 `Transform`，以距离 10 调用
    ///   `orbit_camera_transform_`。
    /// - 判断：相机到行星中心的距离约等于 10，且相机前向（局部 `-Z`）与
    ///   「行星中心 - 相机位置」方向的点积接近 1。
    ///
    /// - Means: build a translated / rotated / scaled planet transform and call
    ///   `orbit_camera_transform_` with distance 10.
    /// - Verdict: the eye-to-center distance is about 10 and the camera forward (local `-Z`)
    ///   has a dot product close to 1 with the direction to the center.
    #[test]
    fn orbit_camera_keeps_distance_and_focus_() {
        let planet = Transform::from_xyz(4.0f32, 0.5f32, -2.0f32)
            .with_rotation(Quat::from_rotation_y(0.7f32))
            .with_scale(Vec3::splat(0.7f32));
        let camera = orbit_camera_transform_(&planet, 10.0f32);

        let to_center = planet.translation - camera.translation;
        assert!(
            (to_center.length() - 10.0f32).abs() < 1.0e-4f32,
            "轨道相机距离应为 10，实际为 {}",
            to_center.length()
        );
        let forward = camera.rotation * Vec3::NEG_Z;
        assert!(
            forward.dot(to_center.normalize()) > 0.9999f32,
            "轨道相机应看向行星中心"
        );
    }

    /// 测试地表视角相机贴在行星表面、略微俯视，并且「上」方向大致是当地径向。
    /// Test that the surface camera sits on the surface, pitches slightly down, and keeps a radial-ish up.
    ///
    /// - 手段：构造一个旋转 / 缩放过的行星变换，以高度 0.1 调用
    ///   `surface_camera_transform_`。
    /// - 判断：相机位置等于局部锚点 `anchor * 1.1` 的世界坐标；相机到行星中心的方向与当地径向
    ///   一致；相机前向朝径向内侧倾斜（点积为负）；相机「上」方向与径向的点积仍大于 0.9。
    ///
    /// - Means: build a rotated / scaled planet transform and call
    ///   `surface_camera_transform_` with altitude 0.1.
    /// - Verdict: the eye equals world-space `anchor * 1.1`; the radial direction matches;
    ///   forward tilts inward toward the planet (negative dot); camera up still has a dot product
    ///   above 0.9 with the radial.
    #[test]
    fn surface_camera_sits_on_surface_and_pitches_down_() {
        let planet = Transform::from_xyz(6.0f32, 0.0f32, 0.0f32)
            .with_rotation(Quat::from_rotation_y(1.1f32))
            .with_scale(Vec3::splat(0.8f32));
        let altitude = 0.1f32;
        let camera = surface_camera_transform_(&planet, altitude);

        let anchor = client_view::surface_anchor_();
        let expected_eye = planet.transform_point(anchor * (1.0f32 + altitude));
        assert!(
            camera.translation.distance(expected_eye) < 1.0e-5f32,
            "地表相机应位于局部锚点乘 (1 + altitude)"
        );

        let radial = (camera.translation - planet.translation).normalize();
        let expected_radial = (planet.rotation * anchor).normalize();
        assert!(
            radial.dot(expected_radial) > 0.9999f32,
            "相机应位于当地径向方向上"
        );

        let forward = camera.rotation * Vec3::NEG_Z;
        assert!(
            forward.dot(radial) < -0.2f32,
            "地表相机应略微俯视，让地平线落在画面中部"
        );
        let up = camera.rotation * Vec3::Y;
        assert!(up.dot(radial) > 0.9f32, "地表相机的上方向应大致是当地径向");
    }

    /// 测试滚轮缩放会按模式调整对应缩放量，并夹紧在允许区间内。
    /// Test that scroll zoom adjusts the mode-specific value and clamps it to the allowed range.
    ///
    /// - 手段：默认 `CameraRig` 分别向两个方向发送很大的滚轮行数。
    /// - 判断：轨道模式的观察距离停在上 / 下限；切到地表模式后高度也停在上 / 下限。
    ///
    /// - Means: send large scroll deltas in both directions to the default `CameraRig`.
    /// - Verdict: the orbit distance stops at the min / max bound; the surface altitude does the same
    ///   after switching modes.
    #[test]
    fn scroll_zoom_clamps_to_limits_() {
        let mut rig = CameraRig::default();

        apply_scroll_zoom_(&mut rig, 100.0f32);
        assert_eq!(rig.orbit_distance_, K_ORBIT_DISTANCE_MIN);
        apply_scroll_zoom_(&mut rig, -100.0f32);
        assert_eq!(rig.orbit_distance_, K_ORBIT_DISTANCE_MAX);

        rig.mode_ = ViewMode::Surface;
        apply_scroll_zoom_(&mut rig, 100.0f32);
        assert_eq!(rig.surface_altitude_, K_SURFACE_ALTITUDE_MIN);
        apply_scroll_zoom_(&mut rig, -100.0f32);
        assert_eq!(rig.surface_altitude_, K_SURFACE_ALTITUDE_MAX);
    }
}
