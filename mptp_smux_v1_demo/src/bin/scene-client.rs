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
    prelude::*,
    window::WindowResolution,
};
use bevy_egui::{EguiContexts, EguiPlugin, EguiPrimaryContextPass, egui};
use mptp_smux_v1_demo::scene::{
    PlanetDescriptor, PlanetDirectory, PlanetId,
    net_::{NetCommand, NetEvent, NetHandle, spawn_net_thread_},
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

    /// 自己的滑杆值。
    speeds: SelfSpeeds,

    /// 最近一次网络错误 / 连接状态。
    status: String,
}

/// 自己的速度滑杆值。
#[derive(Clone, Copy, Debug)]
struct SelfSpeeds {
    /// 公转速度。
    orbit: f32,

    /// 自转速度。
    spin: f32,
}

impl Default for SelfSpeeds {
    fn default() -> Self {
        SelfSpeeds {
            orbit: 0.6f32,
            spin: 1.2f32,
        }
    }
}

/// 网络句柄资源。
#[derive(Resource)]
struct NetResource(NetHandle);

/// 行星可视化标记。
#[derive(Component)]
struct PlanetVisual {
    /// 行星标识。
    id: PlanetId,
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
        .add_systems(Startup, setup_scene_)
        .add_systems(
            Update,
            (
                drain_net_events_,
                sync_planet_entities_,
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
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
) {
    commands.spawn((
        Camera3d::default(),
        Transform::from_xyz(0.0f32, 10.0f32, 16.0f32)
            .looking_at(Vec3::ZERO, Vec3::Y),
    ));

    commands.spawn((
        DirectionalLight {
            illuminance: 6_000.0f32,
            ..default()
        },
        Transform::from_rotation(Quat::from_euler(
            EulerRot::XYZ,
            -1.0f32,
            -0.6f32,
            0.0f32,
        )),
    ));
    commands.spawn((
        PointLight {
            intensity: 2_000_000.0f32,
            range: 120.0f32,
            ..default()
        },
        Transform::from_xyz(0.0f32, 6.0f32, 0.0f32),
    ));

    let unit_sphere = meshes.add(Sphere::new(1.0f32).mesh().uv(24u32, 16u32));
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

/// 排空网络事件，更新场景状态。
fn drain_net_events_(mut state: ResMut<SceneState>, net: Res<NetResource>) {
    while let Ok(event) = net.0.event_rx_.try_recv() {
        match event {
            NetEvent::Joined { descriptor } => {
                state.speeds = SelfSpeeds {
                    orbit: descriptor.orbit_speed,
                    spin: descriptor.spin_speed,
                };
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
                mptp_smux_v1_demo::scene::PlanetEvent::Snapshot(planet)
                | mptp_smux_v1_demo::scene::PlanetEvent::Updated(planet) => {
                    state.planets.insert(planet_id, planet.clone());
                    if state.my_planet.as_ref().is_some_and(|mine| mine.id == planet_id) {
                        state.my_planet = Some(planet);
                    }
                }
                mptp_smux_v1_demo::scene::PlanetEvent::Removed { planet_id, .. } => {
                    state.planets.remove(&planet_id);
                    state.subscriptions.remove(&planet_id);
                    if state.my_planet.as_ref().is_some_and(|mine| mine.id == planet_id) {
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

/// 让可视化实体与「已订阅且已知」的行星集合保持一致。
fn sync_planet_entities_(
    mut commands: Commands,
    state: Res<SceneState>,
    assets: Res<PlanetAssets>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    visuals: Query<(Entity, &PlanetVisual)>,
) {
    let mut existing = BTreeSet::new();
    for (entity, visual) in &visuals {
        let visible = state.subscriptions.contains(&visual.id)
            && state.planets.contains_key(&visual.id);
        if visible {
            existing.insert(visual.id);
        } else {
            commands.entity(entity).despawn();
        }
    }

    for (id, planet) in &state.planets {
        if !state.subscriptions.contains(id) || existing.contains(id) {
            continue;
        }
        let radius = radius_of_(planet.id);
        commands.spawn((
            Mesh3d(assets.unit_sphere.clone()),
            MeshMaterial3d(materials.add(StandardMaterial {
                base_color: color_of_(planet.color),
                emissive: LinearRgba::rgb(0.25f32, 0.25f32, 0.25f32),
                perceptual_roughness: 0.6f32,
                ..default()
            })),
            Transform::from_xyz(radius, 0.0f32, 0.0f32)
                .with_scale(Vec3::splat(0.55f32 + radius * 0.03f32)),
            PlanetVisual { id: *id },
        ));
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
        let radius = radius_of_(planet.id);
        let angle = planet.orbit_speed * now + (planet.id as f32) * 0.7f32;
        transform.translation = Vec3::new(angle.cos() * radius, 0.0f32, angle.sin() * radius);
        transform.rotate_local_y(planet.spin_speed * time.delta_secs());
    }
}

/// 摄像机锁定自己的行星。
fn follow_my_planet_(
    state: Res<SceneState>,
    visuals: Query<(&PlanetVisual, &Transform), Without<Camera3d>>,
    mut cameras: Query<&mut Transform, With<Camera3d>>,
) {
    let Some(mine) = state.my_planet.as_ref() else {
        return;
    };
    let Some((_, planet_transform)) = visuals
        .iter()
        .find(|(visual, _)| visual.id == mine.id)
    else {
        return;
    };
    let target = planet_transform.translation;
    for mut camera in &mut cameras {
        camera.translation = target + Vec3::new(0.0f32, 5.0f32, 9.0f32);
        camera.look_at(target, Vec3::Y);
    }
}

/// `bevy_egui` 面板：行星订阅勾选框 + 自己的速度滑杆。
fn scene_ui_(
    mut contexts: EguiContexts,
    mut state: ResMut<SceneState>,
    net: Res<NetResource>,
) -> Result {
    let Ok(context) = contexts.ctx_mut() else {
        return Ok(());
    };
    egui::Window::new("太阳系场景").show(context, |ui| {
        ui.label(&state.status);
        ui.separator();

        ui.heading("自己的速度");
        let mut orbit = state.speeds.orbit;
        let mut spin = state.speeds.spin;
        let orbit_changed = ui
            .add(egui::Slider::new(&mut orbit, 0.0f32..=3.0f32).text("公转速度"))
            .changed();
        let spin_changed = ui
            .add(egui::Slider::new(&mut spin, 0.0f32..=6.0f32).text("自转速度"))
            .changed();
        if orbit_changed || spin_changed {
            state.speeds = SelfSpeeds { orbit, spin };
            let _ = net.0.cmd_tx_.try_send(NetCommand::UpdateSelf {
                orbit_speed: orbit,
                spin_speed: spin,
            });
        }

        ui.separator();
        ui.heading("行星订阅");
        let my_id = state.my_planet.as_ref().map(|planet| planet.id);
        let mut toggles = Vec::new();
        for planet in state.directory.planets.clone() {
            let is_self = my_id == Some(planet.id);
            let mut checked = is_self || state.subscriptions.contains(&planet.id);
            let label = format!(
                "#{} {}（owner {}）",
                planet.id, planet.name, planet.owner
            );
            let response = ui.add_enabled(
                !is_self,
                egui::Checkbox::new(&mut checked, label),
            );
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
                let _ = net.0.cmd_tx_.try_send(NetCommand::Subscribe {
                    planet_id,
                    uri,
                });
            } else {
                state.subscriptions.remove(&planet_id);
                state.planets.remove(&planet_id);
                let _ = net.0.cmd_tx_.try_send(NetCommand::Unsubscribe { planet_id });
            }
        }
    });
    Ok(())
}

/// 行星轨道半径（纯客户端显示选择）。
fn radius_of_(planet_id: PlanetId) -> f32 {
    4.0f32 + ((planet_id % 6u64) as f32) * 1.8f32
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
    let mut server: SocketAddr = K_DEFAULT_SERVER
        .parse()
        .expect("缺省服务器地址必须合法");
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
                name = args
                    .next()
                    .ok_or_else(|| String::from("`--name` 缺少值"))?;
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
