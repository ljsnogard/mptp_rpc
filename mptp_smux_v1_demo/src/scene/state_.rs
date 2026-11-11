//! 场景的业务状态：行星目录 + 每颗行星的订阅中心。
//!
//! 服务端是**单线程**的（所有连接、所有 session 都在同一条 compio 线程上），因此这里
//! 不需要 `Mutex` / `RwLock`：调用方用 `Rc<RefCell<...>>` 持有它们即可。唯一需要跨任务
//! 传递的是每颗行星的广播队列（[`flume`] 的发送端），它本身就是线程安全的。
//!
//! # 广播策略
//!
//! 每条 `Pull` 订阅有自己的**有界**队列与一个取消触发端。广播时只 `try_send`：
//!
//! - 队列未满：事件入队，订阅者按序读到；
//! - 队列已满（慢订阅者）：**移除该订阅并触发它的取消信号**，其他订阅完全不受影响。
//!
//! # `state_`: scene registry and per-planet hubs
//!
//! The server is single-threaded, so the registry and hubs live behind `Rc<RefCell<..>>`.
//! Each subscription has a bounded queue and its own cancel trigger; `broadcast_` only
//! uses `try_send`, so a slow subscriber is dropped without blocking the others.

use std::{
    cell::RefCell,
    collections::BTreeMap,
    rc::Rc,
    sync::Arc,
};

use crate::scene::{
    cancel_::{SceneCancelToken, SceneCanceller, cancel_pair_},
    protocol_::{
        ClientId, PlanetDescriptor, PlanetDirectory, PlanetEvent, PlanetId, planet_uri_,
    },
};

/// 每条订阅的队列容量。
///
/// 它只影响「慢订阅者多久被判定为慢」：队列满即移除该订阅，不阻塞广播方。
pub const K_SUB_QUEUE_CAP: usize = 16usize;

/// 订阅标识。
pub type SubId = u64;

/// 行星目录与版本。
pub struct PlanetRegistry {
    /// 行星标识 → 当前状态。
    planets_: BTreeMap<PlanetId, PlanetDescriptor>,

    /// 目录版本：加入 / 离开时递增。
    directory_version_: u64,

    /// 下一个行星标识。
    next_planet_id_: PlanetId,
}

impl PlanetRegistry {
    /// 造一个空目录。
    pub fn new_() -> Self {
        PlanetRegistry {
            planets_: BTreeMap::new(),
            directory_version_: 1u64,
            next_planet_id_: 1u64,
        }
    }

    /// 为一条连接创建行星；名字为空时按标识生成。
    pub fn allocate_planet_(
        &mut self,
        owner: ClientId,
        name: &str,
        color: u32,
    ) -> PlanetDescriptor {
        let id = self.next_planet_id_;
        self.next_planet_id_ = self.next_planet_id_.saturating_add(1u64);
        let name = if name.is_empty() {
            format!("planet-{id}")
        } else {
            name.to_string()
        };
        let descriptor = PlanetDescriptor {
            id,
            owner,
            name,
            color,
            stream_uri: planet_uri_(id),
            orbit_speed: 0.6f32,
            spin_speed: 1.2f32,
            version: 1u64,
        };
        self.planets_.insert(id, descriptor.clone());
        self.directory_version_ = self.directory_version_.saturating_add(1u64);
        descriptor
    }

    /// 修改行星速度并递增该行星的版本；行星不存在时返回 `None`。
    pub fn update_planet_(
        &mut self,
        planet_id: PlanetId,
        orbit_speed: f32,
        spin_speed: f32,
    ) -> Option<PlanetDescriptor> {
        let planet = self.planets_.get_mut(&planet_id)?;
        planet.orbit_speed = orbit_speed;
        planet.spin_speed = spin_speed;
        planet.version = planet.version.saturating_add(1u64);
        Some(planet.clone())
    }

    /// 删除行星并递增目录版本；返回被删除的行星。
    pub fn remove_planet_(&mut self, planet_id: PlanetId) -> Option<PlanetDescriptor> {
        let removed = self.planets_.remove(&planet_id)?;
        self.directory_version_ = self.directory_version_.saturating_add(1u64);
        Some(removed)
    }

    /// 取一颗行星的当前快照。
    pub fn snapshot_(&self, planet_id: PlanetId) -> Option<PlanetDescriptor> {
        self.planets_.get(&planet_id).cloned()
    }

    /// 当前行星数量。
    pub fn planet_count_(&self) -> usize {
        self.planets_.len()
    }

    /// 当前目录版本。
    pub fn directory_version_(&self) -> u64 {
        self.directory_version_
    }

    /// 生成目录（不含订阅信息）。
    pub fn directory_(&self) -> PlanetDirectory {
        PlanetDirectory {
            directory_version: self.directory_version_,
            planets: self.planets_.values().cloned().collect(),
        }
    }
}

impl Default for PlanetRegistry {
    fn default() -> Self {
        PlanetRegistry::new_()
    }
}

/// 一条订阅：有界队列 + 取消触发端。
struct Subscriber {
    /// 广播帧的发送端。
    tx_: flume::Sender<Arc<[u8]>>,

    /// 慢订阅者被移除时触发。
    cancel_: SceneCanceller,

    /// 拥有该订阅的连接；显式退订按连接身份清理。
    owner_: ClientId,
}

/// 每颗行星的订阅中心。
pub struct SceneHub {
    /// 行星标识 → (订阅标识 → 订阅)。
    streams_: BTreeMap<PlanetId, BTreeMap<SubId, Subscriber>>,

    /// 下一个订阅标识。
    next_sub_id_: SubId,
}

impl SceneHub {
    /// 造一个空 hub。
    pub fn new_() -> Self {
        SceneHub {
            streams_: BTreeMap::new(),
            next_sub_id_: 1u64,
        }
    }

    /// 登记一条订阅，返回它的标识。
    pub fn subscribe_(
        &mut self,
        planet_id: PlanetId,
        owner: ClientId,
        tx: flume::Sender<Arc<[u8]>>,
        cancel: SceneCanceller,
    ) -> SubId {
        let sub_id = self.next_sub_id_;
        self.next_sub_id_ = self.next_sub_id_.saturating_add(1u64);
        self.streams_.entry(planet_id).or_default().insert(
            sub_id,
            Subscriber {
                tx_: tx,
                cancel_: cancel,
                owner_: owner,
            },
        );
        sub_id
    }

    /// 移除某个连接在某颗行星上的全部订阅，并触发它们的取消信号。
    pub fn unsubscribe_connection_(&mut self, planet_id: PlanetId, owner: ClientId) -> usize {
        let Some(streams) = self.streams_.get_mut(&planet_id) else {
            return 0usize;
        };
        let stale: Vec<SubId> = streams
            .iter()
            .filter(|(_, subscriber)| subscriber.owner_ == owner)
            .map(|(sub_id, _)| *sub_id)
            .collect();
        let removed = stale.len();
        for sub_id in stale {
            if let Some(subscriber) = streams.remove(&sub_id) {
                subscriber.cancel_.cancel_();
            }
        }
        removed
    }

    /// 移除一条订阅；不存在时返回 `false`。
    pub fn unsubscribe_(&mut self, planet_id: PlanetId, sub_id: SubId) -> bool {
        let Some(streams) = self.streams_.get_mut(&planet_id) else {
            return false;
        };
        streams.remove(&sub_id).is_some()
    }

    /// 某颗行星当前的订阅者数量（测试 / 诊断用）。
    pub fn subscriber_count_(&self, planet_id: PlanetId) -> usize {
        self.streams_
            .get(&planet_id)
            .map_or(0usize, BTreeMap::len)
    }

    /// 全部行星的订阅者总数（测试 / 诊断用）。
    pub fn total_subscribers_(&self) -> usize {
        self.streams_.values().map(BTreeMap::len).sum()
    }

    /// 向一颗行星的所有订阅者广播一帧；返回因队列满 / 断开而移除的订阅数。
    ///
    /// 被移除的订阅会**同时**收到取消信号：它可能正卡在向网络写入上，只有取消才能
    /// 让它退出并释放资源。
    pub fn broadcast_(&mut self, planet_id: PlanetId, frame: Arc<[u8]>) -> usize {
        let Some(streams) = self.streams_.get_mut(&planet_id) else {
            return 0usize;
        };
        let mut stale = Vec::new();
        for (sub_id, subscriber) in streams.iter() {
            if subscriber.tx_.try_send(frame.clone()).is_err() {
                stale.push(*sub_id);
            }
        }
        let dropped = stale.len();
        for sub_id in stale {
            if let Some(subscriber) = streams.remove(&sub_id) {
                subscriber.cancel_.cancel_();
            }
        }
        dropped
    }

    /// 删除一颗行星的全部订阅；返回被移除的订阅数。
    pub fn remove_planet_(&mut self, planet_id: PlanetId) -> usize {
        let Some(streams) = self.streams_.remove(&planet_id) else {
            return 0usize;
        };
        let count = streams.len();
        for subscriber in streams.into_values() {
            // 行星已经删除：订阅者收到 Disconnected 后自然会结束。
            drop(subscriber.tx_);
        }
        count
    }
}

impl Default for SceneHub {
    fn default() -> Self {
        SceneHub::new_()
    }
}

/// 一条连接的场景上下文：连接身份、自己的行星、目录与 hub。
///
/// 单线程使用，因此用 `RefCell` / `Cell` 而不是锁；绝不在 `await` 期间持有借用。
pub struct ConnectionCtx {
    /// 连接标识。
    connection_id_: ClientId,

    /// 本连接创建的行星（每个连接只允许 join 一次）。
    own_planet_: RefCell<Option<PlanetId>>,

    /// 场景目录。
    registry_: Rc<RefCell<PlanetRegistry>>,

    /// 订阅中心。
    hub_: Rc<RefCell<SceneHub>>,

    /// 本连接所有 session 的取消触发端。
    cancel_: SceneCanceller,

    /// 本连接所有 session 的取消令牌。
    token_: SceneCancelToken,

    /// 最近一次收到请求的时刻；连接级保活判据。
    last_seen_: std::cell::Cell<std::time::Instant>,
}

impl ConnectionCtx {
    /// 造一条连接的上下文。
    pub fn new_(
        connection_id: ClientId,
        registry: Rc<RefCell<PlanetRegistry>>,
        hub: Rc<RefCell<SceneHub>>,
    ) -> Self {
        let (cancel_, token_) = cancel_pair_();
        ConnectionCtx {
            connection_id_: connection_id,
            own_planet_: RefCell::new(None),
            registry_: registry,
            hub_: hub,
            cancel_,
            token_,
            last_seen_: std::cell::Cell::new(std::time::Instant::now()),
        }
    }

    /// 连接标识。
    pub fn connection_id_(&self) -> ClientId {
        self.connection_id_
    }

    /// 自己的行星（若已 join）。
    pub fn own_planet_(&self) -> Option<PlanetId> {
        *self.own_planet_.borrow()
    }

    /// 记下自己的行星；已经 join 过时返回 `false`。
    pub fn claim_own_planet_(&self, planet_id: PlanetId) -> bool {
        let mut slot = self.own_planet_.borrow_mut();
        if slot.is_some() {
            return false;
        }
        *slot = Some(planet_id);
        true
    }

    /// 目录句柄。
    pub fn registry_(&self) -> &Rc<RefCell<PlanetRegistry>> {
        &self.registry_
    }

    /// 订阅中心句柄。
    pub fn hub_(&self) -> &Rc<RefCell<SceneHub>> {
        &self.hub_
    }

    /// 本连接所有 session 的取消令牌。
    pub fn token_(&self) -> SceneCancelToken {
        self.token_.clone()
    }

    /// 取消本连接所有 session（连接断开时调用）。
    pub fn cancel_(&self) {
        self.cancel_.cancel_();
    }

    /// 记下「刚刚收到过请求」。
    pub fn touch_(&self) {
        self.last_seen_.set(std::time::Instant::now());
    }

    /// 距离上一次请求过去了多久。
    pub fn idle_for_(&self) -> std::time::Duration {
        self.last_seen_.get().elapsed()
    }

    /// 连接离开：删除自己的行星、向订阅者广播 `Removed` 并清空该行星的订阅。
    ///
    /// 幂等：重复调用不会重复删除或重复广播。编码在借用目录 / hub 之前完成，因此不会
    /// 在 `await` 期间持有 `RefCell` 借用。
    pub async fn cleanup_async_(&self) {
        // 本连接不会再产生新的 session；让在途的 Pull 写立即退出。
        self.cancel_.cancel_();
        let Some(planet_id) = self.own_planet_.borrow_mut().take() else {
            return;
        };
        let directory_version = self.registry_.borrow().directory_version_();
        let event = PlanetEvent::Removed {
            planet_id,
            version: directory_version,
        };
        if let Ok(frame) = crate::scene::protocol_::encode_event_async_(&event).await {
            self.hub_.borrow_mut().broadcast_(planet_id, frame);
        }
        let _ = self.registry_.borrow_mut().remove_planet_(planet_id);
        let _ = self.hub_.borrow_mut().remove_planet_(planet_id);
    }

    /// 只能同步完成的兜底清理：不广播 `Removed`，只把行星与订阅从表里摘掉。
    ///
    /// 正常路径用 [`Self::cleanup_async_`]；本方法只由 `Drop` 守卫在任务被取消时调用。
    pub fn cleanup_sync_(&self) {
        let Some(planet_id) = self.own_planet_.borrow_mut().take() else {
            return;
        };
        let _ = self.registry_.borrow_mut().remove_planet_(planet_id);
        let _ = self.hub_.borrow_mut().remove_planet_(planet_id);
    }
}
