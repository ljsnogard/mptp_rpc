//! 场景协议的无 Bevy 集成测试矩阵。
//!
//! 每个用例都跑在**真实 TCP** 上：`compio` 单线程运行时、`smux_v1` 握手、`MptpServlet` /
//! `MptpClient`、`SuffixStream` 后缀流。测试夹具在 `tests/common/mod.rs`。
//!
//! # `scene_stream_`: the Bevy-free scene contract matrix
//!
//! Every case uses a real TCP connection, smux handshake, `MptpServlet` / `MptpClient`
//! and a `SuffixStream` suffix. Fixtures live in `tests/common/mod.rs`.

mod common;

use std::time::Duration;

use common::{
    K_QUIET_WINDOW, SceneFixture, SceneTestClient, connect_and_join_, params_with_speeds_,
    wait_subscribers_, with_timeout_,
};
use mptp_smux_v1_demo::scene::{
    PlanetDescriptor, PlanetEvent, PlanetId, PlanetParams, PlanetTheme,
    protocol_::{K_ORBIT_RADIUS_MAX, K_ORBIT_RADIUS_MIN, K_SCENE_PLANETS, planet_uri_},
};

/// 期待事件是某个行星的快照。
fn expect_snapshot_(event: PlanetEvent, planet_id: PlanetId) -> PlanetDescriptor {
    match event {
        PlanetEvent::Snapshot(planet) => {
            assert_eq!(planet.id, planet_id, "快照行星标识不符");
            planet
        }
        other => panic!("期待 Snapshot({planet_id})，实际是 {other:?}"),
    }
}

/// 期待事件是某个行星的更新。
fn expect_updated_(event: PlanetEvent, planet_id: PlanetId) -> PlanetDescriptor {
    match event {
        PlanetEvent::Updated(planet) => {
            assert_eq!(planet.id, planet_id, "更新行星标识不符");
            planet
        }
        other => panic!("期待 Updated({planet_id})，实际是 {other:?}"),
    }
}

/// 测试 `SuffixStream` 连续写多个 chunk 时，客户端按写入顺序读到。
/// Test that a `SuffixStream` preserves the order of multiple chunks.
///
/// - 手段：连接测试服务端的 `/test/chunks`，它在同一个后缀流里依次写三个 denco 事件，
///   然后 `finish_async`；客户端逐值解码。
/// - 判断：三个事件的 `planet_id` 依次为 1、2、3，且随后读端以 EOF 结束——顺序与
///   chunk 写入顺序一致，流不会在第一个对象后关闭。
///
/// - Means: pull `/test/chunks`, which writes three denco events as three chunks and then
///   finishes; the client decodes them one by one.
/// - Verdict: the ids are 1, 2, 3 in order, and the stream then ends with EOF.
#[compio::test]
async fn suffix_stream_writes_chunks_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");
    let mut pull = client
        .pull_async_("/test/chunks")
        .await
        .expect("订阅 chunks");

    for expected in 1u64..=3u64 {
        let event = with_timeout_(pull.next_event_async_())
            .await
            .expect("读事件");
        match event {
            PlanetEvent::Removed { planet_id, version } => {
                assert_eq!(planet_id, expected);
                assert_eq!(version, expected);
            }
            other => panic!("期待第 {expected} 个 Removed，实际是 {other:?}"),
        }
    }

    // 流已经半关闭：再读应当失败（队列断开）。
    assert!(pull.next_event_async_().await.is_err(), "流应当已结束");
}

/// 测试一个 denco 对象拆成两个 chunk 写入后仍能正确解码。
/// Test that one denco object split across two chunks still decodes correctly.
///
/// - 手段：`/test/split` 把一个 `PlanetEvent::Updated` 的编码字节从中间切开，分两次
///   `write_chunk_async` 写入。
/// - 判断：客户端解出的就是同一个事件：id、名字、颜色、速度、版本全部相等——chunk
///   边界不参与语义。
///
/// - Means: `/test/split` writes one encoded `Updated` in two chunks at the midpoint.
/// - Verdict: the decoded event matches exactly; chunk boundaries carry no semantics.
#[compio::test]
async fn object_split_across_chunks_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let client = SceneTestClient::connect_async_(fixture.addr_())
        .await
        .expect("连接夹具");
    let mut pull = client.pull_async_("/test/split").await.expect("订阅 split");

    let event = with_timeout_(pull.next_event_async_())
        .await
        .expect("读事件");
    let planet = expect_updated_(event, 7u64);
    assert_eq!(planet.name, "split-event");
    assert_eq!(planet.params.color, 0x12_34_56u32);
    assert_eq!(planet.params.orbit_speed, 0.5f32);
    assert_eq!(planet.params.spin_speed, 0.25f32);
    assert_eq!(planet.version, 3u64);
}

/// 测试无限流不会因为一个对象写完而结束。
/// Test that an infinite stream does not end after one object.
///
/// - 手段：A 订阅 B 的行星；B 连续修改 5 次自己的速度，A 逐条读更新。
/// - 判断：A 依次收到 5 条 `Updated`，版本严格递增；期间流一直保持可读。
///
/// - Means: A subscribes to B's planet and B updates five times; A reads each update.
/// - Verdict: A receives five `Updated` events with strictly increasing versions.
#[compio::test]
async fn infinite_stream_stays_open_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 0x111111)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 0x222222)
        .await
        .expect("B join");
    let mut pull = a.pull_async_(&b_planet.stream_uri).await.expect("A 订阅 B");
    let snapshot = expect_snapshot_(
        with_timeout_(pull.next_event_async_()).await.expect("快照"),
        b_planet.id,
    );
    assert_eq!(snapshot.version, 1u64);

    let mut last_version = snapshot.version;
    for round in 0..5u32 {
        let ack = b
            .update_async_(params_with_speeds_(
                &b_planet,
                round as f32 * 0.1f32,
                round as f32 * 0.2f32,
            ))
            .await
            .expect("B 更新");
        let event = with_timeout_(pull.next_event_async_()).await.expect("更新");
        let planet = expect_updated_(event, b_planet.id);
        assert_eq!(planet.version, ack.version);
        assert!(planet.version > last_version);
        last_version = planet.version;
    }
}

/// 测试 `View /scene/planets` 列出所有行星，即使无人订阅。
/// Test that `View /scene/planets` lists every planet even with no subscribers.
///
/// - 手段：A、B 各 join，A 发 `View`，不建立任何 `Pull`。
/// - 判断：目录里恰好有两条行星，URI 分别对应 A、B；hub 的订阅者总数为 0——目录不
///   建立订阅。
///
/// - Means: A and B join, A sends `View` without pulling anything.
/// - Verdict: the directory has both URIs and the hub has zero subscribers.
#[compio::test]
async fn directory_view_lists_streams_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, a_planet) = connect_and_join_(&fixture, "a", 0xAAAAAA)
        .await
        .expect("A join");
    let (_b, b_planet) = connect_and_join_(&fixture, "b", 0xBBBBBB)
        .await
        .expect("B join");

    let directory = a.view_async_().await.expect("View 目录");
    assert_eq!(directory.planets.len(), 2usize);
    let uris: Vec<&str> = directory
        .planets
        .iter()
        .map(|planet| planet.stream_uri.as_str())
        .collect();
    assert!(uris.contains(&a_planet.stream_uri.as_str()));
    assert!(uris.contains(&b_planet.stream_uri.as_str()));
    assert_eq!(fixture.hub_().lock().unwrap().total_subscribers_(), 0usize);
}

/// 测试 `Call /scene/join` 返回自己的行星标识与 URI。
/// Test that `Call /scene/join` returns the caller's own planet id and URI.
///
/// - 手段：A join，并把服务端目录里的快照与返回值比较。
/// - 判断：`stream_uri == /scene/planets/{id}`，目录里存在同一 descriptor；owner 非 0。
///
/// - Means: A joins, then compare the returned descriptor with the registry snapshot.
/// - Verdict: the URI is `/scene/planets/{id}`, the registry holds the same descriptor,
///   and the owner is non-zero.
#[compio::test]
async fn join_returns_own_descriptor_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (client, planet) = connect_and_join_(&fixture, "solo", 0x00FF00)
        .await
        .expect("join");

    assert!(planet.id > 0u64);
    assert_eq!(planet.stream_uri, planet_uri_(planet.id));
    assert!(planet.owner > 0u64);
    assert_eq!(planet.name, "solo");
    assert_eq!(planet.params.color, 0x00FF00u32);
    assert_eq!(planet.params.texture_theme, PlanetTheme::Terran.wire_());
    assert!(
        (K_ORBIT_RADIUS_MIN..=K_ORBIT_RADIUS_MAX).contains(&planet.params.orbit_radius),
        "初始轨道半径应在支持范围内"
    );

    let directory = client.view_async_().await.expect("View");
    let found = directory
        .planets
        .iter()
        .find(|candidate| candidate.id == planet.id)
        .expect("目录里应当有自己的行星");
    assert_eq!(found, &planet);
}

/// 测试 `Pull` 的第一帧是 `Snapshot`，之后是 `Updated`。
/// Test that a `Pull` starts with `Snapshot` and then delivers `Updated`.
///
/// - 手段：A 订阅 B，先收快照；B 改速度后再收一条。
/// - 判断：第一帧一定是 Snapshot（版本 1），第二帧是 Updated（版本 2 且速度等于 ack）。
///
/// - Means: A pulls B, reads the first frame, then B updates and A reads again.
/// - Verdict: frame one is `Snapshot` v1; frame two is `Updated` v2 with the ack's speeds.
#[compio::test]
async fn pull_sends_snapshot_then_updates_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");
    let mut pull = a.pull_async_(&b_planet.stream_uri).await.expect("订阅");
    let snapshot = expect_snapshot_(
        with_timeout_(pull.next_event_async_()).await.expect("快照"),
        b_planet.id,
    );
    assert_eq!(snapshot.version, 1u64);

    let ack = b
        .update_async_(params_with_speeds_(&b_planet, 1.5f32, 2.5f32))
        .await
        .expect("B 更新");
    let updated = expect_updated_(
        with_timeout_(pull.next_event_async_()).await.expect("更新"),
        b_planet.id,
    );
    assert_eq!(updated.version, ack.version);
    assert_eq!(updated.params.orbit_speed, 1.5f32);
    assert_eq!(updated.params.spin_speed, 2.5f32);
}

/// 测试轨道半径、行星半径、颜色与纹理主题的完整参数更新会广播给订阅者。
/// Test that a full parameter update (radii, color, theme) is broadcast to subscribers.
///
/// - 手段：A 订阅 B 并读快照；B 一次性提交新的轨道半径、行星半径、颜色和纹理主题。
/// - 判断：A 收到 `Updated`，其完整 `params` 等于提交值，也等于 `UpdateAck.params`，
///   且版本在快照基础上加一。
///
/// - Means: A pulls B and reads the snapshot; B submits new orbit radius, planet radius,
///   color and texture theme in one update.
/// - Verdict: A receives an `Updated` whose full params equal the submitted and ack params,
///   with the version one above the snapshot.
#[compio::test]
async fn update_broadcasts_visual_params_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");
    let mut pull = a.pull_async_(&b_planet.stream_uri).await.expect("订阅 B");
    let snapshot = expect_snapshot_(
        with_timeout_(pull.next_event_async_())
            .await
            .expect("B 快照"),
        b_planet.id,
    );

    let params = PlanetParams {
        orbit_radius: 9.5f32,
        planet_radius: 1.25f32,
        color: 0x12_34_56u32,
        texture_theme: PlanetTheme::Lava.wire_(),
        ..snapshot.params
    };
    let ack = b.update_async_(params).await.expect("B 更新外观");
    let updated = expect_updated_(
        with_timeout_(pull.next_event_async_())
            .await
            .expect("B 外观更新"),
        b_planet.id,
    );

    assert_eq!(updated.version, snapshot.version + 1u64);
    assert_eq!(updated.params, ack.params);
    assert_eq!(updated.params, params);
}

/// 测试选择性订阅：只订阅 B 时，C 的更新不会到达。
/// Test that selective subscription keeps C's updates away when only B is pulled.
///
/// - 手段：A 只 Pull B；B、C 各更新一次。
/// - 判断：A 收到 B 的 `Updated`；随后在观察窗口内没有 C 的事件。
///
/// - Means: A pulls only B; both B and C update once.
/// - Verdict: A sees B's update and then stays quiet for C.
#[compio::test]
async fn selective_subscription_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");
    let (c, c_planet) = connect_and_join_(&fixture, "c", 3u32)
        .await
        .expect("C join");

    let mut pull = a.pull_async_(&b_planet.stream_uri).await.expect("订阅 B");
    expect_snapshot_(
        with_timeout_(pull.next_event_async_())
            .await
            .expect("B 快照"),
        b_planet.id,
    );

    b.update_async_(params_with_speeds_(&b_planet, 0.3f32, 0.4f32))
        .await
        .expect("B 更新");
    let event = with_timeout_(pull.next_event_async_())
        .await
        .expect("B 更新事件");
    expect_updated_(event, b_planet.id);

    c.update_async_(params_with_speeds_(&c_planet, 0.5f32, 0.6f32))
        .await
        .expect("C 更新");
    let quiet = pull
        .next_event_within_async_(K_QUIET_WINDOW)
        .await
        .expect("等待窗口");
    assert!(quiet.is_none(), "未订阅的 C 不应到达：{quiet:?}");
    assert_eq!(
        fixture.hub_().lock().unwrap().subscriber_count_(c_planet.id),
        0usize
    );
}

/// 测试丢弃 `Pull` 会话后，服务端订阅者归零且后续事件不再到达。
/// Test that dropping a `Pull` removes the server-side subscriber.
///
/// - 手段：A 订阅 B，然后显式退订；服务端 hub 计数等待归零；B 再更新。
/// - 判断：计数从 1 变 0；A 的连接仍然可用（还能 `View`）。
///
/// - Means: A pulls B then unsubscribes; wait for the hub count to reach zero; B updates.
/// - Verdict: the count goes 1 → 0 and A's connection remains usable for `View`.
#[compio::test]
async fn unsubscribe_stops_events_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");

    let mut pull = a.pull_async_(&b_planet.stream_uri).await.expect("订阅");
    expect_snapshot_(
        with_timeout_(pull.next_event_async_()).await.expect("快照"),
        b_planet.id,
    );
    assert_eq!(
        fixture.hub_().lock().unwrap().subscriber_count_(b_planet.id),
        1usize
    );

    pull.unsubscribe_async_().await.expect("退订");
    drop(pull);
    wait_subscribers_(&fixture, b_planet.id, 0usize).await;

    b.update_async_(params_with_speeds_(&b_planet, 0.1f32, 0.2f32))
        .await
        .expect("B 更新");
    let directory = a.view_async_().await.expect("退订后连接仍可用");
    assert_eq!(directory.planets.len(), 2usize);
}

/// 测试重新订阅会先收到最新 `Snapshot`，而不是旧更新。
/// Test that resubscribing starts with a fresh snapshot, not stale updates.
///
/// - 手段：A 订阅 B 后丢弃；B 改速度；A 再次订阅同一条 URI。
/// - 判断：新流的第一帧是 Snapshot，版本与速度都是最新的。
///
/// - Means: A subscribes, drops, B updates, A subscribes again.
/// - Verdict: the new stream's first frame is a Snapshot with the latest version and
///   speeds.
#[compio::test]
async fn resubscribe_gets_fresh_snapshot_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");

    let mut first = a.pull_async_(&b_planet.stream_uri).await.expect("首次订阅");
    expect_snapshot_(
        with_timeout_(first.next_event_async_())
            .await
            .expect("快照"),
        b_planet.id,
    );
    first.unsubscribe_async_().await.expect("首次退订");
    drop(first);
    wait_subscribers_(&fixture, b_planet.id, 0usize).await;

    let ack = b
        .update_async_(params_with_speeds_(&b_planet, 2.5f32, 4.5f32))
        .await
        .expect("B 更新");
    let mut second = a.pull_async_(&b_planet.stream_uri).await.expect("重新订阅");
    let snapshot = expect_snapshot_(
        with_timeout_(second.next_event_async_())
            .await
            .expect("新快照"),
        b_planet.id,
    );
    assert_eq!(snapshot.version, ack.version);
    assert_eq!(snapshot.params.orbit_speed, 2.5f32);
    assert_eq!(snapshot.params.spin_speed, 4.5f32);
}

/// 测试同一连接可以同时持有多个 `Pull`，各自独立。
/// Test that one connection can hold multiple independent pulls.
///
/// - 手段：A 同时订阅 B、C；B 与 C 各更新一次。
/// - 判断：两个 `Pull` 各自收到本行星的更新，互不串流。
///
/// - Means: A pulls B and C at the same time; both update once.
/// - Verdict: each pull receives its own planet's update.
#[compio::test]
async fn multiple_subscriptions_one_connection_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");
    let (c, c_planet) = connect_and_join_(&fixture, "c", 3u32)
        .await
        .expect("C join");

    let mut pull_b = a.pull_async_(&b_planet.stream_uri).await.expect("订阅 B");
    let mut pull_c = a.pull_async_(&c_planet.stream_uri).await.expect("订阅 C");
    expect_snapshot_(
        with_timeout_(pull_b.next_event_async_())
            .await
            .expect("B 快照"),
        b_planet.id,
    );
    expect_snapshot_(
        with_timeout_(pull_c.next_event_async_())
            .await
            .expect("C 快照"),
        c_planet.id,
    );

    b.update_async_(params_with_speeds_(&b_planet, 0.7f32, 0.8f32))
        .await
        .expect("B 更新");
    c.update_async_(params_with_speeds_(&c_planet, 0.9f32, 1.0f32))
        .await
        .expect("C 更新");

    let updated_b = expect_updated_(
        with_timeout_(pull_b.next_event_async_())
            .await
            .expect("B 更新"),
        b_planet.id,
    );
    let updated_c = expect_updated_(
        with_timeout_(pull_c.next_event_async_())
            .await
            .expect("C 更新"),
        c_planet.id,
    );
    assert_eq!(updated_b.params.orbit_speed, 0.7f32);
    assert_eq!(updated_c.params.orbit_speed, 0.9f32);
}

/// 测试两个客户端 join 后可以互相订阅并看到对方。
/// Test that two clients see each other after both join.
///
/// - 手段：A、B 各 join；A 订阅 B，B 订阅 A。
/// - 判断：双方各自收到的第一帧都是对方行星的 Snapshot。
///
/// - Means: A and B join, then subscribe to each other's stream.
/// - Verdict: each side's first frame is the other's snapshot.
#[compio::test]
async fn two_clients_see_each_other_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, a_planet) = connect_and_join_(&fixture, "a", 0x010203)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 0x040506)
        .await
        .expect("B join");

    let mut a_pull_b = a.pull_async_(&b_planet.stream_uri).await.expect("A 订阅 B");
    let mut b_pull_a = b.pull_async_(&a_planet.stream_uri).await.expect("B 订阅 A");

    let seen_by_a = expect_snapshot_(
        with_timeout_(a_pull_b.next_event_async_())
            .await
            .expect("A 看 B"),
        b_planet.id,
    );
    let seen_by_b = expect_snapshot_(
        with_timeout_(b_pull_a.next_event_async_())
            .await
            .expect("B 看 A"),
        a_planet.id,
    );
    assert_eq!(seen_by_a.name, "b");
    assert_eq!(seen_by_b.name, "a");
}

/// 测试未知行星返回 404。
/// Test that an unknown planet returns 404.
///
/// - 手段：A join 后 Pull 一个不存在的 id。
/// - 判断：状态码是 404，而不是 200 或 500。
///
/// - Means: after joining, A pulls a non-existent planet id.
/// - Verdict: the status is 404.
#[compio::test]
async fn unknown_planet_is_404_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let status = a
        .pull_status_async_("/scene/planets/999999")
        .await
        .expect("Pull 未知行星");
    assert_eq!(status.inner(), 404u16);
}

/// 测试不允许的方法返回 405 并带 `Allow` 头。
/// Test that a disallowed method returns 405 with an `Allow` header.
///
/// - 手段：A 对 `/scene/planets` 发 `Post`。
/// - 判断：状态 405；`Allow` 头包含本路径（`/scene/planets`）允许的 HEAD / VIEW。
///
/// - Means: A sends `Post` to `/scene/planets`.
/// - Verdict: status 405 and `Allow` lists HEAD / VIEW for this path.
#[compio::test]
async fn post_is_405_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (status, allow) = a
        .post_allow_async_(K_SCENE_PLANETS)
        .await
        .expect("Post 目录");
    assert_eq!(status.inner(), 405u16);
    let allow = allow.expect("405 应当带 Allow 头");
    for method in ["HEAD", "VIEW"] {
        assert!(allow.contains(method), "Allow 缺少 {method}：{allow:?}");
    }
    assert!(!allow.contains("POST"), "Allow 不应包含 POST：{allow:?}");
}

/// 测试退订只清理自己的订阅，不影响其他订阅。
/// Test that unsubscribing cleans up only the caller's subscription.
///
/// - 手段：A、C 同时订阅 B；A 退订。
/// - 判断：B 的订阅者从 2 变 1；C 仍能收到 B 的更新；C 退订后归零。
///
/// - Means: A and C both pull B; A unsubscribes.
/// - Verdict: the subscriber count goes 2 → 1, C still receives updates, then 0.
#[compio::test]
async fn subscription_cleanup_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    let (b, b_planet) = connect_and_join_(&fixture, "b", 2u32)
        .await
        .expect("B join");
    let (c, _c_planet) = connect_and_join_(&fixture, "c", 3u32)
        .await
        .expect("C join");

    let mut pull_a = a.pull_async_(&b_planet.stream_uri).await.expect("A 订阅");
    let mut pull_c = c.pull_async_(&b_planet.stream_uri).await.expect("C 订阅");
    expect_snapshot_(
        with_timeout_(pull_a.next_event_async_())
            .await
            .expect("A 快照"),
        b_planet.id,
    );
    expect_snapshot_(
        with_timeout_(pull_c.next_event_async_())
            .await
            .expect("C 快照"),
        b_planet.id,
    );
    assert_eq!(
        fixture.hub_().lock().unwrap().subscriber_count_(b_planet.id),
        2usize
    );

    pull_a.unsubscribe_async_().await.expect("A 退订");
    drop(pull_a);
    wait_subscribers_(&fixture, b_planet.id, 1usize).await;

    b.update_async_(params_with_speeds_(&b_planet, 0.11f32, 0.12f32))
        .await
        .expect("B 更新");
    let event = with_timeout_(pull_c.next_event_async_())
        .await
        .expect("C 更新");
    expect_updated_(event, b_planet.id);

    pull_c.unsubscribe_async_().await.expect("C 退订");
    drop(pull_c);
    wait_subscribers_(&fixture, b_planet.id, 0usize).await;
}

/// 测试慢订阅者被移除后，其他订阅继续收到事件。
/// Test that a slow subscriber is dropped while others keep receiving.
///
/// - 手段：A 以容量 1 的本地队列订阅 B 且从不读取（模拟慢客户端）；C 以大队列订阅 B；
///   B 连续发送大量更新。
/// - 判断：服务端最终只剩 C 一个订阅者；C 仍然能读到多条 `Updated`——慢订阅者没有
///   阻塞广播，也没有拖住其他客户端。
///
/// - Means: A pulls B with a one-slot local queue and never reads; C pulls B with a large
///   queue; B sends many updates.
/// - Verdict: only C remains subscribed and still receives updates; A never blocks the
///   broadcast.
#[compio::test]
async fn slow_subscriber_does_not_block_others_() {
    let fixture = SceneFixture::start_async_().await.expect("启动夹具");
    let (a, _a_planet) = connect_and_join_(&fixture, "a", 1u32)
        .await
        .expect("A join");
    // 长名字让每一帧足够大，更快填满慢订阅者的环与队列。
    let long_name = "b".repeat(2048usize);
    let (b, b_planet) = connect_and_join_(&fixture, &long_name, 2u32)
        .await
        .expect("B join");
    let (c, _c_planet) = connect_and_join_(&fixture, "c", 3u32)
        .await
        .expect("C join");

    let slow = a
        .pull_with_capacity_async_(&b_planet.stream_uri, 1usize)
        .await
        .expect("A 慢订阅");
    let mut fast = c
        .pull_with_capacity_async_(&b_planet.stream_uri, 1024usize)
        .await
        .expect("C 正常订阅");
    // 先让两条订阅都建立并收到快照。
    expect_snapshot_(
        with_timeout_(fast.next_event_async_())
            .await
            .expect("C 快照"),
        b_planet.id,
    );

    for round in 0..240u32 {
        b.update_async_(params_with_speeds_(
            &b_planet,
            round as f32 * 0.01f32,
            round as f32 * 0.02f32,
        ))
        .await
        .expect("B 更新");
    }

    // A 的慢订阅最终被移除，只剩 C。
    let deadline = std::time::Instant::now() + Duration::from_secs(10u64);
    loop {
        if fixture.hub_().lock().unwrap().subscriber_count_(b_planet.id) == 1usize {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "慢订阅者没有被移除（仍在 {}）",
            fixture.hub_().lock().unwrap().subscriber_count_(b_planet.id)
        );
        compio::time::sleep(Duration::from_millis(10u64)).await;
    }

    // C 仍然在读：至少再收到一条 Updated。
    let mut updated = 0usize;
    for _ in 0..32usize {
        match fast
            .next_event_within_async_(Duration::from_millis(200u64))
            .await
        {
            Ok(Some(PlanetEvent::Updated(planet))) => {
                assert_eq!(planet.id, b_planet.id);
                updated += 1usize;
            }
            Ok(Some(other)) => panic!("C 收到意外事件：{other:?}"),
            Ok(None) | Err(_) => break,
        }
    }
    assert!(updated > 0usize, "正常订阅者应当继续收到更新");
    drop(slow);
}
