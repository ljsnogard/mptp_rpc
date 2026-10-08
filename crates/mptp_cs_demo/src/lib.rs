#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

//! `mptp_core` 的端到端验证：进程内的客户端 / 服务端环回。
//!
//! # 为什么验证代码放在这里，而不是 `mptp_core` 的 `tests/`
//!
//! 跑真实复用连接需要一整套装配（连接配置、传输环、握手、运行时作用域）。把它塞进
//! `mptp_core` 的 dev-dependencies，等于让「核心协议定义」这个 crate 被迫跟具体的
//! 传输实现一起演进。本 crate 反过来：它依赖 `mptp_core`，可以自由依赖 `smux_v1`。
//!
//! # 形状
//!
//! - [`mux_`]：进程内的连接装配（两条全被动环 + 本地握手）；
//! - [`client_`]：客户端侧配置与一次请求；
//! - [`server_`]：服务端侧配置、handler 与一次服务；
//! - [`run_local_roundtrip_`]：把两侧拼起来跑一次完整往返。

pub mod client_;
pub mod mux_;
pub mod server_;

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrMayCancel};
use abs_mm::CoreAlloc;
use abs_smux::conn::TrConnection;
use anyhow::{Result, anyhow};
pub use client_::{DemoClientAllocCfg, DemoClientCfg, run_client_};
use mm_ptr::{Shared, x_deps::abs_mm};
use mptp_core::{serving::server::SessionContext, specs::Status};
pub use mux_::{DemoConn, DemoRingBuff, connect_pair_};
pub use server_::{DemoServingAllocCfg, DemoServingCfg, build_server_, serve_one_channel_};
use smux_v1::{
    connection::Dock,
    x_deps::{abs_buff, abs_smux, mm_ptr},
};

use crate::mux_::K_LISTEN_DOCK;

/// 跑一次完整的进程内往返：
/// 建一对连接 → 服务端在 [`K_LISTEN_DOCK`] 上监听并服务一条子流 →
/// 客户端向同一个 dock 发起 `View /hello` → 返回对端给出的状态码。
///
/// # Errors
///
/// 建连、绑定 dock、建流、解码、路由或写回任一步失败都会带上下文返回 `Err`。
///
/// # Examples
///
/// ```no_run
/// # async fn demo() -> anyhow::Result<()> {
/// use mptp_cs_demo::run_local_roundtrip_;
/// use smux_v1::{connection::ScopeHost, x_deps::{abs_art, abs_art_bridge}};
/// use abs_art::TrLocalScope;
///
/// let rt = <abs_art_bridge::Runtime<{ abs_art::FULL }>>::current();
/// let scope = rt.local_scope();
/// let status = scope.run_until(run_local_roundtrip_()).await?;
/// assert_eq!(status, mptp_core::specs::Status::Ok);
/// # Ok(())
/// # }
/// ```
pub async fn run_local_roundtrip_() -> Result<Status> {
    let (conn_a, conn_b) = connect_pair_().await?;
    let server = build_server_();

    // 服务端侧：绑定一个**明确**的 dock 并监听。
    let mut binding = conn_b
        .bind_async(Dock::new(K_LISTEN_DOCK))
        .may_cancel_with(NonCancellableToken::new())
        .await
        .map_err(|err| anyhow!("服务端绑定 dock 失败: {err}"))?;
    let mut context = SessionContext;

    // 客户端侧：连接对象经 `Shared` 交给客户端（客户端只持有智能指针，不持有连接本身）。
    let client_conn = Shared::new(conn_a, CoreAlloc);

    let (server_res, client_res) = tokio::join!(
        serve_one_channel_(&server, &mut binding, &mut context),
        run_client_(client_conn, K_LISTEN_DOCK),
    );
    server_res?;
    client_res
}
