//! 场景层：业务协议、目录与订阅中心、compio 配置、连接装配与单一 handler。
//!
//! 传输全部经 `mptp_smux` 公开 API；本模块只放业务状态与装配。

pub mod cancel_;
pub mod cfg_;
pub mod client_;
pub mod conn_;
pub mod handler_;
pub mod net_;
pub mod protocol_;
pub mod server_;
pub mod state_;

pub use cfg_::{ClientCfg, ServingCfg};
pub use client_::SceneClient;
pub use net_::{NetCommand, NetEvent, NetHandle, spawn_net_thread_};
pub use handler_::SceneHandler;
pub use protocol_::{
    PlanetDescriptor, PlanetDirectory, PlanetEvent, PlanetId, UpdateAck, UpdateRequest,
};
pub use state_::{ConnectionCtx, PlanetRegistry, SceneHub};
