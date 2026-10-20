pub mod clients;
pub mod config;

// 七种 access method 的客户端标记 trait 与 RPC 形状本身并列出：使用者既能用形状
// （`TrResrClient` / `TrSessClient`），也能用「某一种方法」的名字（`TrHeadClient` / ……）
// 写约束，不必自己去 `clients::` 里找。
//
// `CALL` 的标记 trait（`TrCallClient`）目前**还没有**：它是形状库里唯一的空缺，见
// `clients.rs` 末尾的说明。
pub use clients::{TrRpcRequest, TrRespClient, TrRpcSession, TrSessClient};

pub mod x_deps {
    pub use mptp_smux;
    pub use mptp_smux::x_deps::{
        abs_buff, abs_cancel, abs_mm, abs_mptp, abs_smux, mm_ptr,
    };
    pub use abs_buff::x_deps::{funty, gen_mcf2};
}
