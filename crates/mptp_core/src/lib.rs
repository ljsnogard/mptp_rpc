#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod access_method;
pub mod client;
pub mod codec;
pub mod messaging;
pub mod routing;
// TODO(重构): `serving` 仍停在旧形状上——它引用已被删除的 `codec::channel::RpcChannel`
// 与不存在的 `transport::TrChannel`。这里先把它从 crate 里摘掉，等它改用 `abs_smux`
// 提供的 channel 概念重新实现之后再放回来。
// pub mod serving;
pub mod specs;

mod std_io_adapt_;

pub mod transport;

pub mod x_deps {
    pub use abs_buff_stdio_adapt;
}
