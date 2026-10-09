#![feature(allocator_ext)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

pub mod access_method;
pub mod client;
pub mod codec;
pub mod messaging;
pub mod routing;
pub mod serving;
pub mod specs;
pub mod transport;

pub mod x_deps {
    pub use abs_art_bridge;
    pub use abs_buff::x_deps::{abs_cancel, anylr, gen_mcf2, funty};
    pub use abs_buff_stdio_adapt;
    pub use abs_buff_stdio_adapt::x_deps::abs_buff;
    pub use abs_smux;

    pub use mm_ptr;
}
