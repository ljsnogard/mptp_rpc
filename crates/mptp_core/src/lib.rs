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
    pub use abs_buff_stdio_adapt;
}
