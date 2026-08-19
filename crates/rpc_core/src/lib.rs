#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]
#![feature(async_fn_traits)]
#![feature(try_trait_v2)]

pub mod access_method;
pub mod client;
pub mod codec;
pub mod messaging;
pub mod routing;
pub mod serving;
pub mod specs;
pub mod transport;

pub mod x_deps {
    pub use buffex;
}
