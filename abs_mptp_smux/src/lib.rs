#![feature(allocator_ext)]
#![feature(unboxed_closures)]

#![no_std]

pub mod client;
pub mod serving;

mod ring_prepare_;

pub mod x_deps {
    pub use abs_mptp;
    pub use abs_mptp::x_deps::{abs_buff, abs_cancel, abs_mm, abs_smux};
}
