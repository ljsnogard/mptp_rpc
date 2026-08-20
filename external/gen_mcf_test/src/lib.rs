#![allow(unused_features)]
// to enable no hand-written poll
#![feature(async_fn_traits)]
#![feature(impl_trait_in_assoc_type)]
#![feature(unboxed_closures)]

// #[cfg(test)]
mod tests_;

/// Regression test for return types containing lifetimes.
mod lifetime_return_test;

/// Regression test for arguments whose types carry a *non-last* inner lifetime
/// (e.g. `&'f mut Borrowed<'a, T>` with `'a: 'f`), the shape that produced the
/// “implementation is not general enough” errors in `rpc_transport_iroh`.
mod two_lt_inner_path_test;

/// Empty mod to check the output of `cargo expand`
mod out_;
