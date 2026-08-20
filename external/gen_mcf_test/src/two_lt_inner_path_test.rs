//! Regression test: `gen_may_cancel_future` with a function whose argument
//! carries a *path type with an inner lifetime* that outlives the
//! cancellation-token lifetime, e.g. `segm: &'f mut SegmRef<'a, ...>`.
//!
//! This is the shape that triggered `channel.rs`'s “implementation is not
//! general enough” errors: the generated future/state/factory chain carries
//! both `'a` and `'f` with the implied `'a: 'f` bound (because `&'f mut T<'a>`
//! requires `T<'a>: 'f`). The factory trait must therefore be usable with
//! arbitrary lifetimes, not tied to the state struct's implied bounds.
//!
//! NOTE: putting such a future into `tokio::spawn` is *still* rejected by rustc
//! (issue #100013 / #130113 — see the gen_mcf_macro README), but plain
//! `.await` / `may_cancel_with().await` usage must keep working.

#![allow(dead_code)]

use std::marker::PhantomData;

use abs_cancel::TrCancellationToken;

/// A stand-in for `SegmRef<'a, T, R>`: a type that borrows for `'a`.
pub struct Borrowed<'a, T>(PhantomData<&'a mut T>);

impl<'a, T> Borrowed<'a, T> {
    pub fn get(&mut self) -> usize {
        1
    }
}

#[gen_mcf_macro::gen_may_cancel_future(TwoLtInnerPath)]
async fn two_lt_inner_path_async_<'a, 'f, T, C>(
    segm: &'f mut Borrowed<'a, T>,
    value: &'f mut T,
    _cancel: &'f mut C,
) -> usize
where
    'a: 'f,
    C: TrCancellationToken,
{
    let _ = segm.get();
    let _ = &mut *value;
    42
}

#[compio::test]
pub async fn run_two_lt_inner_path() {
    use abs_cancel::{NonCancellableToken, TrMayCancel};

    let mut x = 0usize;
    let mut borrowed = Borrowed::<usize>(PhantomData);
    let mut tok = NonCancellableToken::new();

    // IntoFuture path (no cancellation token).
    let r = TwoLtInnerPathAsync(&mut borrowed, &mut x).await;
    assert_eq!(r, 42);

    // TrMayCancel path.
    let r = TwoLtInnerPathAsync(&mut borrowed, &mut x)
        .may_cancel_with(&mut tok)
        .await;
    assert_eq!(r, 42);
}
