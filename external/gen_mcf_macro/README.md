## gen_mcf_macro

Assumed to work with following unstable features:

```rust
// Allow for implementation of the AsyncFn*
#![feature(async_fn_traits)]

// To enable `extern "rust-call" fn` which is used in `impl AsyncFnOnce`
#![feature(unboxed_closures)]

// To enable `type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;`
#![feature(type_alias_impl_trait)]
#![feature(impl_trait_in_assoc_type)]
```

## Usage

```rust

use abs_cancel::{NonCancellableToken, TrMayCancel, TrCancellationToken};

/// # Usage Rules:
/// 0. Must be an `async fn`;
/// 1. At least one lifetime and the last one must be for the cancellation token;
/// 2. The last argument and generic parameter type must be the cancellation token type and constrained with: `TrCancellationToken`;
/// 3. Use a where clause to constrain the cancel token type;
#[gen_may_cancel_future(DoThing)]
async fn do_thing_async<'a, 'b, 'x, 'c, A, B, C>(
    a: &'a mut A,
    b: &'b mut B,
    l: usize,
    x: core::slice::Iter<'x, A>,
    cancel: &'c mut C,
) -> usize
where
    'a: 'c,
    'b: 'c,
    'x: 'c,
    'y: 'c,
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    let _ = (a, b, l, x, cancel);
    42
}

```

Which expands to codes:

```rust

async fn do_thing_async<'a, 'b, 'x, 'c, A, B, C>(
    a: &'a mut A,
    b: &'b mut B,
    l: usize,
    x: core::slice::Iter<'x, A>,
    cancel: &'c mut C,
) -> usize
where
    'a: 'c,
    'b: 'c,
    'x: 'c,
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    let _ = (a, b, l, x, cancel);
    42
}
pub struct DoThingAsync<'c, A, B>(
    &'c mut A,
    &'c mut B,
    usize,
    core::slice::Iter<'c, A>,
)
where
    A: Send,
    B: Sync;
pub struct DoThingFuture<'c, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    params_: ::core::mem::MaybeUninit<DoThingAsync<'c, A, B>>,
    cancel_: &'c mut C,
    future_: Option<
        <DoThingFutureState<
            'c,
            A,
            B,
            C,
        > as ::core::ops::AsyncFnOnce<()>>::CallOnceFuture,
    >,
}
struct DoThingFutureState<'c, A, B, C>(
    ::core::pin::Pin<&'c mut DoThingFuture<'c, A, B, C>>,
)
where
    A: Send,
    B: Sync,
    C: TrCancellationToken;
impl<'c, A, B> ::core::future::IntoFuture for DoThingAsync<'c, A, B>
where
    A: Send,
    B: Sync,
{
    type IntoFuture = DoThingFuture<
        'c,
        A,
        B,
        abs_cancel::NonCancellableToken,
    >;
    type Output = usize;
    fn into_future(self) -> Self::IntoFuture {
        DoThingFuture {
            params_: ::core::mem::MaybeUninit::new(self),
            cancel_: abs_cancel::NonCancellableToken::shared_mut(),
            future_: Option::None,
        }
    }
}
impl<'c, A, B> abs_cancel::TrMayCancel<'c> for DoThingAsync<'c, A, B>
where
    A: Send,
    B: Sync,
{
    type MayCancelOutput = usize;
    fn may_cancel_with<'cancel_, C: abs_cancel::TrCancellationToken>(
        self,
        cancel: &'cancel_ mut C,
    ) -> impl ::core::future::IntoFuture<Output = Self::MayCancelOutput>
    where
        Self: 'cancel_,
        // 与 `TrMayCancel::may_cancel_with` 的 `'f: 'a` 约束对应：
        // cancel token 的借用必须存活不短于数据生命周期 `'c`。
        'cancel_: 'c,
    {
        DoThingFuture {
            params_: ::core::mem::MaybeUninit::new(self),
            cancel_: cancel,
            future_: Option::None,
        }
    }
}
impl<'c, A, B, C> ::core::future::Future for DoThingFuture<'c, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    type Output = usize;
    fn poll(
        self: ::core::pin::Pin<&mut Self>,
        cx: &mut ::core::task::Context<'_>,
    ) -> ::core::task::Poll<Self::Output> {
        let mut this = unsafe {
            let p = self.get_unchecked_mut();
            ::core::ptr::NonNull::new_unchecked(p)
        };
        loop {
            let mut fut_field_ptr = unsafe {
                let ptr = &mut this.as_mut().future_;
                ::core::ptr::NonNull::new_unchecked(ptr)
            };
            let opt_fut = unsafe { fut_field_ptr.as_mut() };
            if let Option::Some(fut) = opt_fut {
                let fut_pin = unsafe { ::core::pin::Pin::new_unchecked(fut) };
                break fut_pin.poll(cx);
            } else {
                let state = DoThingFutureState(unsafe {
                    ::core::pin::Pin::new_unchecked(this.as_mut())
                });
                let fut = AsyncFnOnce::async_call_once(state, ());
                let fut_field_mut = unsafe { fut_field_ptr.as_mut() };
                *fut_field_mut = Option::Some(fut);
            }
        }
    }
}
impl<'c, A, B, C> ::core::ops::AsyncFnOnce<()> for DoThingFutureState<'c, A, B, C>
where
    A: Send,
    B: Sync,
    C: TrCancellationToken,
{
    type Output = usize;
    type CallOnceFuture = impl ::core::future::Future<Output = Self::Output>;
    extern "rust-call" fn async_call_once(self, _: ()) -> Self::CallOnceFuture {
        let f = unsafe { self.0.get_unchecked_mut() };
        let DoThingAsync::<'c, A, B>(p0, p1, p2, p3) = unsafe {
            f.params_.assume_init_read()
        };
        self::do_thing_async(p0, p1, p2, p3, f.cancel_)
    }
}

```

## Return types that carry a lifetime

The generated types only carry a single lifetime parameter — the *last* one
(`'c`, the cancellation-token lifetime). All argument lifetimes are unified to
it. For that reason, a return type that references any of the user-declared
lifetimes is *rewritten* to reference `'c` instead, e.g.:

```rust
#[gen_may_cancel_future(GetRef)]
async fn get_ref_async<'a, 'c, C>(
    s: &'a str,
    cancel: &'c mut C,
) -> &'a str            // rewritten to `&'c str` in the generated impls
where
    'a: 'c,
    C: TrCancellationToken,
{
    s
}
```

This is sound because the `'x: 'c` where-clauses (usage rule 1) guarantee the
async function is still well-formed when every lifetime is instantiated as `'c`.
`'static` and elided lifetimes are left untouched.

Note that `TrMayCancel::may_cancel_with` requires the cancellation-token borrow
to outlive the data lifetime (`'f: 'a` in the trait), because the generated
future stores the token as `&'a mut C` and its output may borrow from `'a`. This
means a temporary token (e.g. `&mut NonCancellableToken::new()`) cannot be used
with an operation whose output is tied to the data lifetime; bind the token to a
named variable instead.

## 内部实现：`__XxxFutureFactory` factory trait

生成代码里，`XxxFuture` 的 `future_` 字段保存内部 async fn 的 future，其类型通过
私有 trait `__XxxFutureFactory` 命名：

```rust
trait __XxxFutureFactory<'a, 'f, A, B, ..., C> {
    type MadeFuture: ::core::future::Future;
    fn make_future(
        f: ::core::pin::Pin<&'f mut XxxFuture<'a, 'f, A, B, ..., C>>,
    ) -> Self::MadeFuture
    where
        /* 原函数的全部 where 约束（含 `'a: 'f` 等生命周期关系） */;
}

impl<...> __XxxFutureFactory<...> for () { ... }
```

工厂 trait 的实现挂在 marker 类型 `()` 上，而不是挂在 `XxxFutureState` 上：
`()` 没有任何 implied bound，因此 impl selection 不依赖 self type 的隐含约束，
这是 factory trait 能做到的最“精准”的形式——泛型参数是命名 `MadeFuture`
所需的最小集合，impl 对所有生命周期实例化都成立。

## 已知限制：rustc #100013 / #130113

如果被包装的 async fn 的参数里带有 *路径类型内部的非最后生命周期*
（例如 `segm: &'f mut SegmRef<'a, T, R>` 且 `'a: 'f`），则生成类型会同时携带
`'a` 与 `'f`（`&'f mut T<'a>` 是 invariant 的，无法把 `'a` 收窄为 `'f`），并且
隐含 `'a: 'f` 约束。把这样的 future 放进 `tokio::spawn`（要求 `Send + 'static`）
时，rustc 在 async 内部（generator interior）无法证明区域间的 `'a: 'f`，
从而报出 “implementation of `X` is not general enough” 或
“lifetime bound not satisfied”，参见：

- <https://github.com/rust-lang/rust/issues/100013>（“We really should accept
  this, but we need implied bounds between the regions in a generator interior”）
- <https://github.com/rust-lang/rust/issues/130113>（“Bogus implementation of
  `<whatever trait you want>` is not general enough with RPITIT + async”）

这是 rustc 本身的限制，不是宏生成代码的问题；在 rustc 修复之前，这类
多生命周期场景不能直接放进 `tokio::spawn`。普通的 `.await` /
`may_cancel_with(...).await` 用法不受影响。
