//! 递归分派用的可取消 future 组合子。
//!
//! 类型级列表（`tlist`）把「有多少个 handler」交给类型系统；但递归分派仍然需要在运行期
//! 二选一：命中当前节点，或继续查尾节点。两个分支的 future 类型不同，本模块用
//! [`EitherMayCancel`] 把它们合成一个类型。
//!
//! 这些类型只服务于 [`Router`](super::Router) 与
//! `rpc_core_v1::serving::RpcRouter` 的实现，属于 `#[doc(hidden)]` 的公共支撑类型。
//!
//! # `cancel_future_`: cancellable future combinators for recursive dispatch
//!
//! A type-level list knows how many handlers exist; at runtime recursive dispatch still has
//! to choose between the current node and its tail. [`EitherMayCancel`] unifies the two
//! different future types without boxing. [`MapErrMayCancel`] maps a handler-specific error
//! into a router-level error while preserving cancellation.

use core::{
    future::{Future, IntoFuture, Ready, ready},
    pin::Pin,
    task::{Context, Poll},
};

use abs_cancel::{TrCancellationToken, TrMayCancel};
use futures::{
    TryFutureExt as _,
    future::{Either, MapErr},
};

/// 一个可取消的「二选一」future。
///
/// [`IntoFuture`] 用 [`futures::future::Either`] 承载两个已经转换的 future；
/// [`TrMayCancel`] 只把取消令牌作用到实际选中的分支。
#[doc(hidden)]
pub enum EitherMayCancel<A, B> {
    /// 选中第一个分支。
    Left(A),

    /// 选中第二个分支。
    Right(B),
}

impl<A, B, T> IntoFuture for EitherMayCancel<A, B>
where
    A: IntoFuture<Output = T>,
    B: IntoFuture<Output = T>,
{
    type IntoFuture = Either<A::IntoFuture, B::IntoFuture>;

    type Output = T;

    fn into_future(self) -> Self::IntoFuture {
        match self {
            EitherMayCancel::Left(left) => Either::Left(left.into_future()),
            EitherMayCancel::Right(right) => Either::Right(right.into_future()),
        }
    }
}

impl<'a, A, B, T> TrMayCancel<'a> for EitherMayCancel<A, B>
where
    A: TrMayCancel<'a, MayCancelOutput = T>,
    B: TrMayCancel<'a, MayCancelOutput = T>,
    Self: 'a,
{
    type MayCancelFuture<'f, C>
        = EitherMayCancel<A::MayCancelFuture<'f, C>, B::MayCancelFuture<'f, C>>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = T;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        match self {
            EitherMayCancel::Left(left) => EitherMayCancel::Left(left.may_cancel_with(cancel)),
            EitherMayCancel::Right(right) => EitherMayCancel::Right(right.may_cancel_with(cancel)),
        }
    }
}

/// 一个立即就绪、且忽略取消令牌的可取消 future。
///
/// 它用于类型级列表的空节点，以及「内部不变量被破坏」这类没有 I/O 的错误路径；这些路径
/// 不需要等待取消信号。
#[doc(hidden)]
pub struct ReadyMayCancel<T> {
    inner_: Ready<T>,
}

impl<T> ReadyMayCancel<T> {
    /// 用 `value` 造一个立即就绪的 future。
    pub fn new(value: T) -> Self {
        ReadyMayCancel {
            inner_: ready(value),
        }
    }
}

impl<T> Future for ReadyMayCancel<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner_).poll(cx)
    }
}

impl<'a, T: 'a> TrMayCancel<'a> for ReadyMayCancel<T> {
    type MayCancelFuture<'f, C>
        = Self
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = T;

    fn may_cancel_with<C>(self, _cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        self
    }
}

/// 把 future 输出里的 `Err(E1)` 映射成 `Err(E2)`，并保留取消语义。
///
/// 映射函数用 `fn` 指针而不是闭包，避免在 `MayCancelFuture` 里再携带一个可能是
/// `FnOnce` 的值；分派路径上的映射都是单一函数。
#[doc(hidden)]
pub struct MapErrMayCancel<F, E1, E2> {
    future_: F,
    map_: fn(E1) -> E2,
}

impl<F, E1, E2> MapErrMayCancel<F, E1, E2> {
    /// 用映射函数包住 `future`。
    pub fn new(future: F, map: fn(E1) -> E2) -> Self {
        MapErrMayCancel {
            future_: future,
            map_: map,
        }
    }
}

impl<F, E1, E2, T> IntoFuture for MapErrMayCancel<F, E1, E2>
where
    F: IntoFuture<Output = Result<T, E1>>,
{
    type IntoFuture = MapErr<F::IntoFuture, fn(E1) -> E2>;

    type Output = Result<T, E2>;

    fn into_future(self) -> Self::IntoFuture {
        self.future_.into_future().map_err(self.map_)
    }
}

impl<'a, F, E1, E2, T> TrMayCancel<'a> for MapErrMayCancel<F, E1, E2>
where
    F: TrMayCancel<'a, MayCancelOutput = Result<T, E1>>,
    E1: 'a,
    E2: 'a,
    Self: 'a,
{
    type MayCancelFuture<'f, C>
        = MapErrMayCancel<F::MayCancelFuture<'f, C>, E1, E2>
    where
        'f: 'a,
        Self: 'f,
        C: 'f + TrCancellationToken;

    type MayCancelOutput = Result<T, E2>;

    fn may_cancel_with<C>(self, cancel: C) -> Self::MayCancelFuture<'a, C>
    where
        C: 'a + TrCancellationToken,
    {
        MapErrMayCancel {
            future_: self.future_.may_cancel_with(cancel),
            map_: self.map_,
        }
    }
}
