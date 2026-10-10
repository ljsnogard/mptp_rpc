//! 解码方向的通用机制：来源缓冲，以及一次解码的异步产物。

use core::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};

use super::{
    codec_::CodecError,
    config_::TrCodecConfig,
    serde_::{Codec, CoundReadDecodeAsync, CoundReadDecodeFuture},
};

/// 解码的来源缓冲。
///
/// 具体从哪条流读由会话配置 `C` 给出。
#[derive(Debug)]
pub struct DecodeBuffRead<C>
where
    C: TrCodecConfig,
{
    _use_c_: PhantomData<fn() -> C>,
}

impl<C> DecodeBuffRead<C>
where
    C: TrCodecConfig,
{
    /// 借用本端的接收半边。
    pub const fn source(&mut self) -> &mut C::ChannelRx {
        todo!()
    }
}

/// 一次解码的异步产物。
///
/// 与 [`EncodeAsync`](super::encode_::EncodeAsync) 对称：借用了调用方的输出槽与来源
/// 缓冲，因此带生命周期；`T` 上没有 serde 约束，「这次解码谁来做」封在内部的
/// [`DecodeDriver`] 里。
pub struct DecodeAsync<'f, T, C>
where
    C: TrCodecConfig,
{
    data_: &'f mut MaybeUninit<T>,
    buff_: &'f mut DecodeBuffRead<C>,
    driver_: DecodeDriver<'f>,
}

impl<'f, T, C> DecodeAsync<'f, T, C>
where
    C: TrCodecConfig,
{
    /// 由具体实现（如内置的 [`Codec`]）在自己的 `decode_async` 里构造。
    pub(in crate::codec) fn new_(
        data: &'f mut MaybeUninit<T>,
        buff: &'f mut DecodeBuffRead<C>,
        driver: DecodeDriver<'f>,
    ) -> Self {
        DecodeAsync {
            data_: data,
            buff_: buff,
            driver_: driver,
        }
    }
}

/// 「谁来解」：把已知的解码器实现穷举进来，与
/// [`EncodeDriver`](super::encode_::EncodeDriver) 对称。
pub(in crate::codec) enum DecodeDriver<'f> {
    /// 内置的 serde 类实现。
    Serde(&'f Codec),
}

impl<'a, T, C> IntoFuture for DecodeAsync<'a, T, C>
where
    T: serde::de::DeserializeOwned,
    C: TrCodecConfig,
{
    type IntoFuture = <Self as TrMayCancel<'a>>::MayCancelFuture<'a, NonCancellableToken>;
    type Output = <Self as TrMayCancel<'a>>::MayCancelOutput;

    /// 配置 NonCancellableToken 参数后直接转发实现
    #[inline]
    fn into_future(self) -> Self::IntoFuture {
        <Self as TrMayCancel<'a>>::may_cancel_with(self, NonCancellableToken::new())
    }
}

impl<'a, T, C> TrMayCancel<'a> for DecodeAsync<'a, T, C>
where
    T: serde::de::DeserializeOwned,
    C: TrCodecConfig,
{
    type MayCancelFuture<'f, K>
        = DecodeMayCancelFuture<'f, T, C, K>
    where
        'f: 'a,
        Self: 'f,
        K: 'f + TrCancellationToken;

    type MayCancelOutput = Result<usize, CodecError>;

    fn may_cancel_with<K>(self, cancel: K) -> Self::MayCancelFuture<'a, K>
    where
        K: 'a + TrCancellationToken,
    {
        match self.driver_ {
            DecodeDriver::Serde(codec) => {
                let async_ = CoundReadDecodeAsync::new(codec, self.data_, self.buff_);
                DecodeMayCancelFuture::Serde(async_.may_cancel_with(cancel))
            }
        }
    }
}

/// 各实现产出的 future 的收容所，与
/// [`EncodeMayCancelFuture`](super::encode_::EncodeMayCancelFuture) 对称。
///
/// 同那边一样必须是 `pub`：公开 trait 的关联类型不能是受限可见的类型。
pub enum DecodeMayCancelFuture<'f, T, C, K>
where
    T: serde::de::DeserializeOwned,
    C: TrCodecConfig,
    K: TrCancellationToken,
{
    /// 内置的 serde 类实现。
    Serde(CoundReadDecodeFuture<'f, 'f, T, C, K>),
}

impl<'f, T, C, K> Future for DecodeMayCancelFuture<'f, T, C, K>
where
    T: serde::de::DeserializeOwned,
    C: TrCodecConfig,
    K: TrCancellationToken + 'f,
{
    type Output = Result<usize, CodecError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 同 `EncodeMayCancelFuture`——只投影到当前变体的字段，不切换变体、
        // 不移出字段，被 pin 住的 future 因此不会被移动。
        unsafe {
            match self.get_unchecked_mut() {
                Self::Serde(fut) => Pin::new_unchecked(fut).poll(cx),
            }
        }
    }
}
