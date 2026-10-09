//! 编码方向的通用机制：目标缓冲，以及一次编码的异步产物。

use core::{
    future::{Future, IntoFuture},
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use abs_buff::x_deps::abs_cancel;
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};

use super::{
    codec_::CodecError,
    config_::TrCodecConfig,
    serde_::{Codec, CountingWriteEncodeAsync, CountingWriteEncodeFuture},
};

/// 编码的目标缓冲。
///
/// 具体写向哪条流由会话配置 `C` 给出。
#[derive(Debug)]
pub struct EncodeBuffWrite<C>
where
    C: TrCodecConfig,
{
    _use_c_: PhantomData<fn() -> C>,
}

impl<C> EncodeBuffWrite<C>
where
    C: TrCodecConfig,
{
    /// 借用本端的发送半边。
    pub fn target(&mut self) -> &mut C::ChannelTx {
        todo!()
    }
}

/// 一次编码的异步产物。
///
/// 它借用了调用方的数据与目标缓冲，因此带生命周期。`T` 上没有 serde 约束：
/// 「这次编码到底谁来做」封在内部的 [`EncodeDriver`] 里，外界看不到。
pub struct EncodeAsync<'f, T, C>
where
    C: TrCodecConfig,
{
    data_: &'f T,
    buff_: &'f mut EncodeBuffWrite<C>,
    driver_: EncodeDriver<'f>,
}

impl<'f, T, C> EncodeAsync<'f, T, C>
where
    C: TrCodecConfig,
{
    /// 由具体实现（如内置的 [`Codec`]）在自己的 `encode_async` 里构造。
    pub(in crate::codec) fn new_(
        data: &'f T,
        buff: &'f mut EncodeBuffWrite<C>,
        driver: EncodeDriver<'f>,
    ) -> Self {
        EncodeAsync {
            data_: data,
            buff_: buff,
            driver_: driver,
        }
    }
}

/// 「谁来编」：把已知的编码器实现穷举进来。
///
/// 为什么不是 `&dyn TrEncoder`：这个载体要装下「带泛型取消令牌的 future」，
/// 而泛型方法正是 object safe 的反面。于是反过来用**封闭**枚举列出已知实现——
/// 新增一种实现时，这里与 [`EncodeAsync`] 里那处 `match` 会被编译器逼着一起改。
pub(in crate::codec) enum EncodeDriver<'f> {
    /// 内置的 serde 类实现。
    Serde(&'f Codec),
}

impl<'a, T, C> IntoFuture for EncodeAsync<'a, T, C>
where
    T: serde::Serialize,
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

impl<'a, T, C> TrMayCancel<'a> for EncodeAsync<'a, T, C>
where
    T: serde::Serialize,
    C: TrCodecConfig,
{
    type MayCancelFuture<'f, K>
        = EncodeMayCancelFuture<'f, T, C, K>
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
            EncodeDriver::Serde(codec) => {
                let async_ = CountingWriteEncodeAsync::new(codec, self.data_, self.buff_);
                EncodeMayCancelFuture::Serde(async_.may_cancel_with(cancel))
            }
        }
    }
}

/// 各实现产出的 future 的收容所。
///
/// [`TrMayCancel::MayCancelFuture`] 是关联类型，一个 impl 只能给出一个具体类型，
/// 而不同实现产出的 future 类型不同——所以这一层同样只能是封闭枚举。
///
/// 必须是 `pub`：[`TrMayCancel`] 是公开 trait，它的关联类型不能是受限可见的类型。
/// 但本模块本身不对外可达，所以它实际仍只在 crate 内可用。
pub enum EncodeMayCancelFuture<'f, T, C, K>
where
    T: serde::Serialize,
    C: TrCodecConfig,
    K: TrCancellationToken,
{
    /// 内置的 serde 类实现。
    Serde(CountingWriteEncodeFuture<'f, 'f, T, C, K>),
}

impl<'f, T, C, K> Future for EncodeMayCancelFuture<'f, T, C, K>
where
    T: serde::Serialize,
    C: TrCodecConfig,
    K: TrCancellationToken + 'f,
{
    type Output = Result<usize, CodecError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // SAFETY: 只把 `Pin` 投影到**当前变体**的字段上，既不切换变体、也不把字段移出，
        // 因此被 pin 住的 future 不会被移动。这正是 `pin-project` 一类工具生成的代码，
        // 手写是因为这里只有一个变体、不值得为它引入依赖。
        unsafe {
            match self.get_unchecked_mut() {
                Self::Serde(fut) => Pin::new_unchecked(fut).poll(cx),
            }
        }
    }
}
