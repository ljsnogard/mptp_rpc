//! body 编解码的**接口层**。
//!
//! 这里只放与具体实现无关的三样东西：
//!
//! - [`CodecError`]：编解码失败的错误；
//! - [`TrEncoder`] / [`TrDecoder`]：把业务类型 `T` 编成 body / 从 body 解出 `T` 的接口；
//! - 两个方向的异步产物 [`EncodeAsync`] / [`DecodeAsync`]（由 `encode_` / `decode_`
//!   子模块定义，这里转出）。
//!
//! # 为什么接口不对 `T` 设 serde 约束
//!
//! 「`T` 要能被 serde 接受」是**某一对具体实现**的能力，不是接口的前提。约束因此留在
//! 实现侧（见 [`serde_`](super::serde_) 模块）。[`CodecRegistry`](super::CodecRegistry)
//! 于是不必区分「`T` 是否能被 serde 接受」，它只要求「有人愿意处理这个 `T`」；
//! 选错了实现时，编译器会在**选择实现的那一行**报错，而不是在注册表的签名里。
//!
//! # 产物为什么能既通用又 object safe
//!
//! [`TrEncoder::encode_async`] 返回的是通用产物 [`EncodeAsync`]——类型里不出现任何
//! 具体实现。这是靠产物内部一个**私有枚举**做到的：泛型方法（带取消令牌的 future）
//! 无法 object safe，于是把动态分发从 `dyn` 换成封闭枚举，藏在产物内部不让外界接触。
//! 本 trait 因此保住 object safety，注册表才能存 `Box<dyn TrEncoder<T, C>, A>`。

use core::mem::MaybeUninit;

use thiserror::Error;

use super::{
    config_::TrCodecConfig,
    decode_::{DecodeAsync, DecodeBuffRead},
    encode_::{EncodeAsync, EncodeBuffWrite},
};

/// Body 编解码错误。
#[derive(Debug, Error)]
pub enum CodecError {
    #[error("encode body failed: {0}")]
    Encode(String),

    #[error("decode body failed: {0}")]
    Decode(String),
}

/// 把一个业务类型 `T` 编码成 body 的接口。
///
/// 本 trait 是 object safe 的，因此可以 `Box<dyn TrEncoder<T, C>, A>` 地存进
/// [`CodecRegistry`](super::CodecRegistry)；擦除只发生在 `T` 这一层。
///
/// 它不对 `T` 设 serde 约束，理由见模块文档；内置的 serde 类实现见
/// [`serde_`](super::serde_)。
pub trait TrEncoder<T, C>
where
    C: TrCodecConfig,
{
    /// 开始一次编码，返回可被 `.await`（或用取消令牌包一层）的产物。
    fn encode_async<'f>(
        &'f self,
        data: &'f T,
        buffer: &'f mut EncodeBuffWrite<C>,
    ) -> EncodeAsync<'f, T, C>;
}

/// 从 body 里解出一个业务类型 `T` 的接口，与 [`TrEncoder`] 对称。
///
/// 同样不对 `T` 设 serde 约束，理由见模块文档。
pub trait TrDecoder<T, C>
where
    C: TrCodecConfig,
{
    /// 开始一次解码，返回可被 `.await`（或用取消令牌包一层）的产物。
    fn decode_async<'f>(
        &'f self,
        data: &'f mut MaybeUninit<T>,
        buffer: &'f mut DecodeBuffRead<C>,
    ) -> DecodeAsync<'f, T, C>;
}
