//! 按注册类型查找 body 编解码器的注册表。
//!
//! # 为什么 `W` / `R` 要作为注册表的类型参数
//!
//! 注册表要回答的问题是「给定一个类型标识，取出能编 / 能解某个 Rust 类型 `T` 的
//! 那对函数」。条目在表里是类型擦除的（`Box<dyn TrEncodeAsync>`），取出时必须靠
//! `Any::downcast_ref` 把抹掉的类型**对回来**——所以凡是参与擦除的泛型，查找时都
//! 必须能写出它的确定值。
//!
//! `T` 由调用点给出，这没问题。而读写目标 `W` / `R` 也**不是真泛型**：在 MPTP 里
//! 它们就是本端子流的收发半边（`ChannelTx<C>` / `ChannelRx<C>`），由会话配置唯一
//! 确定。把这个事实写进类型参数，`downcast_ref::<EncodeStorage<T, W>>()` 就有了
//! 唯一确定的目标类型，条目也能保持「直接写进 ring、不经中间缓冲」的能力。
//!
//! 反过来说，把 `W` / `R` 留成方法级泛型是不可能的：`abs_buff` 的
//! [`TrBuffWrite`] / [`TrBuffRead`] 带 GAT（`WriteAsync<'f>` / `ReadAsync<'f>`），
//! 不是 object safe，没法像 `T` 那样被擦除。
//!
//! # 查找失败的两种情形
//!
//! [`CodecRegistry::find_encode`] / [`CodecRegistry::find_decode`] 返回 `None` 有
//! 两个来源，调用方通常不必区分：该类型标识压根没注册过，或者注册过但它当初绑定的
//! `T`（乃至 `W` / `R`）与本次请求的对不上。后者正是「标识与类型不符」的运行时保护。

use core::{any::Any, marker::PhantomData, ops::Deref};
use std::collections::BTreeMap;

use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_buff_stdio_adapt::x_deps::abs_buff;

use super::CodecAsync;

/// 类型擦除后的编码器条目。
pub trait TrEncodeAsync: Any {
    /// 取回 `&dyn Any`，供注册表把擦除掉的类型对回来。
    fn as_any(&self) -> &dyn Any;
}

/// 类型擦除后的解码器条目。
pub trait TrDecodeAsync: Any {
    /// 取回 `&dyn Any`，供注册表把擦除掉的类型对回来。
    fn as_any(&self) -> &dyn Any;
}

/// 可以注册进 [`CodecRegistry`] 的编码器契约。
///
/// 编码器把 `data` 编码后**直接写入**调用方给出的目标缓冲（`target`），并返回写入
/// 的字节数——它正是 `Body_Size` 头要申明的值。
pub trait TrEncodeFn<T, W>
where
    W: TrBuffWrite<u8>,
    Self: TrEncodeAsync,
{
    /// 把 `data` 编码并写入 `target`。
    ///
    /// 返回的 future 借用了 `self` / `data` / `target`，因此调用点必须在这些借用
    /// 仍然有效的同一个作用域里把它 `await` 掉。
    fn encode_async<'a>(&'a self, data: &'a T, target: &'a mut W) -> CodecAsync<'a, usize>
    where
        Self: 'a,
        T: 'a,
        W: 'a;
}

/// 可以注册进 [`CodecRegistry`] 的解码器契约。
///
/// 解码器从 `source` 读出 `T`，并返回 `(值, 消耗的字节数)`——后者让调用方知道这条
/// 子流上还剩多少字节属于后续内容（推送 / 拉取类会话需要它）。
pub trait TrDecodeFn<T, R>
where
    R: TrBuffRead<u8>,
    Self: TrDecodeAsync,
{
    /// 从 `source` 解码出 `T`。
    fn decode_async<'a>(&'a self, source: &'a mut R) -> CodecAsync<'a, (T, usize)>
    where
        Self: 'a,
        T: 'a,
        R: 'a;
}

/// 擦除后的条目类型：只保留「能被 downcast 回具体存储」这一件事。
type EncodeRef = Box<dyn TrEncodeAsync>;

/// 擦除后的条目类型：只保留「能被 downcast 回具体存储」这一件事。
type DecodeRef = Box<dyn TrDecodeAsync>;

/// 「类型标识 → (编码器, 解码器)」的注册表。
///
/// 三个类型参数分别是：标识类型 `TyId`、本端子流的发送半边 `W`、接收半边 `R`。
/// 后两者由会话配置决定，见模块文档。
///
/// `W` / `R` 本身不出现在条目里（条目是擦除过的），但它们必须留在注册表类型上：
/// 它们正是把条目 downcast 回具体存储时的键之一。这里的 [`PhantomData`] 就是这个
/// 「类型级标记」的载体，不占运行时空间。
///
/// # Examples
///
/// 用内置的 [`Codec`](super::Codec) 注册一个业务类型，并按同样的标识与类型取回：
///
/// ```
/// use mptp_core::codec::{Codec, CodecRegistry};
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct MyBody {
///     n: u32,
/// }
///
/// // 实际使用中这两个类型是本端子流的收发半边；这里用切片凑合，好让示例自洽。
/// type TyTx = &'static mut [u8];
/// type TyRx = &'static [u8];
///
/// const MY_BODY_ID: u64 = 0x1234_5678_u64;
///
/// let mut registry = CodecRegistry::<u64, TyTx, TyRx>::new();
/// registry.add::<MyBody, _, _>(&MY_BODY_ID, (Codec::MsgPack, Codec::MsgPack));
///
/// assert!(registry.find_encode::<MyBody>(&MY_BODY_ID).is_some());
/// assert!(registry.find_decode::<MyBody>(&MY_BODY_ID).is_some());
/// // 换成别的类型就取不到——这是「标识与类型不符」的运行时保护。
/// assert!(registry.find_encode::<u32>(&MY_BODY_ID).is_none());
/// ```
pub struct CodecRegistry<TyId, W, R>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    codecs_: BTreeMap<TyId, (EncodeRef, DecodeRef)>,

    marker_: PhantomData<fn() -> (W, R)>,
}

impl<TyId, W, R> CodecRegistry<TyId, W, R>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    /// 创建一个空注册表。
    pub fn new() -> Self {
        CodecRegistry {
            codecs_: BTreeMap::new(),
            marker_: PhantomData,
        }
    }

    /// 注册某个业务类型 `T` 的编解码器对。
    ///
    /// 同一个 `type_id` 上重复注册会**替换**旧条目，并把旧条目作为返回值交还给
    /// 调用方（不想替换时可以先查再插）。完整用法见 [`CodecRegistry`] 的示例。
    pub fn add<T, TyEncode, TyDecode>(
        &mut self,
        type_id: &TyId,
        pair: (TyEncode, TyDecode),
    ) -> Option<(EncodeRef, DecodeRef)>
    where
        T: 'static,
        TyEncode: TrEncodeFn<T, W> + 'static,
        TyDecode: TrDecodeFn<T, R> + 'static,
    {
        let (enc, dec) = pair;
        let enc: EncodeRef = Box::new(EncodeStorage::<T, W>::new_(enc));
        let dec: DecodeRef = Box::new(DecodeStorage::<T, R>::new_(dec));
        self.codecs_.insert(*type_id, (enc, dec))
    }

    /// 查找类型 `T` 的编码器。
    ///
    /// `T` 同时承担两个角色：它是本次要编码的数据类型，也是「把擦除掉的类型对回来」
    /// 的键。因此返回值要么精确对上注册时的 `T`，要么就是 `None`。
    pub fn find_encode<'f, T>(&'f self, type_id: &TyId) -> Option<&'f dyn TrEncodeFn<T, W>>
    where
        T: 'static,
    {
        let (enc, _) = self.codecs_.get(type_id)?;
        let stor = enc.as_any().downcast_ref::<EncodeStorage<T, W>>()?;
        Option::Some(stor.0.deref())
    }

    /// 查找类型 `T` 的解码器。
    ///
    /// 与 [`CodecRegistry::find_encode`] 同理：`T` 既是要解出的类型，也是把擦除掉的
    /// 类型对回来的键。
    pub fn find_decode<'f, T>(&'f self, type_id: &TyId) -> Option<&'f dyn TrDecodeFn<T, R>>
    where
        T: 'static,
    {
        let (_, dec) = self.codecs_.get(type_id)?;
        let stor = dec.as_any().downcast_ref::<DecodeStorage<T, R>>()?;
        Option::Some(stor.0.deref())
    }

    /// 该类型标识是否已注册。
    pub fn contains(&self, type_id: &TyId) -> bool {
        self.codecs_.contains_key(type_id)
    }

    /// 已注册的条目数量。
    pub fn len(&self) -> usize {
        self.codecs_.len()
    }

    /// 是否没有任何条目。
    pub fn is_empty(&self) -> bool {
        self.codecs_.is_empty()
    }
}

impl<TyId, W, R> Default for CodecRegistry<TyId, W, R>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    W: TrBuffWrite<u8> + 'static,
    R: TrBuffRead<u8> + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// 条目在表里的实际存储类型：`T` 与 `W` 都留在类型里，`downcast_ref` 才能精确命中。
struct EncodeStorage<T, W>(Box<dyn TrEncodeFn<T, W>>)
where
    T: 'static,
    W: TrBuffWrite<u8> + 'static;

impl<T, W> EncodeStorage<T, W>
where
    T: 'static,
    W: TrBuffWrite<u8> + 'static,
{
    fn new_(enc: impl TrEncodeFn<T, W> + 'static) -> Self {
        EncodeStorage(Box::new(enc))
    }
}

impl<T, W> TrEncodeAsync for EncodeStorage<T, W>
where
    T: 'static,
    W: TrBuffWrite<u8> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// 解码器一侧的条目存储，与 [`EncodeStorage`] 同形。
struct DecodeStorage<T, R>(Box<dyn TrDecodeFn<T, R>>)
where
    T: 'static,
    R: TrBuffRead<u8> + 'static;

impl<T, R> DecodeStorage<T, R>
where
    T: 'static,
    R: TrBuffRead<u8> + 'static,
{
    fn new_(dec: impl TrDecodeFn<T, R> + 'static) -> Self {
        DecodeStorage(Box::new(dec))
    }
}

impl<T, R> TrDecodeAsync for DecodeStorage<T, R>
where
    T: 'static,
    R: TrBuffRead<u8> + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::codec::Codec;

    /// 占位业务类型 A：用来验证「注册之后能按类型取回」。
    #[derive(Debug, PartialEq)]
    struct BodyA(u32);

    /// 占位业务类型 B：用来验证「标识相同但类型不同时取不到」。
    #[derive(Debug, PartialEq)]
    struct BodyB(u32);

    /// 测试用发送半边：`abs_buff` 已为 `&mut [u8]` 实现 `TrBuffWrite<u8>`。
    type TestTx = &'static mut [u8];

    /// 测试用接收半边：`abs_buff` 已为 `&[u8]` 实现 `TrBuffRead<u8>`。
    type TestRx = &'static [u8];

    /// 只用于占位的编码器：不真的写目标，只回报数据里声称的字节数。
    struct FakeEncoder;

    /// 只用于占位的解码器：不真的读源，直接给出固定值。
    struct FakeDecoder;

    impl TrEncodeAsync for FakeEncoder {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl TrDecodeAsync for FakeDecoder {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl<W> TrEncodeFn<BodyA, W> for FakeEncoder
    where
        W: TrBuffWrite<u8> + 'static,
    {
        fn encode_async<'a>(&'a self, data: &'a BodyA, _target: &'a mut W) -> CodecAsync<'a, usize>
        where
            Self: 'a,
            BodyA: 'a,
            W: 'a,
        {
            Box::pin(async move { Result::Ok(data.0 as usize) })
        }
    }

    impl<R> TrDecodeFn<BodyA, R> for FakeDecoder
    where
        R: TrBuffRead<u8> + 'static,
    {
        fn decode_async<'a>(&'a self, _source: &'a mut R) -> CodecAsync<'a, (BodyA, usize)>
        where
            Self: 'a,
            BodyA: 'a,
            R: 'a,
        {
            Box::pin(async move { Result::Ok((BodyA(7u32), 4usize)) })
        }
    }

    /// 测试注册表按「标识 + 类型」精确取回编码器与解码器。
    /// - 手段：在空注册表上以 `BodyA` 的类型标识注册一对假编解码器，随后分别用
    ///   `BodyA`、`BodyB` 两种类型去查同一个标识，并另查一个从未注册过的标识。
    /// - 判断：`BodyA` 的编码器与解码器都能取到；同一个标识下换成 `BodyB` 必须返回
    ///   `None`（这正是「标识与类型不符」的运行时保护）；未注册的标识也返回 `None`。
    #[test]
    fn find_hits_only_the_registered_type_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let mut registry = CodecRegistry::<u64, TestTx, TestRx>::new();
        let replaced = registry.add::<BodyA, _, _>(&id_a, (FakeEncoder, FakeDecoder));
        assert!(replaced.is_none(), "首次注册不应替换掉任何条目");

        assert!(
            registry.find_encode::<BodyA>(&id_a).is_some(),
            "BodyA 的编码器应当取到"
        );
        assert!(
            registry.find_decode::<BodyA>(&id_a).is_some(),
            "BodyA 的解码器应当取到"
        );
        assert!(
            registry.find_encode::<BodyB>(&id_a).is_none(),
            "同一标识下换成 BodyB 不应取到编码器"
        );
        assert!(
            registry.find_decode::<BodyB>(&id_a).is_none(),
            "同一标识下换成 BodyB 不应取到解码器"
        );

        let id_missing: u64 = crate::codec::hash_str("BodyMissing");
        assert!(
            registry.find_encode::<BodyA>(&id_missing).is_none(),
            "未注册的标识不应取到编码器"
        );
    }

    /// 测试同一标识上重复注册会替换旧条目，并把旧条目交还给调用方。
    /// - 手段：对同一个标识连续注册两次 `BodyA` 的假编解码器。
    /// - 判断：第二次 `add` 返回 `Some`（旧条目被交还），且注册表条目数仍为 1。
    #[test]
    fn re_register_returns_the_replaced_entry_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let mut registry = CodecRegistry::<u64, TestTx, TestRx>::new();
        let first = registry.add::<BodyA, _, _>(&id_a, (FakeEncoder, FakeDecoder));
        assert!(first.is_none(), "首次注册不应交还任何条目");

        let second = registry.add::<BodyA, _, _>(&id_a, (FakeEncoder, FakeDecoder));
        assert!(second.is_some(), "重复注册应当交还被替换的条目");
        assert_eq!(registry.len(), 1usize, "重复注册不应增加条目数");
        assert!(registry.contains(&id_a), "该标识应当仍然在表里");
    }

    /// 测试内置 `Codec::MsgPack` 能被注册表取回，并真的把数据写进目标缓冲。
    /// - 手段：注册一个 `MsgBody` 结构体，把一块泄漏成 `'static` 的字节缓冲当作发送
    ///   半边交给取回的编码器，在 `tokio` 运行时里驱动 `encode_async`。
    /// - 判断：编码成功，且返回的写入长度与 `rmp_serde::to_vec` 参考编码的长度一致；
    ///   这证明「取回的编码器」不只是类型对得上，而是真的可用。
    #[compio::test]
    async fn registered_msgpack_encoder_writes_into_target_() {
        #[derive(serde::Deserialize, serde::Serialize)]
        struct MsgBody {
            n: u32,
        }

        let id: u64 = crate::codec::hash_str("MsgBody");
        let mut registry = CodecRegistry::<u64, TestTx, TestRx>::new();
        registry.add::<MsgBody, _, _>(&id, (Codec::MsgPack, Codec::MsgPack));

        let expected = rmp_serde::to_vec(&MsgBody { n: 7u32 })
            .expect("参考编码应当成功")
            .len();
        let mut target: &'static mut [u8] = Box::leak(vec![0u8; 64usize].into_boxed_slice());
        let encoder = registry
            .find_encode::<MsgBody>(&id)
            .expect("应当取到编码器");
        let written = encoder
            .encode_async(&MsgBody { n: 7u32 }, &mut target)
            .await
            .expect("MessagePack 编码应当成功");
        assert_eq!(written, expected, "写入长度应与参考编码长度一致");
    }
}
