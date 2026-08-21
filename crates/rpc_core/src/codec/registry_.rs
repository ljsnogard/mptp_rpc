use std::{
    self, any::Any, collections::BTreeMap,
    ops::Deref,
};

use super::{
    CodecAsync,
    channel::{RpcRx, RpcTx},
};

type EncodeRef = Box<dyn TrEncodeAsync>;
type DecodeRef = Box<dyn TrDecodeAsync>;

pub trait TrEncodeAsync: Any {
    fn as_any(&self) -> &dyn Any;
}
pub trait TrDecodeAsync: Any {
    fn as_any(&self) -> &dyn Any;
}

/// 用于注册的编码函数的类型
pub trait TrEncodeFn<T>
where
    Self: TrEncodeAsync,
{
    fn encode_async(
        &self,
        data: &T,
        write: &mut RpcTx,
    ) -> CodecAsync<usize>;
}

/// 用于注册的解码函数的类型
pub trait TrDecodeFn<T>
where
    Self: TrDecodeAsync,
{
    fn decode_async(
        &self,
        read: &mut RpcRx,
    ) -> CodecAsync<(T, usize)>;
}

/// 根据 `HeaderVal` 查找 `BodyCodec` 的注册表。
///
/// 这是一个很小的“策略查找”抽象：调用方只需要持有 `HeaderVal`，不必在业务
/// 代码里到处写 `if header == MsgPack { ... }`。
pub struct CodecRegistry<TyId>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    codecs_: BTreeMap<TyId, (EncodeRef, DecodeRef)>,
}

impl<TyId> CodecRegistry<TyId>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    /// 创建一个默认注册表。
    ///
    /// 目前是零大小结构体；后续如果需要注册自定义 codec，可以改为持有表项。
    pub fn new() -> Self {
        CodecRegistry { codecs_: BTreeMap::new() }
    }

    /// ```no_run
    /// let mut reg = CodecRegistry::new();
    /// reg.add(type_id!(full::path::to::the::Type), (encode, decode));
    /// ```
    pub fn add<TyEncode, TyDecode, TyData>(
        &mut self,
        type_id: &TyId,
        pair: (TyEncode, TyDecode),
    ) -> Option<(EncodeRef, DecodeRef)>
    where
        TyEncode: Sized + TrEncodeFn<TyData>,
        TyDecode: Sized + TrDecodeFn<TyData>,
        TyData: 'static,
    {
        let (enc, dec) = pair;
        let enc: Box<dyn TrEncodeAsync> = Box::new(EncodeStorage::new(enc));
        let dec: Box<dyn TrDecodeAsync> = Box::new(DecodeStorage::new(dec));
        self.codecs_.insert(*type_id, (enc, dec))
    }

    pub fn find_decode<'f, T>(&'f self, type_id: &TyId) -> Option<Decode<'f, T>>
    where
        T: 'static,
    {
        let pair = self.codecs_.get(type_id)?;
        let dec_ref: &dyn TrDecodeAsync = pair.1.deref();
        let dec_stor = dec_ref.as_any().downcast_ref::<DecodeStorage<T>>()?;
        Option::Some(Decode::new(dec_stor.0.deref()))
    }

    pub fn find_encode<'f, T>(&'f self, type_id: &TyId) -> Option<Encode<'f, T>>
    where
        T: 'static,
    {
        let pair = self.codecs_.get(type_id)?;
        let enc_ref: &dyn TrEncodeAsync = pair.0.deref();
        let enc_stor = enc_ref.as_any().downcast_ref::<EncodeStorage<T>>()?;
        Option::Some(Encode::new(enc_stor.0.deref()))
    }
}

pub struct Encode<'a, T>(&'a dyn TrEncodeFn<T>);

impl<'a, T> Encode<'a, T>
where
    T: 'static,
{
    const fn new(enc: &'a dyn TrEncodeFn<T>) -> Self {
        Encode(enc)
    }

    pub fn encode_async(&self, data: &'a T, write: &mut RpcTx) -> CodecAsync<usize> {
        self.0.encode_async(data, write)
    }
}

pub struct Decode<'a, T>(&'a dyn TrDecodeFn<T>);

impl<'a, T> Decode<'a, T>
where
    T: 'static,
{
    const fn new(dec: &'a dyn TrDecodeFn<T>) -> Self {
        Decode(dec)
    }

    pub fn decode_async(&self, read: &mut RpcRx) -> CodecAsync<(T, usize)> {
        self.0.decode_async(read)
    }
}

struct EncodeStorage<T>(Box<dyn TrEncodeFn<T>>);

struct DecodeStorage<T>(Box<dyn TrDecodeFn<T>>);

impl<T> EncodeStorage<T> {
    pub fn new(enc: impl TrEncodeFn<T>) -> Self {
        EncodeStorage(Box::new(enc))
    }
}

impl<T> DecodeStorage<T> {
    pub fn new(dec: impl TrDecodeFn<T>) -> Self {
        DecodeStorage(Box::new(dec))
    }
}

impl<T: 'static> TrEncodeAsync for EncodeStorage<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T: 'static> TrDecodeAsync for DecodeStorage<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}
