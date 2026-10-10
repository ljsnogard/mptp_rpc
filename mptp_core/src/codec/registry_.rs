//! 按注册类型查找 body 编解码器的注册表。
//!
//! # 为什么 `Any` 记住的键必须只含 `T`
//!
//! 注册表要回答的问题是「给定一个类型标识，取出能编 / 能解某个 Rust 类型 `T` 的
//! 那对函数」。条目在表里是类型擦除的（`Box<dyn TrCodecPair, A>`），取出时必须靠
//! `Any::downcast_ref` 把抹掉的类型**对回来**——所以凡是参与擦除的泛型，查找时都
//! 必须能写出它的确定值。
//!
//! 查找侧手里只有 `T`；`E` / `D` 是注册那一刻才出现的具体类型，查找侧写不出来。
//! 于是 `Any` 记住的那个键只能含 `T`。这里的具体做法是：让条目的
//! [`TrCodecPair::key_any`] 交出的**不是** `&self`，而是它内部一个只含 `T` 的小结构
//! [`CodecPairKey`]——`E` / `D` 于是可以放心地和这个键躺在同一块内存里（见下节）。
//!
//! 反过来说，直接把 `E` / `D` 内联进「被 `Any` 记住的那个类型」是不行的：那样键就
//! 带上了 `E` / `D`，`downcast_ref` 再也对不回来。同理，把 `Box<dyn ...>` 直接
//! coerce 成 `Box<dyn Any, A>` 也不行：那次 coercion 会让 `Any` 记住内部的
//! `dyn` 本身（unsized），而 `downcast_ref` 要求目标 `Sized`。
//!
//! # 一次分配装下「一对」和它的键
//!
//! 条目只有**一次**堆分配：`CodecPairKey`（键）与 `E` / `D`（载荷）内联在同一块内存
//! 里，由 `add` 一次 `Box::new_in` 建出来。
//!
//! 键里只有一个按 `(T, E, D)` 单态化的入口函数指针（[`pair_entry_`]）。它接住**整块
//! 条目的首地址**（即 `&dyn TrCodecPair` 的 data 指针），用**编译期常量偏移**定位同一块
//! 内存里的两个字段，并就地完成到 `dyn` 的转型——vtable 由此而来。这一步是必需的：
//! `&dyn TrPollEncode<T>` 是「数据地址 + vtable」，偏移量只解决前者，能把 `*const E`
//! 变成 `*const dyn TrPollEncode<T>` 的代码必须知道 `E`，而查找侧不知道。
//!
//! 起点必须是**整块的首地址**，不能换成一个字段自己的地址：引用的 provenance 只覆盖它
//! 指向的那几个字节，拿它做跨字段算术在 Stacked Borrows 下就是越界访问。
//!
//! 好处是键在类型上不必出现 `E` / `D`，运行时也不必回填任何指针：条目在进 `Box` 之前
//! 就是完整的，之后**可以自由搬家**（入口记的是相对偏移，不是绝对地址）。
//! 代价是每次查找多一次间接调用，换来键从两个胖指针（32 字节）降到 8 字节。
//!
//! # 查找失败的两种情形
//!
//! [`CodecRegistry::find_encode`] / [`CodecRegistry::find_decode`] 返回 `None` 有
//! 两个来源，调用方通常不必区分：该类型标识压根没注册过，或者注册过但它当初绑定的
//! `T` 与本次请求的对不上。后者正是「标识与类型不符」的运行时保护。

use core::{alloc::Allocator, any::Any, marker::PhantomData, mem::offset_of};
use std::collections::BTreeMap;

use super::codec_::{TrDecoder, TrEncoder};
use crate::codec::TrCodecConfig;

/// 注册表里条目的擦除视图。
///
/// 调用者只能**整体持有**它（例如 [`CodecRegistry::add`] 交还的旧条目），
/// 不能把它拆开：取出编解码器必须走
/// [`CodecRegistry::find_encode`] / [`CodecRegistry::find_decode`]，
/// 因为只有那一步手里才有 `T`。
pub trait TrCodecPair: Any {
    /// 取回条目内部那个只含 `T` 的键，供注册表把擦除掉的 `T` 对回来。
    ///
    /// 交出去的不是 `&self`：`self` 的类型带着 `E` / `D`，查找侧写不出来。
    fn key_any(&self) -> &dyn Any;
}

/// 入口交回的一对裸胖指针：编码方向与解码方向。
type PairPtrs<T, C> = (*const dyn TrEncoder<T, C>, *const dyn TrDecoder<T, C>);

/// 单态化入口的签名：接住键的地址，交回一对裸胖指针。
///
/// 抽成别名是因为它同时出现在键的字段与入口函数的签名里，展开写会很长。
/// vtable 就是在这个转型里就位的——只有知道 `E` / `D` 的代码做得到。
type PairEntry<T, C> = fn(*const ()) -> PairPtrs<T, C>;

/// 条目的键：只含 `T`，是 `Any` 唯一记得住的东西。
///
/// 里面只放一个单态化入口，**不含任何指向自身所在内存的数据**——这正是条目可以自由
/// 搬家的原因。入口的用法见 [`pair_entry_`]。
struct CodecPairKey<T, C>
where
    T: 'static,
{
    entry_: PairEntry<T, C>,
}

/// 条目在表里的实际存储：键与 `E` / `D` **内联在同一次分配里**。
///
/// # 不变量
///
/// `key_.entry_` 必须是按**同一组** `(T, E, D)` 单态化的入口：它以**整块首地址**为起点，
/// 靠编译期偏移量在这块内存里定位 `encode_` / `decode_`。这个配对由
/// [`CodecRegistry::add`] 独家建立——键与载荷在同一次调用里一起写入，此后不会被改写，
/// 也没有任何路径能单独换掉其中一个。
///
/// 因为入口记的是**相对**偏移而不是绝对地址，条目不含自引用：移动它是安全的，
/// 把它从一个 [`Box`] 搬到另一个 [`Box`] 也不会失效。
struct CodecPair<T, C, E, D>
where
    T: 'static,
{
    key_: CodecPairKey<T, C>,
    encode_: E,
    decode_: D,
    _use_t_: PhantomData<fn() -> T>,
}

/// 键与载荷之间的单态化入口：把**整块条目的首地址**换算成两个方向的裸胖指针。
///
/// 两段偏移都是**编译期常量**，所以这里不需要任何回填；vtable 也在这里就地完成
/// （`*const E` → `*const dyn TrPollEncode<T>` 的转型需要知道 `E`，只有本函数知道）。
///
/// 参数 `base_ptr` 必须是整块 `CodecPair<T, E, D>` 的首地址，**不要**传键字段自己的地址：
/// 键那 8 字节的引用 provenance 覆盖不到相邻的载荷字段，拿它做跨字段算术在 Stacked
/// Borrows 下就是越界访问（Miri 会当场报 UB）。
fn pair_entry_<T, C, E, D>(base_ptr: *const ()) -> PairPtrs<T, C>
where
    T: 'static,
    C: TrCodecConfig,
    E: TrEncoder<T, C> + 'static,
    D: TrDecoder<T, C> + 'static,
{
    let enc_off = offset_of!(CodecPair<T, C, E, D>, encode_);
    let dec_off = offset_of!(CodecPair<T, C, E, D>, decode_);
    // SAFETY: 调用方保证 `base_ptr` 是整块条目的首地址，其 provenance 覆盖整块；
    // 两段偏移都是编译期常量，因此结果落在这块分配的有效字段上，对齐由字段自身的
    // 布局保证。
    let base = base_ptr as *const u8;
    let enc: *const dyn TrEncoder<T, C> = unsafe { base.add(enc_off) } as *const E;
    let dec: *const dyn TrDecoder<T, C> = unsafe { base.add(dec_off) } as *const D;
    (enc, dec)
}

impl<T, C, E, D> TrCodecPair for CodecPair<T, C, E, D>
where
    T: 'static,
    C: 'static + TrCodecConfig,
    E: TrEncoder<T, C> + 'static,
    D: TrDecoder<T, C> + 'static,
{
    fn key_any(&self) -> &dyn Any {
        &self.key_
    }
}

/// 「类型标识 → (编码器, 解码器)」的注册表。
///
/// 三个类型参数分别是：标识类型 `X`、会话配置 `C`（给出本端子流的收发半边）与条目分配器 `A`。
///
/// # Examples
///
/// 用内置的 [`Codec`](super::Codec) 注册一个业务类型，并按同样的标识与类型取回：
///
/// ```
/// use mptp_core::codec::{Codec, CodecRegistry, TrCodecConfig};
/// use std::alloc::Global;
///
/// // 会话配置给出收发半边；示例里用切片凑合，实际使用中是本端子流的两半。
/// struct MyConfig;
///
/// impl TrCodecConfig for MyConfig {
///     type ChannelTx = &'static mut [u8];
///     type ChannelRx = &'static [u8];
/// }
///
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct MyBody {
///     n: u32,
/// }
///
/// const MY_BODY_ID: u64 = 0x1234_5678_u64;
///
/// let mut registry = CodecRegistry::<u64, MyConfig, Global>::new();
/// registry.add::<MyBody, _, _>(&MY_BODY_ID, (Codec::MsgPack, Codec::MsgPack), Global);
///
/// assert!(registry.find_encode::<MyBody>(&MY_BODY_ID).is_some());
/// assert!(registry.find_decode::<MyBody>(&MY_BODY_ID).is_some());
/// // 换成别的类型就取不到——这是「标识与类型不符」的运行时保护。
/// assert!(registry.find_encode::<u32>(&MY_BODY_ID).is_none());
/// ```
pub struct CodecRegistry<X, C, A>
where
    X: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    C: TrCodecConfig,
    A: Allocator,
{
    codecs_: BTreeMap<X, Box<dyn TrCodecPair, A>>,
    _use_c_: PhantomData<fn() -> C>,
}

impl<X, C, A> CodecRegistry<X, C, A>
where
    X: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    C: 'static + TrCodecConfig,
    A: 'static + Allocator,
{
    /// 创建一个空注册表。
    pub fn new() -> Self {
        CodecRegistry {
            codecs_: BTreeMap::new(),
            _use_c_: PhantomData,
        }
    }

    /// 注册某个业务类型 `T` 的编解码器对。
    ///
    /// 同一个 `type_id` 上重复注册会**替换**旧条目，并把旧条目作为返回值交还给
    /// 调用方（不想替换时可以先查再插）。
    ///
    /// 返回值只能是 `Box` 而不是 `&dyn`：旧条目的所有权必须整体移交出去，借用会指向
    /// 即将被释放的堆块。`alloc` 供这件条目自己的分配使用。
    ///
    /// 整个条目只做**一次**堆分配：键与 `E` / `D` 内联在同一块内存里。
    pub fn add<T, E, D>(
        &mut self,
        type_id: &X,
        pair: (E, D),
        alloc: A,
    ) -> Option<Box<dyn TrCodecPair, A>>
    where
        T: 'static,
        E: 'static + TrEncoder<T, C>,
        D: 'static + TrDecoder<T, C>,
    {
        let (enc, dec) = pair;
        // 条目在这里就是完整的：键里的入口靠编译期偏移定位载荷，不需要回填。
        let entry = CodecPair::<T, C, E, D> {
            key_: CodecPairKey {
                entry_: pair_entry_::<T, C, E, D>,
            },
            encode_: enc,
            decode_: dec,
            _use_t_: PhantomData,
        };
        let entry: Box<dyn TrCodecPair, A> = Box::new_in(entry, alloc);
        self.codecs_.insert(*type_id, entry)
    }

    /// 查找类型 `T` 的编码器。
    ///
    /// `T` 同时承担两个角色：它是本次要编码的数据类型，也是「把擦除掉的类型对回来」
    /// 的键。因此返回值要么精确对上注册时的 `T`，要么就是 `None`。
    pub fn find_encode<'f, T>(&'f self, type_id: &X) -> Option<&'f dyn TrEncoder<T, C>>
    where
        T: 'static,
    {
        let (enc, _) = self.find_ptrs_(type_id)?;
        // SAFETY: 入口由 `add` 按同一块分配的布局单态化，返回值指向那块内存里的
        // `encode_` 字段，因此对齐与有效性都由 `Box::new_in` 保证。
        //
        // provenance：入口的起点是 `&dyn TrCodecPair` 的 data 指针（见 `find_ptrs_`），
        // 它覆盖整块条目，所以 `encode_` 落在这条 provenance 的范围之内。
        //
        // 生命周期：那块内存由 `self.codecs_` 里的 `Box<dyn TrCodecPair, A>` 持有，
        // 而这里持有 `&'f self`，所以它在 `'f` 内不会被释放；归还的引用被约束在
        // `'f` 之内。
        //
        // 类型一致：能走到这里说明 `downcast_ref::<CodecPairKey<T>>()` 成功，而键里的
        // 入口与载荷是同一次 `add` 一起写入的，因此该入口正是按这块内存的
        // `(T, E, D)` 单态化的那个实例，`encode_` 满足 `TrPollEncode<T>`。
        Option::Some(unsafe { &*enc })
    }

    /// 查找类型 `T` 的解码器。
    ///
    /// 与 [`CodecRegistry::find_encode`] 同理：`T` 既是要解出的类型，也是把擦除掉的
    /// 类型对回来的键。
    pub fn find_decode<'f, T>(&'f self, type_id: &X) -> Option<&'f dyn TrDecoder<T, C>>
    where
        T: 'static,
    {
        let (_, dec) = self.find_ptrs_(type_id)?;
        // SAFETY: 同 `find_encode`：入口与载荷由同一次 `add` 写入，返回值指向同一块分配
        // 里的 `decode_` 字段，provenance 覆盖整块条目，生命周期受 `&'f self` 约束。
        Option::Some(unsafe { &*dec })
    }

    /// 该类型标识是否已注册。
    pub fn contains(&self, type_id: &X) -> bool {
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

    /// 借出条目里的单态化入口，换算出两个方向的裸胖指针；`T` 对不上就返回 `None`。
    ///
    /// 这里必须走 [`TrCodecPair::key_any`]，**不能**图省事写成
    /// `&dyn TrCodecPair` 到 `&dyn Any` 的 upcast：upcast 保留的是条目自身的
    /// `TypeId`（`CodecPair<T, E, D>`，带 `E` / `D`），查找侧写不出来，
    /// `downcast_ref` 必然失败。
    ///
    /// 入口要的是**整块条目的首地址**，所以起点取 `&dyn TrCodecPair` 的 data 指针，
    /// 而不是键字段自己的地址：后者的 provenance 只有那 8 字节，覆盖不到载荷字段。
    fn find_ptrs_<T>(&self, type_id: &X) -> Option<PairPtrs<T, C>>
    where
        T: 'static,
        C: 'static,
    {
        let pair: &dyn TrCodecPair = &**self.codecs_.get(type_id)?;
        let key = pair.key_any().downcast_ref::<CodecPairKey<T, C>>()?;
        // 胖指针转瘦指针，丢掉 vtable、保留 data 指针与它的 provenance。
        let base_ptr = pair as *const dyn TrCodecPair as *const ();
        Option::Some((key.entry_)(base_ptr))
    }
}

impl<X, C, A> Default for CodecRegistry<X, C, A>
where
    X: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
    C: 'static + TrCodecConfig,
    A: 'static + Allocator,
{
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests_ {
    //! 注册表的单元测试。
    //!
    //! # 为什么只验证到「类型层面」
    //!
    //! 端到端驱动一次编解码需要两样东西，目前都还拿不到：
    //!
    //! - `EncodeBuffWrite<C>` / `DecodeBuffRead<C>` 还没有可从测试构造的入口
    //!   （`target()` / `source()` 仍是 `todo!()`）；
    //! - `EncodeAsync` / `DecodeAsync` 的字段是私有的，只有 `Codec` 自己的 impl 造得出来，
    //!   因此测试也**无法自造一个 `TrEncoder` / `TrDecoder` 实现**。
    //!
    //! 所以这里用内置的 `Codec::MsgPack` 作为唯一的编解码器对，覆盖注册表自身的性质：
    //! 按 (标识, 类型) 精确取回、多条目互不串台、替换时交还旧条目、条目可搬家、
    //! 一次分配、键只占一个入口指针。等缓冲类型接上真实实现后，
    //! 应补一个真正驱动 `encode_async` / `decode_async` 的测试。

    use core::{
        alloc::{AllocError, Layout},
        mem::size_of,
        ptr::NonNull,
    };
    use std::{alloc::Global, cell::Cell, rc::Rc};

    use super::*;
    use crate::codec::Codec;

    /// 测试用的会话配置：收发半边都用切片凑合。
    ///
    /// `abs_buff` 已为 `&mut [u8]` 实现 `TrBuffWrite<u8>` + `TrProducerState`，
    /// 为 `&[u8]` 实现 `TrBuffRead<u8>` + `TrConsumerState`。
    struct TestConfig;

    impl TrCodecConfig for TestConfig {
        type ChannelTx = &'static mut [u8];
        type ChannelRx = &'static [u8];
    }

    /// 只统计真实堆分配（`size > 0`）的分配器，用来给「一次分配」上回归保护。
    ///
    /// 计数器用 [`Rc`] 共享而不是 `&'static`：它随条目一起被释放，
    /// 免得 Miri 的泄漏检查（正确地）报出来。
    #[derive(Clone)]
    struct CountingA(Rc<Cell<usize>>);

    unsafe impl Allocator for CountingA {
        fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
            if layout.size() > 0 {
                self.0.set(self.0.get() + 1);
            }
            Global.allocate(layout)
        }

        unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
            unsafe { Global.deallocate(ptr, layout) }
        }
    }

    /// 占位业务类型 A：用来验证「注册之后能按类型取回」。
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct BodyA(u32);

    /// 占位业务类型 B：用来验证「标识相同但类型不同时取不到」。
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct BodyB(u32);

    /// 占位业务类型 C：与 `BodyA` 共处一个注册表，用来验证多个条目不串台。
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct BodyC(u32);

    /// 占位业务类型 D：专供「一次分配」测试使用。
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct BodyD(u32);

    /// 测试注册表按「标识 + 类型」精确取回编解码器对。
    /// - 手段：在空注册表上以 `BodyA` 的标识注册一对 `Codec::MsgPack`，随后分别用
    ///   `BodyA`、`BodyB` 两种类型去查同一个标识，并另查一个从未注册过的标识。
    /// - 判断：`BodyA` 的编码器与解码器都能取到；同一个标识下换成 `BodyB` 必须返回
    ///   `None`（这正是「标识与类型不符」的运行时保护）；未注册的标识也返回 `None`。
    #[test]
    fn find_hits_only_the_registered_type_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let mut registry = CodecRegistry::<u64, TestConfig, Global>::new();
        let replaced = registry.add::<BodyA, _, _>(&id_a, (Codec::MsgPack, Codec::MsgPack), Global);
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
        assert!(
            registry.find_decode::<BodyA>(&id_missing).is_none(),
            "未注册的标识不应取到解码器"
        );
    }

    /// 测试同一注册表里多个条目的键互不串台。
    /// - 手段：用两个不同的标识分别注册 `BodyA` 与 `BodyC` 的编解码器对，然后交叉查找：
    ///   用 `BodyA` 与 `BodyC` 各自查自己的标识与对方的标识。
    /// - 判断：每个类型只能在自己的标识上取回；用另一个类型查同一标识必须返回 `None`；
    ///   这验证了「键只含 `T`」的 downcast 判定在多条目共存时仍然精确，
    ///   即每条目的单态化入口只认自己那块内存。
    #[test]
    fn keys_do_not_cross_between_entries_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let id_c: u64 = crate::codec::hash_str("BodyC");
        let mut registry = CodecRegistry::<u64, TestConfig, Global>::new();
        registry.add::<BodyA, _, _>(&id_a, (Codec::MsgPack, Codec::MsgPack), Global);
        registry.add::<BodyC, _, _>(&id_c, (Codec::MsgPack, Codec::MsgPack), Global);
        assert_eq!(registry.len(), 2usize, "两个标识应当各占一个条目");

        assert!(
            registry.find_encode::<BodyA>(&id_a).is_some(),
            "BodyA 应当在自己的标识上取到"
        );
        assert!(
            registry.find_encode::<BodyC>(&id_c).is_some(),
            "BodyC 应当在自己的标识上取到"
        );
        assert!(
            registry.find_encode::<BodyC>(&id_a).is_none(),
            "BodyC 不应在 BodyA 的标识上取到"
        );
        assert!(
            registry.find_encode::<BodyA>(&id_c).is_none(),
            "BodyA 不应在 BodyC 的标识上取到"
        );
        assert!(
            registry.find_decode::<BodyC>(&id_a).is_none(),
            "BodyC 不应在 BodyA 的标识上取到解码器"
        );
        assert!(
            registry.find_decode::<BodyA>(&id_c).is_none(),
            "BodyA 不应在 BodyC 的标识上取到解码器"
        );
    }

    /// 测试同一标识上重复注册会替换旧条目，并把旧条目所有权交还给调用方。
    /// - 手段：对同一个标识连续注册两次 `BodyA` 的编解码器对。
    /// - 判断：第二次 `add` 返回 `Some`（旧条目被交还），且注册表条目数仍为 1；
    ///   交还之后注册表里的新条目仍然可用，说明替换没有破坏入口与载荷的配对。
    #[test]
    fn re_register_returns_the_replaced_entry_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let mut registry = CodecRegistry::<u64, TestConfig, Global>::new();
        let first = registry.add::<BodyA, _, _>(&id_a, (Codec::MsgPack, Codec::MsgPack), Global);
        assert!(first.is_none(), "首次注册不应交还任何条目");

        let second = registry.add::<BodyA, _, _>(&id_a, (Codec::MsgPack, Codec::MsgPack), Global);
        assert!(second.is_some(), "重复注册应当交还被替换的条目");
        assert_eq!(registry.len(), 1usize, "重复注册不应增加条目数");
        assert!(registry.contains(&id_a), "该标识应当仍然在表里");

        assert!(
            registry.find_encode::<BodyA>(&id_a).is_some(),
            "替换之后新条目应当仍然可取"
        );
    }

    /// 测试条目不含自引用：把内容搬出 `Box`、再搬进另一块内存后仍然可用。
    /// - 手段：注册一对编解码器，把条目从注册表取出，upcast 成 `Box<dyn Any, Global>`
    ///   后 downcast 回具体类型，把内容搬出 `Box`（换了内存地址），重新装箱放回注册表，
    ///   随后再查一次编码器与解码器。
    /// - 判断：搬家之后 `find_encode` / `find_decode` 仍能取到，且换成别的类型查同一
    ///   标识仍返回 `None`。若条目退回成自引用（键里存指向自身字段的胖指针），
    ///   搬出来的内容会指向旧地址，这一步就会失配或读到悬空内存。
    #[test]
    fn entry_survives_moving_() {
        let id_a: u64 = crate::codec::hash_str("BodyA");
        let mut registry = CodecRegistry::<u64, TestConfig, Global>::new();
        registry.add::<BodyA, _, _>(&id_a, (Codec::MsgPack, Codec::MsgPack), Global);

        let entry = registry.codecs_.remove(&id_a).expect("条目应当存在");
        let any: Box<dyn Any, Global> = entry;
        let pair = any
            .downcast::<CodecPair<BodyA, TestConfig, Codec, Codec>>()
            .expect("应当能取回具体类型");
        let moved = *pair; // 搬出内容：这块内存与原来那块不是同一处
        let reborn: Box<dyn TrCodecPair, Global> = Box::new_in(moved, Global);
        registry.codecs_.insert(id_a, reborn);

        assert!(
            registry.find_encode::<BodyA>(&id_a).is_some(),
            "搬家之后仍应取到编码器"
        );
        assert!(
            registry.find_decode::<BodyA>(&id_a).is_some(),
            "搬家之后仍应取到解码器"
        );
        assert!(
            registry.find_encode::<BodyB>(&id_a).is_none(),
            "搬家之后类型保护仍应生效"
        );
    }

    /// 测试注册一个条目只做一次堆分配，把「键与载荷内联」这个决策锁住。
    /// - 手段：用只统计真实堆分配的 `CountingA` 注册一对编解码器，随后读取计数并再取回
    ///   一次编码器。
    /// - 判断：分配次数必须恰好为 1；如果将来有人把键与载荷拆成两块内存
    ///   （例如在条目内部再套一个 `Box`），计数会变成 2 而断言失败。
    ///   同时 `find_encode` 仍须成功，确认单态化入口在这一次分配里工作正常。
    /// - 局限：内置 `Codec` 是零大小类型，载荷本身不占空间，所以这条断言当前主要由
    ///   8 字节的键撑起；等出现非零大小的 `TrEncoder` / `TrDecoder` 实现后应替换载荷，
    ///   好把「载荷也内联」一并锁住。
    #[test]
    fn add_allocates_exactly_once_() {
        let counter = Rc::new(Cell::new(0usize));
        let id: u64 = crate::codec::hash_str("BodyD");
        let mut registry = CodecRegistry::<u64, TestConfig, CountingA>::new();
        let replaced = registry.add::<BodyD, _, _>(
            &id,
            (Codec::MsgPack, Codec::MsgPack),
            CountingA(Rc::clone(&counter)),
        );
        assert!(replaced.is_none(), "首次注册不应替换掉任何条目");
        assert_eq!(
            counter.get(),
            1usize,
            "键与载荷应当内联在同一次分配里；大于 1 说明两者被拆开了"
        );
        assert!(
            registry.find_encode::<BodyD>(&id).is_some(),
            "一次分配之后仍然应当能取回编码器"
        );
    }

    /// 测试键只占一个函数指针，把「不存胖指针、只用编译期偏移」这个决策锁住。
    /// - 手段：取 `CodecPairKey<T, C>` 的尺寸。
    /// - 判断：必须恰好等于一个函数指针的尺寸（`size_of::<usize>()`）。
    ///   一旦有人把两个方向的胖指针塞回键里（每个 16 字节），这个断言就会失败。
    #[test]
    fn key_holds_only_one_entry_pointer_() {
        assert_eq!(
            size_of::<CodecPairKey<BodyA, TestConfig>>(),
            size_of::<usize>(),
            "键应当只放一个单态化入口，而不是两个胖指针"
        );
    }
}
