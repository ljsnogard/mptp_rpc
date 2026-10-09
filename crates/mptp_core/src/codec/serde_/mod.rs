//! 内置的 serde 类 body 编解码器。
//!
//! 这一层是「**某一对具体实现**」：它给 `T: serde::Serialize` / `T: DeserializeOwned`
//! 这类业务类型提供 [`TrEncoder`](super::TrEncoder) / [`TrDecoder`](super::TrDecoder)
//! 的实现，而接口层并不因此对 `T` 设任何 serde 约束。
//!
//! # 为什么两边都不走 `std::io` 适配器
//!
//! `rmp_serde` 的编解码入口要的是 `std::io::{Read, Write}`，而 `abs_buff` 的段接口是
//! 异步的。`abs_buff_stdio_adapt` 的 `AsStdWrite` / `AsStdRead` 能把两者接起来，但它们是
//! **同步**的——内部靠 `block_on` 等数据。那在 `smux_v1` 的用法下会死锁：连接的读 / 写
//! 循环被投递到**本线程**（tokio 下即 `LocalSet`），而 `block_on` 把当前线程占住之后，
//! 本地队列再也不会被驱动。
//!
//! 因此两侧都用真正的异步路径：
//!
//! - 编码：`rmp_serde::to_vec` 先在自己内存里算出完整字节（顺带得到 `Body_Size` 需要的
//!   长度），再用异步写出分块落进目标环；
//! - 解码：逐字节异步读出恰好一条 MessagePack 值，并回报消耗的字节数。
//!
//! # 为什么产物内部用枚举而不是 `&dyn`
//!
//! [`EncodeAsync`](super::encode_::EncodeAsync) 这类产物要装下「带泛型取消令牌的
//! future」，而泛型方法是 object safe 的反面。于是载体这一侧反过来用**封闭**枚举
//! 把已知实现列出来：新增一种编解码器时，枚举与它的 `match` 会被编译器逼着一起改。

mod codec_;
mod decoder_;
mod encoder_;

pub use codec_::Codec;
pub(in crate::codec) use decoder_::{CoundReadDecodeAsync, CoundReadDecodeFuture};
pub(in crate::codec) use encoder_::{CountingWriteEncodeAsync, CountingWriteEncodeFuture};
