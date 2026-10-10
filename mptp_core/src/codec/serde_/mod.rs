//! 内置的 serde 类 body 编解码器。
//!
//! 这一层是「**某一对具体实现**」：它给 `T: serde::Serialize` / `T: DeserializeOwned`
//! 这类业务类型提供 [`TrEncoder`](super::TrEncoder) / [`TrDecoder`](super::TrDecoder)
//! 的实现，而接口层并不因此对 `T` 设任何 serde 约束。
//!
//! # 编解码怎么落到缓冲上
//!
//! `rmp_serde` 的编解码入口要的是 `std::io::{Read, Write}`，而 `abs_buff` 的段接口是
//! 异步的；`abs_buff_stdio_adapt` 的 `AsStdWrite` / `AsStdRead` 把两者接起来：
//!
//! - 编码：`rmp_serde::encode::write` 把字节**逐段直接编进目标半边**，外面再套一层计数，
//!   好把写出的字节数回报出去。路径上没有中转缓冲——这正是「边序列化边发送」得以成立的
//!   原因：体一共多少字节，在写出去之前谁都不需要知道；
//! - 解码：`rmp_serde::from_read` 逐段从来源半边读出恰好一条值，并回报消耗的字节数。
//!
//! 这两个适配器内部是**同步等待**（数据没到时靠 `block_on_local` 驱动本线程的本地队列），
//! 那是本框架对 `serde` 的既有妥协，与报文前缀的读写路径同源：同步只停在适配器这一层，
//! 不外溢成调用方的语义——对外交出去的每个入口仍然是 `async` 且可取消的。
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
pub(in crate::codec) use decoder_::{CountingReaderDecodeAsync, CountingReaderDecodeFuture};
pub(in crate::codec) use encoder_::{CountingWriterEncodeAsync, CountingWriterEncodeFuture};
