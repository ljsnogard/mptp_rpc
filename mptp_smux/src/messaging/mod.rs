//! MPTP 报文前缀的**自有线格式**。
//!
//! 这一层把「前缀怎么变成字节」从使用者的配置里收回来：`mptp_smux` 直接依赖 `mp_denco`，
//! 按协议规定的顺序编码 / 解码请求与回复前缀，因此客户端与服务端不再各自实现一份 codec。
//!
//! # 格式
//!
//! - 请求前缀：`access_method + location + headers`；
//! - 回复前缀：`status + headers`。
//!
//! 每一部分都是一个**独立的 MessagePack 值**，按上面列出的顺序连续写出；把它们装进元组或
//! 结构体会多写一个容器头，改变线上字节。头表是「数字键 → 值」的映射，没有头时写 `nil`。
//!
//! # 边界
//!
//! 本模块只负责**前缀**。报文体的序列化格式、以及体之后的后缀流，仍然属于用户应用自己的
//! 范畴：`mptp_smux` 只按 `Body_Size` / `Body_Transfer` 搬运字节，不解释它们的业务含义。
//! 读前缀时不会多读一个字节，所以体的第一个字节始终留在 ring 里交给调用者。
//!
//! # 对外 API 与 RPC 框架
//!
//! [`RequestPrefix`] 保留完整的 `location`，因此 RPC 层可以把
//! `RpcClientInterface::path()` 与服务名、方法名拼成 `/api/v1/customer/goods` 这样的路径；
//! 服务端的 [`Router`](crate::serving::Router) 再按最长前缀把它分派给 handler。
//! [`Headers`] 是开放的数字键映射，RPC 层可以在标准头之外携带自定义元数据。
//!
//! # `messaging`: the wire format owned by `mptp_smux`
//!
//! This layer takes "how a prefix becomes bytes" away from the user's configuration:
//! `mptp_smux` depends on `mp_denco` directly and encodes/decodes request and response
//! prefixes in protocol order, so client and server no longer need two codec
//! implementations.
//!
//! Only the **prefix** is covered. Body serialization and the suffix stream remain the
//! user's own concern; decoding a prefix never touches the first body byte.

mod error_;
mod headers_;
mod prefix_;
mod wire_;

pub use error_::MessagingError;
pub use headers_::{HeaderEntry, HeaderVal, Headers};
pub use prefix_::{RequestPrefix, ResponsePrefix};
pub use wire_::{
    ReadRequestPrefixAsync, ReadResponsePrefixAsync, WriteRequestPrefixAsync,
    WriteResponsePrefixAsync, read_request_prefix_async, read_response_prefix_async,
    write_request_prefix_async, write_response_prefix_async,
};
