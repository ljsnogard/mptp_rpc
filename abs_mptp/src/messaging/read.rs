//! 报文体的读视图抽象：只规定「定长 / 分块二选一」，不绑定具体实现。
//!
//! 具体的 `ChunkedReader` / `LimitedReader` 与其组合枚举在 `mptp_wire`；
//! 本 trait 留在协议层，因为 `TrMptpSession` 的关联类型需要它。

use abs_buff::{TrBuffRead, buffer::TrConsumerState};

/// 对 MPTP 协议中流传输的抽象，即分块传输或者定长传输。
///
/// 合理的实现应该是一个 enum，但由于 enum 包含过多的泛型细节，可以采用本 trait
/// 来抽象指代。
pub trait TrMptpBuffRead {
    /// 分块传输的内容。
    type ChunkedRead: TrBuffRead<u8> + TrConsumerState;

    /// 定长内容。
    type LimitedRead: TrBuffRead<u8> + TrConsumerState;

    /// 测试内在流对象是否分块传输。
    fn try_into_chunked(self) -> Result<Self::ChunkedRead, Self::LimitedRead>;

    /// 测试内在流对象是否定长传输。
    fn try_into_limited(self) -> Result<Self::LimitedRead, Self::ChunkedRead>;
}
