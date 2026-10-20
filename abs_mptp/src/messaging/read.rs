use abs_buff::{TrBuffRead, buffer::TrConsumerState};

use crate::x_deps::abs_buff;
use super::{chunked::ChunkedReader, limited::LimitedReader};


/// 对 MPTP 协议中流传输的抽象，即分块传输或者定长传输。
///
/// 合理的实现应该是一个 enum，但由于 enum 包含过多的泛型细节，可以
/// 采用本 trait 来抽象指代。
pub trait TrMptpBuffRead {
    /// 分块传输的内容
    type ChunkedRead: TrBuffRead<u8> + TrConsumerState;

    /// 定长内容
    type LimitedRead: TrBuffRead<u8> + TrConsumerState;

    /// 测试内在流对象是否分块传输
    fn try_into_chunked(self) -> Result<Self::ChunkedRead, Self::LimitedRead>;

    /// 测试内在流对象是否定长传输
    fn try_into_limited(self) -> Result<Self::LimitedRead, Self::ChunkedRead>;
}

/// 在 MPTP 协议中封装的两种读流，要么是定长，要么分块
pub enum MptpBuffRead<'a, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    Chunked(ChunkedReader<'a, R>),
    Limited(LimitedReader<'a, R>),
}

impl<'a, R> TrMptpBuffRead for MptpBuffRead<'a, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    type ChunkedRead = ChunkedReader<'a, R>;
    type LimitedRead = LimitedReader<'a, R>;

    fn try_into_chunked(self) -> Result<Self::ChunkedRead, Self::LimitedRead> {
        match self {
            Self::Chunked(x) => Result::Ok(x),
            Self::Limited(x) => Result::Err(x),
        }
    }

    fn try_into_limited(self) -> Result<Self::LimitedRead, Self::ChunkedRead> {
        match self {
            Self::Chunked(x) => Result::Err(x),
            Self::Limited(x) => Result::Ok(x),
        }
    }
}
