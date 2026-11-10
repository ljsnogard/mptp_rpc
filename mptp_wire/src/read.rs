//! 报文体读视图的具体组合：定长 / 分块二选一。
//!
//! [`TrMptpBuffRead`] 的抽象留在 `abs_mptp`；本模块提供**具体**的枚举，把
//! [`ChunkedReader`] 与 [`LimitedReader`] 装进同一个类型。

use abs_buff::{TrBuffRead, buffer::TrConsumerState};
pub use abs_mptp::messaging::read::TrMptpBuffRead;

use crate::{chunked::ChunkedReader, limited::LimitedReader};

/// 在 MPTP 协议中封装的两种读流，要么是定长，要么分块。
pub enum MptpBuffRead<'a, R>
where
    R: TrBuffRead<u8> + TrConsumerState,
{
    /// 分块视图。
    Chunked(ChunkedReader<'a, R>),

    /// 定长视图。
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
