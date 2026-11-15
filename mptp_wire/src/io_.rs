//! 报文 IO 的公共地基：精确读满固定长度时的**具体**错误类型。
//!
//! 这一层刻意不含任何缓冲：限长与分块两个视图都建立在
//! [`TrBuffRead`](abs_buff::TrBuffRead) 的段接口之上，本模块只提供它们共用的错误。

use core::{error::Error, fmt};

use abs_buff::error::{ReadErrTag, TrTaggedError};

/// 从读缓冲精确读满一段字节时的错误。
///
/// 与旧的字符串错误不同，每个分支都保留「期望多少、已读多少」这两个可判定的字段；
/// 底层缓冲的错误原样放进 [`ReadExactError::Inner`]，不会退化成一段文本。
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadExactError<TyErr> {
    /// 读满之前调用方通过取消令牌取消了等待。
    Cancelled {
        /// 本次读取期望的总字节数。
        expected: usize,
        /// 取消前已经搬走的字节数。
        filled: usize,
    },

    /// 底层缓冲让出的段一个字节都搬不动，继续等待只会空转。
    Stalled {
        /// 本次读取期望的总字节数。
        expected: usize,
        /// 报错前已经搬走的字节数。
        filled: usize,
    },

    /// 底层读缓冲自身报错（包括对端提前关闭）。
    Inner(TyErr),
}

impl<TyErr> fmt::Display for ReadExactError<TyErr>
where
    TyErr: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadExactError::Cancelled { expected, filled } => {
                write!(
                    f,
                    "精确读取被取消：期望 {expected} 字节，已读 {filled} 字节"
                )
            }
            ReadExactError::Stalled { expected, filled } => {
                write!(
                    f,
                    "精确读取没有进展：期望 {expected} 字节，已读 {filled} 字节"
                )
            }
            ReadExactError::Inner(err) => write!(f, "精确读取时底层缓冲报错：{err}"),
        }
    }
}

// 不实现 `source()`：错误类型参数不要求 `'static`，保留它可以让调用方
// 继续把非 `'static` 的底层错误放进 `Inner`。
impl<TyErr> Error for ReadExactError<TyErr> where TyErr: Error {}

impl<TyErr> TrTaggedError<ReadErrTag> for ReadExactError<TyErr>
where
    TyErr: Error + TrTaggedError<ReadErrTag>,
{
    fn err_tag(&self) -> ReadErrTag {
        match self {
            ReadExactError::Cancelled { .. } => ReadErrTag::Cancelled,
            ReadExactError::Stalled { .. } => ReadErrTag::Unsatisfied,
            ReadExactError::Inner(err) => err.err_tag(),
        }
    }
}
