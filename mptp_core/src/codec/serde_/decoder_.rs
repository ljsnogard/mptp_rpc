//! 内置编解码器在**解码**方向的实现。

use core::mem::MaybeUninit;

use abs_buff::{TrBuffRead, buffer::TrConsumerState, gen_may_cancel_future, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdRead;
use abs_cancel::TrCancellationToken;

use super::codec_::Codec;
use crate::codec::{
    CodecError, TrCodecConfig, TrDecoder,
    decode_::{DecodeAsync, DecodeBuffRead, DecodeDriver},
};

/// 给 [`AsStdRead`] 套一层读计数：解码器要回报消耗了多少字节。
pub struct CountingReader<'a, R, K>
where
    R: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    inner_: AsStdRead<'a, R, K>,
    read_: usize,
}

impl<'a, R, C> CountingReader<'a, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    pub(super) fn new_(inner: AsStdRead<'a, R, C>) -> Self {
        CountingReader {
            inner_: inner,
            read_: 0usize,
        }
    }

    pub const fn read_count(&self) -> usize {
        self.read_
    }
}

impl<R, C> std::io::Read for CountingReader<'_, R, C>
where
    R: TrBuffRead<u8> + TrConsumerState,
    C: TrCancellationToken,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = std::io::Read::read(&mut self.inner_, buf)?;
        self.read_ += read;
        Result::Ok(read)
    }
}

/// serde 类 `T` 的解码实现。
///
/// serde 约束落在这里——接口层的 [`TrDecoder`] 不提它。
impl<T, C> TrDecoder<T, C> for Codec
where
    C: TrCodecConfig,
{
    fn decode_async<'f>(
        &'f self,
        data: &'f mut MaybeUninit<T>,
        buff: &'f mut DecodeBuffRead<C>,
    ) -> DecodeAsync<'f, T, C> {
        DecodeAsync::new_(data, buff, DecodeDriver::Serde(self))
    }
}

#[gen_may_cancel_future(CountingReaderDecode, pub, new(pub(in crate::codec)))]
async fn count_read_decode_async_<'f, T, C, K>(
    codec: &'f Codec,
    data: &'f mut MaybeUninit<T>,
    buff: &'f mut DecodeBuffRead<C>,
    cancel: K,
) -> Result<usize, CodecError>
where
    T: serde::de::DeserializeOwned,
    C: TrCodecConfig,
    K: TrCancellationToken,
{
    // `AsStdRead` 把来源半边暴露成 `std::io::Read`，计数层回报消耗了多少字节。
    let source = buff.source();
    let mut read = CountingReader::new_(AsStdRead::new(source, cancel));
    let value = match codec {
        Codec::MsgPack => rmp_serde::from_read::<_, T>(&mut read)
            .map_err(|err| CodecError::Decode(err.to_string()))?,
        Codec::Json => {
            return Result::Err(CodecError::Decode(
                "JSON 解码尚未实现".to_string(),
            ));
        }
    };
    data.write(value);
    Result::Ok(read.read_count())
}
