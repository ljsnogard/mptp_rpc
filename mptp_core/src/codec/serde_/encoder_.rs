//! 内置编解码器在**编码**方向的实现。

use abs_buff::{TrBuffWrite, buffer::TrProducerState, gen_may_cancel_future, x_deps::abs_cancel};
use abs_buff_stdio_adapt::AsStdWrite;
use abs_cancel::TrCancellationToken;

use super::codec_::Codec;
use crate::codec::{
    CodecError, TrCodecConfig, TrEncoder,
    encode_::{EncodeAsync, EncodeBuffWrite, EncodeDriver},
};

/// 给 [`AsStdWrite`] 套一层写计数：编码器要回报写了多少字节。
pub struct CountingWriter<'a, W, C>
where
    W: TrBuffWrite<u8> + TrProducerState,
    C: TrCancellationToken,
{
    inner_: AsStdWrite<'a, W, C>,
    written_: usize,
}

impl<'a, W, C> CountingWriter<'a, W, C>
where
    W: TrBuffWrite<u8> + TrProducerState,
    C: TrCancellationToken,
{
    pub(super) fn new_(inner: AsStdWrite<'a, W, C>) -> Self {
        CountingWriter {
            inner_: inner,
            written_: 0usize,
        }
    }

    pub const fn written(&self) -> usize {
        self.written_
    }
}

impl<W, C> std::io::Write for CountingWriter<'_, W, C>
where
    W: TrBuffWrite<u8> + TrProducerState,
    C: TrCancellationToken,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner_.write(buf)?;
        self.written_ += written;
        Result::Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner_.flush()
    }
}

/// serde 类 `T` 的编码实现。
///
/// serde 约束落在这里——接口层的 [`TrEncoder`] 不提它。
impl<T, C> TrEncoder<T, C> for Codec
where
    T: serde::Serialize,
    C: TrCodecConfig,
{
    fn encode_async<'f>(
        &'f self,
        data: &'f T,
        buff: &'f mut EncodeBuffWrite<C>,
    ) -> EncodeAsync<'f, T, C> {
        EncodeAsync::new_(data, buff, EncodeDriver::Serde(self))
    }
}

#[gen_may_cancel_future(CountingWriteEncode, pub, new(pub(in crate::codec)))]
async fn count_write_encode_async_<'f, T, C, K>(
    codec: &'f Codec,
    data: &'f T,
    buff: &'f mut EncodeBuffWrite<C>,
    cancel: K,
) -> Result<usize, CodecError>
where
    T: serde::Serialize,
    C: TrCodecConfig,
    K: TrCancellationToken,
{
    // `AsStdWrite` 把目标半边暴露成 `std::io::Write`，`rmp_serde` 于是可以直接往环里
    // 编码；外面再套一层计数，好把 `Body_Size` 需要的长度报回去。
    let target = buff.target();
    let mut write = CountingWriter::new_(AsStdWrite::new(target, cancel));
    match codec {
        Codec::MsgPack => rmp_serde::encode::write(&mut write, data)
            .map_err(|err| CodecError::Encode(err.to_string()))?,
        Codec::Json => todo!("Support JSON codec at the moment."),
    }
    Result::Ok(write.written())
}
