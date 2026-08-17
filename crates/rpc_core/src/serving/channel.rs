use std::boxed::Box;

use buffex::ring_buffer::{RingBuffer, TrRingBuffer};

use crate::transport::TrChannel;

type RingBuf = RingBuffer<Box<[u8]>>;

struct SvcChanCore {
    /// Buffer containing data to send to the client.
    tx_: RingBuf,

    /// Buffer containing data received from the client.
    rx_: RingBuf,
}

/// 专门用于服务端环境，传递给 handler 使用的 channel
pub struct ServiceChannel(Box<SvcChanCore>);

impl TrChannel for ServiceChannel {
    /// No this is not true implement, just a dummy type.
    type Rx<'f> = <RingBuf as TrRingBuffer>::Rx<'f>
    where
        Self: 'f;

    /// No this is not true implement, just a dummy type.
    type Tx<'f> = <RingBuf as TrRingBuffer>::Tx<'f>
    where
        Self: 'f;

    fn split(&mut self) -> (Self::Tx<'_>, Self::Rx<'_>) {
        todo!()
    }
}
