use abs_buff::{
    TrBuffRead, TrBuffWrite,
    buffer::{TrConsumerState, TrProducerState},
};

pub trait TrCodecConfig {
    type ChannelTx: TrBuffWrite<u8> + TrProducerState;
    type ChannelRx: TrBuffRead<u8> + TrConsumerState;
}
