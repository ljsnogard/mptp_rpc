mod codec_;
mod config_;
mod decode_;
mod encode_;
mod registry_;
mod serde_;
mod type_id_;

pub use codec_::{CodecError, TrDecoder, TrEncoder};
pub use config_::TrCodecConfig;
pub use registry_::{CodecRegistry, TrCodecPair};
pub use serde_::Codec;
pub use type_id_::{TrGetTypeId, hash_str};
