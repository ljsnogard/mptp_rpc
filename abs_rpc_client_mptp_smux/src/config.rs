use abs_mptp::{
    client as mptp_client,
    client::config as mptp_cli_cfg,
    spec::TrHeaders,
};
use crate::x_deps::{abs_mptp};

/// The RPC config.
pub trait TrRpcConfig {
    /// The MPTP client type used by the RPC client
    type MptpClient: abs_mptp::client::TrMptpClient;

    type ReqHeaders: TrHeaders;

    type RespHeader: TrHeaders;
}

/// The MPTP client type associated with `C: abs_rpc_mptp_smux::client::TrConfig`;
pub type MptpClient<TyRpcCfg> = <TyRpcCfg as self::TrRpcConfig>::MptpClient;

/// The MPTP client config type associated with `C: abs_rpc_mptp_smux::client::TrConfig`;
pub type MptpClientCfg<TyRpcCfg> = <MptpClient<TyRpcCfg> as mptp_client::TrMptpClient>::Config;

/// The dock type associated with `C: abs_rpc_mptp_smux::client::TrConfig`
pub type Dock<TyRpcCfg> = mptp_cli_cfg::Dock<MptpClientCfg<TyRpcCfg>>;

pub type ReqHeaders<TyRpcCfg> = <TyRpcCfg as self::TrRpcConfig>::ReqHeaders;
pub type RespHeaders<TyRpcCfg> = <TyRpcCfg as self::TrRpcConfig>::RespHeader;
