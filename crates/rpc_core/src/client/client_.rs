use core::ops::Deref;

use thiserror::Error;

use abs_buff::gen_may_cancel_future;
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::{
    messaging,
    transport::{self, TrChannel, TrMuxConn},
};
use super::session::Session;

#[derive(Debug, Error)]
pub enum OperationError {
    #[error("IO error: {0}")]
    IoErr(String),

    #[error("Rpc error: {0}")]
    RpcErr(String),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("Async operation cancelled by user")]
    Cancelled,

    #[error("no connection available for client")]
    ConnectionLost,

    #[error("Error occurs during sending request: {0}")]
    ReqErr(String),

    #[error("Error occurs during recving response: {0}")]
    RespErr(String),
}

pub struct Client<TyConn>
where
    TyConn: Deref<Target: transport::TrMuxConn> + Clone,
{
    conn_: Option<TyConn>,
}

impl<TyConn> Client<TyConn>
where
    TyConn: Deref<Target: transport::TrMuxConn> + Clone,
{
    pub const fn new(conn: TyConn) -> Self {
        Client { conn_: Option::Some(conn) }
    }

    pub fn request_async<'f, TyReq>(
        &'f self,
        request: &'f TyReq,
    ) -> SendClientRequestAsync<'f, TyConn, TyReq>
    where
        TyReq: messaging::TrRpcRequest,
    {
        SendClientRequestAsync(self, request)
    }
}

type ChannelTypeFrom<TyConn> = <<TyConn as Deref>::Target as transport::TrMuxConn>::Channel;

#[gen_may_cancel_future(SendClientRequest)]
async fn client_request_async_<'f, TyConn, TyReq, TyTok>(
    client : &'f Client<TyConn>,
    request: &'f TyReq,
    cancel : &'f mut TyTok,
) -> Result<Session<'f, TyReq, ChannelTypeFrom<TyConn>>, ClientError>
where
    TyConn: Deref<Target: transport::TrMuxConn> + Clone,
    TyReq: messaging::TrRpcRequest,
    TyTok: TrCancellationToken + Clone,
{
    let Option::Some(conn) = &client.conn_ else {
        return Result::Err(ClientError::ConnectionLost);
    };
    let Result::Ok(mut channel) = conn.open_channel_async().may_cancel_with(cancel).await else {
        return Result::Err(ClientError::ConnectionLost);
    };
    let (mut tx, _) = channel.split();
    let send_prefix_res = messaging::request::send_request_prefix_async(request, &mut tx, cancel).await;
    match send_prefix_res {
        Result::Err(_err) => return Result::Err(ClientError::ConnectionLost),
        Result::Ok(_c) => (),
    };
    drop(tx);
    let session = Session::new(request, channel);
    Result::Ok(session)
}
