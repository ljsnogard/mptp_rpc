use core::{
    borrow::Borrow,
    marker::PhantomData,
    mem::MaybeUninit,
};

use thiserror::Error;

use abs_cancel::{TrCancellationToken, TrMayCancel};

use abs_buff::{
    gen_may_cancel_future,
    x_deps::abs_cancel,
};

use crate::{
    messaging,
    conn::{TrMuxConn, TrChannel},
};

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

    #[error("Error occurs during sending request: {0}")]
    ReqErr(String),

    #[error("Error occurs during recving response: {0}")]
    RespErr(String),
}

pub struct Client<TyBorrow, TyConn>
where
    TyBorrow: Borrow<TyConn>,
    TyConn: TrMuxConn,
{
    conn_: TyBorrow,
    _con_: PhantomData<TyConn>,
}

impl<TyBorrow, TyConn> Client<TyBorrow, TyConn>
where
    TyBorrow: Borrow<TyConn>,
    TyConn: TrMuxConn,
{
    pub const fn new(conn: TyBorrow) -> Self {
        Client {
            conn_: conn,
            _con_: PhantomData,
        }
    }
}

pub(crate) struct RequestBuilder;

pub struct HeadRequestBuilder;

pub struct ViewRequestBuilder;

pub struct PostRequestBuilder;

pub struct DropRequestBuilder;

pub struct PushRequestBuilder;

pub struct PullRequestBuilder;

pub struct CallRequestBuilder;


#[gen_may_cancel_future(ChannelReqNilBody)]
async fn channel_req_nil_body_async_<'f, TyChannel, TyCancel>(
    channel: &'f mut TyChannel,
    request: &'f messaging::Request,
    cancel: &'f mut TyCancel,
) -> Result<messaging::Response, ClientError>
where
    TyChannel: TrChannel,
    TyCancel: TrCancellationToken,
{
    let (mut tx, mut rx) = channel.split();
    let mut encode = messaging::EncoderNilBody::new(request);
    let Option::Some(task) = encode.try_write(&mut tx) else {
        todo!()
    };
    let send_res = task.may_cancel_with(cancel).await;
    if let Result::Err(err) = send_res {
        return Result::Err(ClientError::ReqErr(err.to_string()))
    };
    let mut m_resp = MaybeUninit::<messaging::Response>::uninit();
    let resp_res = messaging::decode_msg_async_(&mut m_resp, &mut rx, cancel).await;
    let resp = match resp_res {
        Result::Err(err) => return Result::Err(ClientError::RespErr(err.to_string())),
        Result::Ok(resp) => resp,
    };
    if resp.try_get_body_type().is_some() {

    }
    // TODO: check header and body
    Result::Ok(resp)
}
