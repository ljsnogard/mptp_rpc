use abs_buff::gen_may_cancel_future;
use abs_cancel::TrCancellationToken;
use buffex::x_deps::{abs_buff, abs_cancel};

use crate::{
    client::ClientError, messaging::{self, response::RespPrefix}, transport::TrChannel,
};

pub struct Session<'a, TyReq, TyChan>
where
    TyReq: messaging::TrRpcRequest,
    TyChan: TrChannel,
{
    channel_: TyChan,
    request_: &'a TyReq,
    response_: Option<RespPrefix>,
}

impl<'a, TyReq, TyChan> Session<'a, TyReq, TyChan>
where
    TyReq: messaging::TrRpcRequest,
    TyChan: TrChannel,
{
    pub(crate) const fn new(
        request: &'a TyReq,
        channel: TyChan,
    ) -> Self {
        Session {
            channel_: channel,
            request_: request,
            response_: Option::None,
        }
    }

    pub fn recv_response_async<'f>(
        &'f mut self,
    ) -> SessionRecvRespAsync<'a, 'f, TyReq, TyChan> {
        const MAX_LEN: usize = 1024;
        SessionRecvRespAsync(self, MAX_LEN)
    }
}

#[gen_may_cancel_future(SessionRecvResp)]
async fn sess_recv_resp_async_<'a, 'f, TyReq, TyChan, TyTok>(
    session: &'f mut Session<'a, TyReq, TyChan>,
    max_len: usize,
    cancel: &'f mut TyTok,
) -> Result<RespPrefix, ClientError>
where
    'a: 'f,
    TyReq: messaging::TrRpcRequest,
    TyChan: TrChannel,
    TyTok: TrCancellationToken + Clone,
{
    let (_, mut rx) = session.channel_.split();
    let recv_res = messaging::response::recv_response_prefix_async(&mut rx, max_len, cancel).await;
    let resp = match recv_res {
        Result::Err(err) => {
            let info = format!("Error ({}) in receiving response", err);
            return Result::Err(ClientError::ConnectionLost(info))
        },
        Result::Ok(resp) => resp,
    };
    Result::Ok(resp)
}
