use core:: mem::MaybeUninit;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use abs_buff::{TrBuffRead, TrBuffWrite};
use abs_buff_stdio_adapt::AsStdRead;
use abs_cancel::{TrCancellationToken, TrMayCancel};
use buffex::x_deps::{abs_buff::{self, Demand, buffer::TrBuffSegmMut}, abs_cancel};

use crate::{
    access_method::{AccessMethod, TrAccessMethod},
    messaging,
    specs::Headers,
};

struct ReqBuilderInner {
    method: Option<AccessMethod>,
    path: Option<String>,
    headers: Option<Headers>,
}

impl ReqBuilderInner {
    const fn new() -> Self {
        ReqBuilderInner {
            method: Option::None,
            path: Option::None,
            headers: Option::None,
        }
    }
}

pub struct RequestBuilder(Box<ReqBuilderInner>);

impl RequestBuilder {
    pub fn new() -> Self {
        RequestBuilder(Box::new(ReqBuilderInner::new()))
    }

    pub fn builder<M: TrAccessMethod>() -> Self {
        Self::new().method(M::method())
    }

    pub fn method(mut self, method: AccessMethod) -> Self {
        self.0.method = Option::Some(method);
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.0.path = Option::Some(path.into());
        self
    }

    pub fn headers(mut self, headers: impl TryInto<Headers>) -> Self {
        let Result::Ok(headers) = headers.try_into() else {
            return self;
        };
        self.0.headers = Option::Some(headers);
        self
    }

    pub fn body<T>(self, body: T) -> Self
    where
        T: Serialize + DeserializeOwned,
    {
        todo!()
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReqPrefix(pub AccessMethod, pub String, pub Option<Headers>);

impl ReqPrefix {
    pub fn method(&self) -> AccessMethod {
        self.0
    }

    pub fn location(&self) -> &str {
        &self.1
    }

    pub fn headers(&self) -> Option<&Headers> {
        self.2.as_ref()
    }
}

//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----
// io utils when sending and receiving request from IO.
//-- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ---- ----

/// 写入 request 的 method, location, headers。
/// 内部使用一个 Vec 来计算写了多少字节
pub(crate) async fn send_request_prefix_async<'f, TyReq, TyTx, TyTok>(
    req: &'f TyReq,
    tx: &'f mut TyTx,
    tok: TyTok,
) -> Result<usize, std::io::Error>
where
    TyReq: messaging::TrRpcRequest,
    TyTx: TrBuffWrite<u8>,
    TyTok: TrCancellationToken,
{
    fn serialize_to<Req: messaging::TrRpcRequest>(
        req: &Req,
        buf: &mut Vec<u8>,
    ) -> Result<(), rmp_serde::encode::Error> {
        let mut serializer = rmp_serde::Serializer::new(buf);
        req.method().serialize(&mut serializer)?;
        req.location().serialize(&mut serializer)?;
        req.headers().serialize(&mut serializer)?;
        Result::Ok(())
    }

    let mut buf = Vec::new();
    if let Result::Err(error) = serialize_to(req, &mut buf) {
        return Result::Err(std::io::Error::other(error))
    };
    let size = buf.len();
    if size == 0 {
        let err = "Zero bytes written during serialization.";
        return Result::Err(std::io::Error::other(err));
    }
    let demand = Demand::less_than(size);
    let mut opt_segm = tx
        .write_async(&demand)
        .may_cancel_with(tok)
        .await;
    if let Option::Some(segm) = opt_segm.as_mut().pick_left() {
        let buff = unsafe {
            let p = buf.as_mut_ptr() as *mut MaybeUninit<u8>;
            slice::from_raw_parts_mut(p, size)
        };
        let sent_size = unsafe { segm.move_items_from_buff(buff) };
        return Result::Ok(sent_size);
    }
    if let Option::Some(err) = opt_segm.pick_right() {
        let err = err.to_string();
        return Result::Err(std::io::Error::other(err));
    }
    Result::Ok(0usize)
}

/// Receive and deserialize the request prefix from stream
pub(crate) async fn recv_request_prefix_async<'f, TyRx, TyTok>(
    rx: &'f mut TyRx,
    tok: &'f mut TyTok,
) -> Result<ReqPrefix, std::io::Error>
where
    TyRx: TrBuffTryRead,
    TyTok: TrCancellationToken + Clone,
{
    fn deserialize_prefix<R, C>(
        r: &mut R,
        c: &mut C,
    ) -> Result<ReqPrefix, rmp_serde::decode::Error>
    where
        R: TrBuffTryRead,
        C: TrCancellationToken + Clone,
    {
        let mut std_read = AsStdRead::new(r, c);
        let des_method = rmp_serde::from_read::<_, AccessMethod>(&mut std_read);
        let method = match des_method {
            Result::Err(err) => return Result::Err(err),
            Result::Ok(m) => m,
        };
        let des_location = rmp_serde::from_read::<_, String>(&mut std_read);
        let location = match des_location {
            Result::Err(err) => return Result::Err(err),
            Result::Ok(m) => m,
        };
        let des_headers = rmp_serde::from_read::<_, Option<Headers>>(&mut std_read);
        let headers = match des_headers {
            Result::Err(err) => return Result::Err(err),
            Result::Ok(m) => m,
        };
        Result::Ok(ReqPrefix(method, location, headers))
    }

    deserialize_prefix(rx, tok).map_err(std::io::Error::other)
}
