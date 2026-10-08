use abs_buff::{TrBuffRead, TrBuffWrite, x_deps::abs_cancel};
use abs_cancel::TrCancellationToken;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    access_method::{AccessMethod, TrAccessMethod},
    decode_::read_value_async_,
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

    pub fn body<T>(self, _body: T) -> Self
    where
        T: Serialize + DeserializeOwned,
    {
        todo!()
    }
}

impl Default for RequestBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// 服务端解码路径（`serving`）当前被暂时摘除，本类型与下面的解码函数暂时无人使用；
// 等 `serving` 按 `abs_smux` 的 channel 重做回来即可移除这几处 allow。
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
pub(crate) struct ReqPrefix(pub AccessMethod, pub String, pub Option<Headers>);

#[allow(dead_code)]
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
///
/// 先序列化到本地 `Vec` 以得到 `Body_Size` 需要的长度，再用共享的
/// [`write_all_async_`](crate::encode_::write_all_async_) 完整写进发送半边。
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
        return Result::Err(std::io::Error::other(error));
    }
    let size = buf.len();
    if size == 0 {
        let err = "Zero bytes written during serialization.";
        return Result::Err(std::io::Error::other(err));
    }
    crate::encode_::write_all_async_(tx, &buf, &tok).await?;
    Result::Ok(size)
}

/// Receive and deserialize the request prefix from stream
#[allow(dead_code)]
pub(crate) async fn recv_request_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    tok: TyTok,
) -> Result<ReqPrefix, std::io::Error>
where
    TyRx: TrBuffRead<u8>,
    TyTok: TrCancellationToken,
{
    // 三个前缀值共用一份累积缓冲与「已消费字节数」，因此不会重复解析前面的字节。
    let mut buf: Vec<u8> = Vec::new();
    let mut consumed = 0usize;
    let method: AccessMethod = read_value_async_(rx, &mut buf, &mut consumed, &tok).await?;
    let location: String = read_value_async_(rx, &mut buf, &mut consumed, &tok).await?;
    let headers: Option<Headers> = read_value_async_(rx, &mut buf, &mut consumed, &tok).await?;
    Result::Ok(ReqPrefix(method, location, headers))
}
