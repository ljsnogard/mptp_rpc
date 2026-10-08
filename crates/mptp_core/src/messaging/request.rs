use core::{mem::MaybeUninit, slice};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use abs_buff::{
    Demand, TrBuffRead, TrBuffWrite,
    buffer::{TrBuffSegmMut, TrConsumerState},
    x_deps::abs_cancel,
};
use abs_buff_stdio_adapt::{AsStdRead, x_deps::abs_buff};
use abs_cancel::{TrCancellationToken, TrMayCancel};

use crate::{
    access_method::{AccessMethod, TrAccessMethod},
    messaging,
    specs::Headers,
    std_io_adapt_::StdReadAdapter,
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
        // SAFETY: `buf` 是本函数独占的 `Vec<u8>`，其前 `size` 个字节已初始化；
        // `MaybeUninit<u8>` 与 `u8` 布局相同（同尺寸、同对齐、无 niche），因此按
        // `size` 长度把这段已初始化内存重新解释为 `MaybeUninit<u8>` 切片是健全的。
        let buff = unsafe {
            let p = buf.as_mut_ptr() as *mut MaybeUninit<u8>;
            slice::from_raw_parts_mut(p, size)
        };
        let sent_size = segm.move_items_from_buff(buff);
        return Result::Ok(sent_size);
    }
    if let Option::Some(err) = opt_segm.pick_right() {
        let err = err.to_string();
        return Result::Err(std::io::Error::other(err));
    }
    Result::Ok(0usize)
}

/// Receive and deserialize the request prefix from stream
#[allow(dead_code)]
pub(crate) async fn recv_request_prefix_async<TyRx, TyTok>(
    rx: &mut TyRx,
    tok: TyTok,
) -> Result<ReqPrefix, std::io::Error>
where
    TyRx: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    fn deserialize_prefix<R, C>(
        r: &mut R,
        c: C,
    ) -> Result<ReqPrefix, rmp_serde::decode::Error>
    where
        R: TrBuffRead<u8> + TrConsumerState,
        C: TrCancellationToken,
    {
        // `AsStdRead` 按值持有令牌；再包一层转发，就不必要求令牌可克隆。
        let mut std_read = StdReadAdapter::new_(AsStdRead::new(r, c));
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
