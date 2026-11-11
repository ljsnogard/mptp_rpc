//! 服务端入口：一条 MPTP 连接上的 `MptpServlet`。
//!
//! `MptpServlet` 只服务**一条连接**上的入向子流：它持有一个单一 handler，不管理多条
//! 连接、监听拓扑或服务器级资源。`accept_async` 返回一条可移动的 [`ServingSession`]，
//! 调用者可以用自己的 executor 并发处理多条子流；`mptp_smux` 不 spawn。
//!
//! # `servlet_`: the per-connection server entry
//!
//! `MptpServlet` serves incoming sub-streams of **one** MPTP connection through a
//! **single** handler. `accept_async` returns a movable `ServingSession`; the caller
//! decides how to run many of them concurrently. The crate never spawns.

use alloc::string::ToString;

use core::marker::PhantomData;

use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::{NonCancellableToken, TrCancellationToken, TrMayCancel};
use abs_mptp::spec::Status;
use abs_smux::{
    chan::{TrChannelHalf, TrChannelHandle},
    conn::{TrChannelListener, TrDockBinding},
};
use gen_mcf2::gen_may_cancel_future;
use mptp_wire::{HeaderKey, HeaderVal, ResponsePrefix, read_request_prefix_async};

use crate::{
    copy_::drain_to_eof_async_,
    ring_prepare_::TrPrepFactory,
    error_::{ProtocolError, ServeError, TransportError},
    x_deps::{abs_buff, abs_cancel, abs_mptp, abs_smux, gen_mcf2},
};
use super::{
    config::{
        ChannelHandle, ChannelRx, DockBinding, Listener, MuxCfg, TrConfig,
    },
    handler_::TrServeHandler,
    prep_ctx_::ServingPrepCtx,
    session_::ServingSession,
};

/// 临时 `Allow` 头键：用户自定义区最高位。
///
/// 协议尚未给 `Allow` 分配标准头键；协议分配后替换本常量即可，上层 API 不变。
pub const K_TMP_ALLOW_HEADER_KEY: HeaderKey = HeaderKey::from_num(0x009E);

/// 临时 `Allow` 头的文本名。
pub const K_TMP_ALLOW_HEADER_NAME: &str = "allow";

/// 一次服务端处理的结果。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServeOutcome {
    /// 收尾阶段丢弃的请求字节数（未读完的体 + 未读的后缀）。
    pub dropped_req_bytes: usize,
}

/// 一条 MPTP 连接上的服务端入口。
pub struct MptpServlet<C, H>
where
    C: TrConfig,
    H: TrServeHandler<C>,
{
    /// 单一 handler。
    handler_: H,

    /// `C` 只出现在通道类型与 handler 约束里。
    _use_c_: PhantomData<fn() -> C>,
}

impl<C, H> MptpServlet<C, H>
where
    C: TrConfig,
    H: TrServeHandler<C>,
{
    /// 用单一 handler 构造 servlet。
    ///
    /// 一个 servlet 只服务一条连接；连接由调用者通过 listener 交给它。
    pub const fn new(handler: H) -> Self {
        MptpServlet {
            handler_: handler,
            _use_c_: PhantomData,
        }
    }

    /// 取回 handler。
    pub const fn handler(&self) -> &H {
        &self.handler_
    }

    /// 在 binding 上进入监听。
    pub fn listen_async<'f>(
        &'f self,
        binding: &'f mut DockBinding<C>,
        reserve: usize,
    ) -> <DockBinding<C> as TrDockBinding<MuxCfg<C>>>::ListenAsync<'f>
    where
        C: 'f,
    {
        binding.listen_async(reserve)
    }

    /// 接受一条入向子流：income → ring → accept → 读请求前缀。
    pub fn accept_async<'f>(
        &'f self,
        listener: &'f mut Listener<C>,
    ) -> AcceptAsync<'f, 'f, C>
    where
        C: 'f,
        ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        AcceptAsync::new(listener)
    }

    /// 用单一 handler 处理一条 session。
    pub fn serve_session_async<'f>(
        &'f self,
        session: ServingSession<C>,
    ) -> ServeSessionAsync<'f, 'f, C, H>
    where
        C: 'f,
        H: 'f,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        ServeSessionAsync::new(self, session)
    }

    /// 便捷：在当前 binding 上循环 accept 并 inline 处理。
    ///
    /// 这是「单线程 demo」路径；需要并发时用 [`MptpServlet::accept_async`] +
    /// 调用者自己的 spawner。
    pub fn serve_listener_async<'f>(
        &'f self,
        binding: &'f mut DockBinding<C>,
        reserve: usize,
    ) -> ServeListenerAsync<'f, 'f, C, H>
    where
        C: 'f,
        H: 'f,
        ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
        ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    {
        ServeListenerAsync::new(self, binding, reserve)
    }
}

/// [`MptpServlet::accept_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(Accept, pub, new(pub(crate)))]
async fn servlet_accept_async_<'f, C, K>(
    listener: &'f mut Listener<C>,
    cancel: K,
) -> Result<ServingSession<C>, ServeError>
where
    C: TrConfig + 'f,
    ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let mut handle = listener
        .income_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| {
            ServeError::Transport(TransportError::Accept {
                detail: err.to_string(),
            })
        })?;

    // 每条子流现场生产一份新的 ring；工厂由环境构造。
    let local_dock = handle.local_dock();
    let remote_dock = handle.remote_dock();
    let ctx = ServingPrepCtx::new(&local_dock, &remote_dock);
    let mut factory = C::DefaultPrepFactory::with_ctx(&ctx);
    let prepare = factory
        .get_prepared_async()
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| {
            ServeError::Transport(TransportError::RingPrepare {
                detail: err.to_string(),
            })
        })?;

    let mut welcome: &[u8] = &[];
    let (tx, mut rx) = handle
        .accept_async(&mut welcome, prepare)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| {
            ServeError::Transport(TransportError::Accept {
                detail: err.to_string(),
            })
        })?;

    let prefix = read_request_prefix_async(&mut rx)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| ServeError::Protocol(ProtocolError::RequestPrefixDecode(err)))?;

    ServingSession::new_(prefix, tx, rx)
}

/// [`MptpServlet::serve_session_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ServeSession, pub, new(pub(crate)))]
async fn servlet_serve_session_async_<'f, C, H, K>(
    servlet: &'f MptpServlet<C, H>,
    mut session: ServingSession<C>,
    cancel: K,
) -> Result<ServeOutcome, ServeError>
where
    C: TrConfig + 'f,
    H: TrServeHandler<C> + 'f,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let method = session.method();
    let allow = servlet.handler_.allowed_methods();

    // 405：方法不在 handler 声明的集合内。先写回复，再收尾，不进入 handler。
    if !allow.contains(method) {
        let mut prefix = ResponsePrefix::new(Status::MethodNotAllowed);
        prefix.headers_mut().set(
            K_TMP_ALLOW_HEADER_KEY,
            HeaderVal::from_text(allow.to_allow_value()),
        );
        session
            .response_prefix(prefix)
            .no_body_async()
            .may_cancel_with(cancel.child_token())
            .await?;
        let _ = session
            .finish_async()
            .may_cancel_with(cancel.child_token())
            .await?;
        return Result::Err(ServeError::NotAllowed { method, allow });
    }

    let result = servlet
        .handler_
        .handle_async(method, &mut session)
        .may_cancel_with(cancel.child_token())
        .await;

    match result {
        Result::Ok(()) => {
            if session.has_channel_() && !session.responded_() {
                // handler 没写回复：兜底 500，不把业务细节泄漏到线上。
                let _ = session
                    .response(Status::InternalServerError)
                    .no_body_async()
                    .may_cancel_with(cancel.child_token())
                    .await;
            }
            let dropped_req_bytes = if session.has_channel_() {
                session
                    .finish_async()
                    .may_cancel_with(cancel.child_token())
                    .await?
            } else {
                0usize
            };
            Result::Ok(ServeOutcome { dropped_req_bytes })
        }
        Result::Err(err) => {
            if session.has_channel_() && !session.responded_() {
                let _ = session
                    .response(Status::InternalServerError)
                    .no_body_async()
                    .may_cancel_with(cancel.child_token())
                    .await;
            }
            if session.has_channel_() {
                let _ = session
                    .finish_async()
                    .may_cancel_with(cancel.child_token())
                    .await;
            }
            Result::Err(ServeError::Target {
                detail: err.to_string(),
            })
        }
    }
}

/// [`MptpServlet::serve_listener_async`] 的 step 函数。
#[allow(clippy::needless_lifetimes)]
#[gen_may_cancel_future(ServeListener, pub, new(pub(crate)))]
async fn servlet_serve_listener_async_<'f, C, H, K>(
    servlet: &'f MptpServlet<C, H>,
    binding: &'f mut DockBinding<C>,
    reserve: usize,
    cancel: K,
) -> Result<(), ServeError>
where
    C: TrConfig + 'f,
    H: TrServeHandler<C> + 'f,
    ChannelHandle<C>: TrChannelHalf<MuxCfg<C>>,
    ChannelRx<C>: TrBuffRead<u8> + TrConsumerState,
    K: TrCancellationToken,
{
    let mut listener = servlet
        .listen_async(binding, reserve)
        .may_cancel_with(cancel.child_token())
        .await
        .map_err(|err| {
            ServeError::Transport(TransportError::Listen {
                detail: err.to_string(),
            })
        })?;

    loop {
        let session = servlet
            .accept_async(&mut listener)
            .may_cancel_with(cancel.child_token())
            .await?;
        match servlet
            .serve_session_async(session)
            .may_cancel_with(cancel.child_token())
            .await
        {
            Result::Ok(_) => {}
            // 单个请求被拒或 handler 报错不应拆掉整条监听循环。
            Result::Err(ServeError::NotAllowed { .. }) | Result::Err(ServeError::Target { .. }) => {
            }
            Result::Err(err) => return Result::Err(err),
        }
    }
}

/// 关闭一条已经处理完的子流：先半关闭写端，再读到对端关闭为止。
///
/// 这是 escape hatch 路径（handler 自己驱动 `ServingSession`）的标准收尾入口。顺序
/// 不能反：先放掉 `rx` 会发 `RESET`，可能把还没排空的响应抹掉。
///
/// 返回本条子流上被丢弃的字节数；收尾阶段的读错误**不**上报——它已经不影响已经
/// 写出的响应。
pub async fn close_channel_async<TyTx, TyRx>(tx: TyTx, rx: &mut TyRx) -> usize
where
    TyRx: TrBuffRead<u8>,
{
    drop(tx);
    drain_to_eof_async_(rx, NonCancellableToken::new()).await
}

#[cfg(test)]
mod tests_ {
    use super::*;
    use crate::MethodSet;

    /// 测试 `405` 回复的 `Allow` 头可以写出并解析回原集合。
    /// Test that the `Allow` header of a `405` response round-trips through the header map.
    ///
    /// - 手段：构造 `POST | CALL` 的 `MethodSet`，写进 `ResponsePrefix` 的临时 `Allow` 头，
    ///   再读出来解析。
    /// - 判断：头值是 `"POST,CALL"`，解析结果与原始集合相等。
    ///
    /// - Means: write a `POST | CALL` `MethodSet` into a `ResponsePrefix`'s temporary
    ///   `Allow` header and parse it back.
    /// - Verdict: the value is `"POST,CALL"` and parsing yields the original set.
    #[test]
    fn method_not_allowed_allow_header_round_trips_() {
        let allow = MethodSet::POST.union(MethodSet::CALL);
        let mut prefix = ResponsePrefix::new(Status::MethodNotAllowed);
        prefix.headers_mut().set(
            K_TMP_ALLOW_HEADER_KEY,
            HeaderVal::from_text(allow.to_allow_value()),
        );

        let headers = prefix.headers().expect("刚刚写过头表");
        let value = headers
            .try_get(K_TMP_ALLOW_HEADER_KEY)
            .expect("Allow 头应当在场");
        let text = value.as_text().expect("Allow 头应当是文本");
        assert_eq!(text, "POST,CALL");
        assert_eq!(MethodSet::from_allow_value(text), allow);
    }

    /// 测试 `404` 是普通状态码，不需要框架级错误。
    /// Test that `404` is an ordinary status, not a framework-level error.
    ///
    /// - 手段：构造 `Status::NotFound` 回复前缀。
    /// - 判断：状态码原样保留，`ResponsePrefix` 可以正常构造。
    ///
    /// - Means: build a `Status::NotFound` response prefix.
    /// - Verdict: the status is preserved and the prefix constructs normally.
    #[test]
    fn not_found_is_ordinary_status_() {
        let prefix = ResponsePrefix::new(Status::NotFound);
        assert_eq!(prefix.status(), Status::NotFound);
    }
}
