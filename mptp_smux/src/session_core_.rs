//! 客户端与服务端共享的会话核心：两个半边、请求前缀、体边界与体进度。
//!
//! 它不持有方向性状态；客户端 `MptpSession` 与服务端 `ServingSession` 各自在它之上
//! 维护自己的状态枚举。这样一条连接上可以并行存在多个会话，而体边界判定、体视图与
//! 收尾原语只有一份实现。
//!
//! # `session_core_`: the shared session core
//!
//! The core holds the two halves, the request prefix, the body boundary and the body
//! progress. Directional state lives in `MptpSession` / `ServingSession`; the boundary,
//! views and teardown primitives are implemented once.

use abs_buff::{
    TrBuffRead,
    buffer::TrConsumerState,
};
use abs_cancel::TrCancellationToken;
use mptp_wire::{
    RequestPrefix,
    chunked::ChunkedReader,
    limited::LimitedReader,
};

use crate::{
    body_::{BodyPlan, BodyState},
    copy_::drain_to_end_async_,
    error_::StateError,
    x_deps::{abs_buff, abs_cancel},
};

/// 一次会话的共享核心。
pub(crate) struct SessionCore<Tx, Rx> {
    /// 本次请求的前缀。服务端只用体边界，不读前缀，因此允许在 `server`-only 构建里
    /// 保留这个字段。
    #[allow(dead_code)]
    request_: Option<RequestPrefix>,

    /// 请求体的边界；服务端在构造时定下，客户端在发出前缀后定下。
    body_plan_: Option<BodyPlan>,

    /// 发送半边；`take_tx` / `take_channel` 之后为 `None`。
    tx_: Option<Tx>,

    /// 接收半边；`take_rx` / `take_channel` 之后为 `None`。
    rx_: Option<Rx>,

    /// 体的读取 / 发送进度。
    body_: BodyState,
}

impl<Tx, Rx> SessionCore<Tx, Rx> {
    /// 由「请求前缀 + 体边界 + 两个半边」构造。
    pub(crate) fn new_(
        request: RequestPrefix,
        body_plan: BodyPlan,
        tx: Tx,
        rx: Rx,
    ) -> Self {
        SessionCore {
            request_: Option::Some(request),
            body_plan_: Option::Some(body_plan),
            tx_: Option::Some(tx),
            rx_: Option::Some(rx),
            body_: BodyState::Fresh,
        }
    }

    /// 请求前缀。
    #[cfg(feature = "client")]
    pub(crate) fn request_(&self) -> Option<&RequestPrefix> {
        self.request_.as_ref()
    }

    /// 请求体的边界。
    pub(crate) fn body_plan_(&self) -> Option<BodyPlan> {
        self.body_plan_
    }

    /// 覆盖请求体的边界。
    #[cfg(feature = "client")]
    pub(crate) fn set_body_plan_(&mut self, plan: BodyPlan) {
        self.body_plan_ = Option::Some(plan);
    }

    /// 覆盖体的进度。
    pub(crate) fn set_body_state_(&mut self, state: BodyState) {
        self.body_ = state;
    }

    /// 发送半边是否还在。
    #[cfg(feature = "server")]
    pub(crate) fn has_tx_(&self) -> bool {
        self.tx_.is_some()
    }

    /// 接收半边是否还在。
    #[cfg(feature = "server")]
    pub(crate) fn has_rx_(&self) -> bool {
        self.rx_.is_some()
    }

    /// 借出发送半边。
    pub(crate) fn tx_mut_(&mut self) -> Result<&mut Tx, StateError> {
        self.tx_.as_mut().ok_or(StateError::ChannelTaken)
    }

    /// 借出接收半边。
    pub(crate) fn rx_mut_(&mut self) -> Result<&mut Rx, StateError> {
        self.rx_.as_mut().ok_or(StateError::ChannelTaken)
    }

    /// 同时借出接收半边与体进度槽（两个不相交字段）。
    pub(crate) fn rx_and_body_mut_(&mut self) -> Result<(&mut Rx, &mut BodyState), StateError> {
        let SessionCore { rx_, body_, .. } = self;
        let rx = rx_.as_mut().ok_or(StateError::ChannelTaken)?;
        Result::Ok((rx, body_))
    }

    /// 取走发送半边；之后 `has_tx_` 为 `false`。
    pub(crate) fn take_tx_(&mut self) -> Option<Tx> {
        self.tx_.take()
    }

    /// 取走接收半边；之后 `has_rx_` 为 `false`。
    #[cfg(feature = "server")]
    pub(crate) fn take_rx_(&mut self) -> Option<Rx> {
        self.rx_.take()
    }

    /// 同时取走两个半边。
    #[cfg(feature = "server")]
    pub(crate) fn take_channel_(&mut self) -> Option<(Tx, Rx)> {
        let tx = self.tx_.take()?;
        let rx = self.rx_.take()?;
        Option::Some((tx, rx))
    }
}

/// 按当前进度丢弃尚未读完的体，返回丢弃的字节数。
///
/// `plan` 是前缀已经定下的边界；`state` 是视图写回的进度。两者都只描述「还剩多少」，
/// 与传输类型无关。
#[allow(clippy::needless_lifetimes)]
pub(crate) async fn drain_body_async_<'f, TyRx, TyTok>(
    plan: BodyPlan,
    state: &'f mut BodyState,
    rx: &'f mut TyRx,
    cancel: TyTok,
) -> Result<usize, crate::error_::ProtocolError>
where
    TyRx: TrBuffRead<u8> + TrConsumerState,
    TyTok: TrCancellationToken,
{
    if cancel.is_cancelled() {
        return Result::Err(crate::error_::ProtocolError::DrainBody {
            detail: alloc::string::String::from("调用方取消"),
        });
    }

    match (plan, *state) {
        (BodyPlan::Absent, _) => {
            *state = BodyState::Fresh;
            Result::Ok(0usize)
        }
        (BodyPlan::Sized { .. }, BodyState::Sized { left_: 0usize }) => Result::Ok(0usize),
        (BodyPlan::Sized { size_ }, BodyState::Fresh) => {
            let mut reader = LimitedReader::new(rx, size_);
            let dropped = drain_to_end_async_(&mut reader, cancel).await?;
            *state = BodyState::Sized { left_: 0usize };
            Result::Ok(dropped)
        }
        (BodyPlan::Sized { .. }, BodyState::Sized { left_ }) => {
            let mut reader = LimitedReader::new(rx, left_);
            let dropped = drain_to_end_async_(&mut reader, cancel).await?;
            *state = BodyState::Sized { left_: 0usize };
            Result::Ok(dropped)
        }
        (BodyPlan::Chunked, BodyState::Chunked { ended_: true }) => Result::Ok(0usize),
        (BodyPlan::Chunked, BodyState::Sized { .. }) => {
            let mut reader = ChunkedReader::new(rx);
            let dropped = drain_to_end_async_(&mut reader, cancel).await?;
            *state = BodyState::Chunked { ended_: true };
            Result::Ok(dropped)
        }
        (BodyPlan::Chunked, _) => {
            let mut reader = ChunkedReader::new(rx);
            let dropped = drain_to_end_async_(&mut reader, cancel).await?;
            *state = BodyState::Chunked { ended_: true };
            Result::Ok(dropped)
        }
        // 计划与进度描述不一致只会出现在实现缺陷上；按计划把剩余部分读尽，避免把
        // 字节留在流里。
        (BodyPlan::Sized { size_ }, BodyState::Chunked { .. }) => {
            let mut reader = LimitedReader::new(rx, size_);
            let dropped = drain_to_end_async_(&mut reader, cancel).await?;
            *state = BodyState::Sized { left_: 0usize };
            Result::Ok(dropped)
        }
    }
}
