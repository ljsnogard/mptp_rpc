//! 服务端处理框架：路径路由 + 请求解码 + Handler 分发。
//!
//! # 设计目标
//!
//! 这个模块提供一个“足够简单但可扩展”的 MPTP 服务端基础：
//!
//! - [`TrReqHandler`] 是路径级 handler：它针对某个具体路径上的资源，
//!   处理来自客户端的**任意** `AccessMethod` 请求；
//! - [`TrAccessHandler<M>`] 是方法级细化 handler：它表示“路径 + 特定访问方法”
//!   都匹配时才处理的 handler；
//! - [`Router`] 负责保存路径级和方法级 handler，并在请求到达时选择最具体的那个；
//! - [`Server`] 是基础服务组件：它从 `TrChannel` 的读半通道解码 `Request`，
//!   然后调用匹配的 handler。
//!
//! # 流式设计
//!
//! 序列化和反序列化都在流上进行。`ReqCtx` 向 handler 暴露：
//!
//! - `reader`：请求头之后的请求体 / suffix stream，类型为 `&mut dyn std::io::Read`；
//! - `writer`：回复输出流，类型为 `&mut dyn std::io::Write`。
//!
//! 这样 handler 可以直接使用 [`crate::codec::BodyCodec`] 或 `rmp-serde` /
//! `serde_json` 的流式 API，而不需要先把整个 body 读进内存。
//!
//! # 与 Salvo 的对应关系
//!
//! - `TrReqHandler` 类似于 Salvo 中处理某个路由的 `Handler`；
//! - `TrAccessHandler<M>` 类似于按 HTTP Method 细分的 handler；
//! - `Router` 类似于 Salvo 的 `Router`，但当前只做最基础的精确路径匹配。

pub mod channel;
pub mod handler;
pub mod server;

#[cfg(test)]
mod tests_;
