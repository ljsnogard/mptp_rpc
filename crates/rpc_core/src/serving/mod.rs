//! 服务端处理框架。
//!
//! 这个模块提供：
//!
//! - [`handler::TrReqHandler`]：单个 handler 的抽象；
//! - [`handler::HandlerChain`]：把多个 handler 串成链，让同一个请求有机会
//!   按顺序被感兴趣的 handler 处理；
//! - [`server::Server`]：基础服务器组件，负责解码请求、路由、调用链、写回回复；
//! - [`channel::ServiceChannel`] / [`channel::ClientChannel`]：用于在代码内
//!   模拟客户端和服务端收发请求的内存 channel。
//!
//! # 与 Salvo 的对应关系
//!
//! - `TrReqHandler` 类似于 Salvo 的 `Handler`；
//! - `HandlerChain` 类似于 Salvo 的 handler 链 / 中间件链；
//! - `Server` 类似于 Salvo 的 `Service`，负责把请求交给匹配的链处理。
//!
//! 当前实现先面向“代码内直接模拟收发”的测试场景，后续可以再接入真实传输层。

pub mod channel;
pub mod handler;
pub mod server;

mod cancel_tok_;

#[cfg(test)]
mod tests_;
