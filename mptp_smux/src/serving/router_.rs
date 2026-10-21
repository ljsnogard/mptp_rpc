//! 路径前缀路由：把请求的 `location` 映射到一个 handler。
//!
//! 这是「单 handler 分派」的载体——路由命中之后框架直接调用**那一个** handler，不提供链式
//! 中间件。理由：`abs_mptp` 的 handler 契约已经把「写回复」的权力交给了 handler 自己
//! （它拿到收发两边），链式语义（谁来写最终回复、谁来收尾）在没有回复类型的前提下没有唯一
//! 答案，硬造一条链只会把这件事变成框架的猜谜。
//!
//! 匹配规则是**最长前缀**：登记 `/topic` 与 `/topic/chat` 时，`/topic/chat/42` 命中后者。
//! 平局（同长度）不可能出现——前缀相同就是同一个键。

pub trait TrPathRouter {
    type Handler;

    fn match_path<'f>(
        &'f self,
        path: &'f str,
    ) -> Option<&'f Self::Handler>;
}

pub trait TrPathRouterMut
where
    Self: TrPathRouter,
{
    type Err: core::error::Error;

    fn add_rule<'f, F>(
        &'f mut self,
        rule: &'f str,
        make_handler: F,
    ) -> Result<(), Self::Err>
    where
        Self: Sized,
        F: FnOnce(&'f str) -> Self::Handler;
}
