//! demo 的线上契约：interface / service / body enum 与业务类型。
//!
//! 这里**手动**实现蓝图里的形状，不使用 `#[rpc]` 宏：
//!
//! - interface 只提供路径前缀；
//! - service 路径不包含方法名；
//! - 一个 service 的多个方法组织成一个 body enum，变体名直接用函数名，字段名直接用参数名。
//!
//! # `protocol_`: the hand-written wire contract
//!
//! This module is the manual version of the blueprint's protocol shape. No `#[rpc]` macro is
//! involved: the interface only supplies a path prefix, service paths do not contain method
//! names, and each service's methods live in a body enum whose variant and field names mirror
//! the Rust function and parameter names.

//! # 手写代码 vs 将来 `#[rpc]` 宏生成
//!
//! 这个文件刻意把两类内容放在一起，方便对照“宏将来要生成什么”：
//!
//! ## 用户手写
//!
//! - [`Workspace`] / [`Session`]：业务载体与它们的 `TrEncodable` / `TrDecodable` 派生；
//! - [`WorkspaceService`] / [`SessionService`] 的方法签名：方法名、参数名、参数类型、返回类型；
//! - interface marker（[`ApiV1`] / [`ApiV2`]）：将来写成
//!   `#[rpc(path = "...", services = [...])] struct ApiV1;`；
//! - 服务端业务实现（在 `server_.rs`）：用户实现 service trait 的具体逻辑。
//!
//! ## 将来由 `#[rpc]` 宏生成
//!
//! - service trait 里的 `const SERVICE_PATH`（由 `#[rpc(service = "...")]` 注入）；
//! - **body enum**：每个方法一个 enum 变体，变体名 = 方法名、字段名 = 参数名；
//!   客户端生成带借用字段的版本，服务端生成 owned 版本；
//! - 客户端代理对 service trait 的 `impl`：每个方法构造对应 enum 变体，调用通用
//!   `ClientCallAsync`；
//! - 服务端 dispatch：`match` body enum 变体，调用用户实现；
//! - interface 的 `RpcInterface` / `ServiceSelector` 实现，以及
//!   `router.add_interface(...)` 的注册代码；
//! - 每个方法对应的 `gen_may_cancel_future` 具名 step 函数（宏生成样板，用户不写）。
//!
//! ## 生成思路（宏的大致步骤）
//!
//! 1. 解析 service trait 的每个方法：名字、参数列表、返回类型；
//! 2. 生成 body enum（及 owned 版本），变体直接携带参数；
//! 3. 生成客户端 `impl Service for ClientProxy<...>`，方法体是
//!    `ClientCallAsync::new(self, SERVICE_PATH, Enum::variant { ... })`；
//! 4. 生成服务端 `match` 与 worker 注册：`(SERVICE_PATH, Sender)` 交给 interface 的
//!    `ServiceTrie`；
//! 5. 解析 interface 的 `path` / `services` 列表，生成 `RpcInterface`、`ServiceSelector`
//!    与 `add_interface` 调用；
//! 6. 为所有 async 方法套 `gen_may_cancel_future`，把取消令牌继续向下传。
//!
use abs_cancel::TrMayCancel;
use abs_mptp::spec::Status;
use thiserror::Error;

/// 一个 service interface 的路径前缀。
///
/// 用户写 interface marker；`#[rpc]` 宏将来生成它的 `RpcInterface` / `ServiceSelector` 实现。
pub trait RpcInterface {
    /// 不含首尾 `/` 的 interface 前缀。
    const PATH: &'static str;
}

/// 第一版 interface：`/api/v1/{service}`。
pub struct ApiV1;

impl RpcInterface for ApiV1 {
    const PATH: &'static str = "api/v1";
}

/// 第二版 interface：`/api/v2/{service}`，用来验证一个服务器同时注册多个 interface。
pub struct ApiV2;

impl RpcInterface for ApiV2 {
    const PATH: &'static str = "api/v2";
}

/// 工作区 service 的相对路径。
pub const K_WORKSPACE_SERVICE: &str = "workspace";

/// 会话 service 的相对路径。
pub const K_SESSION_SERVICE: &str = "session";

/// 客户端这一轮发出的 RPC 条数；服务端据此决定何时收尾。
pub const K_EXPECTED_JOBS: usize = 11usize;

/// 工作区。
///
/// 用户手写：业务载体。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct Workspace {
    /// 工作区 ID。
    pub id: u64,

    /// 工作区名称。
    pub name: String,
}

/// 会话。
///
/// 用户手写：业务载体。
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub struct Session {
    /// 会话 ID。
    pub id: u64,

    /// 所属工作区 ID。
    pub workspace_id: u64,

    /// 会话标题。
    pub title: String,
}

/// 工作区 service 的 body enum。
///
/// 将来由 `#[rpc]` 宏生成；这里是手写等价物。
///
/// 变体名与函数名一致，字段名与参数名一致；这是手动版本，所以不追求 PascalCase。
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub enum WorkspaceCall {
    /// 创建。
    create {
        /// 名称。
        name: String,
    },

    /// 列出全部。
    list,

    /// 按 ID 查看。
    get {
        /// 工作区 ID。
        id: u64,
    },

    /// 改名。
    rename {
        /// 工作区 ID。
        id: u64,
        /// 新名称。
        name: String,
    },

    /// 删除。
    delete {
        /// 工作区 ID。
        id: u64,
    },
}

/// 会话 service 的 body enum。
///
/// 将来由 `#[rpc]` 宏生成；这里是手写等价物。
#[allow(non_camel_case_types)]
#[derive(Clone, Debug, Eq, PartialEq, denco_macro::TrEncodable, denco_macro::TrDecodable)]
pub enum SessionCall {
    /// 在指定工作区里创建。
    create {
        /// 所属工作区。
        workspace_id: u64,
        /// 标题。
        title: String,
    },

    /// 列出某个工作区下的全部会话。
    list {
        /// 工作区 ID。
        workspace_id: u64,
    },

    /// 按 ID 查看。
    get {
        /// 会话 ID。
        id: u64,
    },

    /// 改名。
    rename {
        /// 会话 ID。
        id: u64,
        /// 新标题。
        title: String,
    },

    /// 删除。
    delete {
        /// 会话 ID。
        id: u64,
    },
}

/// 用户 service 层的错误。
///
/// 业务错误（NotFound / NotEmpty / Lock）由服务端映射成状态码；`Transport` / `Codec` /
/// `Remote` 主要出现在客户端，用于把传输/编解码/远端状态错误统一成同一种类型。
#[derive(Debug, Error)]
pub enum ServiceError {
    /// workspace 不存在。
    #[error("workspace {0} 不存在")]
    WorkspaceNotFound(u64),

    /// session 不存在。
    #[error("session {0} 不存在")]
    SessionNotFound(u64),

    /// workspace 下还有 session。
    #[error("workspace {0} 还有 session，不能删除")]
    WorkspaceNotEmpty(u64),

    /// 内部锁失败。
    #[error("store 锁失败：{0}")]
    Lock(String),

    /// 传输层错误。
    #[error("传输失败：{0}")]
    Transport(String),

    /// 编解码错误。
    #[error("编解码失败：{0}")]
    Codec(String),

    /// 远端返回了非 200 状态。
    #[error("远端错误：{0}")]
    Remote(String),
}

impl ServiceError {
    /// 映射成线上状态码。
    pub(crate) fn status_(&self) -> Status {
        match self {
            ServiceError::WorkspaceNotFound(_) | ServiceError::SessionNotFound(_) => {
                Status::NotFound
            }
            ServiceError::WorkspaceNotEmpty(_) => Status::BadRequest,
            ServiceError::Lock(_) | ServiceError::Codec(_) => Status::InternalServerError,
            ServiceError::Transport(_) | ServiceError::Remote(_) => Status::BadGateway,
        }
    }
}

/// 一次 RPC 调用的结果类型。
///
/// 框架/协议层提供；用户业务方法统一返回它。
pub type RpcResult<T> = core::result::Result<T, ServiceError>;

/// 用户编写的 workspace RPC service trait。
///
/// 用户只写方法签名；`#[rpc]` 宏将来注入 `SERVICE_PATH`，并生成客户端/服务端实现。
///
/// 这是协议端看到的第一等 API：客户端代理与服务端实现都实现它。方法名直接对应 body enum
/// 的变体名，`SERVICE_PATH` 直接对应 router 里的 service path。
pub trait WorkspaceService: Send + Sync {
    /// 本 service 在 interface 下的相对路径。
    const SERVICE_PATH: &'static str = K_WORKSPACE_SERVICE;

    /// 创建。
    fn create_workspace<'f>(
        &'f self,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>>;

    /// 列出。
    fn list_workspaces<'f>(
        &'f self,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Workspace>>>;

    /// 查看。
    fn get_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Workspace>>>;

    /// 改名。
    fn rename_workspace<'f>(
        &'f self,
        id: u64,
        name: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Workspace>>;

    /// 删除。
    fn delete_workspace<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>>;
}

/// 用户编写的 session RPC service trait。
///
/// 用户只写方法签名；`#[rpc]` 宏将来注入 `SERVICE_PATH`，并生成客户端/服务端实现。
pub trait SessionService: Send + Sync {
    /// 本 service 在 interface 下的相对路径。
    const SERVICE_PATH: &'static str = K_SESSION_SERVICE;

    /// 创建。
    fn create_session<'f>(
        &'f self,
        workspace_id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>>;

    /// 列出。
    fn list_sessions<'f>(
        &'f self,
        workspace_id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Vec<Session>>>;

    /// 查看。
    fn get_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Option<Session>>>;

    /// 改名。
    fn rename_session<'f>(
        &'f self,
        id: u64,
        title: String,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<Session>>;

    /// 删除。
    fn delete_session<'f>(
        &'f self,
        id: u64,
    ) -> impl TrMayCancel<'f, MayCancelOutput = RpcResult<bool>>;
}

/// 拼出 `/{interface}/{service}`。
pub fn location(interface: &str, service: &str) -> String {
    format!(
        "/{}/{}",
        interface.trim_matches('/'),
        service.trim_matches('/')
    )
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 interface + service 路径拼接会消掉多余的首尾斜杠。
    /// Test that interface + service composition normalises surrounding slashes.
    ///
    /// - 手段：分别用带斜杠与不带斜杠的两组参数调用 `location`。
    /// - 判断：两次都得到同一个 `/api/v1/workspace`。
    ///
    /// - Means: call `location` with slashed and unslashed inputs.
    /// - Verdict: both produce `/api/v1/workspace`.
    #[test]
    fn location_normalises_slashes_() {
        assert_eq!(location("api/v1", "workspace"), "/api/v1/workspace");
        assert_eq!(location("/api/v1/", "/workspace/"), "/api/v1/workspace");
    }
}
