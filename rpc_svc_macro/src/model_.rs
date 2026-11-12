//! `#[rpc]` 分析后的中间模型。
//!
//! 分析层只做「能不能稳定生成」的判定，并把生成所需的信息整理成与 `syn` 解耦程度较高
//! 的结构；展开层只读这个模型，不再回头猜 trait / struct 的形状。
//!
//! # `model_`: the analyzed service and interface models
//!
//! The analysis layer decides what can be generated and records every fact the expansion
//! layer needs. Expansion never re-parses the trait or the marker struct.

use zyn::syn;

/// 一个 service trait 的完整模型。
pub(crate) struct ServiceModel_ {
    /// service trait 的名字。
    pub(crate) trait_ident_: syn::Ident,

    /// 已经注入 `SERVICE_PATH` 的 trait 定义。
    pub(crate) trait_item_: syn::ItemTrait,

    /// 生成代码所在的隐藏模块名。
    pub(crate) module_ident_: syn::Ident,

    /// 全部 RPC 方法。
    pub(crate) methods_: Vec<MethodModel_>,

    /// trait 的 supertrait 是否要求 `Send` / `Sync`。
    ///
    /// 是则客户端 impl 需要额外证明 `ClientProxy: Send + Sync`，即底层共享连接 `Sync`。
    pub(crate) requires_send_sync_: bool,
}

/// 一个 interface marker 的完整模型。
pub(crate) struct InterfaceModel_ {
    /// interface marker 类型名。
    pub(crate) struct_ident_: syn::Ident,

    /// interface 的原始结构体定义（attribute 宏需要原样保留）。
    pub(crate) struct_item_: syn::ItemStruct,

    /// interface 路径前缀；不含首尾 `/`。
    pub(crate) path_: String,
}

/// 一个 `#[rpc(service_of(ApiV1))] impl` 块的模型。
///
/// `service_of` 不生成 router 或 worker；它只为用户的 impl 类型补一个
/// `TrRpcService` 实现，让 `RpcRouter::add_service`
/// 能在装配期识别 service 路径并调用 dispatch。
pub(crate) struct ServiceOfModel_ {
    /// 原始 impl 块；展开时原样保留。
    pub(crate) impl_item_: syn::ItemImpl,

    /// impl 的 trait 路径，例如 `GoodsCustomerService`。
    pub(crate) trait_path_: syn::Path,

    /// impl 的自身类型，例如 `GoodsCustomerServiceImpl`。
    pub(crate) self_ty_: syn::Type,

    /// `service_of(...)` 里写的 interface marker 路径，例如 `ApiV1`。
    pub(crate) interface_path_: syn::Path,
}

/// 一个 controller（`#[rpc(interface = "...")] impl` 块）的完整模型。
///
/// 新形态：每个方法是一个独立端点，方法上的 `#[rpc(call/view/head/... = "path")]`
/// 声明访问方法 + 路径。展开层据此为每个方法生成一个 `TrEndpointHandler` 并登记进
/// [`mptp_serving::RadixRouter`]。
///
/// 字段目前只有分析层与测试在读；P1 后续 codegen 会用 `self_ty_` / `path_` / `methods_`。
#[allow(dead_code)]
pub(crate) struct ControllerModel_ {
    /// controller 的自身类型，例如 `SceneService`。
    pub(crate) self_ty_: syn::Type,

    /// 原始 impl 块；方法上的 `#[rpc(...)]` 端点声明属性已被剥离。
    pub(crate) impl_item_: syn::ItemImpl,

    /// interface 路径前缀；不含首尾 `/`。
    pub(crate) path_: String,

    /// 全部端点方法。
    pub(crate) methods_: Vec<EndpointMethodModel_>,
}

/// 一个端点方法的模型。
#[allow(dead_code)]
pub(crate) struct EndpointMethodModel_ {
    /// 方法名。
    pub(crate) ident_: syn::Ident,

    /// 方法签名（`#[rpc(...)]` 端点声明属性已剥离）。
    pub(crate) sig_: syn::Signature,

    /// 方法体（`async fn` 的正文；codegen 会把它重写成 gen_mcf2 step 函数）。
    pub(crate) block_: syn::Block,

    /// 方法上的其余属性（文档、`#[cfg]` 等）。
    pub(crate) attrs_: Vec<syn::Attribute>,

    /// 访问方法 + 相对路径。
    pub(crate) endpoint_: EndpointSpec_,
}

/// 一个端点的访问方法 + 相对路径。
#[allow(dead_code)]
pub(crate) struct EndpointSpec_ {
    /// 访问方法种类。
    pub(crate) method_: MethodKind_,

    /// 相对路径；不含首尾 `/`。
    pub(crate) path_: String,
}

/// 端点方法对应的访问方法种类。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MethodKind_ {
    /// `Call`：功能调用。
    Call,
    /// `View`：读资源。
    View,
    /// `Head`：读元信息。
    Head,
    /// `Post`：上传资源。
    Post,
    /// `Drop`：删除资源。
    Drop,
    /// `Push`：上传流。
    Push,
    /// `Pull`：拉取流。
    Pull,
}

/// 一个 RPC 方法的模型。
pub(crate) struct MethodModel_ {
    /// 方法名，同时也是 body enum 的变体名。
    pub(crate) ident_: syn::Ident,

    /// 方法签名（保留生命周期、where 子句与文档属性）。
    pub(crate) sig_: syn::Signature,

    /// 原方法上的属性（文档、`#[cfg]` 等）。
    pub(crate) attrs_: Vec<syn::Attribute>,

    /// 方法参数。
    pub(crate) args_: Vec<ArgModel_>,

    /// `RpcResult<T>` 里的成功类型 `T`。
    pub(crate) output_ty_: syn::Type,

    /// `impl TrMayCancel<'f, ...>` 里的 `'f`；省略时为 `None`。
    pub(crate) may_cancel_lifetime_: Option<syn::Lifetime>,

    /// 返回类型上除 `TrMayCancel` 之外的其余 bound（例如 `+ Send`）。
    pub(crate) extra_bounds_: Vec<syn::TypeParamBound>,
}

/// 一个方法参数的模型。
pub(crate) struct ArgModel_ {
    /// 参数名（也是 body enum 的字段名）。
    pub(crate) ident_: syn::Ident,

    /// body enum 里的字段类型（客户端与服务端共用 owned 版本）。
    pub(crate) owned_ty_: syn::Type,

    /// 原参数是否是顶层 `&T`；是则在 dispatch 里传 `&field`。
    pub(crate) is_ref_: bool,

    /// 客户端构造 body enum 时，如何把参数变成 owned 字段。
    pub(crate) convert_: ConvertKind_,
}

/// 客户端把借用参数转成 owned 字段的方式。
///
/// `denco_macro` 当前不支持带生命周期参数的派生类型，因此 body enum 统一使用 owned
/// 字段；这里记录每个借用参数的最小转换方式。
pub(crate) enum ConvertKind_ {
    /// 按值参数，直接 move。
    None,

    /// `&str` → `String`。
    ToString,

    /// `&[u8]` → `Vec<u8>`。
    ToVec,

    /// `&String` / `&Vec<u8>` → clone。
    Clone,

    /// `&Copy` 标量 → 解引用。
    Deref,
}
