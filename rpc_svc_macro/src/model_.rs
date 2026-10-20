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

    /// `services = [...]` 里的 trait 路径，以及它们对应的隐藏模块路径。
    ///
    /// 隐藏模块由 service 宏生成在 trait 所在模块里，因此模块路径 = trait 路径的最后一段
    /// 替换成 `__rpc_service_{TraitName}`。
    pub(crate) services_: Vec<ServiceRef_>,

    /// interface worker 组的类型名；生成在 interface marker 所在的同一模块里。
    ///
    /// 不用嵌套模块，是为了让 `services = [...]` 里的 trait 路径保持用户写下的相对关系。
    pub(crate) workers_ident_: syn::Ident,
}

/// interface 列表里的一个 service 引用。
pub(crate) struct ServiceRef_ {
    /// service trait 的路径，例如 `GoodsCustomerService` 或 `super::Foo`。
    pub(crate) trait_path_: syn::Path,

    /// 该 trait 对应的隐藏模块路径，例如 `__rpc_service_GoodsCustomerService`。
    pub(crate) module_path_: syn::Path,
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
