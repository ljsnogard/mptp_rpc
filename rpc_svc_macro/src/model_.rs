//! `#[rpc(interface = "...")]` controller 分析后的中间模型。
//!
//! 分析层只做「能不能稳定生成」的判定，并把生成所需的信息整理成与 `syn` 解耦程度较高
//! 的结构；展开层只读这个模型，不再回头猜 impl 块的形状。
//!
//! # `model_`: the analyzed controller model
//!
//! The analysis layer decides what can be generated and records every fact the expansion
//! layer needs. Expansion never re-parses the impl block.

use zyn::syn;

/// 一个 controller（`#[rpc(interface = "...")] impl` 块）的完整模型。
///
/// 每个方法是一个独立端点，方法上的 `#[rpc(call/view/head/... = "path")]`
/// 声明访问方法 + 路径。展开层据此为每个方法生成一个 `TrEndpointHandler` 并登记进
/// [`mptp_serving::RadixRouter`]。
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
