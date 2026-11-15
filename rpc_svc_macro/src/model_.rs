//! `#[rpc]` 分析后的中间模型。
//!
//! 分析层只做「能不能稳定生成」的判定，并把生成所需的信息整理成与 `syn` 解耦程度较高
//! 的结构；展开层只读这个模型，不再回头猜 impl 块的形状。
//!
//! 三种形态各有模型：controller（`#[rpc(interface = "...")] impl`）、Call service trait
//! （`#[rpc(service = "...", api = X)] trait`）与 Call service 服务端实现
//! （`#[rpc(impl_of(X))] impl`）。trait 侧与 impl 侧共享 [`CallMethodModel_`]：同一套
//! 参数 / 响应 / 载荷形状判定，保证枚举变体、方法表项与分派一一对应。
//!
//! # `model_`: the analyzed macro input
//!
//! The analysis layer decides what can be generated and records every fact the expansion
//! layer needs. Expansion never re-parses the user's item.

use zyn::syn;

/// 去掉标识符可能带的 `r#` 前缀。
pub(crate) fn raw_ident_name_(ident: &syn::Ident) -> String {
    let name = ident.to_string();
    name.strip_prefix("r#")
        .map(ToString::to_string)
        .unwrap_or(name)
}

/// 方法名 → 请求枚举变体名（`list_my_rooms` → `ListMyRooms`）。
pub(crate) fn variant_ident_(method: &syn::Ident) -> syn::Ident {
    let name = raw_ident_name_(method);
    let mut out = String::new();
    for part in name.split('_') {
        if part.is_empty() {
            continue;
        }
        let mut chars = part.chars();
        if let Some(first) = chars.next() {
            out.extend(first.to_uppercase());
            out.push_str(chars.as_str());
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    syn::Ident::new(&out, method.span())
}

/// 方法名 → 隐藏请求载荷关联类型名（`create_room` → `__Req_create_room`）。
pub(crate) fn req_assoc_ident_(method: &syn::Ident) -> syn::Ident {
    syn::Ident::new(&format!("__Req_{}", raw_ident_name_(method)), method.span())
}

/// trait 名 → 请求枚举名（`RoomService` → `RoomServiceCall`）。
pub(crate) fn call_enum_ident_(trait_ident: &syn::Ident) -> syn::Ident {
    syn::Ident::new(
        &format!("{}Call", raw_ident_name_(trait_ident)),
        trait_ident.span(),
    )
}

/// 标识符 → 大写蛇形（`RoomSvc` → `ROOM_SVC`），用于生成 const 名。
pub(crate) fn upper_snake_(ident: &syn::Ident) -> String {
    let name = raw_ident_name_(ident);
    let mut out = String::new();
    let mut prev_lower = false;
    for ch in name.chars() {
        if ch == '_' {
            out.push('_');
            prev_lower = false;
            continue;
        }
        if ch.is_uppercase() && prev_lower {
            out.push('_');
        }
        out.extend(ch.to_uppercase());
        prev_lower = ch.is_lowercase() || ch.is_ascii_digit();
    }
    out
}

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

    /// 用户显式声明的尾参取消令牌（`K: TrCancellationToken`）；`None` 表示由宏注入 `__C`。
    pub(crate) token_param_: Option<TokenParam_>,
}

/// 一个 Call service trait 的完整模型。
///
/// `#[rpc(service = "...", api = X)]` 的展开需要：marker 名与前缀、每个方法的业务参数
/// （以便生成请求枚举与 `__Req_*`）、响应类型（方法级 `RpcDataTypeInfo`），以及方法上
/// 的 `cfg`（枚举变体 / 客户端方法 / 方法表项必须同源）。trait 本身在展开层原样保留，
/// 只追加隐藏关联类型。
#[allow(dead_code)]
pub(crate) struct ServiceModel_ {
    /// 原始 trait item（`#[rpc(...)]` 属性已被 zyn 剥离；暂存形态，展开层再加关联类型）。
    pub(crate) trait_item_: syn::ItemTrait,

    /// trait 名（`RoomService`）。
    pub(crate) trait_ident_: syn::Ident,

    /// 完整服务前缀；不含首尾 `/`。
    pub(crate) prefix_: String,

    /// 生成的零大小 marker（`RoomSvc`）。
    pub(crate) api_: syn::Ident,

    /// 生成的请求枚举名（`RoomServiceCall`）。
    pub(crate) call_enum_ident_: syn::Ident,

    /// 全部 Call 方法。
    pub(crate) methods_: Vec<CallMethodModel_>,
}

/// `#[rpc(impl_of(X))]` trait impl 的完整模型。
#[allow(dead_code)]
pub(crate) struct ImplOfModel_ {
    /// 原始 trait impl 块（业务方法原样保留）。
    pub(crate) impl_item_: syn::ItemImpl,

    /// 被实现的类型（`RoomServiceImpl`）。
    pub(crate) self_ty_: syn::Type,

    /// 被实现的类型名（用于生成模块名）。
    pub(crate) self_ident_: syn::Ident,

    /// trait 路径（`RoomService`）；展开层用它取隐藏关联类型。
    pub(crate) trait_path_: syn::Path,

    /// marker 路径 / 标识符（`RoomSvc`）。
    pub(crate) api_path_: syn::Path,

    /// marker 标识符（用于生成模块名）。
    pub(crate) api_ident_: syn::Ident,

    /// 全部 Call 方法。
    pub(crate) methods_: Vec<CallMethodModel_>,
}

/// 一个 Call 方法（trait 侧与 impl 侧共享同一套形状判定）。
#[allow(dead_code)]
pub(crate) struct CallMethodModel_ {
    /// 方法名。
    pub(crate) ident_: syn::Ident,

    /// 方法签名（客户端代理 / 分派都需要原样复用）。
    pub(crate) sig_: syn::Signature,

    /// 方法上的文档与其它非 `rpc` 属性（trait 原样保留；impl 不重复生成）。
    pub(crate) attrs_: Vec<syn::Attribute>,

    /// 只保留 `#[cfg]` / `#[cfg_attr]`：生成到枚举变体、`__Req_*`、方法表项、客户端方法、
    /// dispatcher 分支上的条件编译属性。
    pub(crate) cfg_attrs_: Vec<syn::Attribute>,

    /// 业务参数（不含 `&self`）。
    pub(crate) params_: Vec<CallParam_>,

    /// 方法成功返回类型（`RpcResult<T>` 的 `T`）。
    pub(crate) response_ty_: syn::Type,

    /// 请求载荷形状。
    pub(crate) payload_: PayloadShape_,

    /// 隐藏关联类型名（`__Req_<operation>`）。
    pub(crate) req_assoc_ident_: syn::Ident,

    /// 请求枚举变体名（UpperCamel）。
    pub(crate) variant_ident_: syn::Ident,
}

/// 一个 Call 方法的业务参数。
#[allow(dead_code)]
pub(crate) struct CallParam_ {
    /// 参数名。
    pub(crate) ident_: syn::Ident,

    /// 线上载荷里的 owned 类型（借用参数已按其白名单转换成 owned）。
    pub(crate) owned_ty_: syn::Type,

    /// 借用参数在客户端如何转成 owned；owned 参数为 [`BorrowMode_::Owned`]。
    pub(crate) borrow_: BorrowMode_,
}

/// 借用参数的 owned 转换方式。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BorrowMode_ {
    /// 本来就是 owned 类型，原样放进载荷。
    Owned,

    /// `&str` → `String`（`.to_string()`）。
    Str,

    /// `&[u8]` → `Vec<u8>`（`.to_vec()`）。
    Bytes,

    /// `&String` / `&Vec<u8>` → 克隆（`.clone()`）。
    Clone,

    /// `&T`（`T: Copy` 标量）→ 解引用（`*arg`）。
    Copy,
}

impl BorrowMode_ {
    /// 是否为借用参数（分派时要把 owned 载荷再借回去）。
    pub(crate) const fn is_borrowed(self) -> bool {
        !matches!(self, BorrowMode_::Owned)
    }
}

/// Call 方法的请求载荷形状。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PayloadShape_ {
    /// 0 参：`()` + unit 变体。
    Unit,

    /// 1 参：该参数的 owned 类型 + newtype 变体。
    Single,

    /// ≥2 参：`__Req_<operation>` 位置字段结构体 + newtype 变体。
    Struct,
}

/// 端点方法显式声明的尾参取消令牌。
#[allow(dead_code)]
pub(crate) struct TokenParam_ {
    /// 参数名（例如 `cancel`）。
    pub(crate) param_ident_: syn::Ident,

    /// 泛型类型参数名（例如 `K`）。
    pub(crate) ty_ident_: syn::Ident,
}

/// 一个自由函数 handler（`#[rpc(method = "full/path")] async fn ...`）的模型。
///
/// 与 controller 不同：没有 `self` 接收者、没有 `interface` 前缀（路径取 `method` 声明里的
/// 完整值）、也没有客户端代理。展开层据此生成一个无状态的单元 handler。
#[allow(dead_code)]
pub(crate) struct FreeFnModel_ {
    /// 原始函数 item（`#[rpc(...)]` 端点声明属性已剥离）。
    pub(crate) fn_item_: syn::ItemFn,

    /// 端点方法模型（无 `self`）。
    pub(crate) method_: EndpointMethodModel_,
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
