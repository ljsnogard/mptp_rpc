//! 访问方法，以及「编译期方法标记 → 运行期方法值」的对应。
//!
//! 方法值用的是**位标志**而不是序号：`Head` 是 `0`，其余每个方法占一位。这既让「这个方法
//! 是否可以带请求体 / 是否必然带附加流」变成可以按位判断的性质，也让取值不与 HTTP 方法
//! 的编号绑定——MPTP 的方法集合由本协议自己定义。
//!
//! # `access_method`: methods and their compile-time markers
//!
//! Method values are **bit flags** rather than ordinals: `Head` is `0` and every other
//! method owns one bit. That makes body/stream expectations decidable bitwise, and keeps
//! the numbering independent of HTTP's.

/// 一次访问使用的方法。
///
/// 它是**线上契约**的一部分：两侧对同一个位的理解必须一致。
///
/// The method of one access. It is part of the **wire contract**: both ends must agree on
/// what each bit means.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum AccessMethod {
    /// 查看资源的元信息，对任意资源这个方法总是可用的
    ///
    /// Inspect a resource's metadata; always available for any resource.
    Head = 0,

    /// 查看资源本体内容，一般没有请求体，资源内容放在回复体内
    ///
    /// View a resource's content: normally no request body, the content travels in the
    /// response body.
    View = 0b_0000_0001,

    /// 上传资源，内容放在请求体内
    ///
    /// Upload a resource; the content travels in the request body.
    Post = 0b_0000_0010,

    /// 删除资源，一般没有请求体
    ///
    /// Delete a resource; normally no request body.
    Drop = 0b_0000_0100,

    /// 向资源端推送流式数据，可以有请求体，客户端必然有附加流
    ///
    /// Push streaming data to the resource end: may carry a request body, and the client
    /// necessarily has an attached stream.
    Push = 0b_0001_0000,

    /// 从资源端拉取数据，可以有请求体和回复体，如成功服务端必然有附加流
    ///
    /// Pull data from the resource end: may carry both request and response bodies, and on
    /// success the server necessarily has an attached stream.
    Pull = 0b_0010_0000,

    /// 调用功能，参数内容由请求体来提供，调用结果可能有附加流
    ///
    /// Invoke a function: arguments come from the request body, and the result may carry an
    /// attached stream.
    Call = 0b_0100_0000,
}

/// 「编译期方法标记 → 运行期方法值」的内部契约。
///
/// 它对本 crate 之外不可见：使用者只用类型参数（`View` / `Post` / ……）表达方法，
/// 通过 [`method_of`] 取值，不必也不应实现本 trait。
///
/// The internal contract mapping a compile-time method marker to its runtime value. It is
/// invisible outside this crate: users express the method with a type parameter and read it
/// back through [`method_of`].
pub impl(crate) trait TrAccessMethod {
    /// 本标记代表的方法。
    ///
    /// The method this marker stands for.
    fn method() -> AccessMethod;
}

/// 返回编译期方法标记 `M` 对应的 [`AccessMethod`]。
///
/// 使用者用类型参数表达「这条报文是什么方法」（例如把 [`View`] 写进泛型实参），需要把它
/// 还原成线上取值时调用本函数。它只做一次常量折叠，没有运行期查表。
///
/// # Examples
///
/// ```
/// use abs_mptp::spec::access_method::{AccessMethod, method_of, View};
///
/// assert_eq!(method_of::<View>(), AccessMethod::View);
/// ```
///
/// Returns the [`AccessMethod`] for the compile-time marker `M`. It is a constant fold,
/// not a runtime table lookup.
pub fn method_of<M: TrAccessMethod>() -> AccessMethod {
    M::method()
}

/// 方法标记：[`AccessMethod::Head`]。
///
/// The marker for [`AccessMethod::Head`].
pub enum Head {}

/// 方法标记：[`AccessMethod::View`]。
///
/// The marker for [`AccessMethod::View`].
pub enum View {}

/// 方法标记：[`AccessMethod::Post`]。
///
/// The marker for [`AccessMethod::Post`].
pub enum Post {}

/// 方法标记：[`AccessMethod::Drop`]。
///
/// The marker for [`AccessMethod::Drop`].
pub enum Drop {}

/// 方法标记：[`AccessMethod::Push`]。
///
/// The marker for [`AccessMethod::Push`].
pub enum Push {}

/// 方法标记：[`AccessMethod::Pull`]。
///
/// The marker for [`AccessMethod::Pull`].
pub enum Pull {}

/// 方法标记：[`AccessMethod::Call`]。
///
/// The marker for [`AccessMethod::Call`].
pub enum Call {}

impl TrAccessMethod for Head {
    fn method() -> AccessMethod {
        AccessMethod::Head
    }
}

impl TrAccessMethod for View {
    fn method() -> AccessMethod {
        AccessMethod::View
    }
}

impl TrAccessMethod for Post {
    fn method() -> AccessMethod {
        AccessMethod::Post
    }
}

impl TrAccessMethod for Drop {
    fn method() -> AccessMethod {
        AccessMethod::Drop
    }
}

impl TrAccessMethod for Push {
    fn method() -> AccessMethod {
        AccessMethod::Push
    }
}

impl TrAccessMethod for Pull {
    fn method() -> AccessMethod {
        AccessMethod::Pull
    }
}

impl TrAccessMethod for Call {
    fn method() -> AccessMethod {
        AccessMethod::Call
    }
}
