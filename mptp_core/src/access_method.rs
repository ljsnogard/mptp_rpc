use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(u8)]
pub enum AccessMethod {
    /// 查看资源的元信息，对任意资源这个方法总是可用的
    Head = 0,

    /// 查看资源本体内容，一般没有请求体，资源内容放在回复体内
    View = 0b_0000_0001,

    /// 上传资源，内容放在请求体内
    Post = 0b_0000_0010,

    /// 删除资源，一般没有请求体
    Drop = 0b_0000_0100,

    /// 向资源端推送流式数据，可以有请求体，客户端必然有附加流
    Push = 0b_0001_0000,

    /// 从资源端拉取数据，可以有请求体和回复体，如成功服务端必然有附加流
    Pull = 0b_0010_0000,

    /// 调用功能，参数内容由请求体来提供，调用结果可能有附加流
    Call = 0b_0100_0000,
}

mod private_sealed_ {
    use super::AccessMethod;

    pub trait TrSealedAccessMethod {
        fn method() -> AccessMethod;
    }
}

/// The method to access the resource on the remote end (server).
pub trait TrAccessMethod: private_sealed_::TrSealedAccessMethod {}

/// 返回编译期方法标记 `M` 对应的 `AccessMethod`。
///
/// 这个函数与 `TrSealedAccessMethod::method` 等价，但可以被 crate 外部安全调用。
pub fn method_of<M: TrAccessMethod>() -> AccessMethod {
    M::method()
}

pub enum Head {}

pub enum View {}

pub enum Post {}

pub enum Drop {}

pub enum Push {}

pub enum Pull {}

pub enum Call {}

impl private_sealed_::TrSealedAccessMethod for Head {
    fn method() -> AccessMethod {
        AccessMethod::Head
    }
}

impl private_sealed_::TrSealedAccessMethod for View {
    fn method() -> AccessMethod {
        AccessMethod::View
    }
}

impl private_sealed_::TrSealedAccessMethod for Post {
    fn method() -> AccessMethod {
        AccessMethod::Post
    }
}

impl private_sealed_::TrSealedAccessMethod for Drop {
    fn method() -> AccessMethod {
        AccessMethod::Drop
    }
}

impl private_sealed_::TrSealedAccessMethod for Push {
    fn method() -> AccessMethod {
        AccessMethod::Push
    }
}

impl private_sealed_::TrSealedAccessMethod for Pull {
    fn method() -> AccessMethod {
        AccessMethod::Pull
    }
}

impl private_sealed_::TrSealedAccessMethod for Call {
    fn method() -> AccessMethod {
        AccessMethod::Call
    }
}

impl TrAccessMethod for Head {}
impl TrAccessMethod for View {}
impl TrAccessMethod for Post {}
impl TrAccessMethod for Drop {}
impl TrAccessMethod for Push {}
impl TrAccessMethod for Pull {}
impl TrAccessMethod for Call {}
