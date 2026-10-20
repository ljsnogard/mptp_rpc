//! 报文前缀（`cs_`）：请求 / 回复共有的头与体声明，以及两侧各自的补充。
//!
//! 「前缀」指一条报文里**体之前**的那部分：方法 / 位置（请求）或状态（回复），加上头表。
//! 本模块只给出形状，不规定编码：具体类型由使用者的 `client` / `serving` 配置提供。
//!
//! # Message prefixes (`cs_`)
//!
//! The "prefix" is everything before the body: method/location (requests) or status
//! (responses), plus the header map. This module only fixes the shape; the concrete types
//! come from the user's `client` / `serving` configuration.

use core::{borrow::Borrow, fmt::Debug};

use crate::spec::{
    self, NumOrStr, StdHeaderKey, StdHeaderVal, TrHeaders, TrNumOrStr, access_method::AccessMethod,
};
use crate::x_deps::funty::Fundamental as _;

/// 报文体按哪种方式传输。
///
/// 判别必须能从**报文头唯一确定**：接收方拿到前缀之后要先知道对方打算怎么发，才能决定
/// 怎么接（见 `messaging::limited` 与 `messaging::chunked`）。
///
/// How a message body is transferred. The decision must be uniquely derivable from the
/// message headers, so the receiver knows how to read the body before touching it.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BodySize<'a> {
    /// 定长：体的总长已经写在头里（`Body_Size`），恰好这么多字节。
    ///
    /// 默认的 [`TrMsgPrefix::body_info`] 会把文本长度解析后以
    /// [`NumOrStr::Num`] 返回；能直接借用头值原文的实现可以返回
    /// [`NumOrStr::Str`]，调用者两种形态都应接受。
    ///
    /// Sized: the total length is already declared in the `Body_Size` header. The default
    /// [`TrMsgPrefix::body_info`] parses textual sizes into [`NumOrStr::Num`];
    /// implementations able to borrow the original text may return [`NumOrStr::Str`], so
    /// callers should accept both forms.
    FixedLength(NumOrStr<usize, &'a str>),

    /// 分块：总长不写在头里，由「2 字节长度前缀 + 内容」的块序列给出。
    ///
    /// Chunked: the total length is not declared; it is given by a sequence of
    /// `2-byte length + payload` chunks.
    ChunkedData,

    /// 没有体：一个字节都不收发。
    ///
    /// No body at all: not a single byte is sent or received.
    NilBody,
}

/// 请求与回复共有的前缀。
///
/// The part shared by request and response prefixes.
pub trait TrMsgPrefix {
    /// 本前缀携带的头表类型。
    ///
    /// The header-map type carried by this prefix.
    type Headers: spec::TrHeaders;

    /// 取头表；没有头时返回 `None`。
    ///
    /// The header map, or `None` when the message carries no headers.
    fn headers(&self) -> Option<&Self::Headers>;

    /// 本条报文的体按哪种方式传输。
    ///
    /// 默认实现从标准头 `Body_Size` / `Body_Transfer` 推导：
    ///
    /// - `Body_Size` 在场且可解析为长度：`0` 视为 [`BodySize::NilBody`]，否则为
    ///   [`BodySize::FixedLength`]；
    /// - 否则 `Body_Transfer` 取 `Chunked`（数字 `0x20` 或文本 `"chunked"`，忽略
    ///   大小写）时为 [`BodySize::ChunkedData`]；
    /// - 其余情况为 [`BodySize::NilBody`]。
    ///
    /// 通用实现无法借用头值内部的文本，因此文本长度会被解析后以
    /// [`NumOrStr::Num`] 返回；需要保留原始文本的实现可以覆盖本方法。
    /// 两个头同时在场属于协议违规，本方法不返回错误，此时优先采用 `Body_Size`。
    ///
    /// How this message's body is transferred. The default implementation derives it from
    /// the standard `Body_Size` / `Body_Transfer` headers. Textual sizes are parsed and
    /// returned as [`NumOrStr::Num`]; implementations that can borrow the original text may
    /// override this method. When both headers are present (a protocol violation), the
    /// default prefers `Body_Size`.
    fn body_info(&self) -> BodySize<'_> {
        let Option::Some(headers) = self.headers() else {
            return BodySize::NilBody;
        };

        if let Option::Some(value) = headers.try_get_std_header(StdHeaderKey::Body_Size) {
            let value = value.clone();
            let size = match TrNumOrStr::into(value) {
                NumOrStr::Num(num) => num.as_usize(),
                NumOrStr::Str(text) => {
                    let Result::Ok(size) = text.borrow().trim().parse::<usize>() else {
                        return BodySize::NilBody;
                    };
                    size
                }
            };
            return if size == 0 {
                BodySize::NilBody
            } else {
                BodySize::FixedLength(NumOrStr::Num(size))
            };
        }

        if let Option::Some(value) = headers.try_get_std_header(StdHeaderKey::Body_Transfer) {
            let value = value.clone();
            let chunked = match TrNumOrStr::into(value) {
                NumOrStr::Num(num) => {
                    let code: StdHeaderVal = num.into();
                    code == StdHeaderVal::Body_Transfer_Chunked
                }
                NumOrStr::Str(text) => text.borrow().trim().eq_ignore_ascii_case("chunked"),
            };
            if chunked {
                return BodySize::ChunkedData;
            }
        }

        BodySize::NilBody
    }
}

/// 请求前缀：在共有部分之上补出「访问哪个资源、用什么方法」。
///
/// A request prefix: the shared part plus "which resource, by which method".
pub trait TrRequestPrefix
where
    Self: TrMsgPrefix + Sized,
{
    /// 本请求使用的访问方法。
    ///
    /// The access method used by this request.
    fn method(&self) -> AccessMethod;

    /// 访问的资源位置（路径）。
    ///
    /// The location (path) of the accessed resource.
    fn location(&self) -> &str;
}

/// 回复前缀：在共有部分之上补出状态码。
///
/// A response prefix: the shared part plus the status code.
pub trait TrResponsePrefix
where
    Self: TrMsgPrefix + Sized,
{
    /// 本回复的状态码。
    ///
    /// The status code of this response.
    fn status(&self) -> spec::Status;
}
