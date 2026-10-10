//! 请求头的构造器。
//!
//! 这里是「怎么把一组头攒出来」的唯一入口：标准头由具名方法写
//! （[`HeadersBuilder::set_body_size`] 等），自定义头走 [`HeadersBuilder::set`]。
//! 攒好之后交给 [`RequestBuilder::headers`](crate::messaging::request::RequestBuilder::headers)
//! 或 [`Request::with_headers`](crate::messaging::Request::with_headers)。

use core::fmt::Display;

use crate::{
    messaging::basic::set_body_size_header_,
    specs::{HeaderKey, HeaderVal, Headers, StdHeaderKey},
};

/// 一组头部的构造器。
///
/// # Examples
///
/// ```
/// use mptp_core::{
///     client::HeadersBuilder,
///     specs::{HeaderVal, StdHeaderKey, StdHeaderVal},
/// };
///
/// let headers = HeadersBuilder::new()
///     .set_body_size(3usize)
///     .set_body_type(&HeaderVal::from(StdHeaderVal::Mime_Body_Type_MsgPack))
///     // 自定义头：键用字符串形态。
///     .set("X-Trace", HeaderVal::from_string("abc"))
///     .build();
///
/// // `Body_Size` 走数字形态，省字节。
/// let size = headers
///     .try_get_header(&StdHeaderKey::Body_Size.into())
///     .expect("Body_Size 应当已经写进去");
/// assert_eq!(size.try_as_header_val().expect("应当是数字形态").into_inner(), 3u16);
/// ```
pub struct HeadersBuilder {
    headers_: Headers,
}

impl HeadersBuilder {
    /// 从一个空头集开始。
    pub fn new() -> Self {
        HeadersBuilder {
            headers_: Headers::new(),
        }
    }

    /// 在一组已有的头之上继续修改。
    pub const fn from_headers(headers: Headers) -> Self {
        HeadersBuilder { headers_: headers }
    }

    /// 设置 `Body_Type`（报文体的 MIME 类型）。
    pub fn set_body_type(self, body_type: &HeaderVal) -> Self {
        self.set(StdHeaderKey::Body_Type, body_type.clone())
    }

    /// 设置 `Body_Size`（报文体的字节数）。
    ///
    /// 能装进 `u16` 时用数字形态，否则退化成十进制字符串——`HeaderVal` 是
    /// 「字符串或 u16」的联合体，两种形态接收方都认，但数字更省字节。
    pub fn set_body_size(mut self, body_size: usize) -> Self {
        set_body_size_header_(&mut self.headers_, body_size);
        self
    }

    /// 设置 `Data_Type_Id`（报文体期待的反序列化类型标识）。
    ///
    /// 取值按 `Display` 格式化成字符串。类型标识常常是个哈希值，用
    /// [`type_id_u64!`](crate::type_id_u64) 现算即可：
    ///
    /// ```
    /// use mptp_core::{client::HeadersBuilder, specs::StdHeaderKey, type_id_u64};
    ///
    /// struct MyBody;
    ///
    /// let headers = HeadersBuilder::new()
    ///     .set_data_type(type_id_u64!(MyBody))
    ///     .build();
    ///
    /// let id = headers
    ///     .try_get_header(&StdHeaderKey::Data_Type_Id.into())
    ///     .expect("Data_Type_Id 应当已经写进去");
    /// assert_eq!(
    ///     id.try_as_str().expect("应当是文本形态"),
    ///     type_id_u64!(MyBody).to_string()
    /// );
    /// ```
    pub fn set_data_type(self, type_id: impl Display) -> Self {
        self.set(
            StdHeaderKey::Data_Type_Id,
            HeaderVal::from_string(type_id.to_string()),
        )
    }

    /// 设置一个任意头：标准头用数值键，自定义头用字符串键。
    ///
    /// 同名键**替换**旧值。
    pub fn set(mut self, key: impl Into<HeaderKey>, val: impl Into<HeaderVal>) -> Self {
        let key = key.into();
        let val = val.into();
        self.headers_.add_or_set_header(&key, &val);
        self
    }

    /// 收尾，产出一组头。
    ///
    /// 空头集也是合法的（协议没有必需头），因此这里没有失败路径。
    pub fn build(self) -> Headers {
        self.headers_
    }
}

impl Default for HeadersBuilder {
    fn default() -> Self {
        Self::new()
    }
}
