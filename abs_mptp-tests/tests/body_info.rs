//! `TrMsgPrefix::body_info` 默认实现的集成测试。
//!
//! 用例只依赖公开契约：一个最小的 `TrHeaders` 实现，加上 `TrMsgPrefix` 的默认
//! `body_info`。覆盖数字 / 文本两种长度形态、两种分块声明、两个头同时在场的冲突，
//! 以及没有头表的空情形。
//!
//! Integration tests for the default `TrMsgPrefix::body_info`. They use a minimal
//! `TrHeaders` implementation and exercise numeric / textual sizes, both chunked forms,
//! the conflicting-both-present case, and the no-header case.

use core::borrow::Borrow;
use std::collections::BTreeMap;

use abs_mptp::{
    messaging::{BodySize, TrMsgPrefix},
    spec::{NumOrStr, StdHeaderKey, StdHeaderVal, TrHeaders, TrNumOrStr},
};

/// 测试用的头值：数字或文本两种形态。
#[derive(Clone, Debug, Eq, PartialEq)]
enum MockVal {
    Num(u16),
    Text(String),
}

impl TrNumOrStr for MockVal {
    type NumType = u16;
    type StrType = String;

    fn into(self) -> NumOrStr<Self::NumType, Self::StrType> {
        match self {
            MockVal::Num(num) => NumOrStr::Num(num),
            MockVal::Text(text) => NumOrStr::Str(text),
        }
    }

    fn try_match_num(self) -> Result<Self::NumType, Self::StrType> {
        match self {
            MockVal::Num(num) => Result::Ok(num),
            MockVal::Text(text) => Result::Err(text),
        }
    }

    fn try_match_str(self) -> Result<Self::StrType, Self::NumType> {
        match self {
            MockVal::Text(text) => Result::Ok(text),
            MockVal::Num(num) => Result::Err(num),
        }
    }
}

/// 头表里的一条，同时借成键与值。
#[derive(Clone, Debug)]
struct MockEntry {
    key_: StdHeaderKey,
    val_: MockVal,
}

impl Borrow<StdHeaderKey> for MockEntry {
    fn borrow(&self) -> &StdHeaderKey {
        &self.key_
    }
}

impl Borrow<MockVal> for MockEntry {
    fn borrow(&self) -> &MockVal {
        &self.val_
    }
}

/// 最小的 `TrHeaders` 实现。
#[derive(Clone, Debug, Default)]
struct MockHeaders {
    map_: BTreeMap<StdHeaderKey, MockEntry>,
}

impl TrHeaders for MockHeaders {
    type Key = StdHeaderKey;
    type Val = MockVal;
    type Entry = MockEntry;
    type EntriesIter = std::vec::IntoIter<MockEntry>;

    fn try_get_std_header(&self, key: StdHeaderKey) -> Option<&Self::Val> {
        self.map_.get(&key).map(|entry| &entry.val_)
    }

    fn try_get_header<K>(&self, key: K) -> Option<&Self::Val>
    where
        K: Borrow<Self::Key>,
    {
        self.map_.get(key.borrow()).map(|entry| &entry.val_)
    }

    fn into_entries(self) -> Self::EntriesIter {
        self.map_
            .into_values()
            .collect::<Vec<MockEntry>>()
            .into_iter()
    }
}

/// 只携带头表的前缀，直接使用 `TrMsgPrefix::body_info` 的默认实现。
struct MockPrefix {
    headers_: Option<MockHeaders>,
}

impl TrMsgPrefix for MockPrefix {
    type Headers = MockHeaders;

    fn headers(&self) -> Option<&Self::Headers> {
        self.headers_.as_ref()
    }
}

/// 用若干键值对构造一个带头表的前缀。
fn prefix_(entries: impl IntoIterator<Item = (StdHeaderKey, MockVal)>) -> MockPrefix {
    let mut headers = MockHeaders::default();
    for (key, val) in entries {
        headers.map_.insert(
            key,
            MockEntry {
                key_: key,
                val_: val,
            },
        );
    }
    MockPrefix {
        headers_: Option::Some(headers),
    }
}

/// 验证默认 `body_info` 同时识别文本与数字形态的 `Body_Size`。
/// Verify the default `body_info` recognises both textual and numeric `Body_Size`.
///
/// - 测试目标 / Goal：文本长度会被解析为 `Num`，数字长度原样转成 `usize`；
///   `0` 与非法文本都按没有体处理。
/// - 测试手段 / Method：分别构造文本 `"12"`、数字 `12`、文本 `"0"`、数字 `0`
///   与非法文本 `"12x"` 五个前缀，调用默认 `body_info`。
/// - 判定标准 / Assert：前两者为 `FixedLength(Num(12))`，后三者为 `NilBody`。
#[test]
fn body_info_reads_both_size_forms_() {
    let text = prefix_([(StdHeaderKey::Body_Size, MockVal::Text("12".into()))]);
    assert_eq!(text.body_info(), BodySize::FixedLength(NumOrStr::Num(12)));

    let num = prefix_([(StdHeaderKey::Body_Size, MockVal::Num(12))]);
    assert_eq!(num.body_info(), BodySize::FixedLength(NumOrStr::Num(12)));

    let zero_text = prefix_([(StdHeaderKey::Body_Size, MockVal::Text("0".into()))]);
    assert_eq!(zero_text.body_info(), BodySize::NilBody);

    let zero_num = prefix_([(StdHeaderKey::Body_Size, MockVal::Num(0))]);
    assert_eq!(zero_num.body_info(), BodySize::NilBody);

    let bad = prefix_([(StdHeaderKey::Body_Size, MockVal::Text("12x".into()))]);
    assert_eq!(bad.body_info(), BodySize::NilBody);
}

/// 验证默认 `body_info` 识别数字与文本两种分块声明。
/// Verify the default `body_info` recognises both numeric and textual chunked declarations.
///
/// - 测试目标 / Goal：`Body_Transfer` 取数字 `0x20` 或大小写不敏感的文本
///   `"chunked"` 时判为分块；其他取值不算分块。
/// - 测试手段 / Method：构造数字 `0x20`、文本 `"chunked"` / `"CHUNKED"` /
///   `" Chunked "`，以及数字 `0x21` 五个前缀。
/// - 判定标准 / Assert：前四者为 `ChunkedData`，最后一个为 `NilBody`。
#[test]
fn body_info_detects_chunked_in_both_forms_() {
    let num = prefix_([(
        StdHeaderKey::Body_Transfer,
        MockVal::Num(StdHeaderVal::Body_Transfer_Chunked.into_inner()),
    )]);
    assert_eq!(num.body_info(), BodySize::ChunkedData);

    for text in ["chunked", "CHUNKED", " Chunked "] {
        let chunked = prefix_([(StdHeaderKey::Body_Transfer, MockVal::Text(text.into()))]);
        assert_eq!(chunked.body_info(), BodySize::ChunkedData);
    }

    let other = prefix_([(StdHeaderKey::Body_Transfer, MockVal::Num(0x21))]);
    assert_eq!(other.body_info(), BodySize::NilBody);
}

/// 验证两个体声明同时在场时默认实现优先采用 `Body_Size`。
/// Verify the default prefers `Body_Size` when both declarations are present.
///
/// - 测试目标 / Goal：协议把同时在场视为违规，但默认 `body_info` 不返回错误，
///   必须有确定的优先级。
/// - 测试手段 / Method：同时写入文本 `Body_Size: "7"` 与数字
///   `Body_Transfer: Chunked`。
/// - 判定标准 / Assert：结果为 `FixedLength(Num(7))`，不是 `ChunkedData`。
#[test]
fn body_info_prefers_body_size_when_both_present_() {
    let prefix = prefix_([
        (StdHeaderKey::Body_Size, MockVal::Text("7".into())),
        (
            StdHeaderKey::Body_Transfer,
            MockVal::Num(StdHeaderVal::Body_Transfer_Chunked.into_inner()),
        ),
    ]);
    assert_eq!(prefix.body_info(), BodySize::FixedLength(NumOrStr::Num(7)));
}

/// 验证没有头表时默认 `body_info` 返回 `NilBody`。
/// Verify the default returns `NilBody` when the prefix carries no header map.
///
/// - 测试目标 / Goal：没有头表就没有任何体声明。
/// - 测试手段 / Method：构造 `headers_ = None` 的前缀并调用 `body_info`。
/// - 判定标准 / Assert：结果为 `NilBody`。
#[test]
fn body_info_without_headers_is_nil_() {
    let prefix = MockPrefix { headers_: None };
    assert_eq!(prefix.body_info(), BodySize::NilBody);
}
