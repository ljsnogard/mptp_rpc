//! `TrMsgPrefix::body_info` 默认实现的集成测试。
//!
//! 用例只依赖公开契约：一个最小的 `TrHeaders` 实现，加上 `TrMsgPrefix` 的默认
//! `body_info`。覆盖数字长度、数字分块声明、对象形态（不是长度 / 不是分块）、两个头
//! 同时在场的冲突，以及没有头表的空情形。
//!
//! Integration tests for the default `TrMsgPrefix::body_info`. They use a minimal
//! `TrHeaders` implementation and exercise numeric sizes, the numeric chunked declaration,
//! object forms (neither a length nor a chunked marker), the conflicting-both-present case,
//! and the no-header case.

use core::borrow::Borrow;
use std::collections::BTreeMap;

use abs_mptp::{
    messaging::{BodySize, TrMsgPrefix},
    spec::{NumOrObj, NumOrStr, ObjEnvelope, StdHeaderKey, StdHeaderVal, TrHeaders},
};

/// 测试用的头值：新的「数字或对象」形态。
///
/// `TrNumOrObj` 已经为 `NumOrObj<N, B>` 提供实现，这里直接复用它；对象形态的载荷用
/// `Vec<u8>` 承载，便于构造测试数据。
type MockVal = NumOrObj<u16, Vec<u8>>;

/// 数字形态的辅助构造。
fn num_(value: u16) -> MockVal {
    NumOrObj::Num(value)
}

/// 对象形态的辅助构造。
fn obj_(type_id: u32, data: &[u8]) -> MockVal {
    NumOrObj::Obj(ObjEnvelope::new(type_id, data.to_vec()))
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
    type Headers<'f>
        = MockHeaders
    where
        Self: 'f;

    fn headers(&self) -> Option<&Self::Headers<'_>> {
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

/// 验证默认 `body_info` 只把数字 `Body_Size` 当成定长声明。
/// Verify the default `body_info` treats only a numeric `Body_Size` as a sized body.
///
/// - 测试目标 / Goal：数字 `12` 判为定长 `12`，数字 `0` 归一成「没有体」。
/// - 测试手段 / Method：分别构造 `Body_Size: Num(12)` 与 `Body_Size: Num(0)` 两个前缀，
///   调用默认 `body_info`。
/// - 判定标准 / Assert：前者为 `FixedLength(Num(12))`，后者为 `NilBody`。
///
/// - Goal: a numeric `12` is a sized body of 12 bytes while a numeric `0` normalises to
///   "no body".
/// - Method: build prefixes with `Body_Size: Num(12)` and `Body_Size: Num(0)` and call the
///   default `body_info`.
/// - Assert: the first is `FixedLength(Num(12))` and the second is `NilBody`.
#[test]
fn body_info_reads_numeric_size_() {
    let sized = prefix_([(StdHeaderKey::Body_Size, num_(12))]);
    assert_eq!(sized.body_info(), BodySize::FixedLength(NumOrStr::Num(12)));

    let zero = prefix_([(StdHeaderKey::Body_Size, num_(0))]);
    assert_eq!(zero.body_info(), BodySize::NilBody);
}

/// 验证对象形态的 `Body_Size` 不是长度声明。
/// Verify that an object-form `Body_Size` is not a length declaration.
///
/// - 测试目标 / Goal：`Body_Size` 是对象形态时按「没有长度」处理，而不是解析载荷。
/// - 测试手段 / Method：构造 `Body_Size: Obj(type_id=0x1234, data=[0x0C])` 的前缀。
/// - 判定标准 / Assert：`body_info` 返回 `NilBody`。
///
/// - Goal: an object-form `Body_Size` counts as "no length" instead of being parsed.
/// - Method: build a prefix with `Body_Size: Obj(type_id=0x1234, data=[0x0C])`.
/// - Assert: `body_info` returns `NilBody`.
#[test]
fn body_info_ignores_object_size_() {
    let prefix = prefix_([(StdHeaderKey::Body_Size, obj_(0x1234, &[0x0C]))]);
    assert_eq!(prefix.body_info(), BodySize::NilBody);
}

/// 验证默认 `body_info` 只认数字 `Body_Transfer: 0x20` 为分块声明。
/// Verify the default `body_info` recognises only numeric `Body_Transfer: 0x20` as chunked.
///
/// - 测试目标 / Goal：数字 `0x20` 判为分块；数字 `0x21` 与对象形态都不算分块。
/// - 测试手段 / Method：分别构造 `Body_Transfer: Num(0x20)`、`Num(0x21)` 与
///   `Obj(0x20 的字节载荷)` 三个前缀。
/// - 判定标准 / Assert：第一个为 `ChunkedData`，后两个为 `NilBody`。
///
/// - Goal: numeric `0x20` is chunked; numeric `0x21` and the object form are not.
/// - Method: build prefixes with `Body_Transfer: Num(0x20)`, `Num(0x21)` and an object
///   carrying the `0x20` payload.
/// - Assert: the first is `ChunkedData`; the other two are `NilBody`.
#[test]
fn body_info_detects_only_numeric_chunked_() {
    let chunked = prefix_([(
        StdHeaderKey::Body_Transfer,
        num_(StdHeaderVal::Body_Transfer_Chunked.into_inner()),
    )]);
    assert_eq!(chunked.body_info(), BodySize::ChunkedData);

    let other = prefix_([(StdHeaderKey::Body_Transfer, num_(0x21))]);
    assert_eq!(other.body_info(), BodySize::NilBody);

    let object = prefix_([(StdHeaderKey::Body_Transfer, obj_(0x20, &[0x20]))]);
    assert_eq!(object.body_info(), BodySize::NilBody);
}

/// 验证两个体声明同时在场时默认实现优先采用 `Body_Size`。
/// Verify the default prefers `Body_Size` when both declarations are present.
///
/// - 测试目标 / Goal：协议把同时在场视为违规，但默认 `body_info` 不返回错误，
///   必须有确定的优先级。
/// - 测试手段 / Method：同时写入数字 `Body_Size: 7` 与数字 `Body_Transfer: Chunked`。
/// - 判定标准 / Assert：结果为 `FixedLength(Num(7))`，不是 `ChunkedData`。
///
/// - Goal: both declarations together are a protocol violation, but the default must pick
///   one deterministically.
/// - Method: write numeric `Body_Size: 7` together with numeric `Body_Transfer: Chunked`.
/// - Assert: the result is `FixedLength(Num(7))`, not `ChunkedData`.
#[test]
fn body_info_prefers_body_size_when_both_present_() {
    let prefix = prefix_([
        (StdHeaderKey::Body_Size, num_(7)),
        (
            StdHeaderKey::Body_Transfer,
            num_(StdHeaderVal::Body_Transfer_Chunked.into_inner()),
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
///
/// - Goal: no header map means no body declaration.
/// - Method: build a prefix with `headers_ = None` and call `body_info`.
/// - Assert: the result is `NilBody`.
#[test]
fn body_info_without_headers_is_nil_() {
    let prefix = MockPrefix { headers_: None };
    assert_eq!(prefix.body_info(), BodySize::NilBody);
}
