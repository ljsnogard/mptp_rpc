//! 访问方法集合：handler 声明自己支持哪些方法。
//!
//! `AccessMethod` 是运行期值；`MethodSet` 是它的位集合，用于 `allowed_methods()` 与
//! `405` 的 `Allow` 头。文本形态是逗号分隔的方法名，便于调试与跨语言实现。
//!
//! # `method_set_`: the set of access methods
//!
//! `AccessMethod` is a runtime value; `MethodSet` is its bit set, used by
//! `allowed_methods()` and the `Allow` header of a `405`. The textual form is a
//! comma-separated list of method names.

use alloc::string::String;

use abs_mptp::spec::access_method::AccessMethod;

/// 一组访问方法。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MethodSet(u8);

impl MethodSet {
    /// 空集合。
    pub const NONE: Self = Self(0);

    /// 只允许 `Head`。
    pub const HEAD: Self = Self(1 << 0);

    /// 只允许 `View`。
    pub const VIEW: Self = Self(1 << 1);

    /// 只允许 `Post`。
    pub const POST: Self = Self(1 << 2);

    /// 只允许 `Drop`。
    pub const DROP: Self = Self(1 << 3);

    /// 只允许 `Push`。
    pub const PUSH: Self = Self(1 << 4);

    /// 只允许 `Pull`。
    pub const PULL: Self = Self(1 << 5);

    /// 只允许 `Call`。
    pub const CALL: Self = Self(1 << 6);

    /// 七种方法全部允许。
    pub const ALL: Self = Self(0b0111_1111);

    /// 由裸位构造；越界位被截断到七位。
    pub const fn from_bits_(bits: u8) -> Self {
        MethodSet(bits & 0b0111_1111)
    }

    /// 裸位。
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// 集合是否为空。
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// 是否包含某个方法。
    pub const fn contains(self, method: AccessMethod) -> bool {
        (self.0 & bit_of_(method)) != 0
    }

    /// 并集。
    pub const fn union(self, other: Self) -> Self {
        MethodSet(self.0 | other.0)
    }

    /// 交集。
    pub const fn intersection(self, other: Self) -> Self {
        MethodSet(self.0 & other.0)
    }

    /// 差集。
    pub const fn difference(self, other: Self) -> Self {
        MethodSet(self.0 & !other.0)
    }

    /// 生成 `Allow` 头的文本值：`"HEAD,VIEW,POST,CALL"`。
    pub fn to_allow_value(self) -> String {
        let mut out = String::new();
        for method in ACCESS_METHODS {
            if self.contains(method) {
                if !out.is_empty() {
                    out.push(',');
                }
                out.push_str(name_of_(method));
            }
        }
        out
    }

    /// 解析 `Allow` 头的文本值；未知项忽略，大小写不敏感。
    pub fn from_allow_value(text: &str) -> Self {
        let mut set = MethodSet::NONE;
        for item in text.split(',') {
            let item = item.trim();
            for method in ACCESS_METHODS {
                if item.eq_ignore_ascii_case(name_of_(method)) {
                    set = set.union(MethodSet::from_bits_(bit_of_(method)));
                }
            }
        }
        set
    }
}

impl core::ops::BitOr for MethodSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.union(rhs)
    }
}

impl core::ops::BitAnd for MethodSet {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.intersection(rhs)
    }
}

impl core::ops::BitOrAssign for MethodSet {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = self.union(rhs);
    }
}

impl core::ops::BitAndAssign for MethodSet {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = self.intersection(rhs);
    }
}

/// 固定顺序，供文本生成与解析使用。
const ACCESS_METHODS: [AccessMethod; 7] = [
    AccessMethod::Head,
    AccessMethod::View,
    AccessMethod::Post,
    AccessMethod::Drop,
    AccessMethod::Push,
    AccessMethod::Pull,
    AccessMethod::Call,
];

/// 方法对应的位。
const fn bit_of_(method: AccessMethod) -> u8 {
    match method {
        AccessMethod::Head => 1 << 0,
        AccessMethod::View => 1 << 1,
        AccessMethod::Post => 1 << 2,
        AccessMethod::Drop => 1 << 3,
        AccessMethod::Push => 1 << 4,
        AccessMethod::Pull => 1 << 5,
        AccessMethod::Call => 1 << 6,
    }
}

/// 方法的文本名（大写）。
const fn name_of_(method: AccessMethod) -> &'static str {
    match method {
        AccessMethod::Head => "HEAD",
        AccessMethod::View => "VIEW",
        AccessMethod::Post => "POST",
        AccessMethod::Drop => "DROP",
        AccessMethod::Push => "PUSH",
        AccessMethod::Pull => "PULL",
        AccessMethod::Call => "CALL",
    }
}

#[cfg(test)]
mod tests_ {
    use super::*;

    /// 测试 `Allow` 文本的生成与解析互为逆运算。
    /// Test that generating and parsing the `Allow` text are inverse operations.
    ///
    /// - 手段：构造 `HEAD | VIEW | POST | CALL`，生成文本再用 `from_allow_value` 解析。
    /// - 判断：解析结果与原始集合相等，文本顺序固定为方法枚举顺序。
    ///
    /// - Means: build `HEAD | VIEW | POST | CALL`, render it, and parse it back.
    /// - Verdict: the parsed set equals the original and the textual order is fixed.
    #[test]
    fn allow_text_round_trips_() {
        let set = MethodSet::HEAD
            .union(MethodSet::VIEW)
            .union(MethodSet::POST)
            .union(MethodSet::CALL);
        let text = set.to_allow_value();
        assert_eq!(text, "HEAD,VIEW,POST,CALL");
        assert_eq!(MethodSet::from_allow_value(&text), set);
    }

    /// 测试未知项被忽略、大小写不敏感。
    /// Test that unknown entries are ignored and matching is case-insensitive.
    ///
    /// - 手段：解析 `"head, view, unknown"`。
    /// - 判断：结果只包含 `HEAD | VIEW`。
    ///
    /// - Means: parse `"head, view, unknown"`.
    /// - Verdict: only `HEAD | VIEW` is present.
    #[test]
    fn allow_text_ignores_unknown_() {
        let set = MethodSet::from_allow_value("head, view, unknown");
        assert_eq!(set, MethodSet::HEAD.union(MethodSet::VIEW));
    }
}
