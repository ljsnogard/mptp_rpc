//! 路径参数表：小参数集走栈上数组，超过 [`SMALL`] 才临时分配。

use alloc::vec::Vec;
use core::{fmt, iter, mem, slice};

/// 单个 URL 参数：键与值都以字节切片保存，读取时才做 UTF-8 校验。
#[derive(PartialEq, Eq, Ord, PartialOrd, Default, Copy, Clone)]
pub(crate) struct Param<'k, 'v> {
    /// 参数名，借用路由表。
    pub(crate) key: &'k [u8],

    /// 参数值，借用被匹配的路径。
    pub(crate) value: &'v [u8],
}

impl<'k, 'v> Param<'k, 'v> {
    const EMPTY: Param<'static, 'static> = Param {
        key: b"",
        value: b"",
    };

    /// 参数名转成 `&str`。
    fn key_str(&self) -> &'k str {
        core::str::from_utf8(self.key).unwrap_or("")
    }

    /// 参数值转成 `&str`。
    fn value_str(&self) -> &'v str {
        core::str::from_utf8(self.value).unwrap_or("")
    }
}

/// 一次成功匹配返回的参数列表。
///
/// 典型路由只有少量参数；前三个参数放在内联数组里，避免为每个请求分配。超过三个
/// 参数时使用一次性 `alloc::vec::Vec`，匹配结束随 `Params` 一起释放。
///
/// ```rust
/// use matchit_route::Router;
///
/// let mut router = Router::new();
/// router.insert("/users/{id}", true).unwrap();
/// let matched = router.at("/users/1").unwrap();
///
/// for (key, value) in matched.params.iter() {
///     assert_eq!((key, value), ("id", "1"));
/// }
/// assert_eq!(matched.params.get("id"), Some("1"));
/// ```
#[derive(PartialEq, Eq, Ord, PartialOrd, Clone)]
pub struct Params<'k, 'v> {
    kind: ParamsKind<'k, 'v>,
}

impl Default for Params<'_, '_> {
    fn default() -> Self {
        Self::new()
    }
}

/// 常见路由的参数数量上限；超过后转为堆上 `Vec`。
const SMALL: usize = 3;

/// 参数表的两种表示。
#[derive(PartialEq, Eq, Ord, PartialOrd, Clone)]
enum ParamsKind<'k, 'v> {
    /// 内联小数组与已用长度。
    Small([Param<'k, 'v>; SMALL], usize),

    /// 参数较多时的临时堆分配。
    Large(Vec<Param<'k, 'v>>),
}

impl<'k, 'v> Params<'k, 'v> {
    /// 造一个空参数表。
    #[inline]
    pub fn new() -> Self {
        Self {
            kind: ParamsKind::Small([Param::EMPTY; SMALL], 0),
        }
    }

    /// 参数个数。
    pub fn len(&self) -> usize {
        match self.kind {
            ParamsKind::Small(_, len) => len,
            ParamsKind::Large(ref vec) => vec.len(),
        }
    }

    /// 截断到 `n` 个参数；回溯时丢弃本次尝试新增的参数。
    pub(crate) fn truncate(&mut self, n: usize) {
        match &mut self.kind {
            ParamsKind::Small(_, len) => *len = n,
            ParamsKind::Large(vec) => vec.truncate(n),
        }
    }

    /// 返回第一个同名参数的值。
    pub fn get(&self, key: impl AsRef<str>) -> Option<&'v str> {
        let key = key.as_ref().as_bytes();

        match &self.kind {
            ParamsKind::Small(arr, len) => arr
                .iter()
                .take(*len)
                .find(|param| param.key == key)
                .map(Param::value_str),
            ParamsKind::Large(vec) => vec
                .iter()
                .find(|param| param.key == key)
                .map(Param::value_str),
        }
    }

    /// 遍历参数。
    pub fn iter(&self) -> ParamsIter<'_, 'k, 'v> {
        ParamsIter::new(self)
    }

    /// 参数表是否为空。
    pub fn is_empty(&self) -> bool {
        match self.kind {
            ParamsKind::Small(_, len) => len == 0,
            ParamsKind::Large(ref vec) => vec.is_empty(),
        }
    }

    /// 追加一个参数；超过内联容量时转成堆分配。
    #[inline]
    pub(crate) fn push(&mut self, key: &'k [u8], value: &'v [u8]) {
        #[cold]
        #[inline(never)]
        fn drain_to_vec<T: Default>(len: usize, elem: T, arr: &mut [T; SMALL]) -> Vec<T> {
            let mut vec = Vec::with_capacity(len + 1);
            vec.extend(arr.iter_mut().map(mem::take));
            vec.push(elem);
            vec
        }

        #[cold]
        #[inline(never)]
        fn push_slow<'k, 'v>(vec: &mut Vec<Param<'k, 'v>>, param: Param<'k, 'v>) {
            vec.push(param);
        }

        let param = Param { key, value };
        match &mut self.kind {
            ParamsKind::Small(arr, len) => {
                if *len >= SMALL {
                    self.kind = ParamsKind::Large(drain_to_vec(*len, param, arr));
                    return;
                }

                arr[*len] = param;
                *len += 1;
            }

            ParamsKind::Large(vec) => push_slow(vec, param),
        }
    }

    /// 对每个参数的键应用一次转换；用于把规范化键替换成真实键。
    #[inline]
    pub(crate) fn for_each_key_mut(&mut self, f: impl Fn((usize, &mut Param<'k, 'v>))) {
        match &mut self.kind {
            ParamsKind::Small(arr, len) => arr.iter_mut().take(*len).enumerate().for_each(f),
            ParamsKind::Large(vec) => vec.iter_mut().enumerate().for_each(f),
        }
    }
}

impl fmt::Debug for Params<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// [`Params`] 的迭代器。
pub struct ParamsIter<'ps, 'k, 'v> {
    kind: ParamsIterKind<'ps, 'k, 'v>,
}

impl<'ps, 'k, 'v> ParamsIter<'ps, 'k, 'v> {
    /// 按参数表的表示选择对应迭代器。
    fn new(params: &'ps Params<'k, 'v>) -> Self {
        let kind = match &params.kind {
            ParamsKind::Small(arr, len) => ParamsIterKind::Small(arr.iter().take(*len)),
            ParamsKind::Large(vec) => ParamsIterKind::Large(vec.iter()),
        };
        Self { kind }
    }
}

/// 两种底层迭代器。
enum ParamsIterKind<'ps, 'k, 'v> {
    /// 内联数组的 `take`。
    Small(iter::Take<slice::Iter<'ps, Param<'k, 'v>>>),

    /// 堆上 `Vec` 的迭代器。
    Large(slice::Iter<'ps, Param<'k, 'v>>),
}

impl<'k, 'v> Iterator for ParamsIter<'_, 'k, 'v> {
    type Item = (&'k str, &'v str);

    fn next(&mut self) -> Option<Self::Item> {
        match self.kind {
            ParamsIterKind::Small(ref mut iter) => {
                iter.next().map(|p| (p.key_str(), p.value_str()))
            }
            ParamsIterKind::Large(ref mut iter) => {
                iter.next().map(|p| (p.key_str(), p.value_str()))
            }
        }
    }
}

impl ExactSizeIterator for ParamsIter<'_, '_, '_> {
    fn len(&self) -> usize {
        match self.kind {
            ParamsIterKind::Small(ref iter) => iter.len(),
            ParamsIterKind::Large(ref iter) => iter.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;

    /// 测试超过内联容量后参数表切换到堆分配表示。
    /// Test that more than `SMALL` parameters switch to the heap representation.
    ///
    /// - 手段：依次 push 五个参数，并逐个用 `get` 读回。
    /// - 判断：内部表示最终必须是 `Large`，且迭代顺序与插入顺序一致。
    ///
    /// - Means: push five parameters one by one and read each back with `get`.
    /// - Verdict: the representation becomes `Large` and iteration keeps insertion order.
    #[test]
    fn heap_alloc() {
        let vec = vec![
            ("hello", "hello"),
            ("world", "world"),
            ("foo", "foo"),
            ("bar", "bar"),
            ("baz", "baz"),
        ];

        let mut params = Params::new();
        for (key, value) in vec.clone() {
            params.push(key.as_bytes(), value.as_bytes());
            assert_eq!(params.get(key), Some(value));
        }

        match params.kind {
            ParamsKind::Large(..) => {}
            _ => panic!(),
        }

        assert!(params.iter().eq(vec));
    }

    /// 测试不超过内联容量时参数表保持栈上表示。
    /// Test that up to `SMALL` parameters stay inline.
    ///
    /// - 手段：push 三个参数并读取。
    /// - 判断：内部表示仍是 `Small`，迭代结果与输入一致。
    ///
    /// - Means: push three parameters and read them back.
    /// - Verdict: the representation is still `Small` and iteration matches the input.
    #[test]
    fn stack_alloc() {
        let vec = vec![("hello", "hello"), ("world", "world"), ("baz", "baz")];

        let mut params = Params::new();
        for (key, value) in vec.clone() {
            params.push(key.as_bytes(), value.as_bytes());
            assert_eq!(params.get(key), Some(value));
        }

        match params.kind {
            ParamsKind::Small(..) => {}
            _ => panic!(),
        }

        assert!(params.iter().eq(vec));
    }

    /// 测试空参数表不会把默认占位项误当成真实参数。
    /// Test that the default placeholder entries are never exposed.
    ///
    /// - 手段：构造空 `Params` 并查询空键。
    /// - 判断：`get("")` 返回 `None`。
    ///
    /// - Means: build an empty `Params` and query the empty key.
    /// - Verdict: `get("")` returns `None`.
    #[test]
    fn ignore_array_default() {
        let params = Params::new();
        assert!(params.get("").is_none());
    }
}
