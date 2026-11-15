//! 带转义位置记录的路径表示：`{{` / `}}` 会被折叠成单个字符，同时保留原始索引。

use core::{fmt, ops::Range};

use bumpalo::{Bump, collections::Vec};

/// 一条已经处理过转义的路径。
///
/// 内部用字节保存路径，并额外记录哪些位置来自 `{{` / `}}`。`Deref` 得到字节切片；
/// 所有分配的 `Vec` 都来自同一个 bump arena。
#[derive(Clone)]
pub struct UnescapedRoute<'a> {
    /// 折叠转义后的路径字节。
    inner: Vec<'a, u8>,

    /// `inner` 中由 `{{` / `}}` 折叠出来的位置。
    escaped: Vec<'a, usize>,

    /// 本路径使用的 arena；克隆与派生新路径时沿用。
    bump: &'a Bump,
}

impl<'a> UnescapedRoute<'a> {
    /// 从 `&str` 复制出一条路径，并折叠 `{{` / `}}`。
    pub fn from_str(route: &str, bump: &'a Bump) -> Self {
        let mut inner = Vec::from_iter_in(route.bytes(), bump);
        let mut escaped = Vec::new_in(bump);
        let mut i = 0;

        while let Some(&c) = inner.get(i) {
            if (c == b'{' && inner.get(i + 1) == Some(&b'{'))
                || (c == b'}' && inner.get(i + 1) == Some(&b'}'))
            {
                inner.remove(i);
                escaped.push(i);
            }

            i += 1;
        }

        UnescapedRoute {
            inner,
            escaped,
            bump,
        }
    }

    /// 造一条空路径。
    pub fn empty(bump: &'a Bump) -> Self {
        UnescapedRoute {
            inner: Vec::new_in(bump),
            escaped: Vec::new_in(bump),
            bump,
        }
    }

    /// 第 `i` 个字节是否来自转义。
    pub fn is_escaped(&self, i: usize) -> bool {
        self.escaped.contains(&i)
    }

    /// 用 `replace` 替换 `range`，并同步调整转义索引。
    ///
    /// 返回被替换掉的字节迭代器。
    pub fn splice(
        &mut self,
        range: Range<usize>,
        replace: Vec<'a, u8>,
    ) -> impl Iterator<Item = u8> + '_ {
        // Ignore any escaped characters in the range being replaced.
        self.escaped.retain(|x| !range.contains(x));

        // Update the escaped indices.
        let offset = (replace.len() as isize) - (range.len() as isize);
        for i in &mut self.escaped {
            if *i > range.end {
                *i = i.checked_add_signed(offset).unwrap_or(*i);
            }
        }

        self.inner.splice(range, replace)
    }

    /// 把另一条路径追加到本路径末尾。
    pub fn append(&mut self, other: &UnescapedRoute<'a>) {
        for i in &other.escaped {
            self.escaped.push(self.inner.len() + i);
        }

        self.inner.extend_from_slice(&other.inner);
    }

    /// 把路径截断到 `to` 个字节。
    pub fn truncate(&mut self, to: usize) {
        self.escaped.retain(|&x| x < to);
        self.inner.truncate(to);
    }

    /// 借出一条带转义位置视图的路径。
    pub fn as_ref<'b>(&'b self) -> UnescapedRef<'a, 'b> {
        UnescapedRef {
            inner: &self.inner,
            escaped: &self.escaped,
            offset: 0,
            bump: self.bump,
        }
    }

    /// 折叠转义后的字节切片。
    pub fn unescaped(&self) -> &[u8] {
        &self.inner
    }

    /// 把路径按 UTF-8 解释成 `&str`；非法时返回 `None`。
    pub fn as_str(&self) -> Option<&str> {
        core::str::from_utf8(&self.inner).ok()
    }

    /// 本路径使用的 arena。
    pub const fn bump(&self) -> &'a Bump {
        self.bump
    }

    /// 在另一块 arena 上复制一份内容。
    pub fn clone_in(&self, bump: &'a Bump) -> Self {
        UnescapedRoute {
            inner: Vec::from_iter_in(self.inner.iter().copied(), bump),
            escaped: Vec::from_iter_in(self.escaped.iter().copied(), bump),
            bump,
        }
    }
}

impl core::ops::Deref for UnescapedRoute<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl fmt::Debug for UnescapedRoute<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            Some(s) => fmt::Debug::fmt(s, f),
            None => fmt::Debug::fmt(&*self.inner, f),
        }
    }
}

/// `UnescapedRoute` 的借用视图。
#[derive(Copy, Clone)]
pub struct UnescapedRef<'a, 'b> {
    /// 折叠转义后的路径字节。
    inner: &'b [u8],

    /// 完整路径的转义位置表。
    escaped: &'b [usize],

    /// 相对于完整路径的字节偏移。
    offset: isize,

    /// 所属 arena，派生新路径时使用。
    bump: &'a Bump,
}

impl<'a, 'b> UnescapedRef<'a, 'b> {
    /// 复制成一条独立路径。
    pub fn to_owned(self) -> UnescapedRoute<'a> {
        let mut escaped = Vec::new_in(self.bump);
        for &i in self.escaped {
            let i = i.checked_add_signed(self.offset);

            match i {
                Some(i) if i < self.inner.len() => escaped.push(i),
                _ => {}
            }
        }

        UnescapedRoute {
            escaped,
            inner: Vec::from_iter_in(self.inner.iter().copied(), self.bump),
            bump: self.bump,
        }
    }

    /// 第 `i` 个字节是否来自转义。
    pub fn is_escaped(&self, i: usize) -> bool {
        if let Some(i) = i.checked_add_signed(-self.offset) {
            return self.escaped.contains(&i);
        }

        false
    }

    /// 切出 `start..`。
    pub fn slice_off(&self, start: usize) -> UnescapedRef<'a, 'b> {
        UnescapedRef {
            inner: &self.inner[start..],
            escaped: self.escaped,
            offset: self.offset - (start as isize),
            bump: self.bump,
        }
    }

    /// 切出 `..end`。
    pub fn slice_until(&self, end: usize) -> UnescapedRef<'a, 'b> {
        UnescapedRef {
            inner: &self.inner[..end],
            escaped: self.escaped,
            offset: self.offset,
            bump: self.bump,
        }
    }

    /// 折叠转义后的字节切片。
    pub fn unescaped(&self) -> &[u8] {
        self.inner
    }
}

impl core::ops::Deref for UnescapedRef<'_, '_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.inner
    }
}

impl fmt::Debug for UnescapedRef<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnescapedRef")
            .field("inner", &core::str::from_utf8(self.inner))
            .field("escaped", &self.escaped)
            .field("offset", &self.offset)
            .finish()
    }
}
