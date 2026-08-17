use std::collections::BTreeMap;

/// 只依据字符串前缀进行匹配的简单路由，demo 用
pub struct Router<T> {
    prefix_map_: BTreeMap<String, T>
}

impl<T> Router<T> {
    pub const fn new() -> Self {
        Router { prefix_map_: BTreeMap::new() }
    }

    pub fn add_target(&mut self, prefix: &str, target: T) -> Option<T> {
        self.prefix_map_
            .insert(prefix.to_string(), target)
    }

    pub fn try_match(&self, prefix: &str) -> Option<&T> {
        self.prefix_map_.get(prefix)
    }
}
