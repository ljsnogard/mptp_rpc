// 本模块的构造器目前全是 `todo!()`，且 `client` 侧还没有接入 header 组装流程。
// 等组装流程落地后即可移除本 allow。
#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::specs::{HeaderKey, HeaderVal, Headers};

pub struct HeadersBuilder(BTreeMap<HeaderKey, HeaderVal>);

impl HeadersBuilder {
    pub fn set_body_type(self, _body_type: &HeaderVal) -> Self {
        todo!()
    }

    pub fn set_body_size(self, _body_size: usize) -> Self {
        todo!()
    }

    pub fn set_data_type<T>(self, _type_id: T) -> Self {
        todo!()
    }

    pub fn set(mut self, key: impl Into<HeaderKey>, val: impl Into<HeaderVal>) -> Self {
        self.add_or_replace(key.into(), val.into());
        self
    }

    fn add_or_replace(&mut self, key: HeaderKey, val: HeaderVal) {
        self.0.insert(key, val);
    }

    pub fn build(self) -> Result<Headers, Self> {
        todo!()
    }
}
