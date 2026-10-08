use std::collections::BTreeMap;

use crate::specs::{HeaderKey, HeaderVal, Headers};

pub struct HeadersBuilder(BTreeMap<HeaderKey, HeaderVal>);

impl HeadersBuilder {
    pub fn set_body_type(self, body_type: &HeaderVal) -> Self {
        todo!()
    }

    pub fn set_body_size(self, body_size: usize) -> Self {
        todo!()
    }

    pub fn set_data_type<T>(self, type_id: T) -> Self {
        todo!()
    }

    pub fn set(
        mut self,
        key: impl Into<HeaderKey>,
        val: impl Into<HeaderVal>,
    ) -> Self {
        self.add_or_replace(key.into(), val.into());
        self
    }

    fn add_or_replace(&mut self, key: HeaderKey, val: HeaderVal) {
        self.0.insert(key.into(), val.into());
    }

    pub fn build(self) -> Result<Headers, Self> {
        todo!()
    }
}
