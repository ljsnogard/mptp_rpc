// use std::{
//     borrow::Borrow,
//     hash::Hash,
//     ops::Deref,
//     sync::Arc,
// };

// #[derive(Clone, Debug)]
// pub struct ArcStr(Arc<[u8]>);

// impl ArcStr {
//     pub fn clone_from_str(_borrow_str: &impl Borrow<str>) -> Self {
//         todo!()
//     }

//     pub fn as_str(&self) -> &str {
//         unsafe { str::from_utf8_unchecked(self.0.deref()) }
//     }
// }

// impl Deref for ArcStr {
//     type Target = str;

//     fn deref(&self) -> &Self::Target {
//         self.as_str()
//     }
// }

// impl Borrow<str> for ArcStr {
//     fn borrow(&self) -> &str {
//         self.as_str()
//     }
// }

// impl PartialEq for ArcStr {
//     fn eq(&self, other: &Self) -> bool {
//         str::eq(self.borrow(), <ArcStr as Borrow<str>>::borrow(other))
//     }
// }

// impl PartialOrd for ArcStr {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         str::partial_cmp(self.borrow(), <ArcStr as Borrow<str>>::borrow(other))
//     }
// }

// impl Eq for ArcStr {}

// impl Ord for ArcStr {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         str::cmp(self.borrow(), <ArcStr as Borrow<str>>::borrow(other))
//     }
// }

// impl Hash for ArcStr {
//     fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
//         str::hash(self.borrow(), state);
//     }
// }
