use core::borrow::Borrow;

use denco_macro::{TrDecodable, TrEncodable};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum U16OrStr<S>
where
    S: Borrow<str>,
{
    U16(u16),
    Str(S),
}
