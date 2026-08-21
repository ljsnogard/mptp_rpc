pub const fn hash_str(s: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325;
    let mut i = 0;
    let bytes = s.as_bytes();
    while i < bytes.len() {
        hash ^= bytes[i] as u64;
        hash = hash.wrapping_mul(0x00000100000001B3);
        i += 1;
    }
    hash
}

// 宏定义：传入类型，返回 u32
#[macro_export]
macro_rules! type_id_u32 {
    ($t:ty) => {{
        // stringify! 会将类型名原样转为字符串字面量
        // 例如 stringify!(crate::foo::Bar) -> "crate::foo::Bar"
        const HASH: u64 = $crate::codec::hash_str(stringify!($t));
        HASH as u32
    }};
}

#[macro_export]
macro_rules! type_id_u64 {
    ($t:ty) => {{
        // stringify! 会将类型名原样转为字符串字面量
        // 例如 stringify!(crate::foo::Bar) -> "crate::foo::Bar"
        const HASH: u64 = $crate::codec::hash_str(stringify!($t));
        HASH
    }};
}

#[macro_export]
macro_rules! impl_get_type_id_u32 {
    ($($t:ty),* $(,)?) => {
        $(
            impl TrGetTypeId<u32> for $t {
                const TYPE_ID: u32 = type_id_u32!($t);
            }
        )*
    };
}

#[macro_export]
macro_rules! impl_get_type_id_u64 {
    ($($t:ty),* $(,)?) => {
        $(
            impl TrGetTypeId<u64> for $t {
                const TYPE_ID: u64 = type_id_u64!($t);
            }
        )*
    };
}

pub trait TrGetTypeId<TyId>
where
    TyId: Clone + Copy + Eq + Ord + PartialEq + PartialOrd,
{
    const TYPE_ID: TyId;
}

impl_get_type_id_u32!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, str, f32, f64, ());
impl_get_type_id_u64!(u8, u16, u32, u64, usize, i8, i16, i32, i64, isize, str, f32, f64, ());
