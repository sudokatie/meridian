//! Type definitions for Meridian.

/// Meridian types.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    String,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    Bool,
    Timestamp,
    Date,
    Time,
    List(Box<Type>),
    Struct(Vec<(String, Type)>),
    Nullable(Box<Type>),
    Enum(Vec<String>),
    Unknown,
}
