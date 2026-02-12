//! Type definitions for Meridian.

/// Meridian types.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    // Primitives
    String,
    Int,
    BigInt,
    Float,
    Double,
    Decimal { precision: u8, scale: u8 },
    Bool,
    // Temporal
    Timestamp,
    Date,
    Time,
    Interval,
    Duration,
    // Complex
    List(Box<Type>),
    Map(Box<Type>, Box<Type>),
    Struct(Vec<(String, Type)>),
    // Streaming
    Stream(Box<Type>),
    Window,
    // Special
    Null,
    Nullable(Box<Type>),
    Enum(Vec<String>),
    // Inference placeholder
    Unknown,
}
