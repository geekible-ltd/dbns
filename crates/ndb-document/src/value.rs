//! In-memory value model and data-type catalogue (spec §6.3).

use std::fmt;

/// The data types supported by the binary document format (spec §6.3).
///
/// The discriminants are the wire type tags used in field-directory entries;
/// they must never be renumbered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum DataType {
    Null = 0,
    Bool = 1,
    Int = 2,
    UInt = 3,
    Float = 4,
    Decimal = 5,
    String = 6,
    Binary = 7,
    DateTime = 8,
    Object = 9,
    Array = 10,
    Uuid = 11,
    /// Float32 vector — a distinct typed representation (spec §6.3).
    Vector = 12,
}

impl DataType {
    /// Maps a wire type tag back to a `DataType`.
    pub fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Null,
            1 => Self::Bool,
            2 => Self::Int,
            3 => Self::UInt,
            4 => Self::Float,
            5 => Self::Decimal,
            6 => Self::String,
            7 => Self::Binary,
            8 => Self::DateTime,
            9 => Self::Object,
            10 => Self::Array,
            11 => Self::Uuid,
            12 => Self::Vector,
            _ => return None,
        })
    }
}

/// Decimal value stored as `mantissa × 10^-scale`.
///
/// Storage only needs a faithful representation; decimal arithmetic arrives
/// with the query engine. The wire encoding is `i128` mantissa + `u8` scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Decimal {
    pub mantissa: i128,
    pub scale: u8,
}

impl Decimal {
    pub const fn new(mantissa: i128, scale: u8) -> Self {
        Self { mantissa, scale }
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return write!(f, "{}", self.mantissa);
        }
        let negative = self.mantissa < 0;
        let digits = self.mantissa.unsigned_abs().to_string();
        let scale = self.scale as usize;
        if negative {
            write!(f, "-")?;
        }
        if digits.len() > scale {
            let (int, frac) = digits.split_at(digits.len() - scale);
            write!(f, "{int}.{frac}")
        } else {
            write!(f, "0.{:0>width$}", digits, width = scale)
        }
    }
}

/// Date-time stored as microseconds since the Unix epoch (UTC).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DateTime(pub i64);

/// An RFC-4122 UUID stored as its raw 16 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Uuid(pub [u8; 16]);

impl fmt::Display for Uuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

/// A nested object: unordered field name → value map.
///
/// `BTreeMap` keeps objects canonical (name-sorted) and makes field order
/// insignificant — matching document semantics where fields are keyed by
/// name, not position.
pub type Object = std::collections::BTreeMap<String, Value>;

/// A single field value within a document.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Decimal(Decimal),
    String(String),
    Binary(Vec<u8>),
    DateTime(DateTime),
    Object(Object),
    Array(Vec<Value>),
    Uuid(Uuid),
    /// Float32 vector values (spec §6.3).
    Vector(Vec<f32>),
}

impl Value {
    /// The data type of this value.
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Bool(_) => DataType::Bool,
            Self::Int(_) => DataType::Int,
            Self::UInt(_) => DataType::UInt,
            Self::Float(_) => DataType::Float,
            Self::Decimal(_) => DataType::Decimal,
            Self::String(_) => DataType::String,
            Self::Binary(_) => DataType::Binary,
            Self::DateTime(_) => DataType::DateTime,
            Self::Object(_) => DataType::Object,
            Self::Array(_) => DataType::Array,
            Self::Uuid(_) => DataType::Uuid,
            Self::Vector(_) => DataType::Vector,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_tags_roundtrip() {
        for tag in 0u8..=12 {
            assert_eq!(DataType::from_tag(tag).unwrap() as u8, tag);
        }
        assert_eq!(DataType::from_tag(13), None);
    }

    #[test]
    fn decimal_display() {
        assert_eq!(Decimal::new(12345, 2).to_string(), "123.45");
        assert_eq!(Decimal::new(-5, 3).to_string(), "-0.005");
        assert_eq!(Decimal::new(42, 0).to_string(), "42");
    }
}
