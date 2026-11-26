//! Human-readable byte sizes for configuration values such as
//! `buffer_pool_size: 3GB`.

use ndb_common::error::Error;
use serde::{Deserialize, Deserializer, de};
use std::fmt;

/// A size in bytes, deserialised from either a raw integer (bytes) or a
/// string with a `KB`/`MB`/`GB`/`TB` suffix (decimal or with optional `i`).
///
/// Examples: `1048576`, `"1MB"`, `"3GB"`, `"512MiB"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ByteSize(pub u64);

impl ByteSize {
    /// Size in bytes.
    pub const fn bytes(self) -> u64 {
        self.0
    }

    /// Parses a size string such as `3GB` or `512MiB`.
    pub fn parse(s: &str) -> ndb_common::Result<Self> {
        let s = s.trim();
        let split = s.find(|c: char| c.is_ascii_alphabetic()).unwrap_or(s.len());
        let (num, unit) = s.split_at(split);
        let value: f64 = num
            .trim()
            .parse()
            .map_err(|_| Error::Config(format!("invalid byte size '{s}'")))?;
        if value < 0.0 {
            return Err(Error::Config(format!("negative byte size '{s}'")));
        }
        let multiplier = match unit.trim().to_ascii_uppercase().as_str() {
            "" | "B" => 1_u64,
            "KB" => 1_000,
            "MB" => 1_000_000,
            "GB" => 1_000_000_000,
            "TB" => 1_000_000_000_000,
            "KIB" => 1 << 10,
            "MIB" => 1 << 20,
            "GIB" => 1 << 30,
            "TIB" => 1 << 40,
            other => {
                return Err(Error::Config(format!(
                    "unknown byte-size unit '{other}' in '{s}'"
                )));
            }
        };
        Ok(ByteSize((value * multiplier as f64) as u64))
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}B", self.0)
    }
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Int(u64),
            Str(String),
        }
        match Repr::deserialize(d)? {
            Repr::Int(v) => Ok(ByteSize(v)),
            Repr::Str(s) => ByteSize::parse(&s).map_err(de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_units() {
        assert_eq!(ByteSize::parse("3GB").unwrap().bytes(), 3_000_000_000);
        assert_eq!(
            ByteSize::parse("512MiB").unwrap().bytes(),
            512 * 1024 * 1024
        );
        assert_eq!(ByteSize::parse("1024").unwrap().bytes(), 1024);
        assert_eq!(ByteSize::parse("2 KB").unwrap().bytes(), 2_000);
        assert!(ByteSize::parse("abc").is_err());
        assert!(ByteSize::parse("1XB").is_err());
        assert!(ByteSize::parse("-5GB").is_err());
    }
}
