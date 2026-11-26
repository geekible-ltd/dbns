//! Order-preserving composite key encoding (spec §5.3 — composite keys).
//!
//! The B+Tree orders raw byte keys lexicographically. Composite keys are
//! encoded so that lexicographic byte order equals logical tuple order:
//!
//! - `UInt`: tag `0x10` + 8-byte big-endian.
//! - `Int`: tag `0x20` + big-endian `value ^ i64::MIN` (flips the sign so
//!   negatives sort before positives).
//! - `Str`/`Bytes`: tag `0x30`/`0x40` + bytes with `0x00` escaped as
//!   `0x00 0xFF`, terminated by `0x00 0x00` — prefix-free, so shorter values
//!   sort first.
//!
//! Callers must use consistent types per position: a `UInt` in position 0
//! must not mix with a `Str` in position 0 — tag order determines
//! cross-type ordering.

/// One component of a composite key.
#[derive(Debug, Clone, PartialEq)]
pub enum KeyPart {
    UInt(u64),
    Int(i64),
    Str(String),
    Bytes(Vec<u8>),
}

const TAG_UINT: u8 = 0x10;
const TAG_INT: u8 = 0x20;
const TAG_STR: u8 = 0x30;
const TAG_BYTES: u8 = 0x40;

/// Encodes a composite key to an order-preserving byte string.
pub fn encode_composite(parts: &[KeyPart]) -> Vec<u8> {
    let mut out = Vec::new();
    for part in parts {
        match part {
            KeyPart::UInt(v) => {
                out.push(TAG_UINT);
                out.extend_from_slice(&v.to_be_bytes());
            }
            KeyPart::Int(v) => {
                out.push(TAG_INT);
                out.extend_from_slice(&((*v as u64) ^ (1u64 << 63)).to_be_bytes());
            }
            KeyPart::Str(s) => {
                out.push(TAG_STR);
                encode_escaped(&mut out, s.as_bytes());
            }
            KeyPart::Bytes(b) => {
                out.push(TAG_BYTES);
                encode_escaped(&mut out, b);
            }
        }
    }
    out
}

/// Writes `bytes` with every 0x00 escaped as `0x00 0xFF`, then a `0x00 0x00`
/// terminator — shorter byte strings always sort before longer ones sharing
/// their prefix.
fn encode_escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        if b == 0 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(b);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint_ordering() {
        let mut keys: Vec<Vec<u8>> = [100u64, 1, 20, 3, 255]
            .iter()
            .map(|&v| encode_composite(&[KeyPart::UInt(v)]))
            .collect();
        keys.sort();
        let order: Vec<u64> = keys
            .iter()
            .map(|k| u64::from_be_bytes(k[1..9].try_into().unwrap()))
            .collect();
        assert_eq!(order, vec![1, 3, 20, 100, 255]);
    }

    #[test]
    fn int_ordering_includes_negatives() {
        let vals = [0i64, -1, 5, -100, i64::MAX, i64::MIN];
        let mut keys: Vec<(Vec<u8>, i64)> = vals
            .iter()
            .map(|&v| (encode_composite(&[KeyPart::Int(v)]), v))
            .collect();
        keys.sort();
        let order: Vec<i64> = keys.into_iter().map(|(_, v)| v).collect();
        let mut sorted = vals;
        sorted.sort();
        assert_eq!(order, sorted);
    }

    #[test]
    fn composite_ordering_is_lexicographic() {
        let mk =
            |t: u64, s: &str| encode_composite(&[KeyPart::UInt(t), KeyPart::Str(s.to_string())]);
        let mut keys = vec![mk(2, "a"), mk(1, "z"), mk(1, "a"), mk(2, "b")];
        keys.sort();
        assert_eq!(keys, vec![mk(1, "a"), mk(1, "z"), mk(2, "a"), mk(2, "b")]);
    }

    #[test]
    fn embedded_nulls_are_escaped() {
        let a = encode_composite(&[KeyPart::Bytes(vec![1, 0, 2])]);
        let b = encode_composite(&[KeyPart::Bytes(vec![1, 0, 3])]);
        let c = encode_composite(&[KeyPart::Bytes(vec![1, 0, 2, 0])]);
        assert!(a < b);
        assert!(a < c); // prefix sorts first
    }
}
