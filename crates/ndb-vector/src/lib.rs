//! Vector primitives (spec §10.1, §10.3).
//!
//! - Distance metrics: cosine, dot product, Euclidean — the §10.1 set.
//! - [`EncodedVector`]: storage codec carrying an encoding tag so f16/int8
//!   quantisation lands without a format migration (§10.3 groundwork).
//!   Float32 is the reference implementation.

use ndb_common::error::Error;

/// Distance metrics (§10.1, §10.2 config).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Cosine,
    Dot,
    Euclidean,
}

/// Higher score = closer (converts distances to a comparable score).
pub fn score(metric: Metric, a: &[f32], b: &[f32]) -> f32 {
    match metric {
        Metric::Cosine => cosine(a, b),
        Metric::Dot => dot(a, b),
        // Score = negative distance so larger is nearer.
        Metric::Euclidean => -euclidean(a, b),
    }
}

pub fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let (mut ab, mut aa, mut bb) = (0.0f64, 0.0f64, 0.0f64);
    for (x, y) in a.iter().zip(b) {
        ab += (*x as f64) * (*y as f64);
        aa += (*x as f64) * (*x as f64);
        bb += (*y as f64) * (*y as f64);
    }
    if aa == 0.0 || bb == 0.0 {
        return 0.0;
    }
    (ab / (aa.sqrt() * bb.sqrt())) as f32
}

pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

pub fn euclidean(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| (x - y) * (x - y))
        .sum::<f32>()
        .sqrt()
}

// ---- storage codec (§10.3 groundwork) -----------------------------------

/// Encoding tags — the stored format version, so future quantised
/// representations coexist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    F32 = 0,
    F16 = 1,  // IEEE half — groundwork, decodes to f32 for scoring
    Int8 = 2, // scalar quantised — scale/offset in the header
}

/// A stored vector: tag + payload (raw f32 LE, f16 LE, or int8 + f32
/// scale/offset).
#[derive(Debug, Clone)]
pub struct EncodedVector {
    pub dims: u16,
    pub encoding: Encoding,
    /// int8 only.
    pub scale: f32,
    pub offset: f32,
    pub payload: Vec<u8>,
}

impl EncodedVector {
    /// Reference encoding (§10.3 — start with Float32).
    pub fn from_f32(v: &[f32]) -> Self {
        let mut payload = Vec::with_capacity(v.len() * 4);
        for f in v {
            payload.extend_from_slice(&f.to_le_bytes());
        }
        Self {
            dims: v.len() as u16,
            encoding: Encoding::F32,
            scale: 0.0,
            offset: 0.0,
            payload,
        }
    }

    /// Half-precision encoding — halves storage at ~1e-3 relative error.
    pub fn from_f32_f16(v: &[f32]) -> Self {
        let mut payload = Vec::with_capacity(v.len() * 2);
        for &f in v {
            payload.extend_from_slice(&f32_to_f16_bits(f).to_le_bytes());
        }
        Self {
            dims: v.len() as u16,
            encoding: Encoding::F16,
            scale: 0.0,
            offset: 0.0,
            payload,
        }
    }

    /// Scalar-quantised encoding — min/max map to int8.
    pub fn from_f32_int8(v: &[f32]) -> Self {
        let (min, max) = v
            .iter()
            .fold((f32::MAX, f32::MIN), |(mn, mx), &f| (mn.min(f), mx.max(f)));
        let scale = if max > min { (max - min) / 255.0 } else { 1.0 };
        let mut payload = Vec::with_capacity(v.len());
        for &f in v {
            payload.push(((f - min) / scale).round().clamp(0.0, 255.0) as u8);
        }
        Self {
            dims: v.len() as u16,
            encoding: Encoding::Int8,
            scale,
            offset: min,
            payload,
        }
    }

    /// Decodes to f32 — all metrics score against decoded values.
    pub fn decode(&self) -> Vec<f32> {
        match self.encoding {
            Encoding::F32 => self
                .payload
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                .collect(),
            Encoding::F16 => self
                .payload
                .chunks_exact(2)
                .map(|c| f16_bits_to_f32(u16::from_le_bytes(c.try_into().unwrap())))
                .collect(),
            Encoding::Int8 => self
                .payload
                .iter()
                .map(|&q| self.offset + q as f32 * self.scale)
                .collect(),
        }
    }

    /// Serialised form stored under `VD{doc_id}` keys.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(11 + self.payload.len());
        out.extend_from_slice(&self.dims.to_le_bytes());
        out.push(self.encoding as u8);
        out.extend_from_slice(&self.scale.to_le_bytes());
        out.extend_from_slice(&self.offset.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn from_bytes(b: &[u8]) -> ndb_common::Result<Self> {
        if b.len() < 11 {
            return Err(Error::Corruption("vector: truncated".into()));
        }
        let dims = u16::from_le_bytes(b[0..2].try_into().unwrap());
        let encoding = match b[2] {
            0 => Encoding::F32,
            1 => Encoding::F16,
            2 => Encoding::Int8,
            t => return Err(Error::Corruption(format!("vector: bad tag {t}"))),
        };
        let scale = f32::from_le_bytes(b[3..7].try_into().unwrap());
        let offset = f32::from_le_bytes(b[7..11].try_into().unwrap());
        Ok(Self {
            dims,
            encoding,
            scale,
            offset,
            payload: b[11..].to_vec(),
        })
    }
}

// IEEE-754 half conversions — small self-contained impl.
fn f32_to_f16_bits(f: f32) -> u16 {
    let b = f.to_bits();
    let sign = ((b >> 16) & 0x8000) as u16;
    let exp = ((b >> 23) & 0xff) as i32 - 127 + 15;
    let mant = (b & 0x7fffff) >> 13;
    if exp <= 0 {
        sign // underflow → ±0
    } else if exp >= 31 {
        sign | 0x7c00 // overflow → ±inf
    } else {
        sign | ((exp as u16) << 10) | mant as u16
    }
}

fn f16_bits_to_f32(h: u16) -> f32 {
    let sign = ((h & 0x8000) as u32) << 16;
    let exp = ((h >> 10) & 0x1f) as i32;
    let mant = (h & 0x3ff) as u32;
    if exp == 0 {
        return f32::from_bits(sign | (mant << 13)); // subnormal approx
    }
    if exp == 31 {
        return f32::from_bits(sign | 0x7f800000 | (mant << 13));
    }
    f32::from_bits(sign | (((exp - 15 + 127) as u32) << 23) | (mant << 13))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances() {
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-6);
        assert!((dot(&[1.0, 2.0], &[3.0, 4.0]) - 11.0).abs() < 1e-6);
        assert!((euclidean(&[0.0, 0.0], &[3.0, 4.0]) - 5.0).abs() < 1e-6);
    }

    #[test]
    fn codec_roundtrips() {
        let v = vec![0.1, -2.5, 3.75, 1e-4];
        assert_eq!(EncodedVector::from_f32(&v).decode(), v);
        let h = EncodedVector::from_f32_f16(&v).decode();
        for (a, b) in v.iter().zip(&h) {
            assert!((a - b).abs() < 1e-2, "{a} vs {b}");
        }
        let q = EncodedVector::from_f32_int8(&v).decode();
        for (a, b) in v.iter().zip(&q) {
            assert!((a - b).abs() < 0.05, "{a} vs {b}");
        }
        // byte roundtrip
        let e = EncodedVector::from_f32_f16(&v);
        assert_eq!(
            EncodedVector::from_bytes(&e.to_bytes()).unwrap().decode(),
            h
        );
    }
}
