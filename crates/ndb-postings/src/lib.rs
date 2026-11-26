//! Posting-list block codec (spec §9.4).
//!
//! A term's postings are stored as a chain of **chunks** — page-bounded,
//! immutable-after-seal blocks containing delta-encoded doc ids plus
//! per-doc term frequency and delta-encoded positions:
//!
//! ```text
//! chunk  := magic u8 | count u16 | last_doc u64 | max_tf u16 | payload
//! entry  := doc_delta varint | tf varint | pos_count varint | pos_deltas varint*
//! ```
//!
//! - **Delta encoding** keeps doc ids small (append order is sorted since
//!   doc ids allocate monotonically).
//! - **Variable-byte** varints everywhere (spec §9.4).
//! - **`max_tf`/`last_doc`** are the block-max/skip metadata Block-Max
//!   WAND needs to prune chunks without decoding them (§9.4).
//! - Chunking IS the segment mechanism: appends touch only the tail
//!   chunk, compaction merges chunks.

use ndb_common::error::Error;

const MAGIC: u8 = 0xB1;
/// Postings bytes a chunk can hold — sized so a chunk fits one page body
/// beside the page header (page body ≈ 4 KB; keep margin).
pub const CHUNK_CAPACITY: usize = 3900;

/// One (doc, tf, positions) posting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub doc_id: u64,
    pub tf: u32,
    pub positions: Vec<u32>,
}

/// A sealed-or-growing block of postings, doc-id sorted.
#[derive(Debug, Clone)]
pub struct Chunk {
    /// Postings in ascending doc_id order.
    pub postings: Vec<Posting>,
}

impl Chunk {
    pub fn new() -> Self {
        Self {
            postings: Vec::new(),
        }
    }

    pub fn last_doc(&self) -> u64 {
        self.postings.last().map(|p| p.doc_id).unwrap_or(0)
    }

    /// Block-max term frequency (§9.4 block-max metadata).
    pub fn max_tf(&self) -> u32 {
        self.postings.iter().map(|p| p.tf).max().unwrap_or(0)
    }

    /// Encoded size if `postings` were written — used to decide whether a
    /// new posting still fits the tail chunk.
    pub fn encoded_len(&self) -> usize {
        let mut n = 13; // magic + count + last_doc + max_tf
        let mut prev = 0u64;
        for p in &self.postings {
            n += varint_len(p.doc_id - prev);
            prev = p.doc_id;
            n += varint_len(p.tf as u64);
            n += varint_len(p.positions.len() as u64);
            let mut pp = 0u64;
            for &pos in &p.positions {
                n += varint_len(pos as u64 - pp);
                pp = pos as u64;
            }
        }
        n
    }

    /// Serialise — always rewrites the header so `max_tf`/`last_doc` stay
    /// honest for block-max pruning.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.push(MAGIC);
        out.extend_from_slice(&(self.postings.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.last_doc().to_le_bytes());
        out.extend_from_slice(&(self.max_tf() as u16).to_le_bytes());
        let mut prev = 0u64;
        for p in &self.postings {
            push_varint(&mut out, p.doc_id - prev);
            prev = p.doc_id;
            push_varint(&mut out, p.tf as u64);
            push_varint(&mut out, p.positions.len() as u64);
            let mut pp = 0u64;
            for &pos in &p.positions {
                push_varint(&mut out, pos as u64 - pp);
                pp = pos as u64;
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> ndb_common::Result<Chunk> {
        if buf.len() < 13 || buf[0] != MAGIC {
            return Err(Error::Corruption("chunk: bad header".into()));
        }
        let count = u16::from_le_bytes(buf[1..3].try_into().unwrap()) as usize;
        let mut r = Reader { buf, pos: 13 };
        let mut postings = Vec::with_capacity(count);
        let mut prev = 0u64;
        for _ in 0..count {
            let doc_id = prev
                .checked_add(r.varint()?)
                .ok_or_else(|| Error::Corruption("chunk: docid overflow".into()))?;
            prev = doc_id;
            let tf = r.varint()? as u32;
            let npos = r.varint()? as usize;
            let mut positions = Vec::with_capacity(npos);
            let mut pp = 0u64;
            for _ in 0..npos {
                let pos = pp + r.varint()?;
                positions.push(pos as u32);
                pp = pos;
            }
            postings.push(Posting {
                doc_id,
                tf,
                positions,
            });
        }
        Ok(Chunk { postings })
    }
}

impl Default for Chunk {
    fn default() -> Self {
        Self::new()
    }
}

/// Append `p` if it fits — caller allocates a new chunk on `false`.
/// `p.doc_id` must be ≥ last doc id (monotonic append preserves order).
pub fn try_append(c: &mut Chunk, p: Posting) -> bool {
    let mut trial = c.clone();
    trial.postings.push(p);
    if trial.encoded_len() > CHUNK_CAPACITY {
        return false;
    }
    *c = trial;
    true
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

pub fn push_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl Reader<'_> {
    fn varint(&mut self) -> ndb_common::Result<u64> {
        let mut v = 0u64;
        let mut shift = 0u32;
        loop {
            if self.pos >= self.buf.len() {
                return Err(Error::Corruption("varint: truncated".into()));
            }
            let b = self.buf[self.pos];
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
            shift += 7;
            if shift >= 64 {
                return Err(Error::Corruption("varint: overflow".into()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn roundtrip() {
        let mut c = Chunk::new();
        let mut d = 1u64;
        for i in 0..50 {
            d += i;
            let positions = (0..(i % 5 + 1)).map(|j| j as u32 * 3 + i as u32).collect();
            assert!(try_append(
                &mut c,
                Posting {
                    doc_id: d,
                    tf: i as u32 + 1,
                    positions
                }
            ));
        }
        let got = Chunk::decode(&c.encode()).unwrap();
        assert_eq!(got.postings, c.postings);
        assert_eq!(got.max_tf(), 50);
        assert_eq!(got.last_doc(), d);
    }

    #[test]
    fn chunk_capacity_enforced() {
        let mut c = Chunk::new();
        let mut n = 0;
        while try_append(
            &mut c,
            Posting {
                doc_id: n + 1,
                tf: 1,
                positions: vec![1, 5, 9],
            },
        ) {
            n += 1;
        }
        assert!(c.encoded_len() <= CHUNK_CAPACITY);
        assert!(n > 100); // sanity: hundreds of postings per chunk
    }

    proptest! {
        #[test]
        fn codec_oracle(docs in proptest::collection::vec(1u64..1_000_000, 1..200)) {
            let mut sorted = docs;
            sorted.sort_unstable();
            sorted.dedup();
            let mut c = Chunk::new();
            let mut kept = Vec::new();
            for d in sorted {
                let p = Posting { doc_id: d, tf: 3, positions: vec![0, 4] };
                if try_append(&mut c, p.clone()) { kept.push(p); } else { break; }
            }
            let got = Chunk::decode(&c.encode()).unwrap();
            prop_assert_eq!(got.postings, kept);
        }
    }
}
