//! MVCC version chains stored inside B+Tree leaf values (spec §5.7,
//! ADR-007).
//!
//! A leaf value is a `VersionedValue`: a newest-first list of versions.
//! Each version carries a tag identifying who wrote it — either a live
//! [`TransactionId`] (uncommitted, visible only to its owner) or a
//! [`CommitSequence`] assigned at commit — plus the value bytes, or a
//! tombstone for deletes.
//!
//! ```text
//! u8 count | repeated count times, newest first:
//!   u8  flags   bit0 = tombstone
//!   u64 tag     uncommitted: txn_id | committed: commit_seq (tag < 2^63; the
//!               high bit distinguishes txn ids, see TXN_BIT)
//!   u32 len     0 for tombstones
//!   len bytes   value
//! ```
//!
//! Snapshot reads resolve the newest version visible at their sequence:
//! committed versions with `seq <= snapshot_seq`, or uncommitted versions
//! owned by the reading transaction.

use ndb_common::error::Error;
use ndb_common::ids::{CommitSequence, TransactionId};

const TXN_BIT: u64 = 1 << 63;
const FLAG_TOMBSTONE: u8 = 1;

/// One entry in a version chain.
#[derive(Debug, Clone, PartialEq)]
pub enum Version {
    /// Uncommitted version owned by a live transaction.
    Uncommitted {
        txn: TransactionId,
        data: Option<Vec<u8>>,
    },
    /// Committed version stamped with the txn's commit sequence.
    Committed {
        seq: CommitSequence,
        data: Option<Vec<u8>>,
    },
}

impl Version {
    pub fn is_tombstone(&self) -> bool {
        match self {
            Version::Uncommitted { data, .. } | Version::Committed { data, .. } => data.is_none(),
        }
    }
}

/// A newest-first MVCC version chain — the leaf value encoding.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VersionedValue(pub Vec<Version>);

impl VersionedValue {
    /// Resolves the value visible at `snapshot_seq` for reader `reader`.
    /// Returns `None` for "no visible value" (never written or tombstoned).
    pub fn resolve(&self, reader: TransactionId, snapshot_seq: CommitSequence) -> Option<Vec<u8>> {
        for v in &self.0 {
            let visible = match v {
                Version::Uncommitted { txn, .. } => *txn == reader,
                Version::Committed { seq, .. } => *seq <= snapshot_seq,
            };
            if visible {
                return match v {
                    Version::Uncommitted { data, .. } | Version::Committed { data, .. } => {
                        data.clone()
                    }
                };
            }
        }
        None
    }

    /// Prepends or replaces the calling txn's uncommitted version.
    pub fn write(&mut self, txn: TransactionId, data: Option<Vec<u8>>) {
        if let Some(Version::Uncommitted { txn: t, .. }) = self.0.first()
            && *t == txn
        {
            self.0[0] = Version::Uncommitted { txn, data };
            return;
        }
        self.0.insert(0, Version::Uncommitted { txn, data });
    }

    /// Stamps the txn's uncommitted version with `seq` at commit. No-op when
    /// the chain's head belongs to another txn (shouldn't happen under the
    /// serialized-writer policy).
    pub fn stamp_commit(&mut self, txn: TransactionId, seq: CommitSequence) {
        if let Some(Version::Uncommitted { txn: t, data }) = self.0.first()
            && *t == txn
        {
            self.0[0] = Version::Committed {
                seq,
                data: data.clone(),
            };
        }
    }

    /// Drops the txn's uncommitted version (rollback).
    pub fn remove_uncommitted(&mut self, txn: TransactionId) {
        self.0
            .retain(|v| !matches!(v, Version::Uncommitted { txn: t, .. } if *t == txn));
    }

    /// Garbage collection (spec §5.7 — safe version GC): keeps every version
    /// with `seq > horizon`, plus the newest version with `seq <= horizon`
    /// *if it carries data* (a boundary tombstone resolves to "absent"
    /// either way, so it and everything older can go). Returns true if the
    /// key should be deleted entirely.
    pub fn gc(&mut self, horizon: CommitSequence) -> bool {
        let mut kept_boundary = false;
        let mut out = Vec::with_capacity(self.0.len());
        for v in self.0.drain(..) {
            match v {
                Version::Uncommitted { .. } => out.push(v),
                Version::Committed { seq, data } => {
                    if seq > horizon {
                        out.push(Version::Committed { seq, data });
                    } else if !kept_boundary {
                        kept_boundary = true;
                        if let Some(d) = data {
                            out.push(Version::Committed { seq, data: Some(d) });
                        }
                    }
                }
            }
        }
        self.0 = out;
        self.0.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(self.0.len() as u8);
        for v in &self.0 {
            match v {
                Version::Uncommitted { txn, data } => {
                    write_version(&mut out, txn.get() | TXN_BIT, data);
                }
                Version::Committed { seq, data } => {
                    write_version(&mut out, *seq, data);
                }
            }
        }
        out
    }

    pub fn decode(buf: &[u8]) -> ndb_common::Result<Self> {
        let mut r = Reader { buf, pos: 0 };
        let count = r.u8()? as usize;
        let mut versions = Vec::with_capacity(count);
        for _ in 0..count {
            let flags = r.u8()?;
            let tag = r.u64()?;
            let len = r.u32()? as usize;
            let data = if flags & FLAG_TOMBSTONE != 0 {
                if len != 0 {
                    return Err(Error::Corruption(
                        "versioned value: tombstone with data".into(),
                    ));
                }
                None
            } else {
                Some(r.bytes(len)?.to_vec())
            };
            versions.push(if tag & TXN_BIT != 0 {
                Version::Uncommitted {
                    txn: TransactionId::new(tag & !TXN_BIT),
                    data,
                }
            } else {
                Version::Committed { seq: tag, data }
            });
        }
        if r.pos != buf.len() {
            return Err(Error::Corruption("versioned value: trailing bytes".into()));
        }
        Ok(VersionedValue(versions))
    }
}

fn write_version(out: &mut Vec<u8>, tag: u64, data: &Option<Vec<u8>>) {
    match data {
        Some(d) => {
            out.push(0);
            out.extend_from_slice(&tag.to_le_bytes());
            out.extend_from_slice(&(d.len() as u32).to_le_bytes());
            out.extend_from_slice(d);
        }
        None => {
            out.push(FLAG_TOMBSTONE);
            out.extend_from_slice(&tag.to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
        }
    }
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    fn u8(&mut self) -> ndb_common::Result<u8> {
        Ok(*self
            .bytes(1)?
            .first()
            .ok_or_else(|| Error::Corruption("versioned value: eof".into()))?)
    }
    fn u32(&mut self) -> ndb_common::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> ndb_common::Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
    fn bytes(&mut self, n: usize) -> ndb_common::Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(Error::Corruption("versioned value: truncated".into()));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let v = VersionedValue(vec![
            Version::Uncommitted {
                txn: TransactionId::new(9),
                data: Some(b"new".to_vec()),
            },
            Version::Committed {
                seq: 5,
                data: Some(b"old".to_vec()),
            },
            Version::Committed { seq: 2, data: None },
        ]);
        assert_eq!(VersionedValue::decode(&v.encode()).unwrap(), v);
    }

    #[test]
    fn resolution_respects_snapshot() {
        let t = TransactionId::new(9);
        let v = VersionedValue(vec![
            Version::Committed {
                seq: 10,
                data: Some(b"v3".to_vec()),
            },
            Version::Committed {
                seq: 5,
                data: Some(b"v2".to_vec()),
            },
            Version::Committed {
                seq: 1,
                data: Some(b"v1".to_vec()),
            },
        ]);
        assert_eq!(v.resolve(t, 12), Some(b"v3".to_vec()));
        assert_eq!(v.resolve(t, 7), Some(b"v2".to_vec()));
        assert_eq!(v.resolve(t, 1), Some(b"v1".to_vec()));
        assert_eq!(v.resolve(t, 0), None);
    }

    #[test]
    fn uncommitted_visible_only_to_owner() {
        let owner = TransactionId::new(1);
        let other = TransactionId::new(2);
        let v = VersionedValue(vec![
            Version::Uncommitted {
                txn: owner,
                data: Some(b"pending".to_vec()),
            },
            Version::Committed {
                seq: 3,
                data: Some(b"stable".to_vec()),
            },
        ]);
        assert_eq!(v.resolve(owner, 100), Some(b"pending".to_vec()));
        assert_eq!(v.resolve(other, 100), Some(b"stable".to_vec()));
        // Transaction 0 = snapshot readers never see uncommitted data.
        assert_eq!(
            v.resolve(TransactionId::new(0), 100),
            Some(b"stable".to_vec())
        );
    }

    #[test]
    fn gc_keeps_horizon_boundary() {
        let mut v = VersionedValue(vec![
            Version::Committed {
                seq: 9,
                data: Some(b"c".to_vec()),
            },
            Version::Committed {
                seq: 7,
                data: Some(b"b".to_vec()),
            },
            Version::Committed {
                seq: 3,
                data: Some(b"a".to_vec()),
            },
            Version::Committed {
                seq: 1,
                data: Some(b"z".to_vec()),
            },
        ]);
        // Horizon 6: keep seq 9 (>6) and seq 3 (newest ≤6); drop seq 1.
        // seq 7 > 6 keeps too.
        assert!(!v.gc(6));
        let seqs: Vec<CommitSequence> =
            v.0.iter()
                .map(|v| match v {
                    Version::Committed { seq, .. } => *seq,
                    _ => 0,
                })
                .collect();
        assert_eq!(seqs, vec![9, 7, 3]);
    }

    #[test]
    fn gc_tombstone_collapse() {
        let mut v = VersionedValue(vec![Version::Committed { seq: 2, data: None }]);
        assert!(v.gc(10)); // sole tombstone below horizon → delete key
        let mut v2 = VersionedValue(vec![
            Version::Committed {
                seq: 20,
                data: Some(b"x".to_vec()),
            },
            Version::Committed { seq: 2, data: None },
        ]);
        assert!(!v2.gc(10));
        assert_eq!(v2.0.len(), 1); // tombstone dropped — not needed
    }
}
