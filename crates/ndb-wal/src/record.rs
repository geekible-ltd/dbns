//! WAL record wire format (spec §5.5, ADR-006).
//!
//! Every record is framed as:
//!
//! ```text
//! u32 len          — byte count of everything after this field
//! u64 lsn          — global log sequence number
//! u64 txn_id       — owning transaction (0 = system records)
//! u8  record_type
//! payload          — type-specific
//! u32 crc32        — CRC32 of (lsn ‖ txn_id ‖ type ‖ payload)
//! ```
//!
//! Torn-write detection: a trailing record whose declared `len` runs past the
//! end of the file, or whose CRC mismatches, ends the recovery scan — appends
//! are atomic only at record granularity, and a crash can leave a partial
//! tail that must be ignored (spec §5.5).

use ndb_common::error::Error;
use ndb_common::ids::{Lsn, PageId, TransactionId};

pub const RECORD_HEADER: usize = 4 + 8 + 8 + 1; // len + lsn + txn + type
const CRC_LEN: usize = 4;

const TYPE_PAGE_UPDATE: u8 = 1;
const TYPE_COMMIT: u8 = 2;
const TYPE_ABORT: u8 = 3;
const TYPE_CHECKPOINT: u8 = 4;

/// A single WAL record.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
    /// Physiological page change: full before/after page images.
    PageUpdate {
        page_id: PageId,
        before: Vec<u8>,
        after: Vec<u8>,
    },
    /// Transaction commit marker — the commit point for durability.
    Commit,
    /// Transaction abort marker.
    Abort,
    /// Checkpoint: all page updates at or below `lsn` have been synced to the
    /// data file; WAL segments below this point may be retired.
    Checkpoint { lsn: Lsn },
}

impl WalRecord {
    /// Serialized byte length (header + payload + crc, excluding the len
    /// prefix itself is included in the stored `len` field semantics — the
    /// stored length covers lsn..=crc).
    pub fn encode(&self, lsn: Lsn, txn: TransactionId) -> Vec<u8> {
        let payload_len = self.payload_len();
        let mut out = Vec::with_capacity(RECORD_HEADER + payload_len + CRC_LEN);
        let len = (8 + 8 + 1 + payload_len + CRC_LEN) as u32;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&lsn.get().to_le_bytes());
        out.extend_from_slice(&txn.get().to_le_bytes());
        match self {
            WalRecord::PageUpdate {
                page_id,
                before,
                after,
            } => {
                out.push(TYPE_PAGE_UPDATE);
                out.extend_from_slice(&page_id.get().to_le_bytes());
                out.extend_from_slice(&(before.len() as u32).to_le_bytes());
                out.extend_from_slice(&(after.len() as u32).to_le_bytes());
                out.extend_from_slice(before);
                out.extend_from_slice(after);
            }
            WalRecord::Commit => out.push(TYPE_COMMIT),
            WalRecord::Abort => out.push(TYPE_ABORT),
            WalRecord::Checkpoint { lsn: c } => {
                out.push(TYPE_CHECKPOINT);
                out.extend_from_slice(&c.get().to_le_bytes());
            }
        }
        let crc = crc32fast::hash(&out[4..]);
        out.extend_from_slice(&crc.to_le_bytes());
        out
    }

    fn payload_len(&self) -> usize {
        match self {
            WalRecord::PageUpdate { before, after, .. } => 8 + 4 + 4 + before.len() + after.len(),
            WalRecord::Commit | WalRecord::Abort => 0,
            WalRecord::Checkpoint { .. } => 8,
        }
    }

    /// Attempts to decode one record at the start of `buf`.
    ///
    /// Returns `Ok(Some((record, lsn, txn, consumed)))` on success,
    /// `Ok(None)` when `buf` doesn't contain a complete record, and `Err` on
    /// corruption (bad CRC — treated by callers as end-of-log).
    pub fn decode(buf: &[u8]) -> ndb_common::Result<Option<(Self, Lsn, TransactionId, usize)>> {
        if buf.len() < RECORD_HEADER + CRC_LEN {
            return Ok(None);
        }
        let len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let total = 4 + len;
        if buf.len() < total {
            return Ok(None); // incomplete tail
        }
        if len < (8 + 8 + 1 + CRC_LEN) {
            return Err(Error::Corruption("wal record length underflow".into()));
        }
        let frame = &buf[4..total];
        let (body, crc_bytes) = frame.split_at(frame.len() - CRC_LEN);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32fast::hash(body) != stored_crc {
            return Err(Error::Corruption("wal record crc mismatch".into()));
        }
        let lsn = Lsn::new(u64::from_le_bytes(body[0..8].try_into().unwrap()));
        let txn = TransactionId::new(u64::from_le_bytes(body[8..16].try_into().unwrap()));
        let payload = &body[17..];
        let record = match body[16] {
            TYPE_PAGE_UPDATE => {
                let page_id = PageId::new(u64::from_le_bytes(payload[0..8].try_into().unwrap()));
                let blen = u32::from_le_bytes(payload[8..12].try_into().unwrap()) as usize;
                let alen = u32::from_le_bytes(payload[12..16].try_into().unwrap()) as usize;
                if payload.len() != 16 + blen + alen {
                    return Err(Error::Corruption("wal page update length mismatch".into()));
                }
                WalRecord::PageUpdate {
                    page_id,
                    before: payload[16..16 + blen].to_vec(),
                    after: payload[16 + blen..].to_vec(),
                }
            }
            TYPE_COMMIT => WalRecord::Commit,
            TYPE_ABORT => WalRecord::Abort,
            TYPE_CHECKPOINT => WalRecord::Checkpoint {
                lsn: Lsn::new(u64::from_le_bytes(payload[0..8].try_into().unwrap())),
            },
            t => return Err(Error::Corruption(format!("unknown wal record type {t}"))),
        };
        Ok(Some((record, lsn, txn, total)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all_types() {
        let cases = vec![
            WalRecord::PageUpdate {
                page_id: PageId::new(7),
                before: vec![1, 2, 3],
                after: vec![4, 5, 6, 7],
            },
            WalRecord::Commit,
            WalRecord::Abort,
            WalRecord::Checkpoint {
                lsn: Lsn::new(99),
            },
        ];
        for (i, r) in cases.iter().enumerate() {
            let bytes = r.encode(Lsn::new(i as u64 + 1), TransactionId::new(42));
            let (got, lsn, txn, used) = WalRecord::decode(&bytes).unwrap().unwrap();
            assert_eq!(&got, r);
            assert_eq!(lsn.get(), i as u64 + 1);
            assert_eq!(txn.get(), 42);
            assert_eq!(used, bytes.len());
        }
    }

    #[test]
    fn truncated_tail_is_incomplete_not_corrupt() {
        let r = WalRecord::PageUpdate {
            page_id: PageId::new(1),
            before: vec![9; 100],
            after: vec![9; 100],
        };
        let bytes = r.encode(Lsn::new(1), TransactionId::new(1));
        for cut in [3, 10, bytes.len() - 2] {
            assert!(WalRecord::decode(&bytes[..cut]).unwrap().is_none());
        }
    }

    #[test]
    fn corrupted_bytes_detected() {
        let r = WalRecord::Commit;
        let mut bytes = r.encode(Lsn::new(1), TransactionId::new(1));
        let n = bytes.len();
        bytes[n - 6] ^= 0xFF; // corrupt inside the crc-protected region
        assert!(matches!(
            WalRecord::decode(&bytes),
            Err(Error::Corruption(_))
        ));
    }
}
