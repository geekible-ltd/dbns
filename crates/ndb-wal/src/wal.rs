//! The write-ahead log: append, flush, rotate, retire, recover
//! (spec §5.5–5.6, ADR-006).
//!
//! - `Wal` appends framed [`WalRecord`]s to the tail segment. `append`
//!   returns the assigned LSN; `flush`/`flush_to` fsync — the commit path
//!   must flush before acknowledging (spec §5.5: never acknowledge a commit
//!   before its durability conditions are satisfied).
//! - On `open`, the recovery scanner walks all segments in order, validates
//!   each record's CRC, truncates a torn tail, and hands the surviving
//!   records to the caller.
//! - `record_checkpoint`/`retire_before` implement segment retirement once a
//!   checkpoint makes older records obsolete (spec §5.6).

use crate::record::WalRecord;
use crate::segment::{DEFAULT_SEGMENT_SIZE, Segment, list_segments, read_records_region};

use ndb_common::error::Error;
use ndb_common::ids::{Lsn, TransactionId};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Records returned by a recovery scan, in append order, plus the LSN to
/// hand out next.
pub struct RecoveredLog {
    pub records: Vec<ScannedRecord>,
    /// Next unused LSN (last assigned + 1, or 1 for an empty log).
    pub next_lsn: u64,
    /// Highest LSN checkpointed in the surviving log, if any.
    pub checkpoint_lsn: Option<Lsn>,
}

/// One record as recovered: the record plus its LSN and owning txn.
#[derive(Debug, Clone)]
pub struct ScannedRecord {
    pub record: WalRecord,
    pub lsn: Lsn,
    pub txn: TransactionId,
    /// Segment sequence the record was read from.
    pub segment: u64,
}

/// The append-only write-ahead log.
pub struct Wal {
    inner: Mutex<Inner>,
    dir: PathBuf,
    segment_size: u64,
}

struct Inner {
    tail: Segment,
    next_lsn: u64,
    /// Highest LSN known durable (fsynced).
    durable_lsn: u64,
    /// Maximum LSN per segment — drives checkpoint-based retirement.
    segment_max: std::collections::BTreeMap<u64, u64>,
}

impl Wal {
    /// Opens (or creates) the WAL in `dir` with a segment rotation size.
    ///
    /// Performs a recovery scan first: any torn tail (partial record from a
    /// crash mid-append) is truncated before appending resumes, and the
    /// surviving records are returned for the caller to replay.
    pub fn open(dir: &Path, segment_size: u64) -> ndb_common::Result<(Self, RecoveredLog)> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Error::Storage(format!("create wal dir {}: {e}", dir.display())))?;
        let segments = list_segments(dir)?;

        // Scan every segment; track the last fully-valid byte offset per
        // segment and the first segment containing a torn/corrupt tail.
        let mut records = Vec::new();
        let mut max_lsn = 0u64;
        let mut checkpoint_lsn = None;
        let mut segment_max = std::collections::BTreeMap::new();
        let mut torn: Option<(u64, PathBuf, u64)> = None; // (seq, path, valid_len)

        for (seq, path) in &segments {
            let region = read_records_region(path)?;
            let mut off = 0usize;
            loop {
                match WalRecord::decode(&region[off..]) {
                    Ok(Some((record, lsn, txn, used))) => {
                        if let WalRecord::Checkpoint { lsn: c } = &record {
                            checkpoint_lsn = Some(*c);
                        }
                        max_lsn = max_lsn.max(lsn.get());
                        segment_max
                            .entry(*seq)
                            .and_modify(|m: &mut u64| *m = (*m).max(lsn.get()))
                            .or_insert(lsn.get());
                        records.push(ScannedRecord {
                            record,
                            lsn,
                            txn,
                            segment: *seq,
                        });
                        off += used;
                    }
                    // Incomplete tail — normal EOF or a torn append.
                    Ok(None) => break,
                    // Corruption mid-segment — treat as end-of-log: a crash
                    // can tear the tail, but anything after corruption is
                    // untrustworthy anyway.
                    Err(Error::Corruption(_)) => break,
                    Err(e) => return Err(e),
                }
            }
            if off < region.len() {
                torn = Some((*seq, path.clone(), off as u64));
                break; // ignore any later segments after a torn one
            }
        }

        // Re-open the tail segment for appending (or create the first one).
        let mut tail = match segments.last() {
            Some((seq, _)) => Segment::open_append(dir, *seq)?,
            None => Segment::create(dir, 1)?,
        };
        // Truncate a torn tail if it is in the tail segment.
        if let Some((seq, _, valid)) = torn
            && seq == tail.seq
        {
            tail.truncate(crate::segment::SEGMENT_HEADER as u64 + valid)?;
        }

        let wal = Wal {
            inner: Mutex::new(Inner {
                tail,
                next_lsn: max_lsn + 1,
                durable_lsn: max_lsn, // scanned content is on disk
                segment_max,
            }),
            dir: dir.to_path_buf(),
            segment_size: if segment_size == 0 {
                DEFAULT_SEGMENT_SIZE
            } else {
                segment_size
            },
        };
        Ok((
            wal,
            RecoveredLog {
                records,
                next_lsn: max_lsn + 1,
                checkpoint_lsn,
            },
        ))
    }

    /// Appends `record` under `txn` and returns its LSN. Buffered — call
    /// [`Wal::flush`] before acknowledging durability.
    pub fn append(&self, txn: TransactionId, record: WalRecord) -> ndb_common::Result<Lsn> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner()) /* poison-recover */;
        let lsn = inner.next_lsn;
        let bytes = record.encode(Lsn::new(lsn), txn);
        inner.tail.append(&bytes)?;
        inner.next_lsn += 1;
        let tail_seq = inner.tail.seq;
        inner
            .segment_max
            .entry(tail_seq)
            .and_modify(|m: &mut u64| *m = (*m).max(lsn))
            .or_insert(lsn);
        // Rotate when the tail crosses the segment size.
        if inner.tail.len >= self.segment_size {
            inner.tail.sync()?;
            inner.tail = Segment::create(&self.dir, inner.tail.seq + 1)?;
        }
        Ok(Lsn::new(lsn))
    }

    /// fsyncs the tail segment and returns the highest durable LSN.
    pub fn flush(&self) -> ndb_common::Result<Lsn> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner()) /* poison-recover */;
        inner.tail.sync()?;
        inner.durable_lsn = inner.next_lsn - 1;
        Ok(Lsn::new(inner.durable_lsn))
    }

    /// Ensures `lsn` is durable — flushes if needed, no-ops if already
    /// durable. The buffer pool's flush gate calls this before writing a
    /// dirty page stamped with `lsn` (WAL-before-data rule).
    pub fn flush_to(&self, lsn: Lsn) -> ndb_common::Result<()> {
        if lsn.get()
            <= self
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner()) /* poison-recover */
                .durable_lsn
        {
            return Ok(());
        }
        self.flush()?;
        Ok(())
    }

    /// Current highest durable LSN.
    pub fn durable_lsn(&self) -> Lsn {
        Lsn::new(
            self.inner
                .lock()
                .unwrap_or_else(|e| e.into_inner()) /* poison-recover */
                .durable_lsn,
        )
    }

    /// Deletes segments whose records all precede the checkpoint LSN. Safe
    /// because every page update at or below the checkpoint has been synced
    /// to the data file (spec §5.6).
    pub fn retire_before(&self, checkpoint_lsn: Lsn) -> ndb_common::Result<usize> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner()) /* poison-recover */;
        let tail_seq = inner.tail.seq;
        let mut removed = 0;
        for (seq, path) in list_segments(&self.dir)? {
            if seq == tail_seq {
                continue; // never retire the live tail
            }
            let max = inner.segment_max.get(&seq).copied();
            if let Some(max) = max
                && max < checkpoint_lsn.get()
            {
                std::fs::remove_file(&path).map_err(|e| {
                    Error::Storage(format!("retire wal segment {}: {e}", path.display()))
                })?;
                inner.segment_max.remove(&seq);
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndb_common::ids::PageId;
    use tempfile::tempdir;

    fn update(i: u8) -> WalRecord {
        WalRecord::PageUpdate {
            page_id: PageId::new(i as u64),
            before: vec![i; 4],
            after: vec![i + 1; 4],
        }
    }

    #[test]
    fn append_flush_recover() {
        let dir = tempdir().unwrap();
        let (wal, rec) = Wal::open(dir.path(), 0).unwrap();
        assert!(rec.records.is_empty());
        let t = TransactionId::new(1);
        let l1 = wal.append(t, update(1)).unwrap();
        let l2 = wal.append(t, WalRecord::Commit).unwrap();
        assert_eq!(l1.get() + 1, l2.get());
        wal.flush().unwrap();
        drop(wal);

        let (_, rec) = Wal::open(dir.path(), 0).unwrap();
        assert_eq!(rec.records.len(), 2);
        assert_eq!(rec.records[0].lsn, l1);
        assert_eq!(rec.records[1].record, WalRecord::Commit);
        assert_eq!(rec.next_lsn, l2.get() + 1);
    }

    #[test]
    fn torn_tail_is_truncated_and_ignored() {
        let dir = tempdir().unwrap();
        let (wal, _) = Wal::open(dir.path(), 0).unwrap();
        let t = TransactionId::new(1);
        wal.append(t, update(1)).unwrap();
        wal.append(t, update(2)).unwrap();
        wal.flush().unwrap();
        // Simulate a torn write: append raw garbage to the segment file.
        let seg_path = Segment::path(dir.path(), 1);
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        use std::io::Write;
        f.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        f.sync_data().unwrap();
        drop(wal);

        let (wal, rec) = Wal::open(dir.path(), 0).unwrap();
        assert_eq!(rec.records.len(), 2); // garbage ignored
        // File was truncated back to the valid end.
        assert_eq!(std::fs::metadata(&seg_path).unwrap().len(), {
            let region = read_records_region(&seg_path).unwrap();
            crate::segment::SEGMENT_HEADER as u64 + region.len() as u64
        });
        // Appends resume on top of the truncated tail.
        wal.append(t, update(3)).unwrap();
        wal.flush().unwrap();
    }

    #[test]
    fn rotation_creates_new_segments() {
        let dir = tempdir().unwrap();
        // Tiny segment size forces rotation after ~2 records.
        let (wal, _) = Wal::open(dir.path(), 100).unwrap();
        let t = TransactionId::new(1);
        for i in 0..10u8 {
            wal.append(t, update(i)).unwrap();
        }
        wal.flush().unwrap();
        assert!(list_segments(dir.path()).unwrap().len() > 1);
        drop(wal);
        // Recovery reads across all segments.
        let (_, rec) = Wal::open(dir.path(), 0).unwrap();
        assert_eq!(rec.records.len(), 10);
    }

    #[test]
    fn retire_before_removes_old_segments() {
        let dir = tempdir().unwrap();
        let (wal, _) = Wal::open(dir.path(), 60).unwrap();
        let t = TransactionId::new(1);
        for i in 0..10u8 {
            wal.append(t, update(i)).unwrap();
        }
        wal.append(t, WalRecord::Checkpoint { lsn: Lsn::new(5) })
            .unwrap();
        wal.flush().unwrap();
        let removed = wal.retire_before(Lsn::new(u64::MAX)).unwrap();
        assert!(removed > 0);
    }
}
