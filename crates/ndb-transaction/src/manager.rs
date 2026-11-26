//! Transaction manager: lifecycle, MVCC snapshots, checkpointing and
//! crash recovery (spec §5.6, §5.7, §7.1 — ADR-006/007).
//!
//! - **Writers are serialized** behind `write_lock` for Phase 3 (ADR-007):
//!   at most one write transaction is active, so page-level journal images
//!   and logical (version-level) rollback are both safe. Readers take
//!   snapshots and never block writers.
//! - **MVCC** uses [`VersionedValue`] chains inside B+Tree leaf values —
//!   snapshot reads resolve versions `<= snapshot_seq`; a GC horizon is the
//!   oldest open snapshot's sequence (expired snapshots stop pinning).
//! - **Durability**: the commit record and WAL flush happen before commit
//!   is acknowledged (spec §5.5); dirty pages only reach the data file after
//!   the WAL position covering them is durable (the pool's flush gate).
//! - **Recovery** (`recover`) replays the WAL: undo first-images of
//!   uncommitted transactions, redo last-images of committed ones.

use crate::journal::{Journal, SYSTEM_TXN};
use crate::versioned::VersionedValue;
use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::error::Error;
use ndb_common::ids::{CommitSequence, Lsn, PageId, TransactionId};
use ndb_page::manager::{PageIo, PageManager};
use ndb_page::page::PageType;
use ndb_wal::{Wal, WalRecord};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// The catalog page — the first page allocated by [`TxnManager::create`]
/// (allocation starts at page 1 since page 0 is the file meta page). Its
/// body stores the B+Tree root pointer and the commit-sequence counter,
/// journaled like any other page.
pub const CATALOG_PAGE: PageId = PageId::new(1);

/// Tunables for the transaction manager (spec §25 shape).
#[derive(Debug, Clone)]
pub struct TxnConfig {
    /// How long a snapshot may stay open before it stops pinning versions
    /// and its reads fail with `SnapshotExpired` (spec §5.7).
    pub snapshot_timeout: Duration,
    /// WAL segment rotation threshold in bytes (0 = default 64 MiB).
    pub wal_segment_size: u64,
}
impl Default for TxnConfig {
    fn default() -> Self {
        Self {
            snapshot_timeout: Duration::from_secs(300),
            wal_segment_size: 0,
        }
    }
}

struct Snapshot {
    seq: CommitSequence,
    deadline: Instant,
}

/// Coordinates transactions over the B+Tree, WAL and buffer pool.
pub struct TxnManager<P: PageIo> {
    wal: Arc<Wal>,
    journal: Arc<Journal>,
    btree: Arc<BTree<P>>,
    pool: Arc<BufferPool<P>>,
    write_lock: Mutex<()>,
    next_txn: AtomicU64,
    commit_seq: AtomicU64,
    snapshots: Mutex<HashMap<TransactionId, Snapshot>>,
    config: TxnConfig,
}

impl<P: PageIo> TxnManager<P> {
    /// Creates a fresh database: allocates the catalog page, creates the
    /// B+Tree and installs journaling on the pool.
    pub fn create(
        pool: Arc<BufferPool<P>>,
        wal: Arc<Wal>,
        config: TxnConfig,
    ) -> ndb_common::Result<Self> {
        let journal = Journal::new(Arc::clone(&wal));
        pool.set_page_hook(Some(journal.hook()));
        {
            let wal2 = Arc::clone(&wal);
            pool.set_flush_gate(Some(Arc::new(move |lsn| wal2.flush_to(Lsn::new(lsn)))));
        }
        // First allocation must land on CATALOG_PAGE.
        let catalog = pool.allocate_page(PageType::Meta)?;
        if catalog != CATALOG_PAGE {
            return Err(Error::Internal(format!(
                "catalog page allocated as {catalog}, expected {CATALOG_PAGE}"
            )));
        }
        let btree = Arc::new(BTree::create(Arc::clone(&pool))?);
        let mgr = Self {
            wal,
            journal,
            btree,
            pool,
            write_lock: Mutex::new(()),
            next_txn: AtomicU64::new(1),
            commit_seq: AtomicU64::new(0),
            snapshots: Mutex::new(HashMap::new()),
            config,
        };
        mgr.write_catalog(0)?;
        Ok(mgr)
    }

    /// Opens an existing database: reads the catalog, reopens the B+Tree
    /// root and restores the commit counter.
    pub fn open(
        pool: Arc<BufferPool<P>>,
        wal: Arc<Wal>,
        config: TxnConfig,
    ) -> ndb_common::Result<Self> {
        let journal = Journal::new(Arc::clone(&wal));
        pool.set_page_hook(Some(journal.hook()));
        {
            let wal2 = Arc::clone(&wal);
            pool.set_flush_gate(Some(Arc::new(move |lsn| wal2.flush_to(Lsn::new(lsn)))));
        }
        let (root, seq) = {
            let g = pool.fetch_read(CATALOG_PAGE)?;
            let b = g.page().body();
            (
                PageId::new(u64::from_le_bytes(b[0..8].try_into().unwrap())),
                u64::from_le_bytes(b[8..16].try_into().unwrap()),
            )
        };
        let btree = Arc::new(BTree::open(Arc::clone(&pool), root)?);
        Ok(Self {
            wal,
            journal,
            btree,
            pool,
            write_lock: Mutex::new(()),
            next_txn: AtomicU64::new(1),
            commit_seq: AtomicU64::new(seq),
            snapshots: Mutex::new(HashMap::new()),
            config,
        })
    }

    /// Persists the catalog: B+Tree root + a commit sequence. Journaled
    /// under the active transaction (or system when none).
    fn write_catalog(&self, seq: CommitSequence) -> ndb_common::Result<()> {
        let mut g = self.pool.fetch_write(CATALOG_PAGE)?;
        let b = g.page_mut().body_mut();
        b[0..8].copy_from_slice(&self.btree.root_id().get().to_le_bytes());
        b[8..16].copy_from_slice(&seq.to_le_bytes());
        self.journal.check_err()
    }

    /// Begins a read-only snapshot transaction.
    pub fn begin(&self) -> ndb_common::Result<Transaction<'_, P>> {
        let id = self.alloc_txn();
        let seq = self.register_snapshot(id);
        Ok(Transaction {
            mgr: self,
            id,
            snapshot_seq: seq,
            write_guard: None,
            write_set: Vec::new(),
            done: false,
            deadline: Instant::now() + self.config.snapshot_timeout,
        })
    }

    /// Begins a write transaction — serialized: blocks until any current
    /// writer finishes (ADR-007 Phase-3 policy).
    pub fn begin_write(&self) -> ndb_common::Result<Transaction<'_, P>> {
        let guard = self.write_lock.lock().unwrap();
        let id = self.alloc_txn();
        let seq = self.register_snapshot(id);
        self.journal.set_active(Some(id));
        Ok(Transaction {
            mgr: self,
            id,
            snapshot_seq: seq,
            write_guard: Some(guard),
            write_set: Vec::new(),
            done: false,
            deadline: Instant::now() + self.config.snapshot_timeout,
        })
    }

    fn alloc_txn(&self) -> TransactionId {
        TransactionId::new(self.next_txn.fetch_add(1, Ordering::SeqCst))
    }

    /// Registers a snapshot and returns its sequence — the seq read and
    /// the registration happen under one lock so a concurrent commit's GC
    /// horizon can never exclude a snapshot it already promised (i.e.
    /// readers either pin the horizon or see the post-commit state).
    fn register_snapshot(&self, id: TransactionId) -> CommitSequence {
        let mut snaps = self.snapshots.lock().unwrap();
        let seq = self.commit_seq.load(Ordering::SeqCst);
        snaps.insert(
            id,
            Snapshot {
                seq,
                deadline: Instant::now() + self.config.snapshot_timeout,
            },
        );
        seq
    }

    fn unregister(&self, id: TransactionId) {
        self.snapshots.lock().unwrap().remove(&id);
    }

    /// The GC horizon computed inside the snapshots lock: the oldest live
    /// snapshot's sequence, or the current commit sequence when none are
    /// open (spec §5.7). Callers must already hold the lock — it is held
    /// for the whole commit stamping loop so no snapshot can register a
    /// sequence below the horizon mid-commit.
    fn gc_horizon_locked(
        snaps: &HashMap<TransactionId, Snapshot>,
        commit_seq: CommitSequence,
    ) -> CommitSequence {
        let now = Instant::now();
        snaps
            .values()
            .filter(|s| s.deadline > now)
            .map(|s| s.seq)
            .min()
            .unwrap_or(commit_seq)
    }

    /// Age of the oldest open snapshot (spec §5.7 — monitoring old
    /// snapshots), or `None`. The earliest deadline belongs to the
    /// longest-running snapshot.
    pub fn oldest_snapshot_age(&self) -> Option<Duration> {
        let snaps = self.snapshots.lock().unwrap();
        let now = Instant::now();
        snaps
            .values()
            .map(|s| {
                self.config
                    .snapshot_timeout
                    .saturating_sub(s.deadline.saturating_duration_since(now))
            })
            .max()
    }

    /// Checkpoint (spec §5.6): quiesces writers, records the boundary,
    /// flushes dirty pages (each gated by WAL-before-data), syncs the data
    /// file, then retires fully-covered WAL segments.
    pub fn checkpoint(&self) -> ndb_common::Result<()> {
        let _w = self.write_lock.lock().unwrap();
        let durable = self.wal.durable_lsn();
        self.wal
            .append(SYSTEM_TXN, WalRecord::Checkpoint { lsn: durable })?;
        self.wal.flush()?;
        self.pool.flush_all()?; // gated: WAL flush_to(page.lsn) per dirty page
        self.wal.retire_before(durable)?;
        Ok(())
    }

    /// The underlying tree — for tests and read-only scans.
    pub fn btree(&self) -> &Arc<BTree<P>> {
        &self.btree
    }

    /// The WAL — exposed for recovery tooling and tests.
    pub fn wal(&self) -> &Arc<Wal> {
        &self.wal
    }
}

/// An active transaction. Dropping a live *write* transaction rolls it
/// back (best-effort).
pub struct Transaction<'m, P: PageIo> {
    mgr: &'m TxnManager<P>,
    id: TransactionId,
    snapshot_seq: CommitSequence,
    write_guard: Option<MutexGuard<'m, ()>>,
    write_set: Vec<Vec<u8>>,
    done: bool,
    deadline: Instant,
}

impl<'m, P: PageIo> Transaction<'m, P> {
    /// Transaction id.
    pub fn id(&self) -> TransactionId {
        self.id
    }
    fn is_writer(&self) -> bool {
        self.write_guard.is_some()
    }
    fn check_live(&self) -> ndb_common::Result<()> {
        if Instant::now() > self.deadline {
            return Err(Error::InvalidInput(
                "snapshot expired — transaction timed out".into(),
            ));
        }
        Ok(())
    }

    /// Reads `key` resolved at this transaction's snapshot sequence.
    pub fn get(&self, key: &[u8]) -> ndb_common::Result<Option<Vec<u8>>> {
        self.check_live()?;
        let raw = match self.mgr.btree.get(key)? {
            Some(v) => v,
            None => return Ok(None),
        };
        let vv = VersionedValue::decode(&raw)?;
        self.mgr.journal.check_err()?;
        Ok(vv.resolve(self.id, self.snapshot_seq))
    }

    /// Inserts or replaces `key` → `val` (write txns only).
    pub fn insert(&mut self, key: &[u8], val: &[u8]) -> ndb_common::Result<()> {
        self.require_writer()?;
        let mut vv = self.read_vv(key)?;
        vv.write(self.id, Some(val.to_vec()));
        self.mgr.btree.insert(key, &vv.encode())?;
        self.mgr.journal.check_err()?;
        self.write_set.push(key.to_vec());
        Ok(())
    }

    /// Deletes `key` at this transaction's snapshot (write txns only).
    pub fn delete(&mut self, key: &[u8]) -> ndb_common::Result<bool> {
        self.require_writer()?;
        let mut vv = self.read_vv(key)?;
        if vv.resolve(self.id, self.snapshot_seq).is_none() {
            return Ok(false);
        }
        vv.write(self.id, None); // tombstone
        self.mgr.btree.insert(key, &vv.encode())?;
        self.mgr.journal.check_err()?;
        self.write_set.push(key.to_vec());
        Ok(true)
    }

    fn require_writer(&self) -> ndb_common::Result<()> {
        if !self.is_writer() {
            return Err(Error::InvalidInput("read-only transaction".into()));
        }
        self.check_live()
    }

    /// Raw unresolved version chain for `key` (empty when absent).
    fn read_vv(&self, key: &[u8]) -> ndb_common::Result<VersionedValue> {
        match self.mgr.btree.get(key)? {
            Some(raw) => VersionedValue::decode(&raw),
            None => Ok(VersionedValue::default()),
        }
    }

    /// Commits: stamps this txn's versions with a new commit sequence (with
    /// inline GC), updates the catalog, appends the commit record and
    /// fsyncs the WAL — in that order, so an acknowledged commit is always
    /// durable (spec §5.5).
    pub fn commit(mut self) -> ndb_common::Result<()> {
        self.finish(true)
    }

    /// Rolls back: logically removes every version this txn wrote, records
    /// the abort, and releases the writer slot.
    pub fn rollback(mut self) -> ndb_common::Result<()> {
        self.finish(false)
    }

    fn finish(&mut self, commit: bool) -> ndb_common::Result<()> {
        self.done = true;
        self.mgr.unregister(self.id);
        if !self.is_writer() {
            return Ok(());
        }
        let result = if commit {
            self.commit_inner()
        } else {
            self.rollback_inner()
        };
        self.mgr.journal.set_active(None);
        self.write_guard = None; // releases the writer slot
        result
    }

    fn commit_inner(&mut self) -> ndb_common::Result<()> {
        // Writers are serialized, so load+1 is a unique commit sequence.
        // The counter is only stored after stamping + WAL flush, making the
        // commit atomically visible to new snapshots (ADR-007).
        let seq = self.mgr.commit_seq.load(Ordering::SeqCst) + 1;
        {
            // Hold the snapshots lock for the whole commit: a snapshot
            // registered before the lock counts toward the GC horizon; one
            // registered after sees the stored seq — a reader can never
            // land between (a stale seq on already-GC'd chains). Stamped
            // versions are only resolvable at seq, stored last, so commit
            // is atomically visible (ADR-007).
            let snaps = self.mgr.snapshots.lock().unwrap();
            let horizon = TxnManager::<P>::gc_horizon_locked(
                &snaps,
                self.mgr.commit_seq.load(Ordering::SeqCst),
            );
            for key in std::mem::take(&mut self.write_set) {
                let mut vv = self.read_vv(&key)?;
                vv.stamp_commit(self.id, seq);
                let dead = vv.gc(horizon);
                if dead || vv.0.is_empty() {
                    self.mgr.btree.delete(&key)?;
                } else {
                    self.mgr.btree.insert(&key, &vv.encode())?;
                }
            }
            self.mgr.write_catalog(seq)?;
            self.mgr.journal.check_err()?;
            self.mgr.wal.append(self.id, WalRecord::Commit)?;
            self.mgr.wal.flush()?;
            // Durable: make the commit visible to new snapshots.
            self.mgr.commit_seq.store(seq, Ordering::SeqCst);
        }
        Ok(())
    }

    fn rollback_inner(&mut self) -> ndb_common::Result<()> {
        // Logical undo: remove this txn's uncommitted version from every
        // touched key; delete keys left with no versions at all.
        for key in std::mem::take(&mut self.write_set) {
            let mut vv = self.read_vv(&key)?;
            vv.remove_uncommitted(self.id);
            if vv.0.is_empty() {
                self.mgr.btree.delete(&key)?;
            } else {
                self.mgr.btree.insert(&key, &vv.encode())?;
            }
        }
        self.mgr.journal.check_err()?;
        self.mgr.wal.append(self.id, WalRecord::Abort)?;
        self.mgr.wal.flush()?;
        Ok(())
    }
}

impl<P: PageIo> Drop for Transaction<'_, P> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.finish(false);
        }
    }
}

/// Crash recovery (spec §5.5, §22.3, ADR-006). Replays the WAL:
///
/// 1. Collect the committed transaction set (`Commit` records; transaction
///    0 — system — is always committed).
/// 2. **Undo**: for every `PageUpdate` by an uncommitted or aborted
///    transaction, restore the FIRST before-image per page (earliest wins).
/// 3. **Redo**: for every `PageUpdate` by a committed transaction, apply
///    the LAST after-image per page.
///
/// Images are full pages so replay is idempotent; a final checkpoint bounds
/// future scans.
pub fn recover<P: PageIo>(
    wal_dir: &Path,
    segment_size: u64,
    mgr: &mut PageManager<P>,
) -> ndb_common::Result<RecoveryReport> {
    let (wal, rec) = Wal::open(wal_dir, segment_size)?;

    let mut committed: std::collections::HashSet<u64> = [SYSTEM_TXN.get()].into_iter().collect();
    for r in &rec.records {
        if let WalRecord::Commit = r.record {
            committed.insert(r.txn.get());
        }
    }

    let mut undo: HashMap<PageId, Vec<u8>> = HashMap::new();
    let mut redo: HashMap<PageId, Vec<u8>> = HashMap::new();
    for r in &rec.records {
        if let WalRecord::PageUpdate {
            page_id,
            before,
            after,
        } = &r.record
        {
            if committed.contains(&r.txn.get()) {
                redo.insert(*page_id, after.clone()); // last wins
            } else {
                undo.entry(*page_id).or_insert_with(|| before.clone()); // first wins
            }
        }
    }

    let mut report = RecoveryReport::default();
    for (id, before) in undo {
        mgr.restore_page(id, &before)?;
        report.pages_undone += 1;
    }
    for (id, after) in redo {
        mgr.restore_page(id, &after)?;
        report.pages_redone += 1;
    }
    mgr.sync()?;

    // Record a checkpoint covering everything replayed so the next open
    // starts fast and old segments can be retired.
    if rec.next_lsn > 1 {
        wal.append(
            SYSTEM_TXN,
            WalRecord::Checkpoint {
                lsn: Lsn::new(rec.next_lsn - 1),
            },
        )?;
        wal.flush()?;
        report.segments_retired = wal.retire_before(Lsn::new(rec.next_lsn - 1))?;
    }
    Ok(report)
}

/// What recovery did.
#[derive(Debug, Default)]
pub struct RecoveryReport {
    pub pages_undone: usize,
    pub pages_redone: usize,
    pub segments_retired: usize,
}
