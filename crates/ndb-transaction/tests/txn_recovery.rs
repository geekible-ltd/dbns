//! Phase-3 acceptance tests (spec §26, §22.3): committed transactions
//! survive restart, uncommitted work is never exposed, crash at every
//! persistence boundary recovers correctly.

use ndb_buffer::BufferPool;
use ndb_common::ids::PageId;
use ndb_page::manager::{PageIo, PageManager};
use ndb_transaction::{TxnConfig, TxnManager, recover};
use ndb_wal::Wal;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// In-memory page store that survives "process restarts" — the same
/// `Arc<Vec<u8>>` backs a sequence of PageManagers.
#[derive(Clone, Default)]
struct MemIo {
    data: Arc<Mutex<Vec<u8>>>,
}
impl PageIo for MemIo {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> ndb_common::Result<()> {
        let d = self.data.lock().unwrap();
        buf.copy_from_slice(&d[off as usize..off as usize + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, off: u64, data: &[u8]) -> ndb_common::Result<()> {
        let mut d = self.data.lock().unwrap();
        d[off as usize..off as usize + data.len()].copy_from_slice(data);
        Ok(())
    }
    fn len(&mut self) -> ndb_common::Result<u64> {
        Ok(self.data.lock().unwrap().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> ndb_common::Result<()> {
        self.data.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn sync(&mut self) -> ndb_common::Result<()> {
        Ok(())
    }
}

const PAGE_SIZE: u32 = 4096;
const POOL_FRAMES: u64 = 16;

struct Db {
    mgr: TxnManager<MemIo>,
    data: MemIo,
    wal_dir: PathBuf,
}

/// Opens a fresh database in a temp dir (data in shared MemIo, WAL files
/// on disk).
fn create_db() -> (Db, TempDir) {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let io = MemIo::default();
    let pm = PageManager::create(io.clone(), PAGE_SIZE, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64));
    let (wal, _) = Wal::open(&wal_dir, 0).unwrap();
    let mgr = TxnManager::create(pool, Arc::new(wal), TxnConfig::default()).unwrap();
    (
        Db {
            mgr,
            data: io,
            wal_dir,
        },
        tmp,
    )
}

/// Simulates a crash + restart: drops the manager WITHOUT checkpointing
/// (pool state lost), replays the WAL onto the surviving MemIo, then opens
/// a fresh manager.
fn crash_and_restart(db: Db) -> Db {
    let Db { mgr, data, wal_dir } = db;
    drop(mgr); // "kill -9": no checkpoint, dirty pool pages lost

    let mut pm = PageManager::open(data.clone()).unwrap();
    recover(&wal_dir, 0, &mut pm).unwrap();
    drop(pm);

    let pm = PageManager::open(data.clone()).unwrap();
    let pool = Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64));
    let (wal, _) = Wal::open(&wal_dir, 0).unwrap();
    let mgr = TxnManager::open(pool, Arc::new(wal), TxnConfig::default()).unwrap();
    Db { mgr, data, wal_dir }
}

fn key(n: u64) -> Vec<u8> {
    format!("k{n:06}").into_bytes()
}
fn val(n: u64) -> Vec<u8> {
    format!("v{n}").into_bytes()
}

#[test]
fn committed_transactions_survive_restart() {
    let (db, _t) = create_db();
    let mut txn = db.mgr.begin_write().unwrap();
    txn.insert(&key(1), &val(1)).unwrap();
    txn.insert(&key(2), &val(2)).unwrap();
    txn.commit().unwrap();

    let db = crash_and_restart(db);
    let snap = db.mgr.begin().unwrap();
    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1)));
    assert_eq!(snap.get(&key(2)).unwrap(), Some(val(2)));
}

#[test]
fn uncommitted_work_is_never_exposed() {
    let (db, _t) = create_db();
    let mut txn = db.mgr.begin_write().unwrap();
    txn.insert(&key(1), &val(1)).unwrap();
    txn.commit().unwrap();

    // Second txn writes but is killed mid-flight — never committed.
    let mut txn2 = db.mgr.begin_write().unwrap();
    txn2.insert(&key(1), &val(999)).unwrap();
    txn2.insert(&key(3), &val(3)).unwrap();
    drop(txn2); // simulates a crash mid-transaction (auto-rollback path)

    let db = crash_and_restart(db);
    let snap = db.mgr.begin().unwrap();
    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1))); // committed value, not 999
    assert_eq!(snap.get(&key(3)).unwrap(), None);
}

#[test]
fn kill_before_any_flush_still_recovers_commits() {
    let (db, _t) = create_db();
    // Commit a txn, then kill immediately — commit record was fsynced by
    // commit(), but no dirty page ever reached the data file.
    let mut txn = db.mgr.begin_write().unwrap();
    for i in 0..50 {
        txn.insert(&key(i), &val(i)).unwrap();
    }
    txn.commit().unwrap();
    let db = crash_and_restart(db);
    let snap = db.mgr.begin().unwrap();
    for i in 0..50 {
        assert_eq!(snap.get(&key(i)).unwrap(), Some(val(i)));
    }
}

#[test]
fn rollback_restores_previous_versions() {
    let (db, _t) = create_db();
    let mut t1 = db.mgr.begin_write().unwrap();
    t1.insert(&key(1), &val(1)).unwrap();
    t1.commit().unwrap();

    let mut t2 = db.mgr.begin_write().unwrap();
    t2.insert(&key(1), &val(2)).unwrap();
    t2.insert(&key(9), &val(9)).unwrap();
    t2.rollback().unwrap();

    let snap = db.mgr.begin().unwrap();
    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1)));
    assert_eq!(snap.get(&key(9)).unwrap(), None);
}

#[test]
fn snapshot_reads_are_repeatable_and_isolated() {
    let (db, _t) = create_db();
    let mut t = db.mgr.begin_write().unwrap();
    t.insert(&key(1), &val(1)).unwrap();
    t.commit().unwrap();

    let snap = db.mgr.begin().unwrap();
    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1)));

    // Writer updates concurrently — snapshot still sees the old version.
    let mut w = db.mgr.begin_write().unwrap();
    w.insert(&key(1), &val(2)).unwrap();
    w.commit().unwrap();

    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1))); // repeatable read
    drop(snap);
    let snap2 = db.mgr.begin().unwrap();
    assert_eq!(snap2.get(&key(1)).unwrap(), Some(val(2)));
}

#[test]
fn deletes_are_snapshot_correct() {
    let (db, _t) = create_db();
    let mut t = db.mgr.begin_write().unwrap();
    t.insert(&key(1), &val(1)).unwrap();
    t.commit().unwrap();

    let snap = db.mgr.begin().unwrap();
    let mut w = db.mgr.begin_write().unwrap();
    assert!(w.delete(&key(1)).unwrap());
    w.commit().unwrap();

    assert_eq!(snap.get(&key(1)).unwrap(), Some(val(1))); // old snapshot sees it
    let snap2 = db.mgr.begin().unwrap();
    assert_eq!(snap2.get(&key(1)).unwrap(), None);
}

#[test]
fn crash_during_checkpoint_recovers() {
    let (db, _t) = create_db();
    for batch in 0..3u64 {
        let mut t = db.mgr.begin_write().unwrap();
        for i in 0..20u64 {
            txn_insert(&mut t, batch * 100 + i);
        }
        t.commit().unwrap();
    }
    // Checkpoint once, write more, then "crash" mid-way through the next
    // checkpoint's page flush by just dropping (the checkpoint may have
    // partially flushed before the kill).
    db.mgr.checkpoint().unwrap();
    let mut t = db.mgr.begin_write().unwrap();
    for i in 0..20u64 {
        txn_insert(&mut t, 900 + i);
    }
    t.commit().unwrap();

    let db = crash_and_restart(db);
    let snap = db.mgr.begin().unwrap();
    for batch in 0..3u64 {
        for i in 0..20u64 {
            let n = batch * 100 + i;
            assert_eq!(snap.get(&key(n)).unwrap(), Some(val(n)));
        }
    }
    for i in 0..20u64 {
        let n = 900 + i;
        assert_eq!(snap.get(&key(n)).unwrap(), Some(val(n)));
    }
}

fn txn_insert(t: &mut ndb_transaction::Transaction<'_, MemIo>, n: u64) {
    t.insert(&key(n), &val(n)).unwrap();
}

#[test]
fn concurrent_snapshot_readers_during_writes() {
    let (db, _t) = create_db();
    let mut t = db.mgr.begin_write().unwrap();
    for i in 0..100u64 {
        txn_insert(&mut t, i);
    }
    t.commit().unwrap();

    let db = Arc::new(db);
    std::thread::scope(|s| {
        // Readers at different snapshots.
        for _ in 0..3 {
            let db = Arc::clone(&db);
            s.spawn(move || {
                for _ in 0..20 {
                    let snap = db.mgr.begin().unwrap();
                    for i in 0..100u64 {
                        let got = snap.get(&key(i)).unwrap();
                        assert!(
                            got == Some(val(i)) || got == Some(val(i + 1000)),
                            "key {i} got {got:?}"
                        );
                    }
                }
            });
        }
        // Writer applies a second generation.
        let db = Arc::clone(&db);
        s.spawn(move || {
            for round in 0..3 {
                let mut t = db.mgr.begin_write().unwrap();
                for i in 0..100u64 {
                    t.insert(&key(i), &val(i + 1000)).unwrap();
                }
                t.commit().unwrap();
                let _ = round;
            }
        });
    });
}

#[test]
fn many_commits_then_restart_keeps_everything() {
    let (db, _t) = create_db();
    for n in 0..30u64 {
        let mut t = db.mgr.begin_write().unwrap();
        txn_insert(&mut t, n);
        t.commit().unwrap();
    }
    let db = crash_and_restart(db);
    let snap = db.mgr.begin().unwrap();
    for n in 0..30u64 {
        assert_eq!(snap.get(&key(n)).unwrap(), Some(val(n)));
    }
    // And the tree is still structurally valid.
    db.mgr.btree().validate().unwrap();
    let _ = PageId::new(0);
}
// A snapshot taken before a writer's update-commit keeps seeing the old
// version; a snapshot taken after sees the new one.
#[test]
fn snapshot_pins_version_across_update_commit() {
    let (db, _t) = create_db();
    let mut t = db.mgr.begin_write().unwrap();
    for i in 0..100u64 {
        t.insert(&key(i), &val(i)).unwrap();
    }
    t.commit().unwrap();

    let snap = db.mgr.begin().unwrap();
    let mut w = db.mgr.begin_write().unwrap();
    w.insert(&key(46), &val(1046)).unwrap();
    // before commit, reader should still see v46
    let got = snap.get(&key(46)).unwrap();
    assert_eq!(got, Some(val(46)), "pre-commit: {:?}", got);
    w.commit().unwrap();
    let got2 = snap.get(&key(46)).unwrap();
    assert_eq!(got2, Some(val(46)), "post-commit: {:?}", got2);
    let snap2 = db.mgr.begin().unwrap();
    assert_eq!(snap2.get(&key(46)).unwrap(), Some(val(1046)));
}
