//! HNSW correctness + recall (spec §26 Phase 7: vector recall benchmarks).

use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::Result;
use ndb_hnsw::{HnswConfig, HnswIndex};
use ndb_page::manager::{PageIo, PageManager};
use ndb_transaction::{TxnConfig, TxnManager};
use ndb_vector::Metric;
use ndb_wal::Wal;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct MemIo {
    data: Arc<Mutex<Vec<u8>>>,
}
impl PageIo for MemIo {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> Result<()> {
        let d = self.data.lock().unwrap();
        buf.copy_from_slice(&d[off as usize..off as usize + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, off: u64, data: &[u8]) -> Result<()> {
        let mut d = self.data.lock().unwrap();
        d[off as usize..off as usize + data.len()].copy_from_slice(data);
        Ok(())
    }
    fn len(&mut self) -> Result<u64> {
        Ok(self.data.lock().unwrap().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> Result<()> {
        self.data.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

struct Env {
    mgr: TxnManager<MemIo>,
    pool: Arc<BufferPool<MemIo>>,
    _tmp: TempDir,
}

fn env() -> Env {
    let tmp = TempDir::new().unwrap();
    let io = MemIo::default();
    let pm = PageManager::create(io, 4096, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, 256 * 4096));
    let (wal, _) = Wal::open(&tmp.path().join("wal"), 0).unwrap();
    let mgr = TxnManager::create(Arc::clone(&pool), Arc::new(wal), TxnConfig::default()).unwrap();
    Env {
        mgr,
        pool,
        _tmp: tmp,
    }
}

/// Deterministic pseudo-vectors — clustered so NN structure exists.
fn gen_vec(seed: u64, dims: usize) -> Vec<f32> {
    let mut x = seed | 1;
    let cluster = (seed % 7) as f32;
    (0..dims)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            cluster + (x % 1000) as f32 / 1000.0
        })
        .collect()
}

fn cfg(dims: u16) -> HnswConfig {
    HnswConfig {
        dims,
        metric: Metric::Euclidean,
        m: 8,
        ef_construction: 60,
        ef_search: 40,
    }
}

#[test]
fn insert_and_exact_search_small() {
    let e = env();
    let tree = Arc::new(BTree::create(Arc::clone(&e.pool)).unwrap());
    let mut t = e.mgr.begin_write().unwrap();
    let idx = HnswIndex::create(Arc::clone(&tree), &mut t, cfg(4)).unwrap();
    for i in 1..=20u64 {
        idx.insert(&mut t, i, &gen_vec(i, 4)).unwrap();
    }
    t.commit().unwrap();

    let t = e.mgr.begin().unwrap();
    let q = gen_vec(5, 4);
    let hits = idx.search(&t, &q, 3, 10, None).unwrap();
    assert_eq!(hits[0].0, 5); // nearest to itself
    assert!(hits.len() == 3);
}

#[test]
fn recall_vs_brute_force() {
    let e = env();
    let tree = Arc::new(BTree::create(Arc::clone(&e.pool)).unwrap());
    let mut t = e.mgr.begin_write().unwrap();
    let idx = HnswIndex::create(Arc::clone(&tree), &mut t, cfg(8)).unwrap();
    const N: u64 = 400;
    for i in 1..=N {
        idx.insert(&mut t, i, &gen_vec(i, 8)).unwrap();
    }
    t.commit().unwrap();

    let t = e.mgr.begin().unwrap();
    let k = 10;
    let mut hits_total = 0;
    let queries = 20;
    for qs in 0..queries {
        let q = gen_vec(1000 + qs, 8);
        let approx: std::collections::HashSet<u64> = idx
            .search(&t, &q, k, 50, None)
            .unwrap()
            .into_iter()
            .map(|h| h.0)
            .collect();
        let exact: std::collections::HashSet<u64> = idx
            .brute_force(&t, &q, k)
            .unwrap()
            .into_iter()
            .map(|h| h.0)
            .collect();
        hits_total += approx.intersection(&exact).count();
    }
    let recall = hits_total as f64 / (queries * k as u64) as f64;
    assert!(recall >= 0.9, "recall@10 = {recall:.3} < 0.9");
    eprintln!("recall@{k} = {recall:.3}");
}

#[test]
fn delete_tombstones_and_update() {
    let e = env();
    let tree = Arc::new(BTree::create(Arc::clone(&e.pool)).unwrap());
    let mut t = e.mgr.begin_write().unwrap();
    let idx = HnswIndex::create(Arc::clone(&tree), &mut t, cfg(4)).unwrap();
    for i in 1..=30u64 {
        idx.insert(&mut t, i, &gen_vec(i, 4)).unwrap();
    }
    idx.delete(&mut t, 5).unwrap();
    // Update doc 7 to land near doc 5's old position.
    idx.update(&mut t, 7, &gen_vec(5, 4)).unwrap();
    t.commit().unwrap();

    let t = e.mgr.begin().unwrap();
    assert_eq!(idx.count(&t).unwrap(), 29);
    let hits = idx.search(&t, &gen_vec(5, 4), 5, 20, None).unwrap();
    assert!(!hits.iter().any(|h| h.0 == 5), "deleted doc returned");
    assert!(
        hits.iter().any(|h| h.0 == 7),
        "updated doc not near new spot"
    );
}

#[test]
fn filtered_search() {
    let e = env();
    let tree = Arc::new(BTree::create(Arc::clone(&e.pool)).unwrap());
    let mut t = e.mgr.begin_write().unwrap();
    let idx = HnswIndex::create(Arc::clone(&tree), &mut t, cfg(4)).unwrap();
    for i in 1..=60u64 {
        idx.insert(&mut t, i, &gen_vec(i, 4)).unwrap();
    }
    t.commit().unwrap();

    let t = e.mgr.begin().unwrap();
    let q = gen_vec(3, 4);
    let evens = |d: u64| d.is_multiple_of(2);
    let hits = idx.search(&t, &q, 5, 40, Some(&evens)).unwrap();
    assert!(hits.iter().all(|h| h.0.is_multiple_of(2)));
    assert!(!hits.is_empty());
}

#[test]
fn dims_mismatch_rejected() {
    let e = env();
    let tree = Arc::new(BTree::create(Arc::clone(&e.pool)).unwrap());
    let mut t = e.mgr.begin_write().unwrap();
    let idx = HnswIndex::create(Arc::clone(&tree), &mut t, cfg(4)).unwrap();
    assert!(idx.insert(&mut t, 1, &[1.0, 2.0]).is_err());
    idx.insert(&mut t, 1, &[1.0, 2.0, 3.0, 4.0]).unwrap();
    assert!(idx.insert(&mut t, 1, &[1.0; 4]).is_err()); // duplicate
    t.rollback().unwrap();
}
