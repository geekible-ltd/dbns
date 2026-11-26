//! Oracle test (spec §22.2 — randomized model checking): every generated
//! operation sequence is replayed against a `BTreeMap`, and the tree's
//! structural invariants are re-validated after each batch.

use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::ids::PageId;
use ndb_page::manager::{PageIo, PageManager};
use proptest::prelude::*;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MemIo {
    data: Mutex<Vec<u8>>,
}
impl PageIo for MemIo {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> ndb_common::Result<()> {
        let d = self.data.get_mut().unwrap();
        buf.copy_from_slice(&d[off as usize..off as usize + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, off: u64, data: &[u8]) -> ndb_common::Result<()> {
        let d = self.data.get_mut().unwrap();
        d[off as usize..off as usize + data.len()].copy_from_slice(data);
        Ok(())
    }
    fn len(&mut self) -> ndb_common::Result<u64> {
        Ok(self.data.get_mut().unwrap().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> ndb_common::Result<()> {
        self.data.get_mut().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn sync(&mut self) -> ndb_common::Result<()> {
        Ok(())
    }
}

fn tree() -> BTree<MemIo> {
    // Small 4 KiB pages force frequent splits and merges.
    let mgr = PageManager::create(MemIo::default(), 4096, 8).unwrap();
    let pool = Arc::new(BufferPool::new(mgr, 64 * 4096));
    BTree::create(pool).unwrap()
}

#[derive(Debug, Clone)]
enum Op {
    Insert(u16, u8),
    Delete(u16),
}

fn ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        prop_oneof![
            3 => (0u16..600, any::<u8>()).prop_map(|(k, v)| Op::Insert(k, v)),
            1 => (0u16..600u16).prop_map(Op::Delete),
        ],
        1..400,
    )
}

fn k(n: u16) -> Vec<u8> {
    format!("{n:05}").into_bytes()
}

proptest! {
    #[test]
    fn matches_btreemap_oracle(seq in ops()) {
        let t = tree();
        let mut model = BTreeMap::new();
        for (i, op) in seq.iter().enumerate() {
            match *op {
                Op::Insert(key, v) => {
                    t.insert(&k(key), &[v]).unwrap();
                    model.insert(k(key), vec![v]);
                }
                Op::Delete(key) => {
                    let was = model.remove(&k(key)).is_some();
                    prop_assert_eq!(t.delete(&k(key)).unwrap(), was);
                }
            }
            // Re-validate structural invariants every 50 ops and on the last op.
            if i % 50 == 49 || i == seq.len() - 1 {
                t.validate().unwrap();
            }
        }
        // Full ordered comparison.
        let actual = t.scan().unwrap();
        let expected: Vec<(Vec<u8>, Vec<u8>)> =
            model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        prop_assert_eq!(actual, expected);
        // Random probes.
        for key in [0u16, 150, 300, 599] {
            prop_assert_eq!(t.get(&k(key)).unwrap(), model.get(&k(key)).cloned());
        }
    }
}

/// Mixed concurrent load: overlapping key ranges so splits and merges
/// contend on shared ancestors.
#[test]
fn concurrent_overlapping_write_read_delete() {
    let t = Arc::new(tree());
    std::thread::scope(|s| {
        for w in 0..4u16 {
            let t = Arc::clone(&t);
            s.spawn(move || {
                for i in 0..200u16 {
                    // Interleaved ranges share interior nodes.
                    let key = w + i * 4;
                    t.insert(&k(key), &w.to_le_bytes()).unwrap();
                }
            });
        }
        for w in 0..2u16 {
            let t = Arc::clone(&t);
            s.spawn(move || {
                for i in 0..50u16 {
                    let key = w * 2 + i * 8;
                    t.delete(&k(key)).unwrap();
                }
            });
        }
        let tr = Arc::clone(&t);
        s.spawn(move || {
            for _ in 0..30 {
                let all = tr.scan().unwrap();
                for w in all.windows(2) {
                    assert!(w[0].0 < w[1].0);
                }
            }
        });
    });
    t.validate().unwrap();
}

/// Deterministic replay of a merge-heavy deletion order across sibling
/// boundaries (regression coverage for the try-latch retry paths).
#[test]
fn alternating_delete_order_exercises_both_merge_paths() {
    let t = tree();
    for i in 0..200u16 {
        t.insert(&k(i), b"v").unwrap();
    }
    // Delete alternately from the front and back of each leaf to trigger
    // both "absorb right sibling" and "unlink rightmost" merges.
    let mut lo = 0u16;
    let mut hi = 199u16;
    while lo <= hi {
        t.delete(&k(lo)).unwrap();
        if lo < hi {
            t.delete(&k(hi)).unwrap();
        }
        lo += 1;
        hi -= 1;
    }
    t.validate().unwrap();
    assert_eq!(t.root_id(), PageId::new(1));
}

/// Millions of operations sustained — spec acceptance criterion.
/// Run explicitly: `cargo test -p ndb-btree --release million_ops -- --ignored`
#[test]
#[ignore]
fn million_ops_sustained() {
    let t = tree();
    for i in 0..1_000_000u64 {
        t.insert(&i.to_be_bytes(), &i.to_le_bytes()).unwrap();
        if i % 100_000 == 0 {
            t.validate().unwrap();
        }
    }
    for i in 0..1_000_000u64 {
        assert_eq!(
            t.get(&i.to_be_bytes()).unwrap(),
            Some(i.to_le_bytes().to_vec())
        );
    }
    t.validate().unwrap();
}
