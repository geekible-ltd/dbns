//! B+Tree with latch-coupled concurrent access (spec §5.3).
//!
//! Locking discipline (pessimistic crabbing):
//!
//! - Readers latch-couple with *read* latches: latch child, then release the
//!   parent.
//! - Writers latch-couple with *write* latches, releasing ancestors once a
//!   child is provably *safe* for the operation (insert: fits without
//!   splitting; delete: won't underflow). Unsafe children keep the whole
//!   ancestor suffix latched so cascades complete atomically.
//! - Sibling latches (needed only when merging the rightmost leaf) are taken
//!   with `try_write`; failure drops all held latches and retries, so no
//!   lock-ordering cycle can form — merge never blocks on an out-of-order
//!   latch.
//!
//! Underflow policy (ADR-005): a node merges when it becomes *empty*
//! (leaf: 0 keys, internal: 0 separators). An empty leaf absorbs its right
//! sibling (which always fits — the right page's contents came from one
//! page); the rightmost leaf is unlinked instead. An empty internal node is
//! spliced out by repointing its parent cell at its single child. When a
//! merge would overflow the target, cells are *redistributed* evenly and the
//! parent separator updated.

use crate::node::{MAX_KEY_SIZE, internal, is_internal, is_leaf, key_count, leaf};
use ndb_buffer::{BufferPool, PageWriteGuard};
use ndb_common::error::Error;
use ndb_common::ids::PageId;
use ndb_page::manager::PageIo;
use ndb_page::page::PageType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const MAX_LATCH_RETRIES: usize = 64;

fn corruption(msg: impl Into<String>) -> Error {
    Error::Corruption(msg.into())
}

/// A B+Tree over a shared buffer pool. Safe to share across threads —
/// all state is interior-mutable.
pub struct BTree<P: PageIo> {
    pool: Arc<BufferPool<P>>,
    root: AtomicU64,
    /// Pages a merge unlinked but could not free because a reader held them
    /// pinned — retried on every operation.
    pending_frees: Mutex<Vec<PageId>>,
}

impl<P: PageIo> BTree<P> {
    /// Creates a new empty tree backed by a fresh leaf root page.
    pub fn create(pool: Arc<BufferPool<P>>) -> ndb_common::Result<Self> {
        let root = pool.allocate_page(PageType::BTreeLeaf)?;
        {
            let mut g = pool.fetch_write(root)?;
            leaf::init(g.page_mut().body_mut());
        }
        Ok(Self {
            pool,
            root: AtomicU64::new(root.get()),
            pending_frees: Mutex::new(Vec::new()),
        })
    }

    /// Opens an existing tree by root page id.
    pub fn open(pool: Arc<BufferPool<P>>, root: PageId) -> ndb_common::Result<Self> {
        let g = pool.fetch_read(root)?;
        let b = g.page().body();
        if !is_leaf(b) && !is_internal(b) {
            return Err(corruption(format!("page {root} is not a B+Tree node")));
        }
        Ok(Self {
            pool,
            root: AtomicU64::new(root.get()),
            pending_frees: Mutex::new(Vec::new()),
        })
    }

    /// The current root page — changes when the tree grows or shrinks.
    pub fn root_id(&self) -> PageId {
        PageId::new(self.root.load(Ordering::Acquire))
    }
    fn set_root(&self, id: PageId) {
        self.root.store(id.get(), Ordering::Release);
    }

    /// Write-latches the current root page. Between reading the root page id
    /// and acquiring the latch a concurrent split may have installed a new
    /// root, so re-check after latching and retry if the id went stale —
    /// a root can only be replaced while it is write-latched, so once we
    /// hold the latch and the id still matches, it cannot go stale.
    fn latch_root_write(&self) -> ndb_common::Result<PageWriteGuard> {
        loop {
            let rid = self.root_id();
            let g = self.pool.fetch_write(rid)?;
            if self.root_id() == rid {
                return Ok(g);
            }
        }
    }

    /// Same stale-root defence for read descents.
    fn latch_root_read(&self) -> ndb_common::Result<ndb_buffer::PageReadGuard> {
        loop {
            let rid = self.root_id();
            let g = self.pool.fetch_read(rid)?;
            if self.root_id() == rid {
                return Ok(g);
            }
        }
    }

    /// Frees a page, or queues it for a later attempt when a reader still
    /// pins it (e.g. a cursor parked on a just-unlinked leaf).
    fn free_page(&self, id: PageId) {
        if self.pool.free_page(id).is_err() {
            self.pending_frees.lock().unwrap().push(id);
        }
    }

    fn drain_pending_frees(&self) {
        let mut pending = self.pending_frees.lock().unwrap();
        pending.retain(|&id| self.pool.free_page(id).is_err());
    }

    /// Point lookup.
    pub fn get(&self, key: &[u8]) -> ndb_common::Result<Option<Vec<u8>>> {
        let mut g = self.latch_root_read()?;
        loop {
            let b = g.page().body();
            if is_leaf(b) {
                let (g, pos) = self.leaf_scan_to(g, key)?;
                return Ok(match pos {
                    Ok(i) => Some(leaf::val(g.page().body(), i).to_vec()),
                    Err(_) => None,
                });
            }
            let child = internal::find_child(b, key);
            g = self.pool.fetch_read(child)?;
        }
    }

    /// Positions a leaf read-guard on `key`, compensating for concurrent
    /// splits: if `key` sorts past this leaf's end it may live in the right
    /// sibling (the leaf split between our parent's routing and our latch),
    /// so hop right while the next leaf's first key is `<= key`.
    ///
    /// Returns the guard and `leaf::find`'s result in that leaf
    /// (`Ok(idx)` = exact, `Err(pos)` = insertion point).
    fn leaf_scan_to(
        &self,
        mut g: ndb_buffer::PageReadGuard,
        key: &[u8],
    ) -> ndb_common::Result<(ndb_buffer::PageReadGuard, Result<usize, usize>)> {
        loop {
            let end = key_count(g.page().body());
            match leaf::find(g.page().body(), key) {
                Ok(i) => return Ok((g, Ok(i))),
                Err(pos) if pos < end => return Ok((g, Err(pos))),
                Err(_) => {
                    let next = leaf::next(g.page().body());
                    let Ok(ng) = self.pool.fetch_read(next) else {
                        return Ok((g, Err(end))); // right leaf freed mid-merge
                    };
                    if next.get() == 0 || !is_leaf(ng.page().body()) {
                        return Ok((g, Err(end)));
                    }
                    let nb = ng.page().body();
                    if key_count(nb) > 0 && key < leaf::key(nb, 0) {
                        // Key sorts before the right leaf's range — it
                        // can't be further right. Position stays at this
                        // leaf's end.
                        return Ok((g, Err(end)));
                    }
                    g = ng; // key belongs at-or-right: keep hopping
                }
            }
        }
    }

    /// Inserts or replaces `(key, val)`.
    pub fn insert(&self, key: &[u8], val: &[u8]) -> ndb_common::Result<()> {
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::InvalidInput(format!(
                "key exceeds the {MAX_KEY_SIZE}-byte limit"
            )));
        }
        if leaf::needed(key.len(), val.len())
            > self.pool.page_size() as usize - 40 - crate::node::NODE_HEADER
        {
            return Err(Error::InvalidInput(
                "value does not fit in a leaf page".into(),
            ));
        }
        for _ in 0..MAX_LATCH_RETRIES {
            if self.insert_once(key, val)? {
                return Ok(());
            }
        }
        Err(Error::Internal(
            "insert: sibling latch retries exhausted".into(),
        ))
    }

    /// One insert attempt; false means a sibling latch was contended.
    fn insert_once(&self, key: &[u8], val: &[u8]) -> ndb_common::Result<bool> {
        self.drain_pending_frees();
        let mut path: Vec<PageWriteGuard> = Vec::new();
        let mut g = self.latch_root_write()?;
        loop {
            let b = g.page().body();
            if is_leaf(b) {
                break;
            }
            let child_id = internal::find_child(b, key);
            let child = self.pool.fetch_write(child_id)?;
            let cb = child.page().body();
            // Safe = cannot split during this insert. Internal children must
            // absorb a worst-case separator; leaves must fit this cell.
            let safe = if is_internal(cb) {
                internal::fits(cb, MAX_KEY_SIZE)
            } else {
                leaf::fits(cb, key.len(), val.len())
            };
            if safe {
                path.clear();
            }
            path.push(g);
            g = child;
        }

        let node_id = g.page().header()?.page_id;
        let pos = match leaf::find(g.page().body(), key) {
            Ok(idx) => {
                // Remove the stale cell and treat this as a fresh insert at
                // `idx` — the replacement may not fit back in place (a grown
                // value on a full leaf), in which case the split path below
                // handles it. Removing first is safe: the leaf still holds
                // every other key, and `fits`/`split` see the reclaimed space.
                leaf::remove_at(g.page_mut().body_mut(), idx);
                idx
            }
            Err(pos) => pos,
        };
        {
            if leaf::fits(g.page().body(), key.len(), val.len()) {
                leaf::insert_at(g.page_mut().body_mut(), pos, key, val)?;
                return Ok(true);
            }
            // Split the leaf: right page takes the upper half.
            let mut entries = leaf::entries(g.page().body());
            entries.insert(pos, (key.to_vec(), val.to_vec()));
            let mid = entries.len() / 2;
            let right_entries = entries.split_off(mid);
            let right_id = self.pool.allocate_page(PageType::BTreeLeaf)?;
            {
                let mut rg = self.pool.fetch_write(right_id)?;
                let rb = rg.page_mut().body_mut();
                leaf::init(rb);
                leaf::rebuild(rb, &right_entries);
                leaf::set_next(rb, leaf::next(g.page().body()));
            }
            let sep = right_entries[0].0.clone();
            let b = g.page_mut().body_mut();
            leaf::rebuild(b, &entries);
            leaf::set_next(b, right_id);
            self.propagate(path, node_id, sep, right_id)?;
            Ok(true)
        }
    }

    /// Propagates a `(separator, right_page)` pair up the latched ancestor
    /// stack, splitting internal nodes as needed and creating a new root
    /// when the old root splits.
    fn propagate(
        &self,
        mut path: Vec<PageWriteGuard>,
        mut left: PageId,
        mut sep: Vec<u8>,
        mut right: PageId,
    ) -> ndb_common::Result<()> {
        while let Some(mut parent) = path.pop() {
            let pb = parent.page().body();
            if internal::fits(pb, sep.len()) {
                let pos = internal::find_pos(pb, &sep);
                internal::insert_at(parent.page_mut().body_mut(), pos, &sep, right)?;
                return Ok(());
            }
            // Split the internal node at the median separator — the median
            // moves up, it is not kept in either child.
            let parent_id = parent.page().header()?.page_id;
            let leftmost = internal::leftmost(pb);
            let mut entries = internal::entries(pb);
            let pos = internal::find_pos(pb, &sep);
            entries.insert(pos, (sep, right));
            let mid = entries.len() / 2;
            let up = entries[mid].0.clone();
            let right_leftmost = entries[mid].1;
            let right_entries = entries.split_off(mid + 1);
            entries.truncate(mid);

            let right_id = self.pool.allocate_page(PageType::BTreeInternal)?;
            {
                let mut rg = self.pool.fetch_write(right_id)?;
                internal::rebuild(rg.page_mut().body_mut(), right_leftmost, &right_entries);
            }
            internal::rebuild(parent.page_mut().body_mut(), leftmost, &entries);
            left = parent_id;
            sep = up;
            right = right_id;
        }
        // The old root split — create a new internal root above it.
        let root_id = self.pool.allocate_page(PageType::BTreeInternal)?;
        {
            let mut rg = self.pool.fetch_write(root_id)?;
            internal::rebuild(rg.page_mut().body_mut(), left, &[(sep, right)]);
        }
        self.set_root(root_id);
        Ok(())
    }

    /// Deletes `key`. Returns whether it was present.
    pub fn delete(&self, key: &[u8]) -> ndb_common::Result<bool> {
        for _ in 0..MAX_LATCH_RETRIES {
            match self.delete_once(key)? {
                DeleteResult::Done(found) => return Ok(found),
                DeleteResult::Retry => continue,
            }
        }
        Err(Error::Internal(
            "delete: sibling latch retries exhausted".into(),
        ))
    }

    fn delete_once(&self, key: &[u8]) -> ndb_common::Result<DeleteResult> {
        self.drain_pending_frees();
        let mut path: Vec<PageWriteGuard> = Vec::new();
        let mut g = self.latch_root_write()?;
        loop {
            let b = g.page().body();
            if is_leaf(b) {
                break;
            }
            let child_id = internal::find_child(b, key);
            let child = self.pool.fetch_write(child_id)?;
            // Safe = cannot underflow (merge only triggers on empty).
            if key_count(child.page().body()) >= 2 {
                path.clear();
            }
            path.push(g);
            g = child;
        }

        let leaf_id = g.page().header()?.page_id;
        let idx = match leaf::find(g.page().body(), key) {
            Err(_) => return Ok(DeleteResult::Done(false)),
            Ok(i) => i,
        };
        leaf::remove_at(g.page_mut().body_mut(), idx);
        if key_count(g.page().body()) > 0 || path.is_empty() {
            return Ok(DeleteResult::Done(true));
        }
        // The leaf is now empty and has a parent → merge.
        match self.merge_leaf(g, leaf_id, &mut path)? {
            Merge::Done => Ok(DeleteResult::Done(true)),
            Merge::Retry => Ok(DeleteResult::Retry),
        }
    }

    /// Merges the empty leaf `node` away.
    fn merge_leaf(
        &self,
        mut node: PageWriteGuard,
        node_id: PageId,
        path: &mut Vec<PageWriteGuard>,
    ) -> ndb_common::Result<Merge> {
        let mut parent = match path.pop() {
            None => return Ok(Merge::Done), // root leaf stays
            Some(p) => p,
        };
        let (node_idx, n_cells) = {
            let pb = parent.page().body();
            match internal::child_index(pb, node_id) {
                Some(i) => (i, key_count(pb)),
                None => return Err(corruption("delete: leaf not found in parent")),
            }
        };
        if node_idx < n_cells {
            // Right sibling exists: absorb it into `node` (it always fits —
            // node is empty and the right's data came from one page), then
            // unlink the right sibling and drop its cell from the parent.
            let right_id = internal::cell_child(parent.page().body(), node_idx);
            let right = match self.pool.try_fetch_write(right_id)? {
                Some(g) => g,
                None => return Ok(Merge::Retry),
            };
            let (entries, right_next) = {
                let rb = right.page().body();
                if !is_leaf(rb) {
                    return Err(corruption("merge: right sibling is not a leaf"));
                }
                (leaf::entries(rb), leaf::next(rb))
            };
            let nb = node.page_mut().body_mut();
            leaf::rebuild(nb, &entries);
            leaf::set_next(nb, right_next);
            internal::remove_at(parent.page_mut().body_mut(), node_idx);
            drop(right);
            drop(node);
            self.free_page(right_id);
            self.fix_parent_after_loss(path, parent)?;
            Ok(Merge::Done)
        } else if node_idx == 0 {
            // Node is the only child — the parent itself is underfull.
            // Nothing to merge the leaf into; handle the parent's underflow.
            self.fix_parent_after_loss(path, parent)?;
            Ok(Merge::Done)
        } else {
            // Rightmost leaf: unlink it — update the left sibling's `next`
            // and drop node's cell from the parent.
            let pb = parent.page().body();
            let left_id = if node_idx == 1 {
                internal::leftmost(pb)
            } else {
                internal::cell_child(pb, node_idx - 2)
            };
            let mut left = match self.pool.try_fetch_write(left_id)? {
                Some(g) => g,
                None => return Ok(Merge::Retry),
            };
            leaf::set_next(left.page_mut().body_mut(), PageId::new(0));
            internal::remove_at(parent.page_mut().body_mut(), node_idx - 1);
            drop(left);
            drop(node);
            self.free_page(node_id);
            self.fix_parent_after_loss(path, parent)?;
            Ok(Merge::Done)
        }
    }

    /// After a child merge removed a cell, an internal node may have become
    /// empty (0 separators + 1 child) — splice it into its parent by
    /// repointing, or collapse it when it is the root.
    fn fix_parent_after_loss(
        &self,
        path: &mut Vec<PageWriteGuard>,
        parent: PageWriteGuard,
    ) -> ndb_common::Result<()> {
        if key_count(parent.page().body()) > 0 {
            return Ok(());
        }
        let parent_id = parent.page().header()?.page_id;
        let only_child = internal::leftmost(parent.page().body());
        match path.pop() {
            // Parent is the root and has a single child — the tree shrinks.
            None => {
                self.set_root(only_child);
                drop(parent);
                self.free_page(parent_id);
                Ok(())
            }
            Some(mut gp) => {
                let idx = internal::child_index(gp.page().body(), parent_id)
                    .ok_or_else(|| corruption("merge: parent not found in grandparent"))?;
                if idx == 0 {
                    internal::set_leftmost(gp.page_mut().body_mut(), only_child);
                } else {
                    internal::set_cell_child(gp.page_mut().body_mut(), idx - 1, only_child);
                }
                drop(parent);
                self.free_page(parent_id);
                Ok(())
            }
        }
    }

    /// Iterates all `(key, value)` pairs in key order via the leaf chain.
    pub fn scan(&self) -> ndb_common::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for entry in self.cursor()? {
            out.push(entry?);
        }
        Ok(out)
    }

    /// Returns entries in `[start, end)` via a leaf-chain scan.
    pub fn range(&self, start: &[u8], end: &[u8]) -> ndb_common::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for entry in self.seek(start)? {
            let (k, v) = entry?;
            if k.as_slice() >= end {
                break;
            }
            out.push((k, v));
        }
        Ok(out)
    }

    /// A cursor positioned at the first key.
    pub fn cursor(&self) -> ndb_common::Result<crate::cursor::Cursor<P>> {
        self.seek(&[])
    }

    /// A cursor positioned at the first key `>= key`.
    pub fn seek(&self, key: &[u8]) -> ndb_common::Result<crate::cursor::Cursor<P>> {
        let mut g = self.latch_root_read()?;
        loop {
            let b = g.page().body();
            if is_leaf(b) {
                let (g, pos) = self.leaf_scan_to(g, key)?;
                let idx = match pos {
                    Ok(i) | Err(i) => i,
                };
                return Ok(crate::cursor::Cursor::new(Arc::clone(&self.pool), g, idx));
            }
            let child = internal::find_child(b, key);
            g = self.pool.fetch_read(child)?;
        }
    }

    /// Validates structural invariants — used by tests and diagnostics:
    ///
    /// - keys are strictly sorted within and across nodes,
    /// - every child lies within its separator bounds,
    /// - all leaves sit at the same depth,
    /// - the leaf chain traverses every leaf in key order.
    pub fn validate(&self) -> ndb_common::Result<()> {
        // Collect levels: (page_id, lower_bound, upper_bound)
        type Bounds = Option<Vec<u8>>;
        let mut level: Vec<(PageId, Bounds, Bounds)> = vec![(self.root_id(), None, None)];
        let mut depth = 0usize;
        let mut leaf_depth = None;
        let mut leaf_pages = Vec::new();
        while !level.is_empty() {
            let mut next_level = Vec::new();
            for (id, lo, hi) in level {
                let g = self.pool.fetch_read(id)?;
                let b = g.page().body();
                if is_leaf(b) {
                    match leaf_depth {
                        None => leaf_depth = Some(depth),
                        Some(d) if d != depth => {
                            return Err(corruption(format!(
                                "validate: leaves at depths {d} and {depth}"
                            )));
                        }
                        _ => {}
                    }
                    leaf_pages.push(id);
                    let n = key_count(b);
                    for i in 0..n {
                        let k = leaf::key(b, i);
                        if i > 0 && leaf::key(b, i - 1) >= k {
                            return Err(corruption(format!("validate: leaf {id} keys unsorted")));
                        }
                        check_bound(k, &lo, &hi, id)?;
                    }
                } else if is_internal(b) {
                    let n = key_count(b);
                    for i in 0..n {
                        let sep = internal::key(b, i);
                        if i > 0 && internal::key(b, i - 1) >= sep {
                            return Err(corruption(format!(
                                "validate: internal {id} separators unsorted"
                            )));
                        }
                        check_bound(sep, &lo, &hi, id)?;
                    }
                    // Each child inherits bounds from neighbouring
                    // separators: c_0 < sep_0, c_i in [sep_{i-1}, sep_i).
                    let children = internal::children(b);
                    for (j, child) in children.iter().enumerate() {
                        let c_lo = if j == 0 {
                            lo.clone()
                        } else {
                            Some(internal::key(b, j - 1).to_vec())
                        };
                        let c_hi = if j < n {
                            Some(internal::key(b, j).to_vec())
                        } else {
                            hi.clone()
                        };
                        next_level.push((*child, c_lo, c_hi));
                    }
                } else {
                    return Err(corruption(format!("validate: page {id} has no node tag")));
                }
            }
            level = next_level;
            depth += 1;
        }
        // Leaf chain must visit leaves in key order.
        let mut prev_last: Option<Vec<u8>> = None;
        for (i, id) in leaf_pages.iter().enumerate() {
            let g = self.pool.fetch_read(*id)?;
            let b = g.page().body();
            let n = key_count(b);
            if n > 0 {
                let first = leaf::key(b, 0).to_vec();
                if let Some(prev) = &prev_last
                    && prev.as_slice() >= first.as_slice()
                {
                    return Err(corruption("validate: leaf chain out of key order"));
                }
                prev_last = Some(leaf::key(b, n - 1).to_vec());
            }
            if i + 1 < leaf_pages.len() {
                let expected_next = leaf_pages[i + 1];
                if leaf::next(b) != expected_next {
                    return Err(corruption(format!(
                        "validate: leaf {id} next={} expected {expected_next}",
                        leaf::next(b)
                    )));
                }
            }
        }
        Ok(())
    }
}

fn check_bound(
    k: &[u8],
    lo: &Option<Vec<u8>>,
    hi: &Option<Vec<u8>>,
    id: PageId,
) -> ndb_common::Result<()> {
    if let Some(lo) = lo
        && k < lo.as_slice()
    {
        return Err(corruption(format!(
            "validate: page {id} key below lower bound"
        )));
    }
    if let Some(hi) = hi
        && k >= hi.as_slice()
    {
        return Err(corruption(format!(
            "validate: page {id} key at/above upper bound"
        )));
    }
    Ok(())
}

enum DeleteResult {
    Done(bool),
    Retry,
}

enum Merge {
    Done,
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::{KeyPart, encode_composite};
    use ndb_page::manager::PageIo;

    #[derive(Default)]
    struct MemIo {
        data: std::sync::Mutex<Vec<u8>>,
    }
    impl PageIo for MemIo {
        fn read_at(&mut self, o: u64, b: &mut [u8]) -> ndb_common::Result<()> {
            let d = self.data.get_mut().unwrap();
            b.copy_from_slice(&d[o as usize..o as usize + b.len()]);
            Ok(())
        }
        fn write_at(&mut self, o: u64, b: &[u8]) -> ndb_common::Result<()> {
            let d = self.data.get_mut().unwrap();
            d[o as usize..o as usize + b.len()].copy_from_slice(b);
            Ok(())
        }
        fn len(&mut self) -> ndb_common::Result<u64> {
            Ok(self.data.get_mut().unwrap().len() as u64)
        }
        fn set_len(&mut self, l: u64) -> ndb_common::Result<()> {
            self.data.get_mut().unwrap().resize(l as usize, 0);
            Ok(())
        }
        fn sync(&mut self) -> ndb_common::Result<()> {
            Ok(())
        }
    }

    fn tree(page_size: u32, pool_frames: usize) -> BTree<MemIo> {
        let mgr = ndb_page::manager::PageManager::create(MemIo::default(), page_size, 8).unwrap();
        let pool = Arc::new(BufferPool::new(mgr, pool_frames as u64 * page_size as u64));
        BTree::create(pool).unwrap()
    }

    fn k(n: u64) -> Vec<u8> {
        format!("{:08}", n).into_bytes()
    }

    #[test]
    fn insert_get_delete_small() {
        let t = tree(4096, 16);
        t.insert(b"alpha", b"1").unwrap();
        t.insert(b"beta", b"2").unwrap();
        assert_eq!(t.get(b"alpha").unwrap(), Some(b"1".to_vec()));
        assert_eq!(t.get(b"beta").unwrap(), Some(b"2".to_vec()));
        assert_eq!(t.get(b"gamma").unwrap(), None);
        assert!(t.delete(b"alpha").unwrap());
        assert_eq!(t.get(b"alpha").unwrap(), None);
        assert!(!t.delete(b"alpha").unwrap());
        t.validate().unwrap();
    }

    #[test]
    fn ascending_inserts_force_splits() {
        let t = tree(4096, 32);
        for i in 0..500u64 {
            t.insert(&k(i), &i.to_le_bytes()).unwrap();
        }
        for i in 0..500u64 {
            assert_eq!(t.get(&k(i)).unwrap(), Some(i.to_le_bytes().to_vec()));
        }
        t.validate().unwrap();
    }

    #[test]
    fn descending_and_random_inserts() {
        let t = tree(4096, 32);
        let mut order: Vec<u64> = (0..400).collect();
        // Deterministic shuffle.
        order.reverse();
        let mut i = 0;
        while i < order.len() {
            let len = order.len();
            order.swap(i, (i * 7 + 3) % len);
            i += 1;
        }
        for &n in &order {
            t.insert(&k(n), b"v").unwrap();
        }
        t.validate().unwrap();
        assert_eq!(t.scan().unwrap().len(), 400);
    }

    #[test]
    fn replace_value() {
        let t = tree(4096, 16);
        t.insert(b"k", b"first").unwrap();
        t.insert(b"k", b"second-longer").unwrap();
        assert_eq!(t.get(b"k").unwrap(), Some(b"second-longer".to_vec()));
        t.insert(b"k", b"s").unwrap();
        assert_eq!(t.get(b"k").unwrap(), Some(b"s".to_vec()));
        t.validate().unwrap();
    }

    #[test]
    fn range_scan() {
        let t = tree(4096, 32);
        for i in 0..100u64 {
            t.insert(&k(i), b"v").unwrap();
        }
        let r = t.range(&k(10), &k(20)).unwrap();
        assert_eq!(r.len(), 10);
        assert_eq!(r[0].0, k(10));
        assert_eq!(r[9].0, k(19));
        // Scan over the whole tree is fully ordered.
        let all = t.scan().unwrap();
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
    }

    #[test]
    fn deletes_collapse_tree() {
        let t = tree(4096, 32);
        for i in 0..300u64 {
            t.insert(&k(i), b"v").unwrap();
        }
        let root_growth = t.root_id();
        assert_ne!(root_growth.get(), 1); // tree split above the first leaf
        // Delete in random-ish order to exercise merges everywhere.
        for i in (0..300u64).rev() {
            assert!(t.delete(&k(i)).unwrap());
        }
        t.validate().unwrap();
        assert_eq!(t.scan().unwrap().len(), 0);
        // Tree collapsed back to a single empty leaf root.
        assert_eq!(t.root_id().get(), 1);
    }

    #[test]
    fn composite_keys_order_correctly() {
        let t = tree(4096, 32);
        // Insert (tenant, ts) composite keys out of order.
        let mut inserted = Vec::new();
        for tenant in [2u64, 1, 3] {
            for ts in [5u64, 1, 9] {
                let key = encode_composite(&[KeyPart::UInt(tenant), KeyPart::UInt(ts)]);
                t.insert(&key, &tenant.to_le_bytes()).unwrap();
                inserted.push(key);
            }
        }
        let all = t.scan().unwrap();
        for w in all.windows(2) {
            assert!(w[0].0 < w[1].0);
        }
        assert_eq!(all.len(), 9);
        t.validate().unwrap();
    }

    #[test]
    fn concurrent_writers_and_readers() {
        let t = Arc::new(tree(4096, 64));
        std::thread::scope(|s| {
            // 4 writers on disjoint ranges.
            for w in 0..4u64 {
                let t = Arc::clone(&t);
                s.spawn(move || {
                    for i in 0..250u64 {
                        let key = w * 1000 + i;
                        t.insert(&k(key), &key.to_le_bytes()).unwrap();
                    }
                });
            }
            // 2 readers scanning repeatedly.
            for _ in 0..2 {
                let t = Arc::clone(&t);
                s.spawn(move || {
                    for _ in 0..20 {
                        let all = t.scan().unwrap();
                        for w in all.windows(2) {
                            assert!(w[0].0 < w[1].0);
                        }
                    }
                });
            }
        });
        t.validate().unwrap();
        for w in 0..4u64 {
            for i in 0..250u64 {
                let key = w * 1000 + i;
                assert_eq!(t.get(&k(key)).unwrap(), Some(key.to_le_bytes().to_vec()));
            }
        }
    }

    #[test]
    fn key_too_large_rejected() {
        let t = tree(4096, 8);
        let big_key = vec![7u8; MAX_KEY_SIZE + 1];
        assert!(matches!(
            t.insert(&big_key, b"v"),
            Err(Error::InvalidInput(_))
        ));
        let big_val = vec![9u8; 4096];
        assert!(matches!(
            t.insert(b"k", &big_val),
            Err(Error::InvalidInput(_))
        ));
    }
}
