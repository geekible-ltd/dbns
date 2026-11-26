//! Buffer pool (spec §5.4): loads pages, caches them in a bounded number of
//! frames, tracks pins and dirty pages, and evicts via the clock
//! (second-chance) algorithm.
//!
//! Concurrency model
//! ─────────────────
//! The frame table is shared; pages are latched individually via
//! `RwLock<Page>`. [`PageReadGuard`] and [`PageWriteGuard`] are *owned*
//! guards: they keep both the frame (`Arc`) and the read/write latch alive
//! until dropped, which is what makes latch-coupled traversal possible in
//! `ndb-btree` — a child page can be latched while the parent latch is held.
//!
//! Memory is strictly bounded: the pool allocates `pool_bytes / page_size`
//! frames up front and never grows (spec §3.1).
//!
//! Phase-2 simplification: the `PageManager` sits behind a single mutex, so
//! page faults and allocations serialise. Dirty write-back happens on
//! eviction and on [`BufferPool::flush_all`]; real checkpointing arrives with
//! the WAL in Phase 3.

use ndb_common::error::Error;
use ndb_common::ids::PageId;
use ndb_page::manager::{PageIo, PageManager};
use ndb_page::page::{Page, PageType};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// Per-frame bookkeeping protected by the pool-state mutex.
#[derive(Debug, Default)]
struct FrameMeta {
    page_id: Option<PageId>,
    pin_count: usize,
    dirty: bool,
    /// Clock reference bit — set on pin, cleared by the eviction scan.
    referenced: bool,
}

/// One buffer-pool frame: a page-sized slot plus its latch and metadata.
struct Frame {
    meta: Mutex<FrameMeta>,
    page: RwLock<Page>,
}

/// Shared, non-generic pool state owned by every guard.
struct Shared {
    frames: Vec<Arc<Frame>>,
    state: Mutex<PoolState>,
    /// §15.7 storage statistics — cumulative fetches and evictions.
    fetches: std::sync::atomic::AtomicU64,
    evictions: std::sync::atomic::AtomicU64,
}

#[derive(Default)]
struct PoolState {
    /// page_id → frame index.
    table: HashMap<PageId, Arc<Frame>>,
    clock_hand: usize,
}

/// Journaling hook (ADR-006): invoked on write-guard drop when the page
/// bytes changed — `(page_id, before_image, after_image) -> lsn`. The LSN
/// is stamped into the page header so the flush gate knows which WAL
/// position covers this page.
pub type PageHook = dyn Fn(PageId, &[u8], &[u8]) -> u64 + Send + Sync;

/// WAL-before-data gate: called with a dirty page's LSN before the page is
/// written to the data file; must ensure the WAL is durable to that LSN.
pub type FlushGate = dyn Fn(u64) -> ndb_common::Result<()> + Send + Sync;

/// A bounded buffer pool over a [`PageManager`].
pub struct BufferPool<P: PageIo> {
    inner: Arc<Inner<P>>,
}

struct Inner<P: PageIo> {
    mgr: Mutex<PageManager<P>>,
    shared: Arc<Shared>,
    page_hook: Mutex<Option<Arc<PageHook>>>,
    flush_gate: Mutex<Option<Arc<FlushGate>>>,
}

impl<P: PageIo> BufferPool<P> {
    /// Creates a pool of `pool_bytes / page_size` frames (minimum 1 frame).
    /// Memory usage is bounded by `pool_bytes` plus overhead.
    pub fn new(mgr: PageManager<P>, pool_bytes: u64) -> Self {
        let page_size = mgr.page_size();
        let n = (pool_bytes / page_size as u64).max(1) as usize;
        let frames = (0..n)
            .map(|_| {
                Arc::new(Frame {
                    meta: Mutex::new(FrameMeta::default()),
                    page: RwLock::new(Page::new(page_size)),
                })
            })
            .collect();
        Self {
            inner: Arc::new(Inner {
                mgr: Mutex::new(mgr),
                shared: Arc::new(Shared {
                    frames,
                    state: Mutex::new(PoolState::default()),
                    fetches: std::sync::atomic::AtomicU64::new(0),
                    evictions: std::sync::atomic::AtomicU64::new(0),
                }),
                page_hook: Mutex::new(None),
                flush_gate: Mutex::new(None),
            }),
        }
    }

    /// Page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.inner.mgr.lock().unwrap().page_size()
    }

    /// Number of frames in the pool.
    pub fn frame_count(&self) -> usize {
        self.inner.shared.frames.len()
    }

    /// Number of pages currently resident.
    /// Cumulative page loads from disk (§15.7 stats).
    pub fn fetch_count(&self) -> u64 {
        self.inner
            .shared
            .fetches
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Cumulative frame evictions (§15.7 stats).
    pub fn eviction_count(&self) -> u64 {
        self.inner
            .shared
            .evictions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Pool handle used by admin endpoints (§15.7).
    pub fn pool_stats(&self) -> (usize, usize, u64, u64) {
        (
            self.frame_count(),
            self.resident_pages(),
            self.fetch_count(),
            self.eviction_count(),
        )
    }

    pub fn resident_pages(&self) -> usize {
        self.inner.shared.state.lock().unwrap().table.len()
    }

    /// Allocates a new page. The initial content is written through the
    /// allocator; subsequent modifications must go through `fetch_write` so
    /// the cached copy stays consistent.
    pub fn allocate_page(&self, page_type: PageType) -> ndb_common::Result<PageId> {
        Ok(self.inner.mgr.lock().unwrap().allocate_page(page_type)?.0)
    }

    /// Frees a page. It must not be pinned; the frame copy (if resident) is
    /// dropped before the free so stale bytes can't be served.
    pub fn free_page(&self, id: PageId) -> ndb_common::Result<()> {
        let frame = self.inner.shared.state.lock().unwrap().table.remove(&id);
        if let Some(frame) = frame {
            let pinned = frame.meta.lock().unwrap().pin_count > 0;
            if pinned {
                // Re-insert before erroring.
                self.inner
                    .shared
                    .state
                    .lock()
                    .unwrap()
                    .table
                    .insert(id, frame);
                return Err(Error::InvalidInput(format!("cannot free pinned page {id}")));
            }
            *frame.page.write().unwrap() = Page::new(self.page_size());
            *frame.meta.lock().unwrap() = FrameMeta::default();
        }
        self.inner.mgr.lock().unwrap().free_page(id)
    }

    /// Fetches `id` with a read latch. Loads from disk on a miss, evicting a
    /// victim frame via the clock algorithm when the pool is full.
    pub fn fetch_read(&self, id: PageId) -> ndb_common::Result<PageReadGuard> {
        let frame = self.pin(id)?;
        let guard = unsafe { force_static_read(frame.page.read().unwrap()) };
        Ok(PageReadGuard {
            frame,
            shared: Arc::clone(&self.inner.shared),
            guard,
        })
    }

    /// Like [`Self::fetch_write`] but returns `None` when the page's write
    /// latch is already held. Used for sibling latching where blocking could
    /// deadlock — the caller drops its held latches and retries.
    pub fn try_fetch_write(&self, id: PageId) -> ndb_common::Result<Option<PageWriteGuard>> {
        let frame = self.pin(id)?;
        // Pull the guard out of the match so the Err arm drops the borrow.
        let g = match frame.page.try_write() {
            Ok(g) => g,
            Err(_) => {
                unpin(&frame);
                return Ok(None);
            }
        };
        frame.meta.lock().unwrap().dirty = true;
        let hook = self.page_hook();
        let before = hook.as_ref().map(|_| g.as_bytes().to_vec());
        let guard = unsafe { force_static_write(g) };
        Ok(Some(PageWriteGuard {
            frame,
            shared: Arc::clone(&self.inner.shared),
            hook,
            before,
            guard,
        }))
    }

    /// Fetches `id` with a write latch and marks the frame dirty — a
    /// conservative choice; clean write-outs are cheap.
    pub fn fetch_write(&self, id: PageId) -> ndb_common::Result<PageWriteGuard> {
        let frame = self.pin(id)?;
        frame.meta.lock().unwrap().dirty = true;
        let g = frame.page.write().unwrap();
        let hook = self.page_hook();
        let before = hook.as_ref().map(|_| g.as_bytes().to_vec());
        let guard = unsafe { force_static_write(g) };
        Ok(PageWriteGuard {
            frame,
            shared: Arc::clone(&self.inner.shared),
            hook,
            before,
            guard,
        })
    }

    /// Installs the journaling hook invoked when a write-latched page is
    /// released with modified bytes (ADR-006).
    pub fn set_page_hook(&self, hook: Option<Arc<PageHook>>) {
        *self.inner.page_hook.lock().unwrap() = hook;
    }

    /// Installs the WAL-before-data gate consulted before writing any dirty
    /// page to the data file.
    pub fn set_flush_gate(&self, gate: Option<Arc<FlushGate>>) {
        *self.inner.flush_gate.lock().unwrap() = gate;
    }

    fn page_hook(&self) -> Option<Arc<PageHook>> {
        self.inner.page_hook.lock().unwrap().clone()
    }

    /// Runs the flush gate for a page about to be written back.
    fn gate(&self, page: &Page) -> ndb_common::Result<()> {
        let gate = self.inner.flush_gate.lock().unwrap().clone();
        if let (Some(gate), Ok(h)) = (gate, page.header()) {
            gate(h.lsn.get())?;
        }
        Ok(())
    }

    /// Flushes every dirty resident page through the page manager, then
    /// syncs the file. Used by checkpointing.
    pub fn flush_all(&self) -> ndb_common::Result<()> {
        // Collect (id, frame) pairs without holding the state lock while
        // writing — frames keep themselves alive via Arc.
        let dirty: Vec<(PageId, Arc<Frame>)> = self
            .inner
            .shared
            .state
            .lock()
            .unwrap()
            .table
            .iter()
            .filter(|(_, f)| f.meta.lock().unwrap().dirty)
            .map(|(id, f)| (*id, Arc::clone(f)))
            .collect();
        let mut mgr = self.inner.mgr.lock().unwrap();
        for (_id, frame) in dirty {
            let mut page = frame.page.write().unwrap();
            self.gate(&page)?;
            mgr.write_page(&mut page)?;
            frame.meta.lock().unwrap().dirty = false;
        }
        mgr.sync()
    }

    /// Pins `id` and returns its frame, loading it if necessary.
    fn pin(&self, id: PageId) -> ndb_common::Result<Arc<Frame>> {
        let shared = &self.inner.shared;
        {
            let state = shared.state.lock().unwrap();
            if let Some(frame) = state.table.get(&id) {
                let mut meta = frame.meta.lock().unwrap();
                meta.pin_count += 1;
                meta.referenced = true;
                return Ok(Arc::clone(frame));
            }
        }

        // Miss: pick a victim frame under the state lock.
        let mut state = shared.state.lock().unwrap();
        let frame = self.evict_candidate(&mut state)?;
        shared
            .evictions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut meta = frame.meta.lock().unwrap();
        // If the victim holds a page, flush it out.
        if let Some(old_id) = meta.page_id.take() {
            let mut page = frame.page.write().unwrap();
            if meta.dirty {
                self.gate(&page)?;
                self.inner.mgr.lock().unwrap().write_page(&mut page)?;
            }
            state.table.remove(&old_id);
        }
        // Load the requested page into the frame.
        let mut mgr = self.inner.mgr.lock().unwrap();
        shared
            .fetches
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let page = mgr.read_page(id)?;
        *frame.page.write().unwrap() = page;
        *meta = FrameMeta {
            page_id: Some(id),
            pin_count: 1,
            dirty: false,
            referenced: true,
        };
        drop(meta);
        state.table.insert(id, Arc::clone(&frame));
        Ok(frame)
    }

    /// Clock (second-chance) eviction: scan frames, clearing reference bits,
    /// until an unreferenced, unpinned frame is found.
    fn evict_candidate(&self, state: &mut PoolState) -> ndb_common::Result<Arc<Frame>> {
        let frames = &self.inner.shared.frames;
        let n = frames.len();
        for _ in 0..(2 * n + 1) {
            let idx = state.clock_hand % n;
            state.clock_hand += 1;
            let frame = &frames[idx];
            let mut meta = frame.meta.lock().unwrap();
            if meta.pin_count > 0 {
                continue;
            }
            if meta.page_id.is_none() {
                return Ok(Arc::clone(frame)); // never used
            }
            if meta.referenced {
                meta.referenced = false;
                continue;
            }
            return Ok(Arc::clone(frame));
        }
        Err(Error::Internal(
            "buffer pool exhausted: every frame is pinned".into(),
        ))
    }
}

// SAFETY for both `force_static_*`: the `RwLock<Page>` lives inside
// `Arc<Frame>`; the returned guard struct holds that Arc, so the lock outlives
// the guard. Frames are never moved or deallocated while an Arc exists.
unsafe fn force_static_read(g: RwLockReadGuard<'_, Page>) -> RwLockReadGuard<'static, Page> {
    unsafe { std::mem::transmute(g) }
}
unsafe fn force_static_write(g: RwLockWriteGuard<'_, Page>) -> RwLockWriteGuard<'static, Page> {
    unsafe { std::mem::transmute(g) }
}

fn unpin(frame: &Arc<Frame>) {
    let mut meta = frame.meta.lock().unwrap();
    debug_assert!(meta.pin_count > 0);
    meta.pin_count = meta.pin_count.saturating_sub(1);
}

/// Owned read latch on a pinned page.
pub struct PageReadGuard {
    frame: Arc<Frame>,
    #[allow(dead_code)]
    shared: Arc<Shared>,
    guard: RwLockReadGuard<'static, Page>,
}

impl PageReadGuard {
    /// The latched page.
    pub fn page(&self) -> &Page {
        &self.guard
    }
}

impl Drop for PageReadGuard {
    fn drop(&mut self) {
        unpin(&self.frame);
    }
}

/// Owned write latch on a pinned page; marks the frame dirty on creation.
pub struct PageWriteGuard {
    frame: Arc<Frame>,
    #[allow(dead_code)]
    shared: Arc<Shared>,
    /// Page bytes at fetch time — captured only when a page hook is set.
    before: Option<Vec<u8>>,
    hook: Option<Arc<PageHook>>,
    guard: RwLockWriteGuard<'static, Page>,
}

impl PageWriteGuard {
    /// The latched page.
    pub fn page(&self) -> &Page {
        &self.guard
    }
    /// Mutable access to the latched page.
    pub fn page_mut(&mut self) -> &mut Page {
        &mut self.guard
    }
}

impl Drop for PageWriteGuard {
    fn drop(&mut self) {
        // Journal the change under the write latch while both images are
        // available, then stamp the page LSN so the flush gate can order
        // WAL before data (ADR-006). The checksum is refreshed BEFORE
        // capturing the after-image so every journaled image verifies —
        // the body's mutations left the stored checksum stale.
        if let (Some(hook), Some(before)) = (&self.hook, &self.before)
            && before.as_slice() != self.guard.as_bytes()
        {
            self.guard.refresh_checksum();
            let after = self.guard.as_bytes();
            let id = self.frame.meta.lock().unwrap().page_id;
            if let Some(id) = id {
                let lsn = hook(id, before, after);
                if let Ok(mut h) = self.guard.header() {
                    h.lsn = ndb_common::ids::Lsn::new(lsn);
                    self.guard.write_header(h);
                }
            }
        }
        unpin(&self.frame);
    }
}

// Guards are movable across threads when the pool components are.
unsafe impl Send for PageReadGuard {}
unsafe impl Sync for PageReadGuard {}
unsafe impl Send for PageWriteGuard {}
unsafe impl Sync for PageWriteGuard {}

#[cfg(test)]
mod tests {
    use super::*;
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

    fn pool(frames: usize) -> BufferPool<MemIo> {
        let mgr = PageManager::create(MemIo::default(), 4096, 4).unwrap();
        BufferPool::new(mgr, frames as u64 * 4096)
    }

    #[test]
    fn fetch_read_write_roundtrip() {
        let pool = pool(4);
        let id = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        {
            let mut g = pool.fetch_write(id).unwrap();
            g.page_mut().body_mut()[..3].copy_from_slice(b"xyz");
        }
        let g = pool.fetch_read(id).unwrap();
        assert_eq!(&g.page().body()[..3], b"xyz");
    }

    #[test]
    fn eviction_reuses_frames_and_preserves_data() {
        let pool = pool(2);
        // Create 4 pages, access them all (pool holds only 2).
        let mut ids = Vec::new();
        for i in 0..4u8 {
            let id = pool.allocate_page(PageType::BTreeLeaf).unwrap();
            {
                let mut g = pool.fetch_write(id).unwrap();
                g.page_mut().body_mut()[0] = i;
            }
            ids.push(id);
        }
        for (i, id) in ids.iter().enumerate() {
            let mut g = pool.fetch_write(*id).unwrap();
            g.page_mut().body_mut()[1] = i as u8;
        }
        assert!(pool.resident_pages() <= 2);
        // All data survives eviction via dirty write-back.
        for (i, id) in ids.iter().enumerate() {
            let g = pool.fetch_read(*id).unwrap();
            assert_eq!(g.page().body()[1], i as u8);
        }
    }

    #[test]
    fn pinned_frames_are_not_evicted() {
        let pool = pool(1);
        let id1 = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        let id2 = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        let _g = pool.fetch_read(id1).unwrap(); // pin the only frame
        // Next fetch has no evictable frame.
        assert!(matches!(pool.fetch_read(id2), Err(Error::Internal(_))));
    }

    #[test]
    fn page_hook_journals_changes_and_stamps_lsn() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let pool = pool(4);
        let calls = Arc::new(AtomicU64::new(0));
        let calls2 = Arc::clone(&calls);
        pool.set_page_hook(Some(Arc::new(move |id, before, after| {
            calls2.fetch_add(1, Ordering::SeqCst);
            assert_eq!(id.get() % 2, 1); // our test pages
            assert_ne!(before, after);
            42 // lsn
        })));

        let id = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        {
            let mut g = pool.fetch_write(id).unwrap();
            g.page_mut().body_mut()[0] = 0xAA;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // LSN was stamped into the header.
        let g = pool.fetch_read(id).unwrap();
        assert_eq!(g.page().header().unwrap().lsn.get(), 42);
        drop(g);
        // A write fetch that changes nothing does not journal.
        {
            let _g = pool.fetch_write(id).unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn flush_gate_runs_before_dirty_write_back() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let seen = Arc::new(AtomicU64::new(0));
        let seen2 = Arc::clone(&seen);
        let pool = pool(1); // single frame forces eviction write-back
        pool.set_page_hook(Some(Arc::new(|_, _, _| 7)));
        pool.set_flush_gate(Some(Arc::new(move |lsn| {
            seen2.store(lsn, Ordering::SeqCst);
            Ok(())
        })));
        let id1 = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        let id2 = pool.allocate_page(PageType::BTreeLeaf).unwrap();
        {
            let mut g = pool.fetch_write(id1).unwrap();
            g.page_mut().body_mut()[0] = 1;
        }
        // Evict id1 by fetching id2 — the gate must have fired with lsn 7.
        let _g = pool.fetch_write(id2).unwrap();
        assert_eq!(seen.load(Ordering::SeqCst), 7);
    }
}
