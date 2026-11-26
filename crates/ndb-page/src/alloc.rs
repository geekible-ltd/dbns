//! Page allocation (spec §5.2).
//!
//! The allocator is purely logical — it tracks the high-water mark, the free
//! list and per-page generations without touching storage. The
//! [`crate::manager::PageManager`] applies its decisions to a [`PageIo`].
//!
//! Phase-1 limitation: the free list and generation table are in-memory.
//! Freed pages are forgotten on restart (harmless — they become unreachable
//! tail space until free-list persistence arrives with the B+Tree metadata
//! pages in Phase 2). Generations are tracked in-memory only.

use ndb_common::error::Error;
use ndb_common::ids::PageId;
use std::collections::{BTreeSet, HashMap};

/// Logical page allocator.
#[derive(Debug, Default)]
pub struct PageAllocator {
    /// Next page ID that has never been allocated (page 0 is the meta page,
    /// so allocation starts at 1).
    high_water: u64,
    /// Previously allocated pages that have been freed, lowest first.
    free: BTreeSet<u64>,
    /// Current generation per page ID.
    generations: HashMap<u64, u64>,
}

impl PageAllocator {
    /// Creates an allocator with the given high-water mark (from the meta
    /// page on reopen).
    pub fn new(high_water: u64) -> Self {
        Self {
            high_water: high_water.max(1),
            free: BTreeSet::new(),
            generations: HashMap::new(),
        }
    }

    /// Raises the high-water mark — recovery may restore pages beyond the
    /// persisted mark before meta-page state catches up.
    pub fn bump_high_water(&mut self, v: u64) {
        self.high_water = self.high_water.max(v);
    }

    /// Next page ID that has never been allocated.
    pub fn high_water(&self) -> u64 {
        self.high_water
    }

    /// The generation a page was last allocated with, if this allocator
    /// allocated it.
    pub fn generation(&self, id: PageId) -> Option<u64> {
        self.generations.get(&id.get()).copied()
    }

    /// Allocates a page: reuses the lowest freed ID when available, otherwise
    /// takes the high-water mark. Returns the ID and its new generation —
    /// incremented on reuse so stale references fail (spec §5.2).
    pub fn allocate(&mut self) -> (PageId, u64) {
        let id = match self.free.iter().next() {
            Some(&id) => {
                self.free.remove(&id);
                id
            }
            None => {
                let id = self.high_water;
                self.high_water += 1;
                id
            }
        };
        let generation = self.generations.get(&id).copied().unwrap_or(0) + 1;
        self.generations.insert(id, generation);
        (PageId::new(id), generation)
    }

    /// Returns a page to the free list. Freeing an already-free page is an
    /// error (double free).
    pub fn free(&mut self, id: PageId) -> ndb_common::Result<()> {
        if id.get() == 0 || id.get() >= self.high_water {
            return Err(Error::InvalidInput(format!(
                "cannot free unallocated page {id}"
            )));
        }
        if !self.free.insert(id.get()) {
            return Err(Error::InvalidInput(format!("double free of page {id}")));
        }
        Ok(())
    }

    /// Number of pages currently on the free list.
    pub fn free_count(&self) -> usize {
        self.free.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_sequentially_then_reuses() {
        let mut a = PageAllocator::new(1);
        assert_eq!(a.allocate(), (PageId::new(1), 1));
        assert_eq!(a.allocate(), (PageId::new(2), 1));
        a.free(PageId::new(1)).unwrap();
        // Reuse bumps the generation.
        assert_eq!(a.allocate(), (PageId::new(1), 2));
        assert_eq!(a.allocate(), (PageId::new(3), 1));
    }

    #[test]
    fn double_free_and_invalid_free_rejected() {
        let mut a = PageAllocator::new(1);
        a.allocate();
        a.free(PageId::new(1)).unwrap();
        assert!(a.free(PageId::new(1)).is_err());
        assert!(a.free(PageId::new(99)).is_err());
        assert!(a.free(PageId::new(0)).is_err());
    }
}
