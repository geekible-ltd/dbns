//! Forward cursor over the leaf chain (spec §5.3 — cursor navigation,
//! ordered iterators, range scans).
//!
//! The cursor holds a read latch on the current leaf. Advancing to the next
//! leaf latch-couples: the right sibling is latched before the current latch
//! is released, so a leaf split concurrent with iteration cannot skip the
//! sibling link.

use ndb_buffer::{BufferPool, PageReadGuard};
use ndb_common::ids::PageId;
use ndb_page::manager::PageIo;
use std::sync::Arc;

/// An ordered cursor over a B+Tree's leaf entries.
pub struct Cursor<P: PageIo> {
    pool: Arc<BufferPool<P>>,
    guard: Option<PageReadGuard>,
    idx: usize,
}

impl<P: PageIo> Cursor<P> {
    /// Positions the cursor on `guard`'s leaf at slot `idx`.
    pub(crate) fn new(pool: Arc<BufferPool<P>>, guard: PageReadGuard, idx: usize) -> Self {
        Self {
            pool,
            guard: Some(guard),
            idx,
        }
    }

    /// The next `(key, value)` in order, or `None` past the last entry.
    fn advance(&mut self) -> ndb_common::Result<Option<(Vec<u8>, Vec<u8>)>> {
        loop {
            let g = match self.guard.as_mut() {
                Some(g) => g,
                None => return Ok(None),
            };
            let next_leaf = {
                let b = g.page().body();
                if self.idx < crate::node::key_count(b) {
                    let entry = (
                        crate::node::leaf::key(b, self.idx).to_vec(),
                        crate::node::leaf::val(b, self.idx).to_vec(),
                    );
                    self.idx += 1;
                    return Ok(Some(entry));
                }
                crate::node::leaf::next(b)
            };
            if next_leaf == PageId::new(0) {
                self.guard = None;
                return Ok(None);
            }
            // Latch the right sibling before releasing the current leaf.
            let ng = self.pool.fetch_read(next_leaf)?;
            *g = ng;
            self.idx = 0;
        }
    }
}

/// Fallible iterator: yields `Result<(key, value)>`, `None` at the end —
/// the same convention as `std::fs::ReadDir`.
impl<P: PageIo> Iterator for Cursor<P> {
    type Item = ndb_common::Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.advance() {
            Ok(e) => e.map(Ok),
            Err(e) => {
                // Don't yield repeated errors: mark the cursor spent.
                self.guard = None;
                Some(Err(e))
            }
        }
    }
}
