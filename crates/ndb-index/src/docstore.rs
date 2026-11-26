//! Document blob storage (spec §5.2, §8.2 — "Document ID → Record
//! location").
//!
//! A leaf value can't hold arbitrarily large documents, so the primary
//! index stores a `RecordEntry` per version:
//!
//! ```text
//! u8 tag
//!   0 → inline: the encoded document bytes follow (small docs)
//!   1 → ref:    u64 head page id — the blob is chained across `Doc` pages
//! ```
//!
//! Chained pages lay out as `u64 next | u16 chunk_len | chunk bytes` in the
//! page body — the document's own header gives its total length, so the
//! reader knows when to stop.
//!
//! Reclaiming pages of dropped versions (GC boundary / rollback) is
//! deferred: page freeing is already best-effort in this phase (the free
//! list is in-memory only), so dropped chains are documented leaks, not
//! corruption. A version-dropped reaper lands with persisted freelists.

use ndb_buffer::BufferPool;
use ndb_common::error::Error;
use ndb_common::ids::PageId;
use ndb_page::manager::PageIo;
use ndb_page::page::PageType;
use std::sync::Arc;

const TAG_INLINE: u8 = 0;
const TAG_CHAINED: u8 = 1;
const CHUNK_HDR: usize = 10; // next u64 + len u16

/// Documents up to this encoded size stay inline in the version entry.
/// Larger ones go to record page chains.
pub const INLINE_LIMIT: usize = 1024;

/// Encodes/decodes the leaf-value record entry and owns the chained-page
/// blob store. Cheap to clone — stateless beyond the pool.
#[derive(Clone)]
pub struct DocStore<P: PageIo> {
    pool: Arc<BufferPool<P>>,
}

impl<P: PageIo> DocStore<P> {
    pub fn new(pool: Arc<BufferPool<P>>) -> Self {
        Self { pool }
    }

    /// The backing pool — index B+Trees are created on it.
    pub(crate) fn pool(&self) -> &Arc<BufferPool<P>> {
        &self.pool
    }

    /// Wraps an encoded document into its leaf-value entry bytes.
    pub fn write(&self, doc_bytes: &[u8]) -> ndb_common::Result<Vec<u8>> {
        if doc_bytes.len() <= INLINE_LIMIT {
            let mut out = Vec::with_capacity(1 + doc_bytes.len());
            out.push(TAG_INLINE);
            out.extend_from_slice(doc_bytes);
            return Ok(out);
        }
        let head = self.write_chain(doc_bytes)?;
        let mut out = Vec::with_capacity(9);
        out.push(TAG_CHAINED);
        out.extend_from_slice(&head.get().to_le_bytes());
        Ok(out)
    }

    /// Reads the document bytes from a leaf-value entry.
    pub fn read(&self, entry: &[u8]) -> ndb_common::Result<Vec<u8>> {
        match entry.first() {
            Some(&TAG_INLINE) => Ok(entry[1..].to_vec()),
            Some(&TAG_CHAINED) => {
                if entry.len() < 9 {
                    return Err(Error::Corruption("record entry: truncated ref".into()));
                }
                let head = PageId::new(u64::from_le_bytes(entry[1..9].try_into().unwrap()));
                self.read_chain(head)
            }
            _ => Err(Error::Corruption("record entry: bad tag".into())),
        }
    }

    /// Splits the blob across a chain of `Doc` pages, returning the head.
    fn write_chain(&self, blob: &[u8]) -> ndb_common::Result<PageId> {
        let capacity = self.pool.page_size() as usize - 40 - CHUNK_HDR;
        let mut head = PageId::new(0);
        let mut prev = PageId::new(0);
        let mut off = 0;
        while off < blob.len() {
            let end = (off + capacity).min(blob.len());
            let chunk = &blob[off..end];
            let id = self.pool.allocate_page(PageType::Doc)?;
            {
                let mut g = self.pool.fetch_write(id)?;
                let b = g.page_mut().body_mut();
                b[0..8].copy_from_slice(&0u64.to_le_bytes());
                b[8..10].copy_from_slice(&(chunk.len() as u16).to_le_bytes());
                b[10..10 + chunk.len()].copy_from_slice(chunk);
            }
            if prev.get() != 0 {
                let mut pg = self.pool.fetch_write(prev)?;
                pg.page_mut().body_mut()[0..8].copy_from_slice(&id.get().to_le_bytes());
            } else {
                head = id;
            }
            prev = id;
            off = end;
        }
        Ok(head)
    }

    fn read_chain(&self, head: PageId) -> ndb_common::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut id = head;
        for _ in 0..1_000_000u32 {
            // bound against corrupt next-links forming a cycle
            if id.get() == 0 {
                break;
            }
            let g = self.pool.fetch_read(id)?;
            let b = g.page().body();
            let next = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let len = u16::from_le_bytes(b[8..10].try_into().unwrap()) as usize;
            if len > b.len() - CHUNK_HDR {
                return Err(Error::Corruption("doc page: chunk exceeds page".into()));
            }
            out.extend_from_slice(&b[CHUNK_HDR..CHUNK_HDR + len]);
            id = PageId::new(next);
        }
        Ok(out)
    }
}
