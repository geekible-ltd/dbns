//! Inverted index over the versioned B+Tree (spec §9.1, §9.4).
//!
//! Key layout inside one dedicated tree:
//!
//! ```text
//! T{field:u16be}{term:utf8} → {u16 chunk_count}{u64 chunk_page_ids}*
//! S{doc_id:u64be}           → per-field token counts (BM25 doc lengths)
//! D{doc_id:u64be}           → deletion tombstone
//! MN                        → live document count
//! MF{field:u16be}           → total token count for the field (→ avgdl)
//! ```
//!
//! Postings live in **chunks**: page-bounded, delta+varint blocks with
//! block-max metadata (`ndb_postings`). Chunks are **copy-on-write** —
//! appends allocate a fresh tail chunk and repoint the term's versioned
//! header, so a rolled-back txn restores the old header which references
//! untouched pages (orphaned fresh pages are the documented page-leak
//! class, reclaimed by compaction).
//!
//! Segment/compaction mapping (§9.4): each sealed chunk is a
//! micro-segment; `compact()` merges all of a term's chunks, drops
//! deleted docs, and repacks — physical index compaction.

pub mod query;

use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::error::Error;
use ndb_common::ids::PageId;
use ndb_page::manager::PageIo;
use ndb_page::page::PageType;
pub use ndb_postings::Posting;
use ndb_postings::{Chunk, try_append};
use ndb_tokenizer::Token;
use ndb_transaction::Transaction;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Per-field analysed text passed to [`InvertedIndex::index_document`].
pub struct FieldText {
    /// Dictionary id of the indexed field.
    pub field_id: u16,
    /// Analysed tokens (position-aware).
    pub tokens: Vec<Token>,
}

/// Statistics for one indexed document — used by BM25.
#[derive(Debug, Clone, Default)]
pub struct DocStats {
    /// field_id → token count in that field.
    pub field_len: BTreeMap<u16, u32>,
}

/// Collection-level stats for scoring.
#[derive(Debug, Clone, Default)]
pub struct CollectionStats {
    /// Live (non-deleted) document count.
    pub doc_count: u64,
    /// field_id → total tokens across docs (avgdl = this / doc_count).
    pub field_total_len: BTreeMap<u16, u64>,
}

/// Stateless entry points — the index is fully described by its tree root.
pub struct InvertedIndex;

// ---- key builders ------------------------------------------------------

fn term_key(field_id: u16, term: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(3 + term.len());
    k.push(b'T');
    k.extend_from_slice(&field_id.to_be_bytes());
    k.extend_from_slice(term.as_bytes());
    k
}

fn stats_key(doc_id: u64) -> Vec<u8> {
    let mut k = vec![b'S'];
    k.extend_from_slice(&doc_id.to_be_bytes());
    k
}

const DEL_PREFIX: u8 = b'D';
const DOC_COUNT_KEY: &[u8] = b"MN";

fn field_total_key(field_id: u16) -> Vec<u8> {
    let mut k = vec![b'M', b'F'];
    k.extend_from_slice(&field_id.to_be_bytes());
    k
}

// ---- codecs ------------------------------------------------------------

fn encode_chain(ids: &[PageId]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + ids.len() * 8);
    v.extend_from_slice(&(ids.len() as u16).to_le_bytes());
    for id in ids {
        v.extend_from_slice(&id.0.to_le_bytes());
    }
    v
}

fn decode_chain(buf: &[u8]) -> ndb_common::Result<Vec<PageId>> {
    if buf.len() < 2 {
        return Err(Error::Corruption("chain: truncated".into()));
    }
    let n = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
    if buf.len() < 2 + n * 8 {
        return Err(Error::Corruption("chain: short ids".into()));
    }
    Ok((0..n)
        .map(|i| {
            PageId::new(u64::from_le_bytes(
                buf[2 + i * 8..10 + i * 8].try_into().unwrap(),
            ))
        })
        .collect())
}

fn encode_stats(s: &DocStats) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(&(s.field_len.len() as u16).to_le_bytes());
    for (f, l) in &s.field_len {
        v.extend_from_slice(&f.to_le_bytes());
        v.extend_from_slice(&l.to_le_bytes());
    }
    v
}

fn decode_stats(buf: &[u8]) -> DocStats {
    let mut s = DocStats::default();
    if buf.len() < 2 {
        return s;
    }
    let n = u16::from_le_bytes(buf[0..2].try_into().unwrap()) as usize;
    for i in 0..n {
        let o = 2 + i * 6;
        if o + 6 > buf.len() {
            break;
        }
        s.field_len.insert(
            u16::from_le_bytes(buf[o..o + 2].try_into().unwrap()),
            u32::from_le_bytes(buf[o + 2..o + 6].try_into().unwrap()),
        );
    }
    s
}

impl InvertedIndex {
    /// Index one document's analysed fields — appends postings into the
    /// term chains and updates stats inside `txn`.
    pub fn index_document<P: PageIo>(
        txn: &mut Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        index_tree: &Arc<BTree<P>>,
        doc_id: u64,
        fields: &[FieldText],
    ) -> ndb_common::Result<()> {
        let mut stats = DocStats::default();
        for f in fields {
            stats.field_len.insert(f.field_id, f.tokens.len() as u32);
            let mut by_term: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
            for t in &f.tokens {
                by_term.entry(t.text.as_str()).or_default().push(t.position);
            }
            for (term, positions) in by_term {
                Self::append_posting(
                    txn,
                    pool,
                    index_tree,
                    f.field_id,
                    term,
                    Posting {
                        doc_id,
                        tf: positions.len() as u32,
                        positions,
                    },
                )?;
            }
            Self::bump_i64(
                txn,
                index_tree,
                &field_total_key(f.field_id),
                f.tokens.len() as i64,
            )?;
        }
        txn.insert_in(index_tree, &stats_key(doc_id), &encode_stats(&stats))?;
        Self::bump_i64(txn, index_tree, DOC_COUNT_KEY, 1)?;
        Ok(())
    }

    /// Physically removes a document's postings: for each term the doc
    /// contained (`fields` re-analysed by the caller), the term's chain is
    /// rewritten copy-on-write without the doc's posting. Removal is inside
    /// `txn`, so a rollback restores the old chain headers — the old doc's
    /// postings come back atomically with its old content.
    pub fn remove_document<P: PageIo>(
        txn: &mut Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        index_tree: &Arc<BTree<P>>,
        doc_id: u64,
        fields: &[FieldText],
        old_stats: &DocStats,
    ) -> ndb_common::Result<()> {
        for f in fields {
            let mut terms: BTreeSet<&str> = BTreeSet::new();
            for t in &f.tokens {
                terms.insert(t.text.as_str());
            }
            for term in terms {
                let key = term_key(f.field_id, term);
                let postings: Vec<Posting> =
                    Self::postings(txn, pool, index_tree, f.field_id, term)?
                        .into_iter()
                        .filter(|p| p.doc_id != doc_id)
                        .collect();
                if postings.is_empty() {
                    txn.delete_in(index_tree, &key)?;
                } else {
                    let ids = Self::write_chunks(txn, pool, &postings)?;
                    txn.insert_in(index_tree, &key, &encode_chain(&ids))?;
                }
            }
        }
        txn.delete_in(index_tree, &stats_key(doc_id))?;
        Self::bump_i64(txn, index_tree, DOC_COUNT_KEY, -1)?;
        for (f, l) in &old_stats.field_len {
            Self::bump_i64(txn, index_tree, &field_total_key(*f), -(*l as i64))?;
        }
        Ok(())
    }

    /// All postings for `term` in `field_id` across the term's chunks.
    pub fn postings<P: PageIo>(
        txn: &Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        index_tree: &Arc<BTree<P>>,
        field_id: u16,
        term: &str,
    ) -> ndb_common::Result<Vec<Posting>> {
        let Some(raw) = txn.get_in(index_tree, &term_key(field_id, term))? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for pid in decode_chain(&raw)? {
            let g = pool.fetch_read(pid)?;
            let chunk = Chunk::decode(g.page().body());
            drop(g);
            out.extend(chunk?.postings);
        }
        Ok(out)
    }

    /// The deletion tombstone set (doc ids).
    pub fn deleted_set<P: PageIo>(
        txn: &Transaction<'_, P>,
        index_tree: &Arc<BTree<P>>,
    ) -> ndb_common::Result<BTreeSet<u64>> {
        let mut out = BTreeSet::new();
        for (k, _) in txn.scan_in(index_tree)? {
            if k.first() != Some(&DEL_PREFIX) || k.len() != 9 {
                continue;
            }
            out.insert(u64::from_be_bytes(k[1..9].try_into().unwrap()));
        }
        Ok(out)
    }

    /// Ordered terms in `field_id` — the term dictionary (§9.1) used by
    /// fuzzy expansion (§9.6).
    pub fn terms<P: PageIo>(
        txn: &Transaction<'_, P>,
        index_tree: &Arc<BTree<P>>,
        field_id: u16,
    ) -> ndb_common::Result<Vec<String>> {
        let mut prefix = vec![b'T'];
        prefix.extend_from_slice(&field_id.to_be_bytes());
        let mut out = Vec::new();
        for (k, _) in txn.scan_in(index_tree)? {
            if k.starts_with(&prefix) {
                out.push(String::from_utf8_lossy(&k[3..]).into_owned());
            }
        }
        Ok(out)
    }

    pub fn doc_stats<P: PageIo>(
        txn: &Transaction<'_, P>,
        index_tree: &Arc<BTree<P>>,
        doc_id: u64,
    ) -> ndb_common::Result<Option<DocStats>> {
        Ok(txn
            .get_in(index_tree, &stats_key(doc_id))?
            .map(|b| decode_stats(&b)))
    }

    pub fn collection_stats<P: PageIo>(
        txn: &Transaction<'_, P>,
        index_tree: &Arc<BTree<P>>,
        field_ids: &[u16],
    ) -> ndb_common::Result<CollectionStats> {
        let doc_count = Self::read_u64(txn, index_tree, DOC_COUNT_KEY)?.unwrap_or(0);
        let mut field_total_len = BTreeMap::new();
        for f in field_ids {
            field_total_len.insert(
                *f,
                Self::read_u64(txn, index_tree, &field_total_key(*f))?.unwrap_or(0),
            );
        }
        Ok(CollectionStats {
            doc_count,
            field_total_len,
        })
    }

    /// Physical compaction: merge each term's chunks, drop deleted docs,
    /// repack into fresh pages (§9.4 index compaction). Orphans old chunk
    /// pages — reclaimed once the freelist is persisted.
    pub fn compact<P: PageIo>(
        txn: &mut Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        index_tree: &Arc<BTree<P>>,
    ) -> ndb_common::Result<u64> {
        let deleted = Self::deleted_set(txn, index_tree)?;
        let terms: Vec<Vec<u8>> = txn
            .scan_in(index_tree)?
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.first() == Some(&b'T'))
            .collect();
        let mut rewritten = 0u64;
        for k in terms {
            let field_id = u16::from_be_bytes(k[1..3].try_into().unwrap());
            let term = String::from_utf8_lossy(&k[3..]).into_owned();
            let postings: Vec<Posting> = Self::postings(txn, pool, index_tree, field_id, &term)?
                .into_iter()
                .filter(|p| !deleted.contains(&p.doc_id))
                .collect();
            if postings.is_empty() {
                txn.delete_in(index_tree, &k)?;
            } else {
                let ids = Self::write_chunks(txn, pool, &postings)?;
                txn.insert_in(index_tree, &k, &encode_chain(&ids))?;
            }
            rewritten += 1;
        }
        Ok(rewritten)
    }

    // ---- internals -----------------------------------------------------

    /// Copy-on-write append: decode the tail chunk, extend it into a fresh
    /// page if it fits, else seal it and start a new chunk — then repoint
    /// the term's versioned header.
    fn append_posting<P: PageIo>(
        txn: &mut Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        index_tree: &Arc<BTree<P>>,
        field_id: u16,
        term: &str,
        p: Posting,
    ) -> ndb_common::Result<()> {
        let key = term_key(field_id, term);
        let mut ids = txn
            .get_in(index_tree, &key)?
            .map(|b| decode_chain(&b))
            .transpose()?
            .unwrap_or_default();

        // Read the current tail chunk (if any) and try appending.
        let mut tail = if let Some(&last) = ids.last() {
            let g = pool.fetch_read(last)?;
            let c = Chunk::decode(g.page().body());
            drop(g);
            c?
        } else {
            Chunk::new()
        };
        if !try_append(&mut tail, p.clone()) {
            // Tail full — seal it (allocate it as-is) and start a new chunk.
            let sealed = Self::write_chunk_page(pool, &tail)?;
            if let Some(slot) = ids.last_mut() {
                *slot = sealed;
            } else {
                ids.push(sealed);
            }
            tail = Chunk::new();
            if !try_append(&mut tail, p) {
                return Err(Error::Corruption("posting larger than a chunk".into()));
            }
        }
        let pid = Self::write_chunk_page(pool, &tail)?;
        if ids.is_empty() {
            ids.push(pid);
        } else {
            *ids.last_mut().unwrap() = pid;
        }
        txn.insert_in(index_tree, &key, &encode_chain(&ids))?;
        Ok(())
    }

    fn write_chunk_page<P: PageIo>(
        pool: &Arc<BufferPool<P>>,
        chunk: &Chunk,
    ) -> ndb_common::Result<PageId> {
        let pid = pool.allocate_page(PageType::Doc)?;
        {
            let mut g = pool.fetch_write(pid)?;
            let enc = chunk.encode();
            let body = g.page_mut().body_mut();
            body[..enc.len()].copy_from_slice(&enc);
            for b in &mut body[enc.len()..] {
                *b = 0;
            }
        }
        Ok(pid)
    }

    fn write_chunks<P: PageIo>(
        txn: &Transaction<'_, P>,
        pool: &Arc<BufferPool<P>>,
        postings: &[Posting],
    ) -> ndb_common::Result<Vec<PageId>> {
        let _ = txn;
        let mut ids = Vec::new();
        let mut chunk = Chunk::new();
        for p in postings {
            if !try_append(&mut chunk, p.clone()) {
                ids.push(Self::write_chunk_page(pool, &chunk)?);
                chunk = Chunk::new();
                if !try_append(&mut chunk, p.clone()) {
                    return Err(Error::Corruption("posting larger than a chunk".into()));
                }
            }
        }
        if !chunk.postings.is_empty() {
            ids.push(Self::write_chunk_page(pool, &chunk)?);
        }
        Ok(ids)
    }

    fn read_u64<P: PageIo>(
        txn: &Transaction<'_, P>,
        t: &Arc<BTree<P>>,
        key: &[u8],
    ) -> ndb_common::Result<Option<u64>> {
        Ok(txn
            .get_in(t, key)?
            .and_then(|b| b[..8].try_into().ok().map(u64::from_le_bytes)))
    }

    fn bump_i64<P: PageIo>(
        txn: &mut Transaction<'_, P>,
        t: &Arc<BTree<P>>,
        key: &[u8],
        d: i64,
    ) -> ndb_common::Result<()> {
        let cur = Self::read_u64(txn, t, key)?.unwrap_or(0) as i64 + d;
        txn.insert_in(t, key, &(cur as u64).to_le_bytes())
    }
}
