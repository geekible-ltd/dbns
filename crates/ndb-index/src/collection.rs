//! Collection — schema-validated document CRUD with primary + secondary
//! index maintenance (spec §2.1, §7.2, §8.2–8.3).
//!
//! All mutations go through a caller's [`Transaction`], so a multi-document
//! write across indexes commits atomically (§7.3): the pk entry, every
//! secondary index entry, and the record blob share one commit sequence.
//!
//! Primary index key = `u64` doc id big-endian; value = `RecordEntry`
//! (inline bytes or record-chain head — see `docstore`).

use crate::docstore::DocStore;
use crate::index;
use crate::meta::{CollectionMeta, IndexDef, TextFieldDef, TextIndexDef, VectorIndexDef};
use ndb_analyzer::{Analyzer, AnalyzerConfig};
use ndb_bm25::{Bm25, WeightedScorer};
use ndb_btree::BTree;
use ndb_btree::key::{KeyPart, encode_composite};
use ndb_common::error::Error;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, EncodeOptions, Schema, codec};
use ndb_hnsw::{HnswConfig, HnswIndex};
use ndb_hybrid::{Candidate, Leg, fuse as fuseHybrid};
use ndb_inverted::query::{Hit, TextQuery};
use ndb_inverted::{FieldText, InvertedIndex};
use ndb_page::manager::PageIo;
use ndb_transaction::Transaction;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// An open collection — resolves its catalog metadata into live B+Tree
/// handles. Cheap: metadata is small and reads happen through the pool.
///
/// Mutations that change metadata (new dictionary fields, doc-id
/// watermark, index admin) mark the collection dirty; the changed meta is
/// rewritten to the catalog inside the caller's transaction before the op
/// returns — an auto-save, so a reopened handle never sees stale state.
pub struct Collection<P: PageIo> {
    meta: CollectionMeta,
    pk: Arc<BTree<P>>,
    /// Index B+Trees, parallel to `meta.indexes`.
    index_trees: Vec<Arc<BTree<P>>>,
    /// Full-text index trees, parallel to `meta.text_indexes` (§9.1).
    text_trees: Vec<Arc<BTree<P>>>,
    /// HNSW index trees, parallel to `meta.vector_indexes` (§10.1).
    vector_trees: Vec<Arc<BTree<P>>>,
    docstore: DocStore<P>,
    /// The system catalog tree + this collection's key in it.
    catalog: Arc<BTree<P>>,
    catalog_key: Vec<u8>,
    /// Shared doc-id allocator (per catalog key, owned by `Engine`) —
    /// collection handles are opened per request, so the cursor must
    /// outlive individual handles or every fresh handle would restart at
    /// the watermark and waste a whole reservation block.
    ids: Arc<IdAlloc>,
    dirty: bool,
}

/// Shared monotonic doc-id allocator for a collection. `next` is the
/// in-memory cursor; `hi` is the reserved bound mirrored to
/// `meta.next_doc_id` a block (1024 ids) at a time so the catalog isn't
/// rewritten per insert. Gaps after restart are allowed — `hi` is the
/// only state that must persist (it bounds every issued id).
pub(crate) struct IdAlloc {
    next: AtomicU64,
    hi: AtomicU64,
}

impl IdAlloc {
    pub(crate) fn new(watermark: u64) -> Self {
        Self {
            next: AtomicU64::new(watermark),
            hi: AtomicU64::new(watermark),
        }
    }
}

impl<P: PageIo> Collection<P> {
    /// Opens a collection from its catalog metadata.
    pub(crate) fn open(
        meta: CollectionMeta,
        pool: &Arc<ndb_buffer::BufferPool<P>>,
        catalog: &Arc<BTree<P>>,
        catalog_key: Vec<u8>,
        ids: Arc<IdAlloc>,
    ) -> ndb_common::Result<Self> {
        let pk = Arc::new(BTree::open(Arc::clone(pool), meta.pk_root)?);
        let mut index_trees = Vec::with_capacity(meta.indexes.len());
        for def in &meta.indexes {
            index_trees.push(Arc::new(BTree::open(Arc::clone(pool), def.root)?));
        }
        let mut text_trees = Vec::with_capacity(meta.text_indexes.len());
        for def in &meta.text_indexes {
            text_trees.push(Arc::new(BTree::open(Arc::clone(pool), def.root)?));
        }
        let mut vector_trees = Vec::with_capacity(meta.vector_indexes.len());
        for def in meta.vector_indexes.iter() {
            vector_trees.push(Arc::new(BTree::open(Arc::clone(pool), def.root)?));
        }
        Ok(Self {
            meta,
            pk,
            index_trees,
            text_trees,
            vector_trees,
            docstore: DocStore::new(Arc::clone(pool)),
            catalog: Arc::clone(catalog),
            catalog_key,
            ids,
            dirty: false,
        })
    }

    /// Assigns the next document id from the shared cursor, bumping the
    /// persisted watermark a block ahead when the cursor reaches it.
    fn assign_id(&mut self) -> DocumentId {
        let id = self.ids.next.fetch_add(1, Ordering::Relaxed);
        self.ensure_watermark(id + 1);
        DocumentId::new(id)
    }

    /// Reserves ids through `next_id + 1023` in the catalog watermark when
    /// `next_id` crosses the current bound.
    fn ensure_watermark(&mut self, next_id: u64) {
        if next_id >= self.ids.hi.load(Ordering::Relaxed) {
            self.ids.hi.fetch_max(next_id + 1023, Ordering::Relaxed);
            let hi = self.ids.hi.load(Ordering::Relaxed);
            if hi > self.meta.next_doc_id {
                self.meta.next_doc_id = hi;
                self.dirty = true;
            }
        }
    }

    /// Rewrites the collection's catalog entry when meta changed.
    fn auto_save(&mut self, txn: &mut Transaction<'_, P>) -> ndb_common::Result<()> {
        if self.dirty {
            txn.insert_in(&self.catalog, &self.catalog_key, &self.meta.encode())?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Collection metadata (mutable — index admin and the doc-id counter
    /// write it back to the catalog).
    pub fn meta(&self) -> &CollectionMeta {
        &self.meta
    }
    pub fn meta_mut(&mut self) -> &mut CollectionMeta {
        &mut self.meta
    }
    pub fn schema(&self) -> &Schema {
        &self.meta.schema
    }

    /// Approximate live-document count for cost estimation (§13) —
    /// derived from the doc-id cursor; overestimates after deletes.
    pub fn scan_count_hint(&self) -> u64 {
        self.ids.next.load(Ordering::Relaxed).saturating_sub(1)
    }

    fn pk_key(id: DocumentId) -> [u8; 8] {
        id.get().to_be_bytes()
    }

    /// Inserts `doc`. If `doc.id` is 0 the collection assigns the next
    /// monotonic id; a non-zero caller-chosen id must not already exist.
    pub fn insert(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: Document,
    ) -> ndb_common::Result<DocumentId> {
        let mut doc = doc;
        let id = if doc.id.get() == 0 {
            doc.id = self.assign_id();
            doc.id
        } else {
            if txn.get_in(&self.pk, &Self::pk_key(doc.id))?.is_some() {
                return Err(Error::InvalidInput(format!(
                    "duplicate document id {}",
                    doc.id.get()
                )));
            }
            self.ids.next.fetch_max(doc.id.get() + 1, Ordering::Relaxed);
            self.ensure_watermark(doc.id.get() + 1);
            doc.id
        };
        self.insert_inner(txn, &doc, 1)?;
        Ok(id)
    }

    /// Shared insert path: validate, encode, store record, index.
    fn insert_inner(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: &Document,
        doc_version: u32,
    ) -> ndb_common::Result<()> {
        self.meta.schema.validate(doc)?;
        let dict_len = self.meta.dict.len();
        let bytes = codec::encode(
            doc,
            &EncodeOptions {
                collection_id: self.meta.id,
                schema_version: self.meta.schema.version,
                doc_version,
            },
            &mut self.meta.dict,
        )?;
        if self.meta.dict.len() != dict_len {
            self.dirty = true;
        }
        let entry = self.docstore.write(&bytes)?;
        self.write_index_entries(txn, doc)?;
        self.index_text_fields(txn, doc)?;
        self.index_vector_fields(txn, doc)?;
        txn.insert_in(&self.pk, &Self::pk_key(doc.id), &entry)?;
        self.auto_save(txn)
    }

    /// Fetches the document at this transaction's snapshot.
    pub fn get(
        &self,
        txn: &Transaction<'_, P>,
        id: DocumentId,
    ) -> ndb_common::Result<Option<Document>> {
        match txn.get_in(&self.pk, &Self::pk_key(id))? {
            Some(entry) => {
                let bytes = self.docstore.read(&entry)?;
                Ok(Some(codec::decode(&bytes, &self.meta.dict)?))
            }
            None => Ok(None),
        }
    }

    /// Replaces the document under `id`. Index entries are recomputed from
    /// the old version and removed before the new ones are written.
    pub fn update(
        &mut self,
        txn: &mut Transaction<'_, P>,
        id: DocumentId,
        doc: Document,
    ) -> ndb_common::Result<()> {
        let old = self
            .get(txn, id)?
            .ok_or_else(|| Error::InvalidInput(format!("document {} not found", id.get())))?;
        let old_version = match txn.get_in(&self.pk, &Self::pk_key(id))? {
            Some(entry) => {
                let bytes = self.docstore.read(&entry)?;
                codec::EncodedDocument::new(&bytes)?.doc_version()
            }
            None => 0,
        };
        self.remove_index_entries(txn, &old)?;
        self.remove_text_entries(txn, &old)?;
        let mut doc = doc;
        doc.id = id;
        self.insert_inner(txn, &doc, old_version + 1)
    }

    /// Deletes `id`: pk tombstone + index entry removal. Returns whether
    /// the document existed.
    pub fn delete(
        &mut self,
        txn: &mut Transaction<'_, P>,
        id: DocumentId,
    ) -> ndb_common::Result<bool> {
        let Some(old) = self.get(txn, id)? else {
            return Ok(false);
        };
        self.remove_index_entries(txn, &old)?;
        self.remove_text_entries(txn, &old)?;
        self.remove_vector_entries(txn, &old)?;
        txn.delete_in(&self.pk, &Self::pk_key(id))
    }

    /// Bulk insertion (spec §7.2): many documents in the caller's one
    /// transaction — atomic across the batch.
    pub fn insert_many(
        &mut self,
        txn: &mut Transaction<'_, P>,
        docs: Vec<Document>,
    ) -> ndb_common::Result<Vec<DocumentId>> {
        let mut ids = Vec::with_capacity(docs.len());
        for doc in docs {
            ids.push(self.insert(txn, doc)?);
        }
        Ok(ids)
    }

    /// All documents in id order, resolved at the snapshot.
    pub fn scan(
        &self,
        txn: &Transaction<'_, P>,
    ) -> ndb_common::Result<Vec<(DocumentId, Document)>> {
        let mut out = Vec::new();
        for (k, entry) in txn.scan_in(&self.pk)? {
            let id = u64::from_be_bytes(
                k.try_into()
                    .map_err(|_| Error::Corruption("pk: bad key width".into()))?,
            );
            let bytes = self.docstore.read(&entry)?;
            let doc = codec::decode(&bytes, &self.meta.dict)?;
            out.push((DocumentId::new(id), doc));
        }
        Ok(out)
    }

    // ── index admin (spec §8.1, §8.3) ──────────────────────────────────

    /// Creates a secondary index, backfilling from existing documents.
    /// Caller persists the mutated meta afterwards (`save_meta`).
    pub fn create_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
        fields: &[String],
        unique: bool,
        sparse: bool,
    ) -> ndb_common::Result<()> {
        if fields.is_empty() {
            return Err(Error::InvalidInput("index needs at least one field".into()));
        }
        if self.meta.indexes.iter().any(|d| d.name == name) {
            return Err(Error::InvalidInput(format!(
                "index '{name}' already exists"
            )));
        }
        // Indexed fields need not exist in the dictionary yet — documents
        // carrying them may arrive later (missing fields index as the null
        // slot, or are skipped by sparse indexes).
        let tree = Arc::new(BTree::create(Arc::clone(self.docstore.pool()))?);
        let def = IndexDef {
            name: name.to_string(),
            fields: fields.to_vec(),
            unique,
            sparse,
            root: tree.root_id(),
        };
        // Backfill from existing documents.
        for (_id, doc) in self.scan(txn)? {
            if let Some((k, v)) = index::entry(&def, &doc.fields, doc.id.get())? {
                if def.unique && txn.get_in(&tree, &k)?.is_some() {
                    return Err(Error::InvalidInput(format!(
                        "unique index '{name}' violation on existing documents"
                    )));
                }
                txn.insert_in(&tree, &k, &v)?;
            }
        }
        self.meta.indexes.push(def);
        self.index_trees.push(tree);
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Drops a secondary index (its pages are orphaned — freeing arrives
    /// with persisted free lists).
    pub fn drop_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let pos = self
            .meta
            .indexes
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::InvalidInput(format!("index '{name}' not found")))?;
        self.meta.indexes.remove(pos);
        self.index_trees.remove(pos);
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Exact-match lookup: documents whose indexed fields equal `parts`
    /// (one part per indexed field, in order).
    pub fn lookup(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        parts: &[KeyPart],
    ) -> ndb_common::Result<Vec<DocumentId>> {
        let (def, tree) = self.index_pair(name)?;
        let prefix = encode_composite(parts);
        let mut out = Vec::new();
        for (k, v) in txn.scan_in(tree)? {
            // Prefix-free encoding: a unique key either equals the prefix
            // or sorts disjointly; non-unique keys always carry the doc-id
            // suffix beyond it.
            let hit = if def.unique {
                k == prefix
            } else {
                k.len() > prefix.len() && k.starts_with(&prefix[..])
            };
            if hit {
                let id = u64::from_le_bytes(
                    v.try_into()
                        .map_err(|_| Error::Corruption("index: bad doc_id value".into()))?,
                );
                out.push(DocumentId::new(id));
            }
        }
        Ok(out)
    }

    /// Range scan over pre-encoded key bounds (used by the query executor):
    /// `field_key >= lo` and `< hi` (or `<= hi` when `inclusive_hi`).
    /// Returns `(full_key, doc_id)` pairs in key order.
    pub fn lookup_ordered_range(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        lo: &[u8],
        hi: Option<&[u8]>,
        inclusive_hi: bool,
    ) -> ndb_common::Result<Vec<(Vec<u8>, DocumentId)>> {
        let (def, tree) = self.index_pair(name)?;
        let mut out = Vec::new();
        for (k, v) in txn.scan_in(tree)? {
            let field_key = if def.unique || k.len() < 9 {
                k.as_slice()
            } else {
                &k[..k.len() - 9]
            };
            let below = field_key >= lo;
            let above = match hi {
                Some(h) if inclusive_hi => field_key <= h,
                Some(h) => field_key < h,
                None => true,
            };
            if below && above {
                let id = u64::from_le_bytes(
                    v.try_into()
                        .map_err(|_| Error::Corruption("index: bad doc_id value".into()))?,
                );
                out.push((k, DocumentId::new(id)));
            }
        }
        Ok(out)
    }

    /// Index entries ordered by key — range scans fall out of the B+Tree's
    /// key order (spec §8.3 ordered/range indexes).
    pub fn lookup_ordered(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        lo: &[KeyPart],
        hi: &[KeyPart],
    ) -> ndb_common::Result<Vec<(Vec<u8>, DocumentId)>> {
        let (def, tree) = self.index_pair(name)?;
        let lo_k = encode_composite(lo);
        let hi_k = encode_composite(hi);
        let mut out = Vec::new();
        for (k, v) in txn.scan_in(tree)? {
            let field_key = if def.unique || k.len() < 9 {
                k.as_slice()
            } else {
                &k[..k.len() - 9] // strip tag+u64 doc-id suffix
            };
            if field_key >= lo_k.as_slice() && field_key < hi_k.as_slice() {
                let id = u64::from_le_bytes(
                    v.try_into()
                        .map_err(|_| Error::Corruption("index: bad doc_id value".into()))?,
                );
                out.push((k, DocumentId::new(id)));
            }
        }
        Ok(out)
    }

    /// Rebuilds an index from documents — validation/repair (spec §8.1).
    /// Replaces the tree root with a freshly backfilled tree.
    pub fn rebuild_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let pos = self
            .meta
            .indexes
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::InvalidInput(format!("index '{name}' not found")))?;
        let def = self.meta.indexes[pos].clone();
        let tree = Arc::new(BTree::create(Arc::clone(self.docstore.pool()))?);
        for (_id, doc) in self.scan(txn)? {
            if let Some((k, v)) = index::entry(&def, &doc.fields, doc.id.get())? {
                txn.insert_in(&tree, &k, &v)?;
            }
        }
        self.meta.indexes[pos].root = tree.root_id();
        self.index_trees[pos] = tree;
        self.dirty = true;
        self.auto_save(txn)
    }

    fn index_pair(&self, name: &str) -> ndb_common::Result<(&IndexDef, &Arc<BTree<P>>)> {
        let pos = self
            .meta
            .indexes
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::InvalidInput(format!("index '{name}' not found")))?;
        Ok((&self.meta.indexes[pos], &self.index_trees[pos]))
    }

    /// Writes this document's entries into every secondary index.
    fn write_index_entries(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: &Document,
    ) -> ndb_common::Result<()> {
        for (def, tree) in self.meta.indexes.iter().zip(&self.index_trees) {
            if let Some((k, v)) = index::entry(def, &doc.fields, doc.id.get())? {
                if def.unique
                    && let Some(owner) = txn.get_in(tree, &k)?
                {
                    let owner = u64::from_le_bytes(
                        owner
                            .try_into()
                            .map_err(|_| Error::Corruption("index: bad owner".into()))?,
                    );
                    if owner != doc.id.get() {
                        return Err(Error::InvalidInput(format!(
                            "unique index '{}' violation: doc {} already holds the key",
                            def.name, owner
                        )));
                    }
                }
                txn.insert_in(tree, &k, &v)?;
            }
        }
        Ok(())
    }

    fn remove_index_entries(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: &Document,
    ) -> ndb_common::Result<()> {
        for (def, tree) in self.meta.indexes.iter().zip(&self.index_trees) {
            if let Some((k, _)) = index::entry(def, &doc.fields, doc.id.get())? {
                txn.delete_in(tree, &k)?;
            }
        }
        Ok(())
    }
}

// ── full-text search (spec §9) ─────────────────────────────────────────

/// A scored document hit from `search_text`/`search_phrase`/`search_fuzzy`.
#[derive(Debug)]
pub struct TextHit {
    pub doc_id: DocumentId,
    pub doc: Document,
    pub score: f64,
}

impl<P: PageIo> Collection<P> {
    /// Creates a full-text index over `fields` — `(field, weight,
    /// analyzer)` triples, so analysis is configured per field (§9.2) and
    /// weighted per field (§9.3). Backfills from existing documents.
    ///
    /// The analyzer's `version` is pinned in the def — bumping it requires
    /// `rebuild_text_index`; there is no silent tokenisation mix (§9.2).
    pub fn create_text_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
        fields: &[(String, f64, AnalyzerConfig)],
    ) -> ndb_common::Result<()> {
        if fields.is_empty() {
            return Err(Error::InvalidInput(
                "text index needs at least one field".into(),
            ));
        }
        if self.meta.text_indexes.iter().any(|d| d.name == name)
            || self.meta.indexes.iter().any(|d| d.name == name)
        {
            return Err(Error::InvalidInput(format!(
                "index '{name}' already exists"
            )));
        }
        let tree = Arc::new(BTree::create(Arc::clone(self.docstore.pool()))?);
        let def = TextIndexDef {
            name: name.to_string(),
            fields: fields
                .iter()
                .map(|(f, w, a)| TextFieldDef {
                    field: f.clone(),
                    weight: *w,
                    analyzer_cfg: Analyzer::encode_cfg(a),
                    analyzer_version: a.version,
                })
                .collect(),
            root: tree.root_id(),
        };
        self.meta.text_indexes.push(def);
        self.text_trees.push(Arc::clone(&tree));
        self.dirty = true;
        // Backfill through the normal maintenance path.
        let docs: Vec<Document> = self.scan(txn)?.into_iter().map(|(_, d)| d).collect();
        for doc in &docs {
            self.index_one_text(txn, &tree, self.meta.text_indexes.len() - 1, doc)?;
        }
        self.auto_save(txn)
    }

    /// Drops a text index (its pages are orphaned — page freeing arrives
    /// with the persisted free list).
    pub fn drop_text_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let pos = self.text_pos(name)?;
        self.meta.text_indexes.remove(pos);
        self.text_trees.remove(pos);
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Rebuilds a text index — required after an analyzer version change
    /// (§9.2) and used for repair. Backfills into a fresh tree and swaps
    /// the root, all inside the caller's transaction.
    pub fn rebuild_text_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let pos = self.text_pos(name)?;
        let tree = Arc::new(BTree::create(Arc::clone(self.docstore.pool()))?);
        self.meta.text_indexes[pos].root = tree.root_id();
        self.text_trees[pos] = Arc::clone(&tree);
        let docs: Vec<Document> = self.scan(txn)?.into_iter().map(|(_, d)| d).collect();
        for doc in &docs {
            self.index_one_text(txn, &tree, pos, doc)?;
        }
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Physical compaction of a text index's posting chunks, dropping
    /// deleted-doc postings (spec §9.4 index compaction).
    pub fn compact_text_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<u64> {
        let pos = self.text_pos(name)?;
        InvertedIndex::compact(txn, self.docstore.pool(), &self.text_trees[pos])
    }

    /// BM25 match search (§9.3): `query_text` is analysed per indexed
    /// field with that field's analyzer; hits merge across fields with
    /// their configured weights.
    pub fn search_text(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        query_text: &str,
        top_k: usize,
    ) -> ndb_common::Result<Vec<TextHit>> {
        let (def, tree) = self.text_pair(name)?;
        let bm25 = Bm25::default();
        let mut scores: std::collections::BTreeMap<u64, f64> = Default::default();
        for f in &def.fields {
            let Some(fid) = self.meta.dict.id(&f.field) else {
                continue;
            };
            let cfg = Analyzer::decode_cfg(&f.analyzer_cfg)?;
            let analyzer = Analyzer::new(cfg);
            let terms: Vec<String> = analyzer
                .analyze(query_text)
                .into_iter()
                .map(|t| t.text)
                .collect();
            if terms.is_empty() {
                continue;
            }
            let q = TextQuery::Match {
                field_id: fid.get() as u16,
                terms,
                must_all: false,
            };
            let scorer = WeightedScorer {
                bm25: &bm25,
                weight: f.weight,
            };
            for Hit { doc_id, score } in ndb_inverted::query::search(
                txn,
                self.docstore.pool(),
                tree,
                &q,
                &scorer,
                fid.get() as u16,
                top_k,
            )? {
                *scores.entry(doc_id).or_default() += score;
            }
        }
        self.materialise_hits(txn, scores, top_k)
    }

    /// Phrase search on one indexed field with positional slop (§9.5).
    pub fn search_phrase(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        field: &str,
        phrase: &str,
        slop: u32,
        top_k: usize,
    ) -> ndb_common::Result<Vec<TextHit>> {
        let (def, tree) = self.text_pair(name)?;
        let fdef = def
            .fields
            .iter()
            .find(|f| f.field == field)
            .ok_or_else(|| Error::InvalidInput(format!("field '{field}' not in index '{name}'")))?;
        let fid = self
            .meta
            .dict
            .id(field)
            .ok_or_else(|| Error::InvalidInput(format!("field '{field}' never interned")))?;
        let analyzer = Analyzer::new(Analyzer::decode_cfg(&fdef.analyzer_cfg)?);
        let terms: Vec<String> = analyzer
            .analyze(phrase)
            .into_iter()
            .map(|t| t.text)
            .collect();
        if terms.is_empty() {
            return Ok(vec![]);
        }
        let q = TextQuery::Phrase {
            field_id: fid.get() as u16,
            terms,
            slop,
        };
        let bm25 = Bm25::default();
        let scorer = WeightedScorer {
            bm25: &bm25,
            weight: fdef.weight,
        };
        let hits = ndb_inverted::query::search(
            txn,
            self.docstore.pool(),
            tree,
            &q,
            &scorer,
            fid.get() as u16,
            top_k,
        )?;
        let scores = hits.into_iter().map(|h| (h.doc_id, h.score)).collect();
        self.materialise_hits(txn, scores, top_k)
    }

    /// Fuzzy search (§9.6): expand `term` over the field's term dictionary
    /// within `max_edits`/`max_expansions`, then OR-match the expansions.
    #[allow(clippy::too_many_arguments)] // mirrors spec §9.6 knobs
    pub fn search_fuzzy(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        field: &str,
        term: &str,
        max_edits: u32,
        max_expansions: usize,
        top_k: usize,
    ) -> ndb_common::Result<Vec<TextHit>> {
        let (def, tree) = self.text_pair(name)?;
        let fdef = def
            .fields
            .iter()
            .find(|f| f.field == field)
            .ok_or_else(|| Error::InvalidInput(format!("field '{field}' not in index '{name}'")))?;
        let fid = self
            .meta
            .dict
            .id(field)
            .ok_or_else(|| Error::InvalidInput(format!("field '{field}' never interned")))?;
        let analyzer = Analyzer::new(Analyzer::decode_cfg(&fdef.analyzer_cfg)?);
        let base = analyzer
            .analyze(term)
            .into_iter()
            .map(|t| t.text)
            .next()
            .ok_or_else(|| Error::InvalidInput("fuzzy term analysed to nothing".into()))?;
        let expanded = ndb_fuzzy::expand(
            txn,
            tree,
            fid.get() as u16,
            &base,
            max_edits,
            max_expansions,
        )?;
        if expanded.is_empty() {
            return Ok(vec![]);
        }
        let q = TextQuery::Match {
            field_id: fid.get() as u16,
            terms: expanded,
            must_all: false,
        };
        let bm25 = Bm25::default();
        let scorer = WeightedScorer {
            bm25: &bm25,
            weight: fdef.weight,
        };
        let hits = ndb_inverted::query::search(
            txn,
            self.docstore.pool(),
            tree,
            &q,
            &scorer,
            fid.get() as u16,
            top_k,
        )?;
        let scores = hits.into_iter().map(|h| (h.doc_id, h.score)).collect();
        self.materialise_hits(txn, scores, top_k)
    }

    // ---- internals -----------------------------------------------------

    fn text_pos(&self, name: &str) -> ndb_common::Result<usize> {
        self.meta
            .text_indexes
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::InvalidInput(format!("text index '{name}' not found")))
    }

    fn text_pair(&self, name: &str) -> ndb_common::Result<(&TextIndexDef, &Arc<BTree<P>>)> {
        let pos = self.text_pos(name)?;
        Ok((&self.meta.text_indexes[pos], &self.text_trees[pos]))
    }

    /// Hydrate `(doc_id → score)` into scored documents.
    fn materialise_hits(
        &self,
        txn: &Transaction<'_, P>,
        scores: std::collections::BTreeMap<u64, f64>,
        top_k: usize,
    ) -> ndb_common::Result<Vec<TextHit>> {
        let mut ranked: Vec<(u64, f64)> = scores.into_iter().collect();
        ranked.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        ranked.truncate(top_k);
        let mut out = Vec::with_capacity(ranked.len());
        for (id, score) in ranked {
            if let Some(doc) = self.get(txn, DocumentId::new(id))? {
                out.push(TextHit {
                    doc_id: DocumentId::new(id),
                    doc,
                    score,
                });
            }
        }
        Ok(out)
    }

    /// Index one document into `tree` at `meta.text_indexes[pos]`.
    fn index_one_text(
        &mut self,
        txn: &mut Transaction<'_, P>,
        tree: &Arc<BTree<P>>,
        pos: usize,
        doc: &Document,
    ) -> ndb_common::Result<()> {
        let field_texts = self.analyze_for_index(pos, doc)?;
        if field_texts.is_empty() {
            return Ok(());
        }
        InvertedIndex::index_document(txn, self.docstore.pool(), tree, doc.id.get(), &field_texts)
    }

    /// Analyses `doc`'s fields per `text_indexes[pos]` — shared by the
    /// index and remove paths so they agree on the term set.
    fn analyze_for_index(
        &mut self,
        pos: usize,
        doc: &Document,
    ) -> ndb_common::Result<Vec<FieldText>> {
        let def = self.meta.text_indexes[pos].clone();
        let mut field_texts = Vec::new();
        for f in &def.fields {
            let Some(fid) = self.meta.dict.id(&f.field) else {
                continue;
            };
            let value = doc.get(&f.field);
            let text = match value {
                Some(ndb_document::Value::String(s)) => s.clone(),
                Some(ndb_document::Value::Array(items)) => items
                    .iter()
                    .filter_map(|v| match v {
                        ndb_document::Value::String(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
                _ => continue, // non-text or absent field: nothing indexed
            };
            let analyzer = Analyzer::new(Analyzer::decode_cfg(&f.analyzer_cfg)?);
            field_texts.push(FieldText {
                field_id: fid.get() as u16,
                tokens: analyzer.analyze(&text),
            });
        }
        Ok(field_texts)
    }

    /// Writes this document's postings into every text index.
    fn index_text_fields(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: &Document,
    ) -> ndb_common::Result<()> {
        for pos in 0..self.meta.text_indexes.len() {
            let tree = Arc::clone(&self.text_trees[pos]);
            self.index_one_text(txn, &tree, pos, doc)?;
        }
        Ok(())
    }

    /// Physically removes this doc's postings from every text index —
    /// re-analysing the old document with each index's analyzers gives the
    /// term set to strip (§9.4; stragglers from analyzer-version skew are
    /// cleaned by `compact_text_index`).
    fn remove_text_entries(
        &mut self,
        txn: &mut Transaction<'_, P>,
        old: &Document,
    ) -> ndb_common::Result<()> {
        for pos in 0..self.meta.text_indexes.len() {
            let tree = Arc::clone(&self.text_trees[pos]);
            let Some(stats) = InvertedIndex::doc_stats(txn, &tree, old.id.get())? else {
                continue;
            };
            let field_texts = self.analyze_for_index(pos, old)?;
            InvertedIndex::remove_document(
                txn,
                self.docstore.pool(),
                &tree,
                old.id.get(),
                &field_texts,
                &stats,
            )?;
        }
        Ok(())
    }
}

/// Extracts the `Value::Vector` for an index field — dotted paths traverse
/// nested objects the same way the executor resolves them (§10.1).
fn vector_field_value<'v>(doc: &'v Document, field: &str) -> Option<&'v Vec<f32>> {
    let mut cur: Option<&ndb_document::Value> = doc.fields.get(field);
    if cur.is_none() && field.contains('.') {
        let mut parts = field.split('.');
        let first = parts.next().unwrap_or(field);
        let mut node = doc.fields.get(first);
        for part in parts {
            match node {
                Some(ndb_document::Value::Object(map)) => node = map.get(part),
                _ => {
                    node = None;
                    break;
                }
            }
        }
        cur = node;
    }
    match cur {
        Some(ndb_document::Value::Vector(v)) => Some(v),
        _ => None,
    }
}

/// One row returned by `search_vector`/`search_hybrid`.
pub struct VectorHit {
    pub doc_id: DocumentId,
    pub doc: Document,
    /// Metric-aligned score — higher is nearer (cosine/dot positive,
    /// euclidean = -distance).
    pub score: f64,
    /// Legs that contributed this hit (`Leg::Vector` for pure vector search).
    pub legs: Vec<String>,
}

impl<P: PageIo> Collection<P> {
    fn vector_pos(&self, name: &str) -> ndb_common::Result<usize> {
        self.meta
            .vector_indexes
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::NotFound(format!("vector index '{name}' not found")))
    }

    fn vector_pair(&self, name: &str) -> ndb_common::Result<(&VectorIndexDef, &Arc<BTree<P>>)> {
        let pos = self.vector_pos(name)?;
        Ok((&self.meta.vector_indexes[pos], &self.vector_trees[pos]))
    }

    fn hnsw(&self, name: &str) -> ndb_common::Result<(&VectorIndexDef, HnswIndex<P>)> {
        let (def, tree) = self.vector_pair(name)?;
        Ok((def, HnswIndex::open(Arc::clone(tree))))
    }

    /// Creates an HNSW index over `field` (a `Value::Vector` document field)
    /// and backfills from existing documents (spec §10.2, §10.5).
    pub fn create_vector_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
        field: &str,
        cfg: HnswConfig,
    ) -> ndb_common::Result<()> {
        if self.meta.vector_indexes.iter().any(|d| d.name == name)
            || self.meta.text_indexes.iter().any(|d| d.name == name)
            || self.meta.indexes.iter().any(|d| d.name == name)
        {
            return Err(Error::InvalidInput(format!(
                "index '{name}' already exists"
            )));
        }
        if cfg.dims == 0 {
            return Err(Error::InvalidInput("vector index dims must be > 0".into()));
        }
        let tree = Arc::new(BTree::create(Arc::clone(self.docstore.pool()))?);
        let hnsw = HnswIndex::create(Arc::clone(&tree), txn, cfg.clone())?;

        // Backfill — docs whose field already carries a conforming vector.
        for (_key, entry) in txn.scan_in(&self.pk)? {
            let bytes = self.docstore.read(&entry)?;
            let doc = codec::decode(&bytes, &self.meta.dict)?;
            if let Some(v) = vector_field_value(&doc, field)
                && v.len() == cfg.dims as usize
            {
                hnsw.insert(txn, doc.id.get(), v)?;
            }
        }

        self.meta.vector_indexes.push(VectorIndexDef {
            name: name.to_string(),
            field: field.to_string(),
            dims: cfg.dims,
            metric: cfg.metric as u8,
            m: cfg.m as u16,
            ef_construction: cfg.ef_construction as u16,
            ef_search: cfg.ef_search as u16,
            root: tree.root_id(),
        });
        self.vector_trees.push(tree);
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Drops a vector index definition + its tree pointer (§10.5 admin).
    pub fn drop_vector_index(
        &mut self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let pos = self.vector_pos(name)?;
        self.meta.vector_indexes.remove(pos);
        self.vector_trees.remove(pos);
        self.dirty = true;
        self.auto_save(txn)
    }

    /// Per-doc maintenance on insert/update (§10.5): the HNSW `update`
    /// reconciles add/change; absent fields become a delete (doc updated to
    /// no longer carry the field).
    fn index_vector_fields(
        &mut self,
        txn: &mut Transaction<'_, P>,
        doc: &Document,
    ) -> ndb_common::Result<()> {
        for pos in 0..self.meta.vector_indexes.len() {
            let tree = Arc::clone(&self.vector_trees[pos]);
            let field = self.meta.vector_indexes[pos].field.clone();
            let hnsw = HnswIndex::<P>::open(tree);
            match vector_field_value(doc, &field) {
                Some(v) => {
                    // insert fails on duplicate id → use update for idempotent
                    // maintenance (update() = delete+insert, see §10.5).
                    hnsw.update(txn, doc.id.get(), v)?;
                }
                None => {
                    hnsw.delete(txn, doc.id.get())?;
                }
            }
        }
        Ok(())
    }

    /// Removes the doc's vector from every HNSW index (§10.5 delete path).
    fn remove_vector_entries(
        &mut self,
        txn: &mut Transaction<'_, P>,
        old: &Document,
    ) -> ndb_common::Result<()> {
        for pos in 0..self.meta.vector_indexes.len() {
            let tree = Arc::clone(&self.vector_trees[pos]);
            HnswIndex::<P>::open(tree).delete(txn, old.id.get())?;
        }
        Ok(())
    }

    /// Approximate k-NN (spec §10.4): returns the top-k documents by metric
    /// score — payload fetched only for the surviving ids (§11.3 late
    /// materialization applies here too).
    pub fn search_vector(
        &self,
        txn: &Transaction<'_, P>,
        name: &str,
        vector: &[f32],
        k: usize,
        ef: usize,
    ) -> ndb_common::Result<Vec<VectorHit>> {
        let (_def, hnsw) = self.hnsw(name)?;
        let hits = hnsw.search(txn, vector, k, ef, None)?;
        let mut out = Vec::with_capacity(hits.len());
        for (doc_id, score) in hits {
            if let Some(doc) = self.get(txn, DocumentId::new(doc_id))? {
                out.push(VectorHit {
                    doc_id: DocumentId::new(doc_id),
                    doc,
                    score: score as f64,
                    legs: vec!["vector".to_string()],
                });
            }
        }
        Ok(out)
    }

    /// Hybrid execution (spec §11.3): validates the query, runs the text +
    /// vector legs as compact `(doc_id, score)` candidates, applies the
    /// candidate filter before RRF, fetches payloads only for the final set,
    /// and returns fused hits.
    pub fn search_hybrid(
        &self,
        txn: &Transaction<'_, P>,
        query: &ndb_hybrid::HybridQuery,
    ) -> ndb_common::Result<Vec<VectorHit>> {
        query.validate()?; // §11.3 step 1 — validate
        let fetch_k = if query.fetch_k > 0 {
            query.fetch_k
        } else {
            query.limit.saturating_mul(4).max(query.limit)
        };
        let pred = query.candidate_filter.as_deref();

        // §11.3 steps 4–5 — retrieval legs (compact candidates only).
        let mut legs: Vec<(Leg, Vec<Candidate>)> = Vec::new();
        if let Some(text_leg) = &query.text {
            let hits = self.search_text(txn, &text_leg.index, &text_leg.query, fetch_k)?;
            let cands: Vec<Candidate> = hits
                .into_iter()
                .map(|h| Candidate {
                    doc: h.doc_id.get(),
                    score: h.score as f32,
                })
                .collect();
            legs.push((Leg::Text, cands));
        }
        if let Some(vec_leg) = &query.vector {
            let (_def, hnsw) = self.hnsw(&vec_leg.index)?;
            let ef = if vec_leg.ef > 0 {
                vec_leg.ef
            } else {
                fetch_k.max(16)
            };
            let hits = hnsw.search(txn, &vec_leg.vector, fetch_k, ef, pred)?;
            legs.push((
                Leg::Vector,
                hits.into_iter()
                    .map(|(d, s)| Candidate { doc: d, score: s })
                    .collect(),
            ));
        }

        // §11.3 steps 6–7 — merge + fusion scores (candidate filter applied
        // to non-vector legs as well so excluded ids can't win rank slots).
        if pred.is_some() {
            for (_kind, list) in legs.iter_mut() {
                list.retain(|c| pred.unwrap()(c.doc));
            }
        }
        let fused = fuseHybrid(query, &legs, query.limit);

        // §11.3 steps 8–10 — fetch payloads + final ranking.
        let mut out = Vec::with_capacity(fused.len());
        for hit in fused {
            if let Some(doc) = self.get(txn, DocumentId::new(hit.doc))? {
                out.push(VectorHit {
                    doc_id: DocumentId::new(hit.doc),
                    doc,
                    score: hit.score,
                    legs: hit
                        .legs
                        .iter()
                        .map(|l| format!("{l:?}").to_lowercase())
                        .collect(),
                });
            }
        }
        Ok(out)
    }
}
