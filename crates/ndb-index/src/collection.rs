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
use crate::meta::{CollectionMeta, IndexDef};
use ndb_btree::BTree;
use ndb_btree::key::{KeyPart, encode_composite};
use ndb_common::error::Error;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, EncodeOptions, Schema, codec};
use ndb_page::manager::PageIo;
use ndb_transaction::Transaction;
use std::sync::Arc;

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
    docstore: DocStore<P>,
    /// The system catalog tree + this collection's key in it.
    catalog: Arc<BTree<P>>,
    catalog_key: Vec<u8>,
    /// In-memory doc-id cursor; `meta.next_doc_id` is the persisted
    /// watermark (ids below it are exhausted or reserved). The watermark
    /// jumps ahead in blocks so the catalog isn't rewritten per insert —
    /// gaps after restart are allowed.
    id_cursor: u64,
    dirty: bool,
}

impl<P: PageIo> Collection<P> {
    /// Opens a collection from its catalog metadata.
    pub(crate) fn open(
        meta: CollectionMeta,
        pool: &Arc<ndb_buffer::BufferPool<P>>,
        catalog: &Arc<BTree<P>>,
        catalog_key: Vec<u8>,
    ) -> ndb_common::Result<Self> {
        let pk = Arc::new(BTree::open(Arc::clone(pool), meta.pk_root)?);
        let mut index_trees = Vec::with_capacity(meta.indexes.len());
        for def in &meta.indexes {
            index_trees.push(Arc::new(BTree::open(Arc::clone(pool), def.root)?));
        }
        let id_cursor = meta.next_doc_id;
        Ok(Self {
            meta,
            pk,
            index_trees,
            docstore: DocStore::new(Arc::clone(pool)),
            catalog: Arc::clone(catalog),
            catalog_key,
            id_cursor,
            dirty: false,
        })
    }

    /// Assigns the next document id from the in-memory cursor, bumping the
    /// persisted watermark a block ahead when the cursor reaches it.
    fn assign_id(&mut self) -> DocumentId {
        let id = self.id_cursor;
        self.id_cursor += 1;
        if self.id_cursor >= self.meta.next_doc_id {
            self.meta.next_doc_id = self.id_cursor + 1023;
            self.dirty = true;
        }
        DocumentId::new(id)
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
            self.id_cursor = self.id_cursor.max(doc.id.get() + 1);
            if self.id_cursor >= self.meta.next_doc_id {
                self.meta.next_doc_id = self.id_cursor + 1023;
                self.dirty = true;
            }
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
