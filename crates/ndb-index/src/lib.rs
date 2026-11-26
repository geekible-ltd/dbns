//! `ndb-index` — database and collection management on the transactional
//! storage engine (spec §2.1, §8.1).
//!
//! ```text
//! Engine ── owns ──> TxnManager (its internal B+Tree is the system catalog)
//!   catalog keys:
//!     "db:{name}"        → database marker (u32 version tag)
//!     "coll:{db}:{name}" → encoded CollectionMeta (pk/index roots, field
//!                          dictionary, schema)
//! Collection ── opens ──> pk BTree (doc_id → RecordEntry) + index BTrees
//! DocStore ── inline-or-chained record blobs across Doc pages
//! ```
//!
//! Every catalog and collection mutation goes through a `Transaction`, so
//! schema/index changes and document writes share the commit boundaries of
//! the caller's transaction (spec §7.2–7.3).

mod collection;
mod docstore;
mod index;
mod meta;

pub use collection::Collection;
pub use docstore::{DocStore, INLINE_LIMIT};
pub use meta::{CollectionMeta, IndexDef};

use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::error::Error;
use ndb_common::ids::CollectionId;
use ndb_document::Schema;
use ndb_page::manager::PageIo;
use ndb_transaction::{Transaction, TxnManager};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The database engine: catalog + collection managers over a [`TxnManager`].
pub struct Engine<P: PageIo> {
    txns: TxnManager<P>,
    pool: Arc<BufferPool<P>>,
    next_collection_id: AtomicU64,
    /// Live doc-id allocators keyed by catalog key — shared across the
    /// per-request `Collection` handles so the in-memory cursor (and the
    /// amortized watermark block) isn't reset on every `collection()` call.
    id_allocs: std::sync::Mutex<std::collections::HashMap<Vec<u8>, Arc<collection::IdAlloc>>>,
}

impl<P: PageIo> Engine<P> {
    /// Creates a fresh engine (empty catalog) over an uninitialised
    /// pool + WAL.
    pub fn create(
        pool: Arc<BufferPool<P>>,
        wal: Arc<ndb_wal::Wal>,
        cfg: ndb_transaction::TxnConfig,
    ) -> ndb_common::Result<Self> {
        Ok(Self {
            txns: TxnManager::create(Arc::clone(&pool), wal, cfg)?,
            pool,
            next_collection_id: AtomicU64::new(1),
            id_allocs: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// Opens an engine over recovered state — the catalog is wherever the
    /// catalog page says it was.
    pub fn open(
        pool: Arc<BufferPool<P>>,
        wal: Arc<ndb_wal::Wal>,
        cfg: ndb_transaction::TxnConfig,
    ) -> ndb_common::Result<Self> {
        let txns = TxnManager::open(Arc::clone(&pool), wal, cfg)?;
        // Collection ids must not collide with surviving ones: seed past
        // the largest id found in the catalog.
        let mut max_id = 0u64;
        {
            let txn = txns.begin()?;
            for (k, v) in txn.scan()? {
                if k.starts_with(b"coll:") {
                    let meta = CollectionMeta::decode(&v)?;
                    max_id = max_id.max(meta.id.get());
                }
            }
        }
        Ok(Self {
            txns,
            pool,
            next_collection_id: AtomicU64::new(max_id + 1),
            id_allocs: std::sync::Mutex::new(std::collections::HashMap::new()),
        })
    }

    /// The underlying transaction manager.
    /// The buffer pool — needed by the aggregation pipeline's `$group`
    /// spill and admin storage stats (§15.7).
    pub fn pool(&self) -> &Arc<BufferPool<P>> {
        &self.pool
    }

    pub fn txns(&self) -> &TxnManager<P> {
        &self.txns
    }

    /// Checkpoint — flushes dirty pages and retires WAL segments.
    pub fn checkpoint(&self) -> ndb_common::Result<()> {
        self.txns.checkpoint()
    }

    // ── database manager (spec §2.1) ──────────────────────────────────

    /// Creates a database namespace.
    pub fn create_database(
        &self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        valid_name(name)?;
        let key = db_key(name);
        if txn.get(&key)?.is_some() {
            return Err(Error::InvalidInput(format!(
                "database '{name}' already exists"
            )));
        }
        txn.insert(&key, &[1])
    }

    /// Drops a database and every collection it contains.
    pub fn drop_database(
        &self,
        txn: &mut Transaction<'_, P>,
        name: &str,
    ) -> ndb_common::Result<()> {
        let prefix = coll_prefix(name);
        let keys: Vec<Vec<u8>> = txn
            .scan()?
            .into_iter()
            .map(|(k, _)| k)
            .filter(|k| k.starts_with(&prefix[..]))
            .collect();
        for k in keys {
            txn.delete(&k)?;
        }
        txn.delete(&db_key(name))?;
        Ok(())
    }

    /// All database names visible at the snapshot.
    pub fn list_databases(&self, txn: &Transaction<'_, P>) -> ndb_common::Result<Vec<String>> {
        let mut out = Vec::new();
        for (k, _) in txn.scan()? {
            if let Some(rest) = k.strip_prefix(b"db:".as_slice()) {
                out.push(String::from_utf8_lossy(rest).to_string());
            }
        }
        Ok(out)
    }

    // ── collection manager (spec §2.1, §8.1) ──────────────────────────

    /// Creates a collection (implicitly creates its database if needed).
    /// Returns the collection id.
    pub fn create_collection(
        &self,
        txn: &mut Transaction<'_, P>,
        db: &str,
        name: &str,
        schema: Schema,
    ) -> ndb_common::Result<CollectionId> {
        valid_name(db)?;
        valid_name(name)?;
        let key = coll_key(db, name);
        if txn.get(&key)?.is_some() {
            return Err(Error::InvalidInput(format!(
                "collection '{db}:{name}' already exists"
            )));
        }
        if txn.get(&db_key(db))?.is_none() {
            txn.insert(&db_key(db), &[1])?;
        }
        let pk = BTree::create(Arc::clone(&self.pool))?;
        let id = CollectionId::new(self.next_collection_id.fetch_add(1, Ordering::Relaxed));
        let meta = CollectionMeta {
            id,
            database: db.to_string(),
            name: name.to_string(),
            pk_root: pk.root_id(),
            next_doc_id: 1,
            dict: Default::default(),
            schema,
            indexes: Vec::new(),
            text_indexes: Vec::new(),
            vector_indexes: Vec::new(),
        };
        txn.insert(&key, &meta.encode())?;
        Ok(id)
    }

    /// Drops a collection (its trees' pages are orphaned — freeing arrives
    /// with persisted free lists).
    pub fn drop_collection(
        &self,
        txn: &mut Transaction<'_, P>,
        db: &str,
        name: &str,
    ) -> ndb_common::Result<()> {
        txn.delete(&coll_key(db, name))?;
        self.id_allocs.lock().unwrap().remove(&coll_key(db, name));
        Ok(())
    }

    /// Opens a collection for reads and writes at the transaction's
    /// snapshot.
    pub fn collection(
        &self,
        txn: &Transaction<'_, P>,
        db: &str,
        name: &str,
    ) -> ndb_common::Result<Option<Collection<P>>> {
        match txn.get(&coll_key(db, name))? {
            Some(bytes) => {
                let meta = CollectionMeta::decode(&bytes)?;
                let key = coll_key(db, name);
                let ids = self
                    .id_allocs
                    .lock()
                    .unwrap()
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(collection::IdAlloc::new(meta.next_doc_id)))
                    .clone();
                Ok(Some(Collection::open(
                    meta,
                    &self.pool,
                    self.txns.btree(),
                    key,
                    ids,
                )?))
            }
            None => Ok(None),
        }
    }

    /// Collection names in a database, at the snapshot.
    pub fn list_collections(
        &self,
        txn: &Transaction<'_, P>,
        db: &str,
    ) -> ndb_common::Result<Vec<String>> {
        let prefix = coll_prefix(db);
        let mut out = Vec::new();
        for (k, _) in txn.scan()? {
            if let Some(rest) = k.strip_prefix(&prefix[..]) {
                out.push(String::from_utf8_lossy(rest).to_string());
            }
        }
        Ok(out)
    }

    /// Persists a collection's mutated metadata (after `create_index` /
    /// `drop_index` / `rebuild_index` / doc-id counter drift).
    pub fn save_meta(
        &self,
        txn: &mut Transaction<'_, P>,
        coll: &Collection<P>,
    ) -> ndb_common::Result<()> {
        let meta = coll.meta();
        txn.insert(&coll_key(&meta.database, &meta.name), &meta.encode())
    }
}

fn db_key(name: &str) -> Vec<u8> {
    format!("db:{name}").into_bytes()
}
fn coll_prefix(db: &str) -> Vec<u8> {
    format!("coll:{db}:").into_bytes()
}
fn coll_key(db: &str, name: &str) -> Vec<u8> {
    format!("coll:{db}:{name}").into_bytes()
}

fn valid_name(name: &str) -> ndb_common::Result<()> {
    if name.is_empty() || name.len() > 128 || name.contains(':') || name.contains('\0') {
        return Err(Error::InvalidInput(format!("invalid name '{name}'")));
    }
    Ok(())
}
