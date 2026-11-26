//! Phase-5 acceptance tests (spec §26 / §9): full-text search — analysis
//! pipeline, BM25 ranking, phrase + fuzzy, deletes + compaction, and
//! index persistence across restart.

use ndb_analyzer::AnalyzerConfig;
use ndb_buffer::BufferPool;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, Schema, Value};
use ndb_index::Engine;
use ndb_page::manager::{PageIo, PageManager};
use ndb_transaction::{TxnConfig, recover};
use ndb_wal::Wal;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct MemIo {
    data: Arc<Mutex<Vec<u8>>>,
}
impl PageIo for MemIo {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> ndb_common::Result<()> {
        let d = self.data.lock().unwrap();
        buf.copy_from_slice(&d[off as usize..off as usize + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, off: u64, data: &[u8]) -> ndb_common::Result<()> {
        let mut d = self.data.lock().unwrap();
        d[off as usize..off as usize + data.len()].copy_from_slice(data);
        Ok(())
    }
    fn len(&mut self) -> ndb_common::Result<u64> {
        Ok(self.data.lock().unwrap().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> ndb_common::Result<()> {
        self.data.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn sync(&mut self) -> ndb_common::Result<()> {
        Ok(())
    }
}

const PAGE_SIZE: u32 = 4096;
const POOL_FRAMES: u64 = 32;

struct Db {
    engine: Engine<MemIo>,
    data: MemIo,
    wal_dir: PathBuf,
}

fn create_db() -> (Db, TempDir) {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let io = MemIo::default();
    let pm = PageManager::create(io.clone(), PAGE_SIZE, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64));
    let (wal, _) = Wal::open(&wal_dir, 0).unwrap();
    let engine = Engine::create(pool, Arc::new(wal), TxnConfig::default()).unwrap();
    (
        Db {
            engine,
            data: io,
            wal_dir,
        },
        tmp,
    )
}

fn crash_and_restart(db: Db) -> Db {
    let Db {
        engine,
        data,
        wal_dir,
    } = db;
    drop(engine);
    let mut pm = PageManager::open(data.clone()).unwrap();
    recover(&wal_dir, 0, &mut pm).unwrap();
    drop(pm);
    let pm = PageManager::open(data.clone()).unwrap();
    let pool = Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64));
    let (wal, _) = Wal::open(&wal_dir, 0).unwrap();
    let engine = Engine::open(pool, Arc::new(wal), TxnConfig::default()).unwrap();
    Db {
        engine,
        data,
        wal_dir,
    }
}

fn doc(fields: &[(&str, Value)]) -> Document {
    let mut d = Document::empty(DocumentId::new(0));
    for (n, v) in fields {
        d.fields.insert(n.to_string(), v.clone());
    }
    d
}

/// Collection "docs" with a text index on title (weight 3) + body (1).
fn boot(db: &Db, docs: &[(&str, &str)]) {
    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine
        .create_collection(&mut txn, "app", "docs", Schema::new())
        .unwrap();
    let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let cfg = AnalyzerConfig::default();
    coll.create_text_index(
        &mut txn,
        "search",
        &[
            ("title".into(), 3.0, cfg.clone()),
            ("body".into(), 1.0, cfg.clone()),
        ],
    )
    .unwrap();
    for (title, body) in docs {
        coll.insert(
            &mut txn,
            doc(&[
                ("title", Value::String(title.to_string())),
                ("body", Value::String(body.to_string())),
            ]),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

fn search_ids(db: &Db, query: &str) -> Vec<u64> {
    let txn = db.engine.txns().begin_write().unwrap();
    let coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = coll.search_text(&txn, "search", query, 10).unwrap();
    txn.commit().unwrap();
    hits.iter().map(|h| h.doc_id.get()).collect()
}

#[test]
fn match_search_ranks_by_bm25() {
    let (db, _tmp) = create_db();
    boot(
        &db,
        &[
            (
                "rust programming",
                "rust is a systems language for speed and safety",
            ),
            (
                "cooking basics",
                "how to cook rice and boil eggs for beginners",
            ),
            (
                "rust book",
                "the rust programming language book about rust rust",
            ),
        ],
    );
    let hits = search_ids(&db, "rust");
    assert_eq!(hits.len(), 2);
    // Doc 3 mentions rust many times (higher tf) — should outrank doc 1.
    let txn = db.engine.txns().begin_write().unwrap();
    let coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = coll.search_text(&txn, "search", "rust", 10).unwrap();
    assert_eq!(hits[0].doc_id.get(), 3);
    assert!(hits[0].score > hits[1].score);
    txn.commit().unwrap();
}

#[test]
fn stop_words_and_stemming_apply() {
    let (db, _tmp) = create_db();
    boot(&db, &[("a", "the cat runs quickly")]);
    // "running" stems to "run"; "the" is a stop word.
    let hits = search_ids(&db, "running");
    assert_eq!(hits, vec![1]);
    assert!(search_ids(&db, "the").is_empty());
}

#[test]
fn phrase_search_respects_order_and_slop() {
    let (db, _tmp) = create_db();
    boot(
        &db,
        &[
            ("a", "the quick brown fox jumps"),
            ("b", "the fox quick brown jumps"),
        ],
    );
    let txn = db.engine.txns().begin().unwrap();
    let coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    // Exact phrase hits doc 1 only.
    let hits = coll
        .search_phrase(&txn, "search", "body", "quick brown fox", 0, 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].doc_id.get(), 1);
    // Slop 2 still only doc 1 (doc 2's order differs by more than slop).
    let hits = coll
        .search_phrase(&txn, "search", "body", "brown quick fox", 0, 10)
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn fuzzy_finds_misspelled_terms() {
    let (db, _tmp) = create_db();
    boot(&db, &[("a", "postgresql is a database")]);
    // "postgreesl" is 2 substitutions away from "postgresql".
    let txn = db.engine.txns().begin().unwrap();
    let coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = coll
        .search_fuzzy(&txn, "search", "body", "postgreesl", 2, 8, 10)
        .unwrap();
    assert_eq!(hits.len(), 1);
    // Beyond the edit bound finds nothing.
    let hits = coll
        .search_fuzzy(&txn, "search", "body", "postgreesl", 1, 8, 10)
        .unwrap();
    assert!(hits.is_empty());
}

#[test]
fn deleted_docs_leave_results_and_compact_is_safe() {
    let (db, _tmp) = create_db();
    boot(
        &db,
        &[
            ("one", "apple banana cherry"),
            ("two", "apple banana grape"),
        ],
    );
    {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
        coll.delete(&mut txn, DocumentId::new(1)).unwrap();
        txn.commit().unwrap();
    }
    // Deleted doc is filtered out.
    assert_eq!(search_ids(&db, "apple"), vec![2]);
    // Compaction physically drops the postings — results stay correct.
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    coll.compact_text_index(&mut txn, "search").unwrap();
    txn.commit().unwrap();
    assert_eq!(search_ids(&db, "apple"), vec![2]);
}

#[test]
fn text_index_survives_restart() {
    let (db, _tmp) = create_db();
    boot(&db, &[("persistent", "data outlives the process")]);
    let db = crash_and_restart(db);
    assert_eq!(search_ids(&db, "outlives"), vec![1]);
    // New writes index correctly after reopen.
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    coll.insert(
        &mut txn,
        doc(&[("body", Value::String("fresh outlives".into()))]),
    )
    .unwrap();
    txn.commit().unwrap();
    assert_eq!(search_ids(&db, "outlives").len(), 2);
}

#[test]
fn field_weighting_changes_ranking() {
    let (db, _tmp) = create_db();
    boot(
        &db,
        &[
            ("database internals", "a boring description"),
            ("internals", "a database of databases"),
        ],
    );
    // "database" appears in title (weight 3) for doc 1, body (weight 1) twice for doc 2.
    let txn = db.engine.txns().begin_write().unwrap();
    let coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = coll.search_text(&txn, "search", "database", 10).unwrap();
    txn.commit().unwrap();
    assert_eq!(hits.len(), 2);
    // Title match at weight 3 should beat two body matches at weight 1.
    assert_eq!(hits[0].doc_id.get(), 1);
}

#[test]
fn update_reindexes_text() {
    let (db, _tmp) = create_db();
    boot(&db, &[("old", "original content")]);
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    coll.update(
        &mut txn,
        DocumentId::new(1),
        doc(&[
            ("title", Value::String("new".into())),
            ("body", Value::String("replacement text".into())),
        ]),
    )
    .unwrap();
    txn.commit().unwrap();
    assert!(search_ids(&db, "original").is_empty());
    assert_eq!(search_ids(&db, "replacement"), vec![1]);
}
