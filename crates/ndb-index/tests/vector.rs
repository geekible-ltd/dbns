//! Phase-7 acceptance tests (spec §26): vector storage, HNSW search, vector
//! updates/filtering, hybrid RRF search, persistence across restart.

use ndb_buffer::BufferPool;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, Schema, Value};
use ndb_hnsw::HnswConfig;
use ndb_hybrid::{Fusion, HybridQuery, TextLeg, VectorLeg};
use ndb_index::Engine;
use ndb_page::manager::{PageIo, PageManager};
use ndb_transaction::{TxnConfig, recover};
use ndb_vector::Metric;
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

struct Db {
    engine: Engine<MemIo>,
    data: MemIo,
    wal_dir: PathBuf,
}

fn create_db() -> (Db, TempDir) {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let io = MemIo::default();
    let pm = PageManager::create(io.clone(), 4096, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, 128 * 4096));
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
    let pool = Arc::new(BufferPool::new(
        PageManager::open(data.clone()).unwrap(),
        128 * 4096,
    ));
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

/// Deterministic vectors — same as ndb-hnsw tests (clustered).
fn gen_vec(seed: u64, dims: usize) -> Vec<f32> {
    let mut x = seed | 1;
    let cluster = (seed % 7) as f32;
    (0..dims)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            cluster + (x % 1000) as f32 / 1000.0
        })
        .collect()
}

fn vcfg(dims: u16) -> HnswConfig {
    HnswConfig {
        dims,
        metric: Metric::Euclidean,
        m: 8,
        ef_construction: 60,
        ef_search: 32,
    }
}

fn bootstrap(engine: &Engine<MemIo>) {
    let mut txn = engine.txns().begin_write().unwrap();
    engine
        .create_collection(&mut txn, "app", "docs", Schema::new())
        .unwrap();
    txn.commit().unwrap();
}

fn seed_docs(engine: &Engine<MemIo>, n: u64) {
    let mut txn = engine.txns().begin_write().unwrap();
    let mut c = engine.collection(&txn, "app", "docs").unwrap().unwrap();
    for i in 1..=n {
        c.insert(
            &mut txn,
            doc(&[
                ("title", Value::String(format!("doc-{i}"))),
                (
                    "body",
                    Value::String(if i % 3 == 0 {
                        "red apple fruit".into()
                    } else {
                        "blue sky ocean".into()
                    }),
                ),
                ("emb", Value::Vector(gen_vec(i, 8))),
            ]),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

#[test]
fn vector_index_create_backfill_search() {
    let (db, _t) = create_db();
    bootstrap(&db.engine);
    seed_docs(&db.engine, 30);

    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.create_vector_index(&mut txn, "v_emb", "emb", vcfg(8))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let q = gen_vec(3, 8);
    let hits = c.search_vector(&txn, "v_emb", &q, 5, 16).unwrap();
    assert!(!hits.is_empty());
    assert_eq!(hits[0].doc_id.get(), 3);
    assert!(hits.iter().all(|h| h.legs == ["vector"]));
}

#[test]
fn vector_maintenance_insert_update_delete() {
    let (db, _t) = create_db();
    bootstrap(&db.engine);

    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.create_vector_index(&mut txn, "v_emb", "emb", vcfg(8))
        .unwrap();
    let id = c
        .insert(&mut txn, doc(&[("emb", Value::Vector(gen_vec(9, 8)))]))
        .unwrap();
    txn.commit().unwrap();

    // update the embedding to land near gen_vec(4)
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let cur = c.get(&txn, id).unwrap().unwrap();
    let mut nd = cur.clone();
    nd.fields.insert("emb".into(), Value::Vector(gen_vec(4, 8)));
    c.update(&mut txn, id, nd).unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = c
        .search_vector(&txn, "v_emb", &gen_vec(4, 8), 3, 16)
        .unwrap();
    assert_eq!(hits[0].doc_id.get(), id.get());

    // delete → absent from results
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.delete(&mut txn, id).unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let hits = c
        .search_vector(&txn, "v_emb", &gen_vec(4, 8), 5, 32)
        .unwrap();
    assert!(!hits.iter().any(|h| h.doc_id.get() == id.get()));
}

#[test]
fn vector_index_survives_restart() {
    let (mut db, _t) = create_db();
    bootstrap(&db.engine);
    seed_docs(&db.engine, 40);

    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.create_vector_index(&mut txn, "v_emb", "emb", vcfg(8))
        .unwrap();
    txn.commit().unwrap();

    db = crash_and_restart(db);

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    assert_eq!(c.meta().vector_indexes.len(), 1);
    let hits = c
        .search_vector(&txn, "v_emb", &gen_vec(7, 8), 3, 16)
        .unwrap();
    assert_eq!(hits[0].doc_id.get(), 7);
}

#[test]
fn hybrid_search_rrf_fusion() {
    let (db, _t) = create_db();
    bootstrap(&db.engine);
    seed_docs(&db.engine, 30);

    use ndb_analyzer::AnalyzerConfig;
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.create_text_index(
        &mut txn,
        "t_body",
        &[("body".to_string(), 1.0, AnalyzerConfig::default())],
    )
    .unwrap();
    c.create_vector_index(&mut txn, "v_emb", "emb", vcfg(8))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    // doc 3: "red apple fruit" AND emb near gen_vec(3).
    let q = HybridQuery {
        text: Some(TextLeg {
            index: "t_body".into(),
            query: "apple".into(),
            weight: 1.0,
        }),
        vector: Some(VectorLeg {
            index: "v_emb".into(),
            vector: gen_vec(3, 8),
            ef: 24,
            weight: 1.0,
        }),
        limit: 5,
        fetch_k: 8,
        fusion: Some(Fusion::new(60, vec![1.0, 1.0]).unwrap()),
        ..Default::default()
    };
    let hits = c.search_hybrid(&txn, &q).unwrap();
    assert!(!hits.is_empty());
    // doc 3 should top the fusion — present in BOTH legs.
    assert_eq!(hits[0].doc_id.get(), 3, "doc in both legs should win RRF");
    assert!(
        hits[0].legs.len() >= 2,
        "provenance should record both legs"
    );
}

#[test]
fn hybrid_candidate_filter_excludes_ids() {
    let (db, _t) = create_db();
    bootstrap(&db.engine);
    seed_docs(&db.engine, 20);

    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    c.create_vector_index(&mut txn, "v_emb", "emb", vcfg(8))
        .unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let q = HybridQuery {
        vector: Some(VectorLeg {
            index: "v_emb".into(),
            vector: gen_vec(3, 8),
            ef: 16,
            weight: 1.0,
        }),
        candidate_filter: Some(Box::new(|d| d != 3)),
        limit: 5,
        ..Default::default()
    };
    let hits = c.search_hybrid(&txn, &q).unwrap();
    assert!(!hits.iter().any(|h| h.doc_id.get() == 3));

    // validation: no legs → error
    let bad = HybridQuery {
        limit: 5,
        ..Default::default()
    };
    assert!(c.search_hybrid(&txn, &bad).is_err());
}
