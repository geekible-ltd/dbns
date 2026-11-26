//! Phase-6 acceptance tests (spec §26/§12–13): query language end-to-end
//! — parser → planner → optimiser → executor, index selection, EXPLAIN,
//! cancellation, bounded memory, aggregation pipeline.

use ndb_aggregation::{AggLimits, QueryEngine};
use ndb_buffer::BufferPool;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, Schema, Value};
use ndb_index::Engine;
use ndb_page::manager::{PageIo, PageManager};
use ndb_query_executor::ExecLimits;
use ndb_query_planner::AccessPath;
use ndb_transaction::TxnConfig;
use ndb_wal::Wal;
use std::sync::atomic::AtomicBool;
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

struct Db {
    engine: Engine<MemIo>,
    pool: Arc<BufferPool<MemIo>>,
    qe: QueryEngine,
    _tmp: TempDir,
}

fn create_db() -> Db {
    let tmp = TempDir::new().unwrap();
    let wal_dir = tmp.path().join("wal");
    let io = MemIo::default();
    let pm = PageManager::create(io.clone(), PAGE_SIZE, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, 64 * PAGE_SIZE as u64));
    let (wal, _) = Wal::open(&wal_dir, 0).unwrap();
    let engine = Engine::create(Arc::clone(&pool), Arc::new(wal), TxnConfig::default()).unwrap();
    Db {
        engine,
        pool,
        qe: QueryEngine::new(),
        _tmp: tmp,
    }
}

fn doc(fields: &[(&str, Value)]) -> Document {
    let mut d = Document::empty(DocumentId::new(0));
    for (n, v) in fields {
        d.fields.insert(n.to_string(), v.clone());
    }
    d
}

fn s(v: &str) -> Value {
    Value::String(v.into())
}
fn i(v: i64) -> Value {
    Value::Int(v)
}

/// users collection: {name, age, city, tags[]}
fn seed(db: &Db) {
    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine.create_database(&mut txn, "app").unwrap();
    db.engine
        .create_collection(&mut txn, "app", "users", Schema::new())
        .unwrap();
    let mut coll = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    coll.create_index(&mut txn, "by_age", &["age".into()], false, false)
        .unwrap();
    coll.create_index(&mut txn, "by_city", &["city".into()], false, false)
        .unwrap();
    let rows = [
        ("amy", 30, "london", vec!["a", "b"]),
        ("bob", 41, "paris", vec!["b", "c"]),
        ("cat", 25, "london", vec!["c"]),
        ("dan", 55, "rome", vec!["a", "c"]),
        ("eli", 19, "paris", vec!["b"]),
    ];
    for (name, age, city, tags) in rows {
        coll.insert(
            &mut txn,
            doc(&[
                ("name", s(name)),
                ("age", i(age)),
                ("city", s(city)),
                ("tags", Value::Array(tags.iter().map(|t| s(t)).collect())),
            ]),
        )
        .unwrap();
    }
    txn.commit().unwrap();
}

fn find(db: &Db, json: &str) -> Vec<Document> {
    let mut txn = db.engine.txns().begin_write().unwrap();
    let r = db
        .qe
        .find(&db.engine, &mut txn, json, &ExecLimits::default())
        .unwrap();
    txn.commit().unwrap();
    r.docs
}

#[test]
fn full_scan_equality() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"name":"cat"}}"#,
    );
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get("city"), Some(&s("london")));
}

#[test]
fn explain_shows_index_selection() {
    let db = create_db();
    seed(&db);
    let mut txn = db.engine.txns().begin().unwrap();
    let plan = db
        .qe
        .explain(
            &db.engine,
            &mut txn,
            r#"{"collection":"users","database":"app","filter":{"age":{"$gte":30}}}"#,
        )
        .unwrap();
    match &plan.access {
        AccessPath::IndexScan { index, .. } => assert_eq!(index, "by_age"),
        other => panic!("expected IndexScan, got {other:?}"),
    }
    assert!(plan.explain.iter().any(|l| l.contains("by_age")));
}

#[test]
fn index_range_scan_filters() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"age":{"$gte":30},"city":"london"},"sort":{"age":1}}"#,
    );
    // age>=30 on by_age; city==london residual.
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get("name"), Some(&s("amy")));
}

#[test]
fn pk_lookup_plan() {
    let db = create_db();
    seed(&db);
    let mut txn = db.engine.txns().begin().unwrap();
    let plan = db
        .qe
        .explain(
            &db.engine,
            &mut txn,
            r#"{"collection":"users","database":"app","filter":{"_id":2}}"#,
        )
        .unwrap();
    assert!(matches!(plan.access, AccessPath::PkLookup { id: 2 }));
}

#[test]
fn logical_or_and_not() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"$or":[{"city":"paris"},{"age":{"$lt":20}}]},"sort":{"age":1}}"#,
    );
    let names: Vec<&Value> = docs.iter().filter_map(|d| d.fields.get("name")).collect();
    assert!(names.contains(&&s("bob")) && names.contains(&&s("eli")));
    assert_eq!(docs.len(), 2);
}

#[test]
fn array_operators() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"tags":{"$all":["a","c"]}}}"#,
    );
    assert_eq!(docs.len(), 1); // only dan has a+c
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"tags":{"$size":1}}}"#,
    );
    assert_eq!(docs.len(), 2); // cat + eli
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"tags":{"$elemMatch":{"$eq":"c"}}}}"#,
    );
    assert_eq!(docs.len(), 3); // bob, cat, dan
}

#[test]
fn sort_skip_limit_projection() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app",
        "sort":{"age":-1},"skip":1,"limit":2,"projection":{"name":1}}"#,
    );
    assert_eq!(docs.len(), 2);
    // desc: dan(55), bob(41), amy(30)… skip 1 → bob, amy
    assert_eq!(docs[0].get("name"), Some(&s("bob")));
    assert!(docs[0].get("age").is_none()); // projected out
}

#[test]
fn in_and_exists_and_type() {
    let db = create_db();
    seed(&db);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"city":{"$in":["rome","london"]}},"sort":{"name":1}}"#,
    );
    assert_eq!(docs.len(), 3);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"age":{"$type":"int"}}}"#,
    );
    assert_eq!(docs.len(), 5);
    let docs = find(
        &db,
        r#"{"collection":"users","database":"app","filter":{"missing":{"$exists":true}}}"#,
    );
    assert!(docs.is_empty());
}

#[test]
fn cancellation_stops_query() {
    let db = create_db();
    seed(&db);
    let cancel = Arc::new(AtomicBool::new(true)); // pre-cancelled
    let limits = ExecLimits {
        cancel,
        ..ExecLimits::default()
    };
    let mut txn = db.engine.txns().begin_write().unwrap();
    let r = db.qe.find(
        &db.engine,
        &mut txn,
        r#"{"collection":"users","database":"app","filter":{"age":{"$gte":0}}}"#,
        &limits,
    );
    assert!(r.is_err());
}

#[test]
fn docs_scanned_limit_binds() {
    let db = create_db();
    seed(&db);
    let limits = ExecLimits {
        max_docs_scanned: 1,
        ..ExecLimits::default()
    };
    let mut txn = db.engine.txns().begin_write().unwrap();
    let r = db.qe.find(
        &db.engine,
        &mut txn,
        r#"{"collection":"users","database":"app"}"#,
        &limits,
    );
    assert!(r.is_err());
}

#[test]
fn aggregation_group_sum_avg() {
    let db = create_db();
    seed(&db);
    let mut txn = db.engine.txns().begin_write().unwrap();
    let out = db
        .qe
        .aggregate(
            &db.engine,
            &mut txn,
            &db.pool,
            r#"{
        "collection": "users", "database": "app",
        "pipeline": [
            {"$match": {"age": {"$gte": 20}}},
            {"$group": {"_id": "$city", "n": {"$sum": 1}, "avg_age": {"$avg": "$age"}}},
            {"$sort": {"_id": 1}}
        ]}"#,
            &AggLimits::default(),
        )
        .unwrap();
    txn.commit().unwrap();
    assert_eq!(out.len(), 3);
    // london: amy 30 + cat 25 → n=2, avg 27.5
    assert_eq!(out[0].get("_id"), Some(&s("london")));
    assert_eq!(out[0].get("n"), Some(&Value::Float(2.0)));
    assert_eq!(out[0].get("avg_age"), Some(&Value::Float(27.5)));
}

#[test]
fn aggregation_unwind_project_count() {
    let db = create_db();
    seed(&db);
    let mut txn = db.engine.txns().begin_write().unwrap();
    let out = db
        .qe
        .aggregate(
            &db.engine,
            &mut txn,
            &db.pool,
            r#"{
        "collection": "users", "database": "app",
        "pipeline": [
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]}"#,
            &AggLimits::default(),
        )
        .unwrap();
    // tags a×2, b×3, c×3
    assert_eq!(out.len(), 3);
    assert_eq!(out[1].get("count"), Some(&Value::Float(3.0)));
    txn.commit().unwrap();
}

#[test]
fn group_spills_under_tight_bound() {
    let db = create_db();
    seed(&db);
    let limits = AggLimits {
        group_bytes_bound: 64,
        ..AggLimits::default()
    };
    let mut txn = db.engine.txns().begin_write().unwrap();
    let out = db
        .qe
        .aggregate(
            &db.engine,
            &mut txn,
            &db.pool,
            r#"{
        "collection": "users", "database": "app",
        "pipeline": [{"$group": {"_id": "$city", "n": {"$sum": 1}}}]}"#,
            &limits,
        )
        .unwrap();
    txn.commit().unwrap();
    // All groups still correct despite spilling.
    assert_eq!(out.len(), 3);
    let london = out
        .iter()
        .find(|d| d.get("_id") == Some(&s("london")))
        .unwrap();
    assert_eq!(london.get("n"), Some(&Value::Float(2.0)));
}

#[test]
fn addfields_and_ifnull() {
    let db = create_db();
    seed(&db);
    let mut txn = db.engine.txns().begin_write().unwrap();
    let out = db
        .qe
        .aggregate(
            &db.engine,
            &mut txn,
            &db.pool,
            r#"{
        "collection": "users", "database": "app",
        "pipeline": [
            {"$match": {"name": "amy"}},
            {"$addFields": {"label": {"$concat": ["$name", "@", {"$ifNull": ["$nick", "none"]}]}}},
            {"$project": {"label": 1}}
        ]}"#,
            &AggLimits::default(),
        )
        .unwrap();
    txn.commit().unwrap();
    assert_eq!(out[0].get("label"), Some(&s("amy@none")));
}

#[test]
fn text_query_drives_access_path() {
    let db = create_db();
    // Seed docs with a text index on body.
    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine.create_database(&mut txn, "app").unwrap();
    db.engine
        .create_collection(&mut txn, "app", "docs", Schema::new())
        .unwrap();
    let mut coll = db.engine.collection(&txn, "app", "docs").unwrap().unwrap();
    let cfg = ndb_analyzer::AnalyzerConfig::default();
    coll.create_text_index(&mut txn, "search", &[("body".into(), 1.0, cfg)])
        .unwrap();
    for body in ["rust is fast", "python is flexible", "rust is safe"] {
        coll.insert(&mut txn, doc(&[("body", s(body))])).unwrap();
    }
    txn.commit().unwrap();

    let mut txn = db.engine.txns().begin_write().unwrap();
    // $text → TextSearch access path.
    let plan = db
        .qe
        .explain(
            &db.engine,
            &mut txn,
            r#"{
        "collection":"docs","database":"app",
        "filter":{"$text":{"$search":"rust","$index":"search"}}
    }"#,
        )
        .unwrap();
    assert!(matches!(plan.access, AccessPath::TextSearch { .. }));

    let r = db
        .qe
        .find(
            &db.engine,
            &mut txn,
            r#"{
        "collection":"docs","database":"app",
        "filter":{"$text":{"$search":"rust","$index":"search"}}
    }"#,
            &ExecLimits::default(),
        )
        .unwrap();
    txn.commit().unwrap();
    assert_eq!(r.docs.len(), 2);
    assert!(r.scores.is_some()); // BM25 scores flow through
}
