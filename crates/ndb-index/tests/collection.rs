//! Phase-4 acceptance tests (spec §26): single-node document database —
//! CRUD, schema validation, primary + secondary indexes, bulk insertion,
//! persistence across restart.

use ndb_buffer::BufferPool;
use ndb_common::ids::DocumentId;
use ndb_index::Engine;
use ndb_page::manager::{PageIo, PageManager};
use ndb_transaction::{TxnConfig, recover};
use ndb_wal::Wal;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// In-memory page store surviving "restarts" (same harness as
/// ndb-transaction's recovery tests).
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

use ndb_document::{Document, Schema, Value};

fn doc(fields: &[(&str, Value)]) -> Document {
    let mut d = Document::empty(DocumentId::new(0));
    for (n, v) in fields {
        d.fields.insert(n.to_string(), v.clone());
    }
    d
}

fn bootstrap(engine: &Engine<MemIo>, schema: Schema) {
    let mut txn = engine.txns().begin_write().unwrap();
    engine
        .create_collection(&mut txn, "app", "users", schema)
        .unwrap();
    txn.commit().unwrap();
}

#[test]
fn crud_roundtrip_and_restart_persistence() {
    let (db, _t) = create_db();
    bootstrap(&db.engine, Schema::new());

    let id = {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        let id = c
            .insert(
                &mut txn,
                doc(&[
                    ("name", Value::String("ada".into())),
                    ("age", Value::UInt(36)),
                ]),
            )
            .unwrap();
        txn.commit().unwrap();
        id
    };

    // read
    {
        let txn = db.engine.txns().begin().unwrap();
        let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        let got = c.get(&txn, id).unwrap().unwrap();
        assert_eq!(got.get("name"), Some(&Value::String("ada".into())));
        assert_eq!(got.get("age"), Some(&Value::UInt(36)));
    }

    // update
    {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        c.update(
            &mut txn,
            id,
            doc(&[
                ("name", Value::String("ada".into())),
                ("age", Value::UInt(37)),
            ]),
        )
        .unwrap();
        txn.commit().unwrap();
    }

    // crash + restart — committed state must persist
    let db = crash_and_restart(db);
    {
        let txn = db.engine.txns().begin().unwrap();
        let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        let got = c.get(&txn, id).unwrap().unwrap();
        assert_eq!(got.get("age"), Some(&Value::UInt(37)));
    }

    // delete
    {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        assert!(c.delete(&mut txn, id).unwrap());
        assert!(!c.delete(&mut txn, id).unwrap());
        txn.commit().unwrap();
    }
    let db = crash_and_restart(db);
    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    assert!(c.get(&txn, id).unwrap().is_none());
}

#[test]
fn secondary_index_lookup_and_update() {
    let (db, _t) = create_db();
    bootstrap(&db.engine, Schema::new());

    let ids = {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        c.create_index(&mut txn, "by_age", &["age".to_string()], false, false)
            .unwrap();
        db.engine.save_meta(&mut txn, &c).unwrap();
        let ids = c
            .insert_many(
                &mut txn,
                vec![
                    doc(&[
                        ("name", Value::String("a".into())),
                        ("age", Value::UInt(30)),
                    ]),
                    doc(&[
                        ("name", Value::String("b".into())),
                        ("age", Value::UInt(30)),
                    ]),
                    doc(&[
                        ("name", Value::String("c".into())),
                        ("age", Value::UInt(40)),
                    ]),
                ],
            )
            .unwrap();
        txn.commit().unwrap();
        ids
    };

    {
        let txn = db.engine.txns().begin().unwrap();
        let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        use ndb_btree::key::KeyPart;
        let mut hits = c.lookup(&txn, "by_age", &[KeyPart::UInt(30)]).unwrap();
        hits.sort();
        assert_eq!(hits, vec![ids[0], ids[1]]);
    }

    // update a doc's indexed field — old index entry must be removed
    {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        c.update(
            &mut txn,
            ids[0],
            doc(&[
                ("name", Value::String("a".into())),
                ("age", Value::UInt(40)),
            ]),
        )
        .unwrap();
        txn.commit().unwrap();
    }
    {
        let txn = db.engine.txns().begin().unwrap();
        let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        use ndb_btree::key::KeyPart;
        assert_eq!(
            c.lookup(&txn, "by_age", &[KeyPart::UInt(30)]).unwrap(),
            vec![ids[1]]
        );
        let mut hits = c.lookup(&txn, "by_age", &[KeyPart::UInt(40)]).unwrap();
        hits.sort();
        assert_eq!(hits, vec![ids[0], ids[2]]);
    }
}

#[test]
fn unique_index_rejects_duplicates() {
    let (db, _t) = create_db();
    bootstrap(&db.engine, Schema::new());
    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    c.create_index(&mut txn, "by_email", &["email".to_string()], true, false)
        .unwrap();
    db.engine.save_meta(&mut txn, &c).unwrap();

    c.insert(&mut txn, doc(&[("email", Value::String("x@y.z".into()))]))
        .unwrap();
    // Same email → violation.
    let dup = c.insert(&mut txn, doc(&[("email", Value::String("x@y.z".into()))]));
    assert!(dup.is_err(), "expected unique violation");
    // Different email → ok.
    c.insert(&mut txn, doc(&[("email", Value::String("a@b.c".into()))]))
        .unwrap();
}

#[test]
fn large_document_uses_record_chain() {
    let (db, _t) = create_db();
    bootstrap(&db.engine, Schema::new());
    let big = "x".repeat(20_000); // ~20KB — far beyond the inline limit
    let id = {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        let id = c
            .insert(&mut txn, doc(&[("blob", Value::String(big.clone()))]))
            .unwrap();
        txn.commit().unwrap();
        id
    };
    let db = crash_and_restart(db);
    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    let got = c.get(&txn, id).unwrap().unwrap();
    assert_eq!(got.get("blob"), Some(&Value::String(big)));
}

#[test]
fn strict_schema_rejects_undeclared_fields() {
    let (db, _t) = create_db();
    let schema = Schema::new().field("name", ndb_document::DataType::String, true);
    let mut schema = schema;
    schema.strict = true;
    bootstrap(&db.engine, schema);

    let mut txn = db.engine.txns().begin_write().unwrap();
    let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    // undeclared field → rejected
    let bad = c.insert(
        &mut txn,
        doc(&[
            ("name", Value::String("x".into())),
            ("extra", Value::Int(1)),
        ]),
    );
    assert!(bad.is_err());
    // missing required field → rejected
    let missing = c.insert(&mut txn, doc(&[("extra2", Value::Int(1))]));
    assert!(missing.is_err());
    // valid doc → ok
    c.insert(&mut txn, doc(&[("name", Value::String("x".into()))]))
        .unwrap();
}

#[test]
fn multi_document_transaction_rolls_back_atomically() {
    let (db, _t) = create_db();
    bootstrap(&db.engine, Schema::new());
    {
        let mut txn = db.engine.txns().begin_write().unwrap();
        let mut c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
        c.insert(&mut txn, doc(&[("n", Value::Int(1))])).unwrap();
        c.insert(&mut txn, doc(&[("n", Value::Int(2))])).unwrap();
        txn.rollback().unwrap();
    }
    let db = crash_and_restart(db);
    let txn = db.engine.txns().begin().unwrap();
    let c = db.engine.collection(&txn, "app", "users").unwrap().unwrap();
    assert_eq!(c.scan(&txn).unwrap().len(), 0);
}

#[test]
fn database_and_collection_management() {
    let (db, _t) = create_db();
    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine.create_database(&mut txn, "prod").unwrap();
    assert!(db.engine.create_database(&mut txn, "prod").is_err());
    db.engine
        .create_collection(&mut txn, "prod", "orders", Schema::new())
        .unwrap();
    db.engine
        .create_collection(&mut txn, "prod", "items", Schema::new())
        .unwrap();
    txn.commit().unwrap();

    let txn = db.engine.txns().begin().unwrap();
    assert_eq!(db.engine.list_databases(&txn).unwrap(), vec!["prod"]);
    let mut colls = db.engine.list_collections(&txn, "prod").unwrap();
    colls.sort();
    assert_eq!(colls, vec!["items", "orders"]);

    // drop one collection; then the whole database
    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine
        .drop_collection(&mut txn, "prod", "items")
        .unwrap();
    txn.commit().unwrap();
    let txn = db.engine.txns().begin().unwrap();
    assert_eq!(
        db.engine.list_collections(&txn, "prod").unwrap(),
        vec!["orders"]
    );

    let mut txn = db.engine.txns().begin_write().unwrap();
    db.engine.drop_database(&mut txn, "prod").unwrap();
    txn.commit().unwrap();
    let txn = db.engine.txns().begin().unwrap();
    assert!(db.engine.list_databases(&txn).unwrap().is_empty());
    assert!(db.engine.list_collections(&txn, "prod").unwrap().is_empty());
}
