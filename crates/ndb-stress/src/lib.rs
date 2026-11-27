//! Stress runner (spec §22 perf testing support) — hammers the database's
//! **gRPC surface** with a weighted mix of writes, reads, queries and
//! text/vector/hybrid searches, then reports per-op throughput + latency
//! percentiles.
//!
//! The runner is transport-real: it dials the server's own gRPC listener
//! with a `tonic` client, so timings include HTTP/2 framing, prost codec
//! and the full service path — not an in-process shortcut.
//!
//! Driven from `POST /admin/stress` (ndb-rest); a `StressHandle` snapshot
//! powers the live UI panel.

use serde_json::{json, Value as J};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ndb_grpc::proto::*;

// ---- configuration --------------------------------------------------------

/// Per-op weights in the mix — 0 disables an op entirely.
#[derive(Debug, Clone)]
pub struct Mix {
    pub insert: u32,
    pub read: u32,
    pub update: u32,
    pub delete: u32,
    pub find: u32,
    pub text: u32,
    pub vector: u32,
    pub hybrid: u32,
}

impl Default for Mix {
    fn default() -> Self {
        Self {
            insert: 10,
            read: 30,
            update: 10,
            delete: 5,
            find: 20,
            text: 10,
            vector: 10,
            hybrid: 5,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StressConfig {
    /// Concurrent worker tasks.
    pub workers: usize,
    /// Run length in seconds (workers phase; seeding is extra).
    pub duration_secs: u64,
    /// Documents bulk-inserted before workers start.
    pub seed_docs: usize,
    /// Search hit rate knob — word pool size for the `body` text field.
    pub word_pool: usize,
    /// HNSW vector dimensionality.
    pub vector_dims: usize,
    /// Database / collection the run owns (created + dropped by the run).
    pub database: String,
    pub collection: String,
    pub mix: Mix,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            workers: 8,
            duration_secs: 30,
            seed_docs: 1000,
            word_pool: 200,
            vector_dims: 8,
            database: "stress".into(),
            collection: "docs".into(),
            mix: Mix::default(),
        }
    }
}

impl StressConfig {
    /// From the REST JSON body — every field optional.
    pub fn from_json(j: &J) -> Result<Self, String> {
        let d = Self::default();
        let u = |k: &str, dv: u64| -> Result<u64, String> {
            match j.get(k) {
                None => Ok(dv),
                Some(v) => v
                    .as_u64()
                    .ok_or_else(|| format!("'{k}' must be a non-negative integer")),
            }
        };
        let mix = match j.get("mix") {
            None => d.mix.clone(),
            Some(J::Object(m)) => Mix {
                insert: w(m, "insert", d.mix.insert)?,
                read: w(m, "read", d.mix.read)?,
                update: w(m, "update", d.mix.update)?,
                delete: w(m, "delete", d.mix.delete)?,
                find: w(m, "find", d.mix.find)?,
                text: w(m, "text", d.mix.text)?,
                vector: w(m, "vector", d.mix.vector)?,
                hybrid: w(m, "hybrid", d.mix.hybrid)?,
            },
            Some(_) => return Err("'mix' must be an object of op weights".into()),
        };
        if mix.total() == 0 {
            return Err("mix: at least one op weight must be > 0".into());
        }
        Ok(Self {
            workers: u("workers", d.workers as u64)? as usize,
            duration_secs: u("duration_secs", d.duration_secs)?,
            seed_docs: u("seed_docs", d.seed_docs as u64)? as usize,
            word_pool: u("word_pool", d.word_pool as u64)? as usize,
            vector_dims: u("vector_dims", d.vector_dims as u64)? as usize,
            database: j
                .get("database")
                .and_then(J::as_str)
                .unwrap_or(&d.database)
                .to_string(),
            collection: j
                .get("collection")
                .and_then(J::as_str)
                .unwrap_or(&d.collection)
                .to_string(),
            mix,
        })
    }
}

fn w(m: &serde_json::Map<String, J>, k: &str, d: u32) -> Result<u32, String> {
    match m.get(k) {
        None => Ok(d),
        Some(v) => v
            .as_u64()
            .map(|x| x as u32)
            .ok_or_else(|| format!("mix.{k} must be a non-negative integer")),
    }
}

impl Mix {
    fn total(&self) -> u32 {
        self.insert
            + self.read
            + self.update
            + self.delete
            + self.find
            + self.text
            + self.vector
            + self.hybrid
    }
    fn pick(&self, r: u64) -> Op {
        let mut x = (r % self.total() as u64) as u32;
        for (w, op) in [
            (self.insert, Op::Insert),
            (self.read, Op::Read),
            (self.update, Op::Update),
            (self.delete, Op::Delete),
            (self.find, Op::Find),
            (self.text, Op::Text),
            (self.vector, Op::Vector),
            (self.hybrid, Op::Hybrid),
        ] {
            if x < w {
                return op;
            }
            x -= w;
        }
        Op::Read
    }
}

// ---- stats: per-op log-bucketed latency histogram -------------------------

/// Op kinds — index into the per-op stats slots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Insert,
    Read,
    Update,
    Delete,
    Find,
    Text,
    Vector,
    Hybrid,
    BulkSeed,
}
const N_OPS: usize = 9;

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Insert => "insert",
            Op::Read => "read",
            Op::Update => "update",
            Op::Delete => "delete",
            Op::Find => "find",
            Op::Text => "text",
            Op::Vector => "vector",
            Op::Hybrid => "hybrid",
            Op::BulkSeed => "seed",
        }
    }
    const ALL: [Op; N_OPS] = [
        Op::Insert,
        Op::Read,
        Op::Update,
        Op::Delete,
        Op::Find,
        Op::Text,
        Op::Vector,
        Op::Hybrid,
        Op::BulkSeed,
    ];
}

const BUCKETS: usize = 24; // 2^24 µs ≈ 16.7 s — anything slower lands in the top bucket

#[derive(Default)]
struct OpStats {
    count: AtomicU64,
    errors: AtomicU64,
    /// Expected-miss results (get/update/delete on a deleted id) — real
    /// work done, just a NotFound response; counted separately from errors.
    misses: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; BUCKETS],
}

impl OpStats {
    fn record(&self, us: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.max_us.fetch_max(us, Ordering::Relaxed);
        let b = (64 - us.leading_zeros().min(63)) as usize;
        self.buckets[b.min(BUCKETS - 1)].fetch_add(1, Ordering::Relaxed);
    }

    fn percentile(&self, count: u64, p: f64) -> u64 {
        if count == 0 {
            return 0;
        }
        let target = ((count as f64) * p).ceil() as u64;
        let mut cum = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            cum += b.load(Ordering::Relaxed);
            if cum >= target {
                return 1u64 << i; // bucket upper bound (µs)
            }
        }
        1u64 << (BUCKETS - 1)
    }

    fn snapshot(&self, elapsed: f64) -> J {
        let count = self.count.load(Ordering::Relaxed);
        json!({
            "count": count,
            "errors": self.errors.load(Ordering::Relaxed),
            "misses": self.misses.load(Ordering::Relaxed),
            "ops_per_sec": if elapsed > 0.0 { (count as f64 / elapsed * 10.0).round() / 10.0 } else { 0.0 },
            "p50_ms": self.percentile(count, 0.50) as f64 / 1000.0,
            "p95_ms": self.percentile(count, 0.95) as f64 / 1000.0,
            "p99_ms": self.percentile(count, 0.99) as f64 / 1000.0,
            "max_ms": self.max_us.load(Ordering::Relaxed) as f64 / 1000.0,
            "avg_ms": if count > 0 { self.sum_us.load(Ordering::Relaxed) as f64 / count as f64 / 1000.0 } else { 0.0 },
        })
    }
}

// ---- run lifecycle ----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Phase {
    Connecting,
    Seeding,
    Running,
    Done,
    Failed,
}

/// A live (or finished) stress run — polled for snapshots by the REST layer.
pub struct StressHandle {
    pub config: StressConfig,
    pub target: String,
    started: Instant,
    stats: [OpStats; N_OPS],
    phase: std::sync::Mutex<Phase>,
    /// Seed/early failure detail when phase == Failed.
    failure: std::sync::Mutex<Option<String>>,
    cancel: AtomicBool,
    /// Highest doc id issued (approximate — reads pick below it).
    high_id: AtomicU64,
    /// Ops completed so far — cheap liveness counter.
    pub ops_done: AtomicU64,
    /// Most recent RPC error string (diagnostics for the report).
    last_error: std::sync::Mutex<Option<String>>,
}

impl StressHandle {
    pub fn stop(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    pub fn is_running(&self) -> bool {
        matches!(
            *self.phase.lock().unwrap(),
            Phase::Connecting | Phase::Seeding | Phase::Running
        )
    }

    /// JSON snapshot for `GET /admin/stress` — live counters + percentiles.
    pub fn snapshot(&self) -> J {
        let phase = self.phase.lock().unwrap().clone();
        let elapsed = self.started.elapsed().as_secs_f64();
        let mut ops = serde_json::Map::new();
        let mut total = 0u64;
        let mut errors = 0u64;
        for op in Op::ALL {
            let s = &self.stats[op as usize];
            ops.insert(op.name().into(), s.snapshot(elapsed));
            total += s.count.load(Ordering::Relaxed);
            errors += s.errors.load(Ordering::Relaxed);
        }
        json!({
            "phase": format!("{phase:?}").to_lowercase(),
            "failure": *self.failure.lock().unwrap(),
            "last_error": *self.last_error.lock().unwrap(),
            "target": self.target,
            "database": self.config.database,
            "collection": self.config.collection,
            "elapsed_secs": (elapsed * 10.0).round() / 10.0,
            "total_ops": total,
            "total_errors": errors,
            "ops": J::Object(ops),
        })
    }
}

/// xorshift64 — cheap per-worker PRNG (no entropy needed).
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

const WORDS: &[&str] = &[
    "amber", "brisk", "cinder", "delta", "ember", "fjord", "glint", "harbor", "iris", "jade",
    "knoll", "lumen", "marsh", "nadir", "onyx", "plume", "quartz", "ridge", "sable", "tide",
    "umber", "vault", "willow", "xenon", "yonder", "zephyr", "atlas", "birch", "cobalt", "dune",
];

/// Starts a stress run against `target` (e.g. `http://127.0.0.1:9090`).
/// Seeds via gRPC, then spawns `workers` tasks until the deadline or
/// [`StressHandle::stop`]. Returns the shared handle immediately.
pub fn start(config: StressConfig, target: String) -> Arc<StressHandle> {
    let h = Arc::new(StressHandle {
        stats: Default::default(),
        phase: std::sync::Mutex::new(Phase::Connecting),
        failure: std::sync::Mutex::new(None),
        cancel: AtomicBool::new(false),
        high_id: AtomicU64::new(1),
        ops_done: AtomicU64::new(0),
        last_error: std::sync::Mutex::new(None),
        started: Instant::now(),
        target,
        config,
    });
    let hh = Arc::clone(&h);
    tokio::spawn(async move {
        if let Err(e) = supervise(hh.clone()).await {
            *hh.failure.lock().unwrap() = Some(e);
            *hh.phase.lock().unwrap() = Phase::Failed;
        }
    });
    h
}

async fn supervise(h: Arc<StressHandle>) -> Result<(), String> {
    let channel = tonic::transport::Channel::from_shared(h.target.clone())
        .map_err(|e| format!("bad gRPC target: {e}"))?
        .connect()
        .await
        .map_err(|e| format!("gRPC connect {}: {e}", h.target))?;

    // ---- seed: fresh db + collection + indexes, bulk docs ------------------
    *h.phase.lock().unwrap() = Phase::Seeding;
    let mut dbs = database_service_client::DatabaseServiceClient::new(channel.clone());
    let mut colls = collection_service_client::CollectionServiceClient::new(channel.clone());
    let mut docs = document_service_client::DocumentServiceClient::new(channel.clone());
    let mut idx = index_service_client::IndexServiceClient::new(channel.clone());

    let db = &h.config.database;
    let coll = &h.config.collection;
    // Idempotent-ish: drop first so repeated runs start clean; ignore
    // not-found on the drop and already-exists on the create.
    let _ = colls
        .drop_collection(CollRef {
            database: db.clone(),
            collection: coll.clone(),
        })
        .await;
    match dbs.create_database(NameReq { name: db.clone() }).await {
        Ok(_) => {}
        Err(e) if e.message().contains("already exists") => {}
        Err(e) => return Err(format!("create_database: {}", e.message())),
    }
    colls
        .create_collection(CreateCollReq {
            database: db.clone(),
            name: coll.clone(),
            schema_json: String::new(),
        })
        .await
        .map_err(|e| format!("create_collection: {}", e.message()))?;
    idx.create_index(IndexDef {
        database: db.clone(),
        collection: coll.clone(),
        name: "txt".into(),
        kind: "text".into(),
        fields: vec![],
        unique: false,
        sparse: false,
        field: "body".into(),
        weights: vec![],
        dims: 0,
        metric: String::new(),
        m: 0,
        ef_construction: 0,
        ef_search: 0,
    })
    .await
    .map_err(|e| format!("create text index: {}", e.message()))?;
    idx.create_index(IndexDef {
        database: db.clone(),
        collection: coll.clone(),
        name: "vec".into(),
        kind: "vector".into(),
        fields: vec![],
        unique: false,
        sparse: false,
        field: "vec".into(),
        weights: vec![],
        dims: h.config.vector_dims as u32,
        metric: "cosine".into(),
        m: 0,
        ef_construction: 0,
        ef_search: 0,
    })
    .await
    .map_err(|e| format!("create vector index: {}", e.message()))?;

    // Bulk seed via the client-streaming rpc — chunked so the message stream
    // doesn't hold all docs at once.
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let t0 = Instant::now();
    const CHUNK: usize = 256;
    let mut seeded = 0usize;
    while seeded < h.config.seed_docs {
        let n = CHUNK.min(h.config.seed_docs - seeded);
        let stream = tokio_stream::iter(
            (0..n)
                .map(|_| DocJson {
                    json: gen_doc(&mut rng, h.config.word_pool, h.config.vector_dims).to_string(),
                })
                .collect::<Vec<_>>(),
        );
        // Routing travels in request metadata, not the stream (§16).
        let mut req = tonic::Request::new(stream);
        req.metadata_mut().insert("x-db", db.parse().unwrap());
        req.metadata_mut()
            .insert("x-collection", coll.parse().unwrap());
        let t = Instant::now();
        let res = docs.bulk_insert(req).await;
        h.stats[Op::BulkSeed as usize].record(t.elapsed().as_micros() as u64);
        match res {
            Ok(r) => seeded += r.into_inner().inserted as usize,
            Err(e) => {
                h.stats[Op::BulkSeed as usize]
                    .errors
                    .fetch_add(1, Ordering::Relaxed);
                return Err(format!("bulk_insert seed: {}", e.message()));
            }
        }
        if h.cancel.load(Ordering::Relaxed) {
            break;
        }
    }
    h.high_id.store(seeded.max(1) as u64 + 1, Ordering::Relaxed);
    let _ = t0;

    // ---- workers -----------------------------------------------------------
    *h.phase.lock().unwrap() = Phase::Running;
    let deadline = Instant::now() + Duration::from_secs(h.config.duration_secs);
    let stop = Arc::new(AtomicBool::new(false));
    let mut joins = Vec::new();
    for w in 0..h.config.workers.max(1) {
        let h2 = Arc::clone(&h);
        let ch = channel.clone();
        let stop2 = Arc::clone(&stop);
        joins.push(tokio::spawn(async move {
            worker(h2, ch, w as u64, deadline, stop2).await;
        }));
    }
    // Watchdog: deadline or external stop flips the shared flag.
    {
        let h2 = Arc::clone(&h);
        let stop2 = Arc::clone(&stop);
        tokio::spawn(async move {
            loop {
                if Instant::now() >= deadline || h2.cancel.load(Ordering::Relaxed) {
                    stop2.store(true, Ordering::Relaxed);
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
    for j in joins {
        let _ = j.await;
    }
    *h.phase.lock().unwrap() = Phase::Done;
    Ok(())
}

/// One worker task — weighted op picks until `stop` or `deadline`.
async fn worker(
    h: Arc<StressHandle>,
    channel: tonic::transport::Channel,
    seed: u64,
    deadline: Instant,
    stop: Arc<AtomicBool>,
) {
    let mut docs = document_service_client::DocumentServiceClient::new(channel.clone());
    let mut search = search_service_client::SearchServiceClient::new(channel);
    let mut rng = Rng(seed.wrapping_add(0x2545F4914F6CDD1D));
    let db = h.config.database.clone();
    let coll = h.config.collection.clone();

    while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
        let op = h.config.mix.pick(rng.next());
        let t = Instant::now();
        let res = run_op(&h, &mut docs, &mut search, &mut rng, &db, &coll, op).await;
        let us = t.elapsed().as_micros() as u64;
        let s = &h.stats[op as usize];
        s.record(us);
        h.ops_done.fetch_add(1, Ordering::Relaxed);
        match res {
            Ok(()) => {}
            Err(OpErr::Miss) => {
                s.misses.fetch_add(1, Ordering::Relaxed);
            }
            Err(OpErr::Rpc(e)) => {
                s.errors.fetch_add(1, Ordering::Relaxed);
                *h.last_error.lock().unwrap() = Some(e.message().to_string());
            }
        }
    }
}

enum OpErr {
    Miss,
    Rpc(tonic::Status),
}

fn gen_doc(rng: &mut Rng, word_pool: usize, dims: usize) -> J {
    let words: Vec<String> = (0..8)
        .map(|_| {
            // Mix a small pool (heavy hitters) with the full list for variety.
            if rng.next().is_multiple_of(3) {
                WORDS[(rng.next() as usize) % word_pool.clamp(1, WORDS.len())].to_string()
            } else {
                WORDS[(rng.next() as usize) % WORDS.len()].to_string()
            }
        })
        .collect();
    json!({
        "name": format!("user-{}", rng.next() % 100_000),
        "email": format!("u{}@example.com", rng.next() % 100_000),
        "age": 18 + (rng.next() % 60),
        "body": words.join(" "),
        "vec": (0..dims).map(|_| ((rng.next() % 1000) as f64) / 1000.0).collect::<Vec<_>>(),
    })
}

async fn run_op(
    h: &Arc<StressHandle>,
    docs: &mut document_service_client::DocumentServiceClient<tonic::transport::Channel>,
    search: &mut search_service_client::SearchServiceClient<tonic::transport::Channel>,
    rng: &mut Rng,
    db: &str,
    coll: &str,
    op: Op,
) -> Result<(), OpErr> {
    let hi = h.high_id.load(Ordering::Relaxed).max(1);
    match op {
        Op::Insert => {
            let doc = gen_doc(rng, h.config.word_pool, h.config.vector_dims);
            let r = docs
                .insert(InsertReq {
                    database: db.into(),
                    collection: coll.into(),
                    doc: Some(DocJson {
                        json: doc.to_string(),
                    }),
                })
                .await
                .map_err(OpErr::Rpc)?;
            h.high_id
                .fetch_max(r.into_inner().id + 1, Ordering::Relaxed);
            Ok(())
        }
        Op::Read => {
            let id = 1 + rng.next() % hi;
            match docs
                .get(DocRef {
                    database: db.into(),
                    collection: coll.into(),
                    id,
                })
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if e.code() == tonic::Code::NotFound => Err(OpErr::Miss),
                Err(e) => Err(OpErr::Rpc(e)),
            }
        }
        Op::Update => {
            let id = 1 + rng.next() % hi;
            match docs
                .update(UpdateReq {
                    database: db.into(),
                    collection: coll.into(),
                    id,
                    patch: Some(DocJson {
                        json: json!({ "touched": rng.next() }).to_string(),
                    }),
                })
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if e.code() == tonic::Code::NotFound => Err(OpErr::Miss),
                Err(e) => Err(OpErr::Rpc(e)),
            }
        }
        Op::Delete => {
            let id = 1 + rng.next() % hi;
            match docs
                .delete(DocRef {
                    database: db.into(),
                    collection: coll.into(),
                    id,
                })
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if e.code() == tonic::Code::NotFound => Err(OpErr::Miss),
                Err(e) => Err(OpErr::Rpc(e)),
            }
        }
        Op::Find => {
            let q = json!({
                "filter": {"age": {"$gte": 18 + (rng.next() % 40)}},
                "limit": 20,
            });
            let mut stream = search
                .find(FindReq {
                    database: db.into(),
                    collection: coll.into(),
                    query_json: q.to_string(),
                })
                .await
                .map_err(OpErr::Rpc)?
                .into_inner();
            while let Some(_d) = stream.message().await.map_err(OpErr::Rpc)? {}
            Ok(())
        }
        Op::Text => {
            let q = WORDS[(rng.next() as usize) % WORDS.len()];
            let mut stream = search
                .text_search(TextSearchReq {
                    database: db.into(),
                    collection: coll.into(),
                    index: "txt".into(),
                    query: q.into(),
                    kind: "match".into(),
                    field: "body".into(),
                    top_k: 10,
                    slop: 0,
                    max_edits: 0,
                    max_expansions: 0,
                })
                .await
                .map_err(OpErr::Rpc)?
                .into_inner();
            while let Some(_d) = stream.message().await.map_err(OpErr::Rpc)? {}
            Ok(())
        }
        Op::Vector => {
            let v: Vec<f32> = (0..h.config.vector_dims)
                .map(|_| ((rng.next() % 1000) as f32) / 1000.0)
                .collect();
            let mut stream = search
                .vector_search(VectorSearchReq {
                    database: db.into(),
                    collection: coll.into(),
                    index: "vec".into(),
                    vector: v,
                    k: 10,
                    ef: 64,
                })
                .await
                .map_err(OpErr::Rpc)?
                .into_inner();
            while let Some(_d) = stream.message().await.map_err(OpErr::Rpc)? {}
            Ok(())
        }
        Op::Hybrid => {
            let v: Vec<f32> = (0..h.config.vector_dims)
                .map(|_| ((rng.next() % 1000) as f32) / 1000.0)
                .collect();
            let mut stream = search
                .hybrid_search(HybridSearchReq {
                    database: db.into(),
                    collection: coll.into(),
                    text_index: "txt".into(),
                    text_query: WORDS[(rng.next() as usize) % WORDS.len()].into(),
                    vector_index: "vec".into(),
                    vector: v,
                    vector_ef: 64,
                    limit: 10,
                    fetch_k: 40,
                    rrf_k: 60,
                    leg_weights: vec![],
                })
                .await
                .map_err(OpErr::Rpc)?
                .into_inner();
            while let Some(_d) = stream.message().await.map_err(OpErr::Rpc)? {}
            Ok(())
        }
        Op::BulkSeed => Ok(()), // never picked from the mix
    }
}
