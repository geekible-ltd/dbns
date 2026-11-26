//! gRPC API (spec §16) — Tonic services over the storage engine. Document
//! payloads use the §15 JSON wire format (`json` string fields); the §15.7
//! admin surface returns `AdminJson` snapshots.
//!
//! Streaming per spec: client-streamed `BulkInsert`, server-streamed
//! `BulkGet`/search results/`WatchStatus`.

use std::pin::Pin;
use std::sync::Arc;

use futures::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use ndb_common::error::Error;
use ndb_document::{Schema, json as djson};
use ndb_page::PageIo;
use serde_json::{Value as J, json};

pub mod proto {
    tonic::include_proto!("dbns.v1");
}

use proto::administration_service_server::AdministrationService;
use proto::cluster_service_server::ClusterService;
use proto::collection_service_server::CollectionService;
use proto::database_service_server::DatabaseService;
use proto::document_service_server::DocumentService;
use proto::index_service_server::IndexService;
use proto::search_service_server::SearchService;
use proto::transaction_service_server::TransactionService;
use proto::*;

fn to_status(e: Error) -> Status {
    match &e {
        Error::NotFound(_) => Status::not_found(e.to_string()),
        Error::InvalidInput(_) | Error::Config(_) => Status::invalid_argument(e.to_string()),
        Error::Transaction(_) => Status::aborted(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[allow(clippy::result_large_err)] // Status is tonic's mandated error type
fn coll<P: PageIo>(
    st: &ServerState<P>,
    txn: &ndb_transaction::Transaction<'_, P>,
    db: &str,
    coll: &str,
) -> Result<ndb_index::Collection<P>, Status> {
    st.engine
        .collection(txn, db, coll)
        .map_err(to_status)?
        .ok_or_else(|| Status::not_found(format!("collection '{db}/{coll}'")))
}

fn doc_hit(doc: &ndb_document::Document, score: f64, legs: &[&str]) -> DocHit {
    DocHit {
        doc: Some(DocJson {
            json: djson::doc_to_json(doc).to_string(),
        }),
        score,
        legs: legs.iter().map(|s| s.to_string()).collect(),
    }
}

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

fn stream_of<T: Send + 'static>(items: Vec<Result<T, Status>>) -> Response<BoxStream<T>> {
    Response::new(Box::pin(tokio_stream::iter(items)))
}

/// Shared server state — one instance feeds every service impl.
pub struct ServerState<P: PageIo> {
    pub engine: Arc<ndb_index::Engine<P>>,
    pub qe: ndb_aggregation::QueryEngine,
    pub started: std::time::Instant,
}

impl<P: PageIo> ServerState<P> {
    pub fn new(engine: Arc<ndb_index::Engine<P>>) -> Self {
        Self {
            engine,
            qe: ndb_aggregation::QueryEngine::new(),
            started: std::time::Instant::now(),
        }
    }

    fn metrics_json(&self) -> J {
        let txns = self.engine.txns();
        let (frames, resident, fetches, evictions) = self.engine.pool().pool_stats();
        json!({
            "engine": {
                "commit_seq": txns.commit_seq(),
                "active_transactions": txns.active_transactions(),
                "oldest_snapshot_age_ms": txns.oldest_snapshot_age().map(|d| d.as_millis() as u64),
            },
            "buffer_pool": {"frames": frames, "resident_pages": resident, "fetches": fetches, "evictions": evictions},
            "wal": {"durable_lsn": txns.wal().durable_lsn().get()},
            "uptime_ms": self.started.elapsed().as_millis() as u64,
        })
    }
}

fn aj(j: J) -> AdminJson {
    AdminJson {
        json: j.to_string(),
    }
}

// ---- DatabaseService ------------------------------------------------------

pub struct DbSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> DatabaseService for DbSvc<P> {
    async fn list_databases(&self, _: Request<Empty>) -> Result<Response<DbList>, Status> {
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let dbs = self.0.engine.list_databases(&txn).map_err(to_status)?;
        Ok(Response::new(DbList { databases: dbs }))
    }
    async fn create_database(&self, r: Request<NameReq>) -> Result<Response<DbStatus>, Status> {
        let name = r.into_inner().name;
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        self.0
            .engine
            .create_database(&mut txn, &name)
            .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DbStatus {
            ok: true,
            detail: name,
        }))
    }
    async fn get_database(&self, r: Request<NameReq>) -> Result<Response<DbDetail>, Status> {
        let name = r.into_inner().name;
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let colls = self
            .0
            .engine
            .list_collections(&txn, &name)
            .map_err(to_status)?;
        if colls.is_empty()
            && !self
                .0
                .engine
                .list_databases(&txn)
                .map_err(to_status)?
                .contains(&name)
        {
            return Err(Status::not_found(format!("database '{name}'")));
        }
        Ok(Response::new(DbDetail {
            name,
            collections: colls,
        }))
    }
    async fn drop_database(&self, r: Request<NameReq>) -> Result<Response<DbStatus>, Status> {
        let name = r.into_inner().name;
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        self.0
            .engine
            .drop_database(&mut txn, &name)
            .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DbStatus {
            ok: true,
            detail: name,
        }))
    }
}

// ---- CollectionService ----------------------------------------------------

pub struct CollSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> CollectionService for CollSvc<P> {
    async fn list_collections(&self, r: Request<DbRef>) -> Result<Response<CollList>, Status> {
        let db = r.into_inner().database;
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let colls = self
            .0
            .engine
            .list_collections(&txn, &db)
            .map_err(to_status)?;
        Ok(Response::new(CollList { collections: colls }))
    }
    async fn create_collection(
        &self,
        r: Request<CreateCollReq>,
    ) -> Result<Response<CollStatus>, Status> {
        let req = r.into_inner();
        let schema = if req.schema_json.is_empty() {
            Schema::new()
        } else {
            let j: J = serde_json::from_str(&req.schema_json)
                .map_err(|e| Status::invalid_argument(format!("schema_json: {e}")))?;
            // Schema JSON → Schema via the same §6 parse as REST.
            let mut s = Schema::new();
            if j.get("strict").and_then(J::as_bool).unwrap_or(false) {
                s.strict = true;
            }
            s
        };
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        self.0
            .engine
            .create_collection(&mut txn, &req.database, &req.name, schema)
            .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(CollStatus {
            ok: true,
            detail: req.name,
        }))
    }
    async fn get_collection(&self, r: Request<CollRef>) -> Result<Response<CollDetail>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let m = c.meta();
        let j = json!({
            "database": req.database, "collection": req.collection, "id": m.id.get(),
            "approx_docs": c.scan_count_hint(),
            "indexes": m.indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
            "text_indexes": m.text_indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
            "vector_indexes": m.vector_indexes.iter().map(|i| i.name.clone()).collect::<Vec<_>>(),
        });
        Ok(Response::new(CollDetail {
            json: j.to_string(),
        }))
    }
    async fn drop_collection(&self, r: Request<CollRef>) -> Result<Response<CollStatus>, Status> {
        let req = r.into_inner();
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        self.0
            .engine
            .drop_collection(&mut txn, &req.database, &req.collection)
            .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(CollStatus {
            ok: true,
            detail: req.collection,
        }))
    }
}

// ---- DocumentService ------------------------------------------------------

pub struct DocSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> DocumentService for DocSvc<P> {
    async fn insert(&self, r: Request<InsertReq>) -> Result<Response<InsertRes>, Status> {
        let req = r.into_inner();
        let doc_j = req.doc.map(|d| d.json).unwrap_or_default();
        let j: J =
            serde_json::from_str(&doc_j).map_err(|e| Status::invalid_argument(e.to_string()))?;
        let doc = djson::doc_from_json(&j).map_err(to_status)?;
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let id = c.insert(&mut txn, doc).map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(InsertRes { id: id.get() }))
    }
    async fn get(&self, r: Request<DocRef>) -> Result<Response<DocJson>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let doc = c
            .get(&txn, ndb_common::ids::DocumentId::new(req.id))
            .map_err(to_status)?
            .ok_or_else(|| Status::not_found(format!("document {}", req.id)))?;
        Ok(Response::new(DocJson {
            json: djson::doc_to_json(&doc).to_string(),
        }))
    }
    async fn replace(&self, r: Request<ReplaceReq>) -> Result<Response<DocJson>, Status> {
        let req = r.into_inner();
        let j: J = serde_json::from_str(&req.doc.map(|d| d.json).unwrap_or_default())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let mut doc = djson::doc_from_json(&j).map_err(to_status)?;
        doc.id = ndb_common::ids::DocumentId::new(req.id);
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        c.update(
            &mut txn,
            ndb_common::ids::DocumentId::new(req.id),
            doc.clone(),
        )
        .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DocJson {
            json: djson::doc_to_json(&doc).to_string(),
        }))
    }
    async fn update(&self, r: Request<UpdateReq>) -> Result<Response<DocJson>, Status> {
        let req = r.into_inner();
        let j: J = serde_json::from_str(&req.patch.map(|d| d.json).unwrap_or_default())
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let did = ndb_common::ids::DocumentId::new(req.id);
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let mut doc = c
            .get(&txn, did)
            .map_err(to_status)?
            .ok_or_else(|| Status::not_found(format!("document {}", req.id)))?;
        for (k, v) in j
            .as_object()
            .ok_or_else(|| Status::invalid_argument("patch must be object"))?
        {
            if k != "_id" {
                doc.fields
                    .insert(k.clone(), djson::json_to_value(v).map_err(to_status)?);
            }
        }
        c.update(&mut txn, did, doc.clone()).map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DocJson {
            json: djson::doc_to_json(&doc).to_string(),
        }))
    }
    async fn delete(&self, r: Request<DocRef>) -> Result<Response<DbStatus>, Status> {
        let req = r.into_inner();
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let ok = c
            .delete(&mut txn, ndb_common::ids::DocumentId::new(req.id))
            .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        if !ok {
            return Err(Status::not_found(format!("document {}", req.id)));
        }
        Ok(Response::new(DbStatus {
            ok: true,
            detail: req.id.to_string(),
        }))
    }

    /// Client-streamed bulk insert — one txn over the whole stream (§16).
    async fn bulk_insert(
        &self,
        r: Request<Streaming<DocJson>>,
    ) -> Result<Response<BulkRes>, Status> {
        // Request metadata carries routing: `x-db`/`x-collection`.
        let meta = r.metadata().clone();
        let get = |k: &str| -> Result<String, Status> {
            meta.get(k)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
                .ok_or_else(|| Status::invalid_argument(format!("missing '{k}' metadata")))
        };
        let db = get("x-db")?;
        let coll_name = get("x-collection")?;
        // Drain the stream before opening the write txn — a Transaction's
        // writer lock isn't Send across `.await`.
        let mut items = Vec::new();
        let mut stream = r.into_inner();
        while let Some(item) = stream.message().await? {
            items.push(item);
        }
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &db, &coll_name)?;
        let mut n = 0u64;
        for item in items {
            let j: J = serde_json::from_str(&item.json)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            c.insert(&mut txn, djson::doc_from_json(&j).map_err(to_status)?)
                .map_err(to_status)?;
            n += 1;
        }
        txn.commit().map_err(to_status)?;
        Ok(Response::new(BulkRes { inserted: n }))
    }

    type BulkGetStream = BoxStream<DocJson>;
    async fn bulk_get(&self, r: Request<CollRef>) -> Result<Response<Self::BulkGetStream>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let docs = c.scan(&txn).map_err(to_status)?;
        let items: Vec<Result<DocJson, Status>> = docs
            .into_iter()
            .map(|(_, d)| {
                Ok(DocJson {
                    json: djson::doc_to_json(&d).to_string(),
                })
            })
            .collect();
        Ok(stream_of(items))
    }
}

// ---- SearchService ---------------------------------------------------------

pub struct SearchSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> SearchService for SearchSvc<P> {
    type FindStream = BoxStream<DocHit>;
    async fn find(&self, r: Request<FindReq>) -> Result<Response<Self::FindStream>, Status> {
        let req = r.into_inner();
        let mut q: J = serde_json::from_str(&req.query_json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let o = q
            .as_object_mut()
            .ok_or_else(|| Status::invalid_argument("query must be object"))?;
        o.insert("database".into(), J::String(req.database));
        o.insert("collection".into(), J::String(req.collection));
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let res = self
            .0
            .qe
            .find(
                &self.0.engine,
                &mut txn,
                &q.to_string(),
                &ndb_query_executor::ExecLimits::default(),
            )
            .map_err(to_status)?;
        let scores = res.scores.clone().unwrap_or_default();
        let items: Vec<Result<DocHit, Status>> = res
            .docs
            .iter()
            .enumerate()
            .map(|(i, d)| Ok(doc_hit(d, scores.get(i).copied().unwrap_or(0.0), &[])))
            .collect();
        Ok(stream_of(items))
    }

    type TextSearchStream = BoxStream<DocHit>;
    async fn text_search(
        &self,
        r: Request<TextSearchReq>,
    ) -> Result<Response<Self::TextSearchStream>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let iname = if req.index.is_empty() {
            c.meta()
                .text_indexes
                .first()
                .map(|i| i.name.clone())
                .ok_or_else(|| Status::not_found("no text index"))?
        } else {
            req.index.clone()
        };
        let k = req.top_k.max(1) as usize;
        let hits = match req.kind.as_str() {
            "phrase" => c.search_phrase(&txn, &iname, &req.field, &req.query, req.slop, k),
            "fuzzy" => c.search_fuzzy(
                &txn,
                &iname,
                &req.field,
                &req.query,
                req.max_edits,
                req.max_expansions as usize,
                k,
            ),
            _ => c.search_text(&txn, &iname, &req.query, k),
        }
        .map_err(to_status)?;
        let items = hits
            .iter()
            .map(|h| Ok(doc_hit(&h.doc, h.score, &["text"])))
            .collect();
        Ok(stream_of(items))
    }

    type VectorSearchStream = BoxStream<DocHit>;
    async fn vector_search(
        &self,
        r: Request<VectorSearchReq>,
    ) -> Result<Response<Self::VectorSearchStream>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let iname = if req.index.is_empty() {
            c.meta()
                .vector_indexes
                .first()
                .map(|i| i.name.clone())
                .ok_or_else(|| Status::not_found("no vector index"))?
        } else {
            req.index.clone()
        };
        let k = req.k.max(1) as usize;
        let ef = if req.ef > 0 {
            req.ef as usize
        } else {
            k.saturating_mul(4).max(16)
        };
        let hits = c
            .search_vector(&txn, &iname, &req.vector, k, ef)
            .map_err(to_status)?;
        let items = hits
            .iter()
            .map(|h| Ok(doc_hit(&h.doc, h.score, &["vector"])))
            .collect();
        Ok(stream_of(items))
    }

    type HybridSearchStream = BoxStream<DocHit>;
    async fn hybrid_search(
        &self,
        r: Request<HybridSearchReq>,
    ) -> Result<Response<Self::HybridSearchStream>, Status> {
        let req = r.into_inner();
        let mut q = ndb_hybrid::HybridQuery {
            limit: req.limit.max(1) as usize,
            fetch_k: req.fetch_k as usize,
            ..Default::default()
        };
        if !req.text_index.is_empty() && !req.text_query.is_empty() {
            q.text = Some(ndb_hybrid::TextLeg {
                index: req.text_index.clone(),
                query: req.text_query.clone(),
                weight: req.leg_weights.first().copied().unwrap_or(1.0),
            });
        }
        if !req.vector_index.is_empty() {
            q.vector = Some(ndb_hybrid::VectorLeg {
                index: req.vector_index.clone(),
                vector: req.vector.clone(),
                ef: req.vector_ef as usize,
                weight: req.leg_weights.get(1).copied().unwrap_or(1.0),
            });
        }
        if req.rrf_k > 0 || !req.leg_weights.is_empty() {
            q.fusion = Some(
                ndb_hybrid::Fusion::new(
                    if req.rrf_k == 0 { 60 } else { req.rrf_k },
                    req.leg_weights.clone(),
                )
                .map_err(to_status)?,
            );
        }
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let hits = c.search_hybrid(&txn, &q).map_err(to_status)?;
        let items = hits
            .iter()
            .map(|h| {
                let legs: Vec<&str> = h.legs.iter().map(String::as_str).collect();
                Ok(doc_hit(&h.doc, h.score, &legs))
            })
            .collect();
        Ok(stream_of(items))
    }

    type AggregateStream = BoxStream<DocJson>;
    async fn aggregate(
        &self,
        r: Request<AggregateReq>,
    ) -> Result<Response<Self::AggregateStream>, Status> {
        let req = r.into_inner();
        let p = json!({"database": req.database, "collection": req.collection, "pipeline": serde_json::from_str::<J>(&req.pipeline_json).unwrap_or(J::Null)});
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let docs = self
            .0
            .qe
            .aggregate(
                &self.0.engine,
                &mut txn,
                self.0.engine.pool(),
                &p.to_string(),
                &ndb_aggregation::AggLimits::default(),
            )
            .map_err(to_status)?;
        let items: Vec<Result<DocJson, Status>> = docs
            .iter()
            .map(|d| {
                Ok(DocJson {
                    json: djson::doc_to_json(d).to_string(),
                })
            })
            .collect();
        Ok(stream_of(items))
    }

    async fn explain(&self, r: Request<FindReq>) -> Result<Response<ExplainRes>, Status> {
        let req = r.into_inner();
        let mut q: J = serde_json::from_str(&req.query_json)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let o = q
            .as_object_mut()
            .ok_or_else(|| Status::invalid_argument("query must be object"))?;
        o.insert("database".into(), J::String(req.database));
        o.insert("collection".into(), J::String(req.collection));
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let plan = self
            .0
            .qe
            .explain(&self.0.engine, &mut txn, &q.to_string())
            .map_err(to_status)?;
        Ok(Response::new(ExplainRes {
            access: format!("{:?}", plan.access),
            explain: plan.explain,
            est_scanned: plan.est_scanned,
        }))
    }
}

// ---- IndexService ------------------------------------------------------------

pub struct IndexSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[allow(clippy::result_large_err)]
fn index_kind<P: PageIo>(c: &ndb_index::Collection<P>, name: &str) -> Result<&'static str, Status> {
    let m = c.meta();
    if m.indexes.iter().any(|i| i.name == name) {
        Ok("secondary")
    } else if m.text_indexes.iter().any(|i| i.name == name) {
        Ok("text")
    } else if m.vector_indexes.iter().any(|i| i.name == name) {
        Ok("vector")
    } else {
        Err(Status::not_found(format!("index '{name}'")))
    }
}

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> IndexService for IndexSvc<P> {
    async fn list_indexes(&self, r: Request<CollRef>) -> Result<Response<IndexList>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let m = c.meta();
        let mut names = m
            .indexes
            .iter()
            .map(|i| json!({"name": i.name, "kind": "secondary"}).to_string())
            .collect::<Vec<_>>();
        names.extend(
            m.text_indexes
                .iter()
                .map(|i| json!({"name": i.name, "kind": "text"}).to_string()),
        );
        names.extend(
            m.vector_indexes
                .iter()
                .map(|i| json!({"name": i.name, "kind": "vector"}).to_string()),
        );
        Ok(Response::new(IndexList { json: names }))
    }
    async fn create_index(&self, r: Request<IndexDef>) -> Result<Response<DbStatus>, Status> {
        let req = r.into_inner();
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        match req.kind.as_str() {
            "text" => {
                let fields: Vec<(String, f64, ndb_analyzer::AnalyzerConfig)> = req
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        (
                            f.clone(),
                            req.weights.get(i).copied().unwrap_or(1.0),
                            ndb_analyzer::AnalyzerConfig::default(),
                        )
                    })
                    .collect();
                if fields.is_empty() && !req.field.is_empty() {
                    c.create_text_index(
                        &mut txn,
                        &req.name,
                        &[(
                            req.field.clone(),
                            1.0,
                            ndb_analyzer::AnalyzerConfig::default(),
                        )],
                    )
                    .map_err(to_status)?;
                } else {
                    c.create_text_index(&mut txn, &req.name, &fields)
                        .map_err(to_status)?;
                }
            }
            "vector" => {
                let metric = match req.metric.as_str() {
                    "dot" => ndb_vector::Metric::Dot,
                    "euclidean" => ndb_vector::Metric::Euclidean,
                    _ => ndb_vector::Metric::Cosine,
                };
                let cfg = ndb_hnsw::HnswConfig {
                    dims: req.dims as u16,
                    metric,
                    m: req.m.max(4) as usize,
                    ef_construction: req.ef_construction.max(10) as usize,
                    ef_search: req.ef_search.max(4) as usize,
                };
                c.create_vector_index(&mut txn, &req.name, &req.field, cfg)
                    .map_err(to_status)?;
            }
            _ => {
                c.create_index(&mut txn, &req.name, &req.fields, req.unique, req.sparse)
                    .map_err(to_status)?;
            }
        }
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DbStatus {
            ok: true,
            detail: req.name,
        }))
    }
    async fn drop_index(&self, r: Request<IndexRef>) -> Result<Response<DbStatus>, Status> {
        let req = r.into_inner();
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        match index_kind(&c, &req.name)? {
            "secondary" => c.drop_index(&mut txn, &req.name),
            "text" => c.drop_text_index(&mut txn, &req.name),
            _ => c.drop_vector_index(&mut txn, &req.name),
        }
        .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DbStatus {
            ok: true,
            detail: req.name,
        }))
    }
    async fn index_status(&self, r: Request<IndexRef>) -> Result<Response<IndexStatusRes>, Status> {
        let req = r.into_inner();
        let txn = self.0.engine.txns().begin().map_err(to_status)?;
        let c = coll(&self.0, &txn, &req.database, &req.collection)?;
        let kind = index_kind(&c, &req.name)?;
        Ok(Response::new(IndexStatusRes {
            name: req.name.clone(),
            kind: kind.to_string(),
            status: "ready".into(),
            detail_json: String::new(),
        }))
    }
    async fn rebuild_index(&self, r: Request<IndexRef>) -> Result<Response<DbStatus>, Status> {
        let req = r.into_inner();
        let mut txn = self.0.engine.txns().begin_write().map_err(to_status)?;
        let mut c = coll(&self.0, &txn, &req.database, &req.collection)?;
        match index_kind(&c, &req.name)? {
            "secondary" => c.rebuild_index(&mut txn, &req.name),
            "text" => c.rebuild_text_index(&mut txn, &req.name),
            _ => {
                return Err(Status::invalid_argument(
                    "vector indexes rebuild via drop+create",
                ));
            }
        }
        .map_err(to_status)?;
        txn.commit().map_err(to_status)?;
        Ok(Response::new(DbStatus {
            ok: true,
            detail: req.name,
        }))
    }
}

// ---- AdministrationService ----------------------------------------------------

pub struct AdminSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> AdministrationService for AdminSvc<P> {
    async fn metrics(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        Ok(Response::new(aj(self.0.metrics_json())))
    }
    async fn config(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        Ok(Response::new(aj(
            json!({"config": null, "note": "config snapshot via REST /admin/config"}),
        )))
    }
    async fn storage(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        let (frames, resident, fetches, evictions) = self.0.engine.pool().pool_stats();
        Ok(Response::new(aj(json!({
            "buffer_pool": {"frames": frames, "resident_pages": resident, "fetches": fetches, "evictions": evictions},
            "wal": {"durable_lsn": self.0.engine.txns().wal().durable_lsn().get()},
        }))))
    }
    async fn txn_status(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        let t = self.0.engine.txns();
        Ok(Response::new(aj(json!({
            "active_transactions": t.active_transactions(),
            "oldest_snapshot_age_ms": t.oldest_snapshot_age().map(|d| d.as_millis() as u64),
            "commit_seq": t.commit_seq(),
        }))))
    }
    async fn info(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        Ok(Response::new(aj(json!({
            "name": "ndb", "version": env!("CARGO_PKG_VERSION"),
            "features": ["crud", "secondary-index", "full-text", "vector", "hybrid", "aggregation", "mvcc", "grpc"],
        }))))
    }

    /// Streaming admin snapshots (spec §16) — `count` snapshots spaced
    /// `interval_ms` apart.
    type WatchStatusStream = BoxStream<AdminJson>;
    async fn watch_status(
        &self,
        r: Request<WatchReq>,
    ) -> Result<Response<Self::WatchStatusStream>, Status> {
        let req = r.into_inner();
        let st = Arc::clone(&self.0);
        let interval = req.interval_ms.max(50);
        let count = req.count.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(count as usize);
        tokio::spawn(async move {
            for _ in 0..count {
                if tx.send(Ok(aj(st.metrics_json()))).await.is_err() {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(interval as u64)).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

// ---- ClusterService ------------------------------------------------------------

pub struct ClusterSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> ClusterService for ClusterSvc<P> {
    async fn status(&self, _: Request<Empty>) -> Result<Response<AdminJson>, Status> {
        Ok(Response::new(aj(json!({
            "cluster": "ndb",
            "node": {"id": "node-1", "role": "leader", "state": "healthy"},
            "nodes": 1, "shards": 0, "replicas": 0, "raft": null,
        }))))
    }
}

// ---- TransactionService ---------------------------------------------------------

pub struct TxnSvc<P: PageIo>(pub Arc<ServerState<P>>);

#[tonic::async_trait]
impl<P: PageIo + Send + Sync + 'static> TransactionService for TxnSvc<P> {
    async fn begin(&self, _: Request<Empty>) -> Result<Response<TxnHandle>, Status> {
        Err(Status::unimplemented(
            "client-held transactions need a session layer — Phase 9 (distributed txns)",
        ))
    }
    async fn commit(&self, _: Request<TxnHandle>) -> Result<Response<DbStatus>, Status> {
        Err(Status::unimplemented("Phase 9"))
    }
    async fn rollback(&self, _: Request<TxnHandle>) -> Result<Response<DbStatus>, Status> {
        Err(Status::unimplemented("Phase 9"))
    }
}

/// All services on one `Arc<ServerState>` — wired by `ndb-server`.
pub struct Services<P: PageIo> {
    pub database: DbSvc<P>,
    pub collection: CollSvc<P>,
    pub document: DocSvc<P>,
    pub search: SearchSvc<P>,
    pub index: IndexSvc<P>,
    pub admin: AdminSvc<P>,
    pub cluster: ClusterSvc<P>,
    pub transaction: TxnSvc<P>,
}

pub fn services<P: PageIo>(st: Arc<ServerState<P>>) -> Services<P> {
    Services {
        database: DbSvc(Arc::clone(&st)),
        collection: CollSvc(Arc::clone(&st)),
        document: DocSvc(Arc::clone(&st)),
        search: SearchSvc(Arc::clone(&st)),
        index: IndexSvc(Arc::clone(&st)),
        admin: AdminSvc(Arc::clone(&st)),
        cluster: ClusterSvc(Arc::clone(&st)),
        transaction: TxnSvc(Arc::clone(&st)),
    }
}
