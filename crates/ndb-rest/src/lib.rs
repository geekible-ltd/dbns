//! REST API over the storage engine (spec §15) — Axum, versioned `/api/v1`.
//!
//! Handlers are thin adapters: every endpoint opens a transaction, calls
//! `Engine`/`Collection`/`QueryEngine`, and returns `doc_to_json` results.
//! Synchronous engine calls run inline — acceptable at Phase-8 scale; a
//! `spawn_blocking` hop is a load-tested refinement for Phase 11.

pub mod auth;

use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use serde_json::{Value as J, json};

use ndb_common::error::Error;
use ndb_document::{Schema, json as djson};
use ndb_page::PageIo;
use serde::Deserialize;

/// Shared application state.
pub struct AppState<P: PageIo> {
    pub engine: Arc<ndb_index::Engine<P>>,
    pub qe: ndb_aggregation::QueryEngine,
    pub auth: auth::AuthState,
    /// Optional effective config for `/admin/config` (§15.7) — secrets are
    /// redacted before serialisation.
    pub config: Mutex<Option<serde_yaml::Value>>,
    pub started: std::time::Instant,
    /// gRPC dial target for the stress runner (`http://127.0.0.1:{port}`),
    /// set by `ndb-server` at boot — the stress panel drives the real gRPC
    /// listener so timings include transport + prost codec.
    pub grpc_target: Mutex<Option<String>>,
    /// At most one stress run at a time.
    pub stress: Mutex<Option<Arc<ndb_stress::StressHandle>>>,
}

impl<P: PageIo> AppState<P> {
    pub fn new(engine: Arc<ndb_index::Engine<P>>) -> Self {
        Self {
            engine,
            qe: ndb_aggregation::QueryEngine::new(),
            auth: Arc::new(auth::AuthConfig::default()),
            config: Mutex::new(None),
            started: std::time::Instant::now(),
            grpc_target: Mutex::new(None),
            stress: Mutex::new(None),
        }
    }
}

/// Builds the `/api/v1` router.
pub fn router<P>(state: Arc<AppState<P>>) -> Router
where
    P: PageIo + Send + Sync + 'static,
{
    // Versioned base path (spec §15) — nested so health/ready live outside
    // the version prefix too.
    let api = Router::new()
        // probes — outside auth
        .route("/health", get(health))
        .route("/ready", get(ready))
        // databases (§15.1)
        .route(
            "/databases",
            get(list_databases::<P>).post(create_database::<P>),
        )
        .route(
            "/databases/{db}",
            get(get_database::<P>).delete(drop_database::<P>),
        )
        // collections (§15.2)
        .route(
            "/databases/{db}/collections",
            get(list_collections::<P>).post(create_collection::<P>),
        )
        .route(
            "/collections/{db}/{coll}",
            get(collection_details::<P>).delete(drop_collection::<P>),
        )
        // documents (§15.3)
        .route(
            "/collections/{db}/{coll}/documents",
            post(insert_document::<P>),
        )
        .route(
            "/collections/{db}/{coll}/documents/{id}",
            get(get_document::<P>)
                .put(replace_document::<P>)
                .patch(patch_document::<P>)
                .delete(delete_document::<P>),
        )
        .route(
            "/collections/{db}/{coll}/documents/bulk",
            post(bulk_documents::<P>),
        )
        // search + query (§15.4)
        .route("/collections/{db}/{coll}/search", post(search_text::<P>))
        .route(
            "/collections/{db}/{coll}/vector-search",
            post(search_vector::<P>),
        )
        .route(
            "/collections/{db}/{coll}/hybrid-search",
            post(search_hybrid::<P>),
        )
        .route("/collections/{db}/{coll}/aggregate", post(aggregate::<P>))
        .route("/collections/{db}/{coll}/explain", post(explain::<P>))
        .route("/collections/{db}/{coll}/query", post(find::<P>))
        // indexes (§15.5)
        .route(
            "/collections/{db}/{coll}/indexes",
            get(list_indexes::<P>).post(create_index::<P>),
        )
        .route(
            "/collections/{db}/{coll}/indexes/{name}",
            get(index_status::<P>).delete(drop_index::<P>),
        )
        .route(
            "/collections/{db}/{coll}/indexes/{name}/rebuild",
            post(rebuild_index::<P>),
        )
        .route(
            "/collections/{db}/{coll}/indexes/{name}/validate",
            get(validate_index::<P>),
        )
        // cluster (§15.6 — single-node stub until Phase 9)
        .route("/cluster/status", get(cluster_status::<P>))
        // admin (§15.7)
        .route("/admin/metrics", get(admin_metrics::<P>))
        .route(
            "/admin/stress",
            post(stress_start::<P>).get(stress_status::<P>),
        )
        .route("/admin/stress/stop", post(stress_stop::<P>))
        .route("/admin/config", get(admin_config::<P>))
        .route("/admin/storage", get(admin_storage::<P>))
        .route("/admin/txn", get(admin_txn::<P>))
        .route("/admin/info", get(admin_info::<P>))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state.auth),
            auth::auth_middleware,
        ))
        .with_state(state);
    Router::new().nest("/api/v1", api)
}

// ---- error mapping -------------------------------------------------------

struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        Self(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match &self.0 {
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::InvalidInput(_) => StatusCode::BAD_REQUEST,
            Error::Transaction(_) => StatusCode::CONFLICT,
            Error::Config(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.0.to_string() }))).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

// ---- probes --------------------------------------------------------------

async fn health() -> &'static str {
    "ok"
}

async fn ready() -> &'static str {
    "ready"
}

// ---- databases (§15.1) ---------------------------------------------------

async fn list_databases<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let dbs = st.engine.list_databases(&txn).map_err(ApiError::from)?;
    Ok(Json(json!({ "databases": dbs })))
}

#[derive(Deserialize)]
struct NameBody {
    name: String,
}

async fn create_database<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Json(body): Json<NameBody>,
) -> ApiResult<impl IntoResponse> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    st.engine
        .create_database(&mut txn, &body.name)
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(json!({ "database": body.name }))))
}

async fn get_database<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path(db): Path<String>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let colls = st
        .engine
        .list_collections(&txn, &db)
        .map_err(ApiError::from)?;
    if colls.is_empty()
        && !st
            .engine
            .list_databases(&txn)
            .map_err(ApiError::from)?
            .contains(&db)
    {
        return Err(ApiError(Error::NotFound(format!("database '{db}'"))));
    }
    Ok(Json(json!({ "database": db, "collections": colls })))
}

async fn drop_database<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path(db): Path<String>,
) -> ApiResult<impl IntoResponse> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    st.engine
        .drop_database(&mut txn, &db)
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- collections (§15.2) -------------------------------------------------

async fn list_collections<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path(db): Path<String>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let colls = st
        .engine
        .list_collections(&txn, &db)
        .map_err(ApiError::from)?;
    Ok(Json(json!({ "collections": colls })))
}

#[derive(Deserialize)]
struct CollectionBody {
    name: String,
    /// Optional JSON schema — `{strict, fields:{name:{type,required}}}`
    /// (§6). Absent → schemaless.
    #[serde(default)]
    schema: Option<J>,
}

async fn create_collection<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path(db): Path<String>,
    Json(body): Json<CollectionBody>,
) -> ApiResult<impl IntoResponse> {
    let schema = match &body.schema {
        Some(j) => schema_from_json(j)?,
        None => Schema::new(),
    };
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    st.engine
        .create_collection(&mut txn, &db, &body.name, schema)
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok((
        StatusCode::CREATED,
        Json(json!({ "collection": body.name, "database": db })),
    ))
}

fn schema_from_json(j: &J) -> ApiResult<Schema> {
    // Minimal §6 shape: {"strict": bool, "fields": {"f": {"type":"int",
    // "required": true}}}
    let mut s = Schema::new();
    if j.get("strict").and_then(J::as_bool).unwrap_or(false) {
        s.strict = true;
    }
    if let Some(fields) = j.get("fields").and_then(J::as_object) {
        for (name, def) in fields {
            let ty = def.get("type").and_then(J::as_str).unwrap_or("any");
            let required = def.get("required").and_then(J::as_bool).unwrap_or(false);
            s = if ty == "any" {
                s.field_any_type(name, required)
            } else {
                s.field(name, parse_dtype(ty)?, required)
            };
        }
    }
    Ok(s)
}

fn parse_dtype(t: &str) -> ApiResult<ndb_document::DataType> {
    use ndb_document::DataType as D;
    Ok(match t.to_ascii_lowercase().as_str() {
        "null" => D::Null,
        "bool" | "boolean" => D::Bool,
        "int" | "int64" => D::Int,
        "uint" | "uint64" => D::UInt,
        "float" | "double" => D::Float,
        "decimal" => D::Decimal,
        "string" => D::String,
        "binary" => D::Binary,
        "datetime" | "date" => D::DateTime,
        "object" => D::Object,
        "array" => D::Array,
        "uuid" => D::Uuid,
        "vector" => D::Vector,
        other => {
            return Err(ApiError(Error::InvalidInput(format!(
                "bad dtype '{other}'"
            ))));
        }
    })
}

async fn collection_details<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = st
        .engine
        .collection(&txn, &db, &coll)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(Error::NotFound(format!("collection '{db}/{coll}'"))))?;
    let m = c.meta();
    Ok(Json(json!({
        "database": db,
        "collection": coll,
        "id": m.id.get(),
        "approx_docs": c.scan_count_hint(),
        "indexes": m.indexes.iter().map(|i| json!({"name": i.name, "fields": i.fields, "unique": i.unique, "sparse": i.sparse})).collect::<Vec<_>>(),
        "text_indexes": m.text_indexes.iter().map(|i| json!({"name": i.name, "fields": i.fields.iter().map(|f| f.field.clone()).collect::<Vec<_>>()})).collect::<Vec<_>>(),
        "vector_indexes": m.vector_indexes.iter().map(|i| json!({"name": i.name, "field": i.field, "dims": i.dims, "metric": i.metric})).collect::<Vec<_>>(),
    })))
}

async fn drop_collection<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
) -> ApiResult<impl IntoResponse> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    st.engine
        .drop_collection(&mut txn, &db, &coll)
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- documents (§15.3) ---------------------------------------------------

async fn insert_document<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<impl IntoResponse> {
    let doc = djson::doc_from_json(&body).map_err(ApiError::from)?;
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let id = c.insert(&mut txn, doc).map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(json!({ "_id": id.get() }))))
}

async fn get_document<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, id)): Path<(String, String, u64)>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let doc = c
        .get(&txn, ndb_common::ids::DocumentId::new(id))
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(Error::NotFound(format!("document {id}"))))?;
    Ok(Json(djson::doc_to_json(&doc)))
}

async fn replace_document<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, id)): Path<(String, String, u64)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let mut doc = djson::doc_from_json(&body).map_err(ApiError::from)?;
    doc.id = ndb_common::ids::DocumentId::new(id);
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    c.update(&mut txn, ndb_common::ids::DocumentId::new(id), doc)
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(Json(json!({ "_id": id, "replaced": true })))
}

async fn patch_document<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, id)): Path<(String, String, u64)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let did = ndb_common::ids::DocumentId::new(id);
    let mut doc = c
        .get(&txn, did)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(Error::NotFound(format!("document {id}"))))?;
    // Merge — PATCH touches only the provided fields.
    let o = body
        .as_object()
        .ok_or_else(|| ApiError(Error::InvalidInput("patch body must be object".into())))?;
    for (k, v) in o {
        if k == "_id" {
            continue;
        }
        doc.fields
            .insert(k.clone(), djson::json_to_value(v).map_err(ApiError::from)?);
    }
    c.update(&mut txn, did, doc).map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(Json(json!({ "_id": id, "patched": true })))
}

async fn delete_document<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, id)): Path<(String, String, u64)>,
) -> ApiResult<impl IntoResponse> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let existed = c
        .delete(&mut txn, ndb_common::ids::DocumentId::new(id))
        .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    if !existed {
        return Err(ApiError(Error::NotFound(format!("document {id}"))));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Bulk operations (§15.3): `{"ops":[{"insert":{doc}},
/// {"update":{"id":n,"fields":{…}}}, {"delete":{"id":n}}]}` — one
/// transaction, atomic across the batch.
async fn bulk_documents<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let ops = body
        .get("ops")
        .and_then(J::as_array)
        .ok_or_else(|| ApiError(Error::InvalidInput("bulk body needs 'ops' array".into())))?;
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let mut done = 0usize;
    for op in ops {
        let o = op
            .as_object()
            .ok_or_else(|| ApiError(Error::InvalidInput("bulk op must be object".into())))?;
        if let Some(d) = o.get("insert") {
            c.insert(&mut txn, djson::doc_from_json(d).map_err(ApiError::from)?)
                .map_err(ApiError::from)?;
        } else if let Some(u) = o.get("update") {
            let id = u
                .get("id")
                .and_then(J::as_u64)
                .ok_or_else(|| ApiError(Error::InvalidInput("update op needs 'id'".into())))?;
            let fields = u
                .get("fields")
                .ok_or_else(|| ApiError(Error::InvalidInput("update op needs 'fields'".into())))?;
            let did = ndb_common::ids::DocumentId::new(id);
            let mut doc = c
                .get(&txn, did)
                .map_err(ApiError::from)?
                .ok_or_else(|| ApiError(Error::NotFound(format!("document {id}"))))?;
            for (k, v) in fields
                .as_object()
                .ok_or_else(|| ApiError(Error::InvalidInput("'fields' must be object".into())))?
            {
                doc.fields
                    .insert(k.clone(), djson::json_to_value(v).map_err(ApiError::from)?);
            }
            c.update(&mut txn, did, doc).map_err(ApiError::from)?;
        } else if let Some(d) = o.get("delete") {
            let id = d
                .get("id")
                .and_then(J::as_u64)
                .ok_or_else(|| ApiError(Error::InvalidInput("delete op needs 'id'".into())))?;
            c.delete(&mut txn, ndb_common::ids::DocumentId::new(id))
                .map_err(ApiError::from)?;
        } else {
            return Err(ApiError(Error::InvalidInput(
                "bulk op needs insert|update|delete".into(),
            )));
        }
        done += 1;
    }
    txn.commit().map_err(ApiError::from)?;
    Ok(Json(json!({ "ok": done })))
}

fn coll_for<P: PageIo>(
    st: &AppState<P>,
    txn: &ndb_transaction::Transaction<'_, P>,
    db: &str,
    coll: &str,
) -> ApiResult<ndb_index::Collection<P>> {
    st.engine
        .collection(txn, db, coll)
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError(Error::NotFound(format!("collection '{db}/{coll}'"))))
}

// ---- search (§15.4) ------------------------------------------------------

async fn search_text<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let query = body
        .get("query")
        .and_then(J::as_str)
        .ok_or_else(|| ApiError(Error::InvalidInput("search needs 'query'".into())))?;
    let index = body.get("index").and_then(J::as_str).unwrap_or("");
    let top_k = body.get("topK").and_then(J::as_u64).unwrap_or(10) as usize;
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    // Resolve the index name — explicit or the collection's only text index.
    let iname = if index.is_empty() {
        c.meta()
            .text_indexes
            .first()
            .map(|i| i.name.clone())
            .ok_or_else(|| ApiError(Error::NotFound("no text index".into())))?
    } else {
        index.to_string()
    };
    let kind = body.get("kind").and_then(J::as_str).unwrap_or("match");
    let hits = match kind {
        "match" => c
            .search_text(&txn, &iname, query, top_k)
            .map_err(ApiError::from)?,
        "phrase" => {
            let field = body.get("field").and_then(J::as_str).unwrap_or("");
            let slop = body.get("slop").and_then(J::as_u64).unwrap_or(0) as u32;
            c.search_phrase(&txn, &iname, field, query, slop, top_k)
                .map_err(ApiError::from)?
        }
        "fuzzy" => {
            let field = body.get("field").and_then(J::as_str).unwrap_or("");
            let max_edits = body.get("maxEdits").and_then(J::as_u64).unwrap_or(2) as u32;
            let max_exp = body.get("maxExpansions").and_then(J::as_u64).unwrap_or(50) as usize;
            c.search_fuzzy(&txn, &iname, field, query, max_edits, max_exp, top_k)
                .map_err(ApiError::from)?
        }
        k => {
            return Err(ApiError(Error::InvalidInput(format!(
                "bad search kind '{k}'"
            ))));
        }
    };
    Ok(Json(json!({
        "hits": hits.iter().map(|h| json!({
            "_id": h.doc_id.get(),
            "score": h.score,
            "doc": djson::doc_to_json(&h.doc),
        })).collect::<Vec<_>>()
    })))
}

async fn search_vector<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let vector = jvec_f32(&body, "vector")?;
    let k = body.get("k").and_then(J::as_u64).unwrap_or(10) as usize;
    let ef = body.get("ef").and_then(J::as_u64).unwrap_or(0) as usize;
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let iname = body
        .get("index")
        .and_then(J::as_str)
        .map(str::to_string)
        .or_else(|| c.meta().vector_indexes.first().map(|i| i.name.clone()))
        .ok_or_else(|| ApiError(Error::NotFound("no vector index".into())))?;
    let ef_n = if ef > 0 {
        ef
    } else {
        k.saturating_mul(4).max(16)
    };
    let hits = c
        .search_vector(&txn, &iname, &vector, k, ef_n)
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "hits": hits.iter().map(|h| json!({
            "_id": h.doc_id.get(),
            "score": h.score,
            "doc": djson::doc_to_json(&h.doc),
        })).collect::<Vec<_>>()
    })))
}

async fn search_hybrid<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let mut q = ndb_hybrid::HybridQuery {
        limit: body.get("limit").and_then(J::as_u64).unwrap_or(10) as usize,
        fetch_k: body.get("fetchK").and_then(J::as_u64).unwrap_or(0) as usize,
        ..Default::default()
    };
    if let Some(t) = body.get("text").and_then(J::as_object) {
        q.text = Some(ndb_hybrid::TextLeg {
            index: t
                .get("index")
                .and_then(J::as_str)
                .unwrap_or_default()
                .to_string(),
            query: t
                .get("query")
                .and_then(J::as_str)
                .ok_or_else(|| ApiError(Error::InvalidInput("text leg needs 'query'".into())))?
                .to_string(),
            weight: t.get("weight").and_then(J::as_f64).unwrap_or(1.0),
        });
    }
    if let Some(v) = body.get("vector").and_then(J::as_object) {
        q.vector = Some(ndb_hybrid::VectorLeg {
            index: v
                .get("index")
                .and_then(J::as_str)
                .unwrap_or_default()
                .to_string(),
            vector: jvec_f32(&J::Object(v.clone()), "vector")?,
            ef: v.get("ef").and_then(J::as_u64).unwrap_or(0) as usize,
            weight: v.get("weight").and_then(J::as_f64).unwrap_or(1.0),
        });
    }
    if let Some(ids) = body.get("candidateIds").and_then(J::as_array) {
        let set: std::collections::HashSet<u64> = ids.iter().filter_map(J::as_u64).collect();
        q.candidate_filter = Some(Box::new(move |d| set.contains(&d)));
    }
    if let Some(f) = body.get("fusion").and_then(J::as_object) {
        let k = f.get("k").and_then(J::as_u64).unwrap_or(60) as u32;
        let weights: Vec<f64> = f
            .get("weights")
            .and_then(J::as_array)
            .map(|a| a.iter().filter_map(J::as_f64).collect())
            .unwrap_or_default();
        q.fusion = Some(ndb_hybrid::Fusion::new(k, weights).map_err(ApiError::from)?);
    }
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let hits = c.search_hybrid(&txn, &q).map_err(ApiError::from)?;
    Ok(Json(json!({
        "hits": hits.iter().map(|h| json!({
            "_id": h.doc_id.get(),
            "score": h.score,
            "legs": h.legs,
            "doc": djson::doc_to_json(&h.doc),
        })).collect::<Vec<_>>()
    })))
}

fn jvec_f32(body: &J, key: &str) -> ApiResult<Vec<f32>> {
    let arr = body.get(key).and_then(J::as_array).ok_or_else(|| {
        ApiError(Error::InvalidInput(format!(
            "'{key}' must be a number array"
        )))
    })?;
    let mut out = Vec::with_capacity(arr.len());
    for e in arr {
        out.push(
            e.as_f64().ok_or_else(|| {
                ApiError(Error::InvalidInput("vector entries must be numbers".into()))
            })? as f32,
        );
    }
    Ok(out)
}

async fn aggregate<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let pipeline = json!({
        "database": db, "collection": coll,
        "pipeline": body.get("pipeline").cloned().unwrap_or(J::Array(vec![])),
    });
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let docs = st
        .qe
        .aggregate(
            &st.engine,
            &mut txn,
            st.engine.pool(),
            &pipeline.to_string(),
            &ndb_aggregation::AggLimits::default(),
        )
        .map_err(ApiError::from)?;
    Ok(Json(
        json!({ "docs": docs.iter().map(djson::doc_to_json).collect::<Vec<_>>() }),
    ))
}

async fn explain<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let query = query_with_coll(&body, &db, &coll)?;
    let mut txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let plan = st
        .qe
        .explain(&st.engine, &mut txn, &query.to_string())
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "access": format!("{:?}", plan.access),
        "explain": plan.explain,
        "est_scanned": plan.est_scanned,
    })))
}

async fn find<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    let query = query_with_coll(&body, &db, &coll)?;
    // Pure read — snapshot txn; begin_write would serialize queries
    // behind the single-writer lock for no benefit.
    let mut txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let r = st
        .qe
        .find(
            &st.engine,
            &mut txn,
            &query.to_string(),
            &ndb_query_executor::ExecLimits::default(),
        )
        .map_err(ApiError::from)?;
    Ok(Json(json!({
        "docs": r.docs.iter().map(djson::doc_to_json).collect::<Vec<_>>(),
        "scores": r.scores,
        "explain": r.explain,
    })))
}

/// Injects `{database, collection}` into a caller's query body — the find
/// JSON format the parser expects.
fn query_with_coll(body: &J, db: &str, coll: &str) -> ApiResult<J> {
    let mut q = body.clone();
    let o = q
        .as_object_mut()
        .ok_or_else(|| ApiError(Error::InvalidInput("query body must be object".into())))?;
    o.insert("database".into(), J::String(db.to_string()));
    o.insert("collection".into(), J::String(coll.to_string()));
    Ok(q)
}

// ---- indexes (§15.5) -----------------------------------------------------

async fn list_indexes<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let m = c.meta();
    let mut all = m
        .indexes
        .iter()
        .map(|i| json!({"name": i.name, "kind": "secondary", "fields": i.fields, "unique": i.unique, "sparse": i.sparse}))
        .collect::<Vec<_>>();
    all.extend(m.text_indexes.iter().map(|i| {
        json!({"name": i.name, "kind": "text", "fields": i.fields.iter().map(|f| f.field.clone()).collect::<Vec<_>>()})
    }));
    all.extend(
        m.vector_indexes
            .iter()
            .map(|i| json!({"name": i.name, "kind": "vector", "field": i.field, "dims": i.dims})),
    );
    Ok(Json(json!({ "indexes": all })))
}

async fn create_index<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll)): Path<(String, String)>,
    Json(body): Json<J>,
) -> ApiResult<impl IntoResponse> {
    let name = body
        .get("name")
        .and_then(J::as_str)
        .ok_or_else(|| ApiError(Error::InvalidInput("index needs 'name'".into())))?
        .to_string();
    let kind = body.get("kind").and_then(J::as_str).unwrap_or("secondary");
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    match kind {
        "secondary" => {
            let fields: Vec<String> = body
                .get("fields")
                .and_then(J::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(|f| f.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if fields.is_empty() {
                return Err(ApiError(Error::InvalidInput(
                    "secondary index needs 'fields'".into(),
                )));
            }
            let unique = body.get("unique").and_then(J::as_bool).unwrap_or(false);
            let sparse = body.get("sparse").and_then(J::as_bool).unwrap_or(false);
            c.create_index(&mut txn, &name, &fields, unique, sparse)
                .map_err(ApiError::from)?;
        }
        "text" => {
            // {"kind":"text","fields":[{"field":"body","weight":1.0}]}
            let defs = body
                .get("fields")
                .and_then(J::as_array)
                .ok_or_else(|| ApiError(Error::InvalidInput("text index needs 'fields'".into())))?;
            let mut fields = Vec::new();
            for d in defs {
                let f = d.get("field").and_then(J::as_str).ok_or_else(|| {
                    ApiError(Error::InvalidInput("text field needs 'field'".into()))
                })?;
                let w = d.get("weight").and_then(J::as_f64).unwrap_or(1.0);
                fields.push((f.to_string(), w, ndb_analyzer::AnalyzerConfig::default()));
            }
            c.create_text_index(&mut txn, &name, &fields)
                .map_err(ApiError::from)?;
        }
        "vector" => {
            // {"kind":"vector","field":"emb","dims":8,"metric":"cosine",
            //  "m":16,"efConstruction":200,"efSearch":64}
            let field = body.get("field").and_then(J::as_str).ok_or_else(|| {
                ApiError(Error::InvalidInput("vector index needs 'field'".into()))
            })?;
            let dims = body.get("dims").and_then(J::as_u64).unwrap_or(0) as u16;
            let metric = match body.get("metric").and_then(J::as_str).unwrap_or("cosine") {
                "cosine" => ndb_vector::Metric::Cosine,
                "dot" => ndb_vector::Metric::Dot,
                "euclidean" => ndb_vector::Metric::Euclidean,
                m => return Err(ApiError(Error::InvalidInput(format!("bad metric '{m}'")))),
            };
            let cfg = ndb_hnsw::HnswConfig {
                dims,
                metric,
                m: body.get("m").and_then(J::as_u64).unwrap_or(16) as usize,
                ef_construction: body
                    .get("efConstruction")
                    .and_then(J::as_u64)
                    .unwrap_or(200) as usize,
                ef_search: body.get("efSearch").and_then(J::as_u64).unwrap_or(64) as usize,
            };
            c.create_vector_index(&mut txn, &name, field, cfg)
                .map_err(ApiError::from)?;
        }
        k => {
            return Err(ApiError(Error::InvalidInput(format!(
                "bad index kind '{k}'"
            ))));
        }
    }
    txn.commit().map_err(ApiError::from)?;
    Ok((StatusCode::CREATED, Json(json!({ "index": name }))))
}

async fn drop_index<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> ApiResult<impl IntoResponse> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let kind = index_kind(&c, &name)?;
    match kind.as_str() {
        "secondary" => c.drop_index(&mut txn, &name),
        "text" => c.drop_text_index(&mut txn, &name),
        _ => c.drop_vector_index(&mut txn, &name),
    }
    .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn index_status<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let kind = index_kind(&c, &name)?;
    let m = c.meta();
    let detail = match kind.as_str() {
        "secondary" => m.indexes.iter().find(|i| i.name == name).map(|i| {
            json!({"fields": i.fields, "unique": i.unique, "sparse": i.sparse, "root": i.root.get()})
        }),
        "text" => m.text_indexes.iter().find(|i| i.name == name).map(|i| {
            json!({"fields": i.fields.iter().map(|f| json!({"field": f.field, "weight": f.weight, "analyzer_version": f.analyzer_version})).collect::<Vec<_>>(), "root": i.root.get()})
        }),
        _ => m.vector_indexes.iter().find(|i| i.name == name).map(|i| {
            json!({"field": i.field, "dims": i.dims, "metric": i.metric, "m": i.m, "ef_construction": i.ef_construction, "ef_search": i.ef_search, "root": i.root.get()})
        }),
    };
    Ok(Json(json!({
        "name": name, "kind": kind, "status": "ready",
        "detail": detail.unwrap_or(J::Null),
    })))
}

async fn rebuild_index<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> ApiResult<Json<J>> {
    let mut txn = st.engine.txns().begin_write().map_err(ApiError::from)?;
    let mut c = coll_for(&st, &txn, &db, &coll)?;
    let kind = index_kind(&c, &name)?;
    match kind.as_str() {
        "secondary" => c.rebuild_index(&mut txn, &name),
        "text" => c.rebuild_text_index(&mut txn, &name),
        _ => {
            return Err(ApiError(Error::InvalidInput(
                "vector indexes rebuild via drop+create".into(),
            )));
        }
    }
    .map_err(ApiError::from)?;
    txn.commit().map_err(ApiError::from)?;
    Ok(Json(json!({ "index": name, "rebuilt": true })))
}

async fn validate_index<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Path((db, coll, name)): Path<(String, String, String)>,
) -> ApiResult<Json<J>> {
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let c = coll_for(&st, &txn, &db, &coll)?;
    let kind = index_kind(&c, &name)?;
    // Validate = definition resolvable + btree opens and scans cleanly.
    let entries = match kind.as_str() {
        "secondary" => c
            .lookup_ordered(&txn, &name, &[], &[])
            .map_err(ApiError::from)?
            .len(),
        _ => 0,
    };
    Ok(Json(
        json!({ "name": name, "kind": kind, "valid": true, "entries_scanned": entries }),
    ))
}

fn index_kind<P: PageIo>(c: &ndb_index::Collection<P>, name: &str) -> ApiResult<String> {
    let m = c.meta();
    if m.indexes.iter().any(|i| i.name == name) {
        Ok("secondary".into())
    } else if m.text_indexes.iter().any(|i| i.name == name) {
        Ok("text".into())
    } else if m.vector_indexes.iter().any(|i| i.name == name) {
        Ok("vector".into())
    } else {
        Err(ApiError(Error::NotFound(format!("index '{name}'"))))
    }
}

// ---- cluster (§15.6 stub) + admin (§15.7) --------------------------------

async fn cluster_status<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    // Single-node until Phase 9 — the shape mirrors what Raft will fill.
    let txn = st.engine.txns().begin().map_err(ApiError::from)?;
    let _ = txn;
    Ok(Json(json!({
        "cluster": "ndb",
        "node": {"id": "node-1", "role": "leader", "state": "healthy"},
        "nodes": 1, "shards": 0, "replicas": 0, "raft": null,
    })))
}

async fn admin_metrics<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let txns = st.engine.txns();
    let (frames, resident, fetches, evictions) = st.engine.pool().pool_stats();
    Ok(Json(json!({
        "engine": {
            "commit_seq": txns.commit_seq(),
            "active_transactions": txns.active_transactions(),
            "oldest_snapshot_age_ms": txns.oldest_snapshot_age().map(|d| d.as_millis() as u64),
        },
        "buffer_pool": {
            "frames": frames, "resident_pages": resident,
            "fetches": fetches, "evictions": evictions,
        },
        "wal": { "durable_lsn": txns.wal().durable_lsn().get() },
        "uptime_ms": st.started.elapsed().as_millis() as u64,
    })))
}

async fn admin_config<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let cfg = st.config.lock().unwrap().clone();
    let j = cfg
        .as_ref()
        .map(|c| {
            let s = serde_json::to_string(c).unwrap_or_else(|_| "{}".into());
            let mut v: J = serde_json::from_str(&s).unwrap_or(J::Null);
            redact(&mut v);
            v
        })
        .unwrap_or(J::Null);
    Ok(Json(
        json!({ "config": j, "redacted": ["secret", "key", "password", "token"] }),
    ))
}

/// Recursively masks values whose key name suggests a secret (§15.7).
fn redact(v: &mut J) {
    const NEEDLES: [&str; 4] = ["secret", "key", "password", "token"];
    match v {
        J::Object(m) => {
            for (k, val) in m.iter_mut() {
                let kl = k.to_ascii_lowercase();
                if NEEDLES.iter().any(|n| kl.contains(n)) {
                    *val = J::String("***".into());
                } else {
                    redact(val);
                }
            }
        }
        J::Array(a) => a.iter_mut().for_each(redact),
        _ => {}
    }
}

async fn admin_storage<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let (frames, resident, fetches, evictions) = st.engine.pool().pool_stats();
    let txns = st.engine.txns();
    Ok(Json(json!({
        "buffer_pool": {
            "frames": frames, "resident_pages": resident,
            "page_size": st.engine.pool().page_size(),
            "fetches": fetches, "evictions": evictions,
        },
        "wal": { "durable_lsn": txns.wal().durable_lsn().get() },
        "checkpoint": { "note": "call engine.checkpoint(); last seq in catalog" },
        "page_allocation": { "allocated_pages": resident },
    })))
}

async fn admin_txn<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let txns = st.engine.txns();
    Ok(Json(json!({
        "active_transactions": txns.active_transactions(),
        "oldest_snapshot_age_ms": txns.oldest_snapshot_age().map(|d| d.as_millis() as u64),
        "commit_seq": txns.commit_seq(),
        "snapshot_retention_horizon": txns.commit_seq(),
    })))
}

async fn admin_info<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    let _ = st;
    Ok(Json(json!({
        "name": "ndb",
        "version": env!("CARGO_PKG_VERSION"),
        "features": ["crud", "secondary-index", "full-text", "vector", "hybrid",
                     "aggregation", "mvcc", "rest"],
        "build": { "rustc": "stable", "profile": "debug-or-release" },
    })))
}

// ---- stress runner (§22 perf support) -------------------------------------

/// `POST /admin/stress` — start a stress run against this node's own gRPC
/// listener. Body: `{workers, duration_secs, seed_docs, mix{insert,read,
/// update,delete,find,text,vector,hybrid}}` (all optional).
async fn stress_start<P: PageIo>(
    State(st): State<Arc<AppState<P>>>,
    Json(body): Json<J>,
) -> ApiResult<Json<J>> {
    if let Some(h) = st.stress.lock().unwrap().as_ref()
        && h.is_running()
    {
        return Err(ApiError(Error::InvalidInput(
            "a stress run is already in progress".into(),
        )));
    }
    let cfg = ndb_stress::StressConfig::from_json(&body)
        .map_err(|e| ApiError(Error::InvalidInput(format!("stress config: {e}"))))?;
    let target = st
        .grpc_target
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| ApiError(Error::InvalidInput("gRPC target not configured".into())))?;
    let h = ndb_stress::start(cfg, target);
    let snap = h.snapshot();
    *st.stress.lock().unwrap() = Some(h);
    Ok(Json(snap))
}

/// `GET /admin/stress` — live status + per-op latency histograms.
async fn stress_status<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    match st.stress.lock().unwrap().as_ref() {
        Some(h) => Ok(Json(h.snapshot())),
        None => Ok(Json(json!({ "phase": "idle" }))),
    }
}

/// `POST /admin/stress/stop` — request cancellation; workers drain fast
/// (checked per op) and the phase flips to done.
async fn stress_stop<P: PageIo>(State(st): State<Arc<AppState<P>>>) -> ApiResult<Json<J>> {
    match st.stress.lock().unwrap().as_ref() {
        Some(h) => {
            h.stop();
            Ok(Json(json!({ "stopping": true })))
        }
        None => Err(ApiError(Error::NotFound("no stress run".into()))),
    }
}
