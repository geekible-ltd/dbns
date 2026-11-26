//! Phase-8 REST integration tests (spec §26): exercise the versioned API
//! end-to-end through the axum router — no socket needed (`oneshot`).

use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use http::{Request, StatusCode};
use ndb_buffer::BufferPool;
use ndb_index::Engine;
use ndb_page::manager::{PageIo, PageManager};
use ndb_rest::AppState;
use ndb_transaction::TxnConfig;
use ndb_wal::Wal;
use serde_json::{Value as J, json};
use tempfile::TempDir;
use tower::ServiceExt;

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

struct Env {
    app: Router,
    _tmp: TempDir,
}

fn env() -> Env {
    let tmp = TempDir::new().unwrap();
    let io = MemIo::default();
    let pm = PageManager::create(io, 4096, 8).unwrap();
    let pool = Arc::new(BufferPool::new(pm, 128 * 4096));
    let (wal, _) = Wal::open(&tmp.path().join("wal"), 0).unwrap();
    let engine = Arc::new(Engine::create(pool, Arc::new(wal), TxnConfig::default()).unwrap());
    let st = Arc::new(AppState::new(engine));
    Env {
        app: ndb_rest::router(st),
        _tmp: tmp,
    }
}

async fn call(app: &Router, method: &str, path: &str, body: Option<J>) -> (StatusCode, J) {
    let b = match &body {
        Some(j) => Body::from(serde_json::to_vec(j).unwrap()),
        None => Body::empty(),
    };
    let req = Request::builder()
        .method(method)
        .uri(format!("/api/v1{path}"))
        .header("content-type", "application/json")
        .body(b)
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 22)
        .await
        .unwrap();
    let j: J = serde_json::from_slice(&bytes).unwrap_or(J::Null);
    (status, j)
}

async fn setup_db_coll(app: &Router) {
    let (s, _) = call(app, "POST", "/databases", Some(json!({"name": "app"}))).await;
    assert_eq!(s, StatusCode::CREATED);
    let (s, _) = call(
        app,
        "POST",
        "/databases/app/collections",
        Some(json!({"name": "users"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
}

#[tokio::test]
async fn database_collection_document_flow() {
    let e = env();
    setup_db_coll(&e.app).await;

    // list
    let (s, j) = call(&e.app, "GET", "/databases", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["databases"], json!(["app"]));

    // insert → get
    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/documents",
        Some(json!({"name": "ada", "age": 36})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    let id = j["_id"].as_u64().unwrap();

    let (s, j) = call(
        &e.app,
        "GET",
        &format!("/collections/app/users/documents/{id}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["name"], "ada");

    // patch merge
    let (s, _) = call(
        &e.app,
        "PATCH",
        &format!("/collections/app/users/documents/{id}"),
        Some(json!({"city": "london"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    let (_, j) = call(
        &e.app,
        "GET",
        &format!("/collections/app/users/documents/{id}"),
        None,
    )
    .await;
    assert_eq!(j["city"], "london");
    assert_eq!(j["name"], "ada"); // patch didn't clobber

    // delete
    let (s, _) = call(
        &e.app,
        "DELETE",
        &format!("/collections/app/users/documents/{id}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let (s, _) = call(
        &e.app,
        "GET",
        &format!("/collections/app/users/documents/{id}"),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn bulk_ops_and_query() {
    let e = env();
    setup_db_coll(&e.app).await;

    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/documents/bulk",
        Some(json!({"ops": [
            {"insert": {"name": "a", "n": 1}},
            {"insert": {"name": "b", "n": 2}},
            {"insert": {"name": "c", "n": 3}},
        ]})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["ok"], 3);

    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/query",
        Some(json!({"filter": {"n": {"$gte": 2}}, "sort": {"n": -1}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["docs"].as_array().unwrap().len(), 2);
    assert_eq!(j["docs"][0]["name"], "c");
}

#[tokio::test]
async fn index_admin_and_explain() {
    let e = env();
    setup_db_coll(&e.app).await;

    let (s, _) = call(
        &e.app,
        "POST",
        "/collections/app/users/indexes",
        Some(json!({"name": "by_n", "kind": "secondary", "fields": ["n"]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);

    let (s, j) = call(&e.app, "GET", "/collections/app/users/indexes", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["indexes"][0]["name"], "by_n");

    let (s, j) = call(&e.app, "GET", "/collections/app/users/indexes/by_n", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["kind"], "secondary");

    call(
        &e.app,
        "POST",
        "/collections/app/users/documents",
        Some(json!({"n": 5})),
    )
    .await;
    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/explain",
        Some(json!({"filter": {"n": 5}})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(j["access"].as_str().unwrap().contains("IndexScan"));

    let (s, _) = call(
        &e.app,
        "DELETE",
        "/collections/app/users/indexes/by_n",
        None,
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn search_endpoints() {
    let e = env();
    setup_db_coll(&e.app).await;

    // text index + docs
    let (s, _) = call(
        &e.app,
        "POST",
        "/collections/app/users/indexes",
        Some(json!({"name": "t", "kind": "text", "fields": [{"field": "body"}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    for body in ["rust is fast", "rust is safe", "python is flexible"] {
        call(
            &e.app,
            "POST",
            "/collections/app/users/documents",
            Some(json!({"body": body})),
        )
        .await;
    }
    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/search",
        Some(json!({"index": "t", "query": "rust", "topK": 5})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["hits"].as_array().unwrap().len(), 2);

    // vector index + docs
    let (s, _) = call(
        &e.app,
        "POST",
        "/collections/app/users/indexes",
        Some(json!({"name": "v", "kind": "vector", "field": "emb", "dims": 2, "metric": "cosine"})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED);
    for (name, v) in [("x", [1.0f32, 0.0]), ("y", [0.9, 0.1]), ("z", [0.0, 1.0])] {
        call(
            &e.app,
            "POST",
            "/collections/app/users/documents",
            Some(json!({"name": name, "emb": {"$vector": v}})),
        )
        .await;
    }
    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/vector-search",
        Some(json!({"index": "v", "vector": [1.0, 0.0], "k": 2})),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["hits"].as_array().unwrap().len(), 2);

    // hybrid
    let (s, j) = call(
        &e.app,
        "POST",
        "/collections/app/users/hybrid-search",
        Some(json!({
            "text": {"index": "t", "query": "rust"},
            "vector": {"index": "v", "vector": [1.0, 0.0], "ef": 16},
            "limit": 4,
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert!(!j["hits"].as_array().unwrap().is_empty());
    assert!(j["hits"][0]["legs"].is_array());
}

#[tokio::test]
async fn admin_and_cluster_endpoints() {
    let e = env();
    setup_db_coll(&e.app).await;

    for path in ["/health", "/ready"] {
        let (s, _) = call(&e.app, "GET", path, None).await;
        assert_eq!(s, StatusCode::OK);
    }
    for path in [
        "/admin/metrics",
        "/admin/config",
        "/admin/storage",
        "/admin/txn",
        "/admin/info",
    ] {
        let (s, _) = call(&e.app, "GET", path, None).await;
        assert_eq!(s, StatusCode::OK, "{path}");
    }
    let (_s, j) = call(&e.app, "GET", "/admin/metrics", None).await;
    assert!(j["engine"]["commit_seq"].is_number());
    assert!(j["buffer_pool"]["frames"].is_number());

    let (s, j) = call(&e.app, "GET", "/cluster/status", None).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(j["nodes"], 1);
}

#[tokio::test]
async fn error_mapping() {
    let e = env();
    let (s, j) = call(&e.app, "GET", "/collections/no/db/documents/1", None).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(j["error"].is_string());

    let (s, _) = call(&e.app, "POST", "/databases", Some(json!({}))).await;
    assert!(s.is_client_error());
}
