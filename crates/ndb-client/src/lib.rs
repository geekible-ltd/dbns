//! Rust client SDK for the NDB REST API (spec §15, §16 deliverable).
//!
//! ```ignore
//! let client = Client::new("http://localhost:8080");
//! let users = client.collection("app", "users");
//! let id = users.insert(json!({"name": "ada"})).await?;
//! let hits = users.query(json!({"filter": {"age": {"$gte": 30}}})).await?;
//! ```
//!
//! All payloads are the §15 JSON wire format — `serde_json::Value` in/out.

use serde_json::{Value as J, json};

/// SDK error: HTTP transport, server error body, or protocol mismatch.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("transport: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("api error {status}: {message}")]
    Api { status: u16, message: String },
    #[error("protocol: {0}")]
    Protocol(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// The API client — cheap to clone (one reqwest::Client inside).
#[derive(Clone)]
pub struct Client {
    base: String,
    http: reqwest::Client,
    token: Option<String>,
}

impl Client {
    /// `base` is the server root — e.g. `http://localhost:8080`; `/api/v1`
    /// is appended per-request.
    pub fn new(base: &str) -> Self {
        Self {
            base: base.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
            token: None,
        }
    }

    /// Bearer token for secured deployments (spec §15.7).
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    fn url(&self, path: &str) -> String {
        format!("{}/api/v1{path}", self.base)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Result<J> {
        let req = match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        };
        let resp = req.send().await?;
        let status = resp.status();
        let body: J = resp.json().await.unwrap_or(J::Null);
        if !status.is_success() {
            return Err(ClientError::Api {
                status: status.as_u16(),
                message: body
                    .get("error")
                    .and_then(J::as_str)
                    .unwrap_or("unknown error")
                    .to_string(),
            });
        }
        Ok(body)
    }

    async fn get(&self, path: &str) -> Result<J> {
        self.send(self.http.get(self.url(path))).await
    }
    async fn post(&self, path: &str, body: &J) -> Result<J> {
        self.send(self.http.post(self.url(path)).json(body)).await
    }
    async fn put(&self, path: &str, body: &J) -> Result<J> {
        self.send(self.http.put(self.url(path)).json(body)).await
    }
    async fn patch(&self, path: &str, body: &J) -> Result<J> {
        self.send(self.http.patch(self.url(path)).json(body)).await
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.send(self.http.delete(self.url(path))).await?;
        Ok(())
    }

    // ---- databases (§15.1) ------------------------------------------------

    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let j = self.get("/databases").await?;
        Ok(j["databases"]
            .as_array()
            .map(|a| a.iter().filter_map(J::as_str).map(str::to_string).collect())
            .unwrap_or_default())
    }

    pub async fn create_database(&self, name: &str) -> Result<()> {
        self.post("/databases", &json!({"name": name})).await?;
        Ok(())
    }

    pub async fn drop_database(&self, name: &str) -> Result<()> {
        self.delete(&format!("/databases/{name}")).await
    }

    // ---- collections (§15.2) ------------------------------------------------

    pub async fn list_collections(&self, db: &str) -> Result<Vec<String>> {
        let j = self.get(&format!("/databases/{db}/collections")).await?;
        Ok(j["collections"]
            .as_array()
            .map(|a| a.iter().filter_map(J::as_str).map(str::to_string).collect())
            .unwrap_or_default())
    }

    /// `schema` is an optional §6 JSON schema object.
    pub async fn create_collection(&self, db: &str, name: &str, schema: Option<J>) -> Result<()> {
        let mut b = json!({"name": name});
        if let Some(s) = schema {
            b["schema"] = s;
        }
        self.post(&format!("/databases/{db}/collections"), &b)
            .await?;
        Ok(())
    }

    pub async fn collection_details(&self, db: &str, name: &str) -> Result<J> {
        self.get(&format!("/collections/{db}/{name}")).await
    }

    pub async fn drop_collection(&self, db: &str, name: &str) -> Result<()> {
        self.delete(&format!("/collections/{db}/{name}")).await
    }

    /// Collection-scoped handle (§15.3–§15.5).
    pub fn collection(&self, db: &str, name: &str) -> Collection<'_> {
        Collection {
            client: self,
            db: db.to_string(),
            name: name.to_string(),
        }
    }

    // ---- admin (§15.7) ------------------------------------------------------

    pub async fn metrics(&self) -> Result<J> {
        self.get("/admin/metrics").await
    }
    pub async fn storage(&self) -> Result<J> {
        self.get("/admin/storage").await
    }
    pub async fn txn_status(&self) -> Result<J> {
        self.get("/admin/txn").await
    }
    pub async fn info(&self) -> Result<J> {
        self.get("/admin/info").await
    }
    pub async fn cluster_status(&self) -> Result<J> {
        self.get("/cluster/status").await
    }
}

/// A handle bound to one collection — methods mirror §15.3–§15.5.
pub struct Collection<'c> {
    client: &'c Client,
    db: String,
    name: String,
}

impl Collection<'_> {
    fn path(&self, suffix: &str) -> String {
        format!("/collections/{}/{}{}", self.db, self.name, suffix)
    }

    /// Inserts a document; returns its `_id`.
    pub async fn insert(&self, doc: J) -> Result<u64> {
        let j = self.client.post(&self.path("/documents"), &doc).await?;
        j["_id"]
            .as_u64()
            .ok_or_else(|| ClientError::Protocol("insert response missing _id".into()))
    }

    pub async fn get(&self, id: u64) -> Result<J> {
        self.client
            .get(&self.path(&format!("/documents/{id}")))
            .await
    }

    /// Full replace.
    pub async fn replace(&self, id: u64, doc: J) -> Result<()> {
        self.client
            .put(&self.path(&format!("/documents/{id}")), &doc)
            .await?;
        Ok(())
    }

    /// Field-merge update (PATCH).
    pub async fn patch(&self, id: u64, fields: J) -> Result<()> {
        self.client
            .patch(&self.path(&format!("/documents/{id}")), &fields)
            .await?;
        Ok(())
    }

    pub async fn delete(&self, id: u64) -> Result<()> {
        self.client
            .delete(&self.path(&format!("/documents/{id}")))
            .await
    }

    /// `ops` = `[{"insert":{…}}, {"update":{"id","fields"}}, {"delete":{"id"}}]`.
    pub async fn bulk(&self, ops: J) -> Result<u64> {
        let j = self
            .client
            .post(&self.path("/documents/bulk"), &ops)
            .await?;
        Ok(j["ok"].as_u64().unwrap_or(0))
    }

    /// Find query — body is the parser's find JSON without
    /// `database`/`collection` (injected server-side).
    pub async fn query(&self, body: J) -> Result<Vec<J>> {
        let j = self.client.post(&self.path("/query"), &body).await?;
        Ok(j["docs"].as_array().cloned().unwrap_or_default())
    }

    /// EXPLAIN — returns `{access, explain, est_scanned}`.
    pub async fn explain(&self, body: J) -> Result<J> {
        self.client.post(&self.path("/explain"), &body).await
    }

    /// Full-text search — `{index?, query, topK?, kind?, field?, slop?, …}`.
    pub async fn search_text(&self, body: J) -> Result<Vec<J>> {
        let j = self.client.post(&self.path("/search"), &body).await?;
        Ok(j["hits"].as_array().cloned().unwrap_or_default())
    }

    /// Vector k-NN — `{index?, vector, k?, ef?}`.
    pub async fn search_vector(&self, body: J) -> Result<Vec<J>> {
        let j = self
            .client
            .post(&self.path("/vector-search"), &body)
            .await?;
        Ok(j["hits"].as_array().cloned().unwrap_or_default())
    }

    /// Hybrid RRF search — `{text?, vector?, candidateIds?, limit, fetchK,
    /// fusion?}`.
    pub async fn search_hybrid(&self, body: J) -> Result<Vec<J>> {
        let j = self
            .client
            .post(&self.path("/hybrid-search"), &body)
            .await?;
        Ok(j["hits"].as_array().cloned().unwrap_or_default())
    }

    /// Aggregation pipeline — `[{"$match":…}, …]`.
    pub async fn aggregate(&self, pipeline: J) -> Result<Vec<J>> {
        let j = self
            .client
            .post(&self.path("/aggregate"), &json!({"pipeline": pipeline}))
            .await?;
        Ok(j["docs"].as_array().cloned().unwrap_or_default())
    }

    // ---- indexes (§15.5) ------------------------------------------------------

    pub async fn list_indexes(&self) -> Result<Vec<J>> {
        let j = self.client.get(&self.path("/indexes")).await?;
        Ok(j["indexes"].as_array().cloned().unwrap_or_default())
    }

    /// `def` = `{name, kind: secondary|text|vector, fields|field, …}`.
    pub async fn create_index(&self, def: J) -> Result<()> {
        self.client.post(&self.path("/indexes"), &def).await?;
        Ok(())
    }

    pub async fn drop_index(&self, name: &str) -> Result<()> {
        self.client
            .delete(&self.path(&format!("/indexes/{name}")))
            .await
    }

    pub async fn index_status(&self, name: &str) -> Result<J> {
        self.client
            .get(&self.path(&format!("/indexes/{name}")))
            .await
    }

    pub async fn rebuild_index(&self, name: &str) -> Result<()> {
        self.client
            .post(&self.path(&format!("/indexes/{name}/rebuild")), &json!({}))
            .await?;
        Ok(())
    }

    /// Document listing helper — `{"filter": {}, "limit": n}`.
    pub async fn list_documents(&self, limit: u64) -> Result<Vec<J>> {
        self.query(json!({"filter": {}, "limit": limit})).await
    }
}
