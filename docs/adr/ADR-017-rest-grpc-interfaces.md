# ADR-017: REST and gRPC interfaces

- Status: accepted
- Date: phase 8 (spec §15–16)

## Context

§15 requires a versioned REST surface (databases, collections, documents,
bulk ops, query/aggregate, text/vector/hybrid search, index admin, EXPLAIN,
admin/metrics) and §16 a gRPC equivalent with streaming bulk ingestion.
Both must sit *above* the engine crates — the API layer is a client of
`ndb-index` + `ndb-aggregation`, not a peer — and the management UI must
consume only the public REST API (§15.8), never internals.

## Decision

- **Three thin crates + one binary**: `ndb-rest` (axum router returning
  `Router`, listener-agnostic — tests drive it in-process), `ndb-grpc`
  (tonic + prost, versioned `proto/ndb/v1/*.proto`), `ndb-client` (Rust SDK
  over REST via reqwest), and `ndb-server` which owns bootstrap: open/create
  the `StorageFile` → `PageManager` → `BufferPool` → WAL recovery →
  `Engine`, then `tokio::select!` over REST listener, gRPC server, ctrl-c;
  a background task checkpoints on `checkpoint_interval_seconds`.
- **URL versioning by nest**: everything API-facing lives under `/api/v1`;
  `health`/`metrics`/`cluster` stay outside the nest since they are ops
  surface, not data-plane (their semantics don't version with the API).
- **Errors map at the boundary**: `ndb_common::Error → HTTP status +
  {"error": msg}` (or `tonic::Status`) in one place per crate; inner layers
  never speak HTTP.
- **gRPC streaming is bounded**: `bulk_insert` drains the client stream
  first, then applies all inserts inside *one* write transaction — the
  transaction's mutex guard is not `Send` across `.await`, and one txn
  gives atomic bulk semantics for free.
- **Doc-id allocator is shared**: `Engine` owns per-collection `IdAlloc`s
  (atomic cursor + watermark bound) so per-request `Collection` handles
  can't reset the cursor — without this every REST insert wasted a whole
  1024-id reservation block and two handles could hand out the same id.
- **Auth**: REST middleware checks the role claim when
  `security.authentication_enabled`; disabled → pass-through (dev mode).
  RBAC proper is Phase 10 (spec §17–18).

## Consequences

- UI (`ui/`, Vite + React + TS) is a pure REST consumer; the vite dev
  proxy forwards `/api` → `:8080`. No engine internals are exported.
- gRPC and REST share `ServerState{engine, config}` — one engine, one
  checkpoint loop, consistent semantics on both surfaces.
- Deferred: TLS termination (§16.4), API-key/RBAC enforcement, connection
  pooling config for the SDK, gRPC reflection, streaming query results
  (only bulk ingest streams today).
