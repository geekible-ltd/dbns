# AGENTS.md — NDB

NDB is a distributed NoSQL search database written in Rust. It combines document
storage, transactional capabilities, and full-text/fuzzy/vector/hybrid search in
a single platform.

**Authoritative spec:** `dbns_spec.md` (30 sections) — read it before making
architectural decisions. This file is a working summary, not a replacement.

## Current state

Phase 3 in progress (see `docs/progress.md`):
- `ndb-wal` — segmented append-only log, len+CRC32 framing, torn-tail
  truncation, rotation + retirement, recovery scanner. ADR-006.
- `ndb-buffer` — plus page-journaling hook + WAL flush gate (ADR-006).
- `ndb-transaction` — `TxnManager`/`Transaction` (serialized writers), MVCC
  versioned leaf values, snapshot isolation, inline GC, catalog page,
  `recover()` (undo uncommitted, redo committed). ADR-007.
- `ndb-buffer` — fixed-frame `BufferPool`, clock eviction, dirty write-back,
  RAII read/write pin guards (`RwLock<Page>` per frame), `try_fetch_write`.
- `ndb-btree` — slotted-page nodes, pessimistic latch-coupled `BTree`
  (get/insert/delete/scan/range/seek), leaf-chain `Cursor`, composite `key`
  encoding, `validate()` invariants. ADR-005.
- `ndb-document` — binary codec (header/directory/payload/CRC32), partial
  field access, `FieldDictionary`, `Schema`. Wire format: `docs/adr/ADR-003`.
- `ndb-page` — versioned pages, CRC32, extent-based `PageManager` over the
  `PageIo` trait. ADR-004.
- `ndb-storage` — `StorageFile` positioned I/O (pread/pwrite on Unix+macOS,
  seek_read/write on Windows) implementing `PageIo`.
- Phase 0 complete: workspace, `ndb-common`, `ndb-config`, `ndb-server`
  lifecycle, ADRs, CI (Ubuntu + Windows + macOS).

## Progress tracking — REQUIRED

We must keep track of completed work from `dbns_spec.md`. The tracker lives
in `docs/progress.md`, organised by roadmap phase and spec section.

- **Update `docs/progress.md` whenever you complete work** — mark items `[x]`,
  or `[~]` with a note for partial work.
- Check it before starting a task so you don't redo or contradict finished
  work.
- When you write an ADR, record it in both `docs/adr/README.md` and
  `docs/progress.md`.

## Project goals

- Custom binary document format + page-oriented B+Tree storage engine
- Custom search: inverted index, BM25, fuzzy, HNSW vectors, hybrid (RRF)
- MongoDB-inspired JSON query language over REST (Axum) and gRPC (Tonic)
- 3-node Raft cluster initially; architecture must scale to 100+ nodes
- Targets: PK lookup P95 <5ms, search P95 <50ms, index visibility <5s,
  failover <30s, zero acknowledged-commit loss
- Cross-platform: production targets are Ubuntu Linux and Windows Server;
  macOS is the development platform — every change must build and test on
  all three

## Non-negotiable architecture rules (spec §3.1, §19.2, §30)

1. **Storage and search are independent subsystems.** Document storage is
   authoritative; all search indexes are derived and rebuildable.
2. All persisted formats must be versioned.
3. The storage engine must not depend on network interfaces.
4. Query execution must be independent of physical storage implementation.
5. Memory must be configurable and bounded — no unbounded growth anywhere.
6. All background operations support backpressure.
7. Dependency rules: `common` → no engine deps; `document` → no networking;
   `storage` → no search; `btree` → no query engine; search accesses documents
   through defined interfaces only; REST/gRPC depend on service interfaces,
   not physical storage crates. **No circular dependencies.**
8. No external database, search engine, or vector DB as a required component.
   Self-contained except optional embedding providers / KMS / monitoring.
9. Async only where genuinely needed — internal storage ops may be sync on
   blocking contexts. Do not put async in low-level page operations.

## Workspace layout

Cargo workspace with crates under `crates/ndb-*` (full list: spec §19).
Plus top-level `proto/`, `tests/`, `benchmarks/`, `examples/`, `docs/`, `tools/`.

**Do NOT scaffold all ~40 spec crates at once** (spec §29). The first iteration
contains only:

```
ndb-common      — shared IDs, error types, result types, common traits
ndb-config      — versioned YAML config, validated before server start (§25)
ndb-document    — binary document encode/decode, field dictionaries, validation
ndb-page        — page structures, allocation, checksums
ndb-storage     — physical file management, persistent storage
ndb-buffer      — buffer pool, memory management
ndb-btree       — primary/secondary B+Tree
ndb-wal         — write-ahead logging, recovery records
ndb-transaction — transaction lifecycle, commit coordination
ndb-server      — application composition, server lifecycle
```

## Development order (spec §26, §29, §30)

1. **Phase 0** — workspace, error handling, config framework, logging, metrics,
   CI, cross-platform builds
2. **Phase 1** — binary document format, field dictionary, page manager
3. **Phase 2** — B+Tree: split/merge, cursors, range scans, buffer pool,
   latch-coupled concurrency (no lock-free until proven & benchmarked)
4. **Phase 3** — WAL, transaction manager, MVCC, checkpointing, crash recovery
5. **Phase 4** — database/collection managers, schema validation, CRUD, indexes
6. Then: full-text search (5), query engine (6), vector/hybrid (7), APIs (8),
   distribution (9), security (10), production hardening (11)

First demonstrable product: single-node DB that can create databases and
collections, insert/retrieve/update/delete binary documents, create secondary
indexes, do indexed lookups, and **recover from forced termination**.

## Tech stack (spec §21)

| Area | Choice |
|---|---|
| Async runtime | Tokio |
| REST | Axum |
| gRPC | Tonic + Protocol Buffers |
| Serialization | Serde |
| Consensus | OpenRaft (integration only — we own state machine, log persistence, snapshots) |
| Compression | Zstandard / LZ4 |
| Crypto | RustCrypto (AES-256-GCM or ChaCha20-Poly1305, TLS 1.3) |
| Metrics | Prometheus-compatible endpoint |
| Tracing | `tracing` |
| Property tests | Proptest |
| Benchmarks | Criterion |

Before adding any dependency: check maintenance status, licence, Windows +
Linux + macOS compatibility, security history, performance. Prefer versions
≥7 days old; no floating `latest`/`*` ranges.

## Key design parameters (spec defaults — benchmark before finalizing)

- Page size: 16 KB (benchmark 8/16/32 KB)
- Replication factor: 3 (leader + 2 followers, majority = 2)
- Virtual shards: 256 per large collection (starting point, tune later)
- BM25: k1 = 1.2, b = 0.75 (configurable)
- Indexing visibility target: ≤5 s after commit; indexing pipeline must be
  durable, ordered per document, idempotent, recoverable, backpressure-aware
- Isolation: snapshot isolation initially
- Fuzzy: configurable edit-distance and expansion limits — never unbounded
- Every query: max execution time, max memory, max result size, cancellation

## Definition of done (spec §27)

Every feature includes: implementation, unit tests, integration tests where
applicable, error handling, logging, metrics, docs, perf tests where
applicable, security review, cross-platform validation. Storage/transaction
changes need recovery tests; distributed changes need failure tests; query
changes need correctness + resource-limit tests.

Required test coverage (spec §22): unit tests per crate; Proptest for B+Tree
invariants, document encoding, WAL recovery, MVCC visibility; crash testing at
every persistence boundary; distributed fault injection later.

## ADRs

Maintain Architecture Decision Records for significant decisions (spec §28).
ADR-001 through ADR-020 are pre-listed in the spec (Rust, workspace, binary
format, pages, B+Tree, WAL, MVCC, Raft, sharding, RF=3, inverted index, BM25,
HNSW, RRF, query language, NRT search, REST/gRPC, RBAC, encryption,
cross-platform storage). Each ADR records: context, problem, alternatives,
decision, consequences, risks, date.

## Working conventions

- British-English technical terms in docs where the spec uses them
  (e.g. "optimiser", "normalisation") to match spec vocabulary; code identifiers
  follow Rust conventions (`snake_case`, no abbreviated public names)
- Config: versioned YAML file; validate fully before server start — clear
  errors, never silently apply defaults
- Config example in spec §25 (node/cluster/storage/transactions/indexing/
  search/vector/security/logging/monitoring sections)
- Document and index schema evolution must never silently reinterpret existing
  field identifiers or mix incompatible analysers/vector spaces
