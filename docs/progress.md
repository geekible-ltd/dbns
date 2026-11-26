# NDB Implementation Progress

Tracks completed work against `dbns_spec.md`. **Update this file whenever work
is completed** — mark partial work explicitly with `[~]` and a note.

Legend: `[x]` done · `[~]` partial · `[ ]` not started

## Roadmap phases (spec §26)

### Phase 0 — Architecture and foundations
- [x] Cargo workspace with initial crate set (spec §19, §29)
- [x] Shared error/result types (`ndb-common`)
- [x] Typed identifiers and `FormatVersion` primitives (`ndb-common`)
- [x] Configuration framework — versioned YAML, full validation before start,
      unknown fields rejected (spec §25, `ndb-config`, `ndb.example.yaml`)
- [x] Logging — `tracing` with structured/JSON output (spec §24)
- [x] Metrics — Prometheus scrape endpoint via `metrics-exporter-prometheus`
      (spec §24; per-subsystem metrics defined but not yet emitted)
- [x] CI pipeline — fmt, clippy, tests, release build on Ubuntu + Windows +
      macOS (spec §26 P0 acceptance)
- [x] ADR framework — `docs/adr/` template + index; ADR-001, ADR-002 written
      (spec §28)
- [ ] Final architecture documentation beyond spec
- [ ] Health endpoint (deferred — arrives with REST service, Phase 8)

### Phase 1 — Binary document storage
- [x] Binary document format encode/decode (spec §6 — `ndb-document::codec`:
      versioned header, field directory, payload, CRC32, partial access via
      `EncodedDocument`)
- [x] Field dictionary (spec §6.4 — `ndb-document::FieldDictionary`: stable
      append-only IDs, versioned persistence, no silent type changes)
- [x] Schema definitions + validation (spec §6 — `ndb-document::Schema`,
      flexible/strict modes)
- [x] Page manager: versioned pages, 40-byte header, CRC32 checksums,
      generation tracking (spec §5.2 — `ndb-page::PageManager`)
- [x] File manager / extent-based allocation (spec §5.2 — `ndb-page` extent
      growth over `ndb-storage::StorageFile` positioned I/O)
- [~] Known Phase-1 limitations: free list + page generations are in-memory
      only (freed pages leak on restart until Phase 2 meta management)

### Phase 2 — B+Tree engine
- [x] B+Tree: internal/leaf nodes, split, merge (spec §5.3 —
      `ndb-btree::node` slotted pages, `tree` split/merge; merge-on-empty
      underflow policy, see ADR-005)
- [x] Cursors, ordered iterators, range scans, composite keys (spec §5.3 —
      `ndb-btree::cursor` latch-coupled leaf-chain iterator, `key`
      order-preserving composite encoding, `tree::range`/`scan`/`seek`)
- [x] Buffer pool with bounded memory + eviction (spec §5.4 —
      `ndb-buffer::BufferPool`: fixed frames, clock eviction, dirty
      write-back on evict, RAII pin guards, `try_fetch_write` for sibling
      latches)
- [x] Concurrent access via latch-coupling (spec §5.3 — pessimistic crabbing
      with safe-child release, stale-root re-check, deferred page frees;
      verified by threaded tests + `BTree::validate`)
- [x] Acceptance: millions of operations (`--ignored` release test:
      1M inserts+lookups with periodic validation), proptest oracle vs
      `BTreeMap` (spec §22.2)
- [~] Known Phase-2 limitations: oversized values rejected (no overflow
      pages yet); free list still in-memory only; no redistribution at
      partial underflow (merge-on-empty only)

### Phase 3 — Transactions and recovery
- [ ] WAL: append-only, checksums, LSN, rotation (spec §5.5)
- [ ] Transaction manager: begin/commit/rollback (spec §7.1)
- [ ] MVCC + snapshot isolation (spec §5.7)
- [ ] Checkpointing + WAL retirement (spec §5.6)
- [ ] Crash recovery — forced-termination tests (spec §22.3)

### Phase 4 — Database functionality
- [ ] Database/collection managers (spec §2.1)
- [ ] CRUD + bulk insertion (spec §7.2)
- [ ] Primary index (spec §8.2)
- [ ] Secondary indexes (spec §8.3)
- [ ] Schema validation (spec §6)

### Phases 5–11 (not started)
- [ ] Phase 5 — full-text search: tokenizer, analyser, inverted index, BM25,
      phrase, fuzzy, segments (spec §9)
- [ ] Phase 6 — query engine: parser, AST, planner, optimiser, aggregation,
      EXPLAIN (spec §12–13)
- [ ] Phase 7 — vector + hybrid: vector storage, HNSW, RRF (spec §10–11)
- [ ] Phase 8 — REST + gRPC + Rust SDK + management web UI (spec §15–16;
      UI: React/TypeScript SPA in `ui/`, separate frontend app, consumes the
      public REST API incl. §15.7 admin endpoints; RBAC integration per §15.8)
- [ ] Phase 9 — distribution: Raft, sharding, failover, distributed query
      (spec §4, §14)
- [ ] Phase 10 — security: multi-tenancy, RBAC, TLS, encryption, audit
      (spec §17)
- [ ] Phase 11 — hardening: rebalancing, backup automation, Ubuntu and
      Windows production validation, large-dataset testing (spec §18, §23)

## ADRs written (spec §28)
- [x] ADR-001 Rust language
- [x] ADR-002 Cargo workspace
- [x] ADR-003 Binary document format
- [x] ADR-004 Page-oriented storage
- [x] ADR-005 B+Tree primary storage
- [ ] ADR-006 through ADR-020 — write when each subsystem's design is finalised
