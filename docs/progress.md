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
- [x] CI pipeline — fmt, clippy, tests, release build on Ubuntu + Windows
      (spec §26 P0 acceptance)
- [x] ADR framework — `docs/adr/` template + index; ADR-001, ADR-002 written
      (spec §28)
- [ ] Final architecture documentation beyond spec
- [ ] Health endpoint (deferred — arrives with REST service, Phase 8)

### Phase 1 — Binary document storage
- [ ] Binary document format encode/decode (spec §6)
- [ ] Field dictionary (spec §6.4)
- [ ] Schema definitions (spec §6)
- [ ] Page manager: 16 KB versioned pages, header, checksums (spec §5.2)
- [ ] File manager / extent-based allocation (spec §5.2)

### Phase 2 — B+Tree engine
- [ ] B+Tree: internal/leaf nodes, split, merge, redistribution (spec §5.3)
- [ ] Cursors, ordered iterators, range scans, composite keys (spec §5.3)
- [ ] Buffer pool with bounded memory + eviction (spec §5.4)
- [ ] Concurrent access via latch-coupling (spec §5.3)

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
- [ ] Phase 8 — REST + gRPC + Rust SDK (spec §15–16)
- [ ] Phase 9 — distribution: Raft, sharding, failover, distributed query
      (spec §4, §14)
- [ ] Phase 10 — security: multi-tenancy, RBAC, TLS, encryption, audit
      (spec §17)
- [ ] Phase 11 — hardening: rebalancing, backup automation, Windows
      validation, large-dataset testing (spec §11? §18, §23)

## ADRs written (spec §28)
- [x] ADR-001 Rust language
- [x] ADR-002 Cargo workspace
- [ ] ADR-003 through ADR-020 — write when each subsystem's design is finalised
