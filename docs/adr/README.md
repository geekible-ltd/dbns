# Architecture Decision Records

Per spec §28, all significant engineering decisions are recorded here.

Copy `template.md` to the next number in sequence. Each ADR records: context,
problem, alternatives considered, decision, consequences, risks, date.

## Index

The spec (§28) pre-allocates this list — add new ADRs beyond ADR-020 as needed.

| ADR | Decision | Status |
|-----|----------|--------|
| ADR-001 | Rust as primary implementation language | accepted |
| ADR-002 | Cargo workspace architecture | accepted |
| ADR-003 | Custom binary document format | accepted |
| ADR-004 | Page-oriented storage | accepted |
| ADR-005 | B+Tree primary storage | accepted |
| ADR-006 | WAL design | accepted |
| ADR-007 | MVCC and transaction isolation | accepted |
| ADR-008 | Raft-based replication | proposed |
| ADR-009 | Virtual shard architecture | proposed |
| ADR-010 | Three replicas per shard | proposed |
| ADR-011 | Inverted index design | accepted |
| ADR-012 | BM25 ranking | accepted |
| ADR-013 | HNSW vector search | proposed |
| ADR-014 | Hybrid search using RRF | proposed |
| ADR-015 | MongoDB-inspired query language | accepted |
| ADR-016 | Near-real-time search visibility | proposed |
| ADR-017 | REST and gRPC interfaces | proposed |
| ADR-018 | Multi-tenancy and RBAC | proposed |
| ADR-019 | Encryption and key management | proposed |
| ADR-020 | Cross-platform storage abstraction | proposed |
| ADR-021 | System catalog, collection metadata and index layout | accepted |
