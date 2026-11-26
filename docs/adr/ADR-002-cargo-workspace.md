# ADR-002: Cargo workspace architecture

- Date: 2026-11-26
- Status: accepted

## Context

The spec (§19) defines ~40 crates under `crates/ndb-*` organised by subsystem:
document, storage, search, query, distribution, security and interfaces, with
a deliberately controlled dependency graph (§19.2).

## Problem

Subsystem isolation, independent testability and enforceable dependency rules
require a multi-crate layout rather than a monolithic package.

## Alternatives considered

- Single crate with modules — simpler but cannot enforce dependency
  boundaries at compile time and couples all compile times together.
- Separate repositories — enforces boundaries but adds versioning and release
  overhead disproportionate to the current stage.

## Decision

One Cargo workspace with a virtual root manifest. Crates live under
`crates/`, shared dependencies are pinned in `[workspace.dependencies]`.

Crates are introduced incrementally per the roadmap (spec §29): the initial
iteration contains `ndb-common`, `ndb-config`, `ndb-document`, `ndb-page`,
`ndb-storage`, `ndb-buffer`, `ndb-btree`, `ndb-wal`, `ndb-transaction` and
`ndb-server`. The remaining spec crates are added only when their phase
begins.

## Consequences

- Dependency rules (§19.2) are enforced by the compiler.
- Each subsystem can be benchmarked and tested in isolation (spec §30).
- Workspace-level shared dependency versions keep builds reproducible.

## Risks

- Crate proliferation increases manifest maintenance; mitigated by adding
  crates only as phases begin.
- Circular dependencies are prohibited and enforced by Cargo itself.
