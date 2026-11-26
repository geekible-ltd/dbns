# ADR-007 — MVCC and transaction isolation

- **Status**: Accepted
- **Date**: Phase 3
- **Spec references**: §5.7 (MVCC), §7.1 (transactions), §22.3 (crash testing)

## Context

Spec §5.7 requires snapshot reads, repeatable reads, per-version txn id +
commit sequence, and safe version GC. The B+Tree must stay a pure ordered
store — txn semantics belong in `ndb-transaction`.

## Decision

- **Versioned values in leaf cells**: every key maps to a `VersionedValue` —
  a newest-first list of versions. Each version is either
  `Uncommitted { txn_id, data }` or `Committed { commit_seq, data }`, where a
  `None` data payload is a tombstone. At most one uncommitted version exists
  per chain (the active writer's, always at the head).
- **Snapshot isolation**: a reader registers `snapshot_seq = commit_seq` under
  the snapshots lock at `begin`. `resolve` picks the newest committed version
  with `seq <= snapshot_seq`; tombstones resolve to absent. Uncommitted heads
  are visible only to their owner txn.
- **Serialized writers**: one global write mutex. Writers never block readers
  (which take only page latches) — documented Phase 3 simplification.
- **Atomic commit visibility**: `commit` holds the snapshots lock across
  version stamping → catalog page write → WAL `Commit` + fsync →
  `commit_seq.store`. A reader can never register a stale sequence on
  already-GC'd chains, and stamped versions are only resolvable once the
  sequence is published — an acknowledged commit is durable (§5.5).
- **Inline GC at commit**: each stamped chain is garbage-collected at horizon
  `min(active snapshot seqs)`. Rule: keep the newest version `<= horizon`
  *only if it carries data* — a boundary tombstone is itself droppable, and a
  fully dead chain is removed from the tree physically.
- **Logical rollback**: `rollback` removes the txn's uncommitted head from
  each touched chain (physical delete when the chain empties). The edits go
  through ordinary write latches so they are journaled like any mutation.
- **Catalog page**: the first allocated page stores the B+Tree root pointer
  and `commit_seq`; journaled like any page, rewritten at each commit.
- **Txn timeouts** (§5.7): a configurable deadline bounds how long a snapshot
  can pin the GC horizon; expired txns error on next use and rollback on drop.

## Consequences

- Readers never block writers or each other; repeatable reads are free.
- +25–50 bytes per version per key; chains grow until GC — value growth on a
  full leaf exercises the split path on ordinary `insert` (a bug found here:
  replace-in-place on a full leaf removed the cell before discovering the
  replacement didn't fit — fixed by removing the stale cell then falling
  through to the normal insert/split path).
- Long-running snapshots can stall GC for their entire lifetime; txn
  timeouts bound this but a stuck writer still blocks all writers.
- Single-writer throughput is a ceiling until lock-free/finer-grained write
  concurrency lands (spec §28 already defers this).
- `commit` failure mid-stamp is compensated: pre-commit chains (kept in
  memory) are restored, uncommitted heads dropped, and an `Abort` record is
  appended — stamped versions can never surface under a later reuse of the
  commit sequence. Compensation is best-effort; the journaled before-images
  let crash recovery reach the same pre-txn state if a restore itself fails.
