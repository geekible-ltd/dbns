# ADR-006 — Write-ahead log: physiological page journaling

- **Status**: Accepted
- **Date**: Phase 3
- **Spec references**: §5.5 (WAL), §5.6 (checkpointing), §22.3 (crash testing)

## Context

Pages are modified through the buffer pool and flushed lazily (steal/no-force
policy). Durability requires every committed change to be reconstructable
after a crash, and uncommitted changes to be removable.

## Decision

- **Physiological records**: each record carries `(txn_id, lsn, page_id,
  before_image, after_image)` — full page bytes. Every write latch released
  through the buffer pool invokes a journaling hook that captures the
  before-image at fetch and appends on guard drop when the bytes changed.
- **Commit/Abort/Checkpoint markers** in the same log stream.
- **WAL-before-data**: the buffer pool calls a *flush gate* (flush WAL up to
  the page's `lsn`) before writing any dirty page to the data file.
- **Segmented append-only files** `wal/{seq}.ndbwal` with per-record
  `len + crc32` framing — torn tails are detected and truncated on scan.
- **Recovery**: undo first (first before-image per page of uncommitted and
  aborted txns), then redo (last after-image per page of committed txns and
  system txn 0). Idempotent because images are exact page states.
- **Checkpointing**: `Checkpoint` record → `pool.flush_all()` → data-file
  fsync → WAL flush → retire WAL segments below the checkpoint LSN.

## Consequences

- +16–32 KB of WAL per modified page — space-heavy but simple and correct;
  delta encoding is a later optimisation.
- Live rollback restores before-images through ordinary write latches —
  compensation records are themselves journaled (ARIES-style), so a crash
  mid-rollback still recovers correctly.
- All logging flows through the pool hook — the B+Tree needs no txn logic.
