# ADR-004: Page-oriented storage

- Date: 2026-11-26
- Status: accepted

## Context

The storage engine manages fixed-size pages (spec §5.2): allocation, reads,
writes, checksums, buffer-pool integration and extent-based file growth.

## Problem

A common, versioned unit of I/O is needed beneath the B+Tree, WAL and
overflow storage so that corruption detection, caching and recovery all share
one abstraction.

## Alternatives considered

- Log-structured storage — good write throughput but complicates the
  page-cache model and in-place secondary index updates the spec assumes.
- Raw file offsets per structure — no shared integrity or eviction unit.

## Decision

Fixed-size pages (default 16 KB, configurable power-of-two 4–64 KB; 8/16/32 KB
to be benchmarked). Every page carries a 40-byte versioned header (page ID,
type, format version, generation, CRC32 checksum, flags, LSN, free-space
hint) and is allocated/freed by a `PageManager` over a `PageIo` abstraction.

Page 0 is the meta page storing page size, extent size and the allocation
high-water mark. The allocator reuses freed IDs and bumps the generation on
each reuse so stale references fail (§5.2). Files grow in whole extents
(default 64 pages).

`PageIo` (read_at/write_at/len/set_len/sync) is implemented by
`ndb-storage::StorageFile` using positioned I/O — `pread`/`pwrite` on Unix and
macOS, `seek_read`/`seek_write` on Windows — keeping concurrent readers free
of shared file cursors.

## Consequences

- Checksum-verified reads catch torn writes and media corruption before the
  buffer pool caches anything.
- Generation checks give cheap stale-reference detection for B+Tree page
  links added in Phase 2.
- In-memory free list is a Phase-1 limitation: freed pages are forgotten on
  restart and leak until free-list persistence arrives with B+Tree metadata
  pages. Likewise generation tracking is in-memory only.
- Buffer pool (§5.4) and WAL (§5.5) slot onto the same page abstraction.

## Risks

- Writing the meta page on every fresh-page allocation adds I/O; amortised by
  extent-based growth and revisit when checkpointing lands in Phase 3.
