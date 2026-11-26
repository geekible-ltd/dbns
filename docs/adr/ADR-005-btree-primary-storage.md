# ADR-005 — B+Tree as the primary ordered index structure

- **Status**: Accepted
- **Date**: (implementation date)
- **Spec references**: §5.3 (B+Tree storage), §5.4 (buffer pool), §26 Phase 2

## Context

NDB needs an ordered on-disk index for primary keys, secondary indexes and
collection data. Spec §5.3 mandates a B+Tree and explicitly forbids lock-free
structures until a pessimistic implementation is proven correct and
benchmarked.

## Decision

- **Slotted-page nodes** inside the 16 KiB page bodies from ADR-004: a
  16-byte node header (level, cell count, rightmost child, hi-key bound,
  leaf sibling links) + a slot array growing down + cells growing up.
- **Pessimistic latch-coupling (crabbing)** on the buffer pool's page
  latches. Writers release ancestors only when the child is provably safe
  (insert: fits without splitting; delete: cannot underflow). Readers
  read-crab (latch child, release parent).
- **Underflow policy**: merge on empty rather than at 50% occupancy. An
  empty leaf absorbs its right sibling (always fits — the right page's data
  came from one page); a rightmost leaf is unlinked; an empty internal node
  is splced into its parent. This is simpler than redistribution and keeps
  occupancy acceptable for a document store.
- **Sibling latches** (needed only when unlinking the rightmost leaf) are
  taken with `try_write`; failure drops all latches and retries, so no
  lock-ordering cycle exists.
- **Stale-root defence**: the root page id lives in an `AtomicU64`; callers
  re-verify the id *after* latching, since a root can only be replaced while
  it is write-latched.
- **Deferred frees**: a merge that unlinks a page a cursor still pins queues
  the free for a later operation instead of failing.
- **Order-preserving composite keys**: `ndb-btree::key` encodes Int/UInt/
  Str/Bytes tuples so byte-lexicographic order equals tuple order.
- **Values are inline** in leaf cells; oversized values are rejected for now
  (overflow page type is reserved for a later phase).

## Consequences

- Concurrent readers and writers make progress with standard textbook
  invariants; the `validate()` method (sortedness, separator bounds, uniform
  leaf depth, ordered leaf chain) is exercised by proptest + threaded tests.
- Writer throughput serializes through held ancestor latches under
  contention — acceptable per spec until benchmarks justify lock-free work.
- Deferred frees can transiently retain pages; they drain on subsequent ops.
- Empty-leftmost-leaf after a root collapse is legal; root leaf may be empty.

## Follow-ups

- Persist the page free list + generations on metadata pages (Phase 2→3 WAL).
- Overflow pages for large values; hi-key bound enforcement during splits.
- Benchmarks before any lock-free reconsideration (spec requirement).
