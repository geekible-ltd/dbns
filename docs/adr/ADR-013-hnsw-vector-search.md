# ADR-013: HNSW vector search

- Status: accepted
- Date: phase 7 (spec §10)

## Context

Vector search needs approximate nearest-neighbour retrieval that (a) lives
inside our storage engine — journaled, MVCC-visible, rollback-safe — and
(b) supports insert/update/delete plus metric and dimension configuration
per index. Building the graph inside the collection's B+Tree keeps one
concurrency and recovery story for every index type.

## Decision

- **One B+Tree per vector index** (same pattern as the inverted index).
  Keys: `VM` meta (dims, metric, M, ef params, node count, entry point,
  RNG state), `VD{doc_id}` → encoded vector, `NG{level}{doc_id}` →
  neighbour lists, `DX{doc_id}` → tombstone.
- **Standard two-layer HNSW** (Malkov & Yashunin): level via
  `floor(-ln(u)·mL)` from a persisted xorshift RNG (deterministic per index,
  no external rand dep); greedy ef=1 descent above the node's level, then
  `ef_construction` beam search per layer below; neighbours chosen by the
  paper's diversity heuristic rather than nearest-M (measurably better
  recall at equal M).
- **Reciprocal edge pruning**: adding a node appends it to each chosen
  neighbour's list, then re-selects down to M (layer-0 cap is `2M`).
- **Deletes are tombstones** (`DX`) — links are not physically removed;
  search skips tombstoned nodes. Update = delete + insert. If the entry
  point is deleted, `pick_entry` reselects the live node with the highest
  top layer in one bounded `NG` scan.
- **Vectors stored via `EncodedVector`** (`ndb-vector`): an encoding tag
  (f32 reference now; f16 and scalar int8 implemented as groundwork) so
  quantisation lands without a format migration (§10.3).
- **Filtering is post-filtering** (§10.5): the ef candidate set is filtered
  before top-k; callers pass `ef > k` for selective predicates. Predicate
  push-down into the graph walk is deferred.

## Consequences

- Graph edits are ordinary versioned `insert_in` writes — commits are
  atomic, rollbacks restore the old graph, and crash recovery replays them
  via WAL like everything else.
- recall@10 = 1.0 at 400 clustered 8-d vectors (m=8, ef_c=60, ef=50) in the
  acceptance test — the graph is correct, not just connected.
- Every distance computation fetches the neighbour's vector from the tree —
  IO-heavy vs a packed slab layout, but uniform with the rest of the engine.
- Tombstoned nodes keep their edges until compaction (not yet scheduled);
  long delete-heavy workloads will accumulate dead links.
