# ADR-012 — BM25 ranking

- **Status**: Accepted
- **Date**: Phase 5
- **Spec references**: §9.3 (BM25), §9.4 (block-max)

## Context

Spec §9.3 requires BM25 with k1 = 1.2, b = 0.75 defaults, both configurable,
plus field weighting, df/tf/avgdl/N maintenance, and versioned global
statistics for distributed comparability later.

## Decision

- **Formula**: `IDF = ln(1 + (N − df + 0.5)/(df + 0.5))` (Lucene-style,
  non-negative) × `tf·(k1+1) / (tf + k1·(1−b + b·dl/avgdl))`.
- **`Bm25` struct** holds k1/b + optional per-field weights; `WeightedScorer`
  multiplies a field's configured `TextFieldDef.weight` per posting.
- **Stats live in the index tree**: `MN` (doc count), `MF{field}` (total
  tokens → avgdl), `S{doc_id}` (per-doc field lengths → dl). Updated inside
  the writer's transaction — consistent at every snapshot.
- **Scorer is a trait** (`ndb_inverted::query::Scorer`) with a `max_score`
  hook fed by per-chunk `max_tf` — the seam Block-Max WAND plugs into when
  top-K pruning is implemented.
- **Multi-field search**: query analysed per field with that field's
  analyzer; scores merged by doc (sum of weighted field contributions).
- Distributed stats versioning is a Phase 9 concern — the
  `CollectionStats` shape already isolates what shards will ship.

## Consequences

- Scores are deterministic and comparable across restarts (stats persist).
- Exact IDF per (field, term); no approximate df tracking.
- top-K is a full scan of candidate postings today — the block-max metadata
  for WAND exists but pruning is unimplemented.
