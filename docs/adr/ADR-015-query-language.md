# ADR-015 — Query language and execution pipeline

- **Status**: Accepted
- **Date**: Phase 6
- **Spec references**: §12 (query language), §13 (query optimisation)

## Context

Spec §12.1 mandates a MongoDB-inspired JSON query language — explicitly
*not* full compatibility — with a versioned grammar, a documented operator
catalogue, a multi-stage execution pipeline, and §13's requirement that
the optimiser be independent of parser and planner.

## Decision

Six crates per §19, dependency-ordered:

- **`ndb-query-ast`** — `Expr`/`FindQuery`/`Pipeline`/`Stage`/`AggExpr`
  types, `GRAMMAR_VERSION`, the `OPERATORS` catalogue (§12.2–§12.7 —
  `$vectorSearch`/`$vectorFilter` reserved for Phase 7).
- **`ndb-query-parser`** — serde_json text → AST. Hard errors on unknown
  operators / mixed include+exclude projections / malformed clauses.
  `json_to_value` doubles as the REST ingestion path later.
- **`ndb-query-planner`** — storage-agnostic: `PlanStats` view in, `Plan`
  out. `AccessPath`: `PkLookup` | `IndexScan{ranges}` | `TextSearch` |
  `FullScan`. Leftmost-prefix index matching; `$in` fans out into point
  ranges (capped at 64); `hi: None` = unbounded.
- **`ndb-query-optimizer`** — cost-picks across candidates
  (pk > text > min-selectivity index > scan), orders residual conjuncts
  cheapest-first, caches plans by canonical query shape.
- **`ndb-query-executor`** — drives the plan: access → fetch → residual
  filter → sort (top-K bounded when limited) → skip/limit → projection.
  `ExecLimits`: wall-clock deadline, max docs scanned, max result bytes,
  `AtomicBool` cancellation — checked per doc (§13).
- **`ndb-aggregation`** — §12.9 stage set incl. `$unwind`/`$addFields`/
  expressions; `$group` bounded by `group_bytes_bound`, spilling partial
  accumulators into a scratch B+Tree (real disk spill via the storage
  engine itself). Also hosts `QueryEngine`, the facade
  (parse→plan→optimise→execute) — topmost layer avoids cycles.

## Consequences

- The full §12.8 pipeline exists end-to-end: `explain()` returns the Plan +
  narration without touching data.
- Sort is materialised in memory (bounded by limits); the merge-sorted
  external sort is the documented upgrade.
- `$text`/`$phrase`/`$fuzzy` route to collection text-index APIs and carry
  BM25 scores through the result.
- Deep-pagination cursors (§13) not yet implemented — offset+limit today.
