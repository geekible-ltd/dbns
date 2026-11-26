# ADR-014: Hybrid search using RRF

- Status: accepted
- Date: phase 7 (spec §11)

## Context

§11 requires full-text + vector (+ fuzzy later) retrieval to merge into one
ranked result. The legs return *different score scales* — BM25 unbounded,
cosine/dot bounded, euclidean negative — so raw score fusion is not
meaningful.

## Decision

- **Reciprocal Rank Fusion** (`ndb-hybrid`): `score(d) = Σ 1/(k + rank_d)`
  per leg, configurable `k` (default 60 per Cormack et al.) and per-leg
  weights. Rank-based fusion is scale-free by construction — no score
  normalisation assumptions.
- **Compact candidates**: legs produce `Candidate{doc_id, score}` — no
  document payloads — matching §11.3's "do not retrieve full document
  payloads until the final result set is known".
- **`Collection::search_hybrid`** implements the §11.3 pipeline: validate →
  resolve indexes → apply the candidate (tenant/security) filter *before*
  fusion so excluded ids can't consume rank slots → run legs → RRF merge →
  fetch payloads for the final top-K only → return hits with per-leg
  provenance.
- **Leg fetch depth** defaults to `limit×4` so fusion sees enough
  candidates; per-leg `ef`/`fetch_k` are configurable on `HybridQuery`.
- Query-language `$vectorSearch` is the *single-leg* path (drives the HNSW
  access path in the planner); true hybrid fusion is deliberately a
  collection API, not squeezed into the linear planner.

## Consequences

- Provenance (`HybridHit.legs`) makes fusion explainable — the acceptance
  test asserts a doc present in both legs outranks single-leg docs.
- RRF has no notion of absolute relevance — a weak text match and a strong
  vector match get symmetric credit; weighted score fusion / learned
  ranking are the documented §11.2 future options.
- Fuzzy legs (`Leg::Fuzzy`) are wired into the fusion type but no leg
  producer exists yet — one-line addition when needed.
