//! Query optimiser (spec §13) — **independent of parser and planner**.
//!
//! Inputs: the `FindQuery` AST + `PlanStats` + the planner's candidate
//! generation. Chooses the cheapest [`Plan`], orders residual conjuncts
//! cheapest-first, applies top-K/bounded-memory choices, and caches plans
//! by query shape (§13 plan caching).

use ndb_query_ast::*;
use ndb_query_planner::*;
use std::collections::HashMap;
use std::sync::Mutex;

/// Which access arm `choose` picked — the literal-free, shape-derived
/// decision the cache may safely store. Re-materialising against the
/// current query rebuilds every literal-bearing piece (pk id, index
/// ranges, query vector, residual predicates) so cached plans can't leak
/// stale literals into fresh queries.
#[derive(Debug, Clone)]
enum AccessChoice {
    Pk,
    Text,
    Vector,
    Index(String),
    Scan,
}

/// Plan cache keyed by a canonical shape string (§13). Values store the
/// `AccessChoice` — never a `Plan`, which embeds literal values.
#[derive(Default)]
pub struct Optimizer {
    cache: Mutex<HashMap<String, AccessChoice>>,
}

impl Optimizer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of cached plans — test/metrics visibility.
    pub fn cache_len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// A canonical key for the query's shape: collection + operator set +
    /// field names, ignoring literal values (literals don't change which
    /// index wins in our cost model).
    pub fn shape_key(q: &FindQuery) -> String {
        let mut s = format!("{}:{}", q.database, q.collection);
        if let Some(f) = &q.filter {
            fn ops(e: &Expr, out: &mut Vec<String>) {
                match e {
                    Expr::Cmp { field, op, .. } => out.push(format!("c{field}{op:?}")),
                    Expr::Logical { op, children } => {
                        out.push(format!("l{op:?}"));
                        for c in children {
                            ops(c, out);
                        }
                    }
                    Expr::Not { inner } => {
                        out.push("lnot".into());
                        ops(inner, out);
                    }
                    Expr::Array { field, op, .. } => out.push(format!("a{field}{op:?}")),
                    Expr::Exists { field, .. } => out.push(format!("x{field}")),
                    Expr::VectorSearch { index, field, .. } => {
                        out.push(format!("vs{index}{field}"))
                    }
                    Expr::VectorFilter { field, .. } => out.push(format!("vf{field}")),
                    Expr::Type { field, .. } => out.push(format!("t{field}")),
                    Expr::Text { .. } => out.push("txt".into()),
                    Expr::Phrase { field, .. } => out.push(format!("ph{field}")),
                    Expr::Fuzzy { field, .. } => out.push(format!("fz{field}")),
                }
            }
            let mut names = Vec::new();
            ops(f, &mut names);
            s.push_str(&names.join(","));
        }
        s
    }

    /// Picks the cheapest plan for `q` against `stats` (§13 index
    /// selection): pk lookup > text access > cheapest index range >
    /// full scan. Filter ordering: surviving conjuncts sorted by
    /// estimated selectivity so cheap, selective predicates run first.
    pub fn optimize(&self, q: &FindQuery, stats: &PlanStats) -> ndb_common::Result<Plan> {
        let key = Self::shape_key(q);
        let cached = self.cache.lock().unwrap().get(&key).cloned();
        if let Some(choice) = cached
            && let Some(p) = self.materialize(q, &choice, stats)?
        {
            return Ok(p);
        }
        // Miss or stale choice (index dropped since caching) — choose fresh.
        let (choice, plan) = self.choose(q, stats)?;
        self.cache.lock().unwrap().insert(key, choice);
        Ok(plan)
    }

    /// Rebuilds a `Plan` for `q` from a cached access decision — literals,
    /// sort/limit/projection and the residual all come from the *current*
    /// query. `Ok(None)` = the choice no longer applies (e.g. the index
    /// was dropped) and the caller should re-choose.
    fn materialize(
        &self,
        q: &FindQuery,
        choice: &AccessChoice,
        stats: &PlanStats,
    ) -> ndb_common::Result<Option<Plan>> {
        let n = stats.doc_count.max(1);
        let mut explain = vec![format!("collection size ~{n} docs")];
        let mut p = match (choice, &q.filter) {
            (AccessChoice::Scan, _) => {
                explain.push("access: full collection scan".into());
                assemble(q, AccessPath::FullScan, &[], n, explain)
            }
            (_, None) => return Ok(None),
            (AccessChoice::Pk, Some(filter)) => {
                let Some(id) = pk_lookup(filter) else {
                    return Ok(None);
                };
                explain.push("access: _id pk lookup".into());
                let consumed = pk_consumed(filter);
                assemble(q, AccessPath::PkLookup { id }, &consumed, 1, explain)
            }
            (AccessChoice::Text, Some(filter)) => {
                let Some((kind, index, ci)) = text_candidate(filter, stats) else {
                    return Ok(None);
                };
                explain.push(format!("access: text index '{index}' ({kind:?})"));
                assemble(
                    q,
                    AccessPath::TextSearch { index, kind },
                    &[ci],
                    n / 10,
                    explain,
                )
            }
            (AccessChoice::Vector, Some(filter)) => {
                let Some((ci, index, field, vector, k, ef)) = vector_candidate(filter, stats)
                else {
                    return Ok(None);
                };
                explain.push(format!("access: vector index '{index}' (hnsw k={k})"));
                assemble(
                    q,
                    AccessPath::VectorSearch {
                        index,
                        field,
                        vector,
                        k,
                        ef,
                    },
                    &[ci],
                    k.max(1) as u64,
                    explain,
                )
            }
            (AccessChoice::Index(name), Some(filter)) => {
                // Narrow stats to the chosen index — skips the other
                // indexes' candidate enumeration, which is what the cache
                // is for.
                let narrowed = PlanStats {
                    indexes: stats
                        .indexes
                        .iter()
                        .filter(|i| i.name == *name)
                        .cloned()
                        .collect(),
                    ..stats.clone()
                };
                let Some(best) = index_candidates(filter, &narrowed)?.into_iter().next() else {
                    return Ok(None);
                };
                explain.push(format!(
                    "access: index '{}' range scan (est {:.0}% selectivity)",
                    best.index_name,
                    best.selectivity * 100.0
                ));
                let est = ((n as f64) * best.selectivity).max(1.0) as u64;
                assemble(
                    q,
                    AccessPath::IndexScan {
                        index: best.index_name.clone(),
                        ranges: best.ranges.clone(),
                    },
                    &best.consumed,
                    est,
                    explain,
                )
            }
        };
        self.order_residual(&mut p);
        Ok(Some(p))
    }

    fn choose(&self, q: &FindQuery, stats: &PlanStats) -> ndb_common::Result<(AccessChoice, Plan)> {
        let n = stats.doc_count.max(1);
        let mut explain = vec![format!("collection size ~{n} docs")];

        // 1) pk lookup — O(1)
        if let Some(filter) = &q.filter {
            if let Some(id) = pk_lookup(filter) {
                explain.push("access: _id pk lookup".into());
                // All conjuncts except the pk clause are residual.
                let consumed = pk_consumed(filter);
                let mut p = assemble(q, AccessPath::PkLookup { id }, &consumed, 1, explain);
                self.order_residual(&mut p);
                return Ok((AccessChoice::Pk, p));
            }

            // 2) text access — a $text/$phrase/$fuzzy clause drives the search.
            if let Some((kind, index, ci)) = text_candidate(filter, stats) {
                explain.push(format!("access: text index '{index}' ({kind:?})"));
                let mut p = assemble(
                    q,
                    AccessPath::TextSearch { index, kind },
                    &[ci],
                    n / 10, // rough: text match returns a small candidate set
                    explain,
                );
                self.order_residual(&mut p);
                return Ok((AccessChoice::Text, p));
            }

            // 2b) vector access — a $vectorSearch clause drives the k-NN
            // index (§12.7). Post-text precedence: hybrid fusion belongs to
            // collection.search_hybrid, not the linear planner.
            if let Some((ci, index, field, vector, k, ef)) = vector_candidate(filter, stats) {
                explain.push(format!("access: vector index '{index}' (hnsw k={k})"));
                let mut p = assemble(
                    q,
                    AccessPath::VectorSearch {
                        index,
                        field,
                        vector,
                        k,
                        ef,
                    },
                    &[ci],
                    k.max(1) as u64,
                    explain,
                );
                self.order_residual(&mut p);
                return Ok((AccessChoice::Vector, p));
            }

            // 3) secondary index ranges — pick minimum estimated scan.
            let cands = index_candidates(filter, stats)?;
            if let Some(best) = cands.iter().min_by(|a, b| {
                a.selectivity
                    .partial_cmp(&b.selectivity)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }) {
                explain.push(format!(
                    "access: index '{}' range scan (est {:.0}% selectivity)",
                    best.index_name,
                    best.selectivity * 100.0
                ));
                let est = ((n as f64) * best.selectivity).max(1.0) as u64;
                let mut p = assemble(
                    q,
                    AccessPath::IndexScan {
                        index: best.index_name.clone(),
                        ranges: best.ranges.clone(),
                    },
                    &best.consumed,
                    est,
                    explain,
                );
                self.order_residual(&mut p);
                return Ok((AccessChoice::Index(best.index_name.clone()), p));
            }
        }

        // 4) full scan.
        explain.push("access: full collection scan".into());
        let mut p = assemble(q, AccessPath::FullScan, &[], n, explain);
        self.order_residual(&mut p);
        Ok((AccessChoice::Scan, p))
    }

    /// Sorts residual conjuncts cheapest/most-selective first (§13 filter
    /// ordering): existence/type checks first (cheapest), comparisons next,
    /// array predicates after, text ops last.
    fn order_residual(&self, p: &mut Plan) {
        let Some(r) = p.residual.take() else { return };
        fn rank(e: &Expr) -> u8 {
            match e {
                Expr::Exists { .. } | Expr::Type { .. } => 0,
                Expr::Cmp { .. } => 1,
                Expr::Not { .. } => 2,
                Expr::Array { .. } => 3,
                Expr::Text { .. } | Expr::Phrase { .. } | Expr::Fuzzy { .. } => 4,
                Expr::VectorFilter { .. } | Expr::VectorSearch { .. } => 4,
                Expr::Logical { .. } => 5,
            }
        }
        let mut conj: Vec<Expr> = conjuncts(&r).into_iter().cloned().collect();
        conj.sort_by_key(rank);
        p.residual = Some(match conj.len() {
            1 => conj.pop().unwrap(),
            _ => Expr::Logical {
                op: LogicalOp::And,
                children: conj,
            },
        });
    }
}

/// Conjunct indices consumed by an `_id`/`id` equality clause.
fn pk_consumed(filter: &Expr) -> Vec<usize> {
    conjuncts(filter)
        .iter()
        .enumerate()
        .filter(|(_, c)| {
            matches!(c, Expr::Cmp { field, op: CmpOp::Eq, .. }
                if field == "_id" || field == "id")
        })
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndb_document::Value;

    fn q(lit: &str) -> FindQuery {
        FindQuery {
            database: "d".into(),
            collection: "c".into(),
            filter: Some(Expr::Cmp {
                field: "name".into(),
                op: CmpOp::Eq,
                value: Value::String(lit.into()),
            }),
            projection: None,
            sort: vec![],
            limit: None,
            skip: 0,
        }
    }

    #[test]
    fn cached_shape_rebuilds_residual_with_fresh_literal() {
        let opt = Optimizer::new();
        let stats = PlanStats::default();
        let _p1 = opt.optimize(&q("alice"), &stats).unwrap();
        assert_eq!(opt.cache_len(), 1);
        // Same shape, different literal — the residual must carry "bob",
        // not the first query's "alice" (regression: stale-literal cache).
        let p2 = opt.optimize(&q("bob"), &stats).unwrap();
        let Some(Expr::Cmp { value, .. }) = p2.residual else {
            panic!("expected residual cmp, got {:?}", p2.residual);
        };
        assert_eq!(value, Value::String("bob".into()));
    }

    #[test]
    fn cached_shape_rebuilds_sort_and_limit() {
        let opt = Optimizer::new();
        let stats = PlanStats::default();
        opt.optimize(&q("a"), &stats).unwrap();
        let mut q2 = q("b");
        q2.sort = vec![SortKey {
            field: "age".into(),
            dir: SortDir::Desc,
        }];
        q2.limit = Some(7);
        let p2 = opt.optimize(&q2, &stats).unwrap();
        assert_eq!(p2.limit, Some(7));
        assert_eq!(p2.sort.len(), 1);
    }
}
