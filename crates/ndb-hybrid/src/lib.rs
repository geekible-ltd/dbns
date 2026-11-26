//! Hybrid retrieval: Reciprocal Rank Fusion + the §11.3 candidate model.
//!
//! Retrieval legs (full-text, vector, fuzzy) each produce a ranked list of
//! [`Candidate`]s — compact `(doc_id, score)` pairs with no document payload.
//! [`Fusion`] merges them by rank position; the executor fetches documents
//! only for the surviving top-K (late materialization, §11.3).

use std::collections::HashMap;

use ndb_common::Result;
use ndb_common::error::Error;

/// A retrieval candidate: document id + per-leg score. No payload.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Candidate {
    pub doc: u64,
    pub score: f32,
}

/// Which retrieval leg produced a candidate (for explain/diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    Text,
    Vector,
    Fuzzy,
}

/// Reciprocal Rank Fusion (§11.2), configurable.
///
/// `score(d) = Σ_legs weight_leg / (k + rank_leg(d))` — rank positions are
/// 1-based per leg; docs absent from a leg contribute 0.
#[derive(Debug, Clone)]
pub struct Fusion {
    /// RRF constant (default 60 — the Cormack et al. recommendation).
    pub k: u32,
    /// Per-leg weights (default 1.0 each).
    pub weights: Vec<f64>,
}

impl Default for Fusion {
    fn default() -> Self {
        Self {
            k: 60,
            weights: Vec::new(),
        }
    }
}

impl Fusion {
    pub fn new(k: u32, weights: Vec<f64>) -> Result<Self> {
        if k == 0 {
            return Err(Error::InvalidInput("rrf k must be > 0".into()));
        }
        Ok(Self { k, weights })
    }

    /// Merges ranked candidate lists; returns `(doc_id, fused_score)` sorted
    /// descending, truncated to `top`.
    pub fn merge(&self, legs: &[Vec<Candidate>], top: usize) -> Vec<(u64, f64)> {
        let mut acc: HashMap<u64, f64> = HashMap::new();
        for (i, list) in legs.iter().enumerate() {
            let w = self.weights.get(i).copied().unwrap_or(1.0);
            if w == 0.0 {
                continue;
            }
            for (rank, c) in list.iter().enumerate() {
                *acc.entry(c.doc).or_insert(0.0) += w / (self.k as f64 + rank as f64 + 1.0);
            }
        }
        let mut out: Vec<(u64, f64)> = acc.into_iter().collect();
        out.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        out.truncate(top);
        out
    }
}

/// A hybrid query (§11.3 validate step happens at the caller).
#[derive(Default)]
pub struct HybridQuery {
    /// Full-text leg: match query against a named text index.
    pub text: Option<TextLeg>,
    /// Vector leg: k-NN against a named vector index.
    pub vector: Option<VectorLeg>,
    /// Candidate-level predicate (tenant/security filter, §11.3) — applied
    /// before fusion so excluded ids can't consume rank slots.
    pub candidate_filter: Option<Box<dyn Fn(u64) -> bool + Send + Sync>>,
    /// Final result count.
    pub limit: usize,
    /// Per-leg fetch depth (defaults to `limit * 4` when 0).
    pub fetch_k: usize,
    pub fusion: Option<Fusion>,
}

#[derive(Debug)]
pub struct TextLeg {
    pub index: String,
    pub query: String,
    /// Weight multiplier on this leg's RRF contribution.
    pub weight: f64,
}

#[derive(Debug)]
pub struct VectorLeg {
    pub index: String,
    /// Query embedding.
    pub vector: Vec<f32>,
    /// Beam width override (0 = index default).
    pub ef: usize,
    pub weight: f64,
}

impl HybridQuery {
    /// §11.3 step 1 — validation shared by callers.
    pub fn validate(&self) -> Result<()> {
        if self.text.is_none() && self.vector.is_none() {
            return Err(Error::InvalidInput(
                "hybrid query needs at least one retrieval leg".into(),
            ));
        }
        if let Some(v) = &self.vector
            && v.vector.is_empty()
        {
            return Err(Error::InvalidInput("vector leg has empty embedding".into()));
        }
        if self.limit == 0 {
            return Err(Error::InvalidInput("hybrid limit must be > 0".into()));
        }
        Ok(())
    }
}

/// One fused result row: doc id + fused score + which legs contributed.
#[derive(Debug, Clone)]
pub struct HybridHit {
    pub doc: u64,
    pub score: f64,
    pub legs: Vec<Leg>,
}

/// Merges legs with RRF and annotates provenance — the §11.3 merge +
/// fusion-score steps. Leg order must match `Fusion.weights`.
pub fn fuse(q: &HybridQuery, legs: &[(Leg, Vec<Candidate>)], fetch_k: usize) -> Vec<HybridHit> {
    let fusion = q.fusion.clone().unwrap_or_default();
    let leg_kinds: Vec<Leg> = legs.iter().map(|l| l.0).collect();
    let lists: Vec<Vec<Candidate>> = legs.iter().map(|l| l.1.clone()).collect();
    let fused = fusion.merge(&lists, fetch_k);

    // provenance: which legs contained each surviving doc
    let mut presence: HashMap<u64, Vec<Leg>> = HashMap::new();
    for (kind, list) in legs {
        for c in list {
            presence.entry(c.doc).or_default().push(*kind);
        }
    }
    fused
        .into_iter()
        .map(|(doc, score)| HybridHit {
            doc,
            score,
            legs: presence.remove(&doc).unwrap_or_else(|| leg_kinds.clone()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leg(v: &[(u64, f32)]) -> Vec<Candidate> {
        v.iter()
            .map(|&(d, s)| Candidate { doc: d, score: s })
            .collect()
    }

    #[test]
    fn rrf_merges_by_rank() {
        let f = Fusion::default();
        // doc1 #1 in text, absent in vector; doc2 #2 text + #1 vector.
        let out = f.merge(
            &[leg(&[(1, 9.0), (2, 5.0)]), leg(&[(2, 0.9), (3, 0.8)])],
            10,
        );
        assert_eq!(out[0].0, 2); // present in both legs wins
        assert!(out[0].1 > out[1].1);
    }

    #[test]
    fn weights_and_topk() {
        let f = Fusion::new(10, vec![1.0, 0.0]).unwrap();
        let out = f.merge(&[leg(&[(1, 1.0)]), leg(&[(2, 1.0)])], 10);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, 1); // zero-weighted leg contributes nothing
        assert!(Fusion::new(0, vec![]).is_err());
    }

    #[test]
    fn fuse_records_provenance() {
        let q = HybridQuery {
            limit: 5,
            ..Default::default()
        };
        let legs = [
            (Leg::Text, leg(&[(1, 1.0), (2, 0.5)])),
            (Leg::Vector, leg(&[(2, 0.9)])),
        ];
        let hits = fuse(&q, &legs, 5);
        assert_eq!(hits[0].doc, 2);
        assert!(hits[0].legs.contains(&Leg::Vector));
    }
}
