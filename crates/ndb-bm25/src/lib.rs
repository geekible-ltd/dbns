//! BM25 ranking (spec §9.3).
//!
//! ```text
//! score(D, T) = IDF(T) · ( tf · (k1+1) ) / ( tf + k1·(1 − b + b·dl/avgdl) )
//! IDF(T)    = ln( 1 + (N − df + 0.5) / (df + 0.5) )        // Lucene-style
//! ```
//!
//! Defaults k1 = 1.2, b = 0.75 — both configurable, plus per-field boosts
//! (§9.3). `Bm25` implements `ndb_inverted::query::Scorer` so queries can
//! mix weighted fields.

use ndb_inverted::query::{ScoreInput, Scorer};

/// Configurable BM25 parameters + per-field boosts.
#[derive(Debug, Clone)]
pub struct Bm25 {
    pub k1: f64,
    pub b: f64,
    /// field_id → multiplicative weight (default 1.0).
    pub field_weights: std::collections::BTreeMap<u16, f64>,
}

impl Default for Bm25 {
    /// Spec defaults: k1 = 1.2, b = 0.75.
    fn default() -> Self {
        Self {
            k1: 1.2,
            b: 0.75,
            field_weights: Default::default(),
        }
    }
}

impl Bm25 {
    /// Lucene-style non-negative IDF.
    pub fn idf(df: u64, doc_count: u64) -> f64 {
        (1.0 + (doc_count as f64 - df as f64 + 0.5) / (df as f64 + 0.5))
            .ln()
            .max(0.0)
    }

    /// The tf·normalisation component for one doc.
    pub fn tf_norm(&self, tf: f64, doc_len: u32, avg_field_len: f64) -> f64 {
        let avg = if avg_field_len > 0.0 {
            avg_field_len
        } else {
            1.0
        };
        (tf * (self.k1 + 1.0)) / (tf + self.k1 * (1.0 - self.b + self.b * doc_len as f64 / avg))
    }

    /// Field weight for `field_id` (1.0 when unlisted).
    pub fn weight(&self, field_id: u16) -> f64 {
        self.field_weights.get(&field_id).copied().unwrap_or(1.0)
    }

    /// Weighted score for a posting on `field_id`.
    pub fn score(&self, field_id: u16, input: &ScoreInput) -> f64 {
        Self::idf(input.df, input.doc_count)
            * self.tf_norm(input.posting.tf as f64, input.doc_len, input.avg_field_len)
            * self.weight(field_id)
    }
}

/// Adapter so `query::search` can take `&Bm25` directly — field weighting
/// happens inside `score` via `ScoreInput`'s posting field set by callers.
/// The trait's `score` is field-agnostic, so weighting is applied by the
/// caller-aware [`WeightedScorer`] below.
impl Scorer for Bm25 {
    fn score(&self, input: &ScoreInput) -> f64 {
        Self::idf(input.df, input.doc_count)
            * self.tf_norm(input.posting.tf as f64, input.doc_len, input.avg_field_len)
    }

    /// Block-max bound: best case is max_tf in a minimal-length doc.
    fn max_score(
        &self,
        max_tf: u32,
        df: u64,
        doc_count: u64,
        avg_field_len: f64,
        doc_len_bound: u32,
    ) -> f64 {
        Self::idf(df, doc_count) * self.tf_norm(max_tf as f64, doc_len_bound, avg_field_len)
    }
}

/// Field-weighted scorer wrapper — multiplies each posting's contribution
/// by `weight` (the `TextFieldDef.weight` configured on the index).
pub struct WeightedScorer<'a> {
    pub bm25: &'a Bm25,
    pub weight: f64,
}

impl Scorer for WeightedScorer<'_> {
    fn score(&self, input: &ScoreInput) -> f64 {
        Scorer::score(self.bm25, input) * self.weight
    }
    fn max_score(
        &self,
        max_tf: u32,
        df: u64,
        doc_count: u64,
        avg_field_len: f64,
        doc_len_bound: u32,
    ) -> f64 {
        self.bm25
            .max_score(max_tf, df, doc_count, avg_field_len, doc_len_bound)
            * self.weight
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndb_inverted::Posting;

    fn input(tf: u32, dl: u32, n: u64, avg: f64, df: u64) -> ScoreInput<'static> {
        // Leak a posting for the 'static lifetime — test-only.
        let p: &'static Posting = Box::leak(Box::new(Posting {
            doc_id: 1,
            tf,
            positions: vec![],
        }));
        ScoreInput {
            posting: p,
            doc_len: dl,
            doc_count: n,
            avg_field_len: avg,
            df,
        }
    }

    #[test]
    fn higher_tf_scores_higher() {
        let b = Bm25::default();
        assert!(
            b.score(0, &input(5, 100, 1000, 100.0, 10))
                > b.score(0, &input(1, 100, 1000, 100.0, 10))
        );
    }

    #[test]
    fn longer_doc_scores_lower() {
        let b = Bm25::default();
        assert!(
            b.score(0, &input(2, 50, 1000, 100.0, 10))
                > b.score(0, &input(2, 500, 1000, 100.0, 10))
        );
    }

    #[test]
    fn rarer_terms_score_higher() {
        let b = Bm25::default();
        assert!(
            b.score(0, &input(1, 100, 1000, 100.0, 2))
                > b.score(0, &input(1, 100, 1000, 100.0, 500))
        );
    }

    #[test]
    fn field_weight_scales() {
        let mut b = Bm25::default();
        b.field_weights.insert(3, 3.0);
        let i = input(2, 100, 100, 100.0, 5);
        assert!((b.score(3, &i) / b.score(0, &i) - 3.0).abs() < 1e-9);
    }
}
