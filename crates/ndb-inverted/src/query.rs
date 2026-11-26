//! Query AST + execution for full-text search (spec §9.1–9.6).

use super::{InvertedIndex, Posting};
use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_page::manager::PageIo;
use ndb_transaction::Transaction;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// What a scorer needs per posting.
pub struct ScoreInput<'a> {
    pub posting: &'a Posting,
    /// Document length for the scored field.
    pub doc_len: u32,
    /// Collection stats: (doc_count, avg field len).
    pub doc_count: u64,
    pub avg_field_len: f64,
    /// Document frequency for this term.
    pub df: u64,
}

/// Pluggable scorer — `ndb-bm25` supplies the production impl; tests can
/// substitute a trivial one.
pub trait Scorer {
    fn score(&self, input: &ScoreInput) -> f64;
    /// Upper bound on a posting's contribution given the chunk's block-max
    /// tf — the hook Block-Max WAND pruning uses (§9.4).
    fn max_score(
        &self,
        max_tf: u32,
        df: u64,
        doc_count: u64,
        avg_field_len: f64,
        doc_len_bound: u32,
    ) -> f64;
}

/// Text query AST.
#[derive(Debug, Clone)]
pub enum TextQuery {
    /// One analysed term on a field.
    Term { field_id: u16, term: String },
    /// OR/AND match over terms (analyzer output for a text query).
    Match {
        field_id: u16,
        terms: Vec<String>,
        must_all: bool,
    },
    /// Ordered terms within `slop` positions (§9.5).
    Phrase {
        field_id: u16,
        terms: Vec<String>,
        slop: u32,
    },
    /// Dictionary expansion by edit distance (§9.6).
    Fuzzy {
        field_id: u16,
        term: String,
        max_edits: u32,
        max_expansions: usize,
    },
}

/// A scored hit.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub doc_id: u64,
    pub score: f64,
}

/// Execute `q` against `tree`, scoring each surviving doc.
pub fn search<P, S>(
    txn: &Transaction<'_, P>,
    pool: &Arc<BufferPool<P>>,
    tree: &Arc<BTree<P>>,
    q: &TextQuery,
    scorer: &S,
    field_id_for_stats: u16,
    top_k: usize,
) -> ndb_common::Result<Vec<Hit>>
where
    P: PageIo,
    S: Scorer,
{
    let deleted = InvertedIndex::deleted_set(txn, tree)?;
    let stats = InvertedIndex::collection_stats(txn, tree, &[field_id_for_stats])?;
    let n = stats.doc_count;
    let avg = stats
        .field_total_len
        .get(&field_id_for_stats)
        .copied()
        .unwrap_or(0) as f64
        / n.max(1) as f64;

    // Accumulate per-doc score over the query's term postings.
    let mut scores: BTreeMap<u64, f64> = BTreeMap::new();

    let mut score_field_term = |fid: u16, term: &str| -> ndb_common::Result<()> {
        let postings = InvertedIndex::postings(txn, pool, tree, fid, term)?;
        let live: Vec<Posting> = postings
            .into_iter()
            .filter(|p| !deleted.contains(&p.doc_id))
            .collect();
        let df = live.len() as u64;
        for p in &live {
            let dl = InvertedIndex::doc_stats(txn, tree, p.doc_id)?
                .and_then(|s| s.field_len.get(&fid).copied())
                .unwrap_or(0);
            let s = scorer.score(&ScoreInput {
                posting: p,
                doc_len: dl,
                doc_count: n,
                avg_field_len: avg,
                df,
            });
            *scores.entry(p.doc_id).or_default() += s;
        }
        Ok(())
    };

    match q {
        TextQuery::Term { field_id, term } => score_field_term(*field_id, term)?,
        TextQuery::Match {
            field_id,
            terms,
            must_all,
        } => {
            for t in terms {
                score_field_term(*field_id, t)?;
            }
            if *must_all {
                // AND semantics: keep docs that matched every term.
                let mut per_term: Vec<BTreeSet<u64>> = Vec::new();
                for t in terms {
                    let ps = InvertedIndex::postings(txn, pool, tree, *field_id, t)?;
                    per_term.push(
                        ps.into_iter()
                            .map(|p| p.doc_id)
                            .filter(|d| !deleted.contains(d))
                            .collect(),
                    );
                }
                if let Some((first, rest)) = per_term.split_first() {
                    let mut keep = first.clone();
                    for s in rest {
                        keep = keep.intersection(s).copied().collect();
                    }
                    scores.retain(|d, _| keep.contains(d));
                }
            }
        }
        TextQuery::Phrase {
            field_id,
            terms,
            slop,
        } => {
            if terms.is_empty() {
                return Ok(vec![]);
            }
            // Load all lists; phrase match on positional intersection.
            let mut lists: Vec<BTreeMap<u64, Vec<u32>>> = Vec::new();
            for t in terms {
                let ps = InvertedIndex::postings(txn, pool, tree, *field_id, t)?;
                let mut m = BTreeMap::new();
                for p in ps.into_iter().filter(|p| !deleted.contains(&p.doc_id)) {
                    m.insert(p.doc_id, p.positions);
                }
                lists.push(m);
            }
            // Docs containing every term.
            let mut cand: BTreeSet<u64> = lists[0].keys().copied().collect();
            for m in &lists[1..] {
                cand = cand
                    .intersection(&m.keys().copied().collect())
                    .copied()
                    .collect();
            }
            for doc in cand {
                if phrase_match(&lists, doc, *slop) {
                    let p = Posting {
                        doc_id: doc,
                        tf: 1,
                        positions: vec![],
                    };
                    let dl = InvertedIndex::doc_stats(txn, tree, doc)?
                        .and_then(|s| s.field_len.get(field_id).copied())
                        .unwrap_or(0);
                    let s = scorer.score(&ScoreInput {
                        posting: &p,
                        doc_len: dl,
                        doc_count: n,
                        avg_field_len: avg,
                        df: cand_count(&lists) as u64,
                    });
                    scores.insert(doc, s);
                }
            }
        }
        TextQuery::Fuzzy { .. } => {
            return Err(ndb_common::error::Error::InvalidInput(
                "fuzzy queries are expanded by ndb-fuzzy first (§9.6)".into(),
            ));
        }
    }

    let mut hits: Vec<Hit> = scores
        .into_iter()
        .map(|(doc_id, score)| Hit { doc_id, score })
        .collect();
    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    hits.truncate(top_k);
    Ok(hits)
}

fn cand_count(lists: &[BTreeMap<u64, Vec<u32>>]) -> usize {
    lists.iter().map(|m| m.len()).sum()
}

/// Ordered positional match with `slop` — positions p_i must satisfy
/// p_{i+1} - p_i ∈ [1, 1+slop] cumulatively (classic Lucene slop).
fn phrase_match(lists: &[BTreeMap<u64, Vec<u32>>], doc: u64, slop: u32) -> bool {
    let first = match lists[0].get(&doc) {
        Some(p) => p.clone(),
        None => return false,
    };
    // Walk candidates: for each start pos in term0, try to extend.
    let mut starts: Vec<i64> = first.iter().map(|&p| p as i64).collect();
    for m in &lists[1..] {
        let Some(next) = m.get(&doc) else {
            return false;
        };
        let mut new_starts = Vec::new();
        for &s in &starts {
            for &np in next {
                let gap = np as i64 - s;
                if gap >= 1 && gap <= 1 + slop as i64 {
                    new_starts.push(np as i64);
                }
            }
        }
        if new_starts.is_empty() {
            return false;
        }
        starts = new_starts;
    }
    !starts.is_empty()
}
