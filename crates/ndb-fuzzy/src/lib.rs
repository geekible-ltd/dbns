//! Fuzzy term matching over the term dictionary (spec §9.6).
//!
//! Bounded Levenshtein expansion: scans the ordered term dictionary,
//! keeps every term within `max_edits` of the query term, capped at
//! `max_expansions` — expansion is never unbounded (§9.6).
//!
//! A DP row computes edit distance with early exit: if the row's minimum
//! already exceeds `max_edits`, the candidate can't match. That's the
//! honest Phase-5 baseline; symmetric-delete preindexing and automaton
//! intersection are the documented next steps.

use ndb_btree::BTree;
use ndb_inverted::InvertedIndex;
use ndb_page::manager::PageIo;
use ndb_transaction::Transaction;
use std::sync::Arc;

/// Bounded Levenshtein distance — `None` when distance > `max`.
/// Full DP matrix with early exit: if a row's minimum already exceeds
/// `max`, no completion can bring the result back under the bound.
pub fn bounded_distance(a: &str, b: &str, max: u32) -> Option<u32> {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let (n, m) = (a.len(), b.len());
    if n.abs_diff(m) > max as usize {
        return None;
    }
    // prev[j] = edit distance between a[..i-1] and b[..j]
    let mut prev: Vec<u32> = (0..=m).map(|j| j as u32).collect();
    for i in 1..=n {
        let mut cur = vec![u32::MAX; m + 1];
        cur[0] = i as u32;
        for j in 1..=m {
            let cost = u32::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1) // delete a[i-1]
                .min(cur[j - 1] + 1) // insert b[j-1]
                .min(prev[j - 1] + cost); // substitute / match
        }
        if cur.iter().all(|&c| c > max) {
            return None; // no cell can reach ≤max — early exit
        }
        prev = cur;
    }
    if prev[m] <= max { Some(prev[m]) } else { None }
}

/// Expand `term` over `field_id`'s dictionary to terms within
/// `max_edits`, at most `max_expansions` results, nearest first.
pub fn expand<P: PageIo>(
    txn: &Transaction<'_, P>,
    index_tree: &Arc<BTree<P>>,
    field_id: u16,
    term: &str,
    max_edits: u32,
    max_expansions: usize,
) -> ndb_common::Result<Vec<String>> {
    let mut scored: Vec<(u32, String)> = Vec::new();
    for cand in InvertedIndex::terms(txn, index_tree, field_id)? {
        if scored.len() >= max_expansions && scored.last().is_some_and(|(d, _)| *d == 0) {
            break; // can't beat exact matches — dictionary exhausted
        }
        if let Some(d) = bounded_distance(term, &cand, max_edits) {
            scored.push((d, cand));
            if scored.len() >= max_expansions * 4 {
                // Hard scan cap — keeps expansion bounded even when the
                // dictionary is enormous (§9.6 no unbounded expansion).
                break;
            }
        }
    }
    scored.sort_by_key(|(d, s)| (*d, s.clone()));
    scored.truncate(max_expansions);
    Ok(scored.into_iter().map(|(_, s)| s).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distances() {
        assert_eq!(bounded_distance("cat", "cat", 2), Some(0));
        assert_eq!(bounded_distance("cat", "bat", 2), Some(1));
        assert_eq!(bounded_distance("cat", "cats", 2), Some(1));
        assert_eq!(bounded_distance("kitten", "sitting", 3), Some(3));
        assert_eq!(bounded_distance("kitten", "sitting", 2), None);
        assert_eq!(bounded_distance("abc", "xyz", 2), None);
    }

    #[test]
    fn band_respects_length_gap() {
        assert_eq!(bounded_distance("a", "abcdefghij", 2), None);
        assert_eq!(bounded_distance("flaw", "lawn", 2), Some(2));
    }
}
