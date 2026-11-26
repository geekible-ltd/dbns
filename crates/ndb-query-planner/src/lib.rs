//! Query planner (spec §12.8 stages "resolve fields" → "identify available
//! indexes" → "estimate selectivity" → "generate execution plan").
//!
//! The planner is **storage-agnostic**: it takes a [`PlanStats`] view of
//! the collection (doc count, secondary + text index definitions) and
//! produces a [`Plan`]. Index *selection across candidates* is the
//! optimiser's job (spec §13 — the optimiser is independent of the parser
//! and the planner).

use ndb_btree::key::{KeyPart, encode_composite};
use ndb_document::Value;
use ndb_query_ast::*;

/// One secondary index as the planner sees it.
#[derive(Debug, Clone)]
pub struct IndexView {
    pub name: String,
    pub fields: Vec<String>,
    pub unique: bool,
}

/// One full-text index as the planner sees it.
#[derive(Debug, Clone)]
pub struct TextIndexView {
    pub name: String,
    pub fields: Vec<String>,
}

/// Collection statistics view (§13 — the optimiser uses collection and
/// index statistics).
#[derive(Debug, Clone, Default)]
pub struct PlanStats {
    pub doc_count: u64,
    pub indexes: Vec<IndexView>,
    pub text_indexes: Vec<TextIndexView>,
}

/// How the executor reaches candidate documents.
#[derive(Debug, Clone, PartialEq)]
pub enum AccessPath {
    /// `_id` equality → direct pk read.
    PkLookup { id: u64 },
    /// Secondary-index range scan → posting ids → doc reads. Multiple
    /// ranges for `$in` fan-out; each is `(lo, hi, hi_inclusive)` —
    /// `hi: None` means unbounded above.
    IndexScan {
        index: String,
        ranges: Vec<(Vec<KeyPart>, Option<Vec<KeyPart>>, bool)>,
    },
    /// Full collection scan.
    FullScan,
    /// Full-text search → scored doc ids.
    TextSearch { index: String, kind: TextKind },
}

/// Text-query flavours routed to collection search APIs (§12.6).
#[derive(Debug, Clone, PartialEq)]
pub enum TextKind {
    Match {
        query: String,
    },
    Phrase {
        field: String,
        terms: String,
        slop: u32,
    },
    Fuzzy {
        field: String,
        term: String,
        max_edits: u32,
        max_expansions: usize,
    },
}

/// A physical plan — one access path + residual work (§12.8).
#[derive(Debug, Clone)]
pub struct Plan {
    pub access: AccessPath,
    /// Filter still applied per candidate doc (non-indexed conjuncts).
    pub residual: Option<Expr>,
    pub sort: Vec<SortKey>,
    pub projection: Option<Projection>,
    pub limit: Option<u64>,
    pub skip: u64,
    /// Estimated docs the access path will touch (optimiser fills cost).
    pub est_scanned: u64,
    /// EXPLAIN lines (§12.8) — the chosen plan's narration.
    pub explain: Vec<String>,
}

/// The part of a filter one secondary index can serve: conjuncts on the
/// index's leftmost fields (all-equality prefix, then at most one range).
pub struct Candidate {
    pub index_name: String,
    /// Equality prefix + optional terminating range — `hi: None` is
    /// unbounded above. `$in` produces one point range per element.
    pub ranges: Vec<(Vec<KeyPart>, Option<Vec<KeyPart>>, bool)>,
    /// Conjunct indices consumed by the index (the rest are residual).
    pub consumed: Vec<usize>,
    /// Rough selectivity: fraction of docs the range covers.
    pub selectivity: f64,
}

/// Splits a filter into AND-able conjuncts (top-level `$and` flattens).
pub fn conjuncts(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::Logical {
            op: LogicalOp::And,
            children,
        } => children.iter().flat_map(conjuncts).collect(),
        other => vec![other],
    }
}

/// Generates every secondary-index access candidate the filter admits.
///
/// For each index, walk its fields left-to-right:
/// - `$eq`/`$in` on a field contributes point values (Cartesian product
///   into `ranges`, capped at 64 to bound fan-out);
/// - the first field with a range operator terminates the prefix with a
///   `(prefix+lo, prefix+hi)` range;
/// - a field with no usable conjunct ends the prefix (leftmost-prefix
///   rule, §13 index selection).
pub fn index_candidates(filter: &Expr, stats: &PlanStats) -> ndb_common::Result<Vec<Candidate>> {
    const MAX_RANGES: usize = 64;
    let conj = conjuncts(filter);
    let mut out = Vec::new();

    for idx in &stats.indexes {
        // prefix_alts: each entry is a partial equality prefix.
        let mut prefixes: Vec<Vec<KeyPart>> = vec![Vec::new()];
        let mut ranges: Vec<(Vec<KeyPart>, Option<Vec<KeyPart>>, bool)> = Vec::new();
        let mut consumed: Vec<usize> = Vec::new();
        let mut selectivity = 1.0f64;

        for (fi, fname) in idx.fields.iter().enumerate() {
            // Gather this field's usable conjuncts.
            let mut points: Vec<KeyPart> = Vec::new();
            let mut lower: Option<(KeyPart, bool, usize)> = None; // (bound, inclusive, conj)
            let mut upper: Option<(KeyPart, bool, usize)> = None;
            for (ci, c) in conj.iter().enumerate() {
                let (f, op, v) = match c {
                    Expr::Cmp { field, op, value } => (field, *op, value),
                    _ => continue,
                };
                if f != fname {
                    continue;
                }
                match op {
                    CmpOp::Eq => {
                        if let Some(p) = value_part(v) {
                            points.push(p);
                            consumed.push(ci);
                        }
                    }
                    CmpOp::In => {
                        if let Value::Array(items) = v {
                            for it in items {
                                if let Some(p) = value_part(it) {
                                    points.push(p);
                                }
                            }
                            if !points.is_empty() {
                                consumed.push(ci);
                            }
                        }
                    }
                    CmpOp::Gt | CmpOp::Gte => {
                        if let Some(p) = value_part(v) {
                            lower = Some((p, op == CmpOp::Gte, ci));
                            consumed.push(ci);
                        }
                    }
                    CmpOp::Lt | CmpOp::Lte => {
                        if let Some(p) = value_part(v) {
                            upper = Some((p, op == CmpOp::Lte, ci));
                            consumed.push(ci);
                        }
                    }
                    _ => {}
                }
            }

            if !points.is_empty() {
                // Equality-ish position: extend each prefix by each point.
                let mut next = Vec::new();
                for pref in &prefixes {
                    for p in &points {
                        let mut np = pref.clone();
                        np.push(p.clone());
                        next.push(np);
                        if next.len() > MAX_RANGES {
                            break;
                        }
                    }
                    if next.len() > MAX_RANGES {
                        break;
                    }
                }
                prefixes = next;
                selectivity *= if idx.unique && fi == 0 {
                    1.0 / stats.doc_count.max(1) as f64
                } else {
                    0.1 * points.len() as f64
                };
                continue;
            }
            if lower.is_some() || upper.is_some() {
                // Terminating range position.
                for pref in &prefixes {
                    let lo = {
                        let mut l = pref.clone();
                        if let Some((b, _, _)) = &lower {
                            l.push(b.clone());
                        }
                        l
                    };
                    let hi = upper.as_ref().map(|(b, _, _)| {
                        let mut h = pref.clone();
                        h.push(b.clone());
                        h
                    });
                    ranges.push((
                        lo,
                        hi,
                        upper.as_ref().map(|(_, inc, _)| *inc).unwrap_or(false),
                    ));
                }
                selectivity *= 0.33;
                break;
            }
            break; // no conjunct on this field → prefix ends
        }

        if prefixes.iter().all(|p| !p.is_empty()) && ranges.is_empty() {
            // Pure point lookups: one exact-key range per prefix.
            for pref in &prefixes {
                ranges.push((pref.clone(), Some(pref.clone()), true));
            }
        }
        // Missing-field null slots also satisfy Eq on absent fields — for a
        // purely-equality prefix the point ranges already cover it.
        if ranges.is_empty() {
            continue;
        }
        out.push(Candidate {
            index_name: idx.name.clone(),
            ranges,
            consumed: {
                let mut c = consumed;
                c.sort_unstable();
                c.dedup();
                c
            },
            selectivity: selectivity.min(1.0),
        });
    }
    Ok(out)
}

/// Maps a `Value` to an index `KeyPart` — same mapping the index uses so
/// bounds land on the right entries (mirrors `ndb_index::index::value_part`).
fn value_part(v: &Value) -> Option<KeyPart> {
    Some(match v {
        Value::Bool(b) => KeyPart::Bool(*b),
        Value::Int(i) => KeyPart::Int(*i),
        Value::UInt(u) => KeyPart::UInt(*u),
        Value::Float(f) => KeyPart::Float(*f),
        Value::String(s) => KeyPart::Str(s.clone()),
        Value::Binary(b) => KeyPart::Bytes(b.clone()),
        Value::Uuid(u) => KeyPart::Bytes(u.0.to_vec()),
        Value::DateTime(d) => KeyPart::Int(d.0),
        _ => return None,
    })
}

/// `_id`/`id` equality → direct pk access (the cheapest path).
pub fn pk_lookup(filter: &Expr) -> Option<u64> {
    conjuncts(filter).iter().find_map(|c| match c {
        Expr::Cmp {
            field,
            op: CmpOp::Eq,
            value,
        } if field == "_id" || field == "id" => match value {
            Value::UInt(u) => Some(*u),
            Value::Int(i) if *i >= 0 => Some(*i as u64),
            _ => None,
        },
        _ => None,
    })
}

/// Text clauses in the filter → a TextSearch access candidate (§12.6).
/// The first text clause found wins as the access path; others (rare) stay
/// residual.
pub fn text_candidate(filter: &Expr, stats: &PlanStats) -> Option<(TextKind, String, usize)> {
    for (i, c) in conjuncts(filter).iter().enumerate() {
        let (kind, iname) = match c {
            Expr::Text { index, query } => (
                TextKind::Match {
                    query: query.clone(),
                },
                index.clone(),
            ),
            Expr::Phrase {
                index,
                field,
                terms,
                slop,
            } => (
                TextKind::Phrase {
                    field: field.clone(),
                    terms: terms.clone(),
                    slop: *slop,
                },
                index.clone(),
            ),
            Expr::Fuzzy {
                index,
                field,
                term,
                max_edits,
                max_expansions,
            } => (
                TextKind::Fuzzy {
                    field: field.clone(),
                    term: term.clone(),
                    max_edits: *max_edits,
                    max_expansions: *max_expansions,
                },
                index.clone(),
            ),
            _ => continue,
        };
        // Resolve the index name — explicit, else the collection's only/
        // first text index.
        let name = if !iname.is_empty() {
            iname
        } else {
            stats.text_indexes.first().map(|t| t.name.clone())?
        };
        if stats.text_indexes.iter().any(|t| t.name == name) {
            return Some((kind, name, i));
        }
    }
    None
}

/// Assembles a full `Plan` once the optimiser has picked an access path —
/// consumed conjuncts leave the residual filter.
pub fn assemble(
    query: &FindQuery,
    access: AccessPath,
    consumed: &[usize],
    est_scanned: u64,
    mut explain: Vec<String>,
) -> Plan {
    let residual = query.filter.as_ref().and_then(|f| {
        let conj = conjuncts(f);
        let keep: Vec<Expr> = conj
            .iter()
            .enumerate()
            .filter(|(i, _)| !consumed.contains(i))
            .map(|(_, e)| (*e).clone())
            .collect();
        match keep.len() {
            0 => None,
            1 => Some(keep[0].clone()),
            _ => Some(Expr::Logical {
                op: LogicalOp::And,
                children: keep,
            }),
        }
    });
    explain.push(format!(
        "residual filter: {}",
        if residual.is_some() {
            "present"
        } else {
            "none"
        }
    ));
    Plan {
        access,
        residual,
        sort: query.sort.clone(),
        projection: query.projection.clone(),
        limit: query.limit,
        skip: query.skip,
        est_scanned,
        explain,
    }
}

/// Convenience for `lo`/`hi` debugging in EXPLAIN output.
pub fn encode_bound(parts: &[KeyPart]) -> String {
    format!("{:?}", encode_composite(parts))
}
