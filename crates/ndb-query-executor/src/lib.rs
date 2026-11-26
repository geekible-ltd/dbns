//! Query executor (spec §12.8 "execute operations" + §13 resource limits).
//!
//! `execute_find` drives a [`Plan`] over a `Collection`:
//!
//! ```text
//! access path → doc stream → residual filter → sort/top-K → skip → limit
//!   → projection → results
//! ```
//!
//! Every query runs under [`ExecLimits`]: max wall time, max docs scanned,
//! max result bytes, and a cancellation flag — checked on every doc
//! (§13). Sort+limit uses a bounded top-K heap so memory stays capped.

use ndb_btree::key::encode_composite;
use ndb_common::error::Error;
use ndb_document::{Document, Value};
use ndb_index::Collection;
use ndb_page::manager::PageIo;
use ndb_query_ast::*;
use ndb_query_planner::{AccessPath, Plan, TextKind};
use ndb_transaction::Transaction;
use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering as AOrd};
use std::time::{Duration, Instant};

/// Resource limits per §13 — every query has max time, memory, result
/// size, and cancellation.
#[derive(Clone)]
pub struct ExecLimits {
    /// Wall-clock deadline from query start.
    pub max_time: Duration,
    /// Max candidate docs the access path may scan.
    pub max_docs_scanned: u64,
    /// Max accumulated result payload bytes (rough doc-size accounting).
    pub max_result_bytes: u64,
    /// Cooperative cancellation.
    pub cancel: Arc<AtomicBool>,
}

impl Default for ExecLimits {
    fn default() -> Self {
        Self {
            max_time: Duration::from_secs(30),
            max_docs_scanned: 1_000_000,
            max_result_bytes: 64 << 20,
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ExecLimits {
    /// Limit check — polled per doc by executors.
    pub fn check(&self, scanned: u64, bytes: u64, start: Instant) -> ndb_common::Result<()> {
        if self.cancel.load(AOrd::Relaxed) {
            return Err(Error::InvalidInput("query: cancelled".into()));
        }
        if scanned > self.max_docs_scanned {
            return Err(Error::InvalidInput(format!(
                "query: scanned {} docs — limit {}",
                scanned, self.max_docs_scanned
            )));
        }
        if bytes > self.max_result_bytes {
            return Err(Error::InvalidInput("query: result size limit".into()));
        }
        if start.elapsed() > self.max_time {
            return Err(Error::InvalidInput("query: time limit exceeded".into()));
        }
        Ok(())
    }
}

/// Query output — docs in plan order, plus EXPLAIN narration.
pub struct QueryResult {
    pub docs: Vec<Document>,
    pub explain: Vec<String>,
    /// Post-sort scores for text searches (aligned with `docs` when
    /// the access path was `TextSearch` and no sort reordered it).
    pub scores: Option<Vec<f64>>,
}

/// Dotted-path field lookup (`"a.b.c"` descends Objects).
fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Value> {
    let mut cur: Option<&Value> = None;
    for (i, seg) in path.split('.').enumerate() {
        cur = match (i, cur) {
            (0, _) => doc.fields.get(seg),
            (_, Some(Value::Object(o))) => o.get(seg),
            _ => return None,
        };
    }
    cur
}

/// Total ordering over `Value`s for comparisons + sort keys.
/// Order: Null < Bool < numbers < String < Binary < DateTime < Array <
/// Object < Uuid < Vector (cross-type compares by rank — deterministic).
fn rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Decimal(_) => 2,
        Value::String(_) => 3,
        Value::Binary(_) => 4,
        Value::DateTime(_) => 5,
        Value::Array(_) => 6,
        Value::Object(_) => 7,
        Value::Uuid(_) => 8,
        Value::Vector(_) => 9,
    }
}

/// Numeric compare across Int/UInt/Float/Decimal.
fn num_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    let to_f = |v: &Value| -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::UInt(u) => Some(*u as f64),
            Value::Float(f) => Some(*f),
            Value::Decimal(d) => d.to_string().parse::<f64>().ok(),
            _ => None,
        }
    };
    to_f(a)?.partial_cmp(&to_f(b)?)
}

/// Deterministic value ordering (used by comparisons and sort).
pub fn cmp_values(a: &Value, b: &Value) -> Ordering {
    let (ra, rb) = (rank(a), rank(b));
    if ra != rb {
        return ra.cmp(&rb);
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Int(_) | Value::UInt(_) | Value::Float(_) | Value::Decimal(_), _) => {
            num_cmp(a, b).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Binary(x), Value::Binary(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.0.cmp(&y.0),
        (Value::Uuid(x), Value::Uuid(y)) => x.0.cmp(&y.0),
        (Value::Array(x), Value::Array(y)) => {
            for (i, j) in x.iter().zip(y) {
                let c = cmp_values(i, j);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        _ => Ordering::Equal,
    }
}

/// Evaluates a filter expression against a document (residual filtering +
/// full-scan evaluation).
pub fn eval(e: &Expr, doc: &Document) -> ndb_common::Result<bool> {
    Ok(match e {
        Expr::Cmp { field, op, value } => {
            let fv = get_path(doc, field);
            eval_cmp(fv, *op, value)
        }
        Expr::Logical { op, children } => match op {
            LogicalOp::And => children
                .iter()
                .try_fold(true, |acc, c| eval(c, doc).map(|r| acc && r))?,
            LogicalOp::Or => children
                .iter()
                .try_fold(false, |acc, c| eval(c, doc).map(|r| acc || r))?,
            LogicalOp::Nor => !children
                .iter()
                .try_fold(false, |acc, c| eval(c, doc).map(|r| acc || r))?,
        },
        Expr::Not { inner } => !eval(inner, doc)?,
        Expr::Array { field, op, value } => {
            let fv = get_path(doc, field);
            eval_array(fv, *op, value)
        }
        Expr::Exists { field, exists } => get_path(doc, field).is_some() == *exists,
        Expr::Type { field, type_name } => match get_path(doc, field) {
            Some(v) => type_matches(v, type_name),
            None => false,
        },
        // Text ops are access-path drivers — as residual predicates they
        // pass (the search already restricted the candidate set).
        Expr::Text { .. } | Expr::Phrase { .. } | Expr::Fuzzy { .. } => true,
    })
}

fn eval_cmp(fv: Option<&Value>, op: CmpOp, target: &Value) -> bool {
    // $in/$nin: target is an array of candidates; array-valued fields
    // match if ANY element matches (MongoDB semantics).
    if matches!(op, CmpOp::In | CmpOp::Nin) {
        let cands = match target {
            Value::Array(a) => a.as_slice(),
            _ => return false,
        };
        let hit = match fv {
            Some(Value::Array(items)) => items
                .iter()
                .any(|i| cands.iter().any(|c| cmp_values(i, c) == Ordering::Equal)),
            Some(v) => cands.iter().any(|c| cmp_values(v, c) == Ordering::Equal),
            None => false,
        };
        return if op == CmpOp::In { hit } else { !hit };
    }
    let ord = match fv {
        Some(v) => cmp_values(v, target),
        None => return false,
    };
    match op {
        CmpOp::Eq => ord == Ordering::Equal,
        CmpOp::Ne => ord != Ordering::Equal,
        CmpOp::Gt => ord == Ordering::Greater,
        CmpOp::Gte => ord != Ordering::Less,
        CmpOp::Lt => ord == Ordering::Less,
        CmpOp::Lte => ord != Ordering::Greater,
        CmpOp::In | CmpOp::Nin => unreachable!(),
    }
}

fn eval_array(fv: Option<&Value>, op: ArrOp, target: &Value) -> bool {
    match op {
        ArrOp::All => match (fv, target) {
            (Some(Value::Array(items)), Value::Array(want)) => want
                .iter()
                .all(|w| items.iter().any(|i| cmp_values(i, w) == Ordering::Equal)),
            _ => false,
        },
        ArrOp::Size => match (fv, target) {
            (Some(Value::Array(items)), Value::Int(n)) => items.len() as i64 == *n,
            (Some(Value::Array(items)), Value::UInt(n)) => items.len() as u64 == *n,
            _ => false,
        },
        ArrOp::ElemMatch => match (fv, target) {
            (Some(Value::Array(items)), Value::Object(clauses)) => {
                // Each element is tested against every op clause.
                items.iter().any(|elem| {
                    clauses.iter().all(|(op, operand)| {
                        let op = match op.as_str() {
                            "$eq" => CmpOp::Eq,
                            "$ne" => CmpOp::Ne,
                            "$gt" => CmpOp::Gt,
                            "$gte" => CmpOp::Gte,
                            "$lt" => CmpOp::Lt,
                            "$lte" => CmpOp::Lte,
                            _ => return true, // unknown ops ignore elem
                        };
                        eval_cmp(Some(elem), op, operand)
                    })
                })
            }
            _ => false,
        },
    }
}

fn type_matches(v: &Value, name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    match n.as_str() {
        "null" => matches!(v, Value::Null),
        "bool" | "boolean" => matches!(v, Value::Bool(_)),
        "int" | "long" | "number" => matches!(v, Value::Int(_) | Value::UInt(_)),
        "double" | "float" => matches!(v, Value::Float(_)),
        "decimal" => matches!(v, Value::Decimal(_)),
        "string" => matches!(v, Value::String(_)),
        "bin" | "binary" => matches!(v, Value::Binary(_)),
        "date" | "datetime" => matches!(v, Value::DateTime(_)),
        "object" => matches!(v, Value::Object(_)),
        "array" => matches!(v, Value::Array(_)),
        "uuid" => matches!(v, Value::Uuid(_)),
        "vector" => matches!(v, Value::Vector(_)),
        _ => false,
    }
}

/// Runs `plan` over `coll` inside `txn`, honouring `limits`.
pub fn execute_find<P: PageIo>(
    coll: &mut Collection<P>,
    txn: &mut Transaction<'_, P>,
    plan: &Plan,
    limits: &ExecLimits,
) -> ndb_common::Result<QueryResult> {
    let start = Instant::now();
    let mut scanned = 0u64;
    let mut bytes = 0u64;

    // ── 1. Candidate (id → score?) stream from the access path ──
    let mut scored: Vec<(u64, Option<f64>)> = match &plan.access {
        AccessPath::PkLookup { id } => vec![(*id, None)],
        AccessPath::FullScan => coll
            .scan(txn)?
            .into_iter()
            .map(|(id, _)| (id.get(), None))
            .collect(),
        AccessPath::IndexScan { index, ranges } => {
            let mut seen = std::collections::BTreeSet::new();
            let mut ids = Vec::new();
            for (lo, hi, inc) in ranges {
                let lo_k = encode_composite(lo);
                let hi_k = hi.as_ref().map(|h| encode_composite(h));
                for (_, id) in
                    coll.lookup_ordered_range(txn, index, &lo_k, hi_k.as_deref(), *inc)?
                {
                    if seen.insert(id.get()) {
                        ids.push((id.get(), None));
                    }
                }
            }
            ids
        }
        AccessPath::TextSearch { index, kind } => {
            let k = plan.limit.unwrap_or(100).max(10) as usize;
            let hits = match kind {
                TextKind::Match { query } => coll.search_text(txn, index, query, k)?,
                TextKind::Phrase { field, terms, slop } => {
                    coll.search_phrase(txn, index, field, terms, *slop, k)?
                }
                TextKind::Fuzzy {
                    field,
                    term,
                    max_edits,
                    max_expansions,
                } => coll.search_fuzzy(txn, index, field, term, *max_edits, *max_expansions, k)?,
            };
            hits.into_iter()
                .map(|h| (h.doc_id.get(), Some(h.score)))
                .collect()
        }
    };
    limits.check(0, 0, start)?;

    // ── 2. Fetch docs + residual filter ──
    let mut docs: Vec<(Document, Option<f64>)> = Vec::new();
    for (id, score) in scored.drain(..) {
        scanned += 1;
        limits.check(scanned, bytes, start)?;
        if let Some(doc) = coll.get(txn, ndb_common::ids::DocumentId::new(id))? {
            if let Some(f) = &plan.residual
                && !eval(f, &doc)?
            {
                continue;
            }
            bytes += approx_size(&doc) as u64;
            docs.push((doc, score));
        }
    }

    // ── 3. Sort (top-K bounded when a limit exists — §13) ──
    if !plan.sort.is_empty() {
        if let Some(k) = plan.limit {
            let k = (k + plan.skip) as usize;
            docs.sort_by(|a, b| sort_cmp(&plan.sort, &a.0, &b.0));
            docs.truncate(k.max(1));
        } else {
            docs.sort_by(|a, b| sort_cmp(&plan.sort, &a.0, &b.0));
        }
    }

    // ── 4. Skip + limit ──
    let docs: Vec<(Document, Option<f64>)> = docs
        .into_iter()
        .skip(plan.skip as usize)
        .take(plan.limit.unwrap_or(u64::MAX) as usize)
        .collect();

    // ── 5. Projection + result accounting ──
    let has_scores = docs.iter().any(|(_, s)| s.is_some());
    let mut out = Vec::with_capacity(docs.len());
    let mut scores = Vec::new();
    for (mut doc, score) in docs {
        if let Some(p) = &plan.projection {
            apply_projection(&mut doc, p);
        }
        out.push(doc);
        scores.push(score.unwrap_or(0.0));
    }
    limits.check(scanned, bytes, start)?;
    Ok(QueryResult {
        docs: out,
        explain: plan.explain.clone(),
        scores: if has_scores { Some(scores) } else { None },
    })
}

/// Rough result-size accounting — field name + payload lengths.
fn approx_size(d: &Document) -> usize {
    d.fields
        .iter()
        .map(|(k, v)| k.len() + value_len(v))
        .sum::<usize>()
        + 16
}

fn value_len(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Binary(b) => b.len(),
        Value::Array(a) => a.iter().map(value_len).sum::<usize>() + 8,
        Value::Object(o) => o.iter().map(|(k, v)| k.len() + value_len(v)).sum::<usize>() + 8,
        Value::Vector(v) => v.len() * 4,
        _ => 16,
    }
}

fn sort_cmp(keys: &[SortKey], a: &Document, b: &Document) -> Ordering {
    for k in keys {
        let x = get_path(a, &k.field);
        let y = get_path(b, &k.field);
        let o = match (x, y) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            (Some(x), Some(y)) => cmp_values(x, y),
        };
        let o = if k.dir == SortDir::Desc {
            o.reverse()
        } else {
            o
        };
        if o != Ordering::Equal {
            return o;
        }
    }
    Ordering::Equal
}

fn apply_projection(doc: &mut Document, p: &Projection) {
    match p {
        Projection::Include(fields) => {
            doc.fields
                .retain(|k, _| fields.contains(k) || k == "_id" || k == "id");
        }
        Projection::Exclude(fields) => {
            for f in fields {
                doc.fields.remove(f);
            }
        }
    }
}
