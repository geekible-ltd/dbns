//! Aggregation framework (spec §12.9).
//!
//! `run_pipeline(coll, txn, pipeline, limits)` streams documents through
//! the stage list:
//!
//! ```text
//! $match → $unwind → $project/$addFields → $group → $sort → $skip/$limit
//!   → $count
//! ```
//!
//! **Bounded memory + disk spilling** (§12.9): `$group` accumulates in a
//! hash map until `group_bytes_bound`, then merges partial accumulators
//! into a scratch B+Tree (`spill` module) — groups re-encountered later
//! are merged into their spilled state, so memory stays bounded for
//! high-cardinality groupings.

pub mod expr;
pub mod group;

use ndb_common::error::Error;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, Value};
use ndb_index::Collection;
use ndb_page::manager::PageIo;
use ndb_query_ast::*;
use ndb_query_executor::{ExecLimits, eval};
use ndb_transaction::Transaction;
use std::collections::BTreeMap;
use std::time::Instant;

/// Bounds specific to aggregation — extends `ExecLimits`.
pub struct AggLimits {
    pub exec: ExecLimits,
    /// Max bytes the in-memory group map may hold before spilling to the
    /// scratch tree (§12.9 bounded memory).
    pub group_bytes_bound: usize,
}

impl Default for AggLimits {
    fn default() -> Self {
        Self {
            exec: ExecLimits::default(),
            group_bytes_bound: 8 << 20,
        }
    }
}

/// Executes `pipeline` and returns result documents. `pool` backs the
/// scratch tree a spilling `$group` needs.
pub fn run_pipeline<P: PageIo>(
    coll: &mut Collection<P>,
    txn: &mut Transaction<'_, P>,
    pool: &std::sync::Arc<ndb_buffer::BufferPool<P>>,
    pipeline: &Pipeline,
    limits: &AggLimits,
) -> ndb_common::Result<Vec<Document>> {
    let start = Instant::now();
    let mut docs = coll
        .scan(txn)?
        .into_iter()
        .map(|(_, d)| d)
        .collect::<Vec<_>>();
    let mut scanned = docs.len() as u64;

    for stage in &pipeline.stages {
        limits.exec.check(scanned, 0, start)?;
        docs = match stage {
            Stage::Match { filter } => docs
                .into_iter()
                .filter_map(|d| eval(filter, &d).ok().filter(|ok| *ok).map(|_| d))
                .collect(),
            Stage::Unwind { field } => unwind(docs, field),
            Stage::Project { spec } => docs
                .iter()
                .map(|d| project(d, spec))
                .collect::<Result<_, _>>()?,
            Stage::AddFields { fields } => docs
                .iter()
                .map(|d| add_fields(d, fields))
                .collect::<Result<_, _>>()?,
            Stage::Group { key, accumulators } => {
                group::run_group(txn, pool, docs, key, accumulators, limits)?
            }
            Stage::Sort { keys } => {
                let mut d = docs;
                d.sort_by(|a, b| {
                    for k in keys {
                        let o = sort_key_cmp(a, b, &k.field);
                        let o = if k.dir == SortDir::Desc {
                            o.reverse()
                        } else {
                            o
                        };
                        if o != std::cmp::Ordering::Equal {
                            return o;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                d
            }
            Stage::Skip { n } => docs.into_iter().skip(*n as usize).collect(),
            Stage::Limit { n } => docs.into_iter().take(*n as usize).collect(),
            Stage::Count { out_field } => {
                let mut d = Document::empty(DocumentId::new(0));
                d.fields
                    .insert(out_field.clone(), Value::Int(docs.len() as i64));
                vec![d]
            }
        };
        scanned += docs.len() as u64;
    }
    Ok(docs)
}

fn sort_key_cmp(a: &Document, b: &Document, field: &str) -> std::cmp::Ordering {
    use ndb_query_executor::cmp_values;
    match (a.fields.get(field), b.fields.get(field)) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => cmp_values(x, y),
    }
}

/// `$unwind`: each array element becomes its own document (field replaced
/// by the element); non-array/absent fields drop the doc.
fn unwind(docs: Vec<Document>, field: &str) -> Vec<Document> {
    let mut out = Vec::new();
    for d in docs {
        match d.fields.get(field) {
            Some(Value::Array(items)) => {
                for item in items {
                    let mut nd = d.clone();
                    nd.fields.insert(field.to_string(), item.clone());
                    out.push(nd);
                }
            }
            _ => continue,
        }
    }
    out
}

fn project(d: &Document, spec: &[ProjectItem]) -> ndb_common::Result<Document> {
    // Pure exclusion list → copy-minus (MongoDB semantics).
    let all_exclude = spec.iter().all(|i| matches!(i, ProjectItem::Exclude(_)));
    let mut nd = Document::empty(d.id);
    if all_exclude {
        nd.fields = d.fields.clone();
    }
    for item in spec {
        match item {
            ProjectItem::Field(f) => {
                if let Some(v) = d.fields.get(f) {
                    nd.fields.insert(f.clone(), v.clone());
                }
            }
            ProjectItem::Exclude(f) => {
                nd.fields.remove(f);
            }
            ProjectItem::Computed { out, expr } => {
                nd.fields.insert(out.clone(), expr::eval(expr, d)?);
            }
        }
    }
    Ok(nd)
}

fn add_fields(d: &Document, fields: &[(String, AggExpr)]) -> ndb_common::Result<Document> {
    let mut nd = d.clone();
    for (name, e) in fields {
        nd.fields.insert(name.clone(), expr::eval(e, d)?);
    }
    Ok(nd)
}

/// Group-key identity helper — serialises a `Value` deterministically
/// (reused by `group` for both map keys and spill-tree keys).
pub fn key_bytes(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    tag_value(v, &mut out);
    out
}

fn tag_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::Null => out.push(0),
        Value::Bool(b) => out.extend_from_slice(&[1, *b as u8]),
        Value::Int(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::UInt(u) => {
            out.push(3);
            out.extend_from_slice(&u.to_be_bytes());
        }
        Value::Float(f) => {
            out.push(4);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Decimal(d) => {
            out.push(5);
            out.extend_from_slice(d.to_string().as_bytes());
        }
        Value::String(s) => {
            out.push(6);
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Binary(b) => {
            out.push(7);
            out.extend_from_slice(&(b.len() as u32).to_be_bytes());
            out.extend_from_slice(b);
        }
        Value::DateTime(d) => {
            out.push(8);
            out.extend_from_slice(&d.0.to_be_bytes());
        }
        Value::Uuid(u) => {
            out.push(9);
            out.extend_from_slice(&u.0);
        }
        Value::Array(items) => {
            out.push(10);
            out.extend_from_slice(&(items.len() as u32).to_be_bytes());
            for i in items {
                tag_value(i, out);
            }
        }
        Value::Object(o) => {
            out.push(11);
            out.extend_from_slice(&(o.len() as u32).to_be_bytes());
            for (k, v) in o {
                out.extend_from_slice(&(k.len() as u16).to_be_bytes());
                out.extend_from_slice(k.as_bytes());
                tag_value(v, out);
            }
        }
        Value::Vector(v) => {
            out.push(12);
            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
            for f in v {
                out.extend_from_slice(&f.to_bits().to_be_bytes());
            }
        }
    }
}

/// Decode a key_bytes buffer back into a `Value` (for $group output docs).
pub fn value_from_key(mut b: &[u8]) -> ndb_common::Result<Value> {
    fn rd(b: &mut &[u8]) -> ndb_common::Result<Value> {
        let tag = *b
            .first()
            .ok_or_else(|| Error::Corruption("gkey: eof".into()))?;
        *b = &b[1..];
        Ok(match tag {
            0 => Value::Null,
            1 => Value::Bool(take::<1>(b)?[0] != 0),
            2 => Value::Int(i64::from_be_bytes(take::<8>(b)?)),
            3 => Value::UInt(u64::from_be_bytes(take::<8>(b)?)),
            4 => Value::Float(f64::from_bits(u64::from_be_bytes(take::<8>(b)?))),
            6 => {
                let n = u32::from_be_bytes(take::<4>(b)?) as usize;
                Value::String(
                    String::from_utf8(take_n(b, n)?.to_vec())
                        .map_err(|_| Error::Corruption("gkey: utf8".into()))?,
                )
            }
            8 => Value::DateTime(ndb_document::DateTime(i64::from_be_bytes(take::<8>(b)?))),
            10 => {
                let n = u32::from_be_bytes(take::<4>(b)?) as usize;
                let mut items = Vec::with_capacity(n);
                for _ in 0..n {
                    items.push(rd(b)?);
                }
                Value::Array(items)
            }
            11 => {
                let n = u32::from_be_bytes(take::<4>(b)?) as usize;
                let mut m = BTreeMap::new();
                for _ in 0..n {
                    let kl = u16::from_be_bytes(take::<2>(b)?) as usize;
                    let k = String::from_utf8(take_n(b, kl)?.to_vec())
                        .map_err(|_| Error::Corruption("gkey: utf8".into()))?;
                    m.insert(k, rd(b)?);
                }
                Value::Object(m)
            }
            t => return Err(Error::Corruption(format!("gkey: bad tag {t}"))),
        })
    }
    fn take<const N: usize>(b: &mut &[u8]) -> ndb_common::Result<[u8; N]> {
        let s = take_n(b, N)?;
        Ok(s.try_into().unwrap())
    }
    fn take_n<'a>(b: &mut &'a [u8], n: usize) -> ndb_common::Result<&'a [u8]> {
        if b.len() < n {
            return Err(Error::Corruption("gkey: short".into()));
        }
        let (h, t) = b.split_at(n);
        *b = t;
        Ok(h)
    }
    rd(&mut b)
}

// ── query facade ────────────────────────────────────────────────────────

use ndb_index::Engine;
use ndb_query_executor::{ExecLimits as FindLimits, QueryResult, execute_find};
use ndb_query_optimizer::Optimizer;
use ndb_query_planner::{Plan, PlanStats};

/// One-call query service: parse JSON → plan → optimise → execute
/// (the §12.8 pipeline end-to-end). Lives in the topmost query crate —
/// dependencies flow downward: aggregation → executor → index.
pub struct QueryEngine {
    opt: Optimizer,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryEngine {
    pub fn new() -> Self {
        Self {
            opt: Optimizer::new(),
        }
    }

    fn stats_for<P: PageIo>(coll: &Collection<P>) -> PlanStats {
        PlanStats {
            doc_count: coll.scan_count_hint(),
            indexes: coll
                .meta()
                .indexes
                .iter()
                .map(|d| ndb_query_planner::IndexView {
                    name: d.name.clone(),
                    fields: d.fields.clone(),
                    unique: d.unique,
                })
                .collect(),
            text_indexes: coll
                .meta()
                .text_indexes
                .iter()
                .map(|d| ndb_query_planner::TextIndexView {
                    name: d.name.clone(),
                    fields: d.fields.iter().map(|f| f.field.clone()).collect(),
                })
                .collect(),
        }
    }

    /// Plans `find_json` — the EXPLAIN path (§12.8): the chosen plan plus
    /// its narration, without executing.
    pub fn explain<P: PageIo>(
        &self,
        engine: &Engine<P>,
        txn: &mut Transaction<'_, P>,
        find_json: &str,
    ) -> ndb_common::Result<Plan> {
        let q = ndb_query_parser::parse_find(find_json)?;
        let coll = engine
            .collection(txn, &q.database, &q.collection)?
            .ok_or_else(|| {
                Error::InvalidInput(format!("collection '{}' not found", q.collection))
            })?;
        self.opt.optimize(&q, &Self::stats_for(&coll))
    }

    /// Runs a find query end-to-end.
    pub fn find<P: PageIo>(
        &self,
        engine: &Engine<P>,
        txn: &mut Transaction<'_, P>,
        find_json: &str,
        limits: &FindLimits,
    ) -> ndb_common::Result<QueryResult> {
        let q = ndb_query_parser::parse_find(find_json)?;
        let mut coll = engine
            .collection(txn, &q.database, &q.collection)?
            .ok_or_else(|| {
                Error::InvalidInput(format!("collection '{}' not found", q.collection))
            })?;
        let plan = self.opt.optimize(&q, &Self::stats_for(&coll))?;
        execute_find(&mut coll, txn, &plan, limits)
    }

    /// Runs an aggregation pipeline (§12.9).
    pub fn aggregate<P: PageIo>(
        &self,
        engine: &Engine<P>,
        txn: &mut Transaction<'_, P>,
        pool: &std::sync::Arc<ndb_buffer::BufferPool<P>>,
        pipeline_json: &str,
        limits: &AggLimits,
    ) -> ndb_common::Result<Vec<Document>> {
        let p = ndb_query_parser::parse_pipeline(pipeline_json)?;
        let mut coll = engine
            .collection(txn, &p.database, &p.collection)?
            .ok_or_else(|| {
                Error::InvalidInput(format!("collection '{}' not found", p.collection))
            })?;
        run_pipeline(&mut coll, txn, pool, &p, limits)
    }
}
