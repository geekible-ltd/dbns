//! `$group` with bounded memory + B+Tree disk spilling (§12.9).
//!
//! Accumulators live in a `BTreeMap<key_bytes, AccRow>` until
//! `group_bytes_bound`, then merge into a scratch B+Tree (allocated in the
//! caller's pool, mutated inside `txn` — journaled like everything else).
//! Later docs merge onto spilled state, so memory stays bounded for
//! high-cardinality groupings; the spill tree is orphaned afterwards (same
//! documented page-leak class as dropped indexes).

use crate::{AggLimits, expr, key_bytes, value_from_key};
use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::ids::DocumentId;
use ndb_document::{Document, Value};
use ndb_page::manager::PageIo;
use ndb_query_ast::{AccOp, Accumulator, AggExpr};
use ndb_transaction::Transaction;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One accumulator's partial state — commutative/associative so partial
/// rows merge freely.
#[derive(Clone)]
enum Acc {
    Sum(f64),
    Avg { sum: f64, n: u64 },
    Min(Value),
    Max(Value),
    First(Option<Value>),
    Last(Option<Value>),
}

impl Acc {
    fn new(op: AccOp) -> Self {
        match op {
            AccOp::Sum => Acc::Sum(0.0),
            AccOp::Avg => Acc::Avg { sum: 0.0, n: 0 },
            AccOp::Min => Acc::Min(Value::Null),
            AccOp::Max => Acc::Max(Value::Null),
            AccOp::First => Acc::First(None),
            AccOp::Last => Acc::Last(None),
        }
    }

    fn feed(&mut self, v: &Value) {
        match self {
            Acc::Sum(s) => *s += expr::as_f64(v).unwrap_or(0.0),
            Acc::Avg { sum, n } => {
                if let Some(f) = expr::as_f64(v) {
                    *sum += f;
                    *n += 1;
                }
            }
            Acc::Min(m) => {
                if matches!(m, Value::Null)
                    || ndb_query_executor::cmp_values(v, m) == std::cmp::Ordering::Less
                {
                    *m = v.clone();
                }
            }
            Acc::Max(m) => {
                if matches!(m, Value::Null)
                    || ndb_query_executor::cmp_values(v, m) == std::cmp::Ordering::Greater
                {
                    *m = v.clone();
                }
            }
            Acc::First(f) => {
                if f.is_none() {
                    *f = Some(v.clone());
                }
            }
            Acc::Last(l) => *l = Some(v.clone()),
        }
    }

    fn merge(&mut self, other: &Acc) {
        match (self, other) {
            (Acc::Sum(a), Acc::Sum(b)) => *a += *b,
            (Acc::Avg { sum: a, n: an }, Acc::Avg { sum: b, n: bn }) => {
                *a += *b;
                *an += *bn;
            }
            (Acc::Min(a), Acc::Min(b)) => {
                if matches!(a, Value::Null)
                    || (!matches!(b, Value::Null)
                        && ndb_query_executor::cmp_values(b, a) == std::cmp::Ordering::Less)
                {
                    *a = b.clone();
                }
            }
            (Acc::Max(a), Acc::Max(b)) => {
                if matches!(a, Value::Null)
                    || (!matches!(b, Value::Null)
                        && ndb_query_executor::cmp_values(b, a) == std::cmp::Ordering::Greater)
                {
                    *a = b.clone();
                }
            }
            (Acc::First(a), Acc::First(b)) => {
                if a.is_none() {
                    *a = b.clone();
                }
            }
            (Acc::Last(a), Acc::Last(b)) => {
                if b.is_some() {
                    *a = b.clone();
                }
            }
            _ => {}
        }
    }

    fn finish(&self) -> Value {
        match self {
            Acc::Sum(s) => Value::Float(*s),
            Acc::Avg { sum, n } => Value::Float(if *n > 0 { *sum / *n as f64 } else { 0.0 }),
            Acc::Min(v) | Acc::Max(v) => v.clone(),
            Acc::First(v) | Acc::Last(v) => v.clone().unwrap_or(Value::Null),
        }
    }

    /// Serialised partial state for the spill tree. Tag 0/1 carry the
    /// op implicitly; tag 2 wraps a `key_bytes`-encoded `Value` and the
    /// op is recovered from the accumulator definition on decode.
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Acc::Sum(s) => {
                out.push(0);
                out.extend_from_slice(&s.to_le_bytes());
            }
            Acc::Avg { sum, n } => {
                out.push(1);
                out.extend_from_slice(&sum.to_le_bytes());
                out.extend_from_slice(&n.to_le_bytes());
            }
            other => {
                out.push(2);
                let v = match other {
                    Acc::Min(v) | Acc::Max(v) => v.clone(),
                    Acc::First(v) | Acc::Last(v) => v.clone().unwrap_or(Value::Null),
                    _ => unreachable!(),
                };
                let b = crate::key_bytes(&v);
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(&b);
            }
        }
        out
    }

    fn decode(op: AccOp, buf: &[u8]) -> Acc {
        match buf.first().copied() {
            Some(0) if buf.len() >= 9 => {
                Acc::Sum(f64::from_le_bytes(buf[1..9].try_into().unwrap()))
            }
            Some(1) if buf.len() >= 17 => Acc::Avg {
                sum: f64::from_le_bytes(buf[1..9].try_into().unwrap()),
                n: u64::from_le_bytes(buf[9..17].try_into().unwrap()),
            },
            Some(2) if buf.len() >= 5 => {
                let n = u32::from_le_bytes(buf[1..5].try_into().unwrap()) as usize;
                let v = value_from_key(&buf[5..(5 + n).min(buf.len())]).unwrap_or(Value::Null);
                match op {
                    AccOp::Min => Acc::Min(v),
                    AccOp::Max => Acc::Max(v),
                    AccOp::First => Acc::First(if matches!(v, Value::Null) {
                        None
                    } else {
                        Some(v)
                    }),
                    AccOp::Last => Acc::Last(if matches!(v, Value::Null) {
                        None
                    } else {
                        Some(v)
                    }),
                    _ => Acc::new(op),
                }
            }
            _ => Acc::new(op),
        }
    }
}

type AccRow = Vec<Acc>;

fn encode_row(row: &AccRow) -> Vec<u8> {
    let mut out = Vec::new();
    for a in row {
        let b = a.encode();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(&b);
    }
    out
}

fn decode_row(defs: &[Accumulator], buf: &[u8]) -> AccRow {
    let mut out = Vec::with_capacity(defs.len());
    let mut pos = 0usize;
    for d in defs {
        if pos + 4 > buf.len() {
            break;
        }
        let n = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + n > buf.len() {
            break;
        }
        out.push(Acc::decode(d.op, &buf[pos..pos + n]));
        pos += n;
    }
    out
}

/// Flushes in-memory rows into the spill tree, merging into any prior
/// spilled row for the same key.
fn flush<P: PageIo>(
    defs: &[Accumulator],
    mem: &mut BTreeMap<Vec<u8>, AccRow>,
    txn: &mut Transaction<'_, P>,
    spill: &Arc<BTree<P>>,
) -> ndb_common::Result<()> {
    for (k, mut row) in std::mem::take(mem) {
        if let Some(old) = txn.get_in(spill, &k)? {
            let mut old = decode_row(defs, &old);
            for (o, n) in old.iter_mut().zip(&row) {
                o.merge(n);
            }
            row = old;
        }
        txn.insert_in(spill, &k, &encode_row(&row))?;
    }
    Ok(())
}

/// Runs `$group` over `docs` with bounded memory.
pub fn run_group<P: PageIo>(
    txn: &mut Transaction<'_, P>,
    pool: &Arc<BufferPool<P>>,
    docs: Vec<Document>,
    key: &AggExpr,
    accumulators: &[Accumulator],
    limits: &AggLimits,
) -> ndb_common::Result<Vec<Document>> {
    let mut mem: BTreeMap<Vec<u8>, AccRow> = BTreeMap::new();
    let mut mem_bytes = 0usize;
    let spill: Arc<BTree<P>> = Arc::new(BTree::create(Arc::clone(pool))?);
    let mut spilled = false;

    for d in &docs {
        let kv = expr::eval(key, d)?;
        let kb = key_bytes(&kv);
        let row = mem
            .entry(kb.clone())
            .or_insert_with(|| accumulators.iter().map(|a| Acc::new(a.op)).collect());
        for (acc, def) in row.iter_mut().zip(accumulators) {
            let v = expr::eval(&def.arg, d)?;
            mem_bytes += crate::key_bytes(&v).len();
            acc.feed(&v);
        }
        mem_bytes += kb.len() + 32; // key + row overhead
        if mem_bytes > limits.group_bytes_bound {
            flush(accumulators, &mut mem, txn, &spill)?;
            mem_bytes = 0;
            spilled = true;
        }
    }

    let mut final_rows: BTreeMap<Vec<u8>, AccRow> = mem;
    if spilled {
        for (k, v) in txn.scan_in(&spill)? {
            let row = decode_row(accumulators, &v);
            final_rows
                .entry(k)
                .and_modify(|r| {
                    for (a, b) in r.iter_mut().zip(&row) {
                        a.merge(b);
                    }
                })
                .or_insert(row);
        }
    }

    let mut out = Vec::with_capacity(final_rows.len());
    for (kb, row) in final_rows {
        let mut doc = Document::empty(DocumentId::new(0));
        doc.fields.insert("_id".into(), value_from_key(&kb)?);
        for (acc, def) in row.iter().zip(accumulators) {
            doc.fields.insert(def.out.clone(), acc.finish());
        }
        out.push(doc);
    }
    Ok(out)
}
