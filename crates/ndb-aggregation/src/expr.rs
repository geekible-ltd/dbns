//! Aggregation expressions (§12.9 initial set).

use ndb_common::error::Error;
use ndb_document::{Document, Value};
use ndb_query_ast::AggExpr;

/// Evaluates an `AggExpr` against a document.
pub fn eval(e: &AggExpr, doc: &Document) -> ndb_common::Result<Value> {
    Ok(match e {
        AggExpr::FieldPath(p) => get_path(doc, p).cloned().unwrap_or(Value::Null),
        AggExpr::Lit(v) => v.clone(),
        AggExpr::Concat(parts) => {
            let mut s = String::new();
            for p in parts {
                match eval(p, doc)? {
                    Value::String(t) => s.push_str(&t),
                    Value::Null => {} // $concat skips nulls (MongoDB: null → null; we take lenient)
                    other => s.push_str(&to_string(&other)),
                }
            }
            Value::String(s)
        }
        AggExpr::ToString(inner) => Value::String(to_string(&eval(inner, doc)?)),
        AggExpr::IfNull { expr, fallback } => {
            let v = eval(expr, doc)?;
            if matches!(v, Value::Null) {
                eval(fallback, doc)?
            } else {
                v
            }
        }
    })
}

/// Dotted-path field access.
pub fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Value> {
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

/// Value → display string for $toString/$concat.
pub fn to_string(v: &Value) -> String {
    match v {
        Value::Null => "null".into(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Decimal(d) => d.to_string(),
        Value::String(s) => s.clone(),
        Value::Binary(b) => format!("<{} bytes>", b.len()),
        Value::DateTime(d) => d.0.to_string(),
        Value::Uuid(u) => u.0.iter().map(|b| format!("{b:02x}")).collect(),
        Value::Array(a) => format!("[{} items]", a.len()),
        Value::Object(o) => format!("<{} fields>", o.len()),
        Value::Vector(v) => format!("<vector:{}>", v.len()),
    }
}

/// Numeric coercion for arithmetic accumulators.
pub fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::UInt(u) => Some(*u as f64),
        Value::Float(f) => Some(*f),
        Value::Decimal(d) => d.to_string().parse().ok(),
        _ => None,
    }
}

/// Errors on totally unexpected input (kept for future typed eval).
pub fn type_err(what: &str) -> ndb_common::Result<Value> {
    Err(Error::InvalidInput(format!("agg: {what}")))
}
