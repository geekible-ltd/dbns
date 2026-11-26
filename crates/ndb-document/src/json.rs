//! JSON ↔ `Value`/`Document` conversion — the wire format for REST/gRPC
//! (spec §15). Field names are stored as names (not dictionary ids) so JSON
//! is self-describing.

use serde_json::{Map, Value as J};

use crate::document::Document;
use crate::value::Value;
use ndb_common::error::Error;

/// Value → serde_json. Lossy only for types JSON lacks: Decimal → string,
/// DateTime → ISO-8601 millis, Uuid → string, Binary → base64-ish hex.
pub fn value_to_json(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::from(*i),
        Value::UInt(u) => J::from(*u),
        Value::Float(f) => J::from(*f),
        Value::Decimal(d) => J::String(d.to_string()),
        Value::String(s) => J::String(s.clone()),
        Value::Binary(b) => J::String(hex(b)),
        Value::DateTime(dt) => J::from(dt.0),
        Value::Object(o) => {
            let mut m = Map::new();
            for (k, v) in o {
                m.insert(k.clone(), value_to_json(v));
            }
            J::Object(m)
        }
        Value::Array(a) => J::Array(a.iter().map(value_to_json).collect()),
        Value::Uuid(u) => J::String(u.to_string()),
        Value::Vector(v) => J::Array(v.iter().map(|f| J::from(*f)).collect()),
    }
}

/// Document → `{ "_id": n, fields… }`.
pub fn doc_to_json(d: &Document) -> J {
    let mut m = Map::new();
    m.insert("_id".into(), J::from(d.id.get()));
    for (k, v) in &d.fields {
        m.insert(k.clone(), value_to_json(v));
    }
    J::Object(m)
}

/// `{ "_id": n, fields… }` → Document; `_id` of 0 means unassigned.
pub fn doc_from_json(j: &J) -> ndb_common::Result<Document> {
    let o = j
        .as_object()
        .ok_or_else(|| Error::InvalidInput("document must be a JSON object".into()))?;
    let id = o.get("_id").and_then(J::as_u64).unwrap_or(0);
    let mut d = Document::empty(ndb_common::ids::DocumentId::new(id));
    for (k, v) in o {
        if k == "_id" {
            continue;
        }
        d.fields.insert(k.clone(), json_to_value(v)?);
    }
    Ok(d)
}

/// serde_json → `Value`. Vectors arrive as `{"$vector": [...]}` (plain
/// numeric arrays stay `Value::Array` — ambiguity is resolved by schema).
pub fn json_to_value(j: &J) -> ndb_common::Result<Value> {
    Ok(match j {
        J::Null => Value::Null,
        J::Bool(b) => Value::Bool(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(u) = n.as_u64() {
                Value::UInt(u)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        J::String(s) => Value::String(s.clone()),
        J::Array(a) => Value::Array(
            a.iter()
                .map(json_to_value)
                .collect::<ndb_common::Result<Vec<_>>>()?,
        ),
        J::Object(o) => {
            if let Some(v) = o.get("$vector") {
                let arr = v
                    .as_array()
                    .ok_or_else(|| Error::InvalidInput("$vector must be an array".into()))?;
                let mut out = Vec::with_capacity(arr.len());
                for e in arr {
                    out.push(e.as_f64().ok_or_else(|| {
                        Error::InvalidInput("$vector entries must be numbers".into())
                    })? as f32);
                }
                return Ok(Value::Vector(out));
            }
            let mut m = crate::value::Object::new();
            for (k, v) in o {
                m.insert(k.clone(), json_to_value(v)?);
            }
            Value::Object(m)
        }
    })
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
