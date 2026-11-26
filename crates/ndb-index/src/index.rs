//! Secondary index entries (spec §8.3).
//!
//! Entry layout in the index's B+Tree:
//!
//! - **non-unique**: key = `encode_composite(field parts…, doc_id)` →
//!   value = `doc_id` (lookups return the value; the doc-id suffix makes
//!   entries unique per document).
//! - **unique**: key = `encode_composite(field parts…)` → value = `doc_id`;
//!   a present entry owned by another document is a uniqueness violation.
//!
//! A missing indexed field encodes as `KeyPart::Bytes(empty)` — a "null"
//! slot that sorts first. `sparse` indexes skip such documents entirely.
//! Values that can't be ordered (Object, Array, Null, Decimal, Vector)
//! are rejected as unindexable at index-definition time.

use crate::meta::IndexDef;
use ndb_btree::key::{KeyPart, encode_composite};
use ndb_common::error::Error;
use ndb_document::{DataType, Value};

/// Maps a document field value to an order-preserving key part.
pub fn value_part(v: &Value) -> ndb_common::Result<KeyPart> {
    Ok(match v {
        Value::Bool(b) => KeyPart::Bool(*b),
        Value::Int(i) => KeyPart::Int(*i),
        Value::UInt(u) => KeyPart::UInt(*u),
        Value::Float(f) => KeyPart::Float(*f),
        Value::String(s) => KeyPart::Str(s.clone()),
        Value::Binary(b) => KeyPart::Bytes(b.clone()),
        Value::Uuid(u) => KeyPart::Bytes(u.0.to_vec()),
        Value::DateTime(d) => KeyPart::Int(d.0),
        other => {
            return Err(Error::InvalidInput(format!(
                "type {:?} is not indexable",
                other.data_type()
            )));
        }
    })
}

/// True if `v` can participate in an index key.
pub fn indexable(v: &Value) -> bool {
    matches!(
        v.data_type(),
        DataType::Bool
            | DataType::Int
            | DataType::UInt
            | DataType::Float
            | DataType::String
            | DataType::Binary
            | DataType::Uuid
            | DataType::DateTime
    )
}

/// Builds `(key, value)` for one document's index entry, or `None` when a
/// sparse index skips a document missing an indexed field.
pub fn entry(
    def: &IndexDef,
    fields: &std::collections::BTreeMap<String, Value>,
    doc_id: u64,
) -> ndb_common::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut parts = Vec::with_capacity(def.fields.len() + 1);
    for f in &def.fields {
        match fields.get(f) {
            Some(v) if indexable(v) => parts.push(value_part(v)?),
            Some(v) => {
                return Err(Error::InvalidInput(format!(
                    "field '{f}' has non-indexable type {:?}",
                    v.data_type()
                )));
            }
            None => {
                if def.sparse {
                    return Ok(None);
                }
                parts.push(KeyPart::Bytes(Vec::new())); // null slot sorts first
            }
        }
    }
    let id_val = doc_id.to_le_bytes().to_vec();
    if def.unique {
        Ok(Some((encode_composite(&parts), id_val)))
    } else {
        parts.push(KeyPart::UInt(doc_id));
        Ok(Some((encode_composite(&parts), id_val)))
    }
}
