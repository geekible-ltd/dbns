//! Schema definitions and validation (spec §6).
//!
//! Schemas are versioned. Validation checks top-level fields only; nested
//! object validation arrives with the query engine.

use crate::document::Document;
use crate::value::DataType;
use ndb_common::error::Error;
use std::collections::BTreeMap;

/// A single field's schema constraint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldSchema {
    /// Required data type, or `None` to accept any type.
    pub data_type: Option<DataType>,
    /// Whether the field must be present (with a non-`Null` value).
    pub required: bool,
}

/// Per-collection schema, versioned (spec §6.2 — documents record the schema
/// version they were written against).
#[derive(Debug, Clone)]
pub struct Schema {
    pub version: u32,
    /// When true, documents containing fields not listed in the schema are
    /// rejected. When false (flexible — the default), undeclared fields are
    /// accepted and interned into the field dictionary.
    pub strict: bool,
    fields: BTreeMap<String, FieldSchema>,
}

impl Schema {
    /// Creates an empty flexible schema at version 1.
    pub fn new() -> Self {
        Self {
            version: 1,
            strict: false,
            fields: BTreeMap::new(),
        }
    }

    /// Adds or replaces a field constraint, keeping the schema version.
    /// Callers evolve constraints by bumping [`Schema::version`] themselves.
    pub fn field(mut self, name: &str, data_type: DataType, required: bool) -> Self {
        self.fields.insert(
            name.to_string(),
            FieldSchema {
                data_type: Some(data_type),
                required,
            },
        );
        self
    }

    /// Adds a field constraint with no fixed type.
    pub fn field_any_type(mut self, name: &str, required: bool) -> Self {
        self.fields.insert(
            name.to_string(),
            FieldSchema {
                data_type: None,
                required,
            },
        );
        self
    }

    /// Constraint for a field, if declared.
    pub fn constraint(&self, name: &str) -> Option<&FieldSchema> {
        self.fields.get(name)
    }

    /// Declared field names.
    pub fn fields(&self) -> impl Iterator<Item = &String> {
        self.fields.keys()
    }

    /// Validates a document against the schema, aggregating every violation.
    pub fn validate(&self, doc: &Document) -> ndb_common::Result<()> {
        let mut problems = Vec::new();

        for (name, constraint) in &self.fields {
            match doc.fields.get(name) {
                None => {
                    if constraint.required {
                        problems.push(format!("required field '{name}' is missing"));
                    }
                }
                Some(crate::value::Value::Null) => {
                    if constraint.required {
                        problems.push(format!("required field '{name}' is null"));
                    }
                }
                Some(value) => {
                    if let Some(expected) = constraint.data_type
                        && value.data_type() != expected
                    {
                        problems.push(format!(
                            "field '{name}' has type {:?}, expected {expected:?}",
                            value.data_type()
                        ));
                    }
                }
            }
        }

        if self.strict {
            for name in doc.fields.keys() {
                if !self.fields.contains_key(name) {
                    problems.push(format!("field '{name}' is not declared in the schema"));
                }
            }
        }

        if problems.is_empty() {
            Ok(())
        } else {
            Err(Error::InvalidInput(format!(
                "schema v{} violation(s):\n  - {}",
                self.version,
                problems.join("\n  - ")
            )))
        }
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;
    use ndb_common::ids::DocumentId;

    fn doc(fields: Vec<(&str, Value)>) -> Document {
        Document::new(
            DocumentId::new(1),
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    #[test]
    fn flexible_schema_accepts_undeclared_fields() {
        let schema = Schema::new().field("name", DataType::String, true);
        let d = doc(vec![
            ("name", Value::String("a".into())),
            ("extra", Value::Int(1)),
        ]);
        assert!(schema.validate(&d).is_ok());
    }

    #[test]
    fn strict_schema_rejects_undeclared_fields() {
        let mut schema = Schema::new().field("name", DataType::String, true);
        schema.strict = true;
        let d = doc(vec![
            ("name", Value::String("a".into())),
            ("extra", Value::Int(1)),
        ]);
        let err = schema.validate(&d).unwrap_err().to_string();
        assert!(err.contains("not declared"));
    }

    #[test]
    fn reports_missing_required_and_type_mismatch() {
        let schema =
            Schema::new()
                .field("name", DataType::String, true)
                .field("price", DataType::Int, true);
        let d = doc(vec![("price", Value::String("oops".into()))]);
        let err = schema.validate(&d).unwrap_err().to_string();
        assert!(err.contains("'name' is missing"));
        assert!(err.contains("'price' has type String"));
    }
}
