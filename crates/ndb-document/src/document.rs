//! In-memory document representation.
//!
//! A document is an identifier plus an ordered map of field name → [`Value`].
//! The binary wire format lives in [`crate::codec`].

use crate::value::Value;
use ndb_common::ids::DocumentId;
use std::collections::BTreeMap;

/// A single document.
#[derive(Debug, Clone, PartialEq)]
pub struct Document {
    /// Document identifier assigned by the collection.
    pub id: DocumentId,
    /// Document payload: field name → value, ordered by name.
    pub fields: BTreeMap<String, Value>,
}

impl Document {
    /// Creates a document with the given ID and fields.
    pub fn new(id: DocumentId, fields: BTreeMap<String, Value>) -> Self {
        Self { id, fields }
    }

    /// Creates an empty document with the given ID.
    pub fn empty(id: DocumentId) -> Self {
        Self::new(id, BTreeMap::new())
    }

    /// Builder-style field insertion.
    pub fn with(mut self, name: &str, value: Value) -> Self {
        self.fields.insert(name.to_string(), value);
        self
    }

    /// Returns a field value by name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }
}
