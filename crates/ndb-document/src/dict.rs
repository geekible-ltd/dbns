//! Per-collection field dictionary (spec §6.4).
//!
//! Field identifiers are assigned append-only and remain stable for the
//! lifetime of their schema version. The dictionary is itself a persisted,
//! versioned structure.

use crate::value::DataType;
use ndb_common::error::Error;
use ndb_common::ids::FieldId;
use ndb_common::version::FormatVersion;
use std::collections::BTreeMap;

const DICT_MAGIC: u32 = 0x4E44_4401; // "NDD" v1
const HEADER_SIZE: usize = 12;
const TYPE_NONE: u8 = 0xFF;

/// A single dictionary entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub id: FieldId,
    pub name: String,
    /// Declared type from the collection schema, if one was registered.
    /// `None` means the field was interned from a schemaless document and
    /// carries no declared type.
    pub declared_type: Option<DataType>,
}

/// Maps field names to stable numeric identifiers and back.
#[derive(Debug, Clone, Default)]
pub struct FieldDictionary {
    by_name: BTreeMap<String, FieldId>,
    by_id: BTreeMap<FieldId, FieldDef>,
    next_id: u32,
}

impl FieldDictionary {
    /// Creates an empty dictionary. Field IDs are assigned starting at 1;
    /// ID 0 is reserved as "no field".
    pub fn new() -> Self {
        Self {
            by_name: BTreeMap::new(),
            by_id: BTreeMap::new(),
            next_id: 1,
        }
    }

    /// Number of registered fields.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether the dictionary is empty.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Looks up `name` without interning — `None` for unseen fields.
    pub fn id(&self, name: &str) -> Option<FieldId> {
        self.by_name.get(name).copied()
    }

    /// Returns the ID for `name`, assigning a new one if the field has not
    /// been seen before. Used when encoding schemaless documents.
    pub fn intern(&mut self, name: &str) -> FieldId {
        if let Some(id) = self.by_name.get(name) {
            return *id;
        }
        let id = FieldId::new(self.next_id);
        self.next_id += 1;
        let def = FieldDef {
            id,
            name: name.to_string(),
            declared_type: None,
        };
        self.by_name.insert(name.to_string(), id);
        self.by_id.insert(id, def);
        id
    }

    /// Declares a field with a schema type. If the field already exists with
    /// a different declared type this is an error: schema evolution must not
    /// silently reinterpret existing field identifiers (spec §6.4).
    pub fn declare(&mut self, name: &str, data_type: DataType) -> ndb_common::Result<FieldId> {
        if let Some(existing) = self.by_name.get(name).and_then(|id| self.by_id.get(id)) {
            match existing.declared_type {
                Some(t) if t != data_type => {
                    return Err(Error::InvalidInput(format!(
                        "field '{name}' already declared as {t:?}; changing its type \
                         requires an explicit schema migration"
                    )));
                }
                Some(_) => return Ok(existing.id),
                None => {}
            }
        }
        let id = self.intern(name);
        self.by_id.get_mut(&id).unwrap().declared_type = Some(data_type);
        Ok(id)
    }

    /// Looks up a field ID by name.
    pub fn field_id(&self, name: &str) -> Option<FieldId> {
        self.by_name.get(name).copied()
    }

    /// Looks up a field definition by ID.
    pub fn field(&self, id: FieldId) -> Option<&FieldDef> {
        self.by_id.get(&id)
    }

    /// Iterates over all definitions in ID order.
    pub fn iter(&self) -> impl Iterator<Item = &FieldDef> {
        self.by_id.values()
    }

    /// Encodes the dictionary to its versioned binary form.
    ///
    /// Layout: `magic u32 | format_version u16 | reserved u16 | count u32`
    /// (12 bytes), then per entry `id u32 | type_tag u8 (0xFF = none) |
    /// name_len u16 | name utf-8 | pad u8`.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + self.by_id.len() * 16);
        out.extend_from_slice(&DICT_MAGIC.to_le_bytes());
        out.extend_from_slice(&FormatVersion::V1.get().to_le_bytes());
        out.extend_from_slice(&[0u8; 2]); // reserved
        out.extend_from_slice(&(self.by_id.len() as u32).to_le_bytes());
        for def in self.by_id.values() {
            out.extend_from_slice(&def.id.get().to_le_bytes());
            out.push(def.declared_type.map(|t| t as u8).unwrap_or(TYPE_NONE));
            let name = def.name.as_bytes();
            out.extend_from_slice(&(name.len() as u16).to_le_bytes());
            out.extend_from_slice(name);
            out.push(0); // pad entry alignment
        }
        out
    }

    /// Decodes a dictionary from its binary form.
    pub fn decode(bytes: &[u8]) -> ndb_common::Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::Corruption(
                "field dictionary: truncated header".into(),
            ));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != DICT_MAGIC {
            return Err(Error::Corruption("field dictionary: bad magic".into()));
        }
        let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        if !FormatVersion::V1.can_read(FormatVersion::new(version)) {
            return Err(Error::Corruption(format!(
                "field dictionary: unsupported format version {version}"
            )));
        }
        let count = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let mut dict = FieldDictionary::new();
        let mut pos = HEADER_SIZE;
        for _ in 0..count {
            if pos + 7 > bytes.len() {
                return Err(Error::Corruption(
                    "field dictionary: truncated entry".into(),
                ));
            }
            let id = FieldId::new(u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()));
            let tag = bytes[pos + 4];
            let declared_type = if tag == TYPE_NONE {
                None
            } else {
                Some(DataType::from_tag(tag).ok_or_else(|| {
                    Error::Corruption(format!("field dictionary: bad type tag {tag}"))
                })?)
            };
            let name_len = u16::from_le_bytes(bytes[pos + 5..pos + 7].try_into().unwrap()) as usize;
            pos += 7;
            if pos + name_len + 1 > bytes.len() {
                return Err(Error::Corruption("field dictionary: truncated name".into()));
            }
            let name = String::from_utf8(bytes[pos..pos + name_len].to_vec())
                .map_err(|_| Error::Corruption("field dictionary: non-utf8 field name".into()))?;
            pos += name_len + 1;
            let def = FieldDef {
                id,
                name: name.clone(),
                declared_type,
            };
            if dict.by_id.insert(id, def).is_some() {
                return Err(Error::Corruption(format!(
                    "field dictionary: duplicate field id {id}"
                )));
            }
            dict.by_name.insert(name, id);
            dict.next_id = dict.next_id.max(id.get() + 1);
        }
        Ok(dict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intern_assigns_stable_ids() {
        let mut dict = FieldDictionary::new();
        let a = dict.intern("name");
        let b = dict.intern("price");
        assert_eq!(dict.intern("name"), a);
        assert_eq!(dict.intern("price"), b);
        assert_ne!(a, b);
        assert_eq!(dict.field_id("name"), Some(a));
        assert_eq!(dict.field(a).unwrap().name, "name");
    }

    #[test]
    fn declare_rejects_silent_type_change() {
        let mut dict = FieldDictionary::new();
        dict.declare("price", DataType::Int).unwrap();
        assert!(dict.declare("price", DataType::String).is_err());
        assert!(dict.declare("price", DataType::Int).is_ok());
    }

    #[test]
    fn declare_upgrades_interned_field() {
        let mut dict = FieldDictionary::new();
        let id = dict.intern("name");
        assert_eq!(dict.field(id).unwrap().declared_type, None);
        dict.declare("name", DataType::String).unwrap();
        assert_eq!(
            dict.field(id).unwrap().declared_type,
            Some(DataType::String)
        );
    }

    #[test]
    fn binary_roundtrip() {
        let mut dict = FieldDictionary::new();
        dict.declare("name", DataType::String).unwrap();
        dict.declare("embedding", DataType::Vector).unwrap();
        dict.intern("ad_hoc");
        let restored = FieldDictionary::decode(&dict.encode()).unwrap();
        for def in dict.iter() {
            let other = restored.field(def.id).unwrap();
            assert_eq!(other, def);
        }
        assert_eq!(restored.len(), dict.len());
    }

    #[test]
    fn rejects_corrupt_dict() {
        assert!(FieldDictionary::decode(&[0u8; 4]).is_err());
        let mut dict = FieldDictionary::new();
        dict.intern("x");
        let mut bytes = dict.encode();
        bytes[0] ^= 0xFF;
        assert!(FieldDictionary::decode(&bytes).is_err());
    }
}
