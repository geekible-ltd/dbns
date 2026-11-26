//! Binary document wire format (spec §6.2).
//!
//! ```text
//! Document layout
//! ─────────────
//! offset  size  field
//! 0       2     magic = "ND" (0x4E44)
//! 2       2     format_version
//! 4       8     document_id
//! 12      8     collection_id
//! 20      4     schema_version
//! 24      4     doc_version
//! 28      4     flags
//! 32      4     payload_size
//! 36      4     field_count
//! 40      ...   field directory (16 bytes per entry)
//!               payload (payload_size bytes)
//!               crc32 u32 over all preceding bytes
//!
//! Directory entry (16 B): field_id u32 | type u8 | flags u8 | reserved u16 |
//!                         offset u32 | length u32
//!   Offsets are relative to the payload start; entries are sorted by
//!   field_id.
//!
//! Nested object value: field_count u32 | directory (16 B/entry) | payload
//! Array value:         elem_count u32  | entries (12 B each: type u8 |
//!                        flags u8 | reserved u16 | offset u32 | length u32) |
//!                        payload
//!   Nested regions are self-contained: inner offsets are relative to the
//!   inner region's own payload.
//! ```
//!
//! The format does not depend on Rust struct layout (spec §6.1). All
//! multi-byte integers are little-endian.

use crate::dict::FieldDictionary;
use crate::document::Document;
use crate::value::{DataType, DateTime, Decimal, Uuid, Value};
use ndb_common::error::Error;
use ndb_common::ids::{CollectionId, DocumentId, FieldId};
use ndb_common::version::FormatVersion;

const MAGIC: u16 = 0x4E44; // "ND"
const HEADER_SIZE: usize = 40;
const DIR_ENTRY_SIZE: usize = 16;
const ARRAY_ENTRY_SIZE: usize = 12;
const CRC_SIZE: usize = 4;
const COUNT_SIZE: usize = 4;

/// Options controlling document encoding.
#[derive(Debug, Clone, Copy)]
pub struct EncodeOptions {
    pub collection_id: CollectionId,
    /// Schema version the document is written against (spec §6.2).
    pub schema_version: u32,
    /// Document version — bumped on update (MVCC, spec §5.7).
    pub doc_version: u32,
}

struct DirEntry {
    field_id: FieldId,
    data_type: DataType,
    offset: u32,
    length: u32,
}

// ── Encoding ─────────────────────────────────────────────────────────────

/// Encodes a document to the binary format, interning any new field names
/// into `dict`.
pub fn encode(
    doc: &Document,
    opts: &EncodeOptions,
    dict: &mut FieldDictionary,
) -> ndb_common::Result<Vec<u8>> {
    let fields: Vec<(FieldId, &Value)> = doc
        .fields
        .iter()
        .map(|(name, v)| (dict.intern(name), v))
        .collect();
    let (entries, payload) = encode_fields(fields, dict)?;

    let total = HEADER_SIZE + entries.len() * DIR_ENTRY_SIZE + payload.len() + CRC_SIZE;
    if total > u32::MAX as usize {
        return Err(Error::InvalidInput("document exceeds 4 GiB".into()));
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&MAGIC.to_le_bytes());
    out.extend_from_slice(&FormatVersion::V1.get().to_le_bytes());
    out.extend_from_slice(&doc.id.get().to_le_bytes());
    out.extend_from_slice(&opts.collection_id.get().to_le_bytes());
    out.extend_from_slice(&opts.schema_version.to_le_bytes());
    out.extend_from_slice(&opts.doc_version.to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes()); // flags
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in &entries {
        write_dir_entry(&mut out, e);
    }
    out.extend_from_slice(&payload);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    debug_assert_eq!(out.len(), total);
    Ok(out)
}

fn write_dir_entry(out: &mut Vec<u8>, e: &DirEntry) {
    out.extend_from_slice(&e.field_id.get().to_le_bytes());
    out.push(e.data_type as u8);
    out.push(0); // flags
    out.extend_from_slice(&[0u8; 2]); // reserved
    out.extend_from_slice(&e.offset.to_le_bytes());
    out.extend_from_slice(&e.length.to_le_bytes());
}

/// Encodes a set of `(field_id, value)` pairs into a directory + payload and
/// returns them separately (the caller decides where the directory lives).
fn encode_fields(
    mut fields: Vec<(FieldId, &Value)>,
    dict: &mut FieldDictionary,
) -> ndb_common::Result<(Vec<DirEntry>, Vec<u8>)> {
    fields.sort_by_key(|(id, _)| id.get());
    let mut entries = Vec::with_capacity(fields.len());
    let mut payload = Vec::new();
    let mut prev_id = None;
    for (id, value) in fields {
        if prev_id == Some(id) {
            return Err(Error::InvalidInput(format!(
                "duplicate field id {id} in document"
            )));
        }
        prev_id = Some(id);
        let offset = payload.len();
        write_value(value, &mut payload, dict)?;
        let length = payload.len() - offset;
        if offset > u32::MAX as usize || length > u32::MAX as usize {
            return Err(Error::InvalidInput("field value exceeds 4 GiB".into()));
        }
        entries.push(DirEntry {
            field_id: id,
            data_type: value.data_type(),
            offset: offset as u32,
            length: length as u32,
        });
    }
    Ok((entries, payload))
}

/// Appends the wire encoding of `value` to `payload`.
fn write_value(
    value: &Value,
    payload: &mut Vec<u8>,
    dict: &mut FieldDictionary,
) -> ndb_common::Result<()> {
    match value {
        Value::Null => {}
        Value::Bool(b) => payload.push(*b as u8),
        Value::Int(v) => payload.extend_from_slice(&v.to_le_bytes()),
        Value::UInt(v) => payload.extend_from_slice(&v.to_le_bytes()),
        Value::Float(v) => payload.extend_from_slice(&v.to_le_bytes()),
        Value::Decimal(d) => {
            payload.extend_from_slice(&d.mantissa.to_le_bytes());
            payload.push(d.scale);
        }
        Value::String(s) => payload.extend_from_slice(s.as_bytes()),
        Value::Binary(b) => payload.extend_from_slice(b),
        Value::DateTime(dt) => payload.extend_from_slice(&dt.0.to_le_bytes()),
        Value::Uuid(u) => payload.extend_from_slice(&u.0),
        Value::Vector(v) => {
            payload.reserve(v.len() * 4);
            for f in v {
                payload.extend_from_slice(&f.to_le_bytes());
            }
        }
        Value::Object(fields) => {
            let region = encode_object_region(fields, dict)?;
            payload.extend_from_slice(&region);
        }
        Value::Array(elements) => {
            let region = encode_array_region(elements, dict)?;
            payload.extend_from_slice(&region);
        }
    }
    Ok(())
}

/// Encodes a nested object as `count | directory | payload`.
fn encode_object_region(
    fields: &crate::value::Object,
    dict: &mut FieldDictionary,
) -> ndb_common::Result<Vec<u8>> {
    let fields: Vec<(FieldId, &Value)> = fields
        .iter()
        .map(|(name, v)| (dict.intern(name), v))
        .collect();
    let (entries, payload) = encode_fields(fields, dict)?;
    let mut region =
        Vec::with_capacity(COUNT_SIZE + entries.len() * DIR_ENTRY_SIZE + payload.len());
    region.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in &entries {
        write_dir_entry(&mut region, e);
    }
    region.extend_from_slice(&payload);
    Ok(region)
}

/// Encodes an array as `count | entries (12 B) | payload`.
fn encode_array_region(
    elements: &[Value],
    dict: &mut FieldDictionary,
) -> ndb_common::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let mut offsets = Vec::with_capacity(elements.len());
    for v in elements {
        let offset = payload.len();
        write_value(v, &mut payload, dict)?;
        offsets.push((
            v.data_type(),
            offset as u32,
            (payload.len() - offset) as u32,
        ));
    }
    let mut region =
        Vec::with_capacity(COUNT_SIZE + elements.len() * ARRAY_ENTRY_SIZE + payload.len());
    region.extend_from_slice(&(elements.len() as u32).to_le_bytes());
    for (ty, off, len) in &offsets {
        region.push(*ty as u8);
        region.push(0); // flags
        region.extend_from_slice(&[0u8; 2]); // reserved
        region.extend_from_slice(&off.to_le_bytes());
        region.extend_from_slice(&len.to_le_bytes());
    }
    region.extend_from_slice(&payload);
    Ok(region)
}

// ── Validation & decoding ────────────────────────────────────────────────

fn corruption(msg: impl Into<String>) -> Error {
    Error::Corruption(msg.into())
}

fn read_u16(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(b[off..off + 2].try_into().unwrap())
}
fn read_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}
fn read_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

/// Validates the structural integrity of an encoded document: magic,
/// version, directory and payload bounds, and the trailing CRC32.
pub fn validate(bytes: &[u8]) -> ndb_common::Result<()> {
    inspect(bytes).map(|_| ())
}

/// Checks the header, CRC and directory bounds; returns the validated
/// header fields needed by the decoder.
fn inspect(bytes: &[u8]) -> ndb_common::Result<(u32, usize, usize)> {
    if bytes.len() < HEADER_SIZE + CRC_SIZE {
        return Err(corruption("document: truncated header"));
    }
    if read_u16(bytes, 0) != MAGIC {
        return Err(corruption("document: bad magic"));
    }
    let version = read_u16(bytes, 2);
    if !FormatVersion::V1.can_read(FormatVersion::new(version)) {
        return Err(corruption(format!(
            "document: unsupported format version {version}"
        )));
    }
    let payload_size = read_u32(bytes, 32) as usize;
    let field_count = read_u32(bytes, 36) as usize;
    let dir_size = field_count
        .checked_mul(DIR_ENTRY_SIZE)
        .ok_or_else(|| corruption("document: field count overflow"))?;
    let expected = HEADER_SIZE
        .checked_add(dir_size)
        .and_then(|n| n.checked_add(payload_size))
        .and_then(|n| n.checked_add(CRC_SIZE))
        .ok_or_else(|| corruption("document: size overflow"))?;
    if expected != bytes.len() {
        return Err(corruption(format!(
            "document: length mismatch (expected {expected}, got {})",
            bytes.len()
        )));
    }
    let stored_crc = read_u32(bytes, bytes.len() - CRC_SIZE);
    let actual_crc = crc32fast::hash(&bytes[..bytes.len() - CRC_SIZE]);
    if stored_crc != actual_crc {
        return Err(corruption("document: checksum mismatch"));
    }
    // Verify every directory entry points inside the payload.
    let payload_start = HEADER_SIZE + dir_size;
    for i in 0..field_count {
        let e = payload_start - dir_size + i * DIR_ENTRY_SIZE;
        let off = read_u32(bytes, e + 8) as usize;
        let len = read_u32(bytes, e + 12) as usize;
        if off.checked_add(len).is_none_or(|end| end > payload_size) {
            return Err(corruption(format!(
                "document: directory entry {i} out of bounds"
            )));
        }
    }
    Ok((payload_size as u32, payload_start, field_count))
}

/// A validated view over an encoded document supporting partial field
/// access: individual fields can be decoded without materialising the whole
/// document (spec §6.1).
#[derive(Debug, Clone, Copy)]
pub struct EncodedDocument<'a> {
    bytes: &'a [u8],
    payload_start: usize,
    field_count: usize,
}

impl<'a> EncodedDocument<'a> {
    /// Validates `bytes` and returns the partial-access view.
    pub fn new(bytes: &'a [u8]) -> ndb_common::Result<Self> {
        let (_, payload_start, field_count) = inspect(bytes)?;
        Ok(Self {
            bytes,
            payload_start,
            field_count,
        })
    }

    pub fn document_id(&self) -> DocumentId {
        DocumentId::new(read_u64(self.bytes, 4))
    }

    pub fn collection_id(&self) -> CollectionId {
        CollectionId::new(read_u64(self.bytes, 12))
    }

    pub fn schema_version(&self) -> u32 {
        read_u32(self.bytes, 20)
    }

    pub fn doc_version(&self) -> u32 {
        read_u32(self.bytes, 24)
    }

    pub fn field_count(&self) -> usize {
        self.field_count
    }

    /// Iterates the field IDs present, in directory order.
    pub fn field_ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        (0..self.field_count)
            .map(move |i| FieldId::new(read_u32(self.bytes, HEADER_SIZE + i * DIR_ENTRY_SIZE)))
    }

    fn entry(&self, field_id: FieldId) -> Option<(DataType, usize, usize)> {
        // Directory is sorted by field_id — binary search.
        let mut lo = 0usize;
        let mut hi = self.field_count;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let e = HEADER_SIZE + mid * DIR_ENTRY_SIZE;
            let id = read_u32(self.bytes, e);
            match id.cmp(&field_id.get()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let ty = DataType::from_tag(self.bytes[e + 4])
                        .ok_or_else(|| {
                            corruption(format!("document: bad type tag {}", self.bytes[e + 4]))
                        })
                        .ok()?;
                    let off = read_u32(self.bytes, e + 8) as usize;
                    let len = read_u32(self.bytes, e + 12) as usize;
                    return Some((ty, self.payload_start + off, len));
                }
            }
        }
        None
    }

    /// Decodes a single field by ID without decoding the rest of the
    /// document. Nested objects require `dict` to resolve inner field names.
    pub fn field(
        &self,
        field_id: FieldId,
        dict: &FieldDictionary,
    ) -> ndb_common::Result<Option<Value>> {
        match self.entry(field_id) {
            None => Ok(None),
            Some((ty, start, len)) => {
                let slice = &self.bytes[start..start + len];
                Ok(Some(read_value(ty, slice, dict)?))
            }
        }
    }

    /// Decodes a single field by name.
    pub fn field_named(
        &self,
        name: &str,
        dict: &FieldDictionary,
    ) -> ndb_common::Result<Option<Value>> {
        match dict.field_id(name) {
            None => Ok(None),
            Some(id) => self.field(id, dict),
        }
    }
}

/// Fully decodes an encoded document.
pub fn decode(bytes: &[u8], dict: &FieldDictionary) -> ndb_common::Result<Document> {
    let view = EncodedDocument::new(bytes)?;
    let mut fields = std::collections::BTreeMap::new();
    for id in view.field_ids() {
        let def = dict
            .field(id)
            .ok_or_else(|| corruption(format!("document: unknown field id {id}")))?;
        let value = view.field(id, dict)?.unwrap();
        fields.insert(def.name.clone(), value);
    }
    Ok(Document::new(view.document_id(), fields))
}

/// Decodes one value of `ty` from `slice`.
fn read_value(ty: DataType, slice: &[u8], dict: &FieldDictionary) -> ndb_common::Result<Value> {
    let fixed = |n: usize| -> ndb_common::Result<()> {
        if slice.len() != n {
            Err(corruption(format!(
                "document: {ty:?} expects {n} bytes, got {}",
                slice.len()
            )))
        } else {
            Ok(())
        }
    };
    Ok(match ty {
        DataType::Null => Value::Null,
        DataType::Bool => {
            fixed(1)?;
            Value::Bool(slice[0] != 0)
        }
        DataType::Int => {
            fixed(8)?;
            Value::Int(i64::from_le_bytes(slice.try_into().unwrap()))
        }
        DataType::UInt => {
            fixed(8)?;
            Value::UInt(u64::from_le_bytes(slice.try_into().unwrap()))
        }
        DataType::Float => {
            fixed(8)?;
            Value::Float(f64::from_le_bytes(slice.try_into().unwrap()))
        }
        DataType::Decimal => {
            fixed(17)?;
            Value::Decimal(Decimal::new(
                i128::from_le_bytes(slice[..16].try_into().unwrap()),
                slice[16],
            ))
        }
        DataType::String => Value::String(
            String::from_utf8(slice.to_vec())
                .map_err(|_| corruption("document: non-utf8 string"))?,
        ),
        DataType::Binary => Value::Binary(slice.to_vec()),
        DataType::DateTime => {
            fixed(8)?;
            Value::DateTime(DateTime(i64::from_le_bytes(slice.try_into().unwrap())))
        }
        DataType::Uuid => {
            fixed(16)?;
            Value::Uuid(Uuid(slice.try_into().unwrap()))
        }
        DataType::Vector => {
            if !slice.len().is_multiple_of(4) {
                return Err(corruption("document: vector length not a multiple of 4"));
            }
            Value::Vector(
                slice
                    .chunks_exact(4)
                    .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
                    .collect(),
            )
        }
        DataType::Object => Value::Object(read_object_region(slice, dict)?),
        DataType::Array => Value::Array(read_array_region(slice, dict)?),
    })
}

/// Decodes a nested object region into a name → value map.
fn read_object_region(
    region: &[u8],
    dict: &FieldDictionary,
) -> ndb_common::Result<crate::value::Object> {
    if region.len() < COUNT_SIZE {
        return Err(corruption("document: truncated nested object"));
    }
    let count = read_u32(region, 0) as usize;
    let dir_size = count
        .checked_mul(DIR_ENTRY_SIZE)
        .ok_or_else(|| corruption("document: nested field count overflow"))?;
    if region.len() < COUNT_SIZE + dir_size {
        return Err(corruption("document: truncated nested directory"));
    }
    let payload_start = COUNT_SIZE + dir_size;
    let payload_size = region.len() - payload_start;
    let mut fields = crate::value::Object::new();
    for i in 0..count {
        let e = COUNT_SIZE + i * DIR_ENTRY_SIZE;
        let id = FieldId::new(read_u32(region, e));
        let ty = DataType::from_tag(region[e + 4])
            .ok_or_else(|| corruption(format!("document: bad type tag {}", region[e + 4])))?;
        let off = read_u32(region, e + 8) as usize;
        let len = read_u32(region, e + 12) as usize;
        if off.checked_add(len).is_none_or(|end| end > payload_size) {
            return Err(corruption("document: nested entry out of bounds"));
        }
        let name = dict
            .field(id)
            .ok_or_else(|| corruption(format!("document: unknown field id {id}")))?
            .name
            .clone();
        let value = read_value(
            ty,
            &region[payload_start + off..payload_start + off + len],
            dict,
        )?;
        if fields.insert(name, value).is_some() {
            return Err(corruption(format!(
                "document: duplicate field id {id} in nested object"
            )));
        }
    }
    Ok(fields)
}

/// Decodes an array region.
fn read_array_region(region: &[u8], dict: &FieldDictionary) -> ndb_common::Result<Vec<Value>> {
    if region.len() < COUNT_SIZE {
        return Err(corruption("document: truncated array"));
    }
    let count = read_u32(region, 0) as usize;
    let dir_size = count
        .checked_mul(ARRAY_ENTRY_SIZE)
        .ok_or_else(|| corruption("document: array element count overflow"))?;
    if region.len() < COUNT_SIZE + dir_size {
        return Err(corruption("document: truncated array directory"));
    }
    let payload_start = COUNT_SIZE + dir_size;
    let payload_size = region.len() - payload_start;
    let mut elements = Vec::with_capacity(count);
    for i in 0..count {
        let e = COUNT_SIZE + i * ARRAY_ENTRY_SIZE;
        let ty = DataType::from_tag(region[e])
            .ok_or_else(|| corruption(format!("document: bad type tag {}", region[e])))?;
        let off = read_u32(region, e + 4) as usize;
        let len = read_u32(region, e + 8) as usize;
        if off.checked_add(len).is_none_or(|end| end > payload_size) {
            return Err(corruption("document: array element out of bounds"));
        }
        elements.push(read_value(
            ty,
            &region[payload_start + off..payload_start + off + len],
            dict,
        )?);
    }
    Ok(elements)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dict_with(names: &[&str]) -> FieldDictionary {
        let mut d = FieldDictionary::new();
        for n in names {
            d.intern(n);
        }
        d
    }

    fn opts() -> EncodeOptions {
        EncodeOptions {
            collection_id: CollectionId::new(7),
            schema_version: 3,
            doc_version: 1,
        }
    }

    fn sample_doc() -> Document {
        Document::new(DocumentId::new(42), Default::default())
            .with("name", Value::String("alice".into()))
            .with("age", Value::Int(34))
            .with("score", Value::Float(9.5))
            .with("active", Value::Bool(true))
            .with(
                "tags",
                Value::Array(vec![Value::String("x".into()), Value::Int(7)]),
            )
            .with(
                "address",
                Value::Object(
                    [
                        ("city".to_string(), Value::String("London".into())),
                        ("zip".to_string(), Value::String("E1".into())),
                    ]
                    .into_iter()
                    .collect(),
                ),
            )
            .with("embedding", Value::Vector(vec![1.0, 2.0, 3.5]))
            .with("uuid", Value::Uuid(Uuid([7u8; 16])))
            .with("balance", Value::Decimal(Decimal::new(12345, 2)))
            .with("blob", Value::Binary(vec![0xDE, 0xAD]))
            .with("created", Value::DateTime(DateTime(1_700_000_000_000_000)))
            .with("nothing", Value::Null)
            .with("big", Value::UInt(u64::MAX))
    }

    #[test]
    fn roundtrip_all_types() {
        let mut dict = FieldDictionary::new();
        let doc = sample_doc();
        let bytes = encode(&doc, &opts(), &mut dict).unwrap();
        let back = decode(&bytes, &dict).unwrap();
        assert_eq!(back, doc);
    }

    #[test]
    fn partial_access_decodes_single_field() {
        let mut dict = FieldDictionary::new();
        let doc = sample_doc();
        let bytes = encode(&doc, &opts(), &mut dict).unwrap();
        let view = EncodedDocument::new(&bytes).unwrap();
        assert_eq!(view.document_id(), DocumentId::new(42));
        assert_eq!(view.collection_id(), CollectionId::new(7));
        assert_eq!(view.schema_version(), 3);
        assert_eq!(
            view.field_named("name", &dict).unwrap(),
            Some(Value::String("alice".into()))
        );
        assert_eq!(
            view.field_named("embedding", &dict).unwrap(),
            Some(Value::Vector(vec![1.0, 2.0, 3.5]))
        );
        // Nested object decodes correctly via partial access.
        match view.field_named("address", &dict).unwrap() {
            Some(Value::Object(fields)) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields.get("city"), Some(&Value::String("London".into())));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(view.field_named("missing", &dict).unwrap(), None);
    }

    #[test]
    fn detects_corruption() {
        let mut dict = FieldDictionary::new();
        let doc = sample_doc();
        let bytes = encode(&doc, &opts(), &mut dict).unwrap();
        // Flip a payload byte → CRC mismatch.
        let mut bad = bytes.clone();
        let mid = bad.len() - 6;
        bad[mid] ^= 0xFF;
        assert!(matches!(
            EncodedDocument::new(&bad),
            Err(Error::Corruption(_))
        ));
        // Truncation.
        assert!(EncodedDocument::new(&bytes[..20]).is_err());
        // Bad magic.
        let mut bad = bytes.clone();
        bad[0] = 0xFF;
        assert!(EncodedDocument::new(&bad).is_err());
    }

    #[test]
    fn unknown_field_id_fails_decode() {
        // Encode with one dictionary, decode with a fresh one that lacks the
        // field IDs.
        let mut enc_dict = FieldDictionary::new();
        let doc = Document::empty(DocumentId::new(1)).with("x", Value::Int(1));
        let bytes = encode(&doc, &opts(), &mut enc_dict).unwrap();
        let fresh = dict_with(&[]);
        assert!(matches!(decode(&bytes, &fresh), Err(Error::Corruption(_))));
    }

    #[test]
    fn empty_document_roundtrips() {
        let mut dict = FieldDictionary::new();
        let doc = Document::empty(DocumentId::new(9));
        let bytes = encode(&doc, &opts(), &mut dict).unwrap();
        let back = decode(&bytes, &dict).unwrap();
        assert_eq!(back.fields.len(), 0);
        assert_eq!(back.id, DocumentId::new(9));
    }
}
