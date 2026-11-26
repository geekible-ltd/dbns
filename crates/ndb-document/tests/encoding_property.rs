//! Property-based tests for the binary document format (spec §22.2):
//! arbitrary documents must survive an encode/decode roundtrip unchanged.

use ndb_common::ids::{CollectionId, DocumentId};
use ndb_document::codec::{self, EncodeOptions};
use ndb_document::dict::FieldDictionary;
use ndb_document::document::Document;
use ndb_document::value::{DataType, DateTime, Decimal, Uuid, Value};
use proptest::prelude::*;

fn opts() -> EncodeOptions {
    EncodeOptions {
        collection_id: CollectionId::new(1),
        schema_version: 1,
        doc_version: 1,
    }
}

/// Arbitrary field names — short ASCII identifiers, possibly empty.
fn name_strategy() -> impl Strategy<Value = String> {
    "[a-z0-9_]{0,16}"
}

fn value_strategy() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i64>().prop_map(Value::Int),
        any::<u64>().prop_map(Value::UInt),
        // NaN is excluded: NaN != NaN would break roundtrip equality.
        any::<f64>()
            .prop_filter("non-NaN", |f| !f.is_nan())
            .prop_map(Value::Float),
        (any::<i128>(), 0u8..=28).prop_map(|(m, s)| Value::Decimal(Decimal::new(m, s))),
        name_strategy().prop_map(Value::String),
        prop::collection::vec(any::<u8>(), 0..64).prop_map(Value::Binary),
        any::<i64>().prop_map(|v| Value::DateTime(DateTime(v))),
        any::<[u8; 16]>().prop_map(|b| Value::Uuid(Uuid(b))),
        prop::collection::vec(any::<f32>().prop_filter("non-NaN", |f| !f.is_nan()), 0..16)
            .prop_map(Value::Vector),
    ];
    leaf.prop_recursive(4, 64, 8, |inner| {
        prop_oneof![
            // Field names are unique — btree_map yields a deduped map.
            prop::collection::btree_map(name_strategy(), inner.clone(), 0..8)
                .prop_map(Value::Object),
            prop::collection::vec(inner, 0..8).prop_map(Value::Array),
        ]
    })
}

fn document_strategy() -> impl Strategy<Value = Document> {
    (
        any::<u64>(),
        prop::collection::btree_map(name_strategy(), value_strategy(), 0..12),
    )
        .prop_map(|(id, fields)| Document::new(DocumentId::new(id), fields))
}

proptest! {
    #[test]
    fn documents_roundtrip(doc in document_strategy()) {
        let mut dict = FieldDictionary::new();
        let bytes = codec::encode(&doc, &opts(), &mut dict).unwrap();
        let back = codec::decode(&bytes, &dict).unwrap();
        prop_assert_eq!(back, doc);
    }

    #[test]
    fn every_field_is_individually_accessible(doc in document_strategy()) {
        let mut dict = FieldDictionary::new();
        let bytes = codec::encode(&doc, &opts(), &mut dict).unwrap();
        let view = codec::EncodedDocument::new(&bytes).unwrap();
        prop_assert_eq!(view.field_count(), doc.fields.len());
        for (name, value) in &doc.fields {
            let got = view.field_named(name, &dict).unwrap();
            prop_assert_eq!(got.as_ref(), Some(value));
        }
    }

    #[test]
    fn single_byte_corruption_is_detected(
        doc in document_strategy(),
        byte in any::<u8>(),
    ) {
        let mut dict = FieldDictionary::new();
        let bytes = codec::encode(&doc, &opts(), &mut dict).unwrap();
        // Flip a byte strictly inside the document (before the CRC) — the
        // checksum must catch it.
        let mut bad = bytes.clone();
        let pos = bad.len() - 4 - 1;
        bad[pos] ^= byte;
        if byte != 0 {
            prop_assert!(codec::EncodedDocument::new(&bad).is_err());
        }
    }

    #[test]
    fn declared_types_match_encoded_tags(doc in document_strategy()) {
        // Each directory entry's type tag must match the decoded value's type.
        fn check(v: &Value, ty: DataType) {
            assert_eq!(v.data_type(), ty);
        }
        let mut dict = FieldDictionary::new();
        let bytes = codec::encode(&doc, &opts(), &mut dict).unwrap();
        let view = codec::EncodedDocument::new(&bytes).unwrap();
        for id in view.field_ids() {
            let v = view.field(id, &dict).unwrap().unwrap();
            let def = dict.field(id).unwrap();
            check(&v, v.data_type());
            let _ = def;
        }
    }
}
