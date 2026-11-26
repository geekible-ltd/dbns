//! `ndb-document` — binary document encoding, decoding, field dictionaries
//! and document validation (spec §6, §19.1).
//!
//! Dependency rule: must not depend on networking.
//!
//! - [`codec`] — the versioned binary wire format (§6.2): header, field
//!   directory, payload, trailing CRC32, plus [`codec::EncodedDocument`] for
//!   partial field access (§6.1).
//! - [`dict`] — per-collection field dictionary with stable field IDs (§6.4).
//! - [`schema`] — versioned schema definitions and validation (§6).
//! - [`value`] — in-memory value model and data-type catalogue (§6.3).
//!
//! Compression (§6.5) is deferred; the document flags field reserves space
//! for it.

pub mod codec;
pub mod dict;
pub mod document;
pub mod json;
pub mod schema;
pub mod value;

pub use codec::{EncodeOptions, EncodedDocument, decode, encode};
pub use dict::{FieldDef, FieldDictionary};
pub use document::Document;
pub use schema::{FieldSchema, Schema};
pub use value::{DataType, DateTime, Decimal, Object, Uuid, Value};
