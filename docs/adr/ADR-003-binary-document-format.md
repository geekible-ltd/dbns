# ADR-003: Custom binary document format

- Date: 2026-11-26
- Status: accepted

## Context

Documents are the database's unit of storage (spec §6). The format must
support partial field access, nested objects, arrays, vectors and
forward-compatible evolution, and must not depend on Rust struct layout.

## Problem

Existing formats (BSON, MessagePack, Protocol Buffers) either impose schema
rigidity, lack a field-ID directory enabling cheap partial reads, or cannot
carry the header metadata NDB requires (document/collection/schema/document
versions).

## Alternatives considered

- BSON — closest fit but no field directory; every field access requires a
  linear scan of length-prefixed values.
- MessagePack — compact but same partial-access limitation.
- FlatBuffers — zero-copy access but requires generated schemas; conflicts
  with flexible schemaless documents.

## Decision

Custom format (`ndb-document::codec`): 40-byte versioned header, per-field
directory (field_id, type tag, offset, length — sorted by field_id for binary
search), contiguous payload, trailing CRC32. Nested objects and arrays are
self-contained regions with their own directory, so inner offsets are
region-relative. Field names are interned to numeric IDs via a per-collection
`FieldDictionary`; IDs are append-only and stable per schema version.

Nested objects use `BTreeMap<String, Value>` so field order is
insignificant — encoding may reorder fields by field ID without changing
document equality.

All values are length-implicit via the directory (no redundant length
prefixes in the payload). Fixed-width scalars are stored little-endian.

## Consequences

- O(log n) field lookup via the sorted directory; a single field can be
  decoded without materialising the document (`EncodedDocument::field`).
- The whole document is integrity-checked by one CRC32; nested regions need
  no separate checksums.
- Schema evolution never reuses a field ID for a different declared type;
  type changes require explicit migration (spec §6.4).
- Compression is deferred (§6.5): the header `flags` field reserves space,
  and payloads are contiguous so a future version can compress whole regions.

## Risks

- Every parser edge is ours to get wrong — mitigated by bounds checks at every
  offset and proptest roundtrip coverage (spec §22.2).
- Format evolution requires version negotiation; `FormatVersion` is embedded
  in every header and dictionary blob.
