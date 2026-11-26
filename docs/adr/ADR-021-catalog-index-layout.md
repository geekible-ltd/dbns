# ADR-021 — System catalog, collection metadata and index layout

- **Status**: Accepted
- **Date**: Phase 4
- **Spec references**: §2.1 (db/collection mgmt), §7.2–7.3 (write/index
  consistency), §8.1–8.3 (indexing architecture)

## Context

Phase 4 needs named databases and collections with schemas, a primary
index per collection (`doc_id → record location`, §8.2), secondary
indexes (§8.3), and index maintenance that commits atomically with
document writes (§7.3).

## Decision

- **The catalog is a B+Tree**: `TxnManager`'s internal tree stores
  `db:{name}` markers and `coll:{db}:{name}` → `CollectionMeta` entries.
  Being a normal tree, catalog changes are MVCC'd, journaled and
  crash-recovered for free.
- **One B+Tree per index**: every collection gets a pk tree
  (`doc_id:u64be → RecordEntry`) plus one tree per secondary index.
  Roots live in `CollectionMeta`; trees share the buffer pool. The txn
  layer's multi-tree write set (`insert_in`/`delete_in`) commits pk +
  index + catalog mutations in one commit sequence.
- **Documents are record entries, not leaf values**: `RecordEntry` =
  `{0|inline bytes}` for docs ≤ 1 KB or `{1|head page}` for a `Doc` page
  chain (`u64 next | u16 len | chunk`). This is §8.2's "record location"
  indirection and removes the leaf-size ceiling on documents.
- **Index entries**: non-unique key =
  `encode_composite(fields…, doc_id) → doc_id`; unique key =
  `encode_composite(fields…) → doc_id` (a present foreign owner is a
  uniqueness violation). Missing fields index as a null slot unless the
  index is `sparse`. Range/ordered access falls out of key order.
- **Doc ids**: `doc.id == 0` → auto-assign from a per-collection
  watermark (`next_doc_id`, persisted ~1024 ids ahead; restart gaps are
  allowed). Non-zero caller ids are honoured after a duplicate check.
- **Auto-save meta**: mutations that grow the dictionary, bump the id
  watermark, or change indexes mark the collection dirty and rewrite
  `CollectionMeta` in the caller's transaction — reopened handles can
  never decode a doc whose fields postdate the persisted dictionary.
- **`ndb-index`** is the crate (spec §19) holding `Engine`, `Collection`,
  `DocStore`, `IndexDef`/`CollectionMeta`.

## Consequences

- Document writes atomically maintain all secondary indexes — readers at
  any snapshot see consistent index/document state (§7.3).
- Dropped collections/indexes and GC'd record chains orphan pages —
  acceptable until the freelist persists and a version-drop reaper lands.
- Index scans materialize whole trees (`txn.scan_in`) — fine for catalog
  and small indexes; lazy cursor APIs come with the query engine.
- Partial (predicate) and bitmap indexes deferred: they need the query
  engine's expression evaluation and §8.4 bitmap structures.
