# ADR-011 — Inverted index design

- **Status**: Accepted
- **Date**: Phase 5
- **Spec references**: §9.1 (inverted index), §9.4 (posting compression),
  §9.5 (phrase), §9.6 (fuzzy), §7.3 (indexing atomicity)

## Context

Spec §9.1 requires a custom inverted index: term dictionary, posting lists,
document identifiers, term frequencies, field identifiers, positions,
document lengths, and collection statistics. §9.4 requires delta encoding,
variable-byte encoding, block-based compression, skip/block-max metadata,
and segment management + index compaction.

## Decision

- **One B+Tree per text index** (`TextIndexDef.root`) — postings inherit
  MVCC, journaling, and crash recovery for free; an index write shares the
  document's commit sequence (§7.3 atomicity).
- **Key layout**: `T{field:u16be}{term}` → chunk-id list (term dictionary =
  the ordered `T` keyspace itself); `S{doc_id}` → per-field token counts;
  `MN` / `MF{field}` counters → doc count + per-field totals (avgdl);
  `D{doc_id}` deletion tombstones.
- **Chunked posting lists**: each term's value is a list of page ids; each
  page is a `Chunk` (~3.9 KB): magic, count, last-doc, **block-max tf**
  (§9.4 skip/block-max metadata) + delta-encoded doc ids with varint tf and
  delta-encoded positions (`ndb-postings`). Chunks are the "segment" unit —
  appends only ever touch the tail chunk.
- **Copy-on-write tail chunks**: appending re-writes the tail chunk into a
  *fresh* page and repoints the versioned header. A rolled-back txn restores
  the old header → old untouched pages → no corrupt state; orphaned pages
  are the documented leak class until the persisted freelist lands.
- **Physical removal on update/delete**: the old doc is re-analysed with the
  index's analyzers and its postings are stripped per term inside the same
  txn — rollback resurrects old headers, which is *correct* (the old doc
  comes back). Tombstones + `compact()` remain for stragglers from
  analyzer-version skew.
- **Compaction** (`compact_text_index`): merges every term's chunks, drops
  tombstoned docs, repacks into full pages — the spec's index compaction.
- **Fuzzy** = bounded Levenshtein scan over the ordered term dictionary with
  expansion caps (§9.6) — symmetric-delete preindexing/automata documented
  as future work.
- **Synchronous indexing**: index maintenance runs inside the caller's
  transaction (visibility at commit — beats the 5 s NRT target); the §7.3
  durable async pipeline is deferred to the distributed phase.

## Consequences

- Posting reads page through the buffer pool like everything else — no
  bespoke mmap format to maintain.
- Per-term read-modify-write is O(active chunk) not O(list) thanks to tail-
  chunk COW; compaction handles the bloat case.
- WAND/Block-Max WAND top-K pruning is *not* yet wired into query execution
  — `max_tf`/`last_doc` metadata is stored per chunk, ready for it.
