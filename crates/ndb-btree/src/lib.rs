//! `ndb-btree` — primary and secondary B+Tree implementations
//! (spec §5.3, §19.1).
//!
//! Dependency rule: must not depend on the query engine.
//! Concurrency: latch-coupling initially — no lock-free operations until the
//! basic implementation is correct and benchmarked (spec §5.3).
//!
//! Implementation begins in Phase 2 (spec §26).
