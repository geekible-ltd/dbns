//! `ndb-transaction` — transaction lifecycle and commit coordination
//! (spec §7, §19.1).
//!
//! MVCC integration and snapshot isolation arrive with `ndb-mvcc` in a later
//! phase; this crate owns begin/commit/rollback coordination (spec §7.1).
//!
//! Implementation begins in Phase 3 (spec §26).
