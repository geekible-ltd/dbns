//! `ndb-wal` — write-ahead logging and recovery records (spec §5.5, §19.1).
//!
//! The database must never acknowledge a committed transaction before its
//! durability conditions are satisfied (spec §5.5).
//!
//! Implementation begins in Phase 3 (spec §26).
