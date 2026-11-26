//! `ndb-buffer` — buffer pool and memory management (spec §5.4, §19.1).
//!
//! Memory limits are configurable and bounded — the pool must never allow
//! unbounded growth (spec §3.1).
//!
//! Implementation begins in Phase 2 (spec §26).
