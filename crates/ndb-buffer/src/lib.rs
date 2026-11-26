//! `ndb-buffer` — buffer pool and memory management (spec §5.4, §19.1).
//!
//! Memory limits are configurable and bounded — the pool never allows
//! unbounded growth (spec §3.1).

pub mod pool;

pub use pool::{BufferPool, PageReadGuard, PageWriteGuard};
