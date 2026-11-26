//! `ndb-wal` — write-ahead logging and recovery records (spec §5.5, §19.1).
//!
//! The database must never acknowledge a committed transaction before its
//! durability conditions are satisfied (spec §5.5).
//!
//! - [`record`] — framed physiological records (len + lsn + txn + type +
//!   payload + crc32), torn-tail safe
//! - [`segment`] — append-only segment files with rotation
//! - [`wal`] — the [`wal::Wal`] writer (append/flush/rotate/retire) and the
//!   recovery scanner
//!
//! Design: `docs/adr/ADR-006-wal-design.md`.

pub mod record;
pub mod segment;
pub mod wal;

pub use record::WalRecord;
pub use wal::{RecoveredLog, ScannedRecord, Wal};
