//! `ndb-transaction` — transaction lifecycle, MVCC snapshots, checkpointing
//! and crash recovery (spec §5.5–5.7, §7.1; ADR-006/007).
//!
//! - [`manager::TxnManager`] — begin/commit/rollback over the B+Tree+WAL
//! - [`manager::recover`] — WAL replay after a crash
//! - [`versioned::VersionedValue`] — the MVCC version chain in leaf values
//! - [`journal`] — the pool-to-WAL journaling hook (ADR-006)
//!
//! Phase-3 policy: writers are serialized; snapshot readers never block.

pub mod journal;
pub mod manager;
pub mod versioned;

pub use journal::{Journal, SYSTEM_TXN};
pub use manager::{CATALOG_PAGE, RecoveryReport, Transaction, TxnConfig, TxnManager, recover};
pub use versioned::{Version, VersionedValue};
