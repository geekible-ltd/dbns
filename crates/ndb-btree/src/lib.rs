//! `ndb-btree` — primary and secondary B+Tree implementations
//! (spec §5.3, §19.1).
//!
//! Dependency rule: must not depend on the query engine.
//! Concurrency: pessimistic latch-coupling — no lock-free operations until
//! the basic implementation is correct and benchmarked (spec §5.3).
//!
//! - [`node`] — slotted-page node layout inside page bodies
//! - [`tree`] — [`tree::BTree`]: get/insert/delete/scan/range/seek
//! - [`cursor`] — forward iterator over the leaf chain
//! - [`key`] — order-preserving composite key encoding

pub mod cursor;
pub mod key;
pub mod node;
pub mod tree;

pub use cursor::Cursor;
pub use key::{KeyPart, encode_composite};
pub use node::MAX_KEY_SIZE;
pub use tree::BTree;
