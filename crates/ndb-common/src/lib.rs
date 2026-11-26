//! `ndb-common` — shared identifiers, error types, result types, common traits
//! and configuration primitives (spec §19.1).
//!
//! Dependency rule: this crate must not depend on any database engine crate.

pub mod error;
pub mod ids;
pub mod version;

pub use error::{Error, Result};
