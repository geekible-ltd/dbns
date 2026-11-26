//! Persisted-format versioning primitives.
//!
//! Spec §3.1: all persisted formats must be versioned. Every on-disk and
//! on-wire structure (documents, pages, WAL records, index segments,
//! snapshots, protocol definitions) carries an explicit `FormatVersion`.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Version of a persisted binary format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(transparent)]
pub struct FormatVersion(u16);

impl FormatVersion {
    /// Initial version of any format.
    pub const V1: FormatVersion = FormatVersion(1);

    /// Creates a version from its raw numeric value.
    pub const fn new(v: u16) -> Self {
        Self(v)
    }

    /// Raw numeric value.
    pub const fn get(self) -> u16 {
        self.0
    }

    /// Whether data written at `other` can be read by a reader at `self`.
    ///
    /// For now versions must match exactly; relaxed compatibility rules are
    /// introduced per format as they evolve.
    pub const fn can_read(self, other: FormatVersion) -> bool {
        self.0 == other.0
    }
}

impl Default for FormatVersion {
    fn default() -> Self {
        Self::V1
    }
}

impl fmt::Display for FormatVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}
