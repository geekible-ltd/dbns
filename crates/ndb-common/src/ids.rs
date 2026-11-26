//! Strongly-typed identifiers used across the workspace.
//!
//! Every identifier is a newtype so that IDs of different domains cannot be
//! interchanged accidentally.

use serde::{Deserialize, Serialize};

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $inner:ty) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
        #[repr(transparent)]
        pub struct $name(pub $inner);

        impl $name {
            /// Creates a new identifier wrapping the raw value.
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the raw underlying value.
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }
    };
}

define_id!(
    /// Unique identifier of a database within a tenant.
    DatabaseId, u64
);
define_id!(
    /// Unique identifier of a collection within a database.
    CollectionId, u64
);
define_id!(
    /// Unique identifier of a document within a collection.
    DocumentId, u64
);
define_id!(
    /// Stable numeric identifier of a field within a collection's field
    /// dictionary. Must remain stable for the lifetime of its schema version
    /// (spec §6.4).
    FieldId, u32
);
define_id!(
    /// Unique identifier of an index.
    IndexId, u64
);
define_id!(
    /// Identifier of a storage page (spec §5.2: 64-bit).
    PageId, u64
);
define_id!(
    /// Log sequence number used by the WAL (spec §5.5).
    Lsn, u64
);
define_id!(
    /// Identifier of a transaction.
    TransactionId, u64
);
define_id!(
    /// Identifier of a node within the cluster (spec §4.4).
    NodeId, u64
);
define_id!(
    /// Identifier of a virtual shard (spec §4.3).
    ShardId, u32
);
define_id!(
    /// Identifier of a tenant (spec §17.1).
    TenantId, u64
);

/// Monotonic sequence number assigned to committed transactions, used for
/// indexing watermarks and search-visibility tracking (spec §7.3–7.4).
pub type CommitSequence = u64;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_roundtrip_raw_values() {
        let doc = DocumentId::new(42);
        assert_eq!(doc.get(), 42);
        assert_eq!(DocumentId::from(42), doc);
        assert_eq!(doc.to_string(), "42");
    }

    #[test]
    fn ids_are_not_interchangeable() {
        fn takes_doc_id(_: DocumentId) {}
        takes_doc_id(DocumentId::new(1));
        // CollectionId::new(1) would not compile here — that is the point.
    }
}
