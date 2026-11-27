//! Raft type configuration for NDB consensus groups (spec §4.2).

use std::fmt;

use openraft::impls::BasicNode;
use serde::{Deserialize, Serialize};

/// Raft node ids are u64s assigned by the cluster metadata (§4.4).
pub type NodeId = u64;

/// Node descriptor — `addr` is the raft transport endpoint (`host:port`).
pub type Node = BasicNode;

/// Application payload for NDB raft groups.
///
/// The substrate is payload-agnostic: the cluster-metadata group (P9-W2)
/// uses [`SmCommand`] to replicate a small KV state machine, and per-shard
/// groups (P9-W4) will carry higher-level write ops encoded in `value`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum SmCommand {
    /// Set `key` to `value` in the replicated state machine.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Remove `key` from the replicated state machine.
    Delete { key: Vec<u8> },
}

impl fmt::Display for SmCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Put { key, .. } => write!(f, "put({})", String::from_utf8_lossy(key)),
            Self::Delete { key } => write!(f, "del({})", String::from_utf8_lossy(key)),
        }
    }
}

/// What a committed [`SmCommand`] yields to its client.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct SmResponse {
    /// Log index the entry was applied at.
    pub last_applied: u64,
    /// The key's previous value (for `Put`/`Delete`), if any.
    pub previous: Option<Vec<u8>>,
}

impl fmt::Display for SmResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "applied@{}", self.last_applied)
    }
}

openraft::declare_raft_types!(
    /// Openraft type configuration for an NDB group.
    #[derive(Serialize, Deserialize)]
    pub NdbTypes: D = SmCommand, R = SmResponse, Node = BasicNode
);

impl fmt::Display for NdbTypes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NdbTypes")
    }
}

/// A live NDB raft group handle (state machine type is part of it).
pub type NdbRaft<P> = openraft::Raft<NdbTypes, crate::sm::KvsMachine<P>>;

/// `bincode` helpers shared by the store + TCP transport.
pub fn to_bytes<T: Serialize>(v: &T) -> std::io::Result<Vec<u8>> {
    bincode::serialize(v).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

pub fn from_bytes<T: serde::de::DeserializeOwned>(b: &[u8]) -> std::io::Result<T> {
    bincode::deserialize(b).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}
