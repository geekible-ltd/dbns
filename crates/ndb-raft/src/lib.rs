//! `ndb-raft` — Raft consensus substrate for NDB distribution (spec §4.2,
//! P9-W1).
//!
//! Openraft 0.10 provides the protocol; this crate owns the three things
//! the spec leaves to us:
//!
//! - [`store::NdbLogStore`] — [`RaftLogStorage`] over a journaled B+Tree, so
//!   raft log/vote/commit-index durability rides the same WAL as user data.
//! - [`sm::KvsMachine`] — [`RaftStateMachine`]: a journaled KV map that the
//!   cluster-metadata group (P9-W2) builds on.
//! - [`net`] — in-process + TCP [`RaftNetworkFactory`] implementations.
//!
//! Per spec §4.2, the raft log is the authoritative ordering layer; the
//! local WAL remains the page-durability layer. Commit-index ↔ WAL
//! coordination lands in P9-W4 (replicated write path).

pub mod net;
pub mod sm;
pub mod store;
pub mod types;

use std::sync::Arc;

use ndb_buffer::BufferPool;
use ndb_page::PageIo;
use ndb_transaction::TxnManager;
use openraft::{Config, Raft};

pub use net::{MemNetworkFactory, MemRouter, TcpNetworkFactory};
pub use sm::KvsMachine;
pub use store::NdbLogStore;
pub use types::{NdbRaft, NdbTypes, Node, NodeId, SmCommand, SmResponse};

/// A raft group hosted on this node: log store + state machine + the
/// openraft [`Raft`] handle, all rooted in one engine's TxnManager/pool.
pub struct RaftNode<P: PageIo + Send + Sync + 'static> {
    pub raft: NdbRaft<P>,
    pub log_store: NdbLogStore<P>,
    pub state_machine: KvsMachine<P>,
}

impl<P: PageIo + Send + Sync + 'static> RaftNode<P> {
    /// Opens (or creates) raft group `group` for `node_id` over the
    /// engine's persistence, using the in-process transport.
    pub async fn open_mem(
        node_id: NodeId,
        group: &str,
        txns: Arc<TxnManager<P>>,
        pool: &Arc<BufferPool<P>>,
        router: Arc<MemRouter<P>>,
        config: Arc<Config>,
    ) -> Result<Self, openraft::error::Fatal<NdbTypes>> {
        let log_store = NdbLogStore::open(Arc::clone(&txns), pool, group)
            .map_err(|e| fatal_storage(e))?;
        let state_machine = KvsMachine::open(txns, pool, group).map_err(|e| fatal_storage(e))?;
        let raft = Raft::new(
            node_id,
            config,
            MemNetworkFactory::new(Arc::clone(&router)),
            log_store.clone(),
            state_machine.clone(),
        )
        .await?;
        router.register(node_id, raft.clone()).await;
        Ok(Self {
            raft,
            log_store,
            state_machine,
        })
    }
}

fn fatal_storage(e: ndb_common::Error) -> openraft::error::Fatal<NdbTypes> {
    openraft::error::Fatal::StorageError(openraft::StorageError::from_io_error(
        openraft::ErrorSubject::Store,
        openraft::ErrorVerb::Write,
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string()),
    ))
}
