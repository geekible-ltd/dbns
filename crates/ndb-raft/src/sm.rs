//! [`RaftStateMachine`]: a journaled KV state machine in a dedicated B+Tree.
//!
//! P9-W1's state machine is deliberately generic — a bincode-KV map — so
//! the cluster-metadata group (P9-W2) can store its schema directly and
//! per-shard groups (P9-W4) can reuse the same apply machinery.
//!
//! ```text
//!   b"applied"   → bincode(Option<LogId>)           last applied log id
//!   b"member"    → bincode(StoredMembership)        last membership
//!   b"snapmeta"  → bincode(SnapshotMeta)            retained snapshot meta
//!   b"snapdata"  → snapshot bytes                   retained snapshot body
//!   b'k' + key   → value bytes                      the replicated KV map
//! ```
//!
//! Apply batches commit through one write transaction — a committed raft
//! entry is durable on this node exactly when its WAL commit is. Client
//! responders fire *after* the txn commit so relaxed reads can't observe
//! an un-acked state.

use std::io;
use std::io::Cursor;
use std::sync::Arc;

use futures::StreamExt;
use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_page::PageIo;
use ndb_transaction::TxnManager;
use openraft::storage::{EntryResponder, RaftStateMachine, RaftSnapshotBuilder};
use openraft::type_config::alias::{
    LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
};
use openraft::entry::RaftEntry;
use openraft::{OptionalSend, Snapshot, SnapshotMeta, StoredMembership};

use crate::store::open_tree;
use crate::types::{from_bytes, to_bytes, NdbTypes, SmCommand, SmResponse};

const KEY_APPLIED: &[u8] = b"applied";
const KEY_SNAP_META: &[u8] = b"snapmeta";
const KEY_SNAP_DATA: &[u8] = b"snapdata";
const KV_PREFIX: u8 = b'k';

fn io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Serializable form of the machine's applied position.
#[derive(serde::Serialize, serde::Deserialize)]
struct AppliedState {
    last_applied: Option<LogIdOf<NdbTypes>>,
    membership: StoredMembershipOf<NdbTypes>,
}

/// [`RaftStateMachine`] over a journaled B+Tree — generic KV for now.
pub struct KvsMachine<P: PageIo> {
    txns: Arc<TxnManager<P>>,
    tree: Arc<BTree<P>>,
}

impl<P: PageIo> Clone for KvsMachine<P> {
    fn clone(&self) -> Self {
        Self {
            txns: Arc::clone(&self.txns),
            tree: Arc::clone(&self.tree),
        }
    }
}

impl<P: PageIo> KvsMachine<P> {
    /// Opens the group's state machine tree (created on first boot).
    pub fn open(
        txns: Arc<TxnManager<P>>,
        pool: &Arc<BufferPool<P>>,
        group: &str,
    ) -> ndb_common::Result<Self> {
        Ok(Self {
            tree: open_tree(&txns, pool, group, "sm")?,
            txns,
        })
    }

    /// Read a replicated key (used by the cluster-metadata layer in W2).
    pub fn get(&self, key: &[u8]) -> ndb_common::Result<Option<Vec<u8>>> {
        let txn = self.txns.begin()?;
        let mut k = Vec::with_capacity(key.len() + 1);
        k.push(KV_PREFIX);
        k.extend_from_slice(key);
        txn.get_in(&self.tree, &k)
    }

    /// The machine's current `k*` map — snapshot source + test assertions.
    pub fn kv_pairs(&self) -> ndb_common::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self.txns.begin()?;
        Ok(txn
            .scan_in(&self.tree)?
            .into_iter()
            .filter(|(k, _)| k.first() == Some(&KV_PREFIX))
            .map(|(k, v)| (k[1..].to_vec(), v))
            .collect())
    }

    fn read_applied(&self, txn: &ndb_transaction::Transaction<'_, P>) -> Result<AppliedState, io::Error> {
        match txn.get_in(&self.tree, KEY_APPLIED).map_err(io_err)? {
            Some(v) => from_bytes(&v),
            None => Ok(AppliedState {
                last_applied: None,
                membership: StoredMembership::default(),
            }),
        }
    }
}

/// Builds snapshots of the KV map (serialized `k*` pairs + applied state).
pub struct KvsSnapshotBuilder<P: PageIo> {
    sm: KvsMachine<P>,
}

impl<P: PageIo + Send + Sync + 'static> RaftSnapshotBuilder<NdbTypes> for KvsSnapshotBuilder<P> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn build_snapshot(&mut self) -> Result<SnapshotOf<NdbTypes, Cursor<Vec<u8>>>, io::Error> {
        let txn = self.sm.txns.begin().map_err(io_err)?;
        let applied = self.sm.read_applied(&txn)?;
        let data = self
            .sm
            .kv_pairs()
            .map_err(io_err)
            .and_then(|pairs| to_bytes(&pairs))?;
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.membership,
            },
            snapshot: Cursor::new(data),
        })
    }
}

impl<P: PageIo + Send + Sync + 'static> RaftStateMachine<NdbTypes> for KvsMachine<P> {
    type SnapshotData = Cursor<Vec<u8>>;
    type SnapshotBuilder = KvsSnapshotBuilder<P>;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<NdbTypes>>, StoredMembershipOf<NdbTypes>), io::Error> {
        let txn = self.txns.begin().map_err(io_err)?;
        let a = self.read_applied(&txn)?;
        Ok((a.last_applied, a.membership))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where
        Strm: futures::Stream<Item = Result<EntryResponder<NdbTypes>, io::Error>>
            + Unpin
            + OptionalSend,
    {
        // Drain first — a Transaction holds a !Send write-lock guard and
        // cannot live across an `.await`.
        let mut batch = Vec::new();
        while let Some(item) = entries.next().await {
            batch.push(item?);
        }
        if batch.is_empty() {
            return Ok(());
        }

        let mut pending_responses: Vec<(Option<openraft::storage::ApplyResponder<NdbTypes>>, SmResponse)> =
            Vec::with_capacity(batch.len());
        {
            let mut txn = self.txns.begin_write().map_err(io_err)?;
            let mut applied = self.read_applied(&txn)?;
            for (entry, responder) in batch {
                let log_id = entry.log_id().clone();
                match entry.payload {
                    openraft::EntryPayload::Blank => {}
                    openraft::EntryPayload::Normal(SmCommand::Put { key, value }) => {
                        let mut k = Vec::with_capacity(key.len() + 1);
                        k.push(KV_PREFIX);
                        k.extend_from_slice(&key);
                        let prev = txn.get_in(&self.tree, &k).map_err(io_err)?;
                        txn.insert_in(&self.tree, &k, &value).map_err(io_err)?;
                        pending_responses.push((
                            responder,
                            SmResponse {
                                last_applied: log_id.index,
                                previous: prev,
                            },
                        ));
                    }
                    openraft::EntryPayload::Normal(SmCommand::Delete { key }) => {
                        let mut k = Vec::with_capacity(key.len() + 1);
                        k.push(KV_PREFIX);
                        k.extend_from_slice(&key);
                        let prev = txn.get_in(&self.tree, &k).map_err(io_err)?;
                        if prev.is_some() {
                            txn.delete_in(&self.tree, &k).map_err(io_err)?;
                        }
                        pending_responses.push((
                            responder,
                            SmResponse {
                                last_applied: log_id.index,
                                previous: prev,
                            },
                        ));
                    }
                    openraft::EntryPayload::Membership(m) => {
                        applied.membership =
                            StoredMembership::new(Some(log_id.clone()), m);
                        if let Some(r) = responder {
                            pending_responses.push((
                                Some(r),
                                SmResponse {
                                    last_applied: log_id.index,
                                    previous: None,
                                },
                            ));
                        }
                    }
                }
                applied.last_applied = Some(log_id);
            }
            txn.insert_in(&self.tree, KEY_APPLIED, &to_bytes(&applied)?)
                .map_err(io_err)?;
            txn.commit().map_err(io_err)?;
        }

        // Responses go out only after the batch is durable.
        for (responder, resp) in pending_responses {
            if let Some(r) = responder {
                r.send(resp);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        KvsSnapshotBuilder { sm: self.clone() }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<NdbTypes>,
        snapshot: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = from_bytes(snapshot.get_ref())?;
        let mut txn = self.txns.begin_write().map_err(io_err)?;
        // Replace the KV map wholesale.
        let doomed: Vec<Vec<u8>> = txn
            .scan_in(&self.tree)
            .map_err(io_err)?
            .into_iter()
            .filter(|(k, _)| k.first() == Some(&KV_PREFIX))
            .map(|(k, _)| k)
            .collect();
        for k in doomed {
            txn.delete_in(&self.tree, &k).map_err(io_err)?;
        }
        for (k, v) in pairs {
            let mut key = Vec::with_capacity(k.len() + 1);
            key.push(KV_PREFIX);
            key.extend_from_slice(&k);
            txn.insert_in(&self.tree, &key, &v).map_err(io_err)?;
        }
        let applied = AppliedState {
            last_applied: meta.last_log_id.clone(),
            membership: meta.last_membership.clone(),
        };
        txn.insert_in(&self.tree, KEY_APPLIED, &to_bytes(&applied)?)
            .map_err(io_err)?;
        txn.insert_in(&self.tree, KEY_SNAP_META, &to_bytes(meta)?)
            .map_err(io_err)?;
        txn.insert_in(&self.tree, KEY_SNAP_DATA, snapshot.get_ref())
            .map_err(io_err)?;
        txn.commit().map_err(io_err)
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<NdbTypes, Cursor<Vec<u8>>>>, io::Error> {
        let txn = self.txns.begin().map_err(io_err)?;
        let (meta, data) = match (
            txn.get_in(&self.tree, KEY_SNAP_META).map_err(io_err)?,
            txn.get_in(&self.tree, KEY_SNAP_DATA).map_err(io_err)?,
        ) {
            (Some(m), Some(d)) => (from_bytes::<SnapshotMetaOf<NdbTypes>>(&m)?, d),
            _ => return Ok(None),
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Cursor::new(data),
        }))
    }
}
