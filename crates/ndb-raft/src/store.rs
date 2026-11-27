//! [`RaftLogStorage`] over NDB persistence — spec §4.2's "Raft log is the
//! authoritative order; the local WAL is the durability layer for pages".
//!
//! One dedicated B+Tree per raft group carries everything the log storage
//! needs, so appends/votes/truncations are journaled and recovered by the
//! same WAL as user data:
//!
//! ```text
//!   b'l' + index.to_be_bytes()  → bincode(Entry)      the raft log itself
//!   b"vote"                     → bincode(Vote)       persisted vote
//!   b"purged"                   → bincode(LogId)      last purged log id
//!   b"commit"                   → bincode(LogId)      saved commit index
//! ```
//!
//! The tree's root page id is a catalog key (`raft:{group}:tree:{role}`),
//! so a group re-opens after restart without out-of-band bookkeeping.
//!
//! Every storage call is its own write transaction → each `append` pays a
//! WAL fsync. Group-commit batching is P9-W4 work; correctness first.

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use ndb_btree::BTree;
use ndb_buffer::BufferPool;
use ndb_common::ids::PageId;
use ndb_page::PageIo;
use ndb_transaction::TxnManager;
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::entry::RaftEntry;
use openraft::{OptionalSend, RaftLogReader};

use crate::types::{from_bytes, to_bytes, NdbTypes};

const LOG_PREFIX: u8 = b'l';
const KEY_VOTE: &[u8] = b"vote";
const KEY_PURGED: &[u8] = b"purged";
const KEY_COMMIT: &[u8] = b"commit";

fn log_key(index: u64) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[0] = LOG_PREFIX;
    k[1..].copy_from_slice(&index.to_be_bytes());
    k
}

fn io_err(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Opens (creating on first use) a dedicated btree for a raft group and
/// records its root in the system catalog under `raft:{group}:tree:{role}`.
pub(crate) fn open_tree<P: PageIo>(
    txns: &Arc<TxnManager<P>>,
    pool: &Arc<BufferPool<P>>,
    group: &str,
    role: &str,
) -> ndb_common::Result<Arc<BTree<P>>> {
    let key = format!("raft:{group}:tree:{role}").into_bytes();
    let mut txn = txns.begin_write()?;
    let tree = match txn.get(&key)? {
        Some(root) => BTree::open(
            Arc::clone(pool),
            PageId::new(u64::from_be_bytes(
                root[..8]
                    .try_into()
                    .map_err(|_| ndb_common::error::Error::Corruption(
                        "raft tree root key malformed".into(),
                    ))?,
            )),
        )?,
        None => {
            let t = BTree::create(Arc::clone(pool))?;
            txn.insert(&key, &t.root_id().get().to_be_bytes())?;
            t
        }
    };
    txn.commit()?;
    Ok(Arc::new(tree))
}

/// [`RaftLogStorage`] backed by a journaled B+Tree.
pub struct NdbLogStore<P: PageIo> {
    txns: Arc<TxnManager<P>>,
    tree: Arc<BTree<P>>,
}

impl<P: PageIo> Clone for NdbLogStore<P> {
    fn clone(&self) -> Self {
        Self {
            txns: Arc::clone(&self.txns),
            tree: Arc::clone(&self.tree),
        }
    }
}

impl<P: PageIo> NdbLogStore<P> {
    /// Opens the group's log store (creating its tree on first boot).
    pub fn open(
        txns: Arc<TxnManager<P>>,
        pool: &Arc<BufferPool<P>>,
        group: &str,
    ) -> ndb_common::Result<Self> {
        Ok(Self {
            tree: open_tree(&txns, pool, group, "log")?,
            txns,
        })
    }
}

/// Reads log entries + the persisted vote via read transactions.
pub struct NdbLogReader<P: PageIo> {
    txns: Arc<TxnManager<P>>,
    tree: Arc<BTree<P>>,
}

impl<P: PageIo + Send + Sync + 'static> RaftLogReader<NdbTypes> for NdbLogReader<P> {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<EntryOf<NdbTypes>>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + OptionalSend,
    {
        let txn = self.txns.begin().map_err(io_err)?;
        let mut out = Vec::new();
        for (k, v) in txn.scan_in(&self.tree).map_err(io_err)? {
            if k.first() != Some(&LOG_PREFIX) {
                continue;
            }
            let idx = u64::from_be_bytes(k[1..9].try_into().unwrap());
            if range.contains(&idx) {
                out.push((idx, from_bytes::<EntryOf<NdbTypes>>(&v)?));
            }
        }
        out.sort_by_key(|(i, _)| *i);
        Ok(out.into_iter().map(|(_, e)| e).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<NdbTypes>>, io::Error> {
        let txn = self.txns.begin().map_err(io_err)?;
        match txn.get_in(&self.tree, KEY_VOTE).map_err(io_err)? {
            Some(v) => Ok(Some(from_bytes(&v)?)),
            None => Ok(None),
        }
    }
}

impl<P: PageIo + Send + Sync + 'static> RaftLogStorage<NdbTypes> for NdbLogStore<P> {
    type LogReader = NdbLogReader<P>;

    async fn get_log_state(&mut self) -> Result<LogState<NdbTypes>, io::Error> {
        let txn = self.txns.begin().map_err(io_err)?;
        let last_purged_log_id = txn
            .get_in(&self.tree, KEY_PURGED)
            .map_err(io_err)?
            .map(|v| from_bytes(&v))
            .transpose()?;
        let mut last_log_id: Option<LogIdOf<NdbTypes>> = None;
        for (k, v) in txn.scan_in(&self.tree).map_err(io_err)? {
            if k.first() == Some(&LOG_PREFIX) {
                let e: EntryOf<NdbTypes> = from_bytes(&v)?;
                if last_log_id.as_ref().map(|l| l.index) < Some(e.log_id().index) {
                    last_log_id = Some(e.log_id());
                }
            }
        }
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        NdbLogReader {
            txns: Arc::clone(&self.txns),
            tree: Arc::clone(&self.tree),
        }
    }

    async fn save_vote(&mut self, vote: &VoteOf<NdbTypes>) -> Result<(), io::Error> {
        let mut txn = self.txns.begin_write().map_err(io_err)?;
        txn.insert_in(&self.tree, KEY_VOTE, &to_bytes(vote)?)
            .map_err(io_err)?;
        txn.commit().map_err(io_err)
    }

    async fn save_committed(&mut self, committed: Option<LogIdOf<NdbTypes>>) -> Result<(), io::Error> {
        let mut txn = self.txns.begin_write().map_err(io_err)?;
        match committed {
            Some(id) => txn
                .insert_in(&self.tree, KEY_COMMIT, &to_bytes(&id)?)
                .map_err(io_err)?,
            None => {
                txn.delete_in(&self.tree, KEY_COMMIT).map_err(io_err)?;
            }
        }
        txn.commit().map_err(io_err)
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<NdbTypes>>, io::Error> {
        let txn = self.txns.begin().map_err(io_err)?;
        txn.get_in(&self.tree, KEY_COMMIT)
            .map_err(io_err)?
            .map(|v| from_bytes(&v))
            .transpose()
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<NdbTypes>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<NdbTypes>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result = (|| -> ndb_common::Result<()> {
            let mut txn = self.txns.begin_write()?;
            for e in entries {
                txn.insert_in(&self.tree, &log_key(e.log_id().index), &to_bytes(&e)?)?;
            }
            txn.commit()
        })();
        // Durability signal: the entries are only acknowledged after the
        // write transaction commits (WAL fsync).
        callback.io_completed(result.map_err(io_err));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogIdOf<NdbTypes>>) -> Result<(), io::Error> {
        let mut txn = self.txns.begin_write().map_err(io_err)?;
        let floor = last_log_id.map(|l| l.index).unwrap_or(0);
        let doomed: Vec<[u8; 9]> = txn
            .scan_in(&self.tree)
            .map_err(io_err)?
            .into_iter()
            .filter(|(k, _)| {
                k.first() == Some(&LOG_PREFIX)
                    && u64::from_be_bytes(k[1..9].try_into().unwrap()) > floor
            })
            .map(|(k, _)| k[..9].try_into().unwrap())
            .collect();
        for k in doomed {
            txn.delete_in(&self.tree, &k).map_err(io_err)?;
        }
        txn.commit().map_err(io_err)
    }

    async fn purge(&mut self, log_id: LogIdOf<NdbTypes>) -> Result<(), io::Error> {
        let mut txn = self.txns.begin_write().map_err(io_err)?;
        let doomed: Vec<[u8; 9]> = txn
            .scan_in(&self.tree)
            .map_err(io_err)?
            .into_iter()
            .filter(|(k, _)| {
                k.first() == Some(&LOG_PREFIX)
                    && u64::from_be_bytes(k[1..9].try_into().unwrap()) <= log_id.index
            })
            .map(|(k, _)| k[..9].try_into().unwrap())
            .collect();
        for k in doomed {
            txn.delete_in(&self.tree, &k).map_err(io_err)?;
        }
        txn.insert_in(&self.tree, KEY_PURGED, &to_bytes(&log_id)?)
            .map_err(io_err)?;
        txn.commit().map_err(io_err)
    }
}
