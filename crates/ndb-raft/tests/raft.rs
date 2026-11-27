//! P9-W1 acceptance: a 3-node in-process raft cluster over NDB
//! persistence — election, replicated `SmCommand`s, and restart recovery
//! proving vote/log/applied-state durability through our own WAL.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ndb_buffer::BufferPool;
use ndb_index::Engine;
use ndb_page::{PageIo, PageManager};
use ndb_raft::{
    KvsMachine, MemRouter, NdbLogStore, NdbTypes, NodeId, SmCommand,
};
use ndb_transaction::TxnConfig;
use ndb_wal::Wal;
use openraft::rt::WatchReceiver;
use openraft::{Config, Raft, ServerState};
use tempfile::TempDir;

/// In-memory page store surviving "restarts" (same harness as the other
/// crates' recovery tests).
#[derive(Clone, Default)]
struct MemIo {
    data: Arc<Mutex<Vec<u8>>>,
}
impl PageIo for MemIo {
    fn read_at(&mut self, off: u64, buf: &mut [u8]) -> ndb_common::Result<()> {
        let d = self.data.lock().unwrap();
        buf.copy_from_slice(&d[off as usize..off as usize + buf.len()]);
        Ok(())
    }
    fn write_at(&mut self, off: u64, data: &[u8]) -> ndb_common::Result<()> {
        let mut d = self.data.lock().unwrap();
        d[off as usize..off as usize + data.len()].copy_from_slice(data);
        Ok(())
    }
    fn len(&mut self) -> ndb_common::Result<u64> {
        Ok(self.data.lock().unwrap().len() as u64)
    }
    fn set_len(&mut self, len: u64) -> ndb_common::Result<()> {
        self.data.lock().unwrap().resize(len as usize, 0);
        Ok(())
    }
    fn sync(&mut self) -> ndb_common::Result<()> {
        Ok(())
    }
}

const PAGE_SIZE: u32 = 4096;
const POOL_FRAMES: u64 = 64;
const GROUP: &str = "cluster";

type TestRaft = Raft<NdbTypes, KvsMachine<MemIo>>;

fn test_config() -> Arc<Config> {
    Arc::new(
        Config {
            cluster_name: "ndb-test".into(),
            election_timeout_min: 150,
            election_timeout_max: 400,
            heartbeat_interval: 50,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    )
}

struct TestNode {
    id: NodeId,
    io: MemIo,
    wal_dir: PathBuf,
    engine: Option<Arc<Engine<MemIo>>>,
    raft: Option<TestRaft>,
    sm: Option<KvsMachine<MemIo>>,
    _tmp: TempDir,
}

impl TestNode {
    fn create(id: NodeId) -> Self {
        let tmp = TempDir::new().unwrap();
        let mut n = Self {
            id,
            io: MemIo::default(),
            wal_dir: tmp.path().join("wal"),
            engine: None,
            raft: None,
            sm: None,
            _tmp: tmp,
        };
        n.boot_engine(true);
        n
    }

    fn boot_engine(&mut self, fresh: bool) {
        let pool = if fresh {
            let pm = PageManager::create(self.io.clone(), PAGE_SIZE, 8).unwrap();
            Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64))
        } else {
            let pm = PageManager::open(self.io.clone()).unwrap();
            Arc::new(BufferPool::new(pm, POOL_FRAMES * PAGE_SIZE as u64))
        };
        let (wal, _) = Wal::open(&self.wal_dir, 0).unwrap();
        let engine = if fresh {
            Engine::create(pool, Arc::new(wal), TxnConfig::default()).unwrap()
        } else {
            Engine::open(pool, Arc::new(wal), TxnConfig::default()).unwrap()
        };
        self.engine = Some(Arc::new(engine));
    }

    async fn boot_raft(&mut self, router: &Arc<MemRouter<MemIo>>, cfg: &Arc<Config>) {
        let engine = self.engine.as_ref().unwrap();
        let log_store = NdbLogStore::open(engine.txns_arc(), engine.pool(), GROUP).unwrap();
        let sm = KvsMachine::open(engine.txns_arc(), engine.pool(), GROUP).unwrap();
        let raft = Raft::new(
            self.id,
            Arc::clone(cfg),
            ndb_raft::MemNetworkFactory::new(Arc::clone(router)),
            log_store,
            sm.clone(),
        )
        .await
        .unwrap();
        router.register(self.id, raft.clone()).await;
        self.raft = Some(raft);
        self.sm = Some(sm);
    }

    /// Simulate a crash+restart: stop raft, drop the engine, recover from
    /// the same page store + WAL, and rejoin the in-process cluster.
    async fn restart(&mut self, router: &Arc<MemRouter<MemIo>>, cfg: &Arc<Config>) {
        let raft = self.raft.take().unwrap();
        let _ = raft.shutdown().await;
        self.engine = None;
        self.boot_engine(false);
        self.boot_raft(router, cfg).await;
    }

    fn raft(&self) -> &TestRaft {
        self.raft.as_ref().unwrap()
    }
    fn sm(&self) -> &KvsMachine<MemIo> {
        self.sm.as_ref().unwrap()
    }
}

async fn wait_leader(nodes: &[&TestNode], timeout: Duration) -> u64 {
    let deadline = Instant::now() + timeout;
    loop {
        for n in nodes {
            let m = n.raft().metrics().borrow_watched().clone();
            if m.state == ServerState::Leader {
                return n.id;
            }
        }
        assert!(Instant::now() < deadline, "no leader elected in {timeout:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn wait_kv(node: &TestNode, key: &[u8], want: Option<&[u8]>, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        match node.sm().get(key).unwrap() {
            v if v.as_deref() == want => return,
            _ => {
                assert!(
                    Instant::now() < deadline,
                    "node {} never applied key {:?} -> {:?}",
                    node.id,
                    key,
                    want
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}

fn members(ids: &[u64]) -> BTreeMap<NodeId, openraft::BasicNode> {
    ids.iter()
        .map(|id| {
            (
                *id,
                openraft::BasicNode {
                    addr: format!("mem://{id}"),
                },
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elects_leader_and_replicates() {
    let router = MemRouter::new();
    let cfg = test_config();
    let mut nodes: Vec<TestNode> = (1..=3).map(TestNode::create).collect();
    for n in &mut nodes {
        n.boot_raft(&router, &cfg).await;
    }

    nodes[0]
        .raft()
        .initialize(members(&[1, 2, 3]))
        .await
        .unwrap();

    let leader = wait_leader(&nodes.iter().collect::<Vec<_>>(), Duration::from_secs(5)).await;
    let leader_node = nodes.iter().find(|n| n.id == leader).unwrap();
    leader_node
        .raft()
        .client_write(SmCommand::Put {
            key: b"alpha".to_vec(),
            value: b"1".to_vec(),
        })
        .await
        .unwrap();

    // The command is applied on every member's state machine.
    for n in &nodes {
        wait_kv(n, b"alpha", Some(b"1"), Duration::from_secs(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovers_log_and_rejoins() {
    let router = MemRouter::new();
    let cfg = test_config();
    let mut nodes: Vec<TestNode> = (1..=3).map(TestNode::create).collect();
    for n in &mut nodes {
        n.boot_raft(&router, &cfg).await;
    }
    nodes[0]
        .raft()
        .initialize(members(&[1, 2, 3]))
        .await
        .unwrap();
    let leader_id = wait_leader(&nodes.iter().collect::<Vec<_>>(), Duration::from_secs(5)).await;
    let leader_raft = nodes
        .iter()
        .find(|n| n.id == leader_id)
        .unwrap()
        .raft()
        .clone();
    leader_raft
        .client_write(SmCommand::Put {
            key: b"persisted".to_vec(),
            value: b"yes".to_vec(),
        })
        .await
        .unwrap();
    wait_kv(&nodes[2], b"persisted", Some(b"yes"), Duration::from_secs(5)).await;

    // Crash node 3 and restart it from its own pages + WAL.
    nodes[2].restart(&router, &cfg).await;

    // More writes through the (surviving) leader — the restarted node
    // must catch up via append_entries rather than re-joining empty.
    leader_raft
        .client_write(SmCommand::Put {
            key: b"after-restart".to_vec(),
            value: b"2".to_vec(),
        })
        .await
        .unwrap();
    wait_kv(&nodes[2], b"persisted", Some(b"yes"), Duration::from_secs(5)).await;
    wait_kv(&nodes[2], b"after-restart", Some(b"2"), Duration::from_secs(5)).await;
}
