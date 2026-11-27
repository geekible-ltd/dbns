//! Raft transports: in-process for single-process clusters (tests,
//! embedded mode) and TCP for real multi-node deployments (spec §4.2).
//!
//! Both implement [`RaftNetworkFactory`] + [`RaftNetworkV2`] — openraft
//! 0.10's transport surface (append_entries / vote / full_snapshot; the
//! pipelined `stream_append` falls back to sequential requests, pre_vote
//! and transfer_leader use defaults).
//!
//! The TCP wire protocol is deliberately trivial: length-prefixed bincode
//! frames, one request-response per connection. `AppendResult` isn't
//! serde-able, so it crosses the wire as [`StreamAppendResult`] (no
//! conflict hints — the leader then locates conflicts by binary search).

use std::collections::BTreeMap;
use std::future::Future;
use std::io::Cursor;
use std::marker::PhantomData;
use std::sync::Arc;

use ndb_page::PageIo;
use openraft::type_config::alias::{
    CommittedLeaderIdOf, MembershipOf, SnapshotOf, VoteOf,
};
use openraft::error::{
    NetworkError, ReplicationClosed, RPCError, StreamingError, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RPCTypes, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendResult, SnapshotResponse, StreamAppendResult, VoteRequest,
    VoteResponse,
};
use openraft::{OptionalSend, Snapshot, SnapshotMeta};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::types::{from_bytes, to_bytes, NdbRaft, NdbTypes, Node, NodeId};

// ── wire format ───────────────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
enum RaftRequest {
    Append(AppendEntriesRequest<NdbTypes>),
    Vote(VoteRequest<NdbTypes>),
    /// Full snapshot install: vote + snapshot meta + serialized body.
    Snapshot {
        vote: VoteOf<NdbTypes>,
        meta: SnapshotMeta<CommittedLeaderIdOf<NdbTypes>, MembershipOf<NdbTypes>>,
        data: Vec<u8>,
    },
}

#[derive(Serialize, Deserialize)]
enum RaftResponse {
    Append(Result<StreamAppendResult<NdbTypes>, String>),
    Vote(Result<VoteResponse<NdbTypes>, String>),
    Snapshot(Result<SnapshotResponse<NdbTypes>, String>),
}

async fn write_frame<T: Serialize>(s: &mut TcpStream, v: &T) -> std::io::Result<()> {
    let bytes = to_bytes(v)?;
    s.write_u32(bytes.len() as u32).await?;
    s.write_all(&bytes).await
}

async fn read_frame<T: serde::de::DeserializeOwned>(s: &mut TcpStream) -> std::io::Result<T> {
    let len = s.read_u32().await? as usize;
    if len > 128 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    from_bytes(&buf)
}

// ── in-process transport ─────────────────────────────────────────────

/// Shared registry of in-process raft nodes; `MemNetworkFactory` clones
/// the `Arc` so every client's factory sees the same cluster.
pub struct MemRouter<P: PageIo + Send + Sync + 'static> {
    nodes: Mutex<BTreeMap<NodeId, NdbRaft<P>>>,
}

impl<P: PageIo + Send + Sync + 'static> Default for MemRouter<P> {
    fn default() -> Self {
        Self {
            nodes: Mutex::new(BTreeMap::new()),
        }
    }
}

impl<P: PageIo + Send + Sync + 'static> MemRouter<P> {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register a live raft handle under its node id.
    pub async fn register(&self, id: NodeId, raft: NdbRaft<P>) {
        self.nodes.lock().await.insert(id, raft);
    }

    /// Drop a node — simulates it going down (calls become Unreachable).
    pub async fn remove(&self, id: NodeId) {
        self.nodes.lock().await.remove(&id);
    }

    async fn get(&self, id: NodeId) -> Option<NdbRaft<P>> {
        self.nodes.lock().await.get(&id).cloned()
    }
}

pub struct MemNetworkFactory<P: PageIo + Send + Sync + 'static> {
    router: Arc<MemRouter<P>>,
}

impl<P: PageIo + Send + Sync + 'static> MemNetworkFactory<P> {
    pub fn new(router: Arc<MemRouter<P>>) -> Self {
        Self { router }
    }
}

impl<P: PageIo + Send + Sync + 'static> RaftNetworkFactory<NdbTypes> for MemNetworkFactory<P> {
    type Network = MemNet<P>;

    async fn new_client(&mut self, target: NodeId, _node: &Node) -> Self::Network {
        MemNet {
            router: Arc::clone(&self.router),
            target,
        }
    }
}

/// In-process client — dispatches RPCs straight onto the target's handle.
pub struct MemNet<P: PageIo + Send + Sync + 'static> {
    router: Arc<MemRouter<P>>,
    target: NodeId,
}

impl<P: PageIo + Send + Sync + 'static> MemNet<P> {
    async fn raft(&self) -> Result<NdbRaft<P>, RPCError<NdbTypes>> {
        self.router
            .get(self.target)
            .await
            .ok_or_else(|| Unreachable::<NdbTypes>::from_string("no such in-process node").into())
    }
}

impl<P: PageIo + Send + Sync + 'static> RaftNetworkV2<NdbTypes> for MemNet<P> {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NdbTypes>,
        _option: RPCOption,
    ) -> Result<AppendResult<NdbTypes>, RPCError<NdbTypes>> {
        eprintln!("->append {} ({} entries)", self.target, rpc.entries.len());
        let raft = self.raft().await?;
        match raft.append_entries(rpc).await {
            Ok(r) => Ok(r),
            Err(e) => {
                eprintln!("mem append to {} failed: {e}", self.target);
                Err(Unreachable::<NdbTypes>::from_string(format!("{e}")).into())
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NdbTypes>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NdbTypes>, RPCError<NdbTypes>> {
        eprintln!("->vote {}", self.target);
        let raft = self.raft().await?;
        raft.vote(rpc)
            .await
            .map_err(|e| Unreachable::<NdbTypes>::from_string(format!("{e}")).into())
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<NdbTypes>,
        snapshot: SnapshotOf<NdbTypes, Cursor<Vec<u8>>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<NdbTypes>, StreamingError<NdbTypes>> {
        let raft = self
            .router
            .get(self.target)
            .await
            .ok_or_else(|| Unreachable::<NdbTypes>::from_string("no such in-process node"))?;
        tokio::select! {
            res = raft.install_full_snapshot(vote, snapshot) => {
                res.map_err(|e| StreamingError::Unreachable(
                    Unreachable::from_string(format!("{e}")),
                ))
            }
            _ = cancel => Err(ReplicationClosed::new("cancelled").into()),
        }
    }
}

// ── TCP transport ─────────────────────────────────────────────────────

/// Client factory that dials `node.addr` per RPC. `self_id` is stamped
/// into [`Timeout`] errors (openraft wants the caller's own node id).
pub struct TcpNetworkFactory<P: PageIo + Send + Sync + 'static> {
    self_id: NodeId,
    _p: PhantomData<P>,
}

impl<P: PageIo + Send + Sync + 'static> TcpNetworkFactory<P> {
    pub fn new(self_id: NodeId) -> Self {
        Self {
            self_id,
            _p: PhantomData,
        }
    }
}

impl<P: PageIo + Send + Sync + 'static> RaftNetworkFactory<NdbTypes> for TcpNetworkFactory<P> {
    type Network = TcpNet;

    async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
        TcpNet {
            self_id: self.self_id,
            target,
            addr: node.addr.clone(),
        }
    }
}

/// One-shot TCP client: dial → request frame → response frame → close.
pub struct TcpNet {
    self_id: NodeId,
    target: NodeId,
    addr: String,
}

impl TcpNet {
    async fn call(
        &mut self,
        req: &RaftRequest,
        option: &RPCOption,
        rpc_type: RPCTypes,
    ) -> Result<RaftResponse, RPCError<NdbTypes>> {
        let work = async {
            let mut s = TcpStream::connect(&self.addr).await?;
            write_frame(&mut s, req).await?;
            read_frame::<RaftResponse>(&mut s).await
        };
        match tokio::time::timeout(option.hard_ttl(), work).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                Err(Unreachable::new(&e).into())
            }
            Ok(Err(e)) => Err(NetworkError::new(&e).into()),
            Err(_) => Err(Timeout {
                action: rpc_type,
                id: self.self_id,
                target: self.target,
                timeout: option.hard_ttl(),
            }
            .into()),
        }
    }
}

impl RaftNetworkV2<NdbTypes> for TcpNet {
    type SnapshotData = Cursor<Vec<u8>>;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NdbTypes>,
        option: RPCOption,
    ) -> Result<AppendResult<NdbTypes>, RPCError<NdbTypes>> {
        match self
            .call(&RaftRequest::Append(rpc), &option, RPCTypes::AppendEntries)
            .await?
        {
            RaftResponse::Append(Ok(wire)) => Ok(wire.map_err(Into::into)),
            RaftResponse::Append(Err(e)) => {
                Err(Unreachable::<NdbTypes>::from_string(e).into())
            }
            _ => Err(NetworkError::from_string("unexpected response type").into()),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NdbTypes>,
        option: RPCOption,
    ) -> Result<VoteResponse<NdbTypes>, RPCError<NdbTypes>> {
        match self
            .call(&RaftRequest::Vote(rpc), &option, RPCTypes::Vote)
            .await?
        {
            RaftResponse::Vote(Ok(resp)) => Ok(resp),
            RaftResponse::Vote(Err(e)) => {
                Err(Unreachable::<NdbTypes>::from_string(e).into())
            }
            _ => Err(NetworkError::from_string("unexpected response type").into()),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<NdbTypes>,
        snapshot: SnapshotOf<NdbTypes, Cursor<Vec<u8>>>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<NdbTypes>, StreamingError<NdbTypes>> {
        let req = RaftRequest::Snapshot {
            vote,
            meta: snapshot.meta,
            data: snapshot.snapshot.into_inner(),
        };
        let work = self.call(&req, &option, RPCTypes::InstallSnapshot);
        let resp = tokio::select! {
            res = work => res,
            _ = cancel => return Err(ReplicationClosed::new("cancelled").into()),
        };
        match resp {
            Ok(RaftResponse::Snapshot(Ok(r))) => Ok(r),
            Ok(RaftResponse::Snapshot(Err(e))) => {
                Err(StreamingError::Unreachable(Unreachable::from_string(e)))
            }
            Ok(_) => Err(StreamingError::Unreachable(Unreachable::from_string(
                "unexpected response type",
            ))),
            Err(RPCError::Unreachable(e)) => Err(e.into()),
            Err(RPCError::Timeout(e)) => Err(e.into()),
            Err(e) => Err(StreamingError::Unreachable(Unreachable::from_string(e))),
        }
    }
}

/// Serves one raft node's RPCs on a TCP listener — spawn per group.
pub async fn serve_tcp<P: PageIo + Send + Sync + 'static>(
    addr: &str,
    raft: NdbRaft<P>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut socket, _) = listener.accept().await?;
        let raft = raft.clone();
        tokio::spawn(async move {
            let result = async {
                let req: RaftRequest = read_frame(&mut socket).await?;
                match req {
                    RaftRequest::Append(rpc) => {
                        let res = raft.append_entries(rpc).await;
                        let body = match res {
                            Ok(r) => Ok(r.map_err(Into::into)),
                            Err(e) => Err(e.to_string()),
                        };
                        write_frame(&mut socket, &RaftResponse::Append(body)).await
                    }
                    RaftRequest::Vote(rpc) => {
                        let res = raft.vote(rpc).await;
                        let body = match res {
                            Ok(r) => Ok(r),
                            Err(e) => Err(e.to_string()),
                        };
                        write_frame(&mut socket, &RaftResponse::Vote(body)).await
                    }
                    RaftRequest::Snapshot { vote, meta, data } => {
                        let snapshot = Snapshot {
                            meta,
                            snapshot: Cursor::new(data),
                        };
                        let res = raft.install_full_snapshot(vote, snapshot).await;
                        let body = match res {
                            Ok(r) => Ok(r),
                            Err(e) => Err(e.to_string()),
                        };
                        write_frame(&mut socket, &RaftResponse::Snapshot(body)).await
                    }
                }
            };
            // A dead/cancelled peer is routine noise — nothing to do.
            let _ = result.await;
        });
    }
}
