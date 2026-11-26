//! HNSW approximate nearest-neighbour index (spec §10.4).
//!
//! The whole graph lives in a per-index B+Tree — nodes, vectors and the
//! entry-point metadata are versioned `insert_in` writes, so graph edits are
//! journaled and roll back with the transaction exactly like the inverted
//! index (ADR-011).
//!
//! Key layout:
//! - `VM`                        → index meta (dims, metric, M, ef, entry point, RNG state)
//! - `VD{doc_id:be u64}`          → [`EncodedVector`] bytes
//! - `NG{level:u8}{doc_id:be u64}`→ `u16 count | doc_id…` neighbour list
//! - `DX{doc_id:be u64}`          → deletion tombstone (§10.5 update path;
//!   edges are lazily pruned at query time — physical unlinking is a
//!   compaction concern)
//!
//! Search is the standard two-phase HNSW walk: greedy descent from the entry
//! point down to layer 1, then a beam (`ef`) search at layer 0. Post-filtering
//! (§10.5) is applied to the expanded candidate set — pass `ef > k`.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use ndb_btree::BTree;
use ndb_common::Result;
use ndb_common::error::Error;
use ndb_page::manager::PageIo;
use ndb_transaction::Transaction;
use ndb_vector::{EncodedVector, Metric, score};

const NONE: u64 = u64::MAX;

/// Index configuration (§10.2 `vectorIndexConfig`).
#[derive(Debug, Clone)]
pub struct HnswConfig {
    pub dims: u16,
    pub metric: Metric,
    /// Max neighbours per node (both directions use `m`; layer-0 uses `2*m`
    /// per the paper).
    pub m: usize,
    /// Beam width during insert.
    pub ef_construction: usize,
    /// Default beam width during search.
    pub ef_search: usize,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            dims: 0,
            metric: Metric::Cosine,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
        }
    }
}

/// Persistent index metadata, stored under key `VM`.
#[derive(Debug, Clone)]
struct Meta {
    cfg: HnswConfig,
    count: u64,
    entry: u64,
    entry_level: u8,
    rng: u64,
}

impl Meta {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(32);
        v.extend_from_slice(&self.cfg.dims.to_le_bytes());
        v.push(self.cfg.metric as u8);
        v.extend_from_slice(&(self.cfg.m as u16).to_le_bytes());
        v.extend_from_slice(&(self.cfg.ef_construction as u16).to_le_bytes());
        v.extend_from_slice(&(self.cfg.ef_search as u16).to_le_bytes());
        v.extend_from_slice(&self.count.to_le_bytes());
        v.extend_from_slice(&self.entry.to_le_bytes());
        v.push(self.entry_level);
        v.extend_from_slice(&self.rng.to_le_bytes());
        v
    }

    fn decode(b: &[u8]) -> Result<Self> {
        if b.len() < 30 {
            return Err(Error::Corruption("hnsw meta: truncated".into()));
        }
        let metric = match b[2] {
            0 => Metric::Cosine,
            1 => Metric::Dot,
            2 => Metric::Euclidean,
            t => return Err(Error::Corruption(format!("hnsw meta: bad metric {t}"))),
        };
        Ok(Self {
            cfg: HnswConfig {
                dims: u16::from_le_bytes(b[0..2].try_into().unwrap()),
                metric,
                m: u16::from_le_bytes(b[3..5].try_into().unwrap()) as usize,
                ef_construction: u16::from_le_bytes(b[5..7].try_into().unwrap()) as usize,
                ef_search: u16::from_le_bytes(b[7..9].try_into().unwrap()) as usize,
            },
            count: u64::from_le_bytes(b[9..17].try_into().unwrap()),
            entry: u64::from_le_bytes(b[17..25].try_into().unwrap()),
            entry_level: b[25],
            rng: u64::from_le_bytes(b[26..34].try_into().unwrap()),
        })
    }
}

/// Candidate ordering for the beam heaps — distance first, then doc id.
#[derive(Debug, Clone, Copy)]
struct Cand {
    /// Score-based distance: `1 - score`, so smaller = nearer for all metrics.
    dist: f32,
    doc: u64,
}

impl PartialEq for Cand {
    fn eq(&self, o: &Self) -> bool {
        self.doc == o.doc
    }
}
impl Eq for Cand {}
impl PartialOrd for Cand {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Cand {
    /// Max-heap order on `dist` — `BinaryHeap<Cand>` pops the *worst*.
    fn cmp(&self, o: &Self) -> Ordering {
        self.dist.total_cmp(&o.dist).then(self.doc.cmp(&o.doc))
    }
}

/// An HNSW index stored in one B+Tree.
pub struct HnswIndex<P: PageIo> {
    tree: Arc<BTree<P>>,
}

impl<P: PageIo> HnswIndex<P> {
    pub fn create(
        tree: Arc<BTree<P>>,
        txn: &mut Transaction<'_, P>,
        cfg: HnswConfig,
    ) -> Result<Self> {
        if cfg.dims == 0 {
            return Err(Error::InvalidInput("vector index dims must be > 0".into()));
        }
        let meta = Meta {
            cfg,
            count: 0,
            entry: NONE,
            entry_level: 0,
            rng: 0x9e3779b97f4a7c15,
        };
        txn.insert_in(&tree, b"VM", &meta.encode())?;
        Ok(Self { tree })
    }

    pub fn open(tree: Arc<BTree<P>>) -> Self {
        Self { tree }
    }

    pub fn tree(&self) -> &Arc<BTree<P>> {
        &self.tree
    }

    fn meta(&self, txn: &Transaction<'_, P>) -> Result<Meta> {
        let raw = txn
            .get_in(&self.tree, b"VM")?
            .ok_or_else(|| Error::Corruption("hnsw: missing meta".into()))?;
        Meta::decode(&raw)
    }

    fn put_meta(&self, txn: &mut Transaction<'_, P>, m: &Meta) -> Result<()> {
        txn.insert_in(&self.tree, b"VM", &m.encode())
    }

    fn vector(&self, txn: &Transaction<'_, P>, doc: u64) -> Result<Vec<f32>> {
        let key = vd_key(doc);
        let raw = txn
            .get_in(&self.tree, &key)?
            .ok_or_else(|| Error::Corruption(format!("hnsw: missing vector {doc}")))?;
        Ok(EncodedVector::from_bytes(&raw)?.decode())
    }

    fn neighbors(&self, txn: &Transaction<'_, P>, level: u8, doc: u64) -> Result<Vec<u64>> {
        let key = ng_key(level, doc);
        match txn.get_in(&self.tree, &key)? {
            Some(raw) => {
                if raw.len() < 2 {
                    return Err(Error::Corruption("hnsw: bad neighbor list".into()));
                }
                let n = u16::from_le_bytes(raw[0..2].try_into().unwrap()) as usize;
                let mut out = Vec::with_capacity(n);
                for c in raw[2..].chunks_exact(8).take(n) {
                    out.push(u64::from_be_bytes(c.try_into().unwrap()));
                }
                Ok(out)
            }
            None => Ok(Vec::new()),
        }
    }

    fn put_neighbors(
        &self,
        txn: &mut Transaction<'_, P>,
        level: u8,
        doc: u64,
        ns: &[u64],
    ) -> Result<()> {
        let mut raw = Vec::with_capacity(2 + ns.len() * 8);
        raw.extend_from_slice(&(ns.len() as u16).to_le_bytes());
        for n in ns {
            raw.extend_from_slice(&n.to_be_bytes());
        }
        txn.insert_in(&self.tree, &ng_key(level, doc), &raw)
    }

    fn dist(&self, meta: &Meta, a: &[f32], b: &[f32]) -> f32 {
        1.0 - score(meta.cfg.metric, a, b)
    }

    /// Inserts a document vector (§10.5 insert path).
    pub fn insert(&self, txn: &mut Transaction<'_, P>, doc: u64, vec: &[f32]) -> Result<()> {
        let mut meta = self.meta(txn)?;
        if vec.len() != meta.cfg.dims as usize {
            return Err(Error::InvalidInput(format!(
                "vector dims {} != index dims {}",
                vec.len(),
                meta.cfg.dims
            )));
        }
        if txn.get_in(&self.tree, &vd_key(doc))?.is_some() {
            return Err(Error::InvalidInput(format!(
                "vector already indexed for doc {doc}"
            )));
        }

        // Node level: floor(-ln(u) * mL), mL = 1/ln(m).
        meta.rng = xorshift(meta.rng);
        let u = (meta.rng as f64) / (u64::MAX as f64);
        let level = (-u.ln() / (meta.cfg.m.max(2) as f64).ln()).floor() as u8;
        let level = level.min(15);

        txn.insert_in(
            &self.tree,
            &vd_key(doc),
            &EncodedVector::from_f32(vec).to_bytes(),
        )?;
        meta.count += 1;

        if meta.entry == NONE {
            meta.entry = doc;
            meta.entry_level = level;
            self.put_meta(txn, &meta)?;
            return Ok(());
        }

        let q = vec;
        let mut ep = meta.entry;
        // Greedy descent to just above the node's own level.
        for lvl in (level.saturating_add(1).max(1)..=meta.entry_level).rev() {
            ep = self.greedy_closest(txn, &meta, lvl, ep, q)?;
        }
        // Beam search + connect at each layer the node participates in.
        let top = level.min(meta.entry_level);
        for lvl in (0..=top).rev() {
            let cands = self.beam_search(txn, &meta, lvl, &[ep], q, meta.cfg.ef_construction)?;
            let m = if lvl == 0 { meta.cfg.m * 2 } else { meta.cfg.m };
            let chosen = self.select_neighbors(txn, &meta, q, &cands, m)?;
            self.put_neighbors(txn, lvl, doc, &chosen)?;
            // Reciprocal edges with pruning.
            for &n in &chosen {
                let mut ns = self.neighbors(txn, lvl, n)?;
                if !ns.contains(&doc) {
                    ns.push(doc);
                    if ns.len() > m {
                        let nvec = self.vector(txn, n)?;
                        let cands: Vec<Cand> = ns
                            .iter()
                            .map(|&d| {
                                let v = self.vector(txn, d).unwrap_or_default();
                                Cand {
                                    dist: self.dist(&meta, &nvec, &v),
                                    doc: d,
                                }
                            })
                            .collect();
                        ns = self.select_neighbors(txn, &meta, &nvec, &cands, m)?;
                    }
                    self.put_neighbors(txn, lvl, n, &ns)?;
                }
            }
            // Narrow the beam to the closest candidate for the next layer down.
            if let Some(best) = chosen
                .iter()
                .map(|&d| {
                    let v = self.vector(txn, d).unwrap_or_default();
                    Cand {
                        dist: self.dist(&meta, q, &v),
                        doc: d,
                    }
                })
                .min_by(|a, b| a.cmp(b))
            {
                ep = best.doc;
            }
        }
        if level > meta.entry_level {
            meta.entry = doc;
            meta.entry_level = level;
        }
        self.put_meta(txn, &meta)
    }

    /// Approximate k-NN. `filter` (when given) post-filters candidates —
    /// pass `ef > k` so enough survive (§10.5 post-filtering).
    pub fn search(
        &self,
        txn: &Transaction<'_, P>,
        q: &[f32],
        k: usize,
        ef: usize,
        filter: Option<&(dyn Fn(u64) -> bool + Send + Sync)>,
    ) -> Result<Vec<(u64, f32)>> {
        let meta = self.meta(txn)?;
        if q.len() != meta.cfg.dims as usize {
            return Err(Error::InvalidInput(format!(
                "query dims {} != index dims {}",
                q.len(),
                meta.cfg.dims
            )));
        }
        if meta.entry == NONE {
            return Ok(Vec::new());
        }
        let mut ep = meta.entry;
        for lvl in (1..=meta.entry_level).rev() {
            ep = self.greedy_closest(txn, &meta, lvl, ep, q)?;
        }
        let cands = self.beam_search(txn, &meta, 0, &[ep], q, ef.max(k))?;
        let mut hits: Vec<(u64, f32)> = Vec::new();
        for c in cands {
            if txn.get_in(&self.tree, &dx_key(c.doc))?.is_some() {
                continue; // tombstoned
            }
            if let Some(f) = filter
                && !f(c.doc)
            {
                continue;
            }
            let s = score(meta.cfg.metric, q, &self.vector(txn, c.doc)?);
            hits.push((c.doc, s));
            if hits.len() == k {
                break;
            }
        }
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        Ok(hits)
    }

    /// Marks a doc's vector deleted; tombstones are skipped at search and the
    /// vector record is dropped (a fresh insert resurrects the id). When the
    /// doc is the graph entry point a replacement is chosen — the live node
    /// with the highest top layer (the ideal entry per the paper).
    pub fn delete(&self, txn: &mut Transaction<'_, P>, doc: u64) -> Result<()> {
        let mut meta = self.meta(txn)?;
        if txn.get_in(&self.tree, &vd_key(doc))?.is_none() {
            return Ok(());
        }
        txn.insert_in(&self.tree, &dx_key(doc), &[])?;
        txn.delete_in(&self.tree, &vd_key(doc))?;
        meta.count = meta.count.saturating_sub(1);
        if meta.entry == doc {
            let (entry, level) = self.pick_entry(txn)?;
            meta.entry = entry;
            meta.entry_level = level;
        }
        self.put_meta(txn, &meta)
    }

    /// Scans `NG` keys for the live node with the highest top layer; falls
    /// back to any live vector at level 0, else `NONE`. One bounded scan —
    /// acceptable at delete time.
    fn pick_entry(&self, txn: &Transaction<'_, P>) -> Result<(u64, u8)> {
        let mut best: Option<(u64, u8)> = None;
        let mut any_live: Option<u64> = None;
        for (k, _v) in txn.scan_in(&self.tree)? {
            if k.starts_with(b"VD") {
                let doc = u64::from_be_bytes(k[2..10].try_into().unwrap());
                if txn.get_in(&self.tree, &dx_key(doc))?.is_none() {
                    any_live.get_or_insert(doc);
                }
            } else if k.starts_with(b"NG") {
                let doc = u64::from_be_bytes(k[3..11].try_into().unwrap());
                if txn.get_in(&self.tree, &dx_key(doc))?.is_some() {
                    continue;
                }
                let lvl = k[2];
                let better = match best {
                    None => true,
                    Some((bd, bl)) => lvl > bl || (lvl == bl && doc < bd),
                };
                if better {
                    best = Some((doc, lvl));
                }
            }
        }
        Ok(match best {
            Some((d, l)) => (d, l),
            None => (any_live.unwrap_or(NONE), 0),
        })
    }

    /// Update = delete + insert (§10.5).
    pub fn update(&self, txn: &mut Transaction<'_, P>, doc: u64, vec: &[f32]) -> Result<()> {
        self.delete(txn, doc)?;
        txn.delete_in(&self.tree, &dx_key(doc))?; // resurrect
        self.insert(txn, doc, vec)
    }

    /// Live vector count (tombstones excluded).
    pub fn count(&self, txn: &Transaction<'_, P>) -> Result<u64> {
        Ok(self.meta(txn)?.count)
    }

    /// Exact brute-force k-NN over all live vectors — for recall benchmarks
    /// and tiny collections.
    pub fn brute_force(
        &self,
        txn: &Transaction<'_, P>,
        q: &[f32],
        k: usize,
    ) -> Result<Vec<(u64, f32)>> {
        let meta = self.meta(txn)?;
        let mut hits: Vec<(u64, f32)> = Vec::new();
        for (key, raw) in txn.scan_in(&self.tree)? {
            if !key.starts_with(b"VD") {
                continue;
            }
            let doc = u64::from_be_bytes(key[2..10].try_into().unwrap());
            if txn.get_in(&self.tree, &dx_key(doc))?.is_some() {
                continue;
            }
            let v = EncodedVector::from_bytes(&raw)?.decode();
            hits.push((doc, score(meta.cfg.metric, q, &v)));
        }
        hits.sort_by(|a, b| b.1.total_cmp(&a.1));
        hits.truncate(k);
        Ok(hits)
    }

    // -- search internals -------------------------------------------------

    /// Greedy ef=1 walk at one layer — returns the closest reachable node.
    fn greedy_closest(
        &self,
        txn: &Transaction<'_, P>,
        meta: &Meta,
        level: u8,
        start: u64,
        q: &[f32],
    ) -> Result<u64> {
        let mut cur = start;
        let mut cur_d = self.dist(meta, q, &self.vector(txn, cur)?);
        loop {
            let mut moved = false;
            for n in self.neighbors(txn, level, cur)? {
                if txn.get_in(&self.tree, &dx_key(n))?.is_some() {
                    continue;
                }
                let d = self.dist(meta, q, &self.vector(txn, n)?);
                if d < cur_d {
                    cur = n;
                    cur_d = d;
                    moved = true;
                }
            }
            if !moved {
                return Ok(cur);
            }
        }
    }

    /// Beam search at one layer (the paper's `SEARCH-LAYER`).
    fn beam_search(
        &self,
        txn: &Transaction<'_, P>,
        meta: &Meta,
        level: u8,
        eps: &[u64],
        q: &[f32],
        ef: usize,
    ) -> Result<Vec<Cand>> {
        let mut visited: HashSet<u64> = eps.iter().copied().collect();
        // candidates: min-heap (nearest on top) via Reverse.
        let mut cand_heap: BinaryHeap<std::cmp::Reverse<Cand>> = BinaryHeap::new();
        // w: max-heap (worst on top).
        let mut w: BinaryHeap<Cand> = BinaryHeap::new();
        for &ep in eps {
            let d = self.dist(meta, q, &self.vector(txn, ep)?);
            cand_heap.push(std::cmp::Reverse(Cand { dist: d, doc: ep }));
            w.push(Cand { dist: d, doc: ep });
        }
        while let Some(std::cmp::Reverse(c)) = cand_heap.pop() {
            let worst = w.peek().map(|x| x.dist).unwrap_or(f32::MAX);
            if c.dist > worst && w.len() >= ef {
                break;
            }
            for n in self.neighbors(txn, level, c.doc)? {
                if !visited.insert(n) {
                    continue;
                }
                if txn.get_in(&self.tree, &dx_key(n))?.is_some() {
                    continue;
                }
                let d = self.dist(meta, q, &self.vector(txn, n)?);
                let worst = w.peek().map(|x| x.dist).unwrap_or(f32::MAX);
                if d < worst || w.len() < ef {
                    cand_heap.push(std::cmp::Reverse(Cand { dist: d, doc: n }));
                    w.push(Cand { dist: d, doc: n });
                    if w.len() > ef {
                        w.pop();
                    }
                }
            }
        }
        Ok(w.into_sorted_vec())
    }

    /// Neighbour selection heuristic (paper alg. 4): keep candidates that
    /// are closer to the node than to every already-selected neighbour —
    /// preserves graph diversity over a plain nearest-M.
    fn select_neighbors(
        &self,
        txn: &Transaction<'_, P>,
        meta: &Meta,
        node_vec: &[f32],
        cands: &[Cand],
        m: usize,
    ) -> Result<Vec<u64>> {
        let mut sorted: Vec<Cand> = cands.to_vec();
        sorted.sort_by(|a, b| a.dist.total_cmp(&b.dist));
        let mut picked: Vec<Cand> = Vec::with_capacity(m);
        let mut vecs: Vec<Vec<f32>> = Vec::with_capacity(m);
        let _ = node_vec;
        for c in sorted {
            if picked.len() >= m {
                break;
            }
            let cvec = self.vector(txn, c.doc)?;
            // Reject c if a picked neighbour is closer to c than c is to the
            // node — keeps the list spatially diverse.
            let dominated = vecs.iter().any(|pv| self.dist(meta, &cvec, pv) < c.dist);
            if !dominated {
                picked.push(c);
                vecs.push(cvec);
            }
        }
        Ok(picked.into_iter().map(|c| c.doc).collect())
    }
}

// ---- key encoding --------------------------------------------------------

fn vd_key(doc: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"VD");
    k.extend_from_slice(&doc.to_be_bytes());
    k
}

fn ng_key(level: u8, doc: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(11);
    k.extend_from_slice(b"NG");
    k.push(level);
    k.extend_from_slice(&doc.to_be_bytes());
    k
}

fn dx_key(doc: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.extend_from_slice(b"DX");
    k.extend_from_slice(&doc.to_be_bytes());
    k
}

fn xorshift(mut x: u64) -> u64 {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_codec() {
        let m = Meta {
            cfg: HnswConfig {
                dims: 8,
                metric: Metric::Euclidean,
                m: 8,
                ef_construction: 40,
                ef_search: 20,
            },
            count: 42,
            entry: 7,
            entry_level: 3,
            rng: 99,
        };
        let d = Meta::decode(&m.encode()).unwrap();
        assert_eq!(d.count, 42);
        assert_eq!(d.entry, 7);
        assert_eq!(d.cfg.dims, 8);
        assert_eq!(d.cfg.metric, Metric::Euclidean);
    }
}
