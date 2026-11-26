//! Slotted-page B+Tree node layout inside a [`Page`] body (spec §5.3).
//!
//! ```text
//! Node header (24 bytes at body start)
//! ────────────────────────────────────
//! 0    u8   tag: LEAF_TAG or INTERNAL_TAG
//! 1    u8   flags
//! 2    u16  key_count
//! 4    u16  cell_start — offset where packed cells begin (grows downward)
//! 6    u16  reserved
//! 8    u64  leaf: next sibling page · internal: leftmost child page
//! 16   u64  reserved
//!
//! Slot array: key_count × u16 body-relative cell offsets at offset 24,
//! sorted by key (binary search).
//!
//! Cells packed downward from the end of the body:
//!   leaf cell:     [u16 klen][u32 vlen][key][val]
//!   internal cell: [u64 child][u16 klen][key]
//!   internal semantics: cell i = (key_i, child_{i+1}); child_{i+1} holds
//!   keys >= key_i. Leftmost child (index 0) lives in the header.
//! ```
//!
//! Deletes leave dead cell bytes until [`compact`] repacks the page — the
//! slot array is the source of truth for "live" cells.

use ndb_common::error::Error;
use ndb_common::ids::PageId;

pub const LEAF_TAG: u8 = 0x0A;
pub const INTERNAL_TAG: u8 = 0x0B;

pub const NODE_HEADER: usize = 24;
const OFF_COUNT: usize = 2;
const OFF_CELL_START: usize = 4;
const OFF_LINK_A: usize = 8;

const LEAF_CELL_HDR: usize = 6; // klen u16 + vlen u32
const INTERNAL_CELL_HDR: usize = 10; // child u64 + klen u16
const SLOT_SIZE: usize = 2;

/// Maximum key length — bounds the worst-case separator cell so
/// latch-coupling safety checks can be computed (ADR-005).
pub const MAX_KEY_SIZE: usize = 512;

fn corruption(msg: impl Into<String>) -> Error {
    Error::Corruption(msg.into())
}

fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(b[off..off + 2].try_into().unwrap())
}
fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}
fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}
fn put_u16(b: &mut [u8], off: usize, v: u16) {
    b[off..off + 2].copy_from_slice(&v.to_le_bytes());
}
fn put_u32(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}
fn put_u64(b: &mut [u8], off: usize, v: u64) {
    b[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

// ── shared header access ──────────────────────────────────────────────────

pub fn tag(b: &[u8]) -> u8 {
    b[0]
}
pub fn is_leaf(b: &[u8]) -> bool {
    tag(b) == LEAF_TAG
}
pub fn is_internal(b: &[u8]) -> bool {
    tag(b) == INTERNAL_TAG
}
pub fn key_count(b: &[u8]) -> usize {
    u16_at(b, OFF_COUNT) as usize
}
fn set_key_count(b: &mut [u8], n: usize) {
    put_u16(b, OFF_COUNT, n as u16);
}
fn cell_start(b: &[u8]) -> usize {
    u16_at(b, OFF_CELL_START) as usize
}
fn set_cell_start(b: &mut [u8], v: usize) {
    put_u16(b, OFF_CELL_START, v as u16);
}
fn slot_off(b: &[u8], i: usize) -> usize {
    u16_at(b, NODE_HEADER + i * SLOT_SIZE) as usize
}
fn set_slot(b: &mut [u8], i: usize, off: usize) {
    put_u16(b, NODE_HEADER + i * SLOT_SIZE, off as u16);
}
fn link_a(b: &[u8]) -> u64 {
    u64_at(b, OFF_LINK_A)
}
fn set_link_a(b: &mut [u8], v: u64) {
    put_u64(b, OFF_LINK_A, v);
}

/// Bytes between the slot array end and packed cells — insertable without
/// compaction.
fn free_space(b: &[u8]) -> usize {
    cell_start(b).saturating_sub(NODE_HEADER + key_count(b) * SLOT_SIZE)
}

fn init_common(b: &mut [u8], tag: u8) {
    b[..NODE_HEADER].fill(0);
    b[0] = tag;
    set_cell_start(b, b.len());
}

/// Binary search over the sorted slot keys. `cell_key` extracts the key from
/// a cell at a given body offset.
fn find_slot(
    b: &[u8],
    key: &[u8],
    cell_key: impl Fn(&[u8], usize) -> &[u8],
) -> Result<usize, usize> {
    let n = key_count(b);
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let k = cell_key(b, slot_off(b, mid));
        match k.cmp(key) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(mid),
        }
    }
    Err(lo)
}

/// Inserts slot `pos` pointing at `cell_off`; writes the cell bytes into the
/// free region. Caller must have ensured space (via `ensure_space`).
fn insert_slot(b: &mut [u8], pos: usize, cell_off: usize, cell_size: usize) {
    let n = key_count(b);
    let new_start = cell_off;
    // Shift slots [pos..n) right by one.
    let slot_base = NODE_HEADER;
    b.copy_within(
        slot_base + pos * SLOT_SIZE..slot_base + n * SLOT_SIZE,
        slot_base + (pos + 1) * SLOT_SIZE,
    );
    set_slot(b, pos, new_start);
    set_key_count(b, n + 1);
    let _ = cell_size;
}

fn remove_slot(b: &mut [u8], idx: usize) {
    let n = key_count(b);
    let slot_base = NODE_HEADER;
    b.copy_within(
        slot_base + (idx + 1) * SLOT_SIZE..slot_base + n * SLOT_SIZE,
        slot_base + idx * SLOT_SIZE,
    );
    set_key_count(b, n - 1);
}

/// Ensures `needed` bytes are available, compacting the page if dead cell
/// space can satisfy the request. Returns whether space exists afterwards.
fn ensure_space(b: &mut [u8], needed: usize, cell_size_fn: impl Fn(&[u8], usize) -> usize) -> bool {
    if free_space(b) >= needed {
        return true;
    }
    compact(b, cell_size_fn);
    free_space(b) >= needed
}

/// Repacks live cells contiguously at the end of the body, reclaiming dead
/// space left by deletes.
fn compact(b: &mut [u8], cell_size_fn: impl Fn(&[u8], usize) -> usize) {
    let n = key_count(b);
    let mut offsets: Vec<(usize, usize)> = (0..n).map(|i| slot_off(b, i)).zip(0..n).collect();
    offsets.sort_unstable(); // by cell offset — they are packed downward
    let mut cursor = b.len();
    let mut new_slots = vec![0u16; n];
    // Collect cells in descending offset order so rewriting is safe.
    let cells: Vec<Vec<u8>> = offsets
        .iter()
        .map(|&(off, _)| {
            let sz = cell_size_fn(b, off);
            b[off..off + sz].to_vec()
        })
        .collect();
    for ((_, slot_idx), cell) in offsets.iter().zip(cells.iter()) {
        cursor -= cell.len();
        new_slots[*slot_idx] = cursor as u16;
    }
    let mut write_at = b.len();
    for cell in &cells {
        write_at -= cell.len();
        b[write_at..write_at + cell.len()].copy_from_slice(cell);
    }
    for (i, off) in new_slots.iter().enumerate() {
        put_u16(b, NODE_HEADER + i * SLOT_SIZE, *off);
    }
    set_cell_start(b, cursor);
}

/// Rewrites the whole node from sorted entries. `cell_size_fn` sizes one
/// entry; `write_cell_fn` serialises it at an offset. Returns false if the
/// entries don't fit the page.
fn rebuild<T>(
    b: &mut [u8],
    entries: &[T],
    size: impl Fn(&T) -> usize,
    write: impl Fn(&mut [u8], usize, &T),
) -> bool {
    let total: usize =
        NODE_HEADER + entries.len() * SLOT_SIZE + entries.iter().map(&size).sum::<usize>();
    if total > b.len() {
        return false;
    }
    let mut cursor = b.len();
    for (i, e) in entries.iter().enumerate() {
        cursor -= size(e);
        write(b, cursor, e);
        set_slot(b, i, cursor);
    }
    set_key_count(b, entries.len());
    set_cell_start(b, cursor);
    true
}

// ── leaf node ─────────────────────────────────────────────────────────────

pub mod leaf {
    use super::*;

    pub fn init(b: &mut [u8]) {
        init_common(b, LEAF_TAG);
        set_link_a(b, 0); // next = none
    }

    /// Right sibling (0 = none). Leaves are singly-linked for range scans.
    pub fn next(b: &[u8]) -> PageId {
        PageId::new(link_a(b))
    }
    pub fn set_next(b: &mut [u8], next: PageId) {
        set_link_a(b, next.get());
    }

    fn cell_size_at(b: &[u8], off: usize) -> usize {
        let klen = u16_at(b, off) as usize;
        let vlen = u32_at(b, off + 2) as usize;
        LEAF_CELL_HDR + klen + vlen
    }

    pub fn key(b: &[u8], i: usize) -> &[u8] {
        let off = slot_off(b, i);
        let klen = u16_at(b, off) as usize;
        &b[off + LEAF_CELL_HDR..off + LEAF_CELL_HDR + klen]
    }

    pub fn val(b: &[u8], i: usize) -> &[u8] {
        let off = slot_off(b, i);
        let klen = u16_at(b, off) as usize;
        let vlen = u32_at(b, off + 2) as usize;
        &b[off + LEAF_CELL_HDR + klen..off + LEAF_CELL_HDR + klen + vlen]
    }

    /// `Ok(index)` when `key` exists, `Err(insert_pos)` otherwise.
    pub fn find(b: &[u8], key: &[u8]) -> Result<usize, usize> {
        find_slot(b, key, |b, off| {
            let klen = u16_at(b, off) as usize;
            &b[off + LEAF_CELL_HDR..off + LEAF_CELL_HDR + klen]
        })
    }

    /// Bytes needed to insert `(key, val)`: slot + cell.
    pub fn needed(klen: usize, vlen: usize) -> usize {
        SLOT_SIZE + LEAF_CELL_HDR + klen + vlen
    }

    /// Whether `(key, val)` fits without splitting (compact is tried).
    pub fn fits(b: &[u8], klen: usize, vlen: usize) -> bool {
        // Peek: free_space without mutating — compaction might recover.
        let mut probe = b.to_vec();
        ensure_space(&mut probe, needed(klen, vlen), cell_size_at)
    }

    /// Inserts or replaces at `pos`. Assumes space was checked via
    /// [`fits`]/[`ensure`] — may compact the page.
    pub fn insert_at(b: &mut [u8], pos: usize, key: &[u8], val: &[u8]) -> ndb_common::Result<()> {
        let needed = needed(key.len(), val.len());
        if !ensure_space(b, needed, cell_size_at) {
            return Err(corruption("leaf: insufficient space"));
        }
        let start = cell_start(b) - (LEAF_CELL_HDR + key.len() + val.len());
        put_u16(b, start, key.len() as u16);
        put_u32(b, start + 2, val.len() as u32);
        b[start + LEAF_CELL_HDR..start + LEAF_CELL_HDR + key.len()].copy_from_slice(key);
        b[start + LEAF_CELL_HDR + key.len()..start + LEAF_CELL_HDR + key.len() + val.len()]
            .copy_from_slice(val);
        set_cell_start(b, start);
        insert_slot(b, pos, start, 0);
        Ok(())
    }

    /// Removes the entry at `idx`; the cell bytes become dead space until
    /// the next compaction.
    pub fn remove_at(b: &mut [u8], idx: usize) {
        remove_slot(b, idx)
    }

    /// All `(key, value)` pairs in slot order.
    pub fn entries(b: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..key_count(b))
            .map(|i| (key(b, i).to_vec(), val(b, i).to_vec()))
            .collect()
    }

    /// Rewrites the leaf from sorted entries; preserves the `next` link.
    /// Returns false if entries don't fit.
    pub fn rebuild(b: &mut [u8], entries: &[(Vec<u8>, Vec<u8>)]) -> bool {
        let next = next(b);
        init(b);
        set_next(b, next);
        super::rebuild(
            b,
            entries,
            |(k, v)| LEAF_CELL_HDR + k.len() + v.len(),
            |b, off, (k, v)| {
                put_u16(b, off, k.len() as u16);
                put_u32(b, off + 2, v.len() as u32);
                b[off + LEAF_CELL_HDR..off + LEAF_CELL_HDR + k.len()].copy_from_slice(k);
                b[off + LEAF_CELL_HDR + k.len()..off + LEAF_CELL_HDR + k.len() + v.len()]
                    .copy_from_slice(v);
            },
        )
    }
}

// ── internal node ─────────────────────────────────────────────────────────

pub mod internal {
    use super::*;

    /// Initialises an internal node with its leftmost child.
    pub fn init(b: &mut [u8], leftmost: PageId) {
        init_common(b, INTERNAL_TAG);
        set_link_a(b, leftmost.get());
    }

    /// Child index 0 (keys below the first separator).
    pub fn leftmost(b: &[u8]) -> PageId {
        PageId::new(link_a(b))
    }
    pub fn set_leftmost(b: &mut [u8], id: PageId) {
        set_link_a(b, id.get());
    }

    fn cell_size_at(b: &[u8], off: usize) -> usize {
        INTERNAL_CELL_HDR + u16_at(b, off + 8) as usize
    }

    /// Separator key of cell `i`.
    pub fn key(b: &[u8], i: usize) -> &[u8] {
        let off = slot_off(b, i);
        let klen = u16_at(b, off + 8) as usize;
        &b[off + INTERNAL_CELL_HDR..off + INTERNAL_CELL_HDR + klen]
    }

    /// Child page of cell `i` (= child_{i+1}).
    pub fn cell_child(b: &[u8], i: usize) -> PageId {
        PageId::new(u64_at(b, slot_off(b, i)))
    }

    /// All children in order: leftmost then each cell's child.
    pub fn children(b: &[u8]) -> Vec<PageId> {
        let mut v = Vec::with_capacity(key_count(b) + 1);
        v.push(leftmost(b));
        for i in 0..key_count(b) {
            v.push(cell_child(b, i));
        }
        v
    }

    /// All `(separator_key, child)` cells in slot order.
    pub fn entries(b: &[u8]) -> Vec<(Vec<u8>, PageId)> {
        (0..key_count(b))
            .map(|i| (key(b, i).to_vec(), cell_child(b, i)))
            .collect()
    }

    /// Routes `key` to a child page: the last child whose separator ≤ key,
    /// or the leftmost child.
    pub fn find_child(b: &[u8], key: &[u8]) -> PageId {
        // Count separators ≤ key: that is the target child index.
        let n = key_count(b);
        let mut lo = 0usize;
        let mut hi = n;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self_key(b, mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        // lo = child index: 0 → leftmost, i → cell_{i-1}.child
        if lo == 0 {
            leftmost(b)
        } else {
            cell_child(b, lo - 1)
        }
    }

    fn self_key(b: &[u8], i: usize) -> &[u8] {
        key(b, i)
    }

    /// The index of `child` among children (0 = leftmost), used to find
    /// sibling relationships.
    pub fn child_index(b: &[u8], child: PageId) -> Option<usize> {
        if leftmost(b) == child {
            return Some(0);
        }
        (0..key_count(b))
            .find(|&i| cell_child(b, i) == child)
            .map(|i| i + 1)
    }

    /// Bytes needed for one `(key, child)` cell + slot.
    pub fn needed(klen: usize) -> usize {
        SLOT_SIZE + INTERNAL_CELL_HDR + klen
    }

    pub fn fits(b: &[u8], klen: usize) -> bool {
        free_space(b) >= needed(klen) || {
            let mut probe = b.to_vec();
            ensure_space(&mut probe, needed(klen), cell_size_at)
        }
    }

    /// Inserts `(key, child)` at sorted position `pos`.
    pub fn insert_at(
        b: &mut [u8],
        pos: usize,
        key: &[u8],
        child: PageId,
    ) -> ndb_common::Result<()> {
        let needed = needed(key.len());
        if !ensure_space(b, needed, cell_size_at) {
            return Err(corruption("internal: insufficient space"));
        }
        let start = cell_start(b) - (INTERNAL_CELL_HDR + key.len());
        put_u64(b, start, child.get());
        put_u16(b, start + 8, key.len() as u16);
        b[start + INTERNAL_CELL_HDR..start + INTERNAL_CELL_HDR + key.len()].copy_from_slice(key);
        set_cell_start(b, start);
        insert_slot(b, pos, start, 0);
        Ok(())
    }

    /// Removes cell `idx` (its separator AND its child pointer).
    pub fn remove_at(b: &mut [u8], idx: usize) {
        remove_slot(b, idx)
    }

    /// Replaces cell `idx`'s child pointer (separator unchanged).
    pub fn set_cell_child(b: &mut [u8], idx: usize, child: PageId) {
        put_u64(b, slot_off(b, idx), child.get());
    }

    /// Insert position for `key` among separators (keeps them sorted).
    pub fn find_pos(b: &[u8], key: &[u8]) -> usize {
        match find_slot(b, key, |b, off| {
            let klen = u16_at(b, off + 8) as usize;
            &b[off + INTERNAL_CELL_HDR..off + INTERNAL_CELL_HDR + klen]
        }) {
            Ok(i) | Err(i) => i,
        }
    }

    /// Rewrites the internal node from a leftmost child + sorted cells.
    /// Returns false if they don't fit.
    pub fn rebuild(b: &mut [u8], leftmost: PageId, entries: &[(Vec<u8>, PageId)]) -> bool {
        init(b, leftmost);
        super::rebuild(
            b,
            entries,
            |(k, _)| INTERNAL_CELL_HDR + k.len(),
            |b, off, (k, c)| {
                put_u64(b, off, c.get());
                put_u16(b, off + 8, k.len() as u16);
                b[off + INTERNAL_CELL_HDR..off + INTERNAL_CELL_HDR + k.len()].copy_from_slice(k);
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body() -> Vec<u8> {
        vec![0u8; 1024]
    }

    #[test]
    fn leaf_insert_find_remove() {
        let mut b = body();
        leaf::init(&mut b);
        leaf::insert_at(&mut b, 0, b"alpha", b"1").unwrap();
        leaf::insert_at(&mut b, 1, b"beta", b"22").unwrap();
        leaf::insert_at(&mut b, 1, b"apple", b"3").unwrap();
        assert_eq!(key_count(&b), 3);
        assert_eq!(leaf::find(&b, b"alpha"), Ok(0));
        assert_eq!(leaf::find(&b, b"apple"), Ok(1));
        assert_eq!(leaf::find(&b, b"beta"), Ok(2));
        assert_eq!(leaf::find(&b, b"zzz"), Err(3));
        assert_eq!(leaf::val(&b, 1), b"3");

        leaf::remove_at(&mut b, 1);
        assert_eq!(key_count(&b), 2);
        assert_eq!(leaf::find(&b, b"apple"), Err(1));
    }

    #[test]
    fn leaf_rebuild_and_next() {
        let mut b = body();
        leaf::init(&mut b);
        leaf::set_next(&mut b, PageId::new(9));
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..5)
            .map(|i| (format!("k{i}").into_bytes(), vec![i]))
            .collect();
        assert!(leaf::rebuild(&mut b, &entries));
        assert_eq!(key_count(&b), 5);
        assert_eq!(leaf::next(&b), PageId::new(9));
        assert_eq!(leaf::key(&b, 4), b"k4");
    }

    #[test]
    fn internal_routing() {
        let mut b = body();
        internal::init(&mut b, PageId::new(100));
        internal::insert_at(&mut b, 0, b"m", PageId::new(101)).unwrap();
        internal::insert_at(&mut b, 1, b"t", PageId::new(102)).unwrap();
        // children: 100 | 101(>=m) | 102(>=t)
        assert_eq!(internal::find_child(&b, b"a"), PageId::new(100));
        assert_eq!(internal::find_child(&b, b"m"), PageId::new(101));
        assert_eq!(internal::find_child(&b, b"s"), PageId::new(101));
        assert_eq!(internal::find_child(&b, b"t"), PageId::new(102));
        assert_eq!(internal::find_child(&b, b"zzz"), PageId::new(102));
        assert_eq!(
            internal::children(&b),
            vec![PageId::new(100), PageId::new(101), PageId::new(102)]
        );
        assert_eq!(internal::child_index(&b, PageId::new(101)), Some(1));
        assert_eq!(internal::child_index(&b, PageId::new(100)), Some(0));
    }

    #[test]
    fn internal_rebuild() {
        let mut b = body();
        internal::init(&mut b, PageId::new(1));
        internal::insert_at(&mut b, 0, b"x", PageId::new(2)).unwrap();
        let entries = internal::entries(&b);
        assert_eq!(entries, vec![(b"x".to_vec(), PageId::new(2))]);
        let mut b2 = body();
        assert!(internal::rebuild(&mut b2, PageId::new(9), &entries));
        assert_eq!(internal::leftmost(&b2), PageId::new(9));
        assert_eq!(internal::find_child(&b2, b"y"), PageId::new(2));
    }
}
