//! Page structures and checksums (spec §5.2).
//!
//! ```text
//! Page layout (header = 40 bytes, body = rest of page)
//! ────────────────────────────────────────────────────
//! offset  size  field
//! 0       2     magic = "NP" (0x4E50)
//! 2       2     format_version
//! 4       1     page_type
//! 5       1     flags
//! 6       2     reserved
//! 8       8     page_id
//! 16      8     generation
//! 24      8     lsn
//! 32      4     checksum (CRC32 over the whole page, this field zeroed)
//! 36      4     free_space (page-type-specific interpretation)
//! ```
//!
//! All integers are little-endian. Checksums detect corruption (spec §5.2);
//! page generations protect against stale references when pages are reused.

use ndb_common::error::Error;
use ndb_common::ids::{Lsn, PageId};
use ndb_common::version::FormatVersion;

/// Default page size (spec §5.2 — 8/16/32 KB to be benchmarked before
/// finalising).
pub const DEFAULT_PAGE_SIZE: u32 = 16 * 1024;

/// Size of the page header in bytes.
pub const HEADER_SIZE: usize = 40;

const MAGIC: u16 = 0x4E50; // "NP"

// Header field offsets.
const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 2;
const OFF_TYPE: usize = 4;
const OFF_FLAGS: usize = 5;
const OFF_PAGE_ID: usize = 8;
const OFF_GENERATION: usize = 16;
const OFF_LSN: usize = 24;
const OFF_CHECKSUM: usize = 32;
const OFF_FREE_SPACE: usize = 36;

/// What a page is used for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// Zero-initialised page that has never been written.
    Uninitialised = 0,
    /// Page 0: file metadata (page size, high-water mark, extent config).
    Meta = 1,
    /// A freed page awaiting reuse.
    Free = 2,
    /// B+Tree internal node (spec §5.3).
    BTreeInternal = 3,
    /// B+Tree leaf node (spec §5.3).
    BTreeLeaf = 4,
    /// Overflow storage for values larger than one page.
    Overflow = 5,
    /// Document record blob (continuation chain for large documents).
    Doc = 6,
}

impl PageType {
    fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Uninitialised,
            1 => Self::Meta,
            2 => Self::Free,
            3 => Self::BTreeInternal,
            4 => Self::BTreeLeaf,
            5 => Self::Overflow,
            6 => Self::Doc,
            _ => return None,
        })
    }
}

/// The fields of a page header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    pub page_id: PageId,
    pub page_type: PageType,
    pub format_version: FormatVersion,
    /// Incremented each time a page is recycled so stale references fail.
    pub generation: u64,
    pub lsn: Lsn,
    pub flags: u8,
    /// Free-space accounting; interpretation is page-type-specific.
    pub free_space: u32,
}

/// A fixed-size page: a boxed buffer exactly `page_size` bytes long.
#[derive(Debug)]
pub struct Page {
    data: Box<[u8]>,
}

impl Page {
    /// Allocates a zeroed page.
    pub fn new(page_size: u32) -> Self {
        Self {
            data: vec![0; page_size as usize].into_boxed_slice(),
        }
    }

    /// Wraps an existing buffer, which must be exactly `page_size` bytes.
    pub fn from_bytes(data: Box<[u8]>, page_size: u32) -> ndb_common::Result<Self> {
        if data.len() != page_size as usize {
            return Err(Error::InvalidInput(format!(
                "page buffer has {} bytes, expected {page_size}",
                data.len()
            )));
        }
        Ok(Self { data })
    }

    /// Page size in bytes.
    pub fn size(&self) -> u32 {
        self.data.len() as u32
    }

    /// The page body (everything after the header).
    pub fn body(&self) -> &[u8] {
        &self.data[HEADER_SIZE..]
    }

    /// Mutable page body.
    pub fn body_mut(&mut self) -> &mut [u8] {
        &mut self.data[HEADER_SIZE..]
    }

    /// Raw access to the whole page buffer.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Raw mutable access — callers are responsible for maintaining the
    /// checksum; use [`Page::refresh_checksum`] after modifying.
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Initialises the header of a fresh page and computes its checksum.
    pub fn init(&mut self, page_id: PageId, page_type: PageType, generation: u64) {
        self.write_header(PageHeader {
            page_id,
            page_type,
            format_version: FormatVersion::V1,
            generation,
            lsn: Lsn::new(0),
            flags: 0,
            free_space: self.body().len() as u32,
        });
    }

    /// Reads the header fields. Does not verify the checksum — see
    /// [`Page::verify`].
    pub fn header(&self) -> ndb_common::Result<PageHeader> {
        let b = &self.data;
        if u16::from_le_bytes(b[OFF_MAGIC..OFF_MAGIC + 2].try_into().unwrap()) != MAGIC {
            return Err(Error::Corruption("page: bad magic".into()));
        }
        let version = u16::from_le_bytes(b[OFF_VERSION..OFF_VERSION + 2].try_into().unwrap());
        if !FormatVersion::V1.can_read(FormatVersion::new(version)) {
            return Err(Error::Corruption(format!(
                "page: unsupported format version {version}"
            )));
        }
        let page_type = PageType::from_tag(b[OFF_TYPE])
            .ok_or_else(|| Error::Corruption(format!("page: bad type {}", b[OFF_TYPE])))?;
        Ok(PageHeader {
            page_id: PageId::new(u64::from_le_bytes(
                b[OFF_PAGE_ID..OFF_PAGE_ID + 8].try_into().unwrap(),
            )),
            generation: u64::from_le_bytes(
                b[OFF_GENERATION..OFF_GENERATION + 8].try_into().unwrap(),
            ),
            lsn: Lsn::new(u64::from_le_bytes(
                b[OFF_LSN..OFF_LSN + 8].try_into().unwrap(),
            )),
            flags: b[OFF_FLAGS],
            free_space: u32::from_le_bytes(
                b[OFF_FREE_SPACE..OFF_FREE_SPACE + 4].try_into().unwrap(),
            ),
            format_version: FormatVersion::new(version),
            page_type,
        })
    }

    /// Writes header fields and recomputes the checksum.
    pub fn write_header(&mut self, header: PageHeader) {
        let b = &mut self.data;
        b[OFF_MAGIC..OFF_MAGIC + 2].copy_from_slice(&MAGIC.to_le_bytes());
        b[OFF_VERSION..OFF_VERSION + 2].copy_from_slice(&header.format_version.get().to_le_bytes());
        b[OFF_TYPE] = header.page_type as u8;
        b[OFF_FLAGS] = header.flags;
        b[OFF_PAGE_ID..OFF_PAGE_ID + 8].copy_from_slice(&header.page_id.get().to_le_bytes());
        b[OFF_GENERATION..OFF_GENERATION + 8].copy_from_slice(&header.generation.to_le_bytes());
        b[OFF_LSN..OFF_LSN + 8].copy_from_slice(&header.lsn.get().to_le_bytes());
        b[OFF_FREE_SPACE..OFF_FREE_SPACE + 4].copy_from_slice(&header.free_space.to_le_bytes());
        self.refresh_checksum();
    }

    /// Recomputes and stores the checksum after direct buffer modification.
    pub fn refresh_checksum(&mut self) {
        self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4].fill(0);
        let crc = compute_checksum(&self.data);
        self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4].copy_from_slice(&crc.to_le_bytes());
    }

    /// Verifies magic, version and checksum. Does not check the page's
    /// identity — callers comparing `page_id` must do so themselves.
    pub fn verify(&self) -> ndb_common::Result<()> {
        let stored = u32::from_le_bytes(
            self.data[OFF_CHECKSUM..OFF_CHECKSUM + 4]
                .try_into()
                .unwrap(),
        );
        let mut copy = self.data.to_vec();
        copy[OFF_CHECKSUM..OFF_CHECKSUM + 4].fill(0);
        if compute_checksum(&copy) != stored {
            return Err(Error::Corruption(format!(
                "page {}: checksum mismatch",
                self.header().map(|h| h.page_id.get()).unwrap_or(u64::MAX)
            )));
        }
        self.header().map(|_| ())
    }
}

fn compute_checksum(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let mut page = Page::new(DEFAULT_PAGE_SIZE);
        page.init(PageId::new(3), PageType::BTreeLeaf, 1);
        let h = page.header().unwrap();
        assert_eq!(h.page_id, PageId::new(3));
        assert_eq!(h.page_type, PageType::BTreeLeaf);
        assert_eq!(h.generation, 1);
        assert_eq!(h.format_version, FormatVersion::V1);
        assert_eq!(
            h.free_space,
            (DEFAULT_PAGE_SIZE as usize - HEADER_SIZE) as u32
        );
        page.verify().unwrap();
    }

    #[test]
    fn detects_corruption() {
        let mut page = Page::new(DEFAULT_PAGE_SIZE);
        page.init(PageId::new(1), PageType::BTreeInternal, 1);
        page.as_bytes_mut()[100] ^= 0xFF;
        assert!(matches!(page.verify(), Err(Error::Corruption(_))));
    }

    #[test]
    fn checksum_refresh_after_body_write() {
        let mut page = Page::new(DEFAULT_PAGE_SIZE);
        page.init(PageId::new(1), PageType::Overflow, 1);
        page.body_mut()[0] = 0xAB;
        page.refresh_checksum();
        page.verify().unwrap();
    }

    #[test]
    fn rejects_bad_buffer_size() {
        assert!(Page::from_bytes(vec![0; 100].into(), DEFAULT_PAGE_SIZE).is_err());
    }
}
