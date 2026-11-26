//! Page manager (spec §5.2): allocates, reads, writes and releases pages on
//! top of a [`PageIo`] backend, with extent-based file growth.
//!
//! Page 0 is the meta page (`PageType::Meta`). Its body stores:
//!
//! ```text
//! offset  size  field
//! 0       4     meta magic = "NDMF" (0x4D46_444E)
//! 4       4     page_size
//! 8       4     extent_pages
//! 12      4     reserved
//! 16      8     high_water (next never-allocated page id)
//! ```

use crate::alloc::PageAllocator;
use crate::page::{DEFAULT_PAGE_SIZE, Page, PageType};
use ndb_common::error::Error;
use ndb_common::ids::PageId;

const META_MAGIC: u32 = 0x4D46_444E; // "NDMF"
/// Smallest supported page size — used to bootstrap-read the meta page before
/// the real page size is known.
const MIN_PAGE_SIZE: u32 = 4096;
const MAX_PAGE_SIZE: u32 = 65536;

/// Positioned page I/O — implemented by `ndb-storage`'s `StorageFile` and by
/// in-memory test backends.
pub trait PageIo {
    /// Reads exactly `buf.len()` bytes at `offset`.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> ndb_common::Result<()>;
    /// Writes all of `buf` at `offset`.
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> ndb_common::Result<()>;
    /// Current length of the backing store in bytes.
    fn len(&mut self) -> ndb_common::Result<u64>;
    /// Whether the backing store is empty.
    fn is_empty(&mut self) -> ndb_common::Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Grows (or shrinks) the backing store to `len` bytes.
    fn set_len(&mut self, len: u64) -> ndb_common::Result<()>;
    /// Flushes durable state to media.
    fn sync(&mut self) -> ndb_common::Result<()>;
}

/// Manages pages over a [`PageIo`] backend.
pub struct PageManager<P: PageIo> {
    io: P,
    page_size: u32,
    extent_pages: u32,
    allocator: PageAllocator,
}

impl<P: PageIo> PageManager<P> {
    /// Formats a fresh file: writes the meta page. The backing store must be
    /// empty (`len == 0`).
    pub fn create(mut io: P, page_size: u32, extent_pages: u32) -> ndb_common::Result<Self> {
        validate_page_size(page_size)?;
        if extent_pages == 0 {
            return Err(Error::InvalidInput("extent_pages must be non-zero".into()));
        }
        if io.len()? != 0 {
            return Err(Error::InvalidInput(
                "cannot create page file over non-empty storage".into(),
            ));
        }
        let mut mgr = Self {
            io,
            page_size,
            extent_pages,
            allocator: PageAllocator::new(1),
        };
        mgr.write_meta_page()?;
        Ok(mgr)
    }

    /// Opens an existing page file, validating the meta page and restoring
    /// the allocation high-water mark.
    ///
    /// The free list is not persisted (Phase-1 limitation) — reopening
    /// forgets freed pages.
    pub fn open(mut io: P) -> ndb_common::Result<Self> {
        let len = io.len()?;
        if len < MIN_PAGE_SIZE as u64 {
            return Err(Error::Corruption("page file too small".into()));
        }
        // Read a minimum-size prefix to learn the real page size. The header
        // sits at offset 0 regardless of page size, but the checksum cannot
        // be verified yet — it covers the whole page.
        let mut probe = Page::new(MIN_PAGE_SIZE);
        io.read_at(0, &mut probe.as_bytes_mut()[..])?;
        let header = probe.header()?;
        if header.page_type != PageType::Meta {
            return Err(Error::Corruption("page 0 is not a meta page".into()));
        }
        let page_size = u32::from_le_bytes(probe.body()[4..8].try_into().unwrap());
        validate_page_size(page_size)?;

        // Re-read at the true page size and verify fully.
        let mut meta = Page::new(page_size);
        io.read_at(0, &mut meta.as_bytes_mut()[..])?;
        meta.verify()?;
        let body = meta.body();
        if u32::from_le_bytes(body[0..4].try_into().unwrap()) != META_MAGIC {
            return Err(Error::Corruption("meta page: bad magic".into()));
        }
        let extent_pages = u32::from_le_bytes(body[8..12].try_into().unwrap());
        let high_water = u64::from_le_bytes(body[16..24].try_into().unwrap());
        if high_water < 1 {
            return Err(Error::Corruption(
                "meta page: invalid high-water mark".into(),
            ));
        }
        Ok(Self {
            io,
            page_size,
            extent_pages,
            allocator: PageAllocator::new(high_water),
        })
    }

    /// Configured page size.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Number of pages ever allocated (including freed ones), i.e. the
    /// allocation high-water mark minus the meta page.
    pub fn allocated_count(&self) -> u64 {
        self.allocator.high_water() - 1
    }

    /// Byte offset of a page within the file.
    fn offset_of(&self, id: PageId) -> u64 {
        id.get() * self.page_size as u64
    }

    /// Allocates a page, growing the file by whole extents when necessary.
    /// Returns the page ID and the freshly initialised (and persisted) page.
    pub fn allocate_page(&mut self, page_type: PageType) -> ndb_common::Result<(PageId, Page)> {
        let (id, generation) = self.allocator.allocate();
        self.ensure_capacity(id)?;
        let mut page = Page::new(self.page_size);
        page.init(id, page_type, generation);
        self.write_page(&mut page)?;
        // Only a fresh page advances the high-water mark — persist it.
        if id.get() == self.allocator.high_water() - 1 {
            self.write_meta_page()?;
        }
        Ok((id, page))
    }

    /// Marks a page free for reuse. Writes a `Free` header so later reads of
    /// the stale page ID still produce structured errors.
    pub fn free_page(&mut self, id: PageId) -> ndb_common::Result<()> {
        self.allocator.free(id)?;
        let generation = self.allocator.generation(id).unwrap_or(1);
        let mut page = Page::new(self.page_size);
        page.init(id, PageType::Free, generation);
        self.write_page(&mut page)
    }

    /// Reads a page, verifying its checksum, identity and generation (a
    /// mismatched generation means a stale reference to a recycled page).
    pub fn read_page(&mut self, id: PageId) -> ndb_common::Result<Page> {
        if id.get() == 0 || id.get() >= self.allocator.high_water() {
            return Err(Error::InvalidInput(format!("page {id} is not allocated")));
        }
        let mut page = Page::new(self.page_size);
        self.io
            .read_at(self.offset_of(id), &mut page.as_bytes_mut()[..])?;
        page.verify()?;
        let header = page.header()?;
        if header.page_id != id {
            return Err(Error::Corruption(format!(
                "page {id}: header contains page {}",
                header.page_id
            )));
        }
        if let Some(expected) = self.allocator.generation(id)
            && header.generation < expected
        {
            return Err(Error::Corruption(format!(
                "page {id}: stale reference (generation {} < {expected})",
                header.generation
            )));
        }
        Ok(page)
    }

    /// Writes a page: refreshes its checksum and persists it. The page must
    /// carry a valid header for an allocated page ID.
    pub fn write_page(&mut self, page: &mut Page) -> ndb_common::Result<()> {
        let header = page.header()?;
        if header.page_id.get() == 0 && header.page_type != PageType::Meta {
            return Err(Error::InvalidInput(
                "page 0 is reserved for metadata".into(),
            ));
        }
        if header.page_id.get() >= self.allocator.high_water() {
            return Err(Error::InvalidInput(format!(
                "page {} is not allocated",
                header.page_id
            )));
        }
        page.refresh_checksum();
        self.io
            .write_at(self.offset_of(header.page_id), page.as_bytes())
    }

    /// Recovery path (spec §22.3): writes a raw page image verbatim —
    /// bypassing the allocation checks in [`Self::write_page`], since the
    /// image may belong to a page beyond the persisted high-water mark.
    /// The image must already carry a valid header and checksum.
    pub fn restore_page(&mut self, id: PageId, image: &[u8]) -> ndb_common::Result<()> {
        if image.len() != self.page_size as usize {
            return Err(Error::InvalidInput(format!(
                "restore_page: image is {} bytes, page size is {}",
                image.len(),
                self.page_size
            )));
        }
        self.ensure_capacity(id)?;
        self.allocator.bump_high_water(id.get() + 1);
        self.io.write_at(self.offset_of(id), image)
    }

    /// Flushes the backing store to durable media.
    pub fn sync(&mut self) -> ndb_common::Result<()> {
        self.io.sync()
    }

    /// Ensures the file is large enough to hold `id`, growing by whole
    /// extents (spec §5.2 — extent-based allocation).
    fn ensure_capacity(&mut self, id: PageId) -> ndb_common::Result<()> {
        let needed = self.offset_of(id) + self.page_size as u64;
        let len = self.io.len()?;
        if needed <= len {
            return Ok(());
        }
        let extent_bytes = self.extent_pages as u64 * self.page_size as u64;
        let new_len = needed.div_ceil(extent_bytes) * extent_bytes;
        self.io.set_len(new_len)
    }

    /// Rewrites the meta page with the current configuration and high-water
    /// mark. Cheap enough to run on every fresh allocation (extents make
    /// fresh allocations amortised-rare).
    fn write_meta_page(&mut self) -> ndb_common::Result<()> {
        self.ensure_capacity(PageId::new(0))?;
        let mut page = Page::new(self.page_size);
        let generation = self.allocator.generation(PageId::new(0)).unwrap_or(0) + 1;
        page.init(PageId::new(0), PageType::Meta, generation);
        let body = page.body_mut();
        body[0..4].copy_from_slice(&META_MAGIC.to_le_bytes());
        body[4..8].copy_from_slice(&self.page_size.to_le_bytes());
        body[8..12].copy_from_slice(&self.extent_pages.to_le_bytes());
        body[16..24].copy_from_slice(&self.allocator.high_water().to_le_bytes());
        page.refresh_checksum();
        self.io.write_at(0, page.as_bytes())
    }
}

fn validate_page_size(page_size: u32) -> ndb_common::Result<()> {
    if !page_size.is_power_of_two() || !(MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&page_size) {
        return Err(Error::InvalidInput(format!(
            "page size {page_size} must be a power of two between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        )));
    }
    Ok(())
}

/// Default extent: 64 pages — 1 MiB at the 16 KB default page size.
pub const DEFAULT_EXTENT_PAGES: u32 = 64;

impl<P: PageIo> PageManager<P> {
    /// Convenience constructor using the default page size and extent.
    pub fn create_default(io: P) -> ndb_common::Result<Self> {
        Self::create(io, DEFAULT_PAGE_SIZE, DEFAULT_EXTENT_PAGES)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory `PageIo` backend.
    #[derive(Default)]
    struct MemIo {
        data: Vec<u8>,
        synced: bool,
    }

    impl PageIo for MemIo {
        fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> ndb_common::Result<()> {
            let start = offset as usize;
            if start + buf.len() > self.data.len() {
                return Err(Error::Storage("read past end".into()));
            }
            buf.copy_from_slice(&self.data[start..start + buf.len()]);
            Ok(())
        }
        fn write_at(&mut self, offset: u64, buf: &[u8]) -> ndb_common::Result<()> {
            let start = offset as usize;
            if start + buf.len() > self.data.len() {
                return Err(Error::Storage("write past end".into()));
            }
            self.data[start..start + buf.len()].copy_from_slice(buf);
            Ok(())
        }
        fn len(&mut self) -> ndb_common::Result<u64> {
            Ok(self.data.len() as u64)
        }
        fn set_len(&mut self, len: u64) -> ndb_common::Result<()> {
            self.data.resize(len as usize, 0);
            Ok(())
        }
        fn sync(&mut self) -> ndb_common::Result<()> {
            self.synced = true;
            Ok(())
        }
    }

    #[test]
    fn create_allocates_and_reads_back() {
        let mut mgr = PageManager::create(MemIo::default(), DEFAULT_PAGE_SIZE, 4).unwrap();
        let (id, mut page) = mgr.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(id, PageId::new(1));
        page.body_mut()[..4].copy_from_slice(b"data");
        mgr.write_page(&mut page).unwrap();

        let read = mgr.read_page(id).unwrap();
        assert_eq!(&read.body()[..4], b"data");
        assert_eq!(read.header().unwrap().page_type, PageType::BTreeLeaf);
    }

    #[test]
    fn file_grows_by_extents() {
        let extent_pages = 2;
        let mut mgr = PageManager::create(MemIo::default(), MIN_PAGE_SIZE, extent_pages).unwrap();
        // Meta page alone → file is one extent (2 pages).
        assert_eq!(mgr.io.len().unwrap(), 2 * MIN_PAGE_SIZE as u64);
        mgr.allocate_page(PageType::BTreeLeaf).unwrap(); // page 1, still one extent
        assert_eq!(mgr.io.len().unwrap(), 2 * MIN_PAGE_SIZE as u64);
        mgr.allocate_page(PageType::BTreeLeaf).unwrap(); // page 2 → second extent
        assert_eq!(mgr.io.len().unwrap(), 4 * MIN_PAGE_SIZE as u64);
    }

    #[test]
    fn free_and_realloc_bumps_generation() {
        let mut mgr = PageManager::create(MemIo::default(), MIN_PAGE_SIZE, 4).unwrap();
        let (id, _) = mgr.allocate_page(PageType::BTreeLeaf).unwrap();
        let gen1 = mgr.read_page(id).unwrap().header().unwrap().generation;
        mgr.free_page(id).unwrap();
        assert!(matches!(
            mgr.read_page(id).unwrap().header().unwrap().page_type,
            PageType::Free
        ));
        let (id2, _) = mgr.allocate_page(PageType::Overflow).unwrap();
        assert_eq!(id2, id);
        assert_eq!(
            mgr.read_page(id2).unwrap().header().unwrap().generation,
            gen1 + 1
        );
    }

    #[test]
    fn reopen_restores_high_water() {
        let mut io = MemIo::default();
        {
            let mut mgr = PageManager::create(io_by_ref(&mut io), MIN_PAGE_SIZE, 4).unwrap();
            mgr.allocate_page(PageType::BTreeLeaf).unwrap();
            mgr.allocate_page(PageType::BTreeLeaf).unwrap();
        }
        let mut mgr = PageManager::open(io).unwrap();
        assert_eq!(mgr.allocated_count(), 2);
        let (id, _) = mgr.allocate_page(PageType::BTreeLeaf).unwrap();
        assert_eq!(id, PageId::new(3)); // no page-id reuse after restart
    }

    /// Lets a `&mut MemIo` act as `PageIo` so a manager can borrow it.
    fn io_by_ref(io: &mut MemIo) -> IoRef<'_> {
        IoRef(io)
    }
    struct IoRef<'a>(&'a mut MemIo);
    impl PageIo for IoRef<'_> {
        fn read_at(&mut self, o: u64, b: &mut [u8]) -> ndb_common::Result<()> {
            self.0.read_at(o, b)
        }
        fn write_at(&mut self, o: u64, b: &[u8]) -> ndb_common::Result<()> {
            self.0.write_at(o, b)
        }
        fn len(&mut self) -> ndb_common::Result<u64> {
            self.0.len()
        }
        fn set_len(&mut self, l: u64) -> ndb_common::Result<()> {
            self.0.set_len(l)
        }
        fn sync(&mut self) -> ndb_common::Result<()> {
            self.0.sync()
        }
    }

    #[test]
    fn corrupt_page_is_detected() {
        let mut io = MemIo::default();
        let id;
        {
            let mut mgr = PageManager::create(io_by_ref(&mut io), MIN_PAGE_SIZE, 4).unwrap();
            let (pid, _) = mgr.allocate_page(PageType::BTreeLeaf).unwrap();
            id = pid;
        }
        // Corrupt a byte in the page body.
        let off = id.get() as usize * MIN_PAGE_SIZE as usize + 200;
        io.data[off] ^= 0xFF;
        let mut mgr = PageManager::open(io).unwrap();
        assert!(matches!(mgr.read_page(id), Err(Error::Corruption(_))));
    }
}
