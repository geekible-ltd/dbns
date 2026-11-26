//! `ndb-page` — page structures, page allocation and page checksums
//! (spec §5.2, §19.1).
//!
//! - [`page`] — fixed-size pages with a 40-byte versioned header
//!   (ID, type, format version, generation, checksum, flags, LSN,
//!   free-space) and CRC32 integrity.
//! - [`alloc`] — the logical allocator: high-water mark, free list,
//!   per-page generations.
//! - [`manager`] — [`manager::PageManager`]: create/open, extent-based
//!   growth, allocation, checksum-verified reads and writes over any
//!   [`manager::PageIo`] backend (real files live in `ndb-storage`).

pub mod alloc;
pub mod manager;
pub mod page;

pub use alloc::PageAllocator;
pub use manager::{DEFAULT_EXTENT_PAGES, PageIo, PageManager};
pub use page::{DEFAULT_PAGE_SIZE, HEADER_SIZE, Page, PageHeader, PageType};
