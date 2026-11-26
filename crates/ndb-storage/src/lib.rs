//! `ndb-storage` — physical file management and persistent storage
//! (spec §5, §19.1).
//!
//! Dependency rule: must not depend on search or networking.
//!
//! [`file::StorageFile`] is the concrete [`ndb_page::manager::PageIo`]
//! backend for [`ndb_page::manager::PageManager`].

pub mod file;

pub use file::StorageFile;

#[cfg(test)]
mod tests {
    use super::*;
    use ndb_page::manager::PageIo;

    fn temp_path(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("ndb-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn positioned_read_write() {
        let path = temp_path("storage-rw.dat");
        let mut f = StorageFile::open_or_create(&path).unwrap();
        f.set_len(64).unwrap();
        f.write_at(0, b"header").unwrap();
        f.write_at(32, b"body").unwrap();
        let mut buf = [0u8; 6];
        f.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, b"header");
        let mut buf = [0u8; 4];
        f.read_at(32, &mut buf).unwrap();
        assert_eq!(&buf, b"body");
        assert_eq!(f.len().unwrap(), 64);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn page_manager_over_real_file() {
        use ndb_page::manager::PageManager;
        use ndb_page::page::PageType;

        let path = temp_path("storage-pages.dat");
        {
            let f = StorageFile::open_or_create(&path).unwrap();
            let mut mgr = PageManager::create(f, 4096, 4).unwrap();
            let (_, mut page) = mgr.allocate_page(PageType::BTreeLeaf).unwrap();
            page.body_mut()[..5].copy_from_slice(b"hello");
            mgr.write_page(&mut page).unwrap();
            mgr.sync().unwrap();
        }
        // Reopen and read back through a fresh manager.
        let f = StorageFile::open_or_create(&path).unwrap();
        let mut mgr = PageManager::open(f).unwrap();
        let page = mgr.read_page(ndb_common::ids::PageId::new(1)).unwrap();
        assert_eq!(&page.body()[..5], b"hello");
        std::fs::remove_file(&path).ok();
    }
}
