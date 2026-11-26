//! Physical file management (spec §5.2, §19.1).
//!
//! `StorageFile` provides positioned reads and writes used by the page
//! manager, WAL and index segments. Positional I/O is used where the OS
//! provides it (`pread`/`pwrite` on Unix incl. macOS, `seek_read`/`seek_write`
//! on Windows) so concurrent readers don't share a file cursor. This is the
//! cross-platform storage abstraction recorded in ADR-020.

use ndb_common::Result;
use ndb_common::error::Error;
use ndb_page::manager::PageIo;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// A database file with positioned I/O.
pub struct StorageFile {
    file: File,
}

impl StorageFile {
    /// Opens an existing file, or creates it if missing.
    pub fn open_or_create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|e| Error::Storage(format!("cannot open {}: {e}", path.display())))?;
        Ok(Self::from_file(file))
    }

    /// Wraps an existing `File`.
    pub fn from_file(file: File) -> Self {
        Self { file }
    }

    /// File length in bytes.
    pub fn file_len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Flushes data and metadata to durable media (`fsync`/`FlushFileBuffers`).
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(Error::Io)
    }

    /// Reads exactly `buf.len()` bytes at `offset` without moving the cursor.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        platform::read_at(&self.file, offset, buf)
    }

    /// Writes all of `buf` at `offset` without moving the cursor.
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        platform::write_at(&self.file, offset, buf)
    }

    /// Truncates or extends the file to `len` bytes (extended region is
    /// zero-filled on all supported platforms).
    pub fn set_file_len(&self, len: u64) -> Result<()> {
        self.file.set_len(len).map_err(Error::Io)
    }
}

impl PageIo for StorageFile {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        StorageFile::read_at(self, offset, buf)
    }
    fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        StorageFile::write_at(self, offset, buf)
    }
    fn len(&mut self) -> Result<u64> {
        StorageFile::file_len(self)
    }
    fn set_len(&mut self, len: u64) -> Result<()> {
        StorageFile::set_file_len(self, len)
    }
    fn sync(&mut self) -> Result<()> {
        // Explicit inherent call — `self.sync()` would recurse into this
        // trait method.
        StorageFile::sync(self)
    }
}

#[cfg(unix)]
mod platform {
    use ndb_common::Result;
    use ndb_common::error::Error;
    use std::fs::File;
    use std::os::unix::fs::FileExt;

    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
        file.read_exact_at(buf, offset).map_err(Error::Io)
    }

    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        file.write_all_at(buf, offset).map_err(Error::Io)
    }
}

#[cfg(windows)]
mod platform {
    use ndb_common::Result;
    use ndb_common::error::Error;
    use std::fs::File;
    use std::io::ErrorKind;
    use std::os::windows::fs::FileExt;

    // `seek_read`/`seek_write` may transfer fewer bytes than requested —
    // loop until the buffer is full/drained.
    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
        let mut done = 0;
        while done < buf.len() {
            let n = file
                .seek_read(&mut buf[done..], offset + done as u64)
                .map_err(Error::Io)?;
            if n == 0 {
                return Err(Error::Io(ErrorKind::UnexpectedEof.into()));
            }
            done += n;
        }
        Ok(())
    }

    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        let mut done = 0;
        while done < buf.len() {
            done += file
                .seek_write(&buf[done..], offset + done as u64)
                .map_err(Error::Io)?;
        }
        Ok(())
    }
}

// Portable fallback for platforms without positioned I/O. `&File` itself
// implements `Read`/`Write`/`Seek`, but seeks mutate the shared file cursor —
// so a process-wide lock keeps the seek+access pair atomic. All supported
// platforms (Linux, Windows, macOS) use the positioned paths above.
#[cfg(not(any(unix, windows)))]
mod platform {
    use ndb_common::Result;
    use ndb_common::error::Error;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::Mutex;

    static LOCK: Mutex<()> = Mutex::new(());

    pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
        let _guard = LOCK.lock().unwrap();
        let mut f = file;
        f.seek(SeekFrom::Start(offset))
            .and_then(|_| f.read_exact(buf))
            .map_err(Error::Io)
    }

    pub fn write_at(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        let _guard = LOCK.lock().unwrap();
        let mut f = file;
        f.seek(SeekFrom::Start(offset))
            .and_then(|_| f.write_all(buf))
            .map_err(Error::Io)
    }
}
