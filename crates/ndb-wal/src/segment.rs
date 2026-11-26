//! WAL segment files (spec §5.5 — log rotation).
//!
//! The log is a sequence of append-only segment files named
//! `{seq:016}.ndbwal` inside the WAL directory. Each segment starts with an
//! 8-byte header (`NDBW` magic + u16 format version + u16 reserved) followed
//! by framed [`WalRecord`]s. Segments are immutable once rotated; a
//! checkpoint below a segment's maximum LSN allows it to be retired.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use ndb_common::error::Error;

pub const SEGMENT_MAGIC: &[u8; 4] = b"NDBW";
pub const SEGMENT_HEADER: usize = 8;
pub const SEGMENT_EXT: &str = "ndbwal";
/// Default rotation threshold: 64 MiB.
pub const DEFAULT_SEGMENT_SIZE: u64 = 64 * 1024 * 1024;

/// A WAL segment file opened for appending.
pub struct Segment {
    file: File,
    pub seq: u64,
    pub path: PathBuf,
    /// Bytes currently in the file (header included).
    pub len: u64,
}

impl Segment {
    /// Path of segment `seq` inside `dir`.
    pub fn path(dir: &Path, seq: u64) -> PathBuf {
        dir.join(format!("{seq:016}.{SEGMENT_EXT}"))
    }

    /// Creates a new empty segment (fails if it already exists) and writes
    /// the header.
    pub fn create(dir: &Path, seq: u64) -> ndb_common::Result<Self> {
        let path = Self::path(dir, seq);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .read(true)
            .open(&path)
            .map_err(|e| io_err("create wal segment", &path, e))?;
        let mut hdr = [0u8; SEGMENT_HEADER];
        hdr[..4].copy_from_slice(SEGMENT_MAGIC);
        hdr[4..6].copy_from_slice(&1u16.to_le_bytes()); // format version
        file.write_all(&hdr)
            .map_err(|e| io_err("write wal segment header", &path, e))?;
        Ok(Segment {
            file,
            seq,
            path,
            len: SEGMENT_HEADER as u64,
        })
    }

    /// Opens an existing segment for appending; recovers its length.
    pub fn open_append(dir: &Path, seq: u64) -> ndb_common::Result<Self> {
        let path = Self::path(dir, seq);
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&path)
            .map_err(|e| io_err("open wal segment", &path, e))?;
        let len = file
            .seek(SeekFrom::End(0))
            .map_err(|e| io_err("seek wal segment", &path, e))?;
        if len < SEGMENT_HEADER as u64 {
            return Err(Error::Corruption(format!(
                "wal segment {} shorter than header",
                path.display()
            )));
        }
        Ok(Segment {
            file,
            seq,
            path,
            len,
        })
    }

    /// Appends `bytes` (a whole encoded record) and returns the file offset.
    pub fn append(&mut self, bytes: &[u8]) -> ndb_common::Result<u64> {
        let off = self.len;
        self.file
            .write_all(bytes)
            .map_err(|e| io_err("append wal record", &self.path, e))?;
        self.len += bytes.len() as u64;
        Ok(off)
    }

    /// fsyncs the segment — required before acknowledging a commit
    /// (spec §5.5).
    pub fn sync(&mut self) -> ndb_common::Result<()> {
        self.file
            .sync_data()
            .map_err(|e| io_err("sync wal segment", &self.path, e))
    }

    /// Truncates the file to `len` — used to drop a torn tail detected by
    /// the recovery scanner before appending resumes.
    pub fn truncate(&mut self, len: u64) -> ndb_common::Result<()> {
        self.file
            .set_len(len)
            .map_err(|e| io_err("truncate wal segment", &self.path, e))?;
        self.file
            .seek(SeekFrom::End(0))
            .map_err(|e| io_err("seek wal segment", &self.path, e))?;
        self.len = len;
        Ok(())
    }
}

/// Lists `(seq, path)` of all segment files in `dir`, sorted by sequence.
pub fn list_segments(dir: &Path) -> ndb_common::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    let rd = std::fs::read_dir(dir).map_err(|e| io_err("list wal directory", dir, e))?;
    for entry in rd {
        let entry = entry.map_err(|e| io_err("list wal directory", dir, e))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(stem) = name.strip_suffix(&format!(".{SEGMENT_EXT}")) else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        out.push((seq, entry.path()));
    }
    out.sort_by_key(|(seq, _)| *seq);
    Ok(out)
}

/// Reads the full record-region of a segment (everything after the header),
/// verifying the magic.
pub fn read_records_region(path: &Path) -> ndb_common::Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|e| io_err("read wal segment", path, e))?;
    let mut hdr = [0u8; SEGMENT_HEADER];
    file.read_exact(&mut hdr)
        .map_err(|e| io_err("read wal segment header", path, e))?;
    if &hdr[..4] != SEGMENT_MAGIC {
        return Err(Error::Corruption(format!(
            "wal segment {} has bad magic",
            path.display()
        )));
    }
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)
        .map_err(|e| io_err("read wal segment", path, e))?;
    Ok(buf)
}

fn io_err(op: &str, path: &Path, e: io::Error) -> Error {
    Error::Storage(format!("{op} {}: {e}", path.display()))
}
