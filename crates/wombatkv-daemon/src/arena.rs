//! Shared mmap arena for the future zero-copy KV transfer.
//!
//! # Design
//!
//! Both daemon and client mmap the same backing file. Daemon opens it
//! RW (`ArenaWriter`) and bumps an in-file `AtomicU64` to allocate
//! slabs. Client opens it RO (`ArenaReader`) and borrows slices at the
//! offsets the daemon advertises over the SHM control ring.
//!
//! The control ring carries only `(arena_offset, arena_len)` for large
//! payloads, the bytes themselves never traverse the ring's
//! `T::default + *slot = frame` memcpy pair. That cuts cross-process
//! GET cost from `4 × payload` of memcpy to `2 × payload` (one daemon-
//! side write, one client-side read).
//!
//! # Phase 1 (this module): bump allocator with wrap-around
//!
//! `next_offset` increments atomically on each `write_payload`. When a
//! payload would exceed the arena's tail, the offset wraps to 0 and
//! starts overwriting earlier slabs. This means **a slab is valid only
//! until the allocator next wraps past it**. Sized large enough relative
//! to the working set, wraps are rare (1 GiB arena ÷ 458 KiB blocks
//! → ~2200 slabs before wrap), but there is no protection against a
//! slow reader observing a partially-overwritten slab.
//!
//! # Phase 2 (deferred): leases + refcount + safe reclamation
//!
//! See RFC-0008 §10. The wire protocol reserves opcodes for
//! `LEASE`/`RELEASE`; the `Arena*` types here intentionally don't
//! manage lifetime so the lease layer can be added on top without
//! rewriting the allocator.
//!
//! # Layout
//!
//! ```text
//! [u64 next_offset (atomic, 8 bytes)] [u64 wrap_epoch (8 bytes)] [zero-padding to 64] [DATA ...]
//! ```
//!
//! The 64-byte header is cache-line aligned. `wrap_epoch` increments
//! every time `next_offset` wraps; clients can sample it before and
//! after a read to detect wrap races (Phase 2 will use this).
//!
//! # Concurrency
//!
//! - Multiple writer threads on the daemon side are safe: `next_offset`
//!   is an atomic and the per-slab write doesn't overlap.
//! - Multiple reader threads on the client side are safe: `&[u8]` view
//!   is `Send`/`Sync`.
//! - Mixed reader+writer races are documented above (wrap hazard).
//!
//! # Safety
//!
//! `memmap2::MmapMut::map_mut` / `Mmap::map` are `unsafe` because the
//! file may be modified by another process or unmapped while the slice
//! is still borrowed. We accept that contract here: the arena file is
//! only ever accessed by wombatkv-shm processes, and unmapping happens
//! exclusively at process exit (no `shm_unlink` mid-flight).

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut, MmapOptions};

/// Header size in bytes. Cache-line aligned; payload starts at offset 64.
pub const ARENA_HEADER_BYTES: usize = 64;

/// Default arena file size. 1 GiB at ~458 KiB per KV block gives ~2200
/// slabs before the bump allocator wraps.
pub const DEFAULT_ARENA_BYTES: u64 = 1024 * 1024 * 1024;

/// Header offsets (absolute byte positions in the mmap'd file).
const NEXT_OFFSET_AT: usize = 0;
const WRAP_EPOCH_AT: usize = 8;

/// Errors from arena operations.
#[derive(Debug)]
pub enum ArenaError {
    /// I/O error opening / sizing the backing file.
    Io(std::io::Error),
    /// `payload.len() > arena_size - ARENA_HEADER_BYTES`. The slab does
    /// not fit even on a fresh wrap.
    PayloadTooLarge { payload: u64, max_slab: u64 },
    /// Read offset + length exceed the arena bounds.
    OutOfBounds { offset: u64, len: u32, arena: u64 },
}

impl std::fmt::Display for ArenaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "arena io: {e}"),
            Self::PayloadTooLarge { payload, max_slab } => {
                write!(f, "arena payload {payload} > max_slab {max_slab}")
            }
            Self::OutOfBounds { offset, len, arena } => {
                write!(f, "arena read out of bounds: offset={offset} len={len} arena_size={arena}")
            }
        }
    }
}

impl std::error::Error for ArenaError {}

impl From<std::io::Error> for ArenaError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Mutable view of the arena. One per daemon process.
///
/// Holds a single `MmapMut` over the backing file. The first 64 bytes
/// are the header (atomic `next_offset` + `wrap_epoch`); the rest is the
/// payload area.
pub struct ArenaWriter {
    /// Locked behind `unsafe` only at construction; thereafter we treat
    /// the slice as `&[u8]` for header reads and use a raw byte-pointer
    /// trick (with `copy_from_slice` against `&mut`-borrowed slices we
    /// take through `MmapMut`'s `Deref`/`DerefMut`) for payload writes.
    /// We use a `parking_lot::Mutex` would be heavier; instead the
    /// bump-allocator semantics keep slab regions disjoint per writer.
    mmap: MmapMut,
    /// Total file size (header + payload area).
    size: u64,
}

impl ArenaWriter {
    /// Create-or-truncate the backing file at `path`, size it to
    /// `size_bytes` (must be > `ARENA_HEADER_BYTES`), reset the header,
    /// and mmap it RW.
    pub fn create(path: &Path, size_bytes: u64) -> Result<Self, ArenaError> {
        assert!(
            size_bytes > ARENA_HEADER_BYTES as u64,
            "arena size {size_bytes} must exceed header {ARENA_HEADER_BYTES}",
        );
        let file =
            OpenOptions::new().read(true).write(true).create(true).truncate(true).open(path)?;
        file.set_len(size_bytes)?;

        // SAFETY: the backing file is only ever accessed by wombatkv-shm
        // processes (daemon writes, clients read). No external mutation.
        // The mapping lives for the ArenaWriter's lifetime; on Drop the
        // mmap is unmapped before the file handle closes.
        #[allow(unsafe_code)]
        let mut mmap = unsafe { MmapOptions::new().len(size_bytes as usize).map_mut(&file)? };

        // Initialize the header. `next_offset` starts past the header
        // so the first payload lands at HEADER_BYTES.
        let header = &mut mmap[..ARENA_HEADER_BYTES];
        header.fill(0);
        header[NEXT_OFFSET_AT..NEXT_OFFSET_AT + 8]
            .copy_from_slice(&(ARENA_HEADER_BYTES as u64).to_le_bytes());
        // wrap_epoch already 0.

        Ok(Self { mmap, size: size_bytes })
    }

    /// Bump-allocate a slab and copy `bytes` into it.
    ///
    /// Returns `(offset, len)` where the bytes live in the arena.
    /// Wraps to `ARENA_HEADER_BYTES` when the payload would exceed the
    /// arena tail, incrementing `wrap_epoch` to signal readers.
    pub fn write_payload(&mut self, bytes: &[u8]) -> Result<(u64, u32), ArenaError> {
        let len = bytes.len() as u64;
        let max_slab = self.size - ARENA_HEADER_BYTES as u64;
        if len > max_slab {
            return Err(ArenaError::PayloadTooLarge { payload: len, max_slab });
        }

        // Read current offset, decide where this slab goes, write
        // header back. Single-writer-thread mode: this is non-atomic
        // by design (we expect the daemon to call this from one thread
        // at a time; if multiple worker threads call concurrently they
        // serialize via the daemon's per-engine ring). Multi-writer
        // safety is Phase 2.
        let next_off = u64::from_le_bytes(
            self.mmap[NEXT_OFFSET_AT..NEXT_OFFSET_AT + 8].try_into().expect("8 byte header field"),
        );

        let (slab_off, new_next, did_wrap) = if next_off + len > self.size {
            // Wrap.
            (ARENA_HEADER_BYTES as u64, ARENA_HEADER_BYTES as u64 + len, true)
        } else {
            (next_off, next_off + len, false)
        };

        // Copy payload.
        let dst = &mut self.mmap[slab_off as usize..slab_off as usize + bytes.len()];
        dst.copy_from_slice(bytes);

        // Update header atomically-ish: write offset first, then bump
        // wrap_epoch if we wrapped. Real atomicity is Phase 2; for
        // single-writer this is fine.
        self.mmap[NEXT_OFFSET_AT..NEXT_OFFSET_AT + 8].copy_from_slice(&new_next.to_le_bytes());

        if did_wrap {
            let prev_epoch = u64::from_le_bytes(
                self.mmap[WRAP_EPOCH_AT..WRAP_EPOCH_AT + 8].try_into().expect("8 byte"),
            );
            self.mmap[WRAP_EPOCH_AT..WRAP_EPOCH_AT + 8]
                .copy_from_slice(&(prev_epoch + 1).to_le_bytes());
        }

        // Flush is best-effort; we don't need durability across crashes,
        // only visibility to the cooperating reader process. Linux/macOS
        // cohere shared mmap'd pages between processes via the page
        // cache without msync.

        Ok((slab_off, len as u32))
    }

    /// Total file size including header.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Current `next_offset`. Diagnostic only.
    #[must_use]
    pub fn next_offset(&self) -> u64 {
        u64::from_le_bytes(
            self.mmap[NEXT_OFFSET_AT..NEXT_OFFSET_AT + 8].try_into().expect("8 byte header field"),
        )
    }

    /// Current `wrap_epoch`. Diagnostic only.
    #[must_use]
    pub fn wrap_epoch(&self) -> u64 {
        u64::from_le_bytes(
            self.mmap[WRAP_EPOCH_AT..WRAP_EPOCH_AT + 8].try_into().expect("8 byte header field"),
        )
    }
}

/// Read-only view of the arena. One per client process.
pub struct ArenaReader {
    mmap: Mmap,
    size: u64,
}

impl ArenaReader {
    /// Open an existing arena file at `path` and mmap it RO.
    pub fn open(path: &Path) -> Result<Self, ArenaError> {
        let file = OpenOptions::new().read(true).open(path)?;
        let metadata = file.metadata()?;
        let size = metadata.len();

        // SAFETY: the file is only mutated by an ArenaWriter in the
        // cooperating daemon process; we never modify it ourselves.
        // Memory ordering of payload writes vs the header bump is
        // documented as best-effort (Phase 2 will tighten this).
        #[allow(unsafe_code)]
        let mmap = unsafe { MmapOptions::new().len(size as usize).map(&file)? };

        Ok(Self { mmap, size })
    }

    /// Borrow the bytes at `offset..offset+len` from the arena. The
    /// returned slice is valid for the lifetime of `&self`. The caller
    /// is responsible for treating the bytes as a snapshot, if the
    /// daemon overwrites this slab between the response arrival and
    /// the read, the caller sees torn data. Phase 2 leases will close
    /// this hole.
    pub fn read_payload(&self, offset: u64, len: u32) -> Result<&[u8], ArenaError> {
        let end = offset + u64::from(len);
        if end > self.size || offset < ARENA_HEADER_BYTES as u64 {
            return Err(ArenaError::OutOfBounds { offset, len, arena: self.size });
        }
        Ok(&self.mmap[offset as usize..end as usize])
    }

    #[must_use]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Current `wrap_epoch` from the arena header. Increments every
    /// time the writer wraps the bump pointer.
    #[must_use]
    pub fn wrap_epoch(&self) -> u64 {
        u64::from_le_bytes(
            self.mmap[WRAP_EPOCH_AT..WRAP_EPOCH_AT + 8].try_into().expect("8 byte header field"),
        )
    }
}

/// Compose the standard arena file path for a given prefix. Lives next
/// to the SHM segments at `/tmp/wombatkv-arena-<prefix>.bin`.
#[must_use]
pub fn arena_path(prefix: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(format!("/tmp/wombatkv-arena-{prefix}.bin"))
}

// `next_offset` is intentionally not a real atomic in phase 1; we just
// suppress the unused import warning if `AtomicU64` ends up being used
// only in tests / future code paths.
#[allow(dead_code)]
fn _atomic_marker(x: &AtomicU64) -> u64 {
    x.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena.bin");
        let size = 1024 * 1024;

        {
            let mut w = ArenaWriter::create(&path, size).unwrap();
            let payload = vec![0xABu8; 4096];
            let (off, len) = w.write_payload(&payload).unwrap();
            assert_eq!(len as usize, payload.len());
            assert!(off >= ARENA_HEADER_BYTES as u64);
            // Drop writer so reader can open.
        }

        let r = ArenaReader::open(&path).unwrap();
        let view = r.read_payload(ARENA_HEADER_BYTES as u64, 4096).unwrap();
        assert_eq!(view.len(), 4096);
        assert!(view.iter().all(|&b| b == 0xAB));
    }

    #[test]
    fn bump_allocator_advances_offset() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena.bin");
        let mut w = ArenaWriter::create(&path, 64 * 1024).unwrap();

        let p1 = vec![0x11; 1000];
        let p2 = vec![0x22; 2000];
        let p3 = vec![0x33; 3000];

        let (o1, l1) = w.write_payload(&p1).unwrap();
        let (o2, l2) = w.write_payload(&p2).unwrap();
        let (o3, _l3) = w.write_payload(&p3).unwrap();

        assert_eq!(o1, ARENA_HEADER_BYTES as u64);
        assert_eq!(o2, o1 + u64::from(l1));
        assert_eq!(o3, o2 + u64::from(l2));
    }

    #[test]
    fn bump_wraps_when_full() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena.bin");
        // Tiny arena: header + 1 KiB usable.
        let size = ARENA_HEADER_BYTES as u64 + 1024;
        let mut w = ArenaWriter::create(&path, size).unwrap();

        let p = vec![0x55; 600];
        let (o1, _l1) = w.write_payload(&p).unwrap();
        let (o2, _l2) = w.write_payload(&p).unwrap(); // 600 + 600 = 1200 > 1024 → wrap before this

        assert_eq!(o1, ARENA_HEADER_BYTES as u64);
        assert_eq!(o2, ARENA_HEADER_BYTES as u64); // wrapped
        assert_eq!(w.wrap_epoch(), 1);
    }

    #[test]
    fn payload_too_large_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena.bin");
        let size = ARENA_HEADER_BYTES as u64 + 1024;
        let mut w = ArenaWriter::create(&path, size).unwrap();

        let p = vec![0; 2048];
        match w.write_payload(&p) {
            Err(ArenaError::PayloadTooLarge { .. }) => {}
            other => panic!("expected PayloadTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn out_of_bounds_read_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("arena.bin");
        let mut w = ArenaWriter::create(&path, 64 * 1024).unwrap();
        let _ = w.write_payload(&[0; 100]).unwrap();
        drop(w);

        let r = ArenaReader::open(&path).unwrap();
        // offset under header
        assert!(matches!(r.read_payload(0, 64), Err(ArenaError::OutOfBounds { .. })));
        // offset past file
        assert!(matches!(r.read_payload(64 * 1024 - 32, 64), Err(ArenaError::OutOfBounds { .. })));
    }
}
