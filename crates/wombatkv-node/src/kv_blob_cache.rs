#![forbid(unsafe_code)]
//! `KvBlobCache` trait + `MmapKvBlobCache` impl.
//!
//! Substrate-trait-adapter for the local hot tier. Algorithm code
//! (`WombatKVKvStore`) holds an `Arc<dyn KvBlobCache>`; foyer becomes
//! one of N adapters, not a hard dep of the algorithm crate.
//!
//! The `FlatFileKvBlobCache` impl is the embedded-mode default: flat
//! `<puffer_dir>/<key-hash>.kv` layout, `std::fs::read()` on read,
//! atomic tmp-then-rename on write. Reads ride the OS page cache for
//! free, the same mechanism that gets ds4-native to 12-15 ms warm
//! TTFT on a 47 MB blob.
//!
//! `std::fs::read` is safe; the workspace forbids `unsafe_code`.
//! `read(2)` from a page-cached file streams at ~5 GB/s on M3 Max,
//! so a 47 MB warm read costs ~9 ms vs foyer's 18 ms.
//!
//! Profile-driven: 2026-05-15 instrumented run showed foyer's
//! `block_on(inner.get())` was 17.96 ms of the 18.36 ms total C ABI
//! warm-read time. Flat file drops that stage to ~9 ms on page-cache
//! hit, ~15 ms on cold OS cache (NVMe-bound).
//!
//! See `bench_data/2026-05-15_warm_profile_attribution/finding.md`.

use bytes::Bytes;
use std::path::{Path, PathBuf};

/// Sync-only cache surface used by `WombatKVKvStore`. Implementations
/// must be `Send + Sync` because the algorithm crate holds them behind
/// `Arc<dyn KvBlobCache>`.
pub trait KvBlobCache: Send + Sync {
    /// Sync read. Returns `(bytes, op_label)` on hit; `op_label` is a
    /// short telemetry tag like `"load_mmap"` or `"load_foyer_ram"`.
    fn get(&self, key: &str) -> Option<(Bytes, &'static str)>;

    /// Sync write. Caches bytes for subsequent reads.
    fn put(&self, key: &str, payload: Bytes);

    /// Sync existence check that does not materialize the payload.
    fn contains(&self, key: &str) -> bool;

    /// Drop all entries. Best-effort, implementations may leave
    /// disk files in place if removing them would race with concurrent
    /// readers.
    fn clear(&self);

    /// Drop one entry by key. Best-effort: missing files succeed.
    /// Returns true if a file was actually removed. Default impl is a
    /// no-op so adding `remove` does not force every existing impl to
    /// implement it.
    fn remove(&self, _key: &str) -> bool {
        false
    }
}

/// Cache-construction failure surface.
#[derive(Debug)]
pub enum KvBlobCacheError {
    Io(String),
    InvalidConfig(String),
}

impl std::fmt::Display for KvBlobCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(m) => write!(f, "WombatKV cache io error: {m}"),
            Self::InvalidConfig(m) => write!(f, "WombatKV cache invalid config: {m}"),
        }
    }
}

impl std::error::Error for KvBlobCacheError {}

/// Flat file-per-blob cache. The hot read path is `std::fs::read()`,
/// which streams from the OS page cache at ~5 GB/s on M3 Max `NVMe`.
/// For a 47 MB blob already cached by the kernel (immediately after
/// our own write, and across process restarts), this is ~9 ms vs
/// foyer's 18 ms async-SSD path.
///
/// Cleanup: writes go via tmp + rename, so partial-state files never
/// reach the canonical path. We don't fsync, the durable tier is S3;
/// local files are best-effort cache.
#[derive(Debug)]
pub struct FlatFileKvBlobCache {
    root: PathBuf,
}

impl FlatFileKvBlobCache {
    /// Construct or attach. Creates the directory if missing.
    pub fn open(root: PathBuf) -> Result<Self, KvBlobCacheError> {
        if let Some(parent) = root.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                KvBlobCacheError::Io(format!("create parent {}: {err}", parent.display()))
            })?;
        }
        std::fs::create_dir_all(&root).map_err(|err| {
            KvBlobCacheError::Io(format!("create root {}: {err}", root.display()))
        })?;
        Ok(Self { root })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Filesystem-safe filename derived from a cache key. Keys may
    /// contain `/` (they're S3-style paths); we replace any disallowed
    /// chars with `_` so the file lives in a flat dir.
    fn path_for_key(&self, key: &str) -> PathBuf {
        let mut safe = String::with_capacity(key.len() + 4);
        for ch in key.chars() {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' || ch == '=' {
                safe.push(ch);
            } else {
                safe.push('_');
            }
        }
        safe.push_str(".kv");
        self.root.join(safe)
    }
}

impl KvBlobCache for FlatFileKvBlobCache {
    fn get(&self, key: &str) -> Option<(Bytes, &'static str)> {
        let path = self.path_for_key(key);
        let vec = std::fs::read(&path).ok()?;
        Some((Bytes::from(vec), "load_flat"))
    }

    fn put(&self, key: &str, payload: Bytes) {
        let path = self.path_for_key(key);
        let tmp = path.with_extension("kv.tmp");
        if let Err(err) = std::fs::write(&tmp, &payload) {
            eprintln!("WombatKV flat cache: write tmp {} failed: {err}", tmp.display());
            return;
        }
        if let Err(err) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            eprintln!(
                "WombatKV flat cache: rename {} -> {} failed: {err}",
                tmp.display(),
                path.display()
            );
        }
    }

    fn contains(&self, key: &str) -> bool {
        self.path_for_key(key).exists()
    }

    fn clear(&self) {
        // Best-effort: re-create the root dir empty. Concurrent readers
        // would race; the durable tier is S3 so a wipe is safe.
        let _ = std::fs::remove_dir_all(&self.root);
        let _ = std::fs::create_dir_all(&self.root);
    }

    fn remove(&self, key: &str) -> bool {
        // Used by the LRU eviction worker. Missing files are not an
        // error, a get_kv-miss path may have skipped writing the flat
        // tier (the foyer/S3 tiers carry the same payload).
        std::fs::remove_file(self.path_for_key(key)).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::{FlatFileKvBlobCache, KvBlobCache};
    use bytes::Bytes;
    use tempfile::tempdir;

    #[test]
    fn flat_cache_roundtrip() {
        let dir = tempdir().unwrap();
        let cache = FlatFileKvBlobCache::open(dir.path().join("puffer")).unwrap();
        let key = "ns/v1/sha=abc";
        let payload = Bytes::from(vec![7_u8; 4096]);
        assert!(!cache.contains(key));
        assert!(cache.get(key).is_none());
        cache.put(key, payload.clone());
        assert!(cache.contains(key));
        let (got, op) = cache.get(key).unwrap();
        assert_eq!(got.as_ref(), payload.as_ref());
        assert_eq!(op, "load_flat");
    }

    #[test]
    fn clear_removes_entries() {
        let dir = tempdir().unwrap();
        let cache = FlatFileKvBlobCache::open(dir.path().join("puffer")).unwrap();
        cache.put("k", Bytes::from(vec![1_u8; 16]));
        assert!(cache.contains("k"));
        cache.clear();
        assert!(!cache.contains("k"));
    }

    #[test]
    fn unsafe_key_chars_get_sanitized() {
        let dir = tempdir().unwrap();
        let cache = FlatFileKvBlobCache::open(dir.path().join("puffer")).unwrap();
        let key = "ns/v1/model=abc/sha=def";
        cache.put(key, Bytes::from(vec![3_u8; 8]));
        let (got, _) = cache.get(key).unwrap();
        assert_eq!(got.len(), 8);
    }
}
