#![forbid(unsafe_code)]
//! Hybrid memory + disk cache backend.
//!
//! Opt-in alternative to `nvme_cache::NvmeHotCache`. Selected at runtime via
//! `WMBT_KV_PUFFER_BACKEND=hybrid`. The cache manages both memory and on-disk
//! tiers internally:
//!
//! - S3-FIFO eviction in memory
//! - Direct I/O on Linux to bypass the OS page cache
//! - `io_uring` async backend (when available) for the disk tier
//! - Block-aligned device layout
//!
//! sync `get` fast path probes the memory tier directly to avoid a
//! `block_on` round trip on warm hits, which is where the production
//! perf wins live. Foyer hybrid cache: in-process RAM (L0) + on-disk
//! SSD (L1).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridCachePolicy, IoEngineConfig,
};
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

/// Which foyer tier served a hit. Returned alongside the payload by
/// `FoyerCache::get_with_tier` so callers can attribute latency to the
/// right tier in observability traces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FoyerHitTier {
    /// In-memory tier (sync probe hit).
    Ram,
    /// On-disk / SSD tier (async hybrid get hit; memory tier missed).
    Ssd,
}

impl FoyerHitTier {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ram => "ram",
            Self::Ssd => "ssd",
        }
    }
}

/// Tuning knobs for the hybrid cache.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FoyerCacheConfig {
    /// Memory-tier budget in bytes.
    pub ram_bytes: u64,
    /// On-disk tier directory. Created if missing.
    pub ssd_dir: PathBuf,
    /// On-disk tier budget in bytes.
    pub ssd_bytes: u64,
    /// Block size for the on-disk engine. Should be at least as large as
    /// the largest blob the caller will store.
    pub block_size: usize,
    /// Per-flusher I/O buffer pool size in bytes.
    ///
    /// Foyer's `BlockEngine` writes via N flushers, each owning an
    /// in-RAM staging buffer of `buffer_pool_size / flushers` bytes
    /// (foyer 0.22 default: 16 MiB / 1 flusher = 16 MiB per flusher).
    /// An entry whose serialized size exceeds the per-flusher buffer
    /// is silently dropped at `Buffer::push` with
    /// `storage_queue_buffer_overflow` and **never reaches SSD**, the
    /// failure mode that broke the `WombatKV` "foyer SSD as a hot tier
    /// across restarts" claim for our 28-42 MB ds4 KV blobs.
    ///
    /// This must be larger than the biggest blob put through `put_kv`.
    /// We default to **256 MiB** so single 30-60 MB ds4 / vLLM KV
    /// payloads land on SSD without the caller having to think about
    /// it. Bump via `WMBT_KV_PUFFER_BUFFER_POOL_BYTES` for larger blobs.
    pub buffer_pool_size: usize,
    /// Use `io_uring` on Linux when true. Falls back to psync otherwise.
    pub iouring: bool,
}

impl Default for FoyerCacheConfig {
    fn default() -> Self {
        Self {
            ram_bytes: 1024 * 1024 * 1024,
            ssd_dir: PathBuf::from("/tmp/wombatkv-puffer"),
            ssd_bytes: 8_u64 * 1024 * 1024 * 1024,
            block_size: 64 * 1024 * 1024,
            buffer_pool_size: 256 * 1024 * 1024,
            iouring: cfg!(target_os = "linux"),
        }
    }
}

/// Cache operation failures surfaced to callers.
#[derive(Debug)]
pub enum FoyerCacheError {
    Io(String),
    InvalidConfig(String),
    Build(String),
    RuntimeInit(String),
}

impl std::fmt::Display for FoyerCacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(message) => write!(f, "WombatKV puffer io error: {message}"),
            Self::InvalidConfig(message) => write!(f, "WombatKV puffer invalid config: {message}"),
            Self::Build(message) => write!(f, "WombatKV puffer build failed: {message}"),
            Self::RuntimeInit(message) => {
                write!(f, "WombatKV puffer runtime init failed: {message}")
            }
        }
    }
}

impl std::error::Error for FoyerCacheError {}

/// Hybrid memory + disk cache backed by foyer.
///
/// Construction requires an async runtime because foyer's builder is async.
/// We pin a dedicated multi-thread runtime to the cache instance and reuse
/// it for the slow path on memory miss.
pub struct FoyerHybridCache {
    inner: HybridCache<String, Bytes>,
    runtime: Arc<Runtime>,
    ssd_dir: PathBuf,
    hits: AtomicU64,
    misses: AtomicU64,
    inserts: AtomicU64,
}

impl FoyerHybridCache {
    /// Build a new hybrid cache. Creates the on-disk directory if missing.
    /// Allocates a fresh tokio runtime for foyer's async paths.
    pub fn open(config: FoyerCacheConfig) -> Result<Arc<Self>, FoyerCacheError> {
        if config.ram_bytes == 0 {
            return Err(FoyerCacheError::InvalidConfig("ram_bytes must be > 0".to_string()));
        }
        if config.ssd_bytes == 0 {
            return Err(FoyerCacheError::InvalidConfig("ssd_bytes must be > 0".to_string()));
        }
        if config.block_size == 0 {
            return Err(FoyerCacheError::InvalidConfig("block_size must be > 0".to_string()));
        }
        if config.buffer_pool_size == 0 {
            return Err(FoyerCacheError::InvalidConfig("buffer_pool_size must be > 0".to_string()));
        }
        if let Some(parent) = config.ssd_dir.parent() {
            std::fs::create_dir_all(parent).map_err(|err| {
                FoyerCacheError::Io(format!("create ssd parent {}: {err}", parent.display()))
            })?;
        }
        std::fs::create_dir_all(&config.ssd_dir).map_err(|err| {
            FoyerCacheError::Io(format!("create ssd dir {}: {err}", config.ssd_dir.display()))
        })?;

        let runtime = Arc::new(
            RuntimeBuilder::new_multi_thread()
                .worker_threads(2)
                .thread_name("wombatkv-puffer")
                .enable_all()
                .build()
                .map_err(|err| FoyerCacheError::RuntimeInit(err.to_string()))?,
        );
        Self::open_with_runtime(config, runtime)
    }

    /// Build the cache with a caller-provided runtime. Useful for sharing a
    /// runtime across multiple subsystems in the same process.
    pub fn open_with_runtime(
        config: FoyerCacheConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Arc<Self>, FoyerCacheError> {
        let ssd_dir = config.ssd_dir.clone();
        let runtime_for_build = runtime.clone();
        let inner = runtime_for_build
            .block_on(async move { build_cache(config).await })
            .map_err(|err| FoyerCacheError::Build(err.to_string()))?;

        Ok(Arc::new(Self {
            inner,
            runtime,
            ssd_dir,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
        }))
    }

    #[must_use]
    pub fn ssd_dir(&self) -> &Path {
        &self.ssd_dir
    }

    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    #[must_use]
    pub fn inserts(&self) -> u64 {
        self.inserts.load(Ordering::Relaxed)
    }

    /// Sync read. Probes the in-memory tier first to avoid a runtime hop
    /// on warm hits. Falls through to foyer's async hybrid `get` only when
    /// the memory tier misses.
    ///
    /// Convenience wrapper over `get_with_tier` for callers that don't
    /// need to distinguish RAM vs SSD hits.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.get_with_tier(key).map(|(bytes, _)| bytes)
    }

    /// Sync read with tier attribution. Returns the tier the value was
    /// served from on hit. Lets callers attribute load latency to the
    /// right tier, critical for diagnosing the "blob too big for RAM
    /// tier, always SSD-streamed" case (e.g. qwen3's pre-allocated 4.7
    /// GiB KV cache against a 2 GiB RAM budget).
    pub fn get_with_tier(&self, key: &str) -> Option<(Bytes, FoyerHitTier)> {
        let t0 = std::time::Instant::now();
        let owned = key.to_string();
        let t_owned = t0.elapsed().as_micros() as u64;
        if let Some(entry) = self.inner.memory().get(&owned) {
            let t_ram = t0.elapsed().as_micros() as u64;
            self.hits.fetch_add(1, Ordering::Relaxed);
            let v = entry.value().clone();
            let t_clone = t0.elapsed().as_micros() as u64;
            crate::embed::emit_timing(
                "foyer_get_with_tier",
                "ram_hit",
                &[
                    ("owned_us", t_owned),
                    ("ram_probe_us", t_ram - t_owned),
                    ("clone_us", t_clone - t_ram),
                    ("total_us", t_clone),
                ],
            );
            return Some((v, FoyerHitTier::Ram));
        }
        let t_ram_miss = t0.elapsed().as_micros() as u64;

        let result = self.runtime.block_on(self.inner.get(&owned));
        let t_ssd = t0.elapsed().as_micros() as u64;
        if let Ok(Some(entry)) = result {
            self.hits.fetch_add(1, Ordering::Relaxed);
            let v = entry.value().clone();
            let t_clone = t0.elapsed().as_micros() as u64;
            crate::embed::emit_timing(
                "foyer_get_with_tier",
                "ssd_hit",
                &[
                    ("owned_us", t_owned),
                    ("ram_miss_us", t_ram_miss - t_owned),
                    ("ssd_block_on_us", t_ssd - t_ram_miss),
                    ("clone_us", t_clone - t_ssd),
                    ("total_us", t_clone),
                ],
            );
            // Sync probe of the memory tier missed but the async
            // hybrid get hit, value came from the disk/SSD tier.
            Some((v, FoyerHitTier::Ssd))
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            let t_total = t0.elapsed().as_micros() as u64;
            crate::embed::emit_timing(
                "foyer_get_with_tier",
                "miss",
                &[
                    ("ram_miss_us", t_ram_miss - t_owned),
                    ("ssd_miss_us", t_ssd - t_ram_miss),
                    ("total_us", t_total),
                ],
            );
            None
        }
    }

    /// Sync existence check that does not materialize the cached payload.
    pub fn contains(&self, key: &str) -> bool {
        let owned = key.to_string();
        self.inner.contains(&owned)
    }

    /// Sync write. Foyer's `insert` is sync; the disk-side flush happens on
    /// background workers driven by the cache's spawner.
    pub fn put(&self, key: &str, payload: Bytes) {
        self.inner.insert(key.to_string(), payload);
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Drop all entries from both tiers.
    pub fn clear(&self) {
        self.runtime.block_on(async {
            let _ = self.inner.clear().await;
        });
    }

    /// Block until the foyer write-queue has drained to SSD.
    ///
    /// Foyer's `Drop` impl spawns a close future on the cache's own
    /// spawner but does NOT wait for it, so process exit immediately
    /// after the last Arc clone drops can leave SSD entries un-flushed.
    /// This method is the explicit drain hook callers should invoke
    /// before tearing down the cache; the [`Drop`] impl on
    /// [`FoyerHybridCache`] also calls it on a best-effort basis.
    pub fn close(&self) {
        self.runtime.block_on(async {
            let _ = self.inner.close().await;
        });
    }
}

impl Drop for FoyerHybridCache {
    fn drop(&mut self) {
        // Block on foyer's close so pending SSD writes flush before the
        // tokio runtime is torn down. Without this, the write-on-insertion
        // path enqueues SSD writes to background workers that never get a
        // chance to run on process exit, entries appear in RAM, never on
        // disk, and a subsequent process restart recovers zero entries.
        self.runtime.block_on(async {
            let _ = self.inner.close().await;
        });
    }
}

type BuildError = Box<dyn std::error::Error + Send + Sync>;

async fn build_cache(config: FoyerCacheConfig) -> Result<HybridCache<String, Bytes>, BuildError> {
    let mut fs_builder = FsDeviceBuilder::new(&config.ssd_dir);
    #[cfg(target_os = "linux")]
    {
        fs_builder = fs_builder.with_direct(true);
    }
    fs_builder = fs_builder.with_capacity(config.ssd_bytes as usize);
    let device = fs_builder.build().map_err(|err| Box::new(err) as BuildError)?;

    // `with_buffer_pool_size` is critical for large-value workloads. Foyer's
    // default per-flusher I/O buffer is 16 MiB, so a single `put_kv` with a
    // value larger than 16 MiB is silently dropped at the flusher buffer
    // (`storage_queue_buffer_overflow`) and the entry never reaches SSD.
    let engine = BlockEngineConfig::new(device)
        .with_block_size(config.block_size)
        .with_buffer_pool_size(config.buffer_pool_size);
    let io_engine = io_engine_config(config.iouring);

    // WriteOnInsertion (not WriteOnEviction) so entries land on SSD on
    // every put, not only after RAM eviction. The previous policy left
    // the SSD tier empty for typical WombatKV workloads (large single
    // blobs that fit in RAM), so foyer recovered 0 entries on every
    // process restart and the "warm SSD tier across restarts" claim was
    // silently broken. Confirmed via the foyer-tier-bench microbench on
    // 2026-05-13: under WriteOnEviction the SSD region was 8 GiB of
    // sparse files with 0 bytes of actual data, and every restart fell
    // through to S3.
    let cache = HybridCacheBuilder::new()
        .with_policy(HybridCachePolicy::WriteOnInsertion)
        .memory(config.ram_bytes as usize)
        .with_weighter(|key: &String, value: &Bytes| key.len() + value.len())
        .storage()
        .with_engine_config(engine)
        .with_io_engine_config(io_engine)
        .build()
        .await
        .map_err(|err| Box::new(err) as BuildError)?;

    Ok(cache)
}

fn io_engine_config(iouring: bool) -> Box<dyn IoEngineConfig> {
    #[cfg(target_os = "linux")]
    if iouring {
        return foyer::UringIoEngineConfig::new().boxed();
    }
    #[cfg(not(target_os = "linux"))]
    let _ = iouring;
    foyer::PsyncIoEngineConfig::new().boxed()
}

/// Read [`FoyerCacheConfig`] from a key/value lookup. Tested in isolation
/// from process env so callers can drive deterministic unit tests without
/// mutating global state.
///
/// Keys: `WMBT_KV_PUFFER_RAM_BYTES`, `WMBT_KV_PUFFER_DIR`, `WMBT_KV_PUFFER_DISK_BYTES`,
/// `WMBT_KV_PUFFER_BLOCK_SIZE_BYTES`, `WMBT_KV_PUFFER_BUFFER_POOL_BYTES`,
/// `WMBT_KV_PUFFER_IOURING`.
#[must_use]
pub fn config_from_lookup<F>(lookup: F) -> FoyerCacheConfig
where
    F: Fn(&str) -> Option<String>,
{
    let mut cfg = FoyerCacheConfig::default();
    if let Some(value) = lookup("WMBT_KV_PUFFER_RAM_BYTES") {
        if let Ok(parsed) = value.parse::<u64>() {
            cfg.ram_bytes = parsed;
        }
    }
    if let Some(value) = lookup("WMBT_KV_PUFFER_DIR") {
        cfg.ssd_dir = PathBuf::from(value);
    }
    if let Some(value) = lookup("WMBT_KV_PUFFER_DISK_BYTES") {
        if let Ok(parsed) = value.parse::<u64>() {
            cfg.ssd_bytes = parsed;
        }
    }
    if let Some(value) = lookup("WMBT_KV_PUFFER_BLOCK_SIZE_BYTES") {
        if let Ok(parsed) = value.parse::<usize>() {
            cfg.block_size = parsed;
        }
    }
    if let Some(value) = lookup("WMBT_KV_PUFFER_BUFFER_POOL_BYTES") {
        if let Ok(parsed) = value.parse::<usize>() {
            cfg.buffer_pool_size = parsed;
        }
    }
    if let Some(value) = lookup("WMBT_KV_PUFFER_IOURING") {
        cfg.iouring = !(value == "0" || value.eq_ignore_ascii_case("false"));
    }
    cfg
}

/// Read [`FoyerCacheConfig`] from process environment variables.
#[must_use]
pub fn config_from_env() -> FoyerCacheConfig {
    config_from_lookup(|key| std::env::var(key).ok())
}

/// Returns true when the supplied lookup yields `hybrid` for `WMBT_KV_PUFFER_BACKEND`.
#[must_use]
pub fn backend_selected_with<F>(lookup: F) -> bool
where
    F: Fn(&str) -> Option<String>,
{
    lookup("WMBT_KV_PUFFER_BACKEND").is_some_and(|value| value.eq_ignore_ascii_case("hybrid"))
}

/// Returns true when `WMBT_KV_PUFFER_BACKEND=hybrid` in the process environment.
#[must_use]
pub fn backend_selected() -> bool {
    backend_selected_with(|key| std::env::var(key).ok())
}

#[cfg(test)]
mod tests {
    use super::{FoyerCacheConfig, FoyerCacheError, FoyerHybridCache};
    use bytes::Bytes;
    use tempfile::tempdir;

    fn small_config(dir: std::path::PathBuf) -> FoyerCacheConfig {
        FoyerCacheConfig {
            ram_bytes: 4 * 1024 * 1024,
            ssd_dir: dir,
            ssd_bytes: 16 * 1024 * 1024,
            block_size: 1024 * 1024,
            buffer_pool_size: 4 * 1024 * 1024,
            iouring: false,
        }
    }

    #[test]
    fn invalid_config_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let mut cfg = small_config(dir.path().to_path_buf());
        cfg.ram_bytes = 0;
        assert!(matches!(FoyerHybridCache::open(cfg), Err(FoyerCacheError::InvalidConfig(_))));

        let mut cfg = small_config(dir.path().to_path_buf());
        cfg.ssd_bytes = 0;
        assert!(matches!(FoyerHybridCache::open(cfg), Err(FoyerCacheError::InvalidConfig(_))));

        let mut cfg = small_config(dir.path().to_path_buf());
        cfg.block_size = 0;
        assert!(matches!(FoyerHybridCache::open(cfg), Err(FoyerCacheError::InvalidConfig(_))));
    }

    #[test]
    fn put_get_round_trip_hits_warm_path_and_returns_none_for_missing_keys() {
        let dir = tempdir().expect("tempdir");
        let cache = FoyerHybridCache::open(small_config(dir.path().to_path_buf()))
            .expect("open foyer cache");

        let payload = Bytes::from_static(b"wombatkv-foyer-round-trip");
        cache.put("ns/key-1", payload.clone());
        assert_eq!(cache.inserts(), 1);

        let fetched = cache.get("ns/key-1").expect("warm hit");
        assert_eq!(fetched, payload);
        assert!(cache.hits() >= 1);

        assert!(cache.get("ns/key-missing").is_none());
        assert_eq!(cache.misses(), 1);
    }

    #[test]
    fn clear_drops_all_entries_so_subsequent_get_misses() {
        let dir = tempdir().expect("tempdir");
        let cache = FoyerHybridCache::open(small_config(dir.path().to_path_buf()))
            .expect("open foyer cache");

        cache.put("ns/key", Bytes::from_static(b"payload"));
        assert!(cache.get("ns/key").is_some());

        cache.clear();
        assert!(cache.get("ns/key").is_none());
    }

    #[test]
    fn config_from_lookup_overrides_defaults() {
        let lookup = |key: &str| match key {
            "WMBT_KV_PUFFER_RAM_BYTES" => Some("65536".to_string()),
            "WMBT_KV_PUFFER_DISK_BYTES" => Some("131072".to_string()),
            "WMBT_KV_PUFFER_BLOCK_SIZE_BYTES" => Some("4096".to_string()),
            "WMBT_KV_PUFFER_IOURING" => Some("false".to_string()),
            "WMBT_KV_PUFFER_DIR" => Some("/tmp/wombatkv-puffer-env-test".to_string()),
            _ => None,
        };

        let cfg = super::config_from_lookup(lookup);
        assert_eq!(cfg.ram_bytes, 65536);
        assert_eq!(cfg.ssd_bytes, 131072);
        assert_eq!(cfg.block_size, 4096);
        assert!(!cfg.iouring);
        assert_eq!(cfg.ssd_dir.to_string_lossy(), "/tmp/wombatkv-puffer-env-test");
    }

    #[test]
    fn config_from_lookup_falls_back_to_defaults_when_lookup_returns_none() {
        let cfg = super::config_from_lookup(|_| None);
        let defaults = FoyerCacheConfig::default();
        assert_eq!(cfg.ram_bytes, defaults.ram_bytes);
        assert_eq!(cfg.ssd_bytes, defaults.ssd_bytes);
        assert_eq!(cfg.block_size, defaults.block_size);
        assert_eq!(cfg.iouring, defaults.iouring);
        assert_eq!(cfg.ssd_dir, defaults.ssd_dir);
    }

    #[test]
    fn backend_selected_only_when_lookup_says_hybrid() {
        assert!(!super::backend_selected_with(|_| None));
        assert!(!super::backend_selected_with(|_| Some("legacy".to_string())));
        assert!(super::backend_selected_with(|_| Some("hybrid".to_string())));
        assert!(super::backend_selected_with(|_| Some("HYBRID".to_string())));
        assert!(super::backend_selected_with(|_| Some("Hybrid".to_string())));
    }
}
