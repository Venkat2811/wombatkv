#![forbid(unsafe_code)]
//! Embeddable KV-cache store API for inference engines.
//!
//! Combines the in-process [`FoyerHybridCache`]
//! (G2 RAM + G3 `NVMe`) with a durable object store (G4: S3, `MinIO`,
//! local fs via [`wombatkv_store::wal_store::InMemoryObjectStore`] for tests) so
//! a single inference engine can:
//!
//! 1. Write a KV blob through both tiers (foyer hot, S3 cold) on prefill.
//! 2. Read with foyer-first, S3-fallback semantics on decode / cold start.
//! 3. Restart cleanly: a fresh process can rehydrate foyer from S3 on
//!    boot, so no work is lost when the engine restarts.
//!
//! The store is generic over [`wombatkv_store::wal_store::ObjectStore`], so the
//! same code path is exercised by unit tests (in-memory) and by live
//! MinIO/S3 integration tests.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use wombatkv_store::wal_store::{ObjectStore, WalStoreError};

use crate::compression::{
    decode_if_compressed, encode_with_header, BlockCompressionConfig, CompressAlgo,
};
use crate::embed_metrics::{metrics, Op};
use crate::foyer_cache::{FoyerCacheConfig, FoyerCacheError, FoyerHitTier, FoyerHybridCache};

const DEFAULT_S3_PREFIX: &str = "kv";

/// Soft warn threshold for [`WombatKVKvStore::bootstrap_world_knowledge`].
/// Logged once when an `ObjectStore::list_prefix` returns ≥ this many
/// keys, multi-tenant production buckets approaching this size are
/// healthy but worth an operator look (cf. RFC 0008 §5).
const BOOTSTRAP_KEY_LIMIT_WARN: usize = 100_000;

/// Hard log threshold for [`WombatKVKvStore::bootstrap_world_knowledge`].
/// Above this we still process every key (the prior contract is
/// preserved) but emit a louder "`limit_exceeded`" event so the caller
/// can decide whether to bound the bootstrap on the next run.
const BOOTSTRAP_KEY_LIMIT_HARD: usize = 1_000_000;

/// Tuning knobs for the embeddable KV store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmbedConfig {
    /// Prefix prepended to every S3 key. Combined with `namespace` per put.
    pub s3_prefix: String,
    /// Foyer config. Reused verbatim for the hybrid memory + disk tier.
    pub foyer: FoyerCacheConfig,
    /// When true, `put_kv` blocks until the S3 write returns. When false,
    /// foyer is updated synchronously and S3 is best-effort.
    pub write_through_s3: bool,
    /// Transparent block-storage compression policy. Default: off. When
    /// enabled, the put path encodes payloads with the `WBZ1` envelope
    /// before the object-store PUT; the get path detects the envelope
    /// on cold reads and decompresses transparently. In-memory tiers
    /// (flat + foyer) always hold uncompressed bytes so cache hits stay
    /// allocation-cheap. See `crate::compression`.
    pub compression: BlockCompressionConfig,
}

impl Default for EmbedConfig {
    fn default() -> Self {
        Self {
            s3_prefix: DEFAULT_S3_PREFIX.to_string(),
            foyer: FoyerCacheConfig::default(),
            write_through_s3: true,
            compression: BlockCompressionConfig::default(),
        }
    }
}

/// Errors surfaced by the embeddable KV store.
#[derive(Debug)]
pub enum EmbedError {
    Foyer(FoyerCacheError),
    ObjectStore(WalStoreError),
    InvalidConfig(String),
}

impl std::fmt::Display for EmbedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Foyer(err) => write!(f, "WombatKV puffer error: {err}"),
            Self::ObjectStore(err) => write!(f, "object store error: {err:?}"),
            Self::InvalidConfig(msg) => write!(f, "invalid config: {msg}"),
        }
    }
}

impl std::error::Error for EmbedError {}

impl From<FoyerCacheError> for EmbedError {
    fn from(value: FoyerCacheError) -> Self {
        Self::Foyer(value)
    }
}

impl From<WalStoreError> for EmbedError {
    fn from(value: WalStoreError) -> Self {
        Self::ObjectStore(value)
    }
}

/// A KV cache lookup hit, with the tier the value came from.
#[derive(Debug, Clone)]
pub enum HitTier {
    Foyer,
    ObjectStore,
}

/// Outcome of a `get_kv` call.
#[derive(Debug, Clone)]
pub enum GetOutcome {
    Hit { tier: HitTier, payload: Bytes },
    Miss,
}

/// Embeddable KV cache store.
///
/// Wraps a [`FoyerHybridCache`] for hot-path lookups and an [`ObjectStore`]
/// (typically S3 / `MinIO`) for durability. Designed to be embedded in an
/// inference engine binary (e.g. `vllm.rs`) without dragging in any
/// additional async runtime.
pub struct WombatKVKvStore<S: ObjectStore> {
    foyer: Arc<FoyerHybridCache>,
    /// Profile-driven addition (2026-05-15 _debug branch): foyer's
    /// `block_on(inner.get())` was 17.96 ms of 18.36 ms warm C ABI time.
    /// Flat file + page-cache hit drops that to ~9 ms. Foyer stays for
    /// the cold-via-S3 path and the multi-tenant SSD-spill story; the
    /// flat tier handles the same-machine warm hot path.
    flat: Arc<crate::kv_blob_cache::FlatFileKvBlobCache>,
    object_store: S,
    s3_prefix: String,
    write_through_s3: bool,
    /// Block compression policy applied at the object-store boundary.
    /// Cache tiers always hold uncompressed bytes. See `EmbedConfig`.
    compression: BlockCompressionConfig,
    /// World-knowledge index: on startup, the puffer can populate this
    /// from `list_prefix` (manifest reads) so it knows what's in the
    /// bucket without lookup-by-lookup S3 round-trips. RFC 0008 §5.
    metadata_index: Arc<wombatkv_radix::InMemoryMetadataIndex>,
}

impl<S: ObjectStore> WombatKVKvStore<S> {
    /// Build a new store. The foyer cache is created up front; the object
    /// store handle is passed in so callers can configure S3 credentials
    /// or supply an in-memory backend for tests.
    pub fn new(config: EmbedConfig, object_store: S) -> Result<Self, EmbedError> {
        if config.s3_prefix.is_empty() {
            return Err(EmbedError::InvalidConfig("s3_prefix must be non-empty".to_string()));
        }
        let flat_root = config.foyer.ssd_dir.join("_flat");
        let flat = Arc::new(
            crate::kv_blob_cache::FlatFileKvBlobCache::open(flat_root)
                .map_err(|err| EmbedError::InvalidConfig(format!("flat cache open: {err}")))?,
        );
        let foyer = FoyerHybridCache::open(config.foyer)?;
        Ok(Self {
            foyer,
            flat,
            object_store,
            s3_prefix: config.s3_prefix,
            write_through_s3: config.write_through_s3,
            compression: config.compression,
            metadata_index: Arc::new(wombatkv_radix::InMemoryMetadataIndex::new()),
        })
    }

    /// Build a store reusing an already-opened foyer instance. Useful when
    /// the engine wants to share one foyer across multiple stores.
    pub fn with_foyer(
        foyer: Arc<FoyerHybridCache>,
        object_store: S,
        s3_prefix: impl Into<String>,
        write_through_s3: bool,
    ) -> Result<Self, EmbedError> {
        let s3_prefix = s3_prefix.into();
        if s3_prefix.is_empty() {
            return Err(EmbedError::InvalidConfig("s3_prefix must be non-empty".to_string()));
        }
        // Sibling flat-file cache next to foyer's SSD tier. Same dir
        // tree, distinct subdir so we don't collide with foyer's blocks.
        let flat_root = foyer.ssd_dir().join("_flat");
        let flat = Arc::new(
            crate::kv_blob_cache::FlatFileKvBlobCache::open(flat_root)
                .map_err(|err| EmbedError::InvalidConfig(format!("flat cache open: {err}")))?,
        );
        Ok(Self {
            foyer,
            flat,
            object_store,
            s3_prefix,
            write_through_s3,
            compression: BlockCompressionConfig::from_env(),
            metadata_index: Arc::new(wombatkv_radix::InMemoryMetadataIndex::new()),
        })
    }

    /// Expose the block-compression policy currently in force. Test
    /// helper, production code reads from the put/get paths instead.
    #[must_use]
    pub fn compression(&self) -> BlockCompressionConfig {
        self.compression
    }

    /// Expose the metadata index. Callers (e.g. the FFI Handle on
    /// startup) can call `bootstrap_world_knowledge` to populate it
    /// from S3, or query it directly for chain-aware lookups.
    pub fn metadata_index(&self) -> Arc<wombatkv_radix::InMemoryMetadataIndex> {
        self.metadata_index.clone()
    }

    /// World-knowledge bootstrap (RFC 0008 §5): walk the S3 prefix for
    /// `namespace`, read each manifest, decode the chunk-hash chain,
    /// populate the in-memory metadata index. After this returns, the
    /// puffer "knows what's in the bucket" without per-request S3
    /// round-trips.
    ///
    /// Cost is O(M) S3 GETs where M = manifest count. Each manifest is
    /// tiny (~800 bytes for 23 chunks). For 100 cached prompts: ~100
    /// small GETs ≈ 1-2 s on local `MinIO`. Run at startup behind an env
    /// gate; not on the request hot path.
    ///
    /// Pagination: the underlying `ObjectStore::list_prefix` is
    /// responsible for exhausting S3's 1000-keys-per-page continuation
    /// loop and returning every match, see the `S3ObjectStore::list_prefix`
    /// impl, which iterates the `rust-s3` `Vec<ListBucketResult>` already
    /// returned in fully-paged form. We add a defensive log + warn here
    /// so an unexpectedly large bucket surfaces in operator logs instead
    /// of silently bottling up: real production buckets above
    /// `BOOTSTRAP_KEY_LIMIT_WARN` keys deserve an operator look (likely
    /// a stale-data sweep is overdue), and above `BOOTSTRAP_KEY_LIMIT_HARD`
    /// we still process them but emit an explicit "exceeded" event so the
    /// caller can decide to bound the work.
    pub fn bootstrap_world_knowledge(&self, namespace: &str) -> Result<usize, EmbedError> {
        use wombatkv_radix::MetadataIndex;
        let started = Instant::now();
        let prefix = if namespace.is_empty() {
            format!("{}/", self.s3_prefix)
        } else {
            format!("{}/{}/", self.s3_prefix, namespace)
        };
        let keys = self.object_store.list_prefix(&prefix).map_err(EmbedError::from)?;
        let key_count = keys.len();
        // DST invariant: bootstrap_world_knowledge must never load more
        // keys than the hard limit emits as warning. Catches future
        // regressions where the limit-warn path silently grows past
        // its envelope. Inert in non-dst builds.
        #[cfg(feature = "dst")]
        wombatkv_dst::assert_always(
            key_count <= BOOTSTRAP_KEY_LIMIT_HARD.saturating_mul(2),
            "bootstrap key_count within 2× hard limit",
            format!("got {key_count} keys, hard limit {BOOTSTRAP_KEY_LIMIT_HARD}"),
        );
        // DST coverage: empty-namespace bootstrap should be exercised
        // by some seeded run. Sticks once any namespace returns 0 keys.
        #[cfg(feature = "dst")]
        wombatkv_dst::assert_sometimes(
            key_count == 0,
            "bootstrap saw empty namespace",
            "DST coverage gate, exercises the empty-bucket cold path",
        );
        if key_count >= BOOTSTRAP_KEY_LIMIT_HARD {
            eprintln!(
                "[MyelonInstr] {{\"scope\":\"wmbt_kv_warn\",\"fn\":\"bootstrap_world_knowledge\",\
                 \"event\":\"key_count_exceeded_hard_limit\",\"keys\":{key_count},\
                 \"limit\":{BOOTSTRAP_KEY_LIMIT_HARD},\"prefix\":\"{prefix}\"}}"
            );
        } else if key_count >= BOOTSTRAP_KEY_LIMIT_WARN {
            eprintln!(
                "[MyelonInstr] {{\"scope\":\"wmbt_kv_warn\",\"fn\":\"bootstrap_world_knowledge\",\
                 \"event\":\"key_count_high\",\"keys\":{key_count},\
                 \"warn_at\":{BOOTSTRAP_KEY_LIMIT_WARN},\"prefix\":\"{prefix}\"}}"
            );
        }
        // Production block-keys land at `wombatkv/v1/block/b3=<hex>` -
        // content-addressed, no per-prompt manifest blob (chain lives only
        // in the in-process metadata index at write time). Bootstrap parses
        // each block key directly and inserts it as a standalone root entry.
        // `lookup_block_prefix` only checks presence, so chain wiring is
        // optional here.
        //
        // Sidecar keys (`wombatkv/v1/sidecar/raw_tail/b3=<hex>`) carry the
        // 28-byte raw_tail payload for warm-restore. We GET each and stuff
        // it into the flat blob cache so the subsequent prompt-path
        // `get_raw_tail_borrowed` is a cache hit instead of an S3 RTT.
        // This shaves ~60ms off the cell-B warm restore on Mac MinIO.
        let mut blocks_loaded: usize = 0;
        let mut skipped_unrecognized: usize = 0;
        let mut sidecars_prewarmed: usize = 0;
        let mut sidecar_bytes_total: usize = 0;
        let mut sidecar_keys: Vec<&String> = Vec::new();

        // Match keys via the canonical prefixes from wombatkv-radix so a
        // future rename touches a single source of truth (the cabi PUT
        // path and the prefetch path read from the same constants).
        let block_key_infix = format!("/{}", wombatkv_radix::BLOCK_KEY_PREFIX);
        let sidecar_key_infix = format!("/{}", wombatkv_radix::SIDECAR_RAW_TAIL_KEY_PREFIX);

        for key in &keys {
            if let Some(idx) = key.find(&block_key_infix) {
                let hex = &key[idx + block_key_infix.len()..];
                if hex.len() == 64 {
                    let mut hash = [0u8; 32];
                    if decode_hex32(hex, &mut hash) {
                        let meta = wombatkv_radix::BlockMeta::new_root(0, [0u8; 24], [0u8; 16]);
                        self.metadata_index.insert(hash, meta);
                        blocks_loaded += 1;
                    }
                }
                continue;
            }
            if key.contains(&sidecar_key_infix) {
                sidecar_keys.push(key);
                continue;
            }
            skipped_unrecognized += 1;
        }
        // Pre-warm raw_tail sidecars into the flat blob cache so the prompt
        // path skips the S3 RTT. We call `self.get_kv(ns, rel_key)` which
        // already does the canonical "GET + decode_if_compressed + flat-cache
        // PUT" pipeline, reusing it means the prewarm cache shape is
        // guaranteed to match what the prompt path looks for.
        for full_key in &sidecar_keys {
            let rel_idx = full_key.find("/wombatkv/v1/").map_or(0, |i| i + 1);
            let rel_key = full_key[rel_idx..].to_string();
            match self.get_kv(namespace, &rel_key) {
                Ok(GetOutcome::Hit { payload, .. }) => {
                    sidecar_bytes_total = sidecar_bytes_total.saturating_add(payload.len());
                    sidecars_prewarmed += 1;
                }
                Ok(GetOutcome::Miss) | Err(_) => {
                    // Best-effort prewarm, a miss here just means the prompt
                    // path will pay the S3 RTT for raw_tail. Not fatal.
                }
            }
        }
        let elapsed_ms = started.elapsed().as_millis();
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_timing\",\"fn\":\"bootstrap_world_knowledge\",\
             \"stages\":{{\"total_ms\":{elapsed_ms},\"blocks_indexed\":{blocks_loaded},\
             \"sidecars_prewarmed\":{sidecars_prewarmed},\
             \"sidecar_bytes_total\":{sidecar_bytes_total},\
             \"unrecognized_keys\":{skipped_unrecognized},\
             \"namespace\":\"{namespace}\"}}}}"
        );
        Ok(blocks_loaded)
    }

    /// L1 bootstrap from `SlateDbMetadataIndex` (RFC 0008 §5 fast path).
    ///
    /// Snapshots all (hash, meta) pairs from the persistent SlateDB-backed
    /// index and bulk-loads them into the in-memory `metadata_index`.
    /// On a fresh process this lets us rehydrate "what's in the world"
    /// in milliseconds, one local `SlateDB` scan vs the O(M) S3 GETs the
    /// S3-based `bootstrap_world_knowledge` would issue.
    ///
    /// Idempotent: `InMemoryMetadataIndex::bulk_load` skips already-present
    /// hashes, so a second call with the same `SlateDB` returns the same
    /// loaded count and does not clobber any in-memory access stamps.
    ///
    /// Returns the number of entries pulled out of `SlateDB` (this is the
    /// `SlateDB` row count, not the net-new RAM-index inserts, by design,
    /// mirroring `bootstrap_world_knowledge`'s "blocks loaded" semantic).
    pub fn bootstrap_from_slatedb(
        &self,
        slatedb_index: &wombatkv_radix::SlateDbMetadataIndex,
    ) -> Result<usize, EmbedError> {
        use wombatkv_radix::MetadataIndex;
        let started = Instant::now();
        let entries = slatedb_index.entries();
        let count = entries.len();
        self.metadata_index.bulk_load(entries);
        let elapsed_ms = started.elapsed().as_millis();
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_timing\",\"fn\":\"bootstrap_from_slatedb\",\
             \"stages\":{{\"total_ms\":{elapsed_ms},\"blocks_loaded\":{count}}}}}"
        );
        Ok(count)
    }

    /// Compose the S3 object key for a given namespace + cache key.
    /// Layout: `{s3_prefix}/{namespace}/{key}`. Namespace and key MUST be
    /// callers' responsibility to keep filesystem-safe.
    #[must_use]
    pub fn object_key(&self, namespace: &str, key: &str) -> String {
        if namespace.is_empty() {
            format!("{}/{}", self.s3_prefix, key)
        } else {
            format!("{}/{}/{}", self.s3_prefix, namespace, key)
        }
    }

    /// Local-only cache key (foyer's key space). Mirrors `object_key` so a
    /// single string identifies the same blob in both tiers.
    fn cache_key(&self, namespace: &str, key: &str) -> String {
        self.object_key(namespace, key)
    }

    /// Write a payload through both tiers.
    ///
    /// Always inserts into foyer synchronously. When `write_through_s3` is
    /// true the call also blocks on the S3 PUT and surfaces any error;
    /// when false the S3 write is best-effort and only logs on failure.
    pub fn put_kv(&self, namespace: &str, key: &str, payload: Bytes) -> Result<(), EmbedError> {
        // DST chaos site:
        // Two complementary fault-injection paths:
        //  (a) dst_buggify!(), probabilistic per-callsite roll driven
        //      by WMBT_KV_DST_BUGGIFY env vars. Used for general chaos
        //      exploration across seeds.
        //  (b) dst_plan::is_put_suppressed(), plan-aware fault dispatch.
        //      Returns true if the loaded FaultPlan has a put-suppressing
        //      event (S3PutFailure / S3PutLatency / KillBeforeChainHead /
        //      KillBeforeSidecar) scheduled for the current op counter.
        //      The DST runner calls dst_plan::set_plan() at startup and
        //      dst_plan::advance_op() between each op so this consult
        //      lines up with the scheduled trigger.
        // Both are inert in non-dst builds. Either path returning true
        // simulates the "S3 PUT failed mid-chain" failure class, the
        // caller's partial-chain-recovery path is what we're exercising.
        #[cfg(feature = "dst")]
        {
            if wombatkv_dst::dst_buggify!() {
                return Err(EmbedError::InvalidConfig(
                    "dst buggify: simulated put_kv S3 PUT failure".to_string(),
                ));
            }
            if wombatkv_dst::dst_plan::is_put_suppressed() {
                return Err(EmbedError::InvalidConfig(
                    "dst_plan: scheduled put-suppressing fault for current op".to_string(),
                ));
            }
        }
        let started = Instant::now();
        let cache_key = self.cache_key(namespace, key);
        let object_key = self.object_key(namespace, key);
        let bytes_len = payload.len() as u64;

        // Write to flat cache first (fast warm-read path). Caches always
        // hold uncompressed bytes so the warm-read TTFT story doesn't
        // pay a per-hit decode tax.
        crate::kv_blob_cache::KvBlobCache::put(self.flat.as_ref(), &cache_key, payload.clone());
        self.foyer.put(&cache_key, payload.clone());

        // Compress (or pass through) at the object-store boundary.
        let on_wire = encode_for_storage(&payload, self.compression, key, "put_kv")?;
        let result = if self.write_through_s3 {
            self.object_store.put_object(&object_key, &on_wire).map_err(EmbedError::from)
        } else {
            if let Err(err) = self.object_store.put_object(&object_key, &on_wire) {
                eprintln!("wombatkv: best-effort S3 PUT failed for {object_key}: {err:?}");
            }
            Ok(())
        };

        let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        metrics().observe(Op::Stash, elapsed_us, bytes_len);
        // emit per-component timing so
        // the global latency_histogram registry picks it up. Tag is
        // <func>:<path>:<stage> per emit_timing's record_global convention.
        emit_timing(
            "WombatKVKvStore.put_kv",
            if self.write_through_s3 { "s3_write_through" } else { "s3_best_effort" },
            &[("total_us", elapsed_us), ("payload_bytes", bytes_len)],
        );
        result
    }

    /// Foyer-sync write, S3 write spawned on a detached thread.
    ///
    /// Returns as soon as foyer has the bytes, typically within a few
    /// hundred microseconds, so callers (e.g. ds4 right after Metal
    /// prefill) can move on to decode while the slow `ObjectStore` PUT
    /// happens off-thread. Foyer is updated atomically inside this call,
    /// so any subsequent `get_kv` against the same key (from the same
    /// process) will hit foyer-RAM, not race the in-flight S3 write.
    ///
    /// Trade-offs vs the synchronous [`Self::put_kv`]:
    /// - Cross-process GET against the same key racing the background
    ///   S3 PUT will miss in S3 until the write completes (foyer is
    ///   process-local). Not safe for cross-engine sharing under that
    ///   pattern; safe for the single-client-per-host shape ds4 uses.
    /// - Spawn failure (rare) loses the S3 write entirely; foyer still
    ///   has it. We log to stderr and otherwise swallow because the
    ///   caller (ds4) has already moved on.
    /// - One detached thread per call. For sustained high-rate puts a
    ///   bounded executor would be safer; the current ds4 pattern is
    ///   one put per chat completion (a few per minute at most), so the
    ///   thread cost is negligible.
    pub fn put_kv_async_s3(this: Arc<Self>, namespace: &str, key: &str, payload: Bytes) {
        let cache_key = this.cache_key(namespace, key);
        let object_key = this.object_key(namespace, key);
        let bytes_len = payload.len() as u64;

        // Write to flat cache first (fast warm-read path). Caches always
        // hold uncompressed bytes.
        crate::kv_blob_cache::KvBlobCache::put(this.flat.as_ref(), &cache_key, payload.clone());
        this.foyer.put(&cache_key, payload.clone());

        let compression_cfg = this.compression;
        let object_store = this.object_store.clone();
        let object_key_owned = object_key;
        let key_owned = key.to_string();
        match std::thread::Builder::new()
            .name(format!("wombatkv-embed-async-s3-{bytes_len}"))
            .spawn(move || {
                // Compress at the wire boundary, same envelope as the
                // synchronous put_kv. Block writes never exceed the
                // user-tunable WMBT_KV_BLOCK_TOKENS so no chunking
                // (the legacy byte-chunked path was deleted).
                let on_wire = match encode_for_storage(
                    &payload,
                    compression_cfg,
                    &key_owned,
                    "put_kv_async_s3",
                ) {
                    Ok(buf) => buf,
                    Err(err) => {
                        eprintln!("wombatkv[embed-async-s3]: compress {object_key_owned}: {err}");
                        return;
                    }
                };
                if let Err(err) = object_store.put_object(&object_key_owned, &on_wire) {
                    eprintln!("wombatkv[embed-async-s3]: put_object {object_key_owned}: {err:?}");
                }
            }) {
            Ok(_) => {}
            Err(err) => {
                eprintln!(
                    "wombatkv[embed-async-s3]: spawn failed (foyer has the bytes, S3 write skipped): {err}"
                );
            }
        }
    }

    /// Look up a payload, foyer-first, S3-fallback. On S3 hit the value is
    /// promoted into foyer so subsequent calls hit the warm path.
    ///
    /// Each load emits a `[MyelonInstr]` JSON line attributing latency to
    /// the actual tier that served the hit (foyer RAM, foyer SSD, S3, or
    /// miss). Critical for diagnosing the "blob too big for RAM" pattern, e.g. qwen3's pre-allocated 4.7 GiB KV cache against a 2 GiB RAM
    /// budget always streams from SSD, which a single `LoadFoyer` bucket
    /// can't tell apart from a fast in-memory hit.
    pub fn get_kv(&self, namespace: &str, key: &str) -> Result<GetOutcome, EmbedError> {
        let started = Instant::now();
        let cache_key = self.cache_key(namespace, key);
        let t_cache_key = started.elapsed().as_micros() as u64;

        // Flat-file fast path, std::fs::read from OS page cache.
        // Profile (2026-05-15) showed foyer's block_on(inner.get()) was
        // 17.96 ms / 18.36 ms warm TTFT. Flat file drops that to ~9 ms.
        if let Some((payload, op_label)) =
            crate::kv_blob_cache::KvBlobCache::get(self.flat.as_ref(), &cache_key)
        {
            let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            let bytes_len = payload.len() as u64;
            // Reuse the LoadFoyerRam Op for flat hits, same shape
            // (local cache hit, no S3) for the metrics aggregator. We
            // distinguish via the [MyelonInstr] op_label.
            metrics().observe(Op::LoadFoyerRam, elapsed_us, bytes_len);
            emit_tier_event(op_label, "flat", key, bytes_len, elapsed_us);
            emit_timing(
                "WombatKVKvStore.get_kv",
                "flat_hit",
                &[
                    ("cache_key_us", t_cache_key),
                    ("flat_call_us", elapsed_us - t_cache_key),
                    ("total_us", elapsed_us),
                    ("payload_bytes", bytes_len),
                ],
            );
            return Ok(GetOutcome::Hit { tier: HitTier::Foyer, payload });
        }
        let t_flat_done = started.elapsed().as_micros() as u64;
        let foyer_result = self.foyer.get_with_tier(&cache_key);
        let t_foyer_done = started.elapsed().as_micros() as u64;
        if let Some((payload, tier)) = foyer_result {
            let bytes_len = payload.len() as u64;
            let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            let op = match tier {
                FoyerHitTier::Ram => Op::LoadFoyerRam,
                FoyerHitTier::Ssd => Op::LoadFoyerSsd,
            };
            metrics().observe(op, elapsed_us, bytes_len);
            emit_tier_event(op.as_str(), tier.as_str(), key, bytes_len, elapsed_us);
            // Foyer hit but flat missed (file evicted/never written). Repair
            // the flat tier for next time.
            crate::kv_blob_cache::KvBlobCache::put(self.flat.as_ref(), &cache_key, payload.clone());
            emit_timing(
                "WombatKVKvStore.get_kv",
                match tier {
                    FoyerHitTier::Ram => "foyer_ram_hit",
                    FoyerHitTier::Ssd => "foyer_ssd_hit",
                },
                &[
                    ("cache_key_us", t_cache_key),
                    ("flat_miss_us", t_flat_done - t_cache_key),
                    ("foyer_call_us", t_foyer_done - t_flat_done),
                    ("total_us", elapsed_us),
                    ("payload_bytes", bytes_len),
                ],
            );
            return Ok(GetOutcome::Hit { tier: HitTier::Foyer, payload });
        }

        let object_key = self.object_key(namespace, key);
        let t_obj_key = started.elapsed().as_micros() as u64;
        match self.object_store.get_object(&object_key) {
            Ok(payload) => {
                let t_s3_done = started.elapsed().as_micros() as u64;
                // `bytes_len` is now recomputed per-branch: the manifest
                // Transparent decompression at the wire boundary. If the
                // blob carries the `WBZ1` envelope, decode into an owned
                // Vec; otherwise pass `payload` through unchanged (the
                // `Cow::Borrowed` branch). The cache tiers always see
                // uncompressed bytes.
                let on_wire_bytes = payload.len() as u64;
                let bytes = match decode_if_compressed(&payload) {
                    std::borrow::Cow::Borrowed(_) => Bytes::from(payload),
                    std::borrow::Cow::Owned(decoded) => Bytes::from(decoded),
                };
                let uncompressed_bytes_len = bytes.len() as u64;
                let t_from_vec = started.elapsed().as_micros() as u64;
                // Populate both tiers so future reads hit flat first.
                crate::kv_blob_cache::KvBlobCache::put(
                    self.flat.as_ref(),
                    &cache_key,
                    bytes.clone(),
                );
                self.foyer.put(&cache_key, bytes.clone());
                let t_foyer_put = started.elapsed().as_micros() as u64;
                let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                metrics().observe(Op::LoadS3, elapsed_us, uncompressed_bytes_len);
                emit_tier_event("load_s3", "s3", key, uncompressed_bytes_len, elapsed_us);
                emit_timing(
                    "WombatKVKvStore.get_kv",
                    "s3_hit",
                    &[
                        ("flat_miss_us", t_flat_done - t_cache_key),
                        ("foyer_miss_us", t_foyer_done - t_flat_done),
                        ("obj_key_us", t_obj_key - t_foyer_done),
                        ("s3_get_us", t_s3_done - t_obj_key),
                        ("bytes_wrap_us", t_from_vec - t_s3_done),
                        ("cache_put_us", t_foyer_put - t_from_vec),
                        ("total_us", elapsed_us),
                        ("payload_bytes", uncompressed_bytes_len),
                        ("on_wire_bytes", on_wire_bytes),
                    ],
                );
                Ok(GetOutcome::Hit { tier: HitTier::ObjectStore, payload: bytes })
            }
            Err(WalStoreError::ObjectNotFound(_)) => {
                let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                metrics().observe(Op::Miss, elapsed_us, 0);
                emit_tier_event("miss", "miss", key, 0, elapsed_us);
                emit_timing("WombatKVKvStore.get_kv", "miss", &[("total_us", elapsed_us)]);
                Ok(GetOutcome::Miss)
            }
            Err(other) => Err(EmbedError::ObjectStore(other)),
        }
    }

    /// Check for a key without materializing the payload.
    ///
    /// This is intentionally separate from `get_kv`: vLLM's scheduler
    /// calls `exists` while deciding whether a prefix can be loaded. If
    /// `exists` falls through to a full GET, large KV payloads traverse the
    /// daemon once during lookup and then again during the real load.
    pub fn exists_kv(&self, namespace: &str, key: &str) -> Result<bool, EmbedError> {
        let cache_key = self.cache_key(namespace, key);
        if self.foyer.contains(&cache_key) {
            return Ok(true);
        }

        let object_key = self.object_key(namespace, key);
        self.object_store.head_object(&object_key).map_err(EmbedError::ObjectStore)
    }

    /// List keys for a namespace as their S3 object keys.
    pub fn list_namespace(&self, namespace: &str) -> Result<Vec<String>, EmbedError> {
        let prefix = if namespace.is_empty() {
            format!("{}/", self.s3_prefix)
        } else {
            format!("{}/{}/", self.s3_prefix, namespace)
        };
        Ok(self.object_store.list_prefix(&prefix)?)
    }

    /// List keys for a namespace relative to that namespace.
    pub fn list_kv_keys(&self, namespace: &str) -> Result<Vec<String>, EmbedError> {
        let prefix = if namespace.is_empty() {
            format!("{}/", self.s3_prefix)
        } else {
            format!("{}/{}/", self.s3_prefix, namespace)
        };
        let mut keys = Vec::new();
        for object_key in self.object_store.list_prefix(&prefix)? {
            if let Some(key) = object_key.strip_prefix(&prefix) {
                keys.push(key.to_string());
            }
        }
        Ok(keys)
    }

    /// Rehydrate foyer from S3. Useful at engine startup so the warm tier
    /// is primed with whatever survived the previous process. Returns the
    /// number of keys restored.
    pub fn restore_from_s3(&self, namespace: &str) -> Result<usize, EmbedError> {
        let started = Instant::now();
        let object_keys = self.list_namespace(namespace)?;
        let mut restored = 0_usize;
        let mut bytes_total: u64 = 0;
        for object_key in object_keys {
            let payload = match self.object_store.get_object(&object_key) {
                Ok(value) => value,
                Err(WalStoreError::ObjectNotFound(_)) => continue,
                Err(other) => return Err(EmbedError::ObjectStore(other)),
            };
            // Decompress at the wire boundary so foyer holds the
            // uncompressed bytes the warm-read path expects. Skips a
            // wasted memcpy on legacy uncompressed blobs via Cow.
            let bytes = match decode_if_compressed(&payload) {
                std::borrow::Cow::Borrowed(_) => Bytes::from(payload),
                std::borrow::Cow::Owned(decoded) => Bytes::from(decoded),
            };
            bytes_total = bytes_total.saturating_add(bytes.len() as u64);
            self.foyer.put(&object_key, bytes);
            restored += 1;
        }
        let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        metrics().observe(Op::RestoreFromS3, elapsed_us, bytes_total);
        Ok(restored)
    }

    /// Delete one block from the object store (and best-effort from the
    /// flat tier). Used by the LRU eviction worker (RFC 0009 §4); not
    /// called on the hot path.
    ///
    /// Returns true iff the object store reported a delete. Foyer is
    /// left untouched: `foyer::HybridCache` does not expose a single-
    /// key remove on its public API in the version we pin; the bytes
    /// will age out naturally as new inserts evict them. The metadata
    /// index (the authority for the per-namespace byte budget) is
    /// updated separately by the worker so the budget accounting stays
    /// correct even while foyer still holds the bytes briefly.
    pub fn delete_kv(&self, namespace: &str, key: &str) -> Result<bool, EmbedError> {
        let cache_key = self.cache_key(namespace, key);
        let object_key = self.object_key(namespace, key);

        // Best-effort flat-tier delete first. If a block leaves the
        // metadata index but remains in flat, the next `get_kv` would
        // still return it (which would defeat eviction).
        let _ = crate::kv_blob_cache::KvBlobCache::remove(self.flat.as_ref(), &cache_key);

        let deleted = self.object_store.delete_object(&object_key)?;
        Ok(deleted)
    }

    /// Drop foyer state. Object-store data is unaffected.
    pub fn clear_foyer(&self) {
        self.foyer.clear();
    }

    /// Drop flat-file blob-cache state. Object-store data is unaffected.
    ///
    /// The flat tier sits in front of foyer on the get path (see commit
    /// `2ca65cb`). Tests that want to exercise the object-store fallback
    /// must clear both tiers, otherwise the flat hit short-circuits before
    /// foyer is consulted. Use [`Self::clear_foyer`] for the foyer tier.
    pub fn clear_flat_cache(&self) {
        crate::kv_blob_cache::KvBlobCache::clear(self.flat.as_ref());
    }

    /// Borrow the underlying foyer cache (e.g. for stats or sharing).
    #[must_use]
    pub fn foyer(&self) -> &Arc<FoyerHybridCache> {
        &self.foyer
    }

    /// Borrow the underlying object store (e.g. for direct list/delete).
    #[must_use]
    pub fn object_store(&self) -> &S {
        &self.object_store
    }

    /// Spawn the background block-prefetch worker (RFC 0008 §6).
    ///
    /// The worker periodically scores the metadata index per the
    /// recency / chain-head / model-affinity heuristic and (v2) issues
    /// `get_kv` GETs for the top-K candidates, materializing the
    /// payloads into the local flat tier so subsequent requests hit
    /// warm. See [`crate::block_prefetch`] for the heuristic.
    ///
    /// Behavior is selected at construction time by the
    /// `WMBT_KV_PREFETCH_DRY_RUN=1` env: when set, the worker scores
    /// and logs only (the v1 escape hatch). Default is v2.
    ///
    /// Holds an `Arc` to self via the `PrefetchFetcher` impl, so the
    /// worker can issue GETs. Dropping the returned worker signals
    /// stop and joins the thread.
    /// Spawn the background LRU eviction worker (RFC 0009 §4).
    ///
    /// The worker periodically scans the in-memory metadata index for
    /// the configured namespace, sums `payload_bytes`, and when the
    /// sum exceeds `LruConfig::namespace_max_bytes`, evicts the
    /// oldest entries (by `last_access_ns`) until the budget has a
    /// 10% headroom.
    ///
    /// Caller is responsible for handing in the optional `SlateDB`
    /// index so the L1 persistence is kept in sync. If `None`, only
    /// the L0 in-memory index and the object store are touched.
    ///
    /// Dropping the returned worker signals stop and joins the thread.
    #[must_use]
    pub fn start_eviction_worker(
        self: &Arc<Self>,
        config: crate::lru::LruConfig,
        slatedb: Option<Arc<wombatkv_radix::SlateDbMetadataIndex>>,
    ) -> crate::lru::LruEvictionWorker {
        let deleter: Arc<dyn crate::lru::EvictionDeleter> =
            Arc::new(KvStoreEvictionDeleter::new(self.clone()));
        let emit = crate::lru::default_emit(config.namespace.clone());
        crate::lru::spawn_worker(self.metadata_index.clone(), slatedb, deleter, config, emit)
    }

    #[must_use]
    pub fn start_prefetcher(
        self: &Arc<Self>,
        config: crate::block_prefetch::PrefetchConfig,
    ) -> crate::block_prefetch::PrefetchWorker {
        let index: Arc<dyn wombatkv_radix::MetadataIndex> = self.metadata_index.clone();
        if crate::block_prefetch::dry_run_enabled() {
            eprintln!("wombatkv[prefetch]: WMBT_KV_PREFETCH_DRY_RUN=1 → v1 log-only path");
            return crate::block_prefetch::spawn_worker(
                index,
                config,
                crate::block_prefetch::default_emit(),
            );
        }
        let fetcher: Arc<dyn crate::block_prefetch::PrefetchFetcher> =
            Arc::new(KvStorePrefetchFetcher::new(self.clone()));
        crate::block_prefetch::spawn_worker_v2(
            index,
            config,
            fetcher,
            crate::block_prefetch::default_v2_emit(),
        )
    }
}

/// `EvictionDeleter` adapter binding the LRU worker to
/// `WombatKVKvStore<S>::delete_kv`. Holds an `Arc<WombatKVKvStore<S>>`
/// so the worker's lifetime is independent of the FFI handle that
/// built the store. The adapter is `S`-generic and we lift it back to
/// `Arc<dyn EvictionDeleter>` at the call site.
struct KvStoreEvictionDeleter<S: ObjectStore> {
    store: Arc<WombatKVKvStore<S>>,
}

impl<S: ObjectStore> KvStoreEvictionDeleter<S> {
    fn new(store: Arc<WombatKVKvStore<S>>) -> Self {
        Self { store }
    }
}

impl<S: ObjectStore> crate::lru::EvictionDeleter for KvStoreEvictionDeleter<S> {
    fn delete_block(&self, namespace: &str, key: &str) -> Result<bool, String> {
        self.store.delete_kv(namespace, key).map_err(|err| format!("{err}"))
    }
}

/// `PrefetchFetcher` adapter binding the algorithm-crate worker to
/// `WombatKVKvStore<S>::get_kv`. Holds an `Arc<WombatKVKvStore<S>>` so
/// the worker's lifetime is independent of the FFI handle that built
/// the store. The adapter is `S`-generic and we lift it back to
/// `Arc<dyn PrefetchFetcher>` at the call site.
struct KvStorePrefetchFetcher<S: ObjectStore> {
    store: Arc<WombatKVKvStore<S>>,
}

impl<S: ObjectStore> KvStorePrefetchFetcher<S> {
    fn new(store: Arc<WombatKVKvStore<S>>) -> Self {
        Self { store }
    }
}

impl<S: ObjectStore> crate::block_prefetch::PrefetchFetcher for KvStorePrefetchFetcher<S> {
    fn contains_flat(&self, namespace: &str, key: &str) -> bool {
        let cache_key = self.store.cache_key(namespace, key);
        crate::kv_blob_cache::KvBlobCache::contains(self.store.flat.as_ref(), &cache_key)
    }

    fn fetch_block(&self, namespace: &str, key: &str) -> Result<Option<u64>, String> {
        match self.store.get_kv(namespace, key) {
            Ok(GetOutcome::Hit { payload, .. }) => Ok(Some(payload.len() as u64)),
            Ok(GetOutcome::Miss) => Ok(None),
            Err(err) => Err(format!("{err}")),
        }
    }
}

/// Emit a single-line `[MyelonInstr]` JSON event attributing one load
/// to a specific tier. Off by default to keep production logs quiet;
/// opt in with `WMBT_KV_TIER_EVENTS=1` (or `=stderr`).
///
/// Format mirrors the existing aggregate metrics envelope so downstream
/// log parsers don't need a second schema:
///
///   [`MyelonInstr`] {"`scope":"wombatkv_tier","op":"load_foyer_ssd`",
///                  "`tier":"ssd","key_hash":"...","bytes":4697949641`,
///                  "`elapsed_us":340512`}
///
/// `key_hash` is the first 16 hex chars of `key` to avoid leaking full
/// content-addressed digests into log aggregation systems while still
/// letting operators correlate adjacent events for the same blob.
fn emit_tier_event(op: &str, tier: &str, key: &str, bytes: u64, elapsed_us: u64) {
    if !tier_events_enabled() {
        return;
    }
    let key_hash: String = key.chars().take(16).collect();
    eprintln!(
        "[MyelonInstr] {{\"scope\":\"wombatkv_tier\",\"op\":\"{op}\",\"tier\":\"{tier}\",\"key_hash\":\"{key_hash}\",\"bytes\":{bytes},\"elapsed_us\":{elapsed_us}}}"
    );
}

/// Fine-grained per-stage timing log for the _debug branch. Gated by
/// `WMBT_KV_TIMING=1`. Format:
///
///   [`MyelonInstr`] {"`scope":"wmbt_kv_timing","fn":"foyer_get_with_tier`",
///                  "`path":"ram_hit","stages":{"ram_probe_us":4`,...}}
///
/// `stages` is a flat (name, u64) list rendered as JSON object. Lets us
/// attribute the warm overhead to specific lines, not just tier-level
/// aggregates.
/// Encode `payload` for object-store transit with the optional
/// `WBZ1` envelope. Returns the raw payload as a fresh `Vec<u8>` when
/// compression is disabled, the cost is one memcpy, same as we already
/// paid for the previous put-path `payload.clone()` into S3.
///
/// Emits a `wmbt_kv_compress` timing event when compression actually
/// ran: stages include compress duration, `uncompressed_bytes`,
/// `compressed_bytes`, and the ratio in basis points so downstream
/// dashboards don't have to division on parse.
fn encode_for_storage(
    payload: &[u8],
    cfg: BlockCompressionConfig,
    key: &str,
    func: &'static str,
) -> Result<Vec<u8>, EmbedError> {
    if !cfg.is_enabled() {
        // Fast path: caller's bytes verbatim. One copy; the S3 SDK
        // requires an owned buffer anyway.
        return Ok(payload.to_vec());
    }
    let started = Instant::now();
    let encoded = encode_with_header(payload, cfg)
        .map_err(|err| EmbedError::InvalidConfig(format!("compress {key}: {err}")))?;
    let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let uncompressed = payload.len() as u64;
    let compressed = encoded.len() as u64;
    let ratio_bps = compressed.saturating_mul(10_000).checked_div(uncompressed).unwrap_or(0);
    if timing_enabled() {
        // Hand-rolled emit so we can include the algo + level alongside
        // the integer stages.
        let algo = match cfg.algo {
            CompressAlgo::Zstd => "zstd",
            CompressAlgo::Lz4 => "lz4",
            CompressAlgo::None => "none",
        };
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_compress\",\"fn\":\"{func}\",\
             \"algo\":\"{algo}\",\"level\":{level},\"stages\":{{\
             \"compress_us\":{elapsed_us},\
             \"uncompressed_bytes\":{uncompressed},\
             \"compressed_bytes\":{compressed},\
             \"ratio_bps\":{ratio_bps}}}}}",
            level = cfg.level,
        );
    }
    Ok(encoded)
}

pub fn emit_timing(func: &str, path: &str, stages: &[(&str, u64)]) {
    // ALWAYS record into the global histogram registry -
    // tags are `<func>:<path>:<stage>` for fine-grained per-step
    // percentile tracking. The eprintln below is the verbose
    // single-event log gated by WMBT_KV_TIMING; histogram
    // aggregation runs even when verbose logging is off so the
    // tail-latency surface is always available via
    // `latency_histogram::snapshot_all()` or the periodic dumper.
    for (name, us) in stages {
        let tag = format!("{func}:{path}:{name}");
        crate::latency_histogram::record_global(&tag, *us);
    }

    if !timing_enabled() {
        return;
    }
    let mut buf = String::with_capacity(160);
    buf.push_str("[MyelonInstr] {\"scope\":\"wmbt_kv_timing\",\"fn\":\"");
    buf.push_str(func);
    buf.push_str("\",\"path\":\"");
    buf.push_str(path);
    buf.push_str("\",\"stages\":{");
    for (i, (name, us)) in stages.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push('"');
        buf.push_str(name);
        buf.push_str("\":");
        buf.push_str(&us.to_string());
    }
    buf.push_str("}}");
    eprintln!("{buf}");
}

fn timing_enabled() -> bool {
    static ENABLED: once_cell::sync::OnceCell<bool> = once_cell::sync::OnceCell::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WMBT_KV_TIMING")
            .is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "stderr"))
    })
}

fn tier_events_enabled() -> bool {
    static ENABLED: once_cell::sync::OnceCell<bool> = once_cell::sync::OnceCell::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WMBT_KV_TIER_EVENTS")
            .is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "stderr"))
    })
}

/// Parse a 64-char lowercase hex string into a 32-byte hash. Returns
/// `false` on any non-hex char or wrong length. Used by
/// `bootstrap_world_knowledge` to extract block hashes directly
/// from the S3 list output without round-tripping through GET.
fn decode_hex32(hex: &str, out: &mut [u8; 32]) -> bool {
    if hex.len() != 64 {
        return false;
    }
    let bytes = hex.as_bytes();
    for i in 0..32 {
        let hi = match bytes[2 * i] {
            b'0'..=b'9' => bytes[2 * i] - b'0',
            b'a'..=b'f' => bytes[2 * i] - b'a' + 10,
            b'A'..=b'F' => bytes[2 * i] - b'A' + 10,
            _ => return false,
        };
        let lo = match bytes[2 * i + 1] {
            b'0'..=b'9' => bytes[2 * i + 1] - b'0',
            b'a'..=b'f' => bytes[2 * i + 1] - b'a' + 10,
            b'A'..=b'F' => bytes[2 * i + 1] - b'A' + 10,
            _ => return false,
        };
        out[i] = (hi << 4) | lo;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::{EmbedConfig, GetOutcome, HitTier, WombatKVKvStore};
    use crate::compression::BlockCompressionConfig;
    use crate::foyer_cache::FoyerCacheConfig;
    use bytes::Bytes;
    use tempfile::tempdir;
    use wombatkv_store::wal_store::InMemoryObjectStore;

    fn small_foyer(dir: std::path::PathBuf) -> FoyerCacheConfig {
        FoyerCacheConfig {
            ram_bytes: 8 * 1024 * 1024,
            ssd_dir: dir,
            ssd_bytes: 32 * 1024 * 1024,
            block_size: 1024 * 1024,
            buffer_pool_size: 4 * 1024 * 1024,
            iouring: false,
        }
    }

    fn build_store(dir: std::path::PathBuf) -> WombatKVKvStore<InMemoryObjectStore> {
        let cfg = EmbedConfig {
            s3_prefix: "test/kv".to_string(),
            foyer: small_foyer(dir),
            write_through_s3: true,
            compression: BlockCompressionConfig::default(),
        };
        WombatKVKvStore::new(cfg, InMemoryObjectStore::default()).expect("build store")
    }

    fn build_store_with_compression(
        dir: std::path::PathBuf,
    ) -> WombatKVKvStore<InMemoryObjectStore> {
        let cfg = EmbedConfig {
            s3_prefix: "test/kv".to_string(),
            foyer: small_foyer(dir),
            write_through_s3: true,
            compression: BlockCompressionConfig {
                algo: crate::compression::CompressAlgo::Zstd,
                level: 3,
            },
        };
        WombatKVKvStore::new(cfg, InMemoryObjectStore::default()).expect("build store")
    }

    /// End-to-end: when compression is on, the S3 object on the wire
    /// carries the `WBZ1` envelope; `get_kv` still hands back the
    /// uncompressed bytes; cache hits stay uncompressed too.
    #[test]
    fn compressed_round_trip_through_object_store() {
        let dir = tempdir().expect("tempdir");
        let store = build_store_with_compression(dir.path().to_path_buf());

        // Highly compressible payload: 64 KiB of zeros + a small trailer.
        let mut payload = vec![0_u8; 64 * 1024];
        payload.extend_from_slice(b"trailer bytes");
        let bytes = Bytes::from(payload.clone());

        store.put_kv("ns", "k1", bytes.clone()).expect("put");

        // The actual S3 object carries the compression envelope.
        let object_key = store.object_key("ns", "k1");
        let on_wire = store.object_store().get_object(&object_key).expect("s3 get");
        assert!(crate::compression::has_magic(&on_wire), "expected WBZ1 magic");
        assert!(
            on_wire.len() < payload.len(),
            "compressed payload should be smaller than the original; got {} vs {}",
            on_wire.len(),
            payload.len()
        );

        // Clearing the local caches forces get_kv to traverse the S3
        // path, exercises the decode-on-read branch.
        store.clear_flat_cache();
        store.clear_foyer();

        match store.get_kv("ns", "k1").expect("get") {
            GetOutcome::Hit { tier, payload: got } => {
                assert!(matches!(tier, HitTier::ObjectStore));
                assert_eq!(got.as_ref(), payload.as_slice());
            }
            GetOutcome::Miss => panic!("expected hit"),
        }
    }

    /// Mixed-state bucket: an uncompressed legacy blob and a freshly
    /// compressed blob written by the same compressed-mode store are
    /// both readable.
    #[test]
    fn mixed_compressed_and_legacy_blobs_both_readable() {
        let dir = tempdir().expect("tempdir");
        let store = build_store_with_compression(dir.path().to_path_buf());

        // Inject a legacy uncompressed blob directly into the object store.
        let legacy_payload = b"legacy uncompressed payload".to_vec();
        let legacy_key = store.object_key("ns", "legacy");
        store.object_store().put_object(&legacy_key, &legacy_payload).expect("legacy put");

        // Write a new blob via the compressed put path.
        let fresh_payload = vec![7_u8; 32 * 1024];
        store.put_kv("ns", "fresh", Bytes::from(fresh_payload.clone())).expect("fresh put");

        // Drop caches so both reads have to touch S3.
        store.clear_flat_cache();
        store.clear_foyer();

        match store.get_kv("ns", "legacy").expect("get legacy") {
            GetOutcome::Hit { payload, .. } => {
                assert_eq!(payload.as_ref(), legacy_payload.as_slice());
            }
            GetOutcome::Miss => panic!("legacy miss"),
        }
        match store.get_kv("ns", "fresh").expect("get fresh") {
            GetOutcome::Hit { payload, .. } => {
                assert_eq!(payload.as_ref(), fresh_payload.as_slice());
            }
            GetOutcome::Miss => panic!("fresh miss"),
        }
    }

    #[test]
    fn put_get_round_trip_serves_from_foyer_warm_path() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        let payload = Bytes::from_static(b"qwen3-pd-payload");
        store.put_kv("ns-a", "seq-1", payload.clone()).expect("put");

        match store.get_kv("ns-a", "seq-1").expect("get") {
            GetOutcome::Hit { tier, payload: got } => {
                assert!(matches!(tier, HitTier::Foyer));
                assert_eq!(got, payload);
            }
            GetOutcome::Miss => panic!("expected hit"),
        }
    }

    #[test]
    fn write_through_s3_persists_to_object_store() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        let payload = Bytes::from_static(b"qwen3-prefill-bytes");
        store.put_kv("ns-a", "seq-7", payload.clone()).expect("put");

        let object_key = store.object_key("ns-a", "seq-7");
        let raw = store.object_store().get_object(&object_key).expect("s3 get");
        // S3 holds the encoded form (zstd by default); round-trip-decode to
        // assert payload equality regardless of compression status.
        let decoded = crate::compression::decode_if_compressed(&raw);
        assert_eq!(&*decoded, payload.as_ref());
    }

    #[test]
    fn s3_fallback_serves_value_when_foyer_was_cleared() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        let payload = Bytes::from_static(b"survives-foyer-clear");
        store.put_kv("ns-a", "seq-9", payload.clone()).expect("put");
        // Flat sits in front of foyer on the get path (commit 2ca65cb);
        // both tiers must be dropped to exercise the S3 fallback.
        store.clear_foyer();
        store.clear_flat_cache();

        match store.get_kv("ns-a", "seq-9").expect("get") {
            GetOutcome::Hit { tier, payload: got } => {
                assert!(matches!(tier, HitTier::ObjectStore));
                assert_eq!(got, payload);
            }
            GetOutcome::Miss => panic!("expected S3 fallback hit"),
        }

        // Subsequent get should be a foyer/flat hit because the previous
        // call promoted the value back into the warm tiers (HitTier::Foyer
        // covers both flat and foyer hits in the current API).
        match store.get_kv("ns-a", "seq-9").expect("get-2") {
            GetOutcome::Hit { tier, .. } => assert!(matches!(tier, HitTier::Foyer)),
            GetOutcome::Miss => panic!("expected foyer promotion"),
        }
    }

    #[test]
    fn restart_pattern_rebuilds_foyer_from_s3_only() {
        let dir = tempdir().expect("tempdir");
        let cfg_a = EmbedConfig {
            s3_prefix: "test/kv".to_string(),
            foyer: small_foyer(dir.path().join("a")),
            write_through_s3: true,
            compression: BlockCompressionConfig::default(),
        };
        let object_store = InMemoryObjectStore::default();
        let store_a = WombatKVKvStore::new(cfg_a, object_store.clone()).expect("a");
        for idx in 0..6_u32 {
            let key = format!("seq-{idx}");
            store_a.put_kv("ns", &key, Bytes::from(vec![idx as u8; 1024])).expect("put");
        }
        drop(store_a); // simulate process crash; foyer state is gone

        // Fresh process: same object store handle, new foyer dir.
        let cfg_b = EmbedConfig {
            s3_prefix: "test/kv".to_string(),
            foyer: small_foyer(dir.path().join("b")),
            write_through_s3: true,
            compression: BlockCompressionConfig::default(),
        };
        let store_b = WombatKVKvStore::new(cfg_b, object_store).expect("b");

        let restored = store_b.restore_from_s3("ns").expect("restore");
        assert_eq!(restored, 6);

        for idx in 0..6_u32 {
            let key = format!("seq-{idx}");
            match store_b.get_kv("ns", &key).expect("get") {
                GetOutcome::Hit { tier, payload } => {
                    assert!(matches!(tier, HitTier::Foyer));
                    assert_eq!(payload.as_ref(), vec![idx as u8; 1024].as_slice());
                }
                GetOutcome::Miss => panic!("expected foyer hit after restore"),
            }
        }
    }

    #[test]
    fn miss_returns_miss_without_falling_through_on_unknown_key() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        assert!(matches!(store.get_kv("ns", "missing").expect("get"), GetOutcome::Miss));
    }

    #[test]
    fn exists_kv_checks_foyer_and_object_store_without_loading_payload() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        assert!(!store.exists_kv("ns", "missing").expect("missing exists"));

        store.put_kv("ns", "present", Bytes::from_static(b"exists-payload")).expect("put");
        assert!(store.exists_kv("ns", "present").expect("foyer exists"));

        store.clear_foyer();
        assert!(store.exists_kv("ns", "present").expect("object store exists"));
    }

    #[test]
    fn list_namespace_returns_only_prefix_matching_keys() {
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());

        store.put_kv("ns-a", "k1", Bytes::from_static(b"a1")).expect("a1");
        store.put_kv("ns-a", "k2", Bytes::from_static(b"a2")).expect("a2");
        store.put_kv("ns-b", "k1", Bytes::from_static(b"b1")).expect("b1");

        let mut a_keys = store.list_namespace("ns-a").expect("list-a");
        let mut b_keys = store.list_namespace("ns-b").expect("list-b");
        a_keys.sort();
        b_keys.sort();
        assert_eq!(a_keys, vec!["test/kv/ns-a/k1", "test/kv/ns-a/k2"]);
        assert_eq!(b_keys, vec!["test/kv/ns-b/k1"]);

        let mut a_relative = store.list_kv_keys("ns-a").expect("relative-a");
        a_relative.sort();
        assert_eq!(a_relative, vec!["k1", "k2"]);
    }

    #[test]
    fn empty_s3_prefix_is_rejected() {
        let dir = tempdir().expect("tempdir");
        let cfg = EmbedConfig {
            s3_prefix: String::new(),
            foyer: small_foyer(dir.path().to_path_buf()),
            write_through_s3: true,
            compression: BlockCompressionConfig::default(),
        };
        let result = WombatKVKvStore::new(cfg, InMemoryObjectStore::default());
        assert!(result.is_err());
    }

    fn mk_meta_for_test(seq: u32, parent: wombatkv_radix::BlockHash) -> wombatkv_radix::BlockMeta {
        wombatkv_radix::BlockMeta::new_successor(
            parent,
            seq,
            1024,
            [42u8; 24],
            *b"test-v1\0\0\0\0\0\0\0\0\0",
        )
    }

    #[test]
    fn bootstrap_from_slatedb_roundtrip() {
        use wombatkv_radix::{BlockMeta, MetadataIndex, SlateDbMetadataIndex};

        let dir = tempdir().expect("tempdir");
        let slatedb_root = dir.path().join("slatedb-root");
        let store = build_store(dir.path().join("kv-store"));

        let slatedb_index =
            SlateDbMetadataIndex::open_local(&slatedb_root, "node-bootstrap-rt", "tenant-a")
                .expect("open slatedb");

        // Seed 10 entries forming a chain: h0 is root, h_i has parent h_{i-1}.
        let mut hashes: Vec<[u8; 32]> = Vec::with_capacity(10);
        for i in 0..10u8 {
            hashes.push([i + 1; 32]);
        }
        let mut parent = BlockMeta::ZERO_HASH;
        for (i, h) in hashes.iter().enumerate() {
            slatedb_index.insert(*h, mk_meta_for_test(i as u32, parent));
            parent = *h;
        }
        assert_eq!(slatedb_index.len(), 10);

        let count = store.bootstrap_from_slatedb(&slatedb_index).expect("bootstrap_from_slatedb");
        assert_eq!(count, 10);

        let in_mem = store.metadata_index();
        assert_eq!(in_mem.len(), 10);
        // Spot-check a couple of entries copied correctly.
        let got = in_mem.get(&hashes[0]).expect("h0 must be in RAM index");
        assert_eq!(got.block_seq, 0);
        let got_last = in_mem.get(&hashes[9]).expect("h9 must be in RAM index");
        assert_eq!(got_last.block_seq, 9);
        assert_eq!(got_last.parent_hash, hashes[8]);
    }

    #[test]
    fn bootstrap_from_slatedb_empty() {
        use wombatkv_radix::{MetadataIndex, SlateDbMetadataIndex};

        let dir = tempdir().expect("tempdir");
        let slatedb_root = dir.path().join("slatedb-root");
        let store = build_store(dir.path().join("kv-store"));

        let slatedb_index =
            SlateDbMetadataIndex::open_local(&slatedb_root, "node-bootstrap-empty", "tenant-a")
                .expect("open slatedb");

        let count =
            store.bootstrap_from_slatedb(&slatedb_index).expect("bootstrap_from_slatedb on empty");
        assert_eq!(count, 0);
        assert_eq!(store.metadata_index().len(), 0);
    }

    /// Simulates the daemon's metadata-persistence contract:
    /// `put_kv_blocks` writes through to both the in-memory index and
    /// `SlateDB`. Across a "daemon restart" (close + reopen at the same
    /// `SlateDB` path), the new process's `bootstrap_from_slatedb` hydrates
    /// the in-memory index so `longest_prefix` matches the previously-
    /// saved chain.
    ///
    /// This is the unit-test equivalent of "daemon starts → client
    /// `put_kv_blocks` → daemon restarts → client `lookup_block_prefix`
    /// finds the chain": we exercise the same code paths the daemon
    /// binary uses (`SlateDbMetadataIndex::insert` per hash + a fresh
    /// store + `bootstrap_from_slatedb` on the next startup) without
    /// spinning up SHM rings or a child process.
    #[test]
    fn daemon_slatedb_writethrough_survives_restart() {
        use wombatkv_radix::{BlockMeta, MetadataIndex, SlateDbMetadataIndex};

        let dir = tempdir().expect("tempdir");
        let slatedb_root = dir.path().join("slatedb-root");
        let node_id = "node-restart";
        let ns = "tenant-a";

        // Chain of three blocks: h0 root, h1 child of h0, h2 child of h1.
        let h0 = [21u8; 32];
        let h1 = [22u8; 32];
        let h2 = [23u8; 32];

        // ---- "First daemon process" ----
        {
            let store = build_store(dir.path().join("kv-store-1"));
            let slatedb_index = SlateDbMetadataIndex::open_local(&slatedb_root, node_id, ns)
                .expect("open slatedb #1");

            // Optional but recommended: bootstrap on first run too, must
            // be empty.
            let n0 = store.bootstrap_from_slatedb(&slatedb_index).expect("bootstrap #1");
            assert_eq!(n0, 0);

            // Mirrors `dispatch_put_kv_blocks_batch`: insert each hash
            // into BOTH the in-memory index AND the SlateDB index.
            let ram = store.metadata_index();
            let m0 = BlockMeta::new_root(1024, [0u8; 24], [0u8; 16]);
            let m1 = BlockMeta::new_successor(h0, 1, 1024, [0u8; 24], [0u8; 16]);
            let m2 = BlockMeta::new_successor(h1, 2, 1024, [0u8; 24], [0u8; 16]);
            ram.insert(h0, m0);
            slatedb_index.insert(h0, m0);
            ram.insert(h1, m1);
            slatedb_index.insert(h1, m1);
            ram.insert(h2, m2);
            slatedb_index.insert(h2, m2);

            // Pre-restart sanity: chain reports as fully present.
            assert_eq!(ram.longest_prefix(&[h0, h1, h2]), 3);
            assert_eq!(slatedb_index.len(), 3);

            // Close SlateDB cleanly so the WAL is flushed to disk.
            // This is what the daemon's Drop path SHOULD do at shutdown;
            // here we do it explicitly to model the next-process read.
            slatedb_index.close().expect("close slatedb #1");
            // store is dropped at end of scope, its in-memory index is
            // lost. This is the bug the persistence fix solves.
        }

        // ---- "Second daemon process" (post-restart) ----
        let store = build_store(dir.path().join("kv-store-2"));
        let slatedb_index = SlateDbMetadataIndex::open_local(&slatedb_root, node_id, ns)
            .expect("reopen slatedb #2");

        // The bootstrap_from_slatedb hydrate is what restores the
        // metadata that the prior `put_kv_blocks` write-through wrote.
        let n = store.bootstrap_from_slatedb(&slatedb_index).expect("bootstrap #2");
        assert_eq!(n, 3, "second-process bootstrap must see the 3 prior writes");

        // Without the write-through fix this would be 0, the prior
        // process's in-memory index dropped at exit. With the fix the
        // RAM index is rehydrated from SlateDB and the chain is intact.
        let ram = store.metadata_index();
        assert_eq!(ram.longest_prefix(&[h0, h1, h2]), 3);
        assert_eq!(ram.longest_prefix(&[h0]), 1);
        // Sequence / parent wiring also survives.
        let got2 = ram.get(&h2).expect("h2 must be present");
        assert_eq!(got2.block_seq, 2);
        assert_eq!(got2.parent_hash, h1);
    }

    /// Regression: a Tier-B bucket with >1000 keys (the legacy S3 page
    /// size) must be indexed in full. Prior to the explicit "consume the
    /// paginated list to completion" contract, a single-page list call
    /// would silently drop everything beyond the first 1000 keys. We
    /// don't have a paginated `InMemoryObjectStore`, instead we rely on
    /// `S3ObjectStore::list_prefix` to exhaust pages via `rust-s3`'s
    /// `Bucket::list` (returning a `Vec<ListBucketResult>` of every
    /// page) and here we assert the embed-side bootstrap iterates EVERY
    /// returned key, so the call contract bottles up the entire prefix.
    #[test]
    fn bootstrap_world_knowledge_indexes_all_tier_b_keys_above_page_size() {
        use wombatkv_radix::MetadataIndex;
        let dir = tempdir().expect("tempdir");
        let store = build_store(dir.path().to_path_buf());
        // 2000 distinct block keys, twice the legacy S3 page
        // size. Each key is `test/kv/ns-pagetest/wombatkv/v1/block/b3=<hex>`.
        let object_store = store.object_store();
        let mut expected_hashes: Vec<[u8; 32]> = Vec::with_capacity(2000);
        for i in 0..2000u32 {
            let mut h = [0u8; 32];
            // Deterministic but non-trivial spread so insert() doesn't
            // see collisions on a tiny prefix.
            let bytes = i.to_be_bytes();
            h[0..4].copy_from_slice(&bytes);
            h[28..32].copy_from_slice(&bytes);
            expected_hashes.push(h);
            let hex = hex32_lower(&h);
            let key = format!("test/kv/ns-pagetest/wombatkv/v1/block/b3={hex}");
            object_store.put_object(&key, &[0u8; 4]).expect("put");
        }
        let loaded = store.bootstrap_world_knowledge("ns-pagetest").expect("bootstrap");
        assert_eq!(
            loaded, 2000,
            "bootstrap must index every block key returned by list_prefix, \
             not just the first page"
        );
        let idx = store.metadata_index();
        assert_eq!(idx.len(), 2000);
        // Spot-check a few hashes from different positions of the input.
        for probe in [0, 999, 1000, 1500, 1999] {
            assert!(
                idx.get(&expected_hashes[probe]).is_some(),
                "hash at offset {probe} missing from metadata index"
            );
        }
    }

    fn hex32_lower(h: &[u8; 32]) -> String {
        let mut s = String::with_capacity(64);
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for byte in h {
            s.push(HEX[(byte >> 4) as usize] as char);
            s.push(HEX[(byte & 0x0f) as usize] as char);
        }
        s
    }

    #[test]
    fn bootstrap_from_slatedb_idempotent() {
        use wombatkv_radix::{BlockMeta, MetadataIndex, SlateDbMetadataIndex};

        let dir = tempdir().expect("tempdir");
        let slatedb_root = dir.path().join("slatedb-root");
        let store = build_store(dir.path().join("kv-store"));

        let slatedb_index =
            SlateDbMetadataIndex::open_local(&slatedb_root, "node-bootstrap-idem", "tenant-a")
                .expect("open slatedb");

        for i in 0..5u8 {
            slatedb_index.insert([i + 1; 32], mk_meta_for_test(u32::from(i), BlockMeta::ZERO_HASH));
        }

        let first = store.bootstrap_from_slatedb(&slatedb_index).expect("first bootstrap");
        assert_eq!(first, 5);

        // Second call returns the same SlateDB row count. `bulk_load` skips
        // already-present hashes, so the RAM index does not grow.
        let second = store.bootstrap_from_slatedb(&slatedb_index).expect("second bootstrap");
        assert_eq!(second, first);
        assert_eq!(store.metadata_index().len(), 5);
    }
}
