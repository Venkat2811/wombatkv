#![forbid(unsafe_code)]
//! Per-namespace LRU eviction worker (RFC 0009).
//!
//! Production-safety story: `WombatKV`'s block storage in S3 grows
//! unboundedly without a budget cap. This module periodically scans
//! the in-memory `MetadataIndex`, sums `payload_bytes` per namespace,
//! and when the sum exceeds the configured byte budget, evicts the
//! oldest entries (by `last_access_ns`) until the namespace fits
//! inside the budget with a 10% headroom.
//!
//! ## Env vars
//!
//! - `WMBT_KV_NAMESPACE_MAX_BYTES=<N>`, per-namespace byte budget.
//!   `0` (default) disables eviction entirely (safe default; existing
//!   deployments see no behavior change). `100 GB` is the suggested
//!   production setting; a single ds4 model footprint fits in well
//!   under that.
//! - `WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS=<N>`, cycle interval, default 30 s.
//!
//! ## Race safety
//!
//! The worker uses a compare-and-delete pattern against
//! `InMemoryMetadataIndex::remove_if_unchanged`: each eviction
//! candidate carries the `last_access_ns` snapshot taken at scoring
//! time. If a concurrent `get_and_touch` raced the worker between
//! snapshot and delete, the CAS fails and the worker silently skips
//! that entry this cycle (revisits next pass). This avoids holding a
//! cross-namespace `tokio::sync::Mutex` on the hot path: the eviction
//! cost is paid only by the worker thread, the request path pays
//! one extra `BlockMeta` comparison.
//!
//! The CAS-failure path is observable in the per-cycle event as
//! `skipped_changed`. A persistently high count indicates either (a)
//! the namespace is genuinely hotter than the budget allows (raise
//! the budget) or (b) the eviction interval is too long and many
//! blocks are getting touched between scan and delete (shrink the
//! interval).
//!
//! ## What gets deleted, by tier
//!
//! Per evicted entry the worker:
//! 1. Removes from `InMemoryMetadataIndex` (CAS as above).
//! 2. Removes from `SlateDbMetadataIndex` (if opened by the caller).
//!    Best-effort: a `SlateDB` delete failure logs+continues; the L0
//!    state is already consistent.
//! 3. Calls `EvictionDeleter::delete_block(namespace, key)` which
//!    routes through `WombatKVKvStore::delete_kv`: the object store
//!    delete + flat-tier unlink. Foyer is intentionally left to age
//!    out naturally (`foyer::HybridCache` does not expose a public
//!    single-key remove in our pinned version; the metadata index is
//!    the authoritative budget so this is correctness-safe).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wombatkv_radix::{BlockHash, BlockMeta, InMemoryMetadataIndex, SlateDbMetadataIndex};

/// Headroom fraction kept below the budget after a cycle: an
/// over-budget namespace shrinks to `budget * (1 - HEADROOM_FRAC)`,
/// not exactly to `budget`, so the worker does not have to run every
/// cycle on a steady-state workload near the cap.
const HEADROOM_FRAC: f64 = 0.10;

/// Tuning knobs for the LRU eviction worker.
#[derive(Clone, Debug)]
pub struct LruConfig {
    /// Per-namespace byte budget. `0` disables the worker entirely.
    pub namespace_max_bytes: u64,
    /// Sleep between scoring cycles.
    pub interval: Duration,
    /// Namespace the worker scans. (Each handle owns one namespace
    /// today; if `WombatKV` later supports multi-namespace handles, this
    /// can fan out per handle.)
    pub namespace: String,
}

impl Default for LruConfig {
    fn default() -> Self {
        Self { namespace_max_bytes: 0, interval: Duration::from_secs(30), namespace: String::new() }
    }
}

impl LruConfig {
    /// Read the env knobs and build a config. Returns `None` when
    /// `WMBT_KV_NAMESPACE_MAX_BYTES` is absent, zero, or unparseable -
    /// that is the off-by-default safe state.
    #[must_use]
    pub fn from_env(namespace: impl Into<String>) -> Option<Self> {
        let max_bytes: u64 = std::env::var("WMBT_KV_NAMESPACE_MAX_BYTES").ok()?.parse().ok()?;
        if max_bytes == 0 {
            return None;
        }
        let interval_secs: u64 = std::env::var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        Some(Self {
            namespace_max_bytes: max_bytes,
            interval: Duration::from_secs(interval_secs.max(1)),
            namespace: namespace.into(),
        })
    }
}

/// Per-cycle outcome counts. Surfaced via the `[MyelonInstr]` event
/// emitted by [`default_emit`].
#[derive(Clone, Debug, Default)]
pub struct EvictionCycleOutcome {
    pub scanned: usize,
    pub total_bytes_before: u64,
    pub over_budget: bool,
    pub blocks_freed: usize,
    pub bytes_freed: u64,
    pub skipped_changed: usize,
    pub delete_failures: usize,
    pub cycle_ms: u128,
}

/// Sync deleter surface implemented by `WombatKVKvStore<S>` so the
/// algorithm crate can call delete without depending on the embed
/// crate's generic `S: ObjectStore` shape. Mirrors the
/// `PrefetchFetcher` pattern in `block_prefetch.rs`.
pub trait EvictionDeleter: Send + Sync {
    /// Delete one block by `(namespace, key)`. Returns true if the
    /// object store reported a delete; false on miss (already gone).
    /// Errors propagate as a string for logging, the worker logs and
    /// continues; one bad delete does not stop the cycle.
    fn delete_block(&self, namespace: &str, key: &str) -> Result<bool, String>;

    /// Resolve a `BlockHash` to the canonical object-store key. The
    /// worker uses this to build the delete call; mirrors
    /// `wombatkv_node::block_prefetch::block_key_for_hash` so both
    /// modules agree on the path layout.
    fn block_key_for_hash(&self, hash: &BlockHash) -> String {
        crate::block_prefetch::block_key_for_hash(hash)
    }
}

/// Owns a background thread that runs the eviction cycle.
///
/// Dropping the worker signals stop and joins the thread. The join
/// runs in `Drop`, so the worker is guaranteed not to outlive its
/// owner.
pub struct LruEvictionWorker {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl LruEvictionWorker {
    /// Request shutdown without joining. Calling `drop` afterwards
    /// will still join.
    pub fn signal_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Returns true while the worker thread has not yet exited.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

impl Drop for LruEvictionWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Closure-based callback for the per-cycle outcome event.
pub type EvictionEmit = Arc<dyn Fn(&EvictionCycleOutcome) + Send + Sync>;

/// Default emit: a `[MyelonInstr]` JSON line on stderr per cycle.
#[must_use]
pub fn default_emit(namespace: String) -> EvictionEmit {
    Arc::new(move |o: &EvictionCycleOutcome| {
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_eviction\",\"fn\":\"eviction_cycle\",\
             \"namespace\":\"{}\",\"stages\":{{\"scanned\":{},\"total_bytes_before\":{},\
             \"over_budget\":{},\"blocks_freed\":{},\"bytes_freed\":{},\
             \"skipped_changed\":{},\"delete_failures\":{},\"cycle_ms\":{}}}}}",
            namespace,
            o.scanned,
            o.total_bytes_before,
            o.over_budget,
            o.blocks_freed,
            o.bytes_freed,
            o.skipped_changed,
            o.delete_failures,
            o.cycle_ms,
        );
    })
}

/// Spawn the eviction worker. The returned worker holds the join
/// handle and signals stop on drop. Returns immediately; the loop
/// runs on the spawned thread.
pub fn spawn_worker(
    index: Arc<InMemoryMetadataIndex>,
    slatedb: Option<Arc<SlateDbMetadataIndex>>,
    deleter: Arc<dyn EvictionDeleter>,
    config: LruConfig,
    emit: EvictionEmit,
) -> LruEvictionWorker {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let handle = std::thread::Builder::new()
        .name("wombatkv-lru-evict".to_string())
        .spawn(move || {
            // Bounded sleep loop: react to stop within ≤ slice (50 ms).
            let slice = Duration::from_millis(50);
            loop {
                if stop_for_thread.load(Ordering::SeqCst) {
                    break;
                }
                let outcome =
                    run_cycle(index.as_ref(), slatedb.as_deref(), deleter.as_ref(), &config);
                emit(&outcome);

                let mut remaining = config.interval;
                while remaining > Duration::ZERO {
                    if stop_for_thread.load(Ordering::SeqCst) {
                        break;
                    }
                    let s = remaining.min(slice);
                    std::thread::sleep(s);
                    remaining = remaining.saturating_sub(s);
                }
            }
        })
        .expect("spawn lru worker");

    LruEvictionWorker { handle: Some(handle), stop }
}

/// Run one eviction cycle synchronously. Exposed for unit tests so
/// the cycle can run inline (no thread orchestration).
///
/// Algorithm:
/// 1. Snapshot `index.entries()` and sum `payload_bytes`.
/// 2. If sum ≤ budget, no-op. Emit "no-op" outcome and return.
/// 3. Otherwise sort the snapshot ascending by `last_access_ns`
///    (oldest first).
/// 4. Walk the sorted list, calling `remove_if_unchanged` + delete
///    until `sum_remaining ≤ budget * (1 - HEADROOM_FRAC)`.
///
/// Step 3 builds a private `Vec<(hash, meta)>` copy; the index's
/// internal lock is released as soon as `entries()` returns.
#[must_use]
pub fn run_cycle(
    index: &InMemoryMetadataIndex,
    slatedb: Option<&SlateDbMetadataIndex>,
    deleter: &dyn EvictionDeleter,
    config: &LruConfig,
) -> EvictionCycleOutcome {
    use wombatkv_radix::MetadataIndex;
    let started = Instant::now();
    let snapshot: Vec<(BlockHash, BlockMeta)> = index.entries();
    let scanned = snapshot.len();
    let total_bytes_before: u64 = snapshot.iter().map(|(_, m)| m.payload_bytes).sum();
    let budget = config.namespace_max_bytes;

    if budget == 0 || total_bytes_before <= budget {
        return EvictionCycleOutcome {
            scanned,
            total_bytes_before,
            over_budget: false,
            blocks_freed: 0,
            bytes_freed: 0,
            skipped_changed: 0,
            delete_failures: 0,
            cycle_ms: started.elapsed().as_millis(),
        };
    }

    // Sort by last_access_ns ascending, oldest first.
    let mut sorted = snapshot;
    sorted.sort_by_key(|a| a.1.last_access_ns);

    // Target: leave HEADROOM_FRAC empty at the top of the budget so
    // we don't run the cycle every interval on a steady-state workload.
    let target = (budget as f64 * (1.0 - HEADROOM_FRAC)) as u64;
    let need_to_free = total_bytes_before.saturating_sub(target);

    let mut bytes_freed = 0_u64;
    let mut blocks_freed = 0_usize;
    let mut skipped_changed = 0_usize;
    let mut delete_failures = 0_usize;

    for (hash, meta) in sorted {
        if bytes_freed >= need_to_free {
            break;
        }
        // CAS on the L0 index. If the stamp drifted, a get_and_touch
        // raced us, skip this block and try the next one.
        if !index.remove_if_unchanged(&hash, meta.last_access_ns) {
            skipped_changed += 1;
            continue;
        }
        // L1 SlateDB remove (best-effort). If this fails, the L0 is
        // already consistent; the next bootstrap_from_slatedb might
        // re-introduce the entry, but the next eviction cycle will
        // re-evict it.
        if let Some(idx) = slatedb {
            let _ = MetadataIndex::remove(idx, &hash);
        }
        // Delete from object store + flat tier.
        let key = deleter.block_key_for_hash(&hash);
        match deleter.delete_block(&config.namespace, &key) {
            Ok(_) => {
                bytes_freed = bytes_freed.saturating_add(meta.payload_bytes);
                blocks_freed += 1;
            }
            Err(err) => {
                eprintln!("wombatkv[lru]: delete_block({key}) failed: {err}");
                delete_failures += 1;
                // Even on delete failure, we still count the block as
                // freed from the budget, the metadata-index removal
                // has already happened. The S3 object is then an
                // orphan: a future GC pass can clean it up. Acceptable
                // for production: budget integrity matters more than
                // a small number of orphaned objects.
                bytes_freed = bytes_freed.saturating_add(meta.payload_bytes);
                blocks_freed += 1;
            }
        }
    }

    EvictionCycleOutcome {
        scanned,
        total_bytes_before,
        over_budget: true,
        blocks_freed,
        bytes_freed,
        skipped_changed,
        delete_failures,
        cycle_ms: started.elapsed().as_millis(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use wombatkv_radix::{BlockMeta, MetadataIndex};

    /// Capture-only deleter for tests. Records (namespace, key) calls
    /// and reports success.
    #[derive(Default)]
    struct CapturingDeleter {
        deleted: Mutex<Vec<(String, String)>>,
    }

    impl EvictionDeleter for CapturingDeleter {
        fn delete_block(&self, namespace: &str, key: &str) -> Result<bool, String> {
            self.deleted.lock().unwrap().push((namespace.to_string(), key.to_string()));
            Ok(true)
        }
    }

    fn mk_block(seq: u32, payload_bytes: u64, age_ns_offset: u64) -> ([u8; 32], BlockMeta) {
        let mut hash = [0u8; 32];
        // Encode seq into the hash so each block has a unique key.
        hash[..4].copy_from_slice(&seq.to_le_bytes());
        let mut meta = BlockMeta::new_root(payload_bytes, [0u8; 24], *b"test-v1\0\0\0\0\0\0\0\0\0");
        // Force a deterministic last_access_ns: smaller seq = older.
        // (We can't override the constructor's `now_ns()` from outside,
        // so set the field directly through a mutable copy.)
        meta.last_access_ns = 1_000_000_000_u64 + age_ns_offset;
        meta.block_seq = seq;
        (hash, meta)
    }

    #[test]
    fn evicts_oldest_when_over_budget() {
        // Seed 100 blocks of 1024 bytes each → 102_400 bytes total.
        // Budget = 50 blocks worth = 51_200 bytes. With 10% headroom
        // the target is 46_080 bytes, so the worker must evict at
        // least (102_400 - 46_080) / 1024 = 55 blocks.
        //
        // `bulk_load` preserves the `last_access_ns` from `mk_block`
        // (it uses `entry().or_insert(m)` without calling `.touch()`),
        // which is what we need for a deterministic LRU test.
        let index = Arc::new(InMemoryMetadataIndex::new());
        let mut seeds = Vec::with_capacity(100);
        for i in 0..100u32 {
            seeds.push(mk_block(i, 1024, u64::from(i) * 1_000_000));
        }
        index.bulk_load(seeds);
        assert_eq!(index.len(), 100);

        let deleter: Arc<dyn EvictionDeleter> = Arc::new(CapturingDeleter::default());
        let config = LruConfig {
            namespace_max_bytes: 50 * 1024,
            interval: Duration::from_secs(30),
            namespace: "test-ns".to_string(),
        };

        let outcome = run_cycle(index.as_ref(), None, deleter.as_ref(), &config);

        assert_eq!(outcome.scanned, 100);
        assert_eq!(outcome.total_bytes_before, 100 * 1024);
        assert!(outcome.over_budget);
        // budget=51_200, target=46_080, need_to_free=56_320, blocks=55.
        assert_eq!(outcome.blocks_freed, 55);
        assert_eq!(outcome.bytes_freed, 55 * 1024);
        assert_eq!(outcome.skipped_changed, 0);
        assert_eq!(outcome.delete_failures, 0);

        // The 55 oldest blocks (seq 0..55) should be gone; 55..100 remain.
        assert_eq!(index.len(), 45);
        for i in 0..55u32 {
            let (h, _) = mk_block(i, 0, 0);
            assert!(index.get(&h).is_none(), "expected seq={i} (oldest) to be evicted");
        }
        for i in 55..100u32 {
            let (h, _) = mk_block(i, 0, 0);
            assert!(index.get(&h).is_some(), "expected seq={i} (newest) to be retained");
        }
    }

    #[test]
    fn no_op_when_under_budget() {
        let index = Arc::new(InMemoryMetadataIndex::new());
        let mut seeds = Vec::with_capacity(10);
        for i in 0..10u32 {
            seeds.push(mk_block(i, 1024, u64::from(i) * 1_000_000));
        }
        index.bulk_load(seeds);

        let deleter: Arc<dyn EvictionDeleter> = Arc::new(CapturingDeleter::default());
        let config = LruConfig {
            namespace_max_bytes: 100 * 1024, // way over what we have
            interval: Duration::from_secs(30),
            namespace: "test-ns".to_string(),
        };

        let outcome = run_cycle(index.as_ref(), None, deleter.as_ref(), &config);

        assert_eq!(outcome.scanned, 10);
        assert_eq!(outcome.total_bytes_before, 10 * 1024);
        assert!(!outcome.over_budget);
        assert_eq!(outcome.blocks_freed, 0);
        assert_eq!(outcome.bytes_freed, 0);
        assert_eq!(index.len(), 10);
    }

    #[test]
    fn cas_skips_concurrently_touched_block() {
        // Pre-seed 3 blocks; "touch" the oldest between snapshot and
        // cycle by mutating its stamp directly via re-insert. (In the
        // real path this is what `get_and_touch` does.)
        let index = Arc::new(InMemoryMetadataIndex::new());
        let seeds =
            vec![mk_block(0, 1024, 0), mk_block(1, 1024, 1_000_000), mk_block(2, 1024, 2_000_000)];
        index.bulk_load(seeds.clone());

        // We want to demonstrate that if we hold a stale snapshot and
        // then "race" a touch, the CAS rejects the eviction. Simulate
        // by manually calling remove_if_unchanged with a stale stamp.
        let (h0, m0) = seeds[0];
        // Pretend the stamp drifted: caller passes the old stamp but
        // the actual entry was touched (we trigger this by inserting
        // a copy with a different last_access_ns).
        index.insert(h0, BlockMeta::new_root(1024, [0; 24], *b"test-v1\0\0\0\0\0\0\0\0\0"));
        // The actual stored stamp is now "now_ns" (set by insert+touch),
        // which differs from the stale m0.last_access_ns we hold.
        assert!(!index.remove_if_unchanged(&h0, m0.last_access_ns));
        assert!(index.get(&h0).is_some());

        // Now force eviction with a tiny budget. The worker will see
        // h0 with its new (fresh) stamp; h1 and h2 are the oldest now
        // (their stamps are still 1e6 and 2e6).
        let deleter: Arc<dyn EvictionDeleter> = Arc::new(CapturingDeleter::default());
        let config = LruConfig {
            namespace_max_bytes: 1024, // only room for one block
            interval: Duration::from_secs(30),
            namespace: "test-ns".to_string(),
        };
        let outcome = run_cycle(index.as_ref(), None, deleter.as_ref(), &config);

        // budget=1024, target=921, total=3072, need=2151 → 3 blocks.
        // But CAS for h0 sees the fresh stamp (no race in this
        // synchronous-only run), so all 3 are evicted in order.
        assert!(outcome.over_budget);
        assert_eq!(outcome.blocks_freed, 3);
        assert_eq!(outcome.skipped_changed, 0);
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn concurrent_put_and_evict_does_not_crash() {
        // Best-effort race test: spawn a producer thread that
        // continuously inserts blocks, then run one eviction cycle
        // from the main thread. We verify only that the cycle returns
        // without panicking AND the index converges to ≤ budget within
        // a few cycles. (A precise count is intentionally not
        // asserted, the CAS path will skip blocks whose stamps drift
        // mid-cycle, and the producer is racing.)
        use std::sync::atomic::{AtomicBool, Ordering};

        let index = Arc::new(InMemoryMetadataIndex::new());
        let deleter: Arc<dyn EvictionDeleter> = Arc::new(CapturingDeleter::default());
        let config = LruConfig {
            namespace_max_bytes: 10 * 1024, // 10 blocks worth
            interval: Duration::from_secs(30),
            namespace: "test-ns".to_string(),
        };

        // Seed: 200 blocks → well over the 10-block budget.
        let mut seeds = Vec::with_capacity(200);
        for i in 0..200u32 {
            seeds.push(mk_block(i, 1024, u64::from(i) * 1_000_000));
        }
        index.bulk_load(seeds);

        // Producer: insert NEW (fresh) blocks while the evictor runs.
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_producer = stop.clone();
        let index_for_producer = index.clone();
        let producer = std::thread::spawn(move || {
            let mut next_seq = 1_000_u32;
            while !stop_for_producer.load(Ordering::SeqCst) {
                let (h, m) = mk_block(next_seq, 1024, u64::MAX / 2);
                index_for_producer.insert(h, m);
                next_seq += 1;
            }
        });

        // Run a few cycles back-to-back so we exercise the CAS skip
        // path with a high probability of mid-cycle insertions.
        for _ in 0..3 {
            let _ = run_cycle(index.as_ref(), None, deleter.as_ref(), &config);
        }

        stop.store(true, Ordering::SeqCst);
        producer.join().expect("producer thread");

        // One final cycle with the producer halted; the index must
        // now satisfy the budget (no live races left).
        let final_outcome = run_cycle(index.as_ref(), None, deleter.as_ref(), &config);

        let final_bytes: u64 = index.entries().iter().map(|(_, m)| m.payload_bytes).sum();
        assert!(
            final_bytes <= config.namespace_max_bytes,
            "post-eviction bytes {final_bytes} > budget {} (outcome={final_outcome:?})",
            config.namespace_max_bytes
        );
    }

    #[test]
    fn from_env_returns_none_when_unset() {
        // Snapshot + restore the env vars so tests don't pollute the
        // process state.
        let saved_max = std::env::var("WMBT_KV_NAMESPACE_MAX_BYTES").ok();
        let saved_int = std::env::var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS").ok();
        std::env::remove_var("WMBT_KV_NAMESPACE_MAX_BYTES");
        std::env::remove_var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS");
        assert!(LruConfig::from_env("any").is_none());

        std::env::set_var("WMBT_KV_NAMESPACE_MAX_BYTES", "0");
        assert!(LruConfig::from_env("any").is_none());

        std::env::set_var("WMBT_KV_NAMESPACE_MAX_BYTES", "1048576");
        std::env::set_var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS", "5");
        let cfg = LruConfig::from_env("ns-a").expect("config");
        assert_eq!(cfg.namespace_max_bytes, 1_048_576);
        assert_eq!(cfg.interval, Duration::from_secs(5));
        assert_eq!(cfg.namespace, "ns-a");

        // Restore.
        match saved_max {
            Some(v) => std::env::set_var("WMBT_KV_NAMESPACE_MAX_BYTES", v),
            None => std::env::remove_var("WMBT_KV_NAMESPACE_MAX_BYTES"),
        }
        match saved_int {
            Some(v) => std::env::set_var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS", v),
            None => std::env::remove_var("WMBT_KV_DAEMON_EVICTION_INTERVAL_SECS"),
        }
    }
}
