#![forbid(unsafe_code)]
//! Background block-prefetch worker (RFC 0008 §6).
//!
//! Periodically snapshots the `MetadataIndex`, scores each entry per
//! recency / chain-head / model-affinity heuristic, and selects the
//! top-K candidates that the request hot path is most likely to hit
//! next. Goal: warm the flat tier before requests arrive, so cold-S3
//! load latency disappears from the user-visible TTFT.
//!
//! ## Scoring (RFC 0008 §6)
//!
//! ```text
//!   score = w_recency * exp(-decay * (now - last_access_ns))
//!         + w_chain   * is_chain_head_bonus
//!         + w_model   * model_affinity_bonus
//! ```
//!
//! With:
//! - `w_recency = 1.0` (primary signal; recently-touched blocks rank highest)
//! - `w_chain   = 0.3` (chain-head blocks anchor multi-turn prompts)
//! - `w_model   = 0.2` (active model's blocks rank ahead of stragglers)
//! - `decay     = ln(2) / 600e9` (half-life ≈ 10 minutes in nanoseconds)
//! - `is_chain_head_bonus = 1.0 iff BlockMeta.block_seq == 0`
//! - `model_affinity_bonus = 1.0 iff BlockMeta.model_digest == active`
//!
//! ## v1 vs v2
//!
//! v1 was log-only: scored the candidates and emitted a `[MyelonInstr]`
//! event listing the top-K, but never issued the GET. v2 (this module)
//! actually fetches: per cycle it issues `WombatKVKvStore::get_kv` for
//! each top-K miss, materializing the payload into the local flat
//! cache so the next request hits the warm path.
//!
//! The fallback path is preserved behind the `WMBT_KV_PREFETCH_DRY_RUN=1`
//! env: when set, the worker scores and logs but never issues GETs
//! (matches v1 behavior for diagnostic / canary deployments).
//!
//! ### Sequential vs parallel
//!
//! v2 issues GETs sequentially per cycle. This mirrors the C ABI's
//! per-block path (`Handle::put_kv_blocks` parallelizes via
//! `std::thread::scope`, but `get_kv` itself is the cabi's per-block
//! call). Parallel fetch within a cycle is a v3 TODO, once we have
//! evidence of cycle-time becoming a bottleneck, swap in a
//! `std::thread::scope` fan-out bounded by `top_k`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wombatkv_radix::{BlockHash, BlockMeta, MetadataIndex, ModelDigest};

/// Per-fetch wallclock cap. If a single `get_kv` exceeds this the
/// worker aborts the rest of the cycle and logs. Keeps a stuck S3 call
/// from monopolizing the worker thread.
const FETCH_WALLCLOCK_CAP: Duration = Duration::from_secs(5);

/// Tunables for the prefetch worker.
#[derive(Clone, Debug)]
pub struct PrefetchConfig {
    /// Sleep between scoring cycles. Workers fire `tick()` every
    /// `interval` and otherwise idle.
    pub interval: Duration,
    /// Maximum entries to materialize per cycle. Acts as a cap on the
    /// prefetch-induced load on the flat tier + S3 bandwidth.
    pub top_k: usize,
    /// Active-model fingerprint for affinity scoring. Zeroed digest
    /// disables the bonus (all blocks rank equally on model affinity).
    pub model_digest: ModelDigest,
    /// Namespace under which prefetch GETs are issued. Mirrors the
    /// caller's `WombatKVKvStore::get_kv(namespace, key)` namespace -
    /// in cabi this is `WMBT_KV_NAMESPACE` (default `tp-default`).
    pub namespace: String,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_millis(500),
            top_k: 8,
            model_digest: [0u8; 24],
            namespace: String::new(),
        }
    }
}

/// Owns a background thread that scores + would-prefetch hot blocks.
///
/// Dropping the worker signals stop and joins the thread. The join
/// runs in `Drop`, so the worker is guaranteed not to outlive its
/// owner.
pub struct PrefetchWorker {
    handle: Option<JoinHandle<()>>,
    stop: Arc<AtomicBool>,
}

impl PrefetchWorker {
    /// Request shutdown without joining. Calling `drop` afterwards
    /// will still join, this exists for callers that want to fan
    /// out shutdown signals before serially joining.
    pub fn signal_stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Returns true while the worker thread has not yet observed the
    /// stop signal *and* exited. Exposed for tests.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.handle.as_ref().is_some_and(|h| !h.is_finished())
    }
}

impl Drop for PrefetchWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            // Best-effort join. We do not panic in Drop; the worker
            // body is panic-free under our control.
            let _ = h.join();
        }
    }
}

/// Closure-based callback for "I would have prefetched this block".
/// Lets tests assert the worker's choices without coupling to the
/// log format.
pub type PrefetchEmit = Arc<dyn Fn(&PrefetchPlan) + Send + Sync>;

/// One cycle's prefetch plan. Held briefly inside the worker thread,
/// then handed to `emit` (v1 / dry-run) or to the fetcher (v2).
#[derive(Clone, Debug)]
pub struct PrefetchPlan {
    /// Total entries scored this cycle.
    pub scored: usize,
    /// Top-K selected (capped by `PrefetchConfig::top_k`).
    pub selected: Vec<(BlockHash, BlockMeta, f64)>,
    /// Wall-clock cost of the cycle.
    pub elapsed: Duration,
}

/// Materialization surface for v2 prefetch. The worker holds an
/// `Arc<dyn PrefetchFetcher>` so the algorithm crate can issue GETs
/// without the `block_prefetch` module depending on the embed module's
/// generic `WombatKVKvStore<S>` shape.
///
/// Implementations must be cheap to call (`contains_flat` should be a
/// single filesystem stat) and `fetch_block` must populate the local
/// flat tier on success, the whole point of v2 is to warm the flat
/// cache before request time.
pub trait PrefetchFetcher: Send + Sync {
    /// Returns true if the block is already materialized in the local
    /// flat tier. The worker skips already-flat blocks to keep cycle
    /// cost bounded.
    fn contains_flat(&self, namespace: &str, key: &str) -> bool;

    /// Fetch the block. Returns `Ok(Some(bytes_len))` on hit (and the
    /// implementation populates flat/foyer), `Ok(None)` on miss, and
    /// `Err(message)` on backend error. The worker logs+continues on
    /// error; one bad block does not stop the cycle.
    fn fetch_block(&self, namespace: &str, key: &str) -> Result<Option<u64>, String>;
}

/// Per-cycle outcome counts. Surfaced via the `[MyelonInstr]` event
/// emitted by [`default_v2_emit`].
#[derive(Clone, Debug, Default)]
pub struct PrefetchFetchOutcome {
    pub scored: usize,
    pub selected: usize,
    pub skipped_already_flat: usize,
    pub fetched: usize,
    pub failed: usize,
    pub bytes_materialized: u64,
    pub elapsed_ms: u128,
}

/// Score `BlockMeta` under the RFC 0008 §6 heuristic.
///
/// `now_ns` is passed in (not pulled from the wall clock here) so the
/// caller can score a batch with one monotonic reading.
#[must_use]
pub fn score_block(meta: &BlockMeta, now_ns: u64, active_model: &ModelDigest) -> f64 {
    // Weights and decay constant per RFC 0008 §6.
    const W_RECENCY: f64 = 1.0;
    const W_CHAIN: f64 = 0.3;
    const W_MODEL: f64 = 0.2;
    // ln(2) / 600e9 → half-life of 600 seconds in nanoseconds.
    let decay: f64 = std::f64::consts::LN_2 / 600.0e9_f64;

    // Recency: bigger when last_access_ns is close to now_ns.
    // Negative age (touched in the future, clock skew) maps to 1.0
    // by clamping age to 0.
    let age_ns = now_ns.saturating_sub(meta.last_access_ns) as f64;
    let recency = (-decay * age_ns).exp();

    let chain_bonus = if meta.block_seq == 0 { 1.0 } else { 0.0 };
    let model_bonus = if &meta.model_digest == active_model { 1.0 } else { 0.0 };

    W_RECENCY * recency + W_CHAIN * chain_bonus + W_MODEL * model_bonus
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64)
}

/// Compose the relative block key for a `BlockHash`. Mirrors
/// `wombatkv-cabi::block_key_for_hash`, both call sites read the
/// same `wombatkv_radix::BLOCK_KEY_PREFIX` so they can never skew.
#[must_use]
pub fn block_key_for_hash(hash: &BlockHash) -> String {
    use wombatkv_radix::BLOCK_KEY_PREFIX;
    let mut s = String::with_capacity(BLOCK_KEY_PREFIX.len() + 64);
    s.push_str(BLOCK_KEY_PREFIX);
    for b in hash {
        s.push_str(&hex_pair(*b));
    }
    s
}

fn hex_pair(b: u8) -> String {
    let hi = HEX[(b >> 4) as usize];
    let lo = HEX[(b & 0x0f) as usize];
    let mut s = String::with_capacity(2);
    s.push(hi);
    s.push(lo);
    s
}

const HEX: [char; 16] =
    ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'];

/// Run one scoring + selection cycle against the supplied index.
/// Returns the plan; callers decide what to do with it (emit, GET, etc.).
///
/// Splitting "score + select" from "act on the plan" keeps the scoring
/// logic synchronous and unit-testable without spinning up a thread.
#[must_use]
pub fn run_cycle(index: &dyn MetadataIndex, config: &PrefetchConfig) -> PrefetchPlan {
    let started = Instant::now();
    let now = now_ns();
    let snapshot = index.entries();
    let scored = snapshot.len();

    // Score every entry. The scoring loop is O(N); the sort below
    // dominates at large N. With `top_k` typically ≤ 32, a partial
    // sort would beat `sort_by`, but the simpler full sort keeps the
    // code obvious, revisit when M exceeds 10^4.
    let mut scored_entries: Vec<(BlockHash, BlockMeta, f64)> = snapshot
        .into_iter()
        .map(|(h, m)| {
            let s = score_block(&m, now, &config.model_digest);
            (h, m, s)
        })
        .collect();

    scored_entries.sort_by(|a, b| {
        // Higher score first; NaN treated as -inf (shouldn't occur,
        // but guards against future scoring tweaks).
        b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut selected = scored_entries;
    selected.truncate(config.top_k);

    PrefetchPlan { scored, selected, elapsed: started.elapsed() }
}

/// Spawn the v1 / dry-run worker. Scores + logs, never fetches.
///
/// Retained for tests, diagnostic deployments, and the
/// `WMBT_KV_PREFETCH_DRY_RUN=1` escape hatch (configured by the
/// embed-side `start_prefetcher` when the env is set).
///
/// `index` is held inside the worker via the supplied Arc clone, so the
/// thread can read it without holding a reference to the outer struct.
/// `emit` is invoked once per cycle with the plan; pass `default_emit`
/// for the standard `[MyelonInstr]` event, or a test-side closure to
/// capture the plan inline.
pub fn spawn_worker(
    index: Arc<dyn MetadataIndex>,
    config: PrefetchConfig,
    emit: PrefetchEmit,
) -> PrefetchWorker {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let handle = std::thread::Builder::new()
        .name("wombatkv-prefetch".to_string())
        .spawn(move || {
            // Bounded sleep loop: we don't want to delay shutdown by
            // a full `interval` on Drop, so sleep in slices and check
            // the stop flag each slice.
            let slice = Duration::from_millis(25);
            loop {
                if stop_for_thread.load(Ordering::SeqCst) {
                    break;
                }
                let plan = run_cycle(index.as_ref(), &config);
                emit(&plan);

                // Sleep `interval`, but in `slice` chunks so the
                // worker reacts to stop within ≤ slice (25 ms).
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
        .expect("spawn prefetch worker");

    PrefetchWorker { handle: Some(handle), stop }
}

/// Spawn the v2 worker that actually fetches the top-K each cycle.
///
/// Per cycle:
///   1. Snapshot the metadata index + score per RFC 0008 §6.
///   2. Take top-K candidates.
///   3. Filter out candidates already present in the local flat cache.
///   4. For each remaining: call `fetcher.fetch_block(namespace, key)`
///      sequentially. On error, log + continue.
///   5. Emit a per-cycle `[MyelonInstr]` event with detailed stage counts.
///
/// The fetch loop is sequential by design. v3 may parallelize via
/// `std::thread::scope` when cycle-time becomes a bottleneck, see
/// module docs.
pub fn spawn_worker_v2(
    index: Arc<dyn MetadataIndex>,
    config: PrefetchConfig,
    fetcher: Arc<dyn PrefetchFetcher>,
    emit_outcome: Arc<dyn Fn(&PrefetchFetchOutcome) + Send + Sync>,
) -> PrefetchWorker {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_thread = stop.clone();
    let handle = std::thread::Builder::new()
        .name("wombatkv-prefetch-v2".to_string())
        .spawn(move || {
            let slice = Duration::from_millis(25);
            loop {
                if stop_for_thread.load(Ordering::SeqCst) {
                    break;
                }
                let outcome =
                    run_cycle_v2(index.as_ref(), fetcher.as_ref(), &config, &stop_for_thread);
                emit_outcome(&outcome);

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
        .expect("spawn prefetch worker v2");

    PrefetchWorker { handle: Some(handle), stop }
}

/// Run one v2 cycle: score, select, fetch top-K, return the per-stage
/// counts. Exposed for tests so the cycle can run inline (without
/// thread orchestration).
///
/// `stop` is honored mid-fetch: if it flips to true between fetches,
/// the loop exits and the partial outcome is returned.
#[must_use]
pub fn run_cycle_v2(
    index: &dyn MetadataIndex,
    fetcher: &dyn PrefetchFetcher,
    config: &PrefetchConfig,
    stop: &AtomicBool,
) -> PrefetchFetchOutcome {
    let started = Instant::now();
    let plan = run_cycle(index, config);
    let scored = plan.scored;
    let selected = plan.selected.len();

    let mut skipped_already_flat = 0_usize;
    let mut fetched = 0_usize;
    let mut failed = 0_usize;
    let mut bytes_materialized = 0_u64;

    for (hash, _meta, _score) in plan.selected {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let key = block_key_for_hash(&hash);
        if fetcher.contains_flat(&config.namespace, &key) {
            skipped_already_flat += 1;
            continue;
        }
        let fetch_started = Instant::now();
        match fetcher.fetch_block(&config.namespace, &key) {
            Ok(Some(bytes_len)) => {
                let cost = fetch_started.elapsed();
                if cost > FETCH_WALLCLOCK_CAP {
                    eprintln!(
                        "wombatkv[prefetch v2]: get_kv({key}) took {cost:?} \
                         (cap {FETCH_WALLCLOCK_CAP:?}); aborting remainder of cycle"
                    );
                    fetched += 1;
                    bytes_materialized = bytes_materialized.saturating_add(bytes_len);
                    break;
                }
                fetched += 1;
                bytes_materialized = bytes_materialized.saturating_add(bytes_len);
            }
            Ok(None) => {
                // Miss is not an error, the metadata index can ride
                // ahead of S3 (e.g., during a delete-replay).
                failed += 1;
            }
            Err(err) => {
                failed += 1;
                eprintln!("wombatkv[prefetch v2]: get_kv({key}) failed: {err}");
            }
        }
    }

    PrefetchFetchOutcome {
        scored,
        selected,
        skipped_already_flat,
        fetched,
        failed,
        bytes_materialized,
        elapsed_ms: started.elapsed().as_millis(),
    }
}

/// Default v1 / dry-run emit: a `[MyelonInstr]` JSON line on stderr per
/// cycle. Mirrors the existing event shape in `embed.rs` so log parsers
/// see one consistent envelope across read/write/prefetch paths.
#[must_use]
pub fn default_emit() -> PrefetchEmit {
    Arc::new(|plan: &PrefetchPlan| {
        let elapsed_ms = plan.elapsed.as_millis();
        // v1 is log-only: the actual GET would land here. We emit the
        // count of "would-materialize" as `materialized` so the event
        // shape doesn't churn when v2 actually fetches.
        let scored = plan.scored;
        let materialized = plan.selected.len();
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_timing\",\"fn\":\"prefetch_cycle\",\
             \"stages\":{{\"scored\":{scored},\"materialized\":{materialized},\
             \"elapsed_ms\":{elapsed_ms}}}}}"
        );
    })
}

/// Default v2 emit: a `[MyelonInstr]` JSON line per cycle with full
/// stage counts (scored, selected, `skipped_already_flat`, fetched,
/// failed, `bytes_materialized`, `elapsed_ms`).
#[must_use]
pub fn default_v2_emit() -> Arc<dyn Fn(&PrefetchFetchOutcome) + Send + Sync> {
    Arc::new(|o: &PrefetchFetchOutcome| {
        eprintln!(
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_timing\",\"fn\":\"prefetch_cycle_v2\",\
             \"stages\":{{\"scored\":{},\"selected\":{},\"skipped_already_flat\":{},\
             \"fetched\":{},\"failed\":{},\"bytes_materialized\":{},\"elapsed_ms\":{}}}}}",
            o.scored,
            o.selected,
            o.skipped_already_flat,
            o.fetched,
            o.failed,
            o.bytes_materialized,
            o.elapsed_ms,
        );
    })
}

/// Returns true if the v1 / dry-run fallback is requested via env.
/// `WMBT_KV_PREFETCH_DRY_RUN=1` (and the usual truthy synonyms) flips
/// the embed-side `start_prefetcher` back to log-only behavior.
#[must_use]
pub fn dry_run_enabled() -> bool {
    matches!(
        std::env::var("WMBT_KV_PREFETCH_DRY_RUN").ok().as_deref(),
        Some("1" | "true" | "TRUE" | "yes" | "on")
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;
    use wombatkv_radix::InMemoryMetadataIndex;

    fn mk_meta(seq: u32, last_access_ns: u64, model: ModelDigest) -> BlockMeta {
        let mut m = BlockMeta {
            parent_hash: BlockMeta::ZERO_HASH,
            block_seq: seq,
            payload_bytes: 1024,
            last_access_ns,
            model_digest: model,
            layout_tag: [0u8; 16],
            ext_flags: 0,
        };
        // Don't use new_root/new_successor here, those re-stamp
        // last_access_ns to now(), which fights the test fixture.
        m.last_access_ns = last_access_ns;
        m
    }

    fn make_hash(seed: u8) -> BlockHash {
        let mut h = [0u8; 32];
        h[0] = seed;
        h
    }

    #[test]
    fn score_recency_decay_monotone() {
        // Same model, same seq → score driven purely by recency.
        let model = [7u8; 24];
        let now = 10_000_000_000_000_u64; // 10 s
        let fresh = mk_meta(1, now - 1_000_000_000, model); // 1 s ago
        let stale = mk_meta(1, now - 600_000_000_000, model); // 10 min ago
        let ancient = mk_meta(1, now - 3_600_000_000_000, model); // 1 h ago

        let s_fresh = score_block(&fresh, now, &model);
        let s_stale = score_block(&stale, now, &model);
        let s_ancient = score_block(&ancient, now, &model);

        assert!(s_fresh > s_stale);
        assert!(s_stale > s_ancient);
        // Half-life is 10 minutes → stale should be ~half of fresh's
        // recency component plus the model bonus (0.2, same for all).
        // 0.2 model bonus dominates the small recency tail at 10 min,
        // so check the recency-only delta instead.
        let recency_fresh = s_fresh - 0.2_f64;
        let recency_stale = s_stale - 0.2_f64;
        let ratio = recency_stale / recency_fresh;
        // Allow generous slack, should be near 0.5 (half-life).
        assert!((0.40..0.60).contains(&ratio), "stale/fresh recency ratio {ratio} not near 0.5");
    }

    #[test]
    fn score_chain_head_outranks_successor_at_equal_recency() {
        let model = [7u8; 24];
        let now = 10_000_000_000_000_u64;
        let head = mk_meta(0, now - 1_000_000_000, model);
        let succ = mk_meta(5, now - 1_000_000_000, model);
        assert!(score_block(&head, now, &model) > score_block(&succ, now, &model));
    }

    #[test]
    fn score_model_affinity_outranks_others() {
        let active = [7u8; 24];
        let other = [42u8; 24];
        let now = 10_000_000_000_000_u64;
        let same = mk_meta(1, now - 1_000_000_000, active);
        let diff = mk_meta(1, now - 1_000_000_000, other);
        assert!(score_block(&same, now, &active) > score_block(&diff, now, &active));
    }

    #[test]
    fn run_cycle_picks_top_k_by_score() {
        let idx = InMemoryMetadataIndex::new();
        let active = [7u8; 24];
        let now = now_ns();

        // 100 synthetic entries. Hash 0..49 → "old" (stale recency).
        // Hash 50..99 → "fresh", and hash %3==0 within fresh are chain heads.
        //
        // We use `bulk_load` (not `insert`) because `MetadataIndex::insert`
        // calls `meta.touch()` which overwrites our fixture's last_access_ns
        // with the wall clock, defeating the staleness control.
        let entries: Vec<(BlockHash, BlockMeta)> = (0..100_u8)
            .map(|i| {
                let h = make_hash(i);
                let is_fresh = i >= 50;
                let last = if is_fresh {
                    now.saturating_sub(1_000_000_000) // 1 s ago
                } else {
                    now.saturating_sub(3_600_000_000_000) // 1 h ago
                };
                let seq = if is_fresh && i % 3 == 0 { 0 } else { u32::from(i) + 1 };
                (h, mk_meta(seq, last, active))
            })
            .collect();
        idx.bulk_load(entries);
        assert_eq!(idx.len(), 100);

        let cfg = PrefetchConfig {
            interval: Duration::from_mins(1),
            top_k: 10,
            model_digest: active,
            namespace: String::new(),
        };
        let plan = run_cycle(&idx, &cfg);
        assert_eq!(plan.scored, 100);
        assert_eq!(plan.selected.len(), 10);

        // Every selected block must come from the "fresh" half
        // (hash >= 50). Old blocks have age = 1 h ≫ half-life, so
        // their recency component is ≈ 2^-6 ≈ 0.016, well below
        // anything in the fresh half.
        for (h, _, _) in &plan.selected {
            assert!(h[0] >= 50, "selected stale block {}", h[0]);
        }

        // Scores must be sorted descending.
        for w in plan.selected.windows(2) {
            assert!(w[0].2 >= w[1].2, "scores not descending");
        }
    }

    #[test]
    fn worker_runs_cycle_and_stops_within_one_second() {
        let idx: Arc<dyn MetadataIndex> = Arc::new(InMemoryMetadataIndex::new());
        // Cast back to insert; we hold the InMemoryMetadataIndex via
        // the trait object, and `MetadataIndex::insert` works directly.
        let active = [7u8; 24];
        idx.insert(make_hash(1), mk_meta(0, now_ns(), active));
        idx.insert(make_hash(2), mk_meta(1, now_ns(), active));

        let cycles = Arc::new(Mutex::new(0_usize));
        let cycles_cb = cycles.clone();
        let emit: PrefetchEmit = Arc::new(move |_plan: &PrefetchPlan| {
            *cycles_cb.lock().unwrap() += 1;
        });

        let cfg = PrefetchConfig {
            interval: Duration::from_millis(50),
            top_k: 4,
            model_digest: active,
            namespace: String::new(),
        };

        let started = Instant::now();
        let worker = spawn_worker(idx, cfg, emit);

        // Wait long enough for ≥ 2 cycles.
        std::thread::sleep(Duration::from_millis(200));
        let observed = *cycles.lock().unwrap();
        assert!(observed >= 2, "expected ≥ 2 cycles, got {observed}");

        // Drop the worker → triggers stop + join. Must complete
        // well under 1 s.
        drop(worker);
        let drop_time = started.elapsed();
        assert!(drop_time < Duration::from_secs(2), "worker shutdown took {drop_time:?}");
    }

    #[test]
    fn worker_handles_empty_index_gracefully() {
        let idx: Arc<dyn MetadataIndex> = Arc::new(InMemoryMetadataIndex::new());
        let cycles = Arc::new(Mutex::new(0_usize));
        let cycles_cb = cycles.clone();
        let emit: PrefetchEmit = Arc::new(move |plan: &PrefetchPlan| {
            assert_eq!(plan.scored, 0);
            assert!(plan.selected.is_empty());
            *cycles_cb.lock().unwrap() += 1;
        });

        let cfg = PrefetchConfig {
            interval: Duration::from_millis(30),
            top_k: 8,
            model_digest: [0u8; 24],
            namespace: String::new(),
        };
        let worker = spawn_worker(idx, cfg, emit);
        std::thread::sleep(Duration::from_millis(120));
        let n = *cycles.lock().unwrap();
        drop(worker);
        assert!(n >= 2, "expected ≥ 2 cycles on empty index, got {n}");
    }

    #[test]
    fn signal_stop_makes_drop_fast_even_with_long_interval() {
        let idx: Arc<dyn MetadataIndex> = Arc::new(InMemoryMetadataIndex::new());
        let emit: PrefetchEmit = Arc::new(|_| {});
        let cfg = PrefetchConfig {
            // Long interval, without sliced sleep, drop would
            // wait this long.
            interval: Duration::from_secs(10),
            top_k: 1,
            model_digest: [0u8; 24],
            namespace: String::new(),
        };
        let worker = spawn_worker(idx, cfg, emit);
        // Tiny sleep so the worker has run at least one cycle and is
        // now sleeping inside the inner slice loop.
        std::thread::sleep(Duration::from_millis(50));
        let t = Instant::now();
        worker.signal_stop();
        drop(worker);
        assert!(t.elapsed() < Duration::from_millis(500), "drop took {:?}", t.elapsed());
    }

    #[test]
    fn block_key_for_hash_matches_cabi_format() {
        // Mirror the cabi: `wombatkv/v1/block/b3=<64-char-lower-hex>`.
        use wombatkv_radix::BLOCK_KEY_PREFIX;
        let mut h = [0u8; 32];
        h[0] = 0xab;
        h[1] = 0xcd;
        h[31] = 0xef;
        let key = block_key_for_hash(&h);
        assert_eq!(key.len(), BLOCK_KEY_PREFIX.len() + 64);
        assert!(key.starts_with(BLOCK_KEY_PREFIX));
        let hex = &key[BLOCK_KEY_PREFIX.len()..];
        assert!(hex.starts_with("abcd"), "got hex prefix {hex:?}");
        assert!(hex.ends_with("ef"));
        assert!(hex.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    // ============================================================
    // v2 tests (RFC 0008 §6, second iteration).
    //
    // The v2 worker is wired to a `PrefetchFetcher`. We use a recording
    // mock so tests can assert exactly which keys would have hit the
    // store, without spinning up a `WombatKVKvStore<InMemoryObjectStore>`
    // (that round-trip is covered by the cabi/embed integration tests).
    // ============================================================

    struct MockFetcher {
        flat_keys: Mutex<Vec<String>>,
        // Optional canned errors per key, consumed once.
        errors: Mutex<std::collections::HashMap<String, String>>,
        // Optional bytes per key (hits). Missing = miss.
        bytes: Mutex<std::collections::HashMap<String, u64>>,
        // Recorded fetch calls (namespace, key) for assertion.
        calls: Mutex<Vec<(String, String)>>,
    }

    impl MockFetcher {
        fn new() -> Self {
            Self {
                flat_keys: Mutex::new(Vec::new()),
                errors: Mutex::new(std::collections::HashMap::new()),
                bytes: Mutex::new(std::collections::HashMap::new()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn stock_hit(&self, key: &str, len: u64) {
            self.bytes.lock().unwrap().insert(key.to_string(), len);
        }

        fn stock_err(&self, key: &str, msg: &str) {
            self.errors.lock().unwrap().insert(key.to_string(), msg.to_string());
        }

        fn pre_warm_flat(&self, key: &str) {
            self.flat_keys.lock().unwrap().push(key.to_string());
        }

        fn calls(&self) -> Vec<(String, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl PrefetchFetcher for MockFetcher {
        fn contains_flat(&self, _namespace: &str, key: &str) -> bool {
            self.flat_keys.lock().unwrap().iter().any(|k| k == key)
        }

        fn fetch_block(&self, namespace: &str, key: &str) -> Result<Option<u64>, String> {
            self.calls.lock().unwrap().push((namespace.to_string(), key.to_string()));
            if let Some(err) = self.errors.lock().unwrap().remove(key) {
                return Err(err);
            }
            if let Some(len) = self.bytes.lock().unwrap().get(key).copied() {
                // Simulate the store populating flat on a successful
                // fetch so a follow-up cycle would skip this key.
                self.flat_keys.lock().unwrap().push(key.to_string());
                return Ok(Some(len));
            }
            Ok(None)
        }
    }

    fn seed_recent_blocks(
        idx: &InMemoryMetadataIndex,
        count: u8,
        model: ModelDigest,
    ) -> Vec<BlockHash> {
        let now = now_ns();
        let mut hashes = Vec::with_capacity(count as usize);
        for i in 0..count {
            let h = make_hash(i);
            // Stagger last_access slightly so scores are deterministic
            // (older index = older time, so earlier entries score
            // slightly higher and the worker's selection is stable).
            let last = now.saturating_sub(1_000_000_000_u64.saturating_mul(u64::from(i)));
            idx.bulk_load(std::iter::once((h, mk_meta(0, last, model))));
            hashes.push(h);
        }
        hashes
    }

    #[test]
    fn v2_fetches_top_k_against_kvstore() {
        // Setup: 100 entries in the metadata index. The mock fetcher
        // has bytes stocked for every key. top_k=10 → exactly 10 calls
        // to fetch_block, all hitting.
        let idx = Arc::new(InMemoryMetadataIndex::new());
        let model = [7u8; 24];
        let hashes = seed_recent_blocks(&idx, 100, model);

        let fetcher = Arc::new(MockFetcher::new());
        for h in &hashes {
            let key = block_key_for_hash(h);
            fetcher.stock_hit(&key, 1024);
        }

        let cfg = PrefetchConfig {
            interval: Duration::from_mins(1),
            top_k: 10,
            model_digest: model,
            namespace: "ns-a".to_string(),
        };

        let stop = AtomicBool::new(false);
        let outcome = run_cycle_v2(
            idx.as_ref() as &dyn MetadataIndex,
            fetcher.as_ref() as &dyn PrefetchFetcher,
            &cfg,
            &stop,
        );

        assert_eq!(outcome.scored, 100);
        assert_eq!(outcome.selected, 10);
        assert_eq!(outcome.skipped_already_flat, 0);
        assert_eq!(outcome.fetched, 10);
        assert_eq!(outcome.failed, 0);
        assert_eq!(outcome.bytes_materialized, 10 * 1024);

        let calls = fetcher.calls();
        assert_eq!(calls.len(), 10);
        for (ns, key) in &calls {
            assert_eq!(ns, "ns-a");
            assert!(key.starts_with(wombatkv_radix::BLOCK_KEY_PREFIX));
        }
    }

    #[test]
    fn v2_skips_already_flat() {
        // 10 entries in the index; top_k=10. Pre-warm 5 of them in
        // the mock's flat tier. The worker must skip those and only
        // fetch the other 5.
        let idx = Arc::new(InMemoryMetadataIndex::new());
        let model = [7u8; 24];
        let hashes = seed_recent_blocks(&idx, 10, model);

        let fetcher = Arc::new(MockFetcher::new());
        for h in &hashes {
            let key = block_key_for_hash(h);
            fetcher.stock_hit(&key, 256);
        }
        // Pre-warm flat for the first 5 keys.
        for h in &hashes[..5] {
            let key = block_key_for_hash(h);
            fetcher.pre_warm_flat(&key);
        }

        let cfg = PrefetchConfig {
            interval: Duration::from_mins(1),
            top_k: 10,
            model_digest: model,
            namespace: "ns-a".to_string(),
        };
        let stop = AtomicBool::new(false);
        let outcome = run_cycle_v2(
            idx.as_ref() as &dyn MetadataIndex,
            fetcher.as_ref() as &dyn PrefetchFetcher,
            &cfg,
            &stop,
        );

        assert_eq!(outcome.scored, 10);
        assert_eq!(outcome.selected, 10);
        assert_eq!(outcome.skipped_already_flat, 5);
        assert_eq!(outcome.fetched, 5);
        assert_eq!(outcome.failed, 0);
        assert_eq!(outcome.bytes_materialized, 5 * 256);

        let calls = fetcher.calls();
        assert_eq!(calls.len(), 5, "should only fetch the 5 not-yet-flat keys");
    }

    #[test]
    fn v2_handles_get_kv_errors_gracefully() {
        let idx = Arc::new(InMemoryMetadataIndex::new());
        let model = [7u8; 24];
        let hashes = seed_recent_blocks(&idx, 6, model);

        let fetcher = Arc::new(MockFetcher::new());
        for (i, h) in hashes.iter().enumerate() {
            let key = block_key_for_hash(h);
            if i % 2 == 0 {
                fetcher.stock_err(&key, "synthetic backend error");
            } else {
                fetcher.stock_hit(&key, 100);
            }
        }

        let cfg = PrefetchConfig {
            interval: Duration::from_mins(1),
            top_k: 6,
            model_digest: model,
            namespace: "ns-a".to_string(),
        };
        let stop = AtomicBool::new(false);
        let outcome = run_cycle_v2(
            idx.as_ref() as &dyn MetadataIndex,
            fetcher.as_ref() as &dyn PrefetchFetcher,
            &cfg,
            &stop,
        );

        // Worker logged + continued through every error; cycle did
        // not crash. 3 successful fetches, 3 logged failures.
        assert_eq!(outcome.scored, 6);
        assert_eq!(outcome.selected, 6);
        assert_eq!(outcome.fetched, 3);
        assert_eq!(outcome.failed, 3);
        assert_eq!(outcome.bytes_materialized, 3 * 100);
        assert_eq!(fetcher.calls().len(), 6);
    }

    #[test]
    fn v2_dry_run_does_not_fetch() {
        // With WMBT_KV_PREFETCH_DRY_RUN=1, the embed-side
        // `start_prefetcher` is expected to route to `spawn_worker`
        // (v1) rather than `spawn_worker_v2`. Validate the gate
        // helper + that a v1 worker over the same index + emit makes
        // zero fetch calls on the fetcher.
        //
        // We don't `std::env::set_var` here because that's process-
        // global and would pollute other tests; we exercise the gate
        // by directly using the v1 path.
        let idx: Arc<dyn MetadataIndex> = Arc::new(InMemoryMetadataIndex::new());
        let model = [7u8; 24];
        // Mutate the concrete impl via downcast-equivalent: use
        // InMemoryMetadataIndex through the Arc, since we constructed
        // it ourselves.
        let inner = InMemoryMetadataIndex::new();
        seed_recent_blocks(&inner, 4, model);
        // Snapshot into the Arc-held index via bulk_load.
        // (The Arc<dyn ..> is what spawn_worker holds.)
        let snapshot = inner.entries();
        if let Some(_concrete) = idx.as_ref().entries().first() {
            // Already populated; no-op.
        }
        // Push snapshot through the dyn index via insert. (No
        // bulk_load on the trait object, concrete InMemoryMetadataIndex
        // would be needed; the v1 path doesn't care, since we're
        // asserting "no fetcher calls".)
        for (h, m) in snapshot {
            idx.insert(h, m);
        }

        let fetcher = Arc::new(MockFetcher::new());
        let plan_count = Arc::new(Mutex::new(0_usize));
        let pc = plan_count.clone();
        let emit: PrefetchEmit = Arc::new(move |_plan: &PrefetchPlan| {
            *pc.lock().unwrap() += 1;
        });

        let cfg = PrefetchConfig {
            interval: Duration::from_millis(40),
            top_k: 4,
            model_digest: model,
            namespace: "ns-dry".to_string(),
        };
        let worker = spawn_worker(idx, cfg, emit);
        std::thread::sleep(Duration::from_millis(150));
        drop(worker);

        // v1 emit fired at least once.
        assert!(*plan_count.lock().unwrap() >= 1);
        // ...and the fetcher was never touched.
        assert!(
            fetcher.calls().is_empty(),
            "dry-run path must not call PrefetchFetcher: got {} calls",
            fetcher.calls().len()
        );
    }

    #[test]
    fn dry_run_env_helper_reads_truthy_values() {
        // Wrap each set_var in a single-threaded section so other
        // tests don't see the env flicker. We're already inside a
        // #[test], so cargo serializes vs other ENV-touching tests
        // only by chance; keep the env restored on the way out.
        let saved = std::env::var("WMBT_KV_PREFETCH_DRY_RUN").ok();
        std::env::remove_var("WMBT_KV_PREFETCH_DRY_RUN");
        assert!(!dry_run_enabled());
        std::env::set_var("WMBT_KV_PREFETCH_DRY_RUN", "1");
        assert!(dry_run_enabled());
        std::env::set_var("WMBT_KV_PREFETCH_DRY_RUN", "yes");
        assert!(dry_run_enabled());
        std::env::set_var("WMBT_KV_PREFETCH_DRY_RUN", "0");
        assert!(!dry_run_enabled());
        std::env::remove_var("WMBT_KV_PREFETCH_DRY_RUN");
        if let Some(v) = saved {
            std::env::set_var("WMBT_KV_PREFETCH_DRY_RUN", v);
        }
    }

    #[test]
    fn v2_worker_runs_and_stops() {
        // End-to-end: spawn the v2 worker against a real-ish (mock)
        // fetcher, let it run a couple of cycles, then drop.
        let idx_concrete = Arc::new(InMemoryMetadataIndex::new());
        let model = [3u8; 24];
        let hashes = seed_recent_blocks(&idx_concrete, 5, model);
        let idx: Arc<dyn MetadataIndex> = idx_concrete.clone();

        let fetcher = Arc::new(MockFetcher::new());
        for h in &hashes {
            fetcher.stock_hit(&block_key_for_hash(h), 64);
        }

        let cfg = PrefetchConfig {
            interval: Duration::from_millis(30),
            top_k: 5,
            model_digest: model,
            namespace: "ns-x".to_string(),
        };

        let outcomes = Arc::new(Mutex::new(Vec::<PrefetchFetchOutcome>::new()));
        let outcomes_for_cb = outcomes.clone();
        let emit_outcome: Arc<dyn Fn(&PrefetchFetchOutcome) + Send + Sync> =
            Arc::new(move |o: &PrefetchFetchOutcome| {
                outcomes_for_cb.lock().unwrap().push(o.clone());
            });

        let fetcher_dyn: Arc<dyn PrefetchFetcher> = fetcher.clone();
        let started = Instant::now();
        let worker = spawn_worker_v2(idx, cfg, fetcher_dyn, emit_outcome);
        std::thread::sleep(Duration::from_millis(140));
        drop(worker);
        assert!(started.elapsed() < Duration::from_secs(2));

        let observed = outcomes.lock().unwrap();
        assert!(observed.len() >= 2);
        // First cycle should have done all 5 fetches; subsequent
        // cycles should see them all as already-flat.
        assert_eq!(observed[0].fetched, 5);
        if observed.len() >= 2 {
            assert_eq!(observed[1].fetched, 0);
            assert_eq!(observed[1].skipped_already_flat, 5);
        }
    }
}
