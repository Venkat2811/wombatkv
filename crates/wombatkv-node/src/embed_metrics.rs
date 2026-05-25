#![forbid(unsafe_code)]
//! Lightweight observability for the embeddable KV store.
//!
//! Tracks per-operation latency histograms (microseconds) and bytes
//! moved. Percentile computation is on-demand via sort, fine for the
//! sample sizes we hit on a single-engine warm path; for very high
//! throughput an HDR/CKMS-style sketch would be more appropriate.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Op tag for metric attribution. Cheap enum so the hot path doesn't
/// touch a string.
///
/// `LoadFoyerRam` / `LoadFoyerSsd` split the legacy `LoadFoyer` bucket
/// by hit tier. `LoadFoyer` remains for callers that don't / can't
/// distinguish; new code paths should pick one of the tiered variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    Stash,
    LoadFoyer,
    LoadFoyerRam,
    LoadFoyerSsd,
    LoadS3,
    Miss,
    RestoreFromS3,
}

impl Op {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Stash => "stash",
            Self::LoadFoyer => "load_foyer",
            Self::LoadFoyerRam => "load_foyer_ram",
            Self::LoadFoyerSsd => "load_foyer_ssd",
            Self::LoadS3 => "load_s3",
            Self::Miss => "miss",
            Self::RestoreFromS3 => "restore_from_s3",
        }
    }
}

/// Snapshot of one op's stats. Cheap to clone.
#[derive(Debug, Clone, Default)]
pub struct OpSnapshot {
    pub op: &'static str,
    pub count: u64,
    pub bytes_total: u64,
    pub micros_total: u64,
    pub p10_us: u64,
    pub p25_us: u64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p99_9_us: u64,
    pub p99_99_us: u64,
    pub max_us: u64,
}

impl OpSnapshot {
    #[must_use]
    pub fn throughput_mb_per_s(&self) -> f64 {
        if self.micros_total == 0 {
            return 0.0;
        }
        let mb = (self.bytes_total as f64) / (1024.0 * 1024.0);
        let s = (self.micros_total as f64) / 1_000_000.0;
        mb / s
    }
}

/// One per-op accumulator. Counters are atomic; samples live in a Mutex
/// so we can compute exact percentiles on snapshot. Soft cap prevents
/// unbounded growth.
struct OpAccumulator {
    op: Op,
    count: AtomicU64,
    bytes_total: AtomicU64,
    micros_total: AtomicU64,
    samples: Mutex<Vec<u32>>,
    max_samples: usize,
}

impl OpAccumulator {
    fn new(op: Op, max_samples: usize) -> Self {
        Self {
            op,
            count: AtomicU64::new(0),
            bytes_total: AtomicU64::new(0),
            micros_total: AtomicU64::new(0),
            samples: Mutex::new(Vec::with_capacity(max_samples.min(8192))),
            max_samples,
        }
    }

    fn observe(&self, micros: u64, bytes: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes_total.fetch_add(bytes, Ordering::Relaxed);
        self.micros_total.fetch_add(micros, Ordering::Relaxed);
        if let Ok(mut samples) = self.samples.lock() {
            // Reservoir: when we exceed cap, replace oldest 25% so we
            // retain a mix of recent + historical samples without growing
            // unboundedly.
            if samples.len() >= self.max_samples {
                let drop = self.max_samples / 4;
                samples.drain(0..drop);
            }
            samples.push(u32::try_from(micros).unwrap_or(u32::MAX));
        }
    }

    fn snapshot(&self) -> OpSnapshot {
        let mut samples = self.samples.lock().map(|guard| guard.clone()).unwrap_or_default();
        samples.sort_unstable();
        let count = self.count.load(Ordering::Relaxed);
        let bytes_total = self.bytes_total.load(Ordering::Relaxed);
        let micros_total = self.micros_total.load(Ordering::Relaxed);

        OpSnapshot {
            op: self.op.as_str(),
            count,
            bytes_total,
            micros_total,
            p10_us: percentile(&samples, 10.0),
            p25_us: percentile(&samples, 25.0),
            p50_us: percentile(&samples, 50.0),
            p90_us: percentile(&samples, 90.0),
            p95_us: percentile(&samples, 95.0),
            p99_us: percentile(&samples, 99.0),
            p99_9_us: percentile(&samples, 99.9),
            p99_99_us: percentile(&samples, 99.99),
            max_us: u64::from(samples.last().copied().unwrap_or(0)),
        }
    }
}

fn percentile(sorted_samples: &[u32], pct: f64) -> u64 {
    if sorted_samples.is_empty() {
        return 0;
    }
    let n = sorted_samples.len();
    let rank = (pct / 100.0) * (n.saturating_sub(1) as f64);
    let idx = rank.round() as usize;
    u64::from(sorted_samples[idx.min(n - 1)])
}

/// Aggregate metrics across all ops. Cheap to clone; intentionally
/// process-global so the engine and any cli-stats utility look at the
/// same registry.
pub struct EmbedMetrics {
    stash: OpAccumulator,
    load_foyer: OpAccumulator,
    load_foyer_ram: OpAccumulator,
    load_foyer_ssd: OpAccumulator,
    load_s3: OpAccumulator,
    miss: OpAccumulator,
    restore_from_s3: OpAccumulator,
}

impl EmbedMetrics {
    fn new() -> Self {
        let cap = std::env::var("WMBT_KV_METRICS_MAX_SAMPLES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(4096);
        Self {
            stash: OpAccumulator::new(Op::Stash, cap),
            load_foyer: OpAccumulator::new(Op::LoadFoyer, cap),
            load_foyer_ram: OpAccumulator::new(Op::LoadFoyerRam, cap),
            load_foyer_ssd: OpAccumulator::new(Op::LoadFoyerSsd, cap),
            load_s3: OpAccumulator::new(Op::LoadS3, cap),
            miss: OpAccumulator::new(Op::Miss, cap),
            restore_from_s3: OpAccumulator::new(Op::RestoreFromS3, cap),
        }
    }

    pub fn observe(&self, op: Op, micros: u64, bytes: u64) {
        match op {
            Op::Stash => self.stash.observe(micros, bytes),
            Op::LoadFoyer => self.load_foyer.observe(micros, bytes),
            Op::LoadFoyerRam => {
                self.load_foyer.observe(micros, bytes);
                self.load_foyer_ram.observe(micros, bytes);
            }
            Op::LoadFoyerSsd => {
                self.load_foyer.observe(micros, bytes);
                self.load_foyer_ssd.observe(micros, bytes);
            }
            Op::LoadS3 => self.load_s3.observe(micros, bytes),
            Op::Miss => self.miss.observe(micros, bytes),
            Op::RestoreFromS3 => self.restore_from_s3.observe(micros, bytes),
        }
    }

    #[must_use]
    pub fn snapshot_all(&self) -> Vec<OpSnapshot> {
        vec![
            self.stash.snapshot(),
            self.load_foyer.snapshot(),
            self.load_foyer_ram.snapshot(),
            self.load_foyer_ssd.snapshot(),
            self.load_s3.snapshot(),
            self.miss.snapshot(),
            self.restore_from_s3.snapshot(),
        ]
    }

    /// Render a JSON-line report suitable for log emission.
    #[must_use]
    pub fn to_json_lines(&self) -> String {
        let mut out = String::new();
        for snap in self.snapshot_all() {
            out.push_str(&format!(
                "{{\"scope\":\"wombatkv_metrics\",\"op\":\"{}\",\"count\":{},\"bytes_total\":{},\"throughput_mb_per_s\":{:.3},\"p10_us\":{},\"p25_us\":{},\"p50_us\":{},\"p90_us\":{},\"p95_us\":{},\"p99_us\":{},\"p99_9_us\":{},\"p99_99_us\":{},\"max_us\":{}}}\n",
                snap.op,
                snap.count,
                snap.bytes_total,
                snap.throughput_mb_per_s(),
                snap.p10_us,
                snap.p25_us,
                snap.p50_us,
                snap.p90_us,
                snap.p95_us,
                snap.p99_us,
                snap.p99_9_us,
                snap.p99_99_us,
                snap.max_us,
            ));
        }
        out
    }
}

static GLOBAL: once_cell::sync::Lazy<EmbedMetrics> = once_cell::sync::Lazy::new(EmbedMetrics::new);

/// Borrow the process-global metrics registry.
#[must_use]
pub fn metrics() -> &'static EmbedMetrics {
    &GLOBAL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentiles_compute_correctly_on_known_distribution() {
        let m = EmbedMetrics::new();
        for v in 1..=100 {
            m.observe(Op::Stash, v, 0);
        }
        let snap = m.snapshot_all().into_iter().find(|s| s.op == "stash").unwrap();
        assert_eq!(snap.count, 100);
        // Percentiles use nearest-rank with round-half-up; for 1..=100
        // we accept the off-by-one between (50 vs 51) etc.
        assert!(snap.p50_us == 50 || snap.p50_us == 51);
        assert!(snap.p99_us == 99 || snap.p99_us == 100);
        assert_eq!(snap.max_us, 100);
    }

    #[test]
    fn throughput_is_computed_per_op() {
        let m = EmbedMetrics::new();
        m.observe(Op::Stash, 1_000_000, 1024 * 1024); // 1 MB in 1 s
        m.observe(Op::Stash, 1_000_000, 1024 * 1024);
        let snap = m.snapshot_all().into_iter().find(|s| s.op == "stash").unwrap();
        // 2 MB in 2 s = 1 MB/s
        assert!((snap.throughput_mb_per_s() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn json_line_output_includes_all_ops() {
        let m = EmbedMetrics::new();
        m.observe(Op::Stash, 100, 1000);
        let json = m.to_json_lines();
        for op in [
            "stash",
            "load_foyer",
            "load_foyer_ram",
            "load_foyer_ssd",
            "load_s3",
            "miss",
            "restore_from_s3",
        ] {
            assert!(json.contains(&format!("\"op\":\"{op}\"")), "missing op {op} in {json}");
        }
    }

    #[test]
    fn load_foyer_ram_increments_both_legacy_and_tiered_counters() {
        // Dual-recording keeps legacy load_foyer counters intact for
        // existing dashboards / tests while the new tiered split is
        // available to callers that want it.
        let m = EmbedMetrics::new();
        m.observe(Op::LoadFoyerRam, 50, 1000);
        let snaps = m.snapshot_all();
        let by_op = |o: &str| snaps.iter().find(|s| s.op == o).unwrap();
        assert_eq!(by_op("load_foyer").count, 1);
        assert_eq!(by_op("load_foyer_ram").count, 1);
        assert_eq!(by_op("load_foyer_ssd").count, 0);
    }

    #[test]
    fn load_foyer_ssd_routes_to_correct_bucket() {
        let m = EmbedMetrics::new();
        m.observe(Op::LoadFoyerSsd, 5000, 4_000_000_000);
        let snaps = m.snapshot_all();
        let by_op = |o: &str| snaps.iter().find(|s| s.op == o).unwrap();
        assert_eq!(by_op("load_foyer").count, 1);
        assert_eq!(by_op("load_foyer_ram").count, 0);
        assert_eq!(by_op("load_foyer_ssd").count, 1);
        assert_eq!(by_op("load_foyer_ssd").bytes_total, 4_000_000_000);
    }
}

#[cfg(test)]
mod ops_through_embed {
    use super::*;
    use crate::embed::{EmbedConfig, WombatKVKvStore};
    use crate::foyer_cache::FoyerCacheConfig;
    use bytes::Bytes;
    use tempfile::tempdir;
    use wombatkv_store::wal_store::InMemoryObjectStore;

    fn small_foyer(dir: std::path::PathBuf) -> FoyerCacheConfig {
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
    fn put_get_round_trip_records_stash_and_load_foyer_observations() {
        let dir = tempdir().expect("tempdir");
        let cfg = EmbedConfig {
            s3_prefix: "metrics/test".to_string(),
            foyer: small_foyer(dir.path().to_path_buf()),
            write_through_s3: true,
            compression: crate::compression::BlockCompressionConfig::default(),
        };
        let store = WombatKVKvStore::new(cfg, InMemoryObjectStore::default()).expect("store");
        let pre = metrics().snapshot_all();
        let pre_stash = pre.iter().find(|s| s.op == "stash").unwrap().count;
        let pre_load = pre.iter().find(|s| s.op == "load_foyer").unwrap().count;

        store.put_kv("ns", "k", Bytes::from_static(b"abc")).expect("put");
        let _ = store.get_kv("ns", "k").expect("get");

        let post = metrics().snapshot_all();
        let post_stash = post.iter().find(|s| s.op == "stash").unwrap().count;
        let post_load = post.iter().find(|s| s.op == "load_foyer").unwrap().count;
        // metrics() is a process-global singleton; parallel tests pollute
        // the snapshot. Assert we observed AT LEAST our own put + get,
        // not strict equality. This makes the test robust to running
        // alongside the broader (now ~180-test) suite.
        assert!(
            post_stash > pre_stash,
            "expected stash count to grow by ≥1; pre={pre_stash} post={post_stash}"
        );
        assert!(
            post_load > pre_load,
            "expected load_foyer count to grow by ≥1; pre={pre_load} post={post_load}"
        );
    }
}
