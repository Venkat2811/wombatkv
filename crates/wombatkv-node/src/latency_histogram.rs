//! Fixed-bucket latency histogram with lock-free inserts + a global
//! registry keyed by `(fn, path)` tag for emit_timing integration.
//!
//! Records microsecond latencies into log-scale buckets (powers of
//! 2). 30 buckets covers 1µs to ~1 hour, enough for our actual
//! workload (S3 GET 10-100ms, Metal decode 1-2s, daemon RPC <µs to
//! ms). p50/p90/p99/p99.9 derived via bucket-boundary interpolation.
//!
//! # Why fixed buckets vs HDR or T-digest
//!
//! - **HDR** (hdrhistogram crate): more accurate but heavy dep and
//!   each instance is ~100KB. We want one histogram per (fn, path)
//!   tag, easily 20+ tags → 2MB+ per daemon. Overkill for alpha.
//! - **T-digest**: better accuracy at the tails but requires a
//!   merge operation that's not lock-free.
//! - **Fixed log-scale buckets**: cheap (240 bytes per histogram),
//!   lock-free, perfect-enough percentile accuracy for our alpha
//!   "is the tail blowing up?" question.
//!
//! Each bucket boundary = `2^bucket_idx` microseconds. Bucket N
//! covers `[2^N, 2^(N+1))` µs. Bucket 0 = 1-2µs; bucket 20 = 1.05s
//! to 2.1s; bucket 30 = ~17min to ~34min.
//!
//! # Use
//!
//! ```ignore
//! use wombatkv_node::latency_histogram::LatencyHistogram;
//! let h = LatencyHistogram::new();
//! h.record_us(123);
//! h.record_us(456);
//! let snap = h.snapshot();
//! assert!(snap.total_count() == 2);
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

/// Number of log-scale buckets. 30 covers 1µs to ~17min.
pub const HISTOGRAM_BUCKETS: usize = 30;

/// Lock-free fixed-bucket latency histogram.
///
/// Hot-path `record_us` is one `floor(log2(us))` + one atomic
/// increment. Read-path `snapshot` reads all buckets non-atomically
/// (relaxed); concurrent records during a snapshot may miss the
/// snapshot or be counted partially, which is fine for percentile
/// estimation.
pub struct LatencyHistogram {
    buckets: [AtomicU64; HISTOGRAM_BUCKETS],
    overflow: AtomicU64, // > 2^HISTOGRAM_BUCKETS µs
    sum_us: AtomicU64,
    count: AtomicU64,
    max_us: AtomicU64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    #[must_use]
    pub fn new() -> Self {
        // Cannot construct [T; N] with non-Copy T directly; use array::from_fn.
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    /// Record a latency measurement in microseconds.
    pub fn record_us(&self, us: u64) {
        let bucket = bucket_for(us);
        if bucket >= HISTOGRAM_BUCKETS {
            self.overflow.fetch_add(1, Ordering::Relaxed);
        } else {
            self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // Max via CAS loop; uncontended in practice.
        let mut cur = self.max_us.load(Ordering::Relaxed);
        while us > cur {
            match self.max_us.compare_exchange_weak(cur, us, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(observed) => cur = observed,
            }
        }
    }

    /// Snapshot the histogram for percentile reading.
    #[must_use]
    pub fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [0u64; HISTOGRAM_BUCKETS];
        for (i, b) in self.buckets.iter().enumerate() {
            buckets[i] = b.load(Ordering::Relaxed);
        }
        HistogramSnapshot {
            buckets,
            overflow: self.overflow.load(Ordering::Relaxed),
            sum_us: self.sum_us.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
            max_us: self.max_us.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero. Useful for periodic-emit windows.
    pub fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.overflow.store(0, Ordering::Relaxed);
        self.sum_us.store(0, Ordering::Relaxed);
        self.count.store(0, Ordering::Relaxed);
        self.max_us.store(0, Ordering::Relaxed);
    }
}

/// Point-in-time snapshot of a `LatencyHistogram`.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    pub buckets: [u64; HISTOGRAM_BUCKETS],
    pub overflow: u64,
    pub sum_us: u64,
    pub count: u64,
    pub max_us: u64,
}

impl HistogramSnapshot {
    /// Total number of samples recorded.
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.count
    }

    /// Mean latency in microseconds. `None` if no samples.
    #[must_use]
    pub fn mean_us(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            Some(self.sum_us as f64 / self.count as f64)
        }
    }

    /// Percentile in microseconds via bucket-boundary interpolation.
    /// `p` is in `[0.0, 100.0]`. Returns `None` if no samples.
    #[must_use]
    pub fn percentile_us(&self, p: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        let target = ((self.count as f64) * (p / 100.0)).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                // Bucket i covers [2^i, 2^(i+1)) µs. Return the
                // bucket upper bound as a conservative estimate.
                let upper = 1u64 << (i + 1).min(63);
                return Some(upper);
            }
        }
        if cumulative + self.overflow >= target {
            // Fell into the overflow bucket; return max observed.
            return Some(self.max_us);
        }
        Some(self.max_us)
    }

    /// Convenience: the four percentiles we usually want.
    #[must_use]
    pub fn p50_p90_p99_p999(&self) -> Option<(u64, u64, u64, u64)> {
        Some((
            self.percentile_us(50.0)?,
            self.percentile_us(90.0)?,
            self.percentile_us(99.0)?,
            self.percentile_us(99.9)?,
        ))
    }
}

// =========================================================================
// Global per-(fn, path) registry
// =========================================================================

/// Process-wide registry mapping `tag` strings (typically
/// `"<func>:<path>"`) to histogram instances. Lazy-initialized on
/// first record.
fn registry() -> &'static RwLock<HashMap<String, Arc<LatencyHistogram>>> {
    static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<LatencyHistogram>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Record a latency observation against the global registry under
/// `tag`. Creates the histogram on first use. Read-lock-fast path
/// for repeat tags; write-lock only on first-use insert.
pub fn record_global(tag: &str, us: u64) {
    // Hot path: read-lock, find tag, record.
    {
        let r = registry().read().expect("histogram registry poisoned");
        if let Some(h) = r.get(tag) {
            h.record_us(us);
            return;
        }
    }
    // Cold path: write-lock, get-or-insert, record.
    let mut w = registry().write().expect("histogram registry poisoned");
    let h = w.entry(tag.to_string()).or_insert_with(|| Arc::new(LatencyHistogram::new())).clone();
    drop(w);
    h.record_us(us);
}

/// Snapshot every histogram in the registry.
#[must_use]
pub fn snapshot_all() -> HashMap<String, HistogramSnapshot> {
    let r = registry().read().expect("histogram registry poisoned");
    r.iter().map(|(k, h)| (k.clone(), h.snapshot())).collect()
}

/// Reset every histogram in the registry. Useful for periodic-emit
/// windows where each window's stats are reported then zeroed.
pub fn reset_all() {
    let r = registry().read().expect("histogram registry poisoned");
    for h in r.values() {
        h.reset();
    }
}

/// Emit a single MyelonInstr line per (tag, snapshot), the same
/// shape as `embed::emit_timing` events so existing log consumers
/// can pick it up without schema changes.
pub fn emit_snapshot_jsonl<W: std::io::Write>(out: &mut W) -> std::io::Result<()> {
    let snaps = snapshot_all();
    for (tag, snap) in snaps {
        if snap.total_count() == 0 {
            continue;
        }
        let (p50, p90, p99, p999) = snap.p50_p90_p99_p999().unwrap_or((0, 0, 0, 0));
        writeln!(
            out,
            "[MyelonInstr] {{\"scope\":\"wmbt_kv_latency_histogram\",\"tag\":\"{tag}\",\
             \"count\":{},\"mean_us\":{:.1},\"p50_us\":{},\"p90_us\":{},\
             \"p99_us\":{},\"p999_us\":{},\"max_us\":{},\"overflow\":{}}}",
            snap.total_count(),
            snap.mean_us().unwrap_or(0.0),
            p50,
            p90,
            p99,
            p999,
            snap.max_us,
            snap.overflow,
        )?;
    }
    Ok(())
}

/// `floor(log2(us))` for bucket selection. `us == 0` → 0 (the
/// 1-2µs bucket). Saturates at HISTOGRAM_BUCKETS for the overflow
/// path.
fn bucket_for(us: u64) -> usize {
    if us <= 1 {
        0
    } else {
        // 63 - leading_zeros gives floor(log2)
        let bits = 64 - us.leading_zeros() as usize - 1;
        bits.min(HISTOGRAM_BUCKETS)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_for_known_values() {
        assert_eq!(bucket_for(0), 0);
        assert_eq!(bucket_for(1), 0);
        assert_eq!(bucket_for(2), 1);
        assert_eq!(bucket_for(3), 1);
        assert_eq!(bucket_for(4), 2);
        assert_eq!(bucket_for(7), 2);
        assert_eq!(bucket_for(8), 3);
        assert_eq!(bucket_for(1024), 10);
        assert_eq!(bucket_for(1_000_000), 19); // ~1s
    }

    #[test]
    fn empty_snapshot_has_no_percentile() {
        let h = LatencyHistogram::new();
        let snap = h.snapshot();
        assert!(snap.percentile_us(50.0).is_none());
        assert!(snap.mean_us().is_none());
        assert_eq!(snap.total_count(), 0);
    }

    #[test]
    fn percentile_walks_buckets() {
        let h = LatencyHistogram::new();
        for us in [100, 200, 300, 400, 500, 1000, 2000, 3000, 4000, 5000] {
            h.record_us(us);
        }
        let snap = h.snapshot();
        assert_eq!(snap.total_count(), 10);
        // p50 should land in a low bucket (100-500µs range → bucket
        // 6-9, upper bound 128-1024µs).
        let p50 = snap.percentile_us(50.0).unwrap();
        assert!(p50 >= 128, "p50={p50}");
        assert!(p50 <= 1024, "p50={p50}");
        // p99 should be in the high bucket (5000µs → bucket 12, upper 8192).
        let p99 = snap.percentile_us(99.0).unwrap();
        assert!(p99 >= 8192, "p99={p99}");
    }

    #[test]
    fn reset_zeroes_all_counters() {
        let h = LatencyHistogram::new();
        h.record_us(1000);
        h.record_us(2000);
        assert_eq!(h.snapshot().total_count(), 2);
        h.reset();
        assert_eq!(h.snapshot().total_count(), 0);
        assert!(h.snapshot().mean_us().is_none());
    }

    #[test]
    fn overflow_bucket_catches_extreme_values() {
        let h = LatencyHistogram::new();
        // ~17 minutes, beyond HISTOGRAM_BUCKETS=30 boundary
        h.record_us(2u64.pow(31));
        let snap = h.snapshot();
        assert_eq!(snap.overflow, 1);
        assert_eq!(snap.total_count(), 1);
    }

    #[test]
    fn mean_and_max_match() {
        let h = LatencyHistogram::new();
        h.record_us(100);
        h.record_us(200);
        h.record_us(300);
        let snap = h.snapshot();
        assert_eq!(snap.max_us, 300);
        assert!((snap.mean_us().unwrap() - 200.0).abs() < 0.1);
    }

    #[test]
    fn lock_free_concurrent_inserts() {
        use std::thread;
        let h = Arc::new(LatencyHistogram::new());
        let mut handles = vec![];
        for t in 0..8 {
            let h = Arc::clone(&h);
            handles.push(thread::spawn(move || {
                for i in 0..1000 {
                    h.record_us((t * 1000 + i) as u64 + 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(h.snapshot().total_count(), 8000);
    }

    #[test]
    fn global_registry_creates_histograms_on_first_use() {
        // Use a tag that no other test in this crate writes to so we
        // get a clean baseline.
        let tag = "test_global_registry_creates_v1";
        record_global(tag, 100);
        record_global(tag, 200);
        record_global(tag, 300);
        let snaps = snapshot_all();
        let snap = snaps.get(tag).expect("registered");
        assert_eq!(snap.total_count(), 3);
        assert!(snap.mean_us().unwrap() > 100.0);
    }

    #[test]
    fn emit_snapshot_jsonl_writes_one_line_per_tag() {
        let tag = "test_emit_jsonl_v1";
        record_global(tag, 1000);
        let mut buf = Vec::new();
        emit_snapshot_jsonl(&mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        // The line for OUR tag (others may exist from concurrent tests).
        let our_line = text.lines().find(|l| l.contains(tag)).expect("our tag in jsonl");
        assert!(our_line.contains("p50_us"));
        assert!(our_line.contains("\"count\":"));
    }
}
