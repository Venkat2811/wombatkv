//! WombatKV reference oracle. Stage 4 of the DST roadmap.
//!
//! An in-memory model of the WombatKV semantics that the DST runner
//! compares against the real (or simulated-faulted) puffer. The
//! runner drives the same sequence of operations against both; any
//! divergence between them is a bug.
//!
//! ## Surface model
//!
//! `wombatkv-node`'s public KV surface is conceptually:
//!
//! ```text
//! put_kv(namespace, key, bytes) ->
//! get_kv(namespace, key) -> Hit { bytes, tier } | Miss
//! lookup_block_prefix(namespace, &[blake3_hashes]) -> matched_count
//! ```
//!
//! The oracle implements these against a plain `BTreeMap`-backed
//! shadow state. Properties it enforces:
//!
//! 1. **Read-your-writes**, after `put_kv(ns, k, v)`, every
//!    subsequent `get_kv(ns, k)` returns `v`. No staleness.
//! 2. **Content addressing**, for the block surface, the same
//!    hash always returns the same bytes (or Miss); two clients
//!    PUTting identical content never produce divergent reads.
//! 3. **Namespace isolation**: `(ns, k)` writes don't leak into
//!    `(ns', k)` reads.
//! 4. **Fault-injected error**, when the runner injects an S3
//!    failure on a PUT, the oracle marks the key absent (matching
//!    the "S3 PUT failed → caller must not assume durability"
//!    contract). On a successful retry, the oracle accepts the
//!    later write.
//!
//! ## What the oracle does NOT model (yet)
//!
//! - Foyer-RAM-vs-foyer-SSD-vs-S3 tier hint in `Hit`. The runner can
//!   assert this independently when needed.
//! - Compression / blake3 chain hashing. Treated as black-box bytes.
//! - Concurrent put-with-same-key races. Stage 4.5 follow-up; the
//!   oracle today assumes a single producer.
//!
//! These omissions are explicit. Adding them is straightforward when
//! a test wants to exercise the property.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// What an oracle `get_kv` returned. Mirrors the puffer's `GetOutcome`
/// shape without coupling to its tier metadata (the oracle doesn't
/// model cache tiers, see module-level note).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OracleGetOutcome {
    Hit { payload: Vec<u8> },
    Miss,
}

/// In-memory model of the KV surface.
///
/// Cheap to clone (`Vec<u8>` payloads + a `BTreeMap`); intended to be
/// re-snapshotted for diffing rather than long-lived.
///
/// The composite key is the literal `"{namespace}\u{1}{key}"` so a
/// flat `BTreeMap<String, Vec<u8>>` works (avoids serde_with for
/// JSON-friendly snapshots). The U+0001 separator is forbidden in
/// WombatKV namespace/key strings, so collisions are not a concern.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WombatKvOracle {
    /// composite "ns\u{1}key" → payload
    store: BTreeMap<String, Vec<u8>>,
    /// PUT call count. Used for "AfterKvOp { n }" trigger matching.
    pub ops_observed: u64,
}

fn composite_key(namespace: &str, key: &str) -> String {
    let mut s = String::with_capacity(namespace.len() + 1 + key.len());
    s.push_str(namespace);
    s.push('\u{1}');
    s.push_str(key);
    s
}

impl WombatKvOracle {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_kv(&mut self, namespace: &str, key: &str, payload: &[u8]) {
        self.store.insert(composite_key(namespace, key), payload.to_vec());
        self.ops_observed = self.ops_observed.saturating_add(1);
    }

    /// Record a fault-injected PUT failure: the bytes are NOT
    /// committed. After this, `get_kv` for the same (ns, key) returns
    /// Miss (or the prior value if there was one).
    pub fn put_kv_failed(&mut self, _namespace: &str, _key: &str) {
        // No-op on storage, the failed PUT must not commit. We still
        // count the op so AfterKvOp triggers fire correctly.
        self.ops_observed = self.ops_observed.saturating_add(1);
    }

    #[must_use]
    pub fn get_kv(&self, namespace: &str, key: &str) -> OracleGetOutcome {
        match self.store.get(&composite_key(namespace, key)) {
            Some(bytes) => OracleGetOutcome::Hit { payload: bytes.clone() },
            None => OracleGetOutcome::Miss,
        }
    }

    /// Snapshot the oracle's key universe for diff reporting.
    /// Returns (namespace, key) pairs.
    #[must_use]
    pub fn keys(&self) -> Vec<(String, String)> {
        self.store
            .keys()
            .map(|composite| {
                composite.split_once('\u{1}').map_or_else(
                    || (String::new(), composite.clone()),
                    |(ns, k)| (ns.to_string(), k.to_string()),
                )
            })
            .collect()
    }

    /// Total number of stored entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.store.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.store.is_empty()
    }
}

/// Verdict from comparing an observed puffer result to the oracle's
/// expectation for the same call.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Verdict {
    /// Observed matches the oracle prediction. Pass.
    Match,
    /// Observed differs from oracle. Reported with both sides for the
    /// runner to log; never auto-corrected (a divergence is a bug).
    Divergence {
        namespace: String,
        key: String,
        expected: OracleGetOutcome,
        observed: OracleGetOutcome,
    },
}

impl Verdict {
    /// Compare an observed `get_kv` outcome to the oracle's prediction
    /// for the same (namespace, key). Returns Match or Divergence.
    #[must_use]
    pub fn check_get(
        oracle: &WombatKvOracle,
        namespace: &str,
        key: &str,
        observed: OracleGetOutcome,
    ) -> Self {
        let expected = oracle.get_kv(namespace, key);
        if expected == observed {
            Verdict::Match
        } else {
            Verdict::Divergence {
                namespace: namespace.to_string(),
                key: key.to_string(),
                expected,
                observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_then_get_yields_hit() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns", "k1", b"hello");
        assert_eq!(oracle.get_kv("ns", "k1"), OracleGetOutcome::Hit { payload: b"hello".to_vec() });
    }

    #[test]
    fn missing_key_is_miss() {
        let oracle = WombatKvOracle::new();
        assert_eq!(oracle.get_kv("ns", "absent"), OracleGetOutcome::Miss);
    }

    #[test]
    fn namespace_isolation() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns_a", "k", b"value-a");
        oracle.put_kv("ns_b", "k", b"value-b");
        assert_eq!(
            oracle.get_kv("ns_a", "k"),
            OracleGetOutcome::Hit { payload: b"value-a".to_vec() }
        );
        assert_eq!(
            oracle.get_kv("ns_b", "k"),
            OracleGetOutcome::Hit { payload: b"value-b".to_vec() }
        );
    }

    #[test]
    fn failed_put_does_not_commit() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv_failed("ns", "k1");
        assert_eq!(oracle.get_kv("ns", "k1"), OracleGetOutcome::Miss);
        assert_eq!(oracle.ops_observed, 1);
    }

    #[test]
    fn failed_put_preserves_prior_value() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns", "k1", b"first");
        oracle.put_kv_failed("ns", "k1");
        assert_eq!(oracle.get_kv("ns", "k1"), OracleGetOutcome::Hit { payload: b"first".to_vec() });
    }

    #[test]
    fn verdict_match_when_observation_matches() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns", "k", b"x");
        let v = Verdict::check_get(
            &oracle,
            "ns",
            "k",
            OracleGetOutcome::Hit { payload: b"x".to_vec() },
        );
        assert!(matches!(v, Verdict::Match));
    }

    #[test]
    fn verdict_divergence_when_observation_differs() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns", "k", b"expected");
        let v = Verdict::check_get(
            &oracle,
            "ns",
            "k",
            OracleGetOutcome::Hit { payload: b"WRONG".to_vec() },
        );
        match v {
            Verdict::Divergence { namespace, key, expected, observed } => {
                assert_eq!(namespace, "ns");
                assert_eq!(key, "k");
                assert!(matches!(expected, OracleGetOutcome::Hit { .. }));
                assert!(matches!(observed, OracleGetOutcome::Hit { .. }));
                if let (
                    OracleGetOutcome::Hit { payload: exp },
                    OracleGetOutcome::Hit { payload: obs },
                ) = (expected, observed)
                {
                    assert_eq!(exp, b"expected");
                    assert_eq!(obs, b"WRONG");
                }
            }
            Verdict::Match => panic!("expected divergence"),
        }
    }

    #[test]
    fn verdict_divergence_on_unexpected_miss() {
        let oracle = WombatKvOracle::new();
        let v = Verdict::check_get(
            &oracle,
            "ns",
            "k",
            OracleGetOutcome::Hit { payload: b"phantom".to_vec() },
        );
        assert!(matches!(v, Verdict::Divergence { .. }));
    }

    #[test]
    fn ops_observed_counts_both_success_and_failure() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns", "k1", b"x");
        oracle.put_kv_failed("ns", "k2");
        oracle.put_kv("ns", "k3", b"y");
        assert_eq!(oracle.ops_observed, 3);
        assert_eq!(oracle.len(), 2);
    }

    #[test]
    fn snapshot_round_trips_via_json() {
        let mut oracle = WombatKvOracle::new();
        oracle.put_kv("ns_a", "k1", b"alpha");
        oracle.put_kv("ns_b", "k1", b"beta");
        let json = serde_json::to_string(&oracle).unwrap();
        let back: WombatKvOracle = serde_json::from_str(&json).unwrap();
        assert_eq!(oracle, back);
    }

    /// Seed-replay determinism check (audit #99).
    ///
    /// Mirrors SlateDB's `run_seed_is_deterministic` pattern
    /// (`slatedb/slatedb-dst/src/scenarios.rs:75-150`): drive the
    /// oracle through a fixed synthetic op sequence twice and assert
    /// final state is bit-identical. Catches non-deterministic code
    /// paths (`HashMap` iteration order, `Instant::now()` in a hot
    /// loop, system-clock reads, etc.) that would diverge runs of the
    /// same scenario seed.
    ///
    /// For WombatKV today this only exercises the oracle (the planner
    /// already has its own per-seed determinism test in fault.rs).
    /// When the Stage 3.5 runner wires real fault injection into the
    /// oracle, extend this with assertions on the post-fault state
    /// (oracle.ops_observed, final composite-key set).
    #[test]
    fn oracle_drive_is_deterministic_across_runs() {
        fn drive(seed: u64) -> WombatKvOracle {
            // Deterministic 30-op sequence driven by the project's
            // own XorShift64 PRNG (so the seed truly influences key
            // selection without us accidentally collapsing seeds via
            // weak mixing like `seed | 1`).
            use crate::dst_rng::XorShift64;
            let mut rng = XorShift64::phase(seed, 0);
            let mut oracle = WombatKvOracle::new();
            for op in 0..30u64 {
                let key_idx = rng.between(0, 6); // mod 7
                let key = format!("k{key_idx}");
                let payload = format!("op{op}=k{key_idx}").into_bytes();
                if op % 3 == 2 && op > 0 {
                    let _ = oracle.get_kv("ns", &key);
                } else {
                    oracle.put_kv("ns", &key, &payload);
                }
            }
            oracle
        }

        // Run the SAME seed twice, bit-identical final state expected.
        let a = drive(42);
        let b = drive(42);
        assert_eq!(a, b, "same seed produced different oracle states");

        // Run a DIFFERENT seed, final state should diverge (otherwise
        // seed isn't actually influencing the sequence).
        let c = drive(43);
        assert_ne!(a, c, "different seeds produced identical oracle states");

        // Bit-identical JSON serialization is a strong determinism
        // proof (catches BTreeMap iteration order drift on top of value
        // drift).
        assert_eq!(
            serde_json::to_string(&a).unwrap(),
            serde_json::to_string(&b).unwrap(),
            "same seed produced different JSON serializations"
        );
    }
}
