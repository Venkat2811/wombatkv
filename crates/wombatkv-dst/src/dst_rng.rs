//! Seeded RNG for deterministic simulation.
//!
//! Slim port of myelon's seeded-cascade pattern. The full myelon
//! `dst_runtime` carries Disruptor-orchestration state we don't need;
//! WombatKV only needs the RNG primitive so application code can
//! sample from a deterministic source given a seed.
//!
//! Cascade pattern (per DST RFC §3.5): a single u64 seed cascades
//! through XOR phases to derive different deterministic streams:
//!   phase 0 → config (shard count, query count, top_k, nprobe)
//!   phase 1 → fault schedule
//!   phase 2 → payload bytes
//!   phase 3 → spawn order, etc.
//!
//! Each phase uses the same XorShift64 algorithm with the seed XORed
//! by a phase-specific constant. Replay with the same seed produces
//! identical sequences of u64 values.

#[derive(Debug, Clone, Copy)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    /// Construct from a seed. `seed = 0` is reserved (XorShift64
    /// degenerates); we substitute a non-zero value. Phase XOR-keys
    /// are the caller's responsibility, see RFC §3.5.
    pub fn new(seed: u64) -> Self {
        Self { state: if seed == 0 { 0xdead_beef_cafe_babe } else { seed } }
    }

    /// Phase-keyed constructor. Convenience for the seed-cascade
    /// pattern: `XorShift64::phase(master_seed, 1)` produces a stream
    /// independent of `XorShift64::phase(master_seed, 0)` while still
    /// being a deterministic function of `master_seed`.
    pub fn phase(seed: u64, phase: u64) -> Self {
        // Mix the master with the phase via the same constant pattern
        // that BUGGIFY uses for callsite hashing. This keeps the
        // streams uncorrelated even with adjacent phase numbers.
        let mixed = seed ^ phase.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        Self::new(mixed)
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    pub fn next_u32(&mut self, bound: u32) -> u32 {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % u64::from(bound)) as u32
    }

    /// Sample a `u64` in `[lo, hi)`. Caller must ensure `hi > lo`.
    pub fn between(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(hi > lo);
        let span = hi - lo;
        lo + (self.next_u64() % span)
    }

    /// Log-uniform sample in `[lo, hi]`. Useful for seed-derived
    /// configurations (queue depths, payload sizes) where uniform-on-
    /// log gives more interesting variety than uniform-on-linear.
    pub fn log_uniform(&mut self, lo: u64, hi: u64) -> u64 {
        debug_assert!(lo > 0 && hi >= lo);
        let lo_log = (lo as f64).ln();
        let hi_log = (hi as f64).ln();
        let frac = (self.next_u64() as f64) / (u64::MAX as f64);
        let v = (lo_log + frac * (hi_log - lo_log)).exp();
        (v as u64).clamp(lo, hi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_with_seed() {
        let mut a = XorShift64::new(42);
        let mut b = XorShift64::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = XorShift64::new(42);
        let mut b = XorShift64::new(43);
        let mut diff = 0;
        for _ in 0..100 {
            if a.next_u64() != b.next_u64() {
                diff += 1;
            }
        }
        assert!(diff > 90, "expected >90% divergence, got {diff}");
    }

    #[test]
    fn phase_streams_are_independent() {
        let mut a = XorShift64::phase(42, 0);
        let mut b = XorShift64::phase(42, 1);
        let mut diff = 0;
        for _ in 0..100 {
            if a.next_u64() != b.next_u64() {
                diff += 1;
            }
        }
        assert!(diff > 90, "phase streams should diverge: got {diff}");
    }

    #[test]
    fn between_is_in_range() {
        let mut rng = XorShift64::new(7);
        for _ in 0..1000 {
            let v = rng.between(10, 20);
            assert!((10..20).contains(&v));
        }
    }

    #[test]
    fn log_uniform_in_bounds() {
        let mut rng = XorShift64::new(7);
        for _ in 0..100 {
            let v = rng.log_uniform(256, 131_072);
            assert!((256..=131_072).contains(&v));
        }
    }

    #[test]
    fn seed_zero_is_replaced() {
        // XorShift64 with seed 0 would emit all zeros; check we hot-fix it.
        let mut rng = XorShift64::new(0);
        let v = rng.next_u64();
        assert_ne!(v, 0);
    }
}
