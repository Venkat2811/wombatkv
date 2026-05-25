//! Deterministic simulation testing (DST) primitives for WombatKV.
//!
//! Stage 0: port the BUGGIFY / AssertionLog / seeded RNG primitives
//! from `the prior DST harness` (which themselves originate from the
//! `myelon-playground/crates/dst-fixtures` source-of-truth). These
//! are domain-agnostic; the only thing per-project is which call
//! sites get the BUGGIFY macro and which invariants get asserted.
//!
//! ## Usage in WombatKV application code
//!
//! Gate the chaos branch on a `dst` feature flag on the crate so
//! the dead code only compiles in DST builds:
//!
//! ```ignore
//! #[cfg(feature = "dst")]
//! if wombatkv_dst::dst_buggify!() {
//!     // chaos action: yield, sleep, return early Err, etc.
//! }
//!
//! wombatkv_dst::assert_always(
//!     manifest.version > 0 || store.is_empty(),
//!     "post-bootstrap store has version OR is empty",
//!     "RFC 0011 stage0 invariant",
//! );
//! ```
//!
//! ## Stage roadmap
//!
//! - **Stage 0 (this crate today):** primitives, assertions,
//!   buggify, RNG. Crate builds standalone; no wombatkv-* dep yet.
//! - **Stage 1:** wire `dst_buggify!()` call sites into
//!   `wombatkv-node` (chain save/load, foyer cache, block prefetch)
//!   and `wombatkv-daemon` (request handler, arena alloc) behind a
//!   `dst` feature flag.
//! - **Stage 2:** define WombatKVFailureClass (`fault` module) and
//!   the trigger schedule keyed by KV op count / S3 op count / time.
//! - **Stage 3:** `wombatkv_dst_runner` binary, drives a single
//!   seeded DST run end-to-end, comparing observed state against
//!   an in-memory oracle (Stage 4).
//! - **Stage 4:** oracle, reference in-process implementation of
//!   the WombatKV semantics that real implementations are checked
//!   against. Catches divergence between embedded-mode and
//!   daemon-mode behavior.
//! - **Stage 5:** seed sweep scripts + CI integration.

pub mod dst_assertions;
pub mod dst_buggify;
pub mod dst_plan;
pub mod dst_rng;
pub mod fault;
pub mod oracle;

pub use dst_assertions::{
    assert_always, assert_reachable, assert_sometimes, assert_unreachable, reset_global_assertions,
    snapshot_global_assertions, AssertionKind, AssertionLog, AssertionViolation,
};
pub use dst_buggify::{buggify, reset as reset_buggify_counters, BuggifyConfig, ScopedBuggify};
pub use dst_rng::XorShift64;
pub use fault::{
    schedule_for_class, FaultPlan, FaultTrigger, S3ErrorKind, ScheduledFault, WombatKvFailureClass,
    WombatKvFaultEvent,
};
pub use oracle::{OracleGetOutcome, Verdict, WombatKvOracle};
