//! wombatkv-dst-runner-child: DST Stage 3.5 cross-process executor.
//!
//! Spawned by the parent `wombatkv-dst-runner` with the loaded plan
//! exposed via env. This child:
//!
//!   1. Loads the FaultPlan from `DST_FAULT_PLAN_FILE`.
//!   2. Initializes the in-memory `WombatKvOracle`.
//!   3. Drives a deterministic 30-op put/get sequence against
//!      BOTH the oracle AND a real (S3-faked) state store.
//!   4. At each op, consults the fault plan: if a put-class event
//!      triggers, the real-side write is SUPPRESSED to mimic what
//!      the production code would do if `dst_buggify!()` fired.
//!   5. Compares the final state of both sides. Divergence = fail.
//!   6. Writes a structured `ChildReport` JSON to
//!      `DST_CHILD_REPORT_FILE` (or stdout if unset).
//!
//! ## What this is NOT (yet)
//!
//! - We do NOT spawn a real `wombatkv-daemon` process and route ops
//!   over SHM/TCP/HTTP. That's the next iteration, needs the daemon
//!   binary plumbed with the `--dst-plan <path>` flag + buggify hooks
//!   wired into the production-code fault sites.
//! - We do NOT exercise the `WombatKVKvStore` (the real foyer + S3
//!   path) yet. The "real-side store" here is a stand-in `BTreeMap`
//!   that models put-failure / put-success, that's enough to prove
//!   the oracle-divergence detection wiring works end-to-end.
//! - When the real daemon-driven path lands, this child binary
//!   becomes thin orchestration around it; the oracle + divergence
//!   logic stays the same.
//!
//! ## Env contract
//!
//! - `DST_FAULT_PLAN_FILE` (required), path to a FaultPlan JSON.
//! - `DST_CHILD_REPORT_FILE` (optional), where to write the report.
//!   Defaults to stdout.
//! - `DST_CHILD_OP_COUNT` (optional, default 30), number of ops
//!   the child drives.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use wombatkv_dst::dst_plan;
use wombatkv_dst::oracle::{OracleGetOutcome, WombatKvOracle};
use wombatkv_dst::{FaultPlan, FaultTrigger, WombatKvFaultEvent};

/// Structured report the child writes after a run. The parent runner
/// reads + diffs this against expectations.
#[derive(Debug, Serialize, Deserialize)]
struct ChildReport {
    /// Seed + class from the loaded plan (echoed for parent
    /// correlation).
    plan_seed: u64,
    plan_class: String,
    plan_event_count: usize,

    /// How many ops we actually drove.
    ops_total: u32,
    /// How many put-class faults fired (and which side was suppressed).
    suppressed_writes: u32,
    /// Per-fired-event log: `(op_n, event_idx, event_debug)`.
    fired_events: Vec<FiredEvent>,

    /// Final state on both sides, empty if matched, populated keys
    /// if divergent.
    oracle_key_count: usize,
    store_key_count: usize,
    divergent_keys: Vec<String>,

    /// Verdict: "pass" / "diverged" / "errored".
    verdict: String,
    /// Optional error message when verdict != "pass".
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FiredEvent {
    op_n: u32,
    event_idx: usize,
    event_debug: String,
    action: String,
}

/// Suppress conditions for the put-side fault model. Mirrors the
/// classification in `wombatkv_dst_runner::execute_synthetic_run`.
fn is_put_suppressing(event: &WombatKvFaultEvent) -> bool {
    matches!(
        event,
        WombatKvFaultEvent::S3PutFailure { .. }
            | WombatKvFaultEvent::S3PutLatency { .. }
            | WombatKvFaultEvent::KillBeforeChainHead
            | WombatKvFaultEvent::KillBeforeSidecar
    )
}

fn main() -> Result<()> {
    let plan_path: PathBuf = std::env::var("DST_FAULT_PLAN_FILE")
        .map_err(|_| anyhow!("DST_FAULT_PLAN_FILE not set"))?
        .into();
    let report_path: Option<PathBuf> =
        std::env::var("DST_CHILD_REPORT_FILE").ok().map(PathBuf::from);
    let op_count: u32 =
        std::env::var("DST_CHILD_OP_COUNT").ok().and_then(|s| s.parse().ok()).unwrap_or(30);

    let plan_json = std::fs::read_to_string(&plan_path)
        .with_context(|| format!("read {}", plan_path.display()))?;
    let plan: FaultPlan = serde_json::from_str(&plan_json)
        .with_context(|| format!("parse FaultPlan from {}", plan_path.display()))?;

    let report = run(&plan, op_count);
    let report_json = serde_json::to_string_pretty(&report)?;
    if let Some(p) = report_path.as_ref() {
        std::fs::write(p, &report_json)
            .with_context(|| format!("write report to {}", p.display()))?;
    } else {
        println!("{report_json}");
    }

    // Exit code reflects verdict so the parent can check via wait.
    match report.verdict.as_str() {
        "pass" => Ok(()),
        _ => Err(anyhow!("dst-runner-child verdict: {}", report.verdict)),
    }
}

fn run(plan: &FaultPlan, op_count: u32) -> ChildReport {
    let namespace = "dst-child";
    let mut oracle = WombatKvOracle::new();
    // Stand-in for the real wombatkv-node KvStore. The dst_plan
    // primitive is the SAME one the production embed::put_kv consults
    // (via `dst_plan::is_put_suppressed()` under #[cfg(feature = "dst")]),
    // so when we plug the real WombatKVKvStore in here the suppression
    // logic stays unchanged, production code does the suppression
    // itself by returning Err, we just mirror to oracle.
    let mut store: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut fired: Vec<FiredEvent> = Vec::new();
    let mut suppressed = 0u32;

    // Install the plan in the global dst_plan state. Production code
    // paths that have `#[cfg(feature = "dst")] dst_plan::is_put_suppressed()`
    // checks now see this plan's events.
    dst_plan::set_plan(plan.clone());

    for op_n in 0..op_count {
        let key = format!("k{op_n:02}");
        let payload = format!("v{op_n:02}").into_bytes();

        // Look up any events at this op for the report, separate from
        // the fire decision, which goes through dst_plan.
        for (idx, sched) in plan.events.iter().enumerate() {
            if let FaultTrigger::AfterKvOp { n } = sched.trigger {
                if n == op_n + 1 {
                    let suppressing = is_put_suppressing(&sched.event);
                    fired.push(FiredEvent {
                        op_n,
                        event_idx: idx,
                        event_debug: format!("{:?}", sched.event),
                        action: if suppressing { "suppress_put" } else { "noop_logged" }
                            .to_string(),
                    });
                }
            }
        }

        // Consult dst_plan, same call production code would make.
        let suppress = dst_plan::is_put_suppressed();

        if suppress {
            suppressed += 1;
            oracle.put_kv_failed(namespace, &key);
            // store: the put is suppressed → no insert (mirroring what
            // production put_kv does when it returns Err from
            // the cfg(feature = "dst") dst_plan check).
        } else {
            oracle.put_kv(namespace, &key, &payload);
            store.insert(key.clone(), payload.clone());
        }

        // Advance op counter AFTER the op completes (so the next op
        // consults the new value).
        dst_plan::advance_op();
    }

    // Clean up so subsequent runs in the same process start fresh.
    dst_plan::clear();

    // Divergence detection: oracle.store and `store` should agree on
    // composite-key contents. Oracle uses "ns\u{1}k" composite; we
    // strip the namespace prefix for the comparison.
    let oracle_keys: BTreeMap<String, Vec<u8>> = oracle
        .keys()
        .into_iter()
        .filter_map(|(ns, k)| {
            if ns == namespace {
                match oracle.get_kv(&ns, &k) {
                    OracleGetOutcome::Hit { payload } => Some((k, payload)),
                    OracleGetOutcome::Miss => None,
                }
            } else {
                None
            }
        })
        .collect();

    let mut divergent: Vec<String> = Vec::new();
    for (k, v) in &oracle_keys {
        match store.get(k) {
            Some(sv) if sv == v => {}
            _ => divergent.push(format!("oracle has k={k:?}, store mismatch")),
        }
    }
    for k in store.keys() {
        if !oracle_keys.contains_key(k) {
            divergent.push(format!("store has k={k:?}, oracle missing"));
        }
    }

    let verdict = if divergent.is_empty() { "pass" } else { "diverged" };
    ChildReport {
        plan_seed: plan.seed,
        plan_class: plan.class.map(|c| format!("{c:?}")).unwrap_or_default(),
        plan_event_count: plan.events.len(),
        ops_total: op_count,
        suppressed_writes: suppressed,
        fired_events: fired,
        oracle_key_count: oracle_keys.len(),
        store_key_count: store.len(),
        divergent_keys: divergent,
        verdict: verdict.to_string(),
        error: None,
    }
}
