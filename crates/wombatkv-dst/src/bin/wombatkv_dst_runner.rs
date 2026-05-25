//! wombatkv-dst-runner. Stage 3 of the DST roadmap.
//!
//! Drives a single seeded DST run end-to-end. This Stage 3 binary
//! handles only the planning + reporting half of the lifecycle:
//!
//!   1. Parse `--seed` + `--class`.
//!   2. Derive a deterministic FaultPlan via `schedule_for_class`.
//!   3. Write the plan to `--plan-file` as pretty JSON.
//!   4. Print a one-line summary the calling sweep script can parse.
//!
//! Stage 3.5 (next session) will spawn a child puffer/daemon with
//! `DST_FAULT_PLAN_FILE` + `WMBT_KV_DST_BUGGIFY` set, drive a sequence of
//! KV operations, and report the result against the in-memory
//! oracle (Stage 4).
//!
//! ## Usage
//!
//! ```text
//! wombatkv-dst-runner \
//!     --seed 42 \
//!     --class transient_s3 \
//!     --plan-file /tmp/wombatkv-dst-42-transient_s3.json
//! ```
//!
//! ## Sweep idiom
//!
//! ```sh
//! for seed in $(seq 1 100); do
//!   for class in transient_s3 corrupt_block partial_chain \
//!                concurrent_save daemon_restart sidecar_drift \
//!                foyer_eviction; do
//!     wombatkv-dst-runner --seed $seed --class $class \
//!         --plan-file /tmp/plans/$seed-$class.json
//!   done
//! done
//! ```
//!
//! Each invocation is independent + deterministic; failed seeds
//! re-run with the same seed reproduce the exact plan.

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use wombatkv_dst::oracle::WombatKvOracle;
use wombatkv_dst::{schedule_for_class, WombatKvFailureClass};

#[derive(Parser, Debug)]
#[command(name = "wombatkv-dst-runner")]
#[command(about = "Generate a deterministic fault plan for a single DST scenario", long_about = None)]
struct Args {
    /// Master seed for the run. Same seed + same class → same plan.
    #[arg(long)]
    seed: u64,

    /// Which failure class to exercise.
    #[arg(long, value_enum)]
    class: ClassArg,

    /// Where to write the fault plan JSON.
    #[arg(long, default_value = "/tmp/wombatkv-dst-plan.json")]
    plan_file: PathBuf,

    /// Also print the full plan to stdout (in addition to the
    /// one-line summary).
    #[arg(long)]
    print_plan: bool,

    /// (Stage 3.5, alpha.13+) After writing the plan, drive a
    /// synthetic 20-op sequence against the in-memory oracle and
    /// emit per-trigger-evaluation results.
    #[arg(long)]
    execute: bool,

    /// (Stage 3.5, alpha.13-polish) Spawn the
    /// `wombatkv-dst-runner-child` binary in a subprocess with the
    /// plan loaded, wait for it to drive ops + emit a ChildReport
    /// JSON, then parse the report and print its verdict. Resolves
    /// the child binary via `--child-bin <path>` (defaults to
    /// `./target/release/wombatkv-dst-runner-child` relative to the
    /// parent's cwd).
    #[arg(long)]
    spawn_child: bool,

    /// Path to the child binary (used when `--spawn-child` is set).
    /// Defaults to the workspace's release-build location.
    #[arg(long, default_value = "./target/release/wombatkv-dst-runner-child")]
    child_bin: PathBuf,

    /// Where the child should write its report. When `--spawn-child`
    /// is set, this defaults to `<plan_file>.report.json` so the
    /// parent has a deterministic path to read from.
    #[arg(long)]
    child_report_file: Option<PathBuf>,

    /// Op-count to pass to the child (DST_CHILD_OP_COUNT). Default
    /// 30.
    #[arg(long, default_value_t = 30)]
    child_op_count: u32,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ClassArg {
    TransientS3,
    CorruptBlock,
    PartialChain,
    ConcurrentSave,
    DaemonRestart,
    SidecarDrift,
    FoyerEviction,
    // RFC 0018 Phase 6, transport-layer chaos
    TransportDrop,
    TransportPartialRead,
    TransportSlowWrite,
    // alpha.13+, wire / storage / format / version recovery
    WireEnvelopeCorruption,
    OldSidecarV3,
    OldBlockV1,
    SlateDbWriteFailure,
    SlateDbManifestCorruption,
    MultiEnginePrefixConflict,
    // alpha.14+, daemon startup configuration recovery
    DaemonTpcOnlyNoShmClient,
    // alpha hardening 2026-05-24: OS resource exhaustion
    ResourceFdExhaustion,
    ResourceMemoryPressure,
    ResourceDiskFull,
    // alpha hardening 2026-05-24, cross-platform behavior
    PlatformShmHidden,
    PlatformIoUringJitter,
    PlatformKqueueOnly,
}

impl From<ClassArg> for WombatKvFailureClass {
    fn from(c: ClassArg) -> Self {
        match c {
            ClassArg::TransientS3 => WombatKvFailureClass::TransientS3Failure,
            ClassArg::CorruptBlock => WombatKvFailureClass::CorruptBlockBytes,
            ClassArg::PartialChain => WombatKvFailureClass::PartialChainSave,
            ClassArg::ConcurrentSave => WombatKvFailureClass::ConcurrentSameKeySave,
            ClassArg::DaemonRestart => WombatKvFailureClass::DaemonRestartMidLookup,
            ClassArg::SidecarDrift => WombatKvFailureClass::SidecarDriftAfterChain,
            ClassArg::FoyerEviction => WombatKvFailureClass::FoyerEvictionMidGet,
            ClassArg::TransportDrop => WombatKvFailureClass::TransportConnectionDropMidRPC,
            ClassArg::TransportPartialRead => WombatKvFailureClass::TransportPartialReadOnHeader,
            ClassArg::TransportSlowWrite => WombatKvFailureClass::TransportSlowWrite,
            ClassArg::WireEnvelopeCorruption => WombatKvFailureClass::WireEnvelopeCorruption,
            ClassArg::OldSidecarV3 => WombatKvFailureClass::OldSidecarV3InBucket,
            ClassArg::OldBlockV1 => WombatKvFailureClass::OldBlockV1InBucket,
            ClassArg::SlateDbWriteFailure => WombatKvFailureClass::SlateDbWriteFailure,
            ClassArg::SlateDbManifestCorruption => WombatKvFailureClass::SlateDbManifestCorruption,
            ClassArg::MultiEnginePrefixConflict => WombatKvFailureClass::MultiEnginePrefixConflict,
            ClassArg::DaemonTpcOnlyNoShmClient => WombatKvFailureClass::DaemonTpcOnlyNoShmClient,
            ClassArg::ResourceFdExhaustion => WombatKvFailureClass::ResourceFdExhaustion,
            ClassArg::ResourceMemoryPressure => WombatKvFailureClass::ResourceMemoryPressure,
            ClassArg::ResourceDiskFull => WombatKvFailureClass::ResourceDiskFull,
            ClassArg::PlatformShmHidden => WombatKvFailureClass::PlatformShmHidden,
            ClassArg::PlatformIoUringJitter => WombatKvFailureClass::PlatformIoUringJitter,
            ClassArg::PlatformKqueueOnly => WombatKvFailureClass::PlatformKqueueOnly,
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let class: WombatKvFailureClass = args.class.into();
    let plan = schedule_for_class(args.seed, class);

    if let Some(parent) = args.plan_file.parent() {
        if !parent.as_os_str().is_empty() && !parent.exists() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create dir {}", parent.display()))?;
        }
    }

    let json = serde_json::to_string_pretty(&plan).map_err(|e| anyhow!("serialize plan: {e}"))?;
    std::fs::write(&args.plan_file, &json)
        .with_context(|| format!("write {}", args.plan_file.display()))?;

    if args.print_plan {
        println!("{json}");
    }

    // Sweep-script-parseable summary on one line.
    println!(
        "wombatkv-dst-runner seed={} class={:?} events={} plan={}",
        args.seed,
        class,
        plan.events.len(),
        args.plan_file.display(),
    );

    // (Stage 3.5, alpha.13+) Optionally drive the oracle through
    // a synthetic 20-op KV sequence. This validates that:
    //   - The schedule's events fire (placeholder check; full
    //     fault-injection wiring is next-session work)
    //   - The oracle observes the expected number of ops
    //   - put_kv / get_kv round-trip cleanly under the schedule
    if args.execute {
        execute_synthetic_run(&plan)?;
    }

    if args.spawn_child {
        spawn_child_and_report(&args)?;
    }

    Ok(())
}

/// Spawn `wombatkv-dst-runner-child` with the plan loaded via env,
/// wait, parse + summarize its report. Returns Err if the child
/// exits non-zero (verdict != "pass") or the report can't be parsed.
fn spawn_child_and_report(args: &Args) -> Result<()> {
    let report_path = args
        .child_report_file
        .clone()
        .unwrap_or_else(|| args.plan_file.with_extension("report.json"));
    if !args.child_bin.exists() {
        return Err(anyhow!(
            "child binary not found at {}; build with `cargo build --release -p wombatkv-dst`",
            args.child_bin.display()
        ));
    }
    let mut cmd = std::process::Command::new(&args.child_bin);
    cmd.env("DST_FAULT_PLAN_FILE", &args.plan_file);
    cmd.env("DST_CHILD_REPORT_FILE", &report_path);
    cmd.env("DST_CHILD_OP_COUNT", args.child_op_count.to_string());
    let status = cmd
        .status()
        .with_context(|| format!("spawn child binary at {}", args.child_bin.display()))?;
    let report_json = std::fs::read_to_string(&report_path)
        .with_context(|| format!("read child report from {}", report_path.display()))?;
    let report: serde_json::Value = serde_json::from_str(&report_json)
        .with_context(|| format!("parse child report from {}", report_path.display()))?;
    let verdict = report["verdict"].as_str().unwrap_or("<missing>");
    let suppressed = report["suppressed_writes"].as_u64().unwrap_or(0);
    let divergent = report["divergent_keys"].as_array().map_or(0, std::vec::Vec::len);
    let oracle_n = report["oracle_key_count"].as_u64().unwrap_or(0);
    let store_n = report["store_key_count"].as_u64().unwrap_or(0);
    println!(
        "wombatkv-dst-runner spawn-child: verdict={verdict} suppressed_writes={suppressed} \
         oracle_keys={oracle_n} store_keys={store_n} divergent_keys={divergent} \
         exit_status={} report={}",
        status.code().unwrap_or(-1),
        report_path.display(),
    );
    if verdict != "pass" {
        return Err(anyhow!("child verdict {verdict:?}; divergent_keys={divergent}"));
    }
    Ok(())
}

/// Drive an oracle through a deterministic 20-op KV sequence with
/// oracle-level fault injection (Stage 3.5 partial, alpha.13-polish
/// audit fix #100).
///
/// When a scheduled event triggers at an op, we apply the fault's
/// effect to the oracle BEFORE checking RYW:
///   - S3PutFailure / S3PutLatency → put_kv_failed (no oracle write)
///   - S3GetCorrupt / S3GetFailure → record_observation with Miss
///     after the put (next read would see Miss for the get-side fault)
///   - KillBeforeChainHead / KillBeforeSidecar → put_kv_failed (the
///     put never made it past the chain head; oracle stays at prior state)
///   - All other event variants → counted in `would_fire` but no
///     oracle effect yet (those need real-daemon driver to test;
///     this oracle-level harness can only model the put-side faults).
///
/// Full daemon-level fault injection (where the puffer's REAL S3
/// path returns the fault, not just the oracle's model of it) is
/// the next milestone, needs a daemon-spawning child runner + the
/// dst_buggify hooks wired into wombatkv-store / wombatkv-node.
fn execute_synthetic_run(plan: &wombatkv_dst::FaultPlan) -> Result<()> {
    use wombatkv_dst::oracle::OracleGetOutcome;
    use wombatkv_dst::{FaultTrigger, WombatKvFaultEvent};
    let mut oracle = WombatKvOracle::new();
    let namespace = "dst-synthetic";
    let mut fired = Vec::new();
    let mut suppressed_writes = 0u32;
    for op_n in 0..20u32 {
        let key = format!("k{op_n:02}");
        let payload = format!("v{op_n:02}").into_bytes();

        // Check schedule for events that fire BEFORE this op's put.
        // AfterKvOp { n } means "after the Nth op", so an event with
        // n == op_n+1 fires after THIS op completes, but for the
        // put-side oracle effect we treat the fault as suppressing
        // THIS op's write (simulating "the put never made it
        // durable"). That's a conservative model that catches the
        // typical put-failed recovery pattern.
        let mut suppress_this_write = false;
        for (idx, sched) in plan.events.iter().enumerate() {
            if let FaultTrigger::AfterKvOp { n } = sched.trigger {
                if n == op_n + 1 {
                    fired.push((op_n, idx, format!("{:?}", sched.event)));
                    match &sched.event {
                        WombatKvFaultEvent::S3PutFailure { .. }
                        | WombatKvFaultEvent::S3PutLatency { .. }
                        | WombatKvFaultEvent::KillBeforeChainHead
                        | WombatKvFaultEvent::KillBeforeSidecar => {
                            suppress_this_write = true;
                        }
                        _ => {
                            // Get-side / wire-side / SlateDB-side
                            // faults don't affect the oracle's
                            // put model. Real-daemon driver tests
                            // those paths.
                        }
                    }
                }
            }
        }

        if suppress_this_write {
            oracle.put_kv_failed(namespace, &key);
            suppressed_writes += 1;
            // After a put-failure, RYW does NOT hold for this key -
            // it should miss.
            match oracle.get_kv(namespace, &key) {
                OracleGetOutcome::Miss => { /* expected */ }
                OracleGetOutcome::Hit { .. } => {
                    return Err(anyhow!(
                        "oracle invariant violation: put_kv_failed at op {op_n} \
                         left a Hit instead of Miss"
                    ));
                }
            }
            continue;
        }

        oracle.put_kv(namespace, &key, &payload);
        match oracle.get_kv(namespace, &key) {
            OracleGetOutcome::Hit { payload: bytes } => {
                if bytes != payload {
                    return Err(anyhow!(
                        "oracle RYW violation: op_n={op_n} expected={payload:?} got={bytes:?}"
                    ));
                }
            }
            OracleGetOutcome::Miss => {
                return Err(anyhow!("oracle RYW violation: op_n={op_n} got Miss"));
            }
        }
    }
    println!(
        "wombatkv-dst-runner execute: ops=20 oracle_size={} ops_observed={} \
         events_fired={} suppressed_writes={}",
        oracle.len(),
        oracle.ops_observed,
        fired.len(),
        suppressed_writes,
    );
    for (op_n, idx, ev) in fired {
        println!("  at op {op_n}: event[{idx}] {ev}");
    }
    Ok(())
}
