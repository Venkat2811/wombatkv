#![deny(unsafe_code)]
//! wombatkv-dst-driver. Stage 3.5 of the DST roadmap.
//!
//! Closes the loop the Stage-3 planner left open: spawns a real
//! `wombatkv-daemon` child, drives a seeded sequence of put/get
//! ops against it, compares every result to the `WombatKvOracle`,
//! and reports `Verdict::Match` vs `Verdict::Divergence` counts.
//! Any divergence is a bug surfaced by DST.
//!
//! Lives in `wombatkv-daemon` rather than `wombatkv-dst` so it can
//! use the `RemoteKvStoreClient` without inverting the dep graph
//! (wombatkv-dst is leaf; wombatkv-daemon depends on wombatkv-node
//! and on wombatkv-dst behind the `dst` feature flag).
//!
//! ## Use
//!
//! Pre-req: `wombatkv-daemon` binary built with `--features dst`
//! on `PATH` (or pass `--daemon-bin <path>`). The driver spawns it
//! as a child.
//!
//! ```text
//! cargo build -p wombatkv-daemon --features dst --release
//! ./target/release/wombatkv-dst-driver \
//!     --seed 42 \
//!     --class transient-s3 \
//!     --ops 50
//! ```
//!
//! Output ends in a sweep-parseable summary:
//!
//! ```text
//! wombatkv-dst-driver seed=42 class=TransientS3Failure ops=50 \
//!     matches=50 divergences=0 verdict=PASS
//! ```
//!
//! ## Sweep integration
//!
//! Replace `wombatkv-dst-runner` in `scripts/dst-sweep.sh` with
//! `wombatkv-dst-driver` (or run alongside) to get end-to-end
//! seeded testing rather than just plan-generation.

#[cfg(not(feature = "dst"))]
fn main() {
    eprintln!(
        "wombatkv-dst-driver requires the `dst` cargo feature. \
         Rebuild with: cargo build -p wombatkv-daemon --features dst --bin wombatkv-dst-driver"
    );
    std::process::exit(2);
}

#[cfg(feature = "dst")]
fn main() -> std::process::ExitCode {
    dst_driver::main()
}

#[cfg(feature = "dst")]
mod dst_driver {
    use std::path::PathBuf;
    use std::process::{Child, Command, ExitCode, Stdio};
    use std::time::{Duration, Instant};

    use anyhow::{anyhow, Context, Result};
    use bytes::Bytes;
    use clap::{Parser, ValueEnum};
    use wombatkv_daemon::RemoteKvStoreClient;
    use wombatkv_dst::{
        schedule_for_class, OracleGetOutcome, Verdict, WombatKvFailureClass, WombatKvOracle,
        XorShift64,
    };

    #[derive(Parser, Debug)]
    #[command(
        name = "wombatkv-dst-driver",
        about = "Stage 3.5: spawn child puffer, drive ops, verify against oracle"
    )]
    pub struct Args {
        /// Master seed for the run.
        #[arg(long)]
        pub seed: u64,

        /// Failure class to inject.
        #[arg(long, value_enum)]
        pub class: ClassArg,

        /// Number of put → get ops to drive.
        #[arg(long, default_value_t = 30)]
        pub ops: u32,

        /// Where to write the fault plan JSON for the spawned daemon.
        #[arg(long, default_value = "/tmp/wombatkv-dst-driver-plan.json")]
        pub plan_file: PathBuf,

        /// Daemon binary (must be built with --features dst).
        #[arg(long, default_value = "target/debug/wombatkv-daemon")]
        pub daemon_bin: PathBuf,

        /// SHM prefix for the spawned daemon (short to fit macOS budget).
        #[arg(long, default_value = "dstdrv")]
        pub daemon_prefix: String,

        /// Seconds to wait for the daemon to come up. Polls 100ms.
        #[arg(long, default_value_t = 10)]
        pub daemon_start_timeout_s: u64,

        /// MinIO endpoint for the daemon's object store.
        #[arg(long, default_value = "http://127.0.0.1:9200")]
        pub s3_endpoint: String,

        /// Bucket to use (auto-derived if unset; pin for reproducibility).
        #[arg(long, default_value = "wombatkv-dst-driver")]
        pub s3_bucket: String,

        /// Puffer dir for foyer cache.
        #[arg(long, default_value = "/tmp/wombatkv-dst-driver-puffer")]
        pub puffer_dir: PathBuf,

        /// Print per-op verdicts (verbose).
        #[arg(long)]
        pub verbose: bool,
    }

    #[derive(Copy, Clone, Debug, ValueEnum)]
    pub enum ClassArg {
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
        // wire / storage / format / version recovery
        WireEnvelopeCorruption,
        OldSidecarV3,
        OldBlockV1,
        SlateDbWriteFailure,
        SlateDbManifestCorruption,
        MultiEnginePrefixConflict,
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
                ClassArg::TransportPartialRead => {
                    WombatKvFailureClass::TransportPartialReadOnHeader
                }
                ClassArg::TransportSlowWrite => WombatKvFailureClass::TransportSlowWrite,
                ClassArg::WireEnvelopeCorruption => WombatKvFailureClass::WireEnvelopeCorruption,
                ClassArg::OldSidecarV3 => WombatKvFailureClass::OldSidecarV3InBucket,
                ClassArg::OldBlockV1 => WombatKvFailureClass::OldBlockV1InBucket,
                ClassArg::SlateDbWriteFailure => WombatKvFailureClass::SlateDbWriteFailure,
                ClassArg::SlateDbManifestCorruption => {
                    WombatKvFailureClass::SlateDbManifestCorruption
                }
                ClassArg::MultiEnginePrefixConflict => {
                    WombatKvFailureClass::MultiEnginePrefixConflict
                }
            }
        }
    }

    struct ChildGuard {
        child: Child,
        prefix: String,
    }

    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            // Best-effort cleanup of any lingering SHM segments
            // (the daemon's own cleanup_prefix_segments handles the
            // happy path; this catches force-kill scenarios).
            wombatkv_daemon::cleanup_prefix_segments(&self.prefix);
        }
    }

    fn spawn_daemon(args: &Args) -> Result<ChildGuard> {
        // Clean up any prior state for this prefix.
        wombatkv_daemon::cleanup_prefix_segments(&args.daemon_prefix);
        std::fs::create_dir_all(&args.puffer_dir)?;

        let child = Command::new(&args.daemon_bin)
            .arg("--prefix")
            .arg(&args.daemon_prefix)
            .env("WMBT_KV_S3_ENDPOINT", &args.s3_endpoint)
            .env("WMBT_KV_BUCKET", &args.s3_bucket)
            .env("WMBT_KV_PUFFER_DIR", args.puffer_dir.as_os_str())
            .env("WMBT_KV_DST_BUGGIFY", "1")
            .env("WMBT_KV_DST_BUGGIFY_SEED", args.seed.to_string())
            .env("DST_FAULT_PLAN_FILE", &args.plan_file)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("failed to spawn {}", args.daemon_bin.display()))?;
        Ok(ChildGuard { child, prefix: args.daemon_prefix.clone() })
    }

    fn connect_with_retry(prefix: &str, timeout: Duration) -> Result<RemoteKvStoreClient> {
        let deadline = Instant::now() + timeout;
        let mut last_err = None;
        while Instant::now() < deadline {
            match RemoteKvStoreClient::connect(prefix) {
                Ok(c) => return Ok(c),
                Err(e) => {
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(150));
                }
            }
        }
        Err(anyhow!(
            "daemon connect timed out after {:?}; last error: {}",
            timeout,
            last_err.map(|e| e.to_string()).unwrap_or_else(|| "(none captured)".to_string())
        ))
    }

    pub fn main() -> ExitCode {
        let args = Args::parse();
        match run(&args) {
            Ok(report) => {
                println!(
                    "wombatkv-dst-driver seed={} class={:?} ops={} matches={} divergences={} verdict={}",
                    args.seed,
                    WombatKvFailureClass::from(args.class),
                    report.ops_run,
                    report.matches,
                    report.divergences.len(),
                    if report.divergences.is_empty() {
                        "PASS"
                    } else {
                        "FAIL"
                    },
                );
                if !report.divergences.is_empty() {
                    for d in &report.divergences {
                        println!("  divergence: {d}");
                    }
                    return ExitCode::FAILURE;
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("driver error: {e:#}");
                ExitCode::FAILURE
            }
        }
    }

    struct Report {
        ops_run: u32,
        matches: u32,
        divergences: Vec<String>,
    }

    fn run(args: &Args) -> Result<Report> {
        // 0) Fail loud at driver start when the prefix would push the
        // daemon's SHM segment names past the macOS portable budget.
        // Without this the driver would retry attach for the full
        // `daemon_start_timeout_s` (default 10s) and surface a cryptic
        // ENOENT / ENAMETOOLONG from the bottom of the SHM stack.
        // Same `validate_segment_name_budget` the daemon calls; we
        // pull it forward to the driver entry-point so the operator
        // sees the actionable error in seconds, not after the
        // attach-retry loop drains its budget.
        wombatkv_daemon::validate_segment_name_budget(&args.daemon_prefix)
            .map_err(|e| anyhow!("dst-driver: prefix rejected: {e}"))?;

        // 1) Write the deterministic fault plan to the file the daemon
        // will read via DST_FAULT_PLAN_FILE.
        let plan = schedule_for_class(args.seed, args.class.into());
        std::fs::write(&args.plan_file, serde_json::to_string_pretty(&plan)?)?;
        if args.verbose {
            eprintln!("wrote plan: {} ({} events)", args.plan_file.display(), plan.events.len());
        }

        // 2) Spawn the daemon child with DST env wired.
        let _guard = spawn_daemon(args)?;
        if args.verbose {
            eprintln!("spawned daemon, waiting for SHM attach...");
        }

        // 3) Connect, retrying until daemon is ready or timeout.
        let client = connect_with_retry(
            &args.daemon_prefix,
            Duration::from_secs(args.daemon_start_timeout_s),
        )?;
        if args.verbose {
            eprintln!("client connected, driving {} ops", args.ops);
        }

        // 4) Seed-derived op stream (phase 2 reserved for payloads).
        let mut payload_rng = XorShift64::phase(args.seed, 2);
        // phase 3 = key choice (some ops re-use prior keys for get-after-put)
        let mut key_rng = XorShift64::phase(args.seed, 3);

        let namespace = "dst-driver";
        let mut oracle = WombatKvOracle::new();
        let mut report = Report { ops_run: 0, matches: 0, divergences: Vec::new() };

        let mut prior_keys: Vec<String> = Vec::new();

        for op_idx in 0..args.ops {
            // 50/50 split: put a new key, OR get a prior key.
            let do_get = !prior_keys.is_empty() && (key_rng.next_u32(100) < 50);

            if do_get {
                let key_idx = key_rng.next_u32(u32::try_from(prior_keys.len()).unwrap_or(1));
                let key = prior_keys[key_idx as usize].clone();
                let observed = match client.get_kv(namespace, &key) {
                    Ok(wombatkv_daemon::RemoteGetOutcome::Hit { payload, .. }) => {
                        OracleGetOutcome::Hit { payload: payload.to_vec() }
                    }
                    Ok(wombatkv_daemon::RemoteGetOutcome::Miss) => OracleGetOutcome::Miss,
                    Err(e) => {
                        report.divergences.push(format!("op {op_idx}: get_kv({key}) error: {e}"));
                        continue;
                    }
                };
                let verdict = Verdict::check_get(&oracle, namespace, &key, observed);
                match verdict {
                    Verdict::Match => report.matches += 1,
                    Verdict::Divergence { expected, observed: obs, .. } => {
                        report.divergences.push(format!(
                            "op {op_idx}: get_kv({key}) expected={:?} observed={:?}",
                            describe_outcome(&expected),
                            describe_outcome(&obs),
                        ));
                    }
                }
            } else {
                let key = format!("k{:08x}", payload_rng.next_u64());
                let len = (payload_rng.next_u32(4096) + 128) as usize;
                let payload = vec![payload_rng.next_u32(256) as u8; len];
                match client.put_kv(namespace, &key, Bytes::from(payload.clone())) {
                    Ok(()) => {
                        oracle.put_kv(namespace, &key, &payload);
                        prior_keys.push(key);
                        report.matches += 1;
                    }
                    Err(e) => {
                        // PUT failure may be DST-injected; the oracle
                        // records put_kv_failed so subsequent GETs see
                        // Miss. This matches the contract.
                        oracle.put_kv_failed(namespace, &key);
                        if args.verbose {
                            eprintln!("op {op_idx}: put_kv({key}) failed: {e} (oracle recorded as not-committed)");
                        }
                        report.matches += 1;
                    }
                }
            }
            report.ops_run += 1;
        }

        Ok(report)
    }

    fn describe_outcome(o: &OracleGetOutcome) -> String {
        match o {
            OracleGetOutcome::Hit { payload } => format!("Hit({} bytes)", payload.len()),
            OracleGetOutcome::Miss => "Miss".to_string(),
        }
    }
}
