#![deny(unsafe_code)]
//! Side-by-side embedded vs daemon perf bench.
//!
//! Phase 1: build `WombatKVKvStore` in-process, run full PUT+GET
//! workload across payload sizes, report latencies.
//!
//! Phase 2: spawn `wombatkv-daemon` as a child, connect a
//! `RemoteKvStoreClient`, run the same workload, report latencies.
//!
//! Output is a side-by-side table so the IPC overhead of the daemon path is visible
//! at each payload size. PUT numbers are dominated by S3 writethrough
//! and are reported for completeness; GET numbers (foyer hits) are the
//! relevant comparison for the IPC cost.

use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;

use wombatkv_daemon::{RemoteGetOutcome, RemoteKvStoreClient, DEFAULT_RING_DEPTH};

use wombatkv_node::embed::{EmbedConfig, GetOutcome, WombatKVKvStore};
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

const BENCH_THREAD_STACK_BYTES: usize = 32 * 1024 * 1024;
const SIZES_KIB: &[usize] = &[4, 64, 256, 1024, 1536];
const NS_EMBEDDED: &str = "embedded-bench";
const NS_DAEMON: &str = "daemon-bench";

fn main() -> ExitCode {
    let join = thread::Builder::new()
        .name("wombatkv-emb-vs-dmn-bench".to_string())
        .stack_size(BENCH_THREAD_STACK_BYTES)
        .spawn(run_bench)
        .expect("spawn bench worker");
    if let Ok(code) = join.join() {
        code
    } else {
        eprintln!("bench worker panicked");
        ExitCode::FAILURE
    }
}

#[derive(Debug, Clone, Default)]
struct Stage {
    label: String,
    payload_bytes: usize,
    samples_us: Vec<f64>,
}

impl Stage {
    fn percentile(&self, q: f64) -> f64 {
        if self.samples_us.is_empty() {
            return 0.0;
        }
        let mut sorted = self.samples_us.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let idx = ((sorted.len() as f64 - 1.0) * q).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }
    fn p50(&self) -> f64 {
        self.percentile(0.50)
    }
    fn p99(&self) -> f64 {
        self.percentile(0.99)
    }
    fn ops_per_s(&self) -> f64 {
        let total_us: f64 = self.samples_us.iter().sum();
        if total_us > 0.0 {
            (self.samples_us.len() as f64) / (total_us / 1_000_000.0)
        } else {
            0.0
        }
    }
    fn mb_per_s(&self) -> f64 {
        let total_us: f64 = self.samples_us.iter().sum();
        if total_us > 0.0 && self.payload_bytes > 0 {
            ((self.samples_us.len() as f64) * (self.payload_bytes as f64))
                / (1024.0 * 1024.0)
                / (total_us / 1_000_000.0)
        } else {
            0.0
        }
    }
}

fn run_bench() -> ExitCode {
    let embedded = match build_embedded_store() {
        Ok(s) => s,
        Err(err) => {
            eprintln!("build embedded store: {err}");
            return ExitCode::FAILURE;
        }
    };

    println!("\n== Embedded (in-process WombatKVKvStore) ==");
    let embedded_stages = run_workload_embedded(&embedded);

    drop(embedded);

    println!("\n== Daemon (SHM via wombatkv-daemon) ==");
    let prefix = run_tag();
    let bin = if let Some(p) = daemon_bin() {
        p
    } else {
        eprintln!("daemon binary not found; daemon phase skipped");
        print_summary(&embedded_stages, &[]);
        return ExitCode::SUCCESS;
    };

    // Spawn child daemon; one prefix is enough for this bench.
    let child = match Command::new(&bin)
        .arg("--prefix")
        .arg(&prefix)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(c) => c,
        Err(err) => {
            eprintln!("spawn daemon: {err}");
            return ExitCode::FAILURE;
        }
    };
    let mut guard = ChildGuard(Some(child));

    // Wait for the daemon's segments to exist by attempting attach.
    let client = match wait_connect_remote(&prefix) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("connect remote: {err}");
            let _ = guard.0.as_mut().unwrap().kill();
            return ExitCode::FAILURE;
        }
    };

    let daemon_stages = run_workload_daemon(&client);

    drop(client);
    let _ = guard.0.as_mut().unwrap().kill();
    let _ = guard.0.as_mut().unwrap().wait();

    print_summary(&embedded_stages, &daemon_stages);
    ExitCode::SUCCESS
}

fn run_workload_embedded(store: &WombatKVKvStore<S3ObjectStore>) -> Vec<Stage> {
    let mut out = Vec::new();
    // Warmup: a few cheap PUTs so foyer is hot and S3 connection is open.
    for i in 0..5 {
        let _ = store.put_kv(NS_EMBEDDED, &format!("warmup-{i}"), Bytes::from(vec![0u8; 4096]));
    }

    for &kib in SIZES_KIB {
        let payload = vec![0xCDu8; kib * 1024];
        let n = if kib >= 256 { 50 } else { 200 };

        // PUT
        let mut put = Stage {
            label: format!("put_{kib}KiB"),
            payload_bytes: payload.len(),
            samples_us: Vec::with_capacity(n),
        };
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let t0 = Instant::now();
            store.put_kv(NS_EMBEDDED, &key, Bytes::from(payload.clone())).expect("embedded put");
            put.samples_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        emit_stage(&put);
        out.push(put);

        // GET (warm, foyer hits)
        let mut get = Stage {
            label: format!("get_{kib}KiB"),
            payload_bytes: payload.len(),
            samples_us: Vec::with_capacity(n),
        };
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let t0 = Instant::now();
            match store.get_kv(NS_EMBEDDED, &key).expect("embedded get") {
                GetOutcome::Hit { payload: p, .. } => {
                    assert_eq!(p.len(), payload.len());
                }
                GetOutcome::Miss => panic!("embedded unexpected miss"),
            }
            get.samples_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        emit_stage(&get);
        out.push(get);
    }
    out
}

fn run_workload_daemon(client: &RemoteKvStoreClient) -> Vec<Stage> {
    let mut out = Vec::new();

    // Warmup: PING + 5 small PUTs so the daemon foyer + S3 are warm.
    for _ in 0..5 {
        client.ping().expect("warmup ping");
    }
    for i in 0..5 {
        client
            .put_kv(NS_DAEMON, &format!("warmup-{i}"), Bytes::from(vec![0u8; 4096]))
            .expect("warmup put");
    }

    // Add a PING_RTT stage (control-plane only, no payload).
    let ping_n = 500_usize;
    let mut ping = Stage {
        label: "ping_rtt".to_string(),
        payload_bytes: 0,
        samples_us: Vec::with_capacity(ping_n),
    };
    for _ in 0..ping_n {
        let t0 = Instant::now();
        client.ping().expect("ping");
        ping.samples_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
    }
    emit_stage(&ping);
    out.push(ping);

    for &kib in SIZES_KIB {
        let payload = vec![0xCDu8; kib * 1024];
        let n = if kib >= 256 { 50 } else { 200 };

        // PUT
        let mut put = Stage {
            label: format!("put_{kib}KiB"),
            payload_bytes: payload.len(),
            samples_us: Vec::with_capacity(n),
        };
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let t0 = Instant::now();
            client.put_kv(NS_DAEMON, &key, Bytes::from(payload.clone())).expect("daemon put");
            put.samples_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        emit_stage(&put);
        out.push(put);

        // GET (foyer hits, daemon side)
        let mut get = Stage {
            label: format!("get_{kib}KiB"),
            payload_bytes: payload.len(),
            samples_us: Vec::with_capacity(n),
        };
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let t0 = Instant::now();
            match client.get_kv(NS_DAEMON, &key).expect("daemon get") {
                RemoteGetOutcome::Hit { payload: p, .. } => {
                    assert_eq!(p.len(), payload.len());
                }
                RemoteGetOutcome::Miss => panic!("m1 unexpected miss"),
            }
            get.samples_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
        }
        emit_stage(&get);
        out.push(get);
    }
    out
}

fn emit_stage(s: &Stage) {
    let p50 = s.p50();
    let p99 = s.p99();
    let ops = s.ops_per_s();
    let mbps = s.mb_per_s();
    println!(
        "  {:>15}  count={:>4}  p50={:>10.2} µs  p99={:>10.2} µs  ops/s={:>9.0}  MB/s={:>8.2}",
        s.label,
        s.samples_us.len(),
        p50,
        p99,
        ops,
        mbps,
    );
}

fn print_summary(embedded: &[Stage], m1: &[Stage]) {
    println!("\n== Summary (p50 µs, embedded vs daemon) ==");
    println!("  {:<14} | {:>10}  {:>10}  {:>10}", "stage", "emb p50 µs", "dmn p50 µs", "Δ µs");
    println!("  {}", "-".repeat(54));
    let m0_map: std::collections::HashMap<String, f64> =
        embedded.iter().map(|s| (s.label.clone(), s.p50())).collect();
    for s in m1 {
        if let Some(m0_p50) = m0_map.get(&s.label) {
            let m1_p50 = s.p50();
            println!(
                "  {:<14} | {:>10.2}  {:>10.2}  {:>10.2}",
                s.label,
                m0_p50,
                m1_p50,
                m1_p50 - m0_p50,
            );
        } else {
            // Stages only present in daemon mode (e.g. ping_rtt) get a marker.
            println!("  {:<14} | {:>10}  {:>10.2}  {:>10}", s.label, "-", s.p50(), "-");
        }
    }
}

fn build_embedded_store() -> Result<WombatKVKvStore<S3ObjectStore>, String> {
    let s3_cfg = S3ObjectStoreConfig::from_env().map_err(|e| format!("{e:?}"))?;
    let s3 = S3ObjectStore::new(s3_cfg).map_err(|e| format!("{e:?}"))?;
    s3.ensure_bucket().map_err(|e| format!("{e:?}"))?;

    let mut foyer = FoyerCacheConfig::default();
    foyer.ssd_dir = std::env::var("WMBT_KV_PUFFER_DIR")
        .map_or_else(|_| PathBuf::from("/tmp/wombatkv-embedded-vs-m1-bench-foyer"), PathBuf::from);
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_RAM_BYTES") {
        if let Ok(p) = value.parse::<u64>() {
            foyer.ram_bytes = p;
        }
    }
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_DISK_BYTES") {
        if let Ok(p) = value.parse::<u64>() {
            foyer.ssd_bytes = p;
        }
    }
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_BLOCK_SIZE_BYTES") {
        if let Ok(p) = value.parse::<usize>() {
            foyer.block_size = p;
        }
    }
    foyer.iouring = false;

    let s3_prefix =
        std::env::var("WMBT_KV_S3_PREFIX").unwrap_or_else(|_| "kv/embedded-vs-m1-bench".into());
    let cfg = EmbedConfig {
        s3_prefix,
        foyer,
        write_through_s3: true,
        compression: wombatkv_node::compression::BlockCompressionConfig::from_env(),
    };
    Arc::new(()); // (no-op; here just to anchor the import order)
    WombatKVKvStore::new(cfg, s3).map_err(|e| format!("{e}"))
}

fn wait_connect_remote(prefix: &str) -> Result<RemoteKvStoreClient, String> {
    // `connect_with_depth` already loops with a 30 s deadline and uses
    // POSIX `shm_open`, which is portable across Linux + macOS. No
    // platform-specific probe needed.
    RemoteKvStoreClient::connect_with_depth(prefix, DEFAULT_RING_DEPTH)
        .map_err(|err| format!("{err}"))
}

fn daemon_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("WMBT_KV_DAEMON_SHM_DAEMON_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let exe = std::env::current_exe().ok()?;
    let mut dir = exe.clone();
    while let Some(parent) = dir.parent() {
        let cand = parent.join("wombatkv-daemon");
        if cand.is_file() {
            return Some(cand);
        }
        dir = parent.to_path_buf();
    }
    None
}

fn run_tag() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let s = format!("{nanos:x}");
    let n = s.len();
    format!("b{}", &s[n.saturating_sub(6)..])
}

struct ChildGuard(Option<Child>);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
            // give myelon a moment to release SHM segments
            thread::sleep(Duration::from_millis(50));
        }
    }
}
