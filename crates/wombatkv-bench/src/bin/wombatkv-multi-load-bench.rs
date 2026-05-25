#![deny(unsafe_code)]
//! wombatkv-multi-load-bench, concurrent multi-client load harness.
//!
//! Spawns ONE `wombatkv-daemon` child with N prefixes (`--prefix p0
//! --prefix p1 ...`), then spawns N client threads each connecting to
//! its dedicated prefix. All clients share the same foyer cache + the
//! same S3 bucket on the daemon side, so this is "multi-client load
//! against one shared backend", the load story the single-client
//! `wombatkv-load-bench` couldn't tell.
//!
//! Reports aggregated PUT + GET latency percentiles + per-client
//! throughput. Identifies contention by comparing per-client tails
//! against the single-client baseline.
//!
//! ## Use
//!
//! Pre-req: MinIO running on `127.0.0.1:9200` (or wherever).
//!
//! ```text
//! wombatkv-multi-load-bench \
//!     --clients 4 \
//!     --ops 500 \
//!     --payload-bytes 4096 \
//!     --s3-endpoint http://127.0.0.1:9200
//! ```
//!
//! Output ends in a sweep-parseable summary line.

use std::path::PathBuf;
use std::process::{Child, Command, ExitCode, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::Parser;
use wombatkv_daemon::RemoteKvStoreClient;

const BENCH_THREAD_STACK_BYTES: usize = 32 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "wombatkv-multi-load-bench")]
struct Args {
    /// Number of concurrent client threads (each gets its own SHM prefix).
    #[arg(long, default_value_t = 4)]
    clients: u32,

    /// PUT+GET pairs per client.
    #[arg(long, default_value_t = 250)]
    ops: u32,

    /// Payload size for each PUT.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: u32,

    /// Warmup ops dropped from each client's percentile measurement.
    #[arg(long, default_value_t = 20)]
    warmup_ops: u32,

    /// MinIO endpoint for the daemon's object store.
    #[arg(long, default_value = "http://127.0.0.1:9200")]
    s3_endpoint: String,

    /// Bucket to use (shared across all clients).
    #[arg(long, default_value = "wombatkv-multi-load")]
    s3_bucket: String,

    /// Puffer dir for the daemon's foyer cache.
    #[arg(long, default_value = "/tmp/wombatkv-multi-load-puffer")]
    puffer_dir: PathBuf,

    /// Daemon binary path.
    #[arg(long, default_value = "target/debug/wombatkv-daemon")]
    daemon_bin: PathBuf,

    /// SHM-prefix base. The N prefixes will be `<base>0`, `<base>1`, ...
    /// Keep short, macOS SHM names cap at 31 chars and need room for
    /// the `wmbt_kv_..._resp` suffix.
    #[arg(long, default_value = "ml")]
    prefix_base: String,

    /// Seconds to wait for the daemon to become ready before connecting.
    #[arg(long, default_value_t = 12)]
    daemon_start_timeout_s: u64,
}

fn percentile(samples: &[f64], q: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let len = sorted.len();
    let idx = (((len - 1) as f64) * q).round() as usize;
    sorted[idx.min(len - 1)]
}

fn max(samples: &[f64]) -> f64 {
    samples.iter().copied().fold(0.0_f64, f64::max)
}

struct DaemonGuard {
    child: Child,
    prefixes: Vec<String>,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for prefix in &self.prefixes {
            wombatkv_daemon::cleanup_prefix_segments(prefix);
        }
    }
}

fn spawn_daemon(args: &Args, prefixes: &[String]) -> anyhow::Result<DaemonGuard> {
    for p in prefixes {
        wombatkv_daemon::cleanup_prefix_segments(p);
    }
    std::fs::create_dir_all(&args.puffer_dir)?;

    let mut cmd = Command::new(&args.daemon_bin);
    for p in prefixes {
        cmd.arg("--prefix").arg(p);
    }
    let child = cmd
        .env("WMBT_KV_S3_ENDPOINT", &args.s3_endpoint)
        .env("WMBT_KV_BUCKET", &args.s3_bucket)
        .env("WMBT_KV_PUFFER_DIR", args.puffer_dir.as_os_str())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(DaemonGuard { child, prefixes: prefixes.to_vec() })
}

fn connect_with_retry(prefix: &str, timeout: Duration) -> anyhow::Result<RemoteKvStoreClient> {
    let deadline = Instant::now() + timeout;
    loop {
        match RemoteKvStoreClient::connect(prefix) {
            Ok(c) => return Ok(c),
            Err(e) => {
                if Instant::now() >= deadline {
                    return Err(anyhow::anyhow!("connect {prefix} timed out: {e}"));
                }
                std::thread::sleep(Duration::from_millis(150));
            }
        }
    }
}

#[derive(Default, Clone)]
struct ClientResults {
    put_us: Vec<f64>,
    get_us: Vec<f64>,
}

fn run_client(
    client_id: u32,
    client: Arc<RemoteKvStoreClient>,
    namespace: String,
    ops: u32,
    warmup_ops: u32,
    payload: Bytes,
) -> anyhow::Result<ClientResults> {
    let mut res = ClientResults::default();
    for op in 0..ops {
        let key = format!("c{client_id:03}-op{op:08}");

        let t0 = Instant::now();
        client.put_kv(&namespace, &key, payload.clone())?;
        let put_us_v = t0.elapsed().as_micros() as f64;

        let t1 = Instant::now();
        let _outcome = client.get_kv(&namespace, &key)?;
        let get_us_v = t1.elapsed().as_micros() as f64;

        if op >= warmup_ops {
            res.put_us.push(put_us_v);
            res.get_us.push(get_us_v);
        }
    }
    Ok(res)
}

fn run_bench(args: Args) -> anyhow::Result<()> {
    let prefixes: Vec<String> =
        (0..args.clients).map(|i| format!("{}{i}", args.prefix_base)).collect();

    // Validate prefix budget early, clearer than the daemon's
    // open-retry storm. `wombatkv_daemon::validate_segment_name_budget`
    // accounts for both the wrapper (wk + role) AND the disruptor-mp
    // `_producer_seq` auxiliary suffix that binds the macOS budget.
    for p in &prefixes {
        wombatkv_daemon::validate_segment_name_budget(p).map_err(|e| anyhow::anyhow!(e))?;
    }

    println!(
        "wombatkv-multi-load-bench clients={} ops={}/client payload={}B warmup={}/client",
        args.clients, args.ops, args.payload_bytes, args.warmup_ops
    );
    println!("prefixes: {:?}", prefixes);

    let _guard = spawn_daemon(&args, &prefixes)?;

    // Build payload + share across threads.
    let payload = Bytes::from(vec![0xab; args.payload_bytes as usize]);
    let namespace = "multi-load".to_string();

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.clients as usize);
    for (i, prefix) in prefixes.iter().enumerate() {
        let prefix_owned = prefix.clone();
        let payload_clone = payload.clone();
        let namespace_clone = namespace.clone();
        let ops = args.ops;
        let warmup = args.warmup_ops;
        let timeout = Duration::from_secs(args.daemon_start_timeout_s);
        let i_u32 = u32::try_from(i).unwrap_or(0);
        let h = std::thread::Builder::new()
            .name(format!("client-{i}"))
            .stack_size(BENCH_THREAD_STACK_BYTES)
            .spawn(move || -> anyhow::Result<ClientResults> {
                let client = Arc::new(connect_with_retry(&prefix_owned, timeout)?);
                run_client(i_u32, client, namespace_clone, ops, warmup, payload_clone)
            })?;
        handles.push(h);
    }

    let mut all_put: Vec<f64> = Vec::new();
    let mut all_get: Vec<f64> = Vec::new();
    for h in handles {
        let res = h.join().map_err(|_| anyhow::anyhow!("client thread panicked"))??;
        all_put.extend(res.put_us);
        all_get.extend(res.get_us);
    }
    let elapsed = start.elapsed();
    let total_measured =
        (args.ops.saturating_sub(args.warmup_ops)) as usize * args.clients as usize;
    let throughput_ops = ((total_measured * 2) as f64) / elapsed.as_secs_f64();

    println!();
    println!("===== wombatkv-multi-load-bench results =====");
    println!(
        "clients={} ops/client={} total measured={} (excluded warmup={}) elapsed={:.2}s",
        args.clients,
        args.ops,
        total_measured,
        args.warmup_ops,
        elapsed.as_secs_f64()
    );
    println!(
        "aggregate throughput: {throughput_ops:.0} ops/s (put+get combined across all clients)"
    );
    println!();
    println!("PUT latency (aggregated across {} clients, µs)", args.clients);
    println!(
        "  p50: {:>8.1}   p95: {:>8.1}   p99: {:>8.1}   max: {:>8.1}",
        percentile(&all_put, 0.50),
        percentile(&all_put, 0.95),
        percentile(&all_put, 0.99),
        max(&all_put),
    );
    println!("GET latency (aggregated across {} clients, µs)", args.clients);
    println!(
        "  p50: {:>8.1}   p95: {:>8.1}   p99: {:>8.1}   max: {:>8.1}",
        percentile(&all_get, 0.50),
        percentile(&all_get, 0.95),
        percentile(&all_get, 0.99),
        max(&all_get),
    );

    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    let join = std::thread::Builder::new()
        .name("wombatkv-multi-load-bench".to_string())
        .stack_size(BENCH_THREAD_STACK_BYTES)
        .spawn(move || run_bench(args))
        .expect("spawn bench worker");
    match join.join() {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(e)) => {
            eprintln!("bench failed: {e:#}");
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("bench worker panicked");
            ExitCode::FAILURE
        }
    }
}
