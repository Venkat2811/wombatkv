#![deny(unsafe_code)]
//! wombatkv-load-bench, sustained-load harness for the daemon.
//!
//! Drives a sequential put → get workload through a running daemon
//! and reports latency percentiles + throughput. This is the
//! starting point for P10 (daemon-mode load validation) from RFC
//! 0011, it intentionally stays single-client because the 1P-1C
//! disruptor SHM transport is single-producer by construction;
//! multi-producer load would require either N daemons or a
//! transport change. Tracked as a separate v0.2 follow-up.
//!
//! ## Pre-req
//!
//! Start a daemon in a separate terminal:
//!
//! ```text
//! wombatkv-daemon \
//!     --prefix loadbench \
//!     --namespace loadbench-ns \
//!     --bucket loadbench-bucket \
//!     --endpoint http://127.0.0.1:9200
//! ```
//!
//! ## Run
//!
//! ```text
//! wombatkv-load-bench \
//!     --daemon-prefix loadbench \
//!     --ops 5000 \
//!     --payload-bytes 4096
//! ```
//!
//! ## What it measures
//!
//! For each of N operations, in order:
//!   - PUT with unique key + fixed payload, time end-to-end
//!   - GET the same key (foyer-RAM hit expected after PUT), time it
//!
//! Reports:
//!   - PUT p50/p95/p99/max latency in µs
//!   - GET p50/p95/p99/max latency in µs
//!   - Sustained throughput in ops/s (PUT + GET combined)
//!   - Slowest-op trace for tail diagnostics

use std::process::ExitCode;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use wombatkv_daemon::RemoteKvStoreClient;

const BENCH_THREAD_STACK_BYTES: usize = 32 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "wombatkv-load-bench")]
struct Args {
    /// SHM prefix the daemon is bound to (matches `wombatkv-daemon --prefix`)
    #[arg(long)]
    daemon_prefix: String,

    /// Namespace to PUT/GET into (independent of daemon's own namespace)
    #[arg(long, default_value = "loadbench")]
    namespace: String,

    /// Total number of PUT+GET pairs to issue
    #[arg(long, default_value_t = 1000)]
    ops: usize,

    /// Payload size for each PUT (bytes). Larger → exercises the
    /// large-payload transport path.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: usize,

    /// Warmup ops to drop from the percentile measurement (first-call
    /// JIT / SHM-attach noise).
    #[arg(long, default_value_t = 50)]
    warmup_ops: usize,
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

fn run_bench(args: Args) -> anyhow::Result<()> {
    let payload = Bytes::from(vec![0xab; args.payload_bytes]);
    let client = RemoteKvStoreClient::connect(&args.daemon_prefix)?;

    let mut put_us: Vec<f64> = Vec::with_capacity(args.ops);
    let mut get_us: Vec<f64> = Vec::with_capacity(args.ops);
    let mut slowest_put_op: usize = 0;
    let mut slowest_put_us: f64 = 0.0;
    let mut slowest_get_op: usize = 0;
    let mut slowest_get_us: f64 = 0.0;

    println!(
        "wombatkv-load-bench connect ok prefix={} namespace={} ops={} payload={}B warmup={}",
        args.daemon_prefix, args.namespace, args.ops, args.payload_bytes, args.warmup_ops,
    );

    let start = Instant::now();
    for op in 0..args.ops {
        let key = format!("op-{op:08}");

        let t0 = Instant::now();
        client.put_kv(&args.namespace, &key, payload.clone())?;
        #[allow(clippy::cast_precision_loss)]
        let put_us_v = t0.elapsed().as_micros() as f64;

        let t1 = Instant::now();
        let _outcome = client.get_kv(&args.namespace, &key)?;
        #[allow(clippy::cast_precision_loss)]
        let get_us_v = t1.elapsed().as_micros() as f64;

        if op >= args.warmup_ops {
            put_us.push(put_us_v);
            get_us.push(get_us_v);

            if put_us_v > slowest_put_us {
                slowest_put_us = put_us_v;
                slowest_put_op = op;
            }
            if get_us_v > slowest_get_us {
                slowest_get_us = get_us_v;
                slowest_get_op = op;
            }
        }

        // Progress every 10% (only after warmup).
        if args.ops >= 10 && (op + 1) % args.ops.div_ceil(10) == 0 {
            println!(
                "  progress: {}/{} (put_p50={:.0}µs get_p50={:.0}µs)",
                op + 1,
                args.ops,
                percentile(&put_us, 0.50),
                percentile(&get_us, 0.50),
            );
        }
    }
    let elapsed = start.elapsed();
    let measured = args.ops.saturating_sub(args.warmup_ops);
    #[allow(clippy::cast_precision_loss)]
    let throughput = ((measured * 2) as f64) / elapsed.as_secs_f64();

    println!();
    println!("===== wombatkv-load-bench results =====");
    println!(
        "ops measured: {measured} (excluded {} warmup) | total elapsed: {:.2}s | throughput: {throughput:.0} ops/s (put+get)",
        args.warmup_ops,
        elapsed.as_secs_f64(),
    );
    println!();
    println!("PUT latency (µs)");
    println!(
        "  p50: {:>8.1}   p95: {:>8.1}   p99: {:>8.1}   max: {:>8.1} (op {})",
        percentile(&put_us, 0.50),
        percentile(&put_us, 0.95),
        percentile(&put_us, 0.99),
        max(&put_us),
        slowest_put_op,
    );
    println!("GET latency (µs)");
    println!(
        "  p50: {:>8.1}   p95: {:>8.1}   p99: {:>8.1}   max: {:>8.1} (op {})",
        percentile(&get_us, 0.50),
        percentile(&get_us, 0.95),
        percentile(&get_us, 0.99),
        max(&get_us),
        slowest_get_op,
    );
    println!();
    println!("Slowest PUT was {slowest_put_us:.1}µs at op {slowest_put_op}");
    println!("Slowest GET was {slowest_get_us:.1}µs at op {slowest_get_op}");

    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    let join = std::thread::Builder::new()
        .name("wombatkv-load-bench".to_string())
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
