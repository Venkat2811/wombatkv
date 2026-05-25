#![deny(unsafe_code)]
//! wombatkv-tcp-multi-load-bench, concurrent N-client TCP load.
//!
//! Companion to `wombatkv-multi-load-bench` (SHM transport).
//! Spawns N client threads, each opening its own TcpKvClient
//! connection to the same daemon address, and drives a sequential
//! PUT → GET workload. Aggregates latency percentiles + throughput
//! across all clients.
//!
//! Designed to exercise the daemon's TCP path under concurrent
//! pressure, where the compio + SO_REUSEPORT N-shard architecture
//! should beat std::net thread-per-conn. Single-client TCP perf
//! (wombatkv-tcp-smoke) is bandwidth-bound, the win shows up at
//! N >= 4 concurrent clients.
//!
//! ## Use
//!
//! Daemon side:
//! ```text
//! WMBT_KV_TCP_TPC_THREADS=4 \
//! wombatkv-daemon --tcp 0.0.0.0:7878
//! ```
//!
//! Client side:
//! ```text
//! wombatkv-tcp-multi-load-bench \
//!     --addr 203.0.113.5:7878 \
//!     --clients 8 \
//!     --ops 200 \
//!     --payload-bytes 4096
//! ```

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use wombatkv_daemon::tcp_transport::{TcpGetOutcome, TcpKvClient};

const BENCH_THREAD_STACK_BYTES: usize = 32 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "wombatkv-tcp-multi-load-bench")]
struct Args {
    /// host:port the daemon is bound to.
    #[arg(long)]
    addr: String,

    /// Number of concurrent client threads (each opens its own TCP
    /// connection, the daemon must support multiple concurrent
    /// connections; compio + SO_REUSEPORT does, std::net is fine too).
    #[arg(long, default_value_t = 4)]
    clients: u32,

    /// PUT+GET pairs per client.
    #[arg(long, default_value_t = 200)]
    ops: u32,

    /// Payload size for each PUT.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: u32,

    /// Warmup ops dropped from each client's percentile measurement.
    #[arg(long, default_value_t = 20)]
    warmup_ops: u32,

    /// Namespace for the PUT/GET workload.
    #[arg(long, default_value = "tcp-multi-load")]
    namespace: String,
}

fn percentile(samples: &[f64], q: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = (((sorted.len() - 1) as f64) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn max(samples: &[f64]) -> f64 {
    samples.iter().copied().fold(0.0_f64, f64::max)
}

#[derive(Default, Clone)]
struct ClientResults {
    put_us: Vec<f64>,
    get_us: Vec<f64>,
}

fn run_client(
    client_id: u32,
    addr: String,
    namespace: String,
    ops: u32,
    warmup_ops: u32,
    payload: Bytes,
) -> anyhow::Result<ClientResults> {
    let client = TcpKvClient::connect(addr.as_str())
        .map_err(|e| anyhow::anyhow!("client {client_id} connect: {e}"))?;
    let mut res = ClientResults::default();
    for op in 0..ops {
        let key = format!("c{client_id:03}-op{op:08}");

        let t0 = Instant::now();
        client
            .put_kv(&namespace, &key, payload.clone())
            .map_err(|e| anyhow::anyhow!("client {client_id} put op {op}: {e}"))?;
        #[allow(clippy::cast_precision_loss)]
        let put_us_v = t0.elapsed().as_micros() as f64;

        let t1 = Instant::now();
        match client.get_kv(&namespace, &key) {
            Ok(TcpGetOutcome::Hit { payload: bytes }) => {
                if bytes.len() != payload.len() {
                    return Err(anyhow::anyhow!(
                        "client {client_id} op {op} payload size mismatch (got {} expected {})",
                        bytes.len(),
                        payload.len()
                    ));
                }
            }
            Ok(TcpGetOutcome::Miss) => {
                return Err(anyhow::anyhow!("client {client_id} op {op} unexpected MISS"));
            }
            Err(e) => return Err(anyhow::anyhow!("client {client_id} get op {op}: {e}")),
        }
        #[allow(clippy::cast_precision_loss)]
        let get_us_v = t1.elapsed().as_micros() as f64;

        if op >= warmup_ops {
            res.put_us.push(put_us_v);
            res.get_us.push(get_us_v);
        }
    }
    Ok(res)
}

fn run_bench(args: Args) -> anyhow::Result<()> {
    println!(
        "wombatkv-tcp-multi-load-bench addr={} clients={} ops={}/client payload={}B warmup={}/client",
        args.addr, args.clients, args.ops, args.payload_bytes, args.warmup_ops
    );

    let payload = Bytes::from(vec![0xab; args.payload_bytes as usize]);
    let namespace = Arc::new(args.namespace);

    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.clients as usize);
    for i in 0..args.clients {
        let addr = args.addr.clone();
        let ns = Arc::clone(&namespace);
        let payload = payload.clone();
        let ops = args.ops;
        let warmup = args.warmup_ops;
        let h = std::thread::Builder::new()
            .name(format!("tcp-client-{i}"))
            .stack_size(BENCH_THREAD_STACK_BYTES)
            .spawn(move || -> anyhow::Result<ClientResults> {
                run_client(i, addr, (*ns).clone(), ops, warmup, payload)
            })?;
        handles.push(h);
    }

    let mut all_put: Vec<f64> = Vec::new();
    let mut all_get: Vec<f64> = Vec::new();
    let mut failed_clients = 0u32;
    for h in handles {
        match h.join() {
            Ok(Ok(res)) => {
                all_put.extend(res.put_us);
                all_get.extend(res.get_us);
            }
            Ok(Err(e)) => {
                eprintln!("client failed: {e}");
                failed_clients += 1;
            }
            Err(_) => {
                eprintln!("client panicked");
                failed_clients += 1;
            }
        }
    }
    let elapsed = start.elapsed();
    let total_measured =
        (args.ops.saturating_sub(args.warmup_ops)) as usize * args.clients as usize;
    #[allow(clippy::cast_precision_loss)]
    let throughput = ((total_measured * 2) as f64) / elapsed.as_secs_f64();

    println!();
    println!("===== wombatkv-tcp-multi-load-bench results =====");
    println!(
        "clients={} ops/client={} total measured={} failed_clients={} elapsed={:.2}s",
        args.clients,
        args.ops,
        total_measured,
        failed_clients,
        elapsed.as_secs_f64()
    );
    println!("aggregate throughput: {throughput:.0} ops/s (put+get combined across all clients)");
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

    if failed_clients > 0 {
        Err(anyhow::anyhow!("{failed_clients} clients failed"))
    } else {
        Ok(())
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    let join = std::thread::Builder::new()
        .name("wombatkv-tcp-multi-load-bench".to_string())
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
