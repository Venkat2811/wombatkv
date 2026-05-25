#![deny(unsafe_code)]
//! wombatkv-tcp-smoke, cross-machine sanity + first-light latency probe.
//!
//! Connects to a `wombatkv-daemon` over TCP, fires a sequence of
//! pings + put/get roundtrips, and reports per-op latency percentiles.
//! Designed as the "first time wombatkv talked across machines" smoke
//! test: Mac client → Linux daemon, no S3 yet beyond ping.
//!
//! ```text
//! wombatkv-tcp-smoke \
//!     --addr 203.0.113.5:7878 \
//!     --pings 100 \
//!     --rounds 100 \
//!     --payload-bytes 4096
//! ```

use std::process::ExitCode;
use std::time::Instant;

use bytes::Bytes;
use clap::Parser;
use wombatkv_daemon::tcp_transport::{TcpGetOutcome, TcpKvClient};

#[derive(Parser, Debug)]
#[command(name = "wombatkv-tcp-smoke")]
struct Args {
    /// host:port the daemon's `--tcp` is bound to.
    #[arg(long)]
    addr: String,

    /// Ping iterations (drops first 5 as warmup).
    #[arg(long, default_value_t = 100)]
    pings: u32,

    /// Put/get roundtrips against the same key sequence (drops first 5).
    #[arg(long, default_value_t = 100)]
    rounds: u32,

    /// Payload size for each PUT.
    #[arg(long, default_value_t = 4096)]
    payload_bytes: u32,

    /// Namespace to write into (so smoke runs don't collide with bench
    /// namespaces).
    #[arg(long, default_value = "tcp-smoke")]
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

fn main() -> ExitCode {
    let args = Args::parse();
    let client = match TcpKvClient::connect(&args.addr) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connect {} failed: {e}", args.addr);
            return ExitCode::FAILURE;
        }
    };
    println!("wombatkv-tcp-smoke connected addr={}", args.addr);

    // ----- pings -----
    const WARMUP: u32 = 5;
    let mut ping_us: Vec<f64> = Vec::with_capacity(args.pings as usize);
    for i in 0..args.pings {
        let t0 = Instant::now();
        if let Err(e) = client.ping() {
            eprintln!("ping {i} failed: {e}");
            return ExitCode::FAILURE;
        }
        let us = t0.elapsed().as_micros() as f64;
        if i >= WARMUP {
            ping_us.push(us);
        }
    }

    // ----- put / get roundtrips -----
    let payload = Bytes::from(vec![0xab_u8; args.payload_bytes as usize]);
    let mut put_us: Vec<f64> = Vec::with_capacity(args.rounds as usize);
    let mut get_us: Vec<f64> = Vec::with_capacity(args.rounds as usize);
    for i in 0..args.rounds {
        let key = format!("smoke-{i:08}");

        let t0 = Instant::now();
        if let Err(e) = client.put_kv(&args.namespace, &key, payload.clone()) {
            eprintln!("put {i} failed: {e}");
            return ExitCode::FAILURE;
        }
        let put_us_v = t0.elapsed().as_micros() as f64;

        let t1 = Instant::now();
        match client.get_kv(&args.namespace, &key) {
            Ok(TcpGetOutcome::Hit { payload: got }) => {
                if got.len() != args.payload_bytes as usize {
                    eprintln!(
                        "get {i}: payload size mismatch (got {} expected {})",
                        got.len(),
                        args.payload_bytes
                    );
                    return ExitCode::FAILURE;
                }
            }
            Ok(TcpGetOutcome::Miss) => {
                eprintln!("get {i} unexpected MISS for key just PUT");
                return ExitCode::FAILURE;
            }
            Err(e) => {
                eprintln!("get {i} failed: {e}");
                return ExitCode::FAILURE;
            }
        }
        let get_us_v = t1.elapsed().as_micros() as f64;

        if i >= WARMUP {
            put_us.push(put_us_v);
            get_us.push(get_us_v);
        }
    }

    println!();
    println!("===== wombatkv-tcp-smoke results =====");
    println!(
        "PING latency (µs): p50={:.1} p95={:.1} p99={:.1} max={:.1} samples={}",
        percentile(&ping_us, 0.50),
        percentile(&ping_us, 0.95),
        percentile(&ping_us, 0.99),
        max(&ping_us),
        ping_us.len()
    );
    println!(
        "PUT  latency (µs): p50={:.1} p95={:.1} p99={:.1} max={:.1} samples={}",
        percentile(&put_us, 0.50),
        percentile(&put_us, 0.95),
        percentile(&put_us, 0.99),
        max(&put_us),
        put_us.len()
    );
    println!(
        "GET  latency (µs): p50={:.1} p95={:.1} p99={:.1} max={:.1} samples={}",
        percentile(&get_us, 0.50),
        percentile(&get_us, 0.95),
        percentile(&get_us, 0.99),
        max(&get_us),
        get_us.len()
    );

    ExitCode::SUCCESS
}
