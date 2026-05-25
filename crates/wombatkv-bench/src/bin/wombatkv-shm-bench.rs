#![deny(unsafe_code)]
//! End-to-end perf bench for the SHM puffer daemon.
//!
//! Spawns `wombatkv-daemon` as a subprocess, opens the client side
//! of the ring pair, and runs three workloads:
//!   1. PING ping-pong (control-plane RTT, no payload)
//!   2. `PUT_SMALL` + `GET_SMALL` on 4 KiB and 256 KiB payloads
//!   3. EXISTS round-trip after writes
//!
//! Reports p10/p25/p50/p90/p95/p99/p99.9/p99.99/max in microseconds plus
//! ops/s and MB/s for each stage. Use this to compare SHM vs UDS.

use std::process::{Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use wombatkv_daemon::{
    op as op_codes, segment_names, status, WireRequest, WireResponse, ATTACH_TIMEOUT,
    DEFAULT_RING_DEPTH,
};

const NS: &str = "shm-bench";
/// 32 MB stack for the bench worker.
const BENCH_THREAD_STACK_BYTES: usize = 32 * 1024 * 1024;

fn main() -> ExitCode {
    // Move the entire SHM hot path onto a dedicated thread with a
    // big stack so attach + per-iteration work doesn't blow the
    // 8 MB default main-thread stack.
    let join = thread::Builder::new()
        .name("wombatkv-puffer-shm-bench".to_string())
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

fn run_bench() -> ExitCode {
    // Keep prefix short, myelon SHM segment names + cursor suffixes
    // (wmbt_kv_<prefix>_req_cr) must fit in 31 chars on macOS.
    let prefix = format!("b{}", run_tag());
    let depth = std::env::var("WMBT_KV_DAEMON_SHM_DEPTH")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_RING_DEPTH);

    // 1. Spawn daemon
    let bin = match std::env::var("WMBT_KV_DAEMON_SHM_DAEMON_BIN") {
        Ok(path) => std::path::PathBuf::from(path),
        Err(_) => default_daemon_bin(),
    };
    if !bin.is_file() {
        eprintln!("daemon binary not found at {bin:?}");
        return ExitCode::FAILURE;
    }
    let mut cmd = Command::new(&bin);
    cmd.arg("--prefix").arg(&prefix);
    cmd.stdout(Stdio::null()).stderr(Stdio::null());
    let child = cmd.spawn().expect("spawn daemon");
    let mut guard = ChildGuard(Some(child));

    // 2. Open client side once daemon prints "ready"
    let (req_seg, resp_seg) = segment_names(&prefix);
    let (mut req_producer, mut resp_consumer) = wait_open_client(&req_seg, &resp_seg, depth);

    // Discover the daemon's consumer cursor on the request ring
    if !req_producer.discover_consumer_id(wombatkv_daemon::REQ_CONSUMER_ID, ATTACH_TIMEOUT) {
        eprintln!("client could not discover daemon consumer");
        let _ = guard.0.as_mut().unwrap().kill();
        return ExitCode::FAILURE;
    }

    // 3. Workload A: PING ping-pong
    let ping_n = 1000_usize;
    let mut latencies_us = Vec::with_capacity(ping_n);
    for i in 0..ping_n {
        let req = WireRequest {
            id: i as u64,
            op: op_codes::PING,
            namespace: String::new(),
            key: String::new(),
            payload: Vec::new(),
        };
        let t0 = Instant::now();
        req_producer.publish(&req, 1).expect("publish");
        let (_kind, resp) = resp_consumer.recv_owned::<WireResponse>().expect("recv");
        let dt = t0.elapsed();
        if resp.id != i as u64 || resp.status != status::OK {
            eprintln!("ping mismatch at i={i}: got id={} status={}", resp.id, resp.status);
            return ExitCode::FAILURE;
        }
        latencies_us.push(dt.as_nanos() as f64 / 1000.0);
    }
    print_stage("ping_rtt", &mut latencies_us, 0);

    // 4. Workload B: PUT then GET across the full SHM-frame size range.
    // FRAME_DATA_BYTES is 2 MiB - 16; rkyv envelope is ~64 bytes per
    // WireResponse. Walk from a control-class payload up to ~1.5 MiB.
    for &kib in &[4_usize, 64, 256, 1024, 1536] {
        let payload = vec![0xCDu8; kib * 1024];
        // PUT
        let n = if kib >= 256 { 200 } else { 1000 };
        let mut put_us = Vec::with_capacity(n);
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let req = WireRequest {
                id: i as u64,
                op: op_codes::PUT,
                namespace: NS.into(),
                key,
                payload: payload.clone(),
            };
            let t0 = Instant::now();
            req_producer.publish(&req, 1).expect("publish put");
            let (_kind, resp) = resp_consumer.recv_owned::<WireResponse>().expect("recv put");
            let dt = t0.elapsed();
            assert_eq!(resp.status, status::OK, "put failed: {}", resp.message);
            put_us.push(dt.as_nanos() as f64 / 1000.0);
        }
        print_stage(&format!("put_{kib}KiB"), &mut put_us, payload.len());

        // GET
        let mut get_us = Vec::with_capacity(n);
        for i in 0..n {
            let key = format!("k-{kib}-{i}");
            let req = WireRequest {
                id: i as u64,
                op: op_codes::GET,
                namespace: NS.into(),
                key,
                payload: Vec::new(),
            };
            let t0 = Instant::now();
            req_producer.publish(&req, 1).expect("publish get");
            let (_kind, resp) = resp_consumer.recv_owned::<WireResponse>().expect("recv get");
            let dt = t0.elapsed();
            assert_eq!(resp.status, status::OK, "get failed: {}", resp.message);
            assert_eq!(resp.payload.len(), payload.len(), "get payload size mismatch");
            get_us.push(dt.as_nanos() as f64 / 1000.0);
        }
        print_stage(&format!("get_{kib}KiB"), &mut get_us, payload.len());
    }

    // 5. EXISTS hot-path
    let key0 = "k-4-0";
    let mut exists_us = Vec::with_capacity(1000);
    for i in 0..1000_usize {
        let req = WireRequest {
            id: i as u64,
            op: op_codes::EXISTS,
            namespace: NS.into(),
            key: key0.into(),
            payload: Vec::new(),
        };
        let t0 = Instant::now();
        req_producer.publish(&req, 1).expect("publish exists");
        let (_kind, resp) = resp_consumer.recv_owned::<WireResponse>().expect("recv exists");
        let dt = t0.elapsed();
        assert_eq!(resp.status, status::OK, "exists failed: {}", resp.message);
        exists_us.push(dt.as_nanos() as f64 / 1000.0);
    }
    print_stage("exists_rtt", &mut exists_us, 0);

    let _ = guard.0.as_mut().unwrap().kill();
    ExitCode::SUCCESS
}

fn print_stage(label: &str, samples: &mut [f64], payload_bytes: usize) {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let count = samples.len() as f64;
    let total_us: f64 = samples.iter().sum();
    let total_s = total_us / 1_000_000.0;
    let ops_per_s = if total_s > 0.0 { count / total_s } else { 0.0 };
    let mb_per_s = if total_s > 0.0 {
        (count * payload_bytes as f64) / (1024.0 * 1024.0) / total_s
    } else {
        0.0
    };

    let p = |q: f64| -> f64 {
        let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
        samples[idx.min(samples.len() - 1)]
    };

    println!(
        "{{\"scope\":\"wmbt_kv_shm_bench\",\"stage\":\"{label}\",\"count\":{},\"payload_bytes\":{},\"ops_per_s\":{:.0},\"mb_per_s\":{:.2},\"p10_us\":{:.2},\"p25_us\":{:.2},\"p50_us\":{:.2},\"p90_us\":{:.2},\"p95_us\":{:.2},\"p99_us\":{:.2},\"p99_9_us\":{:.2},\"p99_99_us\":{:.2},\"max_us\":{:.2}}}",
        samples.len(),
        payload_bytes,
        ops_per_s,
        mb_per_s,
        p(0.10),
        p(0.25),
        p(0.50),
        p(0.90),
        p(0.95),
        p(0.99),
        p(0.999),
        p(0.9999),
        samples.last().copied().unwrap_or(0.0),
    );
}

fn wait_open_client(
    req_seg: &str,
    resp_seg: &str,
    depth: usize,
) -> (
    myelon::typed_transport::TypedProducer<wombatkv_daemon::ShmFrame>,
    myelon::typed_transport::TypedConsumer<wombatkv_daemon::ShmFrame>,
) {
    use std::time::Instant as I;
    let deadline = I::now() + Duration::from_secs(30);
    loop {
        match wombatkv_daemon::open_client(req_seg, resp_seg, depth) {
            Ok(pair) => return pair,
            Err(_) if I::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
            Err(err) => panic!("open_client: {err}"),
        }
    }
}

fn default_daemon_bin() -> std::path::PathBuf {
    // Walk up from current_exe()/parent until we find wombatkv-daemon
    let exe = std::env::current_exe().expect("current_exe");
    let mut dir = exe.clone();
    while let Some(parent) = dir.parent() {
        let cand = parent.join("wombatkv-daemon");
        if cand.is_file() {
            return cand;
        }
        dir = parent.to_path_buf();
    }
    std::path::PathBuf::from("./target/release/wombatkv-daemon")
}

fn run_tag() -> String {
    // POSIX SHM names cap at 31 chars on macOS. Use the LAST 6 hex chars
    // of nanos so consecutive runs get different segment names (the high
    // hex digits change very slowly).
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    let s = format!("{nanos:x}");
    let n = s.len();
    s.chars().skip(n.saturating_sub(6)).collect()
}

struct ChildGuard(Option<Child>);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
