#![deny(unsafe_code)]
//! Standalone arena throughput bench (future zero-copy Phase 1).
//!
//! Measures the cost of writing a payload into the mmap'd arena and
//! reading it back from a separate `ArenaReader` view. This is the
//! lower bound on the future zero-copy GET path, when we wire it into the daemon
//! protocol (Phase 2), real GETs will pay this much for the data plane,
//! plus ~5-10 µs for the small (offset, len) round-trip on the SHM
//! ring and the foyer hit on the daemon side.
//!
//! Output is a JSON-line per stage matching the existing bench format,
//! so the harness scripts that parse `wombatkv-shm-bench` can consume
//! this too.

use std::time::Instant;

use wombatkv_daemon::{ArenaReader, ArenaWriter, ARENA_HEADER_BYTES};

const SIZES_KIB: &[usize] = &[4, 64, 256, 1024, 1536];

fn main() -> std::process::ExitCode {
    let path =
        std::path::PathBuf::from(format!("/tmp/wombatkv-arena-bench-{}.bin", std::process::id()));
    let _cleanup = PathCleanup(path.clone());
    let arena_size: u64 = 4 * 1024 * 1024 * 1024; // 4 GiB so 1.5 MiB × N fits
    let mut writer = match ArenaWriter::create(&path, arena_size) {
        Ok(w) => w,
        Err(err) => {
            eprintln!("ArenaWriter::create: {err}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let reader = match ArenaReader::open(&path) {
        Ok(r) => r,
        Err(err) => {
            eprintln!("ArenaReader::open: {err}");
            return std::process::ExitCode::FAILURE;
        }
    };

    println!(
        "{{\"scope\":\"wmbt_kv_arena_bench\",\"event\":\"opening\",\"arena_size\":{arena_size},\"path\":\"{}\"}}",
        path.display()
    );

    for &kib in SIZES_KIB {
        let payload = vec![0xCDu8; kib * 1024];
        let n = if kib >= 256 { 200 } else { 1000 };

        // Pre-fill the arena once so the page cache is hot.
        let _ = writer.write_payload(&payload).expect("warmup write");

        // WRITE: just the daemon-side memcpy into the arena slab.
        let mut write_us = Vec::with_capacity(n);
        let mut offsets = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = Instant::now();
            let (off, _len) = writer.write_payload(&payload).expect("write");
            write_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            offsets.push(off);
        }
        emit_stage(&format!("arena_write_{kib}KiB"), &mut write_us, payload.len());

        // READ: just the client-side memcpy out of the arena slab. We
        // read into a fresh Vec each iteration so the cost reflects
        // what the engine pays per call (not zero-copy borrow).
        let mut read_us = Vec::with_capacity(n);
        for off in &offsets {
            let t0 = Instant::now();
            let view = reader.read_payload(*off, payload.len() as u32).expect("read");
            // Force the memcpy that a real engine call would do
            // (e.g., to construct a `Bytes` from the borrowed slice).
            let mut owned = Vec::with_capacity(view.len());
            owned.extend_from_slice(view);
            read_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            // Sanity.
            std::hint::black_box(owned);
        }
        emit_stage(&format!("arena_read_{kib}KiB"), &mut read_us, payload.len());

        // Combined: write then read in lock-step (one process, no IPC).
        // This is the lower bound on the data-plane portion of a zero-copy
        // GET (still need to add SHM ring control-plane overhead).
        let mut rt_us = Vec::with_capacity(n);
        for _ in 0..n {
            let t0 = Instant::now();
            let (off, len) = writer.write_payload(&payload).expect("rt write");
            let view = reader.read_payload(off, len).expect("rt read");
            let mut owned = Vec::with_capacity(view.len());
            owned.extend_from_slice(view);
            rt_us.push(t0.elapsed().as_nanos() as f64 / 1000.0);
            std::hint::black_box(owned);
        }
        emit_stage(&format!("arena_write_then_read_{kib}KiB"), &mut rt_us, payload.len());
    }

    println!(
        "{{\"scope\":\"wmbt_kv_arena_bench\",\"event\":\"done\",\"final_offset\":{},\"wrap_epoch\":{}}}",
        writer.next_offset() - ARENA_HEADER_BYTES as u64,
        writer.wrap_epoch()
    );
    std::process::ExitCode::SUCCESS
}

struct PathCleanup(std::path::PathBuf);
impl Drop for PathCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn emit_stage(label: &str, samples: &mut [f64], payload_bytes: usize) {
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
        "{{\"scope\":\"wmbt_kv_arena_bench\",\"stage\":\"{label}\",\"count\":{},\"payload_bytes\":{},\"ops_per_s\":{:.0},\"mb_per_s\":{:.2},\"p10_us\":{:.2},\"p50_us\":{:.2},\"p90_us\":{:.2},\"p99_us\":{:.2},\"p99_9_us\":{:.2},\"max_us\":{:.2}}}",
        samples.len(),
        payload_bytes,
        ops_per_s,
        mb_per_s,
        p(0.10),
        p(0.50),
        p(0.90),
        p(0.99),
        p(0.999),
        samples.last().copied().unwrap_or(0.0),
    );
}
