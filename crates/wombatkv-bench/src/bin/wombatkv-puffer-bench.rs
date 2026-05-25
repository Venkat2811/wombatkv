#![forbid(unsafe_code)]
//! Drive the embeddable wombatkv KV store through a multi-stage
//! workload and print percentile latencies + throughput per op.
//!
//! Stages:
//!   1. cold stash: PUT N payloads (foyer + S3 write-through)
//!   2. warm get : GET each payload from foyer
//!   3. cold get : drop foyer, GET each from S3 fallback
//!   4. restart  : drop the entire store, rebuild, `restore_from_s3`,
//!                 then GET each (now warm in foyer)
//!
//! Required env: `WMBT_KV_S3`_*, optional `WMBT_KV_PUFFER`_*, plus
//!   `WMBT_KV_BENCH_OPS`              total ops per stage (default 64)
//!   `WMBT_KV_BENCH_PAYLOAD_KIB`      per-op payload size (default 4096)
//!   `WMBT_KV_BENCH_NAMESPACE`        unique namespace (default: timestamped)

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use wombatkv_node::embed::{EmbedConfig, GetOutcome, WombatKVKvStore};
use wombatkv_node::embed_metrics::metrics;
use wombatkv_node::foyer_cache::FoyerCacheConfig;
use wombatkv_store::wal_store::{S3ObjectStore, S3ObjectStoreConfig};

fn main() -> ExitCode {
    let ops: usize =
        std::env::var("WMBT_KV_BENCH_OPS").ok().and_then(|s| s.parse().ok()).unwrap_or(64);
    let payload_kib: usize = std::env::var("WMBT_KV_BENCH_PAYLOAD_KIB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4096);
    let namespace =
        std::env::var("WMBT_KV_BENCH_NAMESPACE").unwrap_or_else(|_| format!("bench-{}", run_id()));

    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"event\":\"start\",\"ops\":{ops},\"payload_kib\":{payload_kib},\"namespace\":\"{namespace}\"}}"
    );
    let payload_bytes = vec![0xAB_u8; payload_kib * 1024];
    let payload = Bytes::from(payload_bytes);

    let store = match build_store() {
        Ok(s) => s,
        Err(err) => {
            eprintln!("wombatkv-puffer-bench: failed to open store: {err}");
            return ExitCode::FAILURE;
        }
    };

    // Stage 1: cold stash
    let stage_start = Instant::now();
    for idx in 0..ops {
        let key = format!("seq-{idx:08x}");
        if let Err(err) = store.put_kv(&namespace, &key, payload.clone()) {
            eprintln!("put failed at idx {idx}: {err}");
            return ExitCode::FAILURE;
        }
    }
    let stage_secs = stage_start.elapsed().as_secs_f64();
    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"stage\":\"cold_stash\",\"ops\":{ops},\"wall_s\":{:.3},\"ops_per_s\":{:.2}}}",
        stage_secs,
        (ops as f64) / stage_secs.max(1e-9)
    );

    // Stage 2: warm get (foyer)
    let stage_start = Instant::now();
    let mut warm_hits = 0_usize;
    for idx in 0..ops {
        let key = format!("seq-{idx:08x}");
        match store.get_kv(&namespace, &key) {
            Ok(GetOutcome::Hit { .. }) => warm_hits += 1,
            other => {
                eprintln!("warm get missed for {key}: {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }
    let stage_secs = stage_start.elapsed().as_secs_f64();
    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"stage\":\"warm_get\",\"hits\":{warm_hits},\"wall_s\":{:.3},\"ops_per_s\":{:.2}}}",
        stage_secs,
        (warm_hits as f64) / stage_secs.max(1e-9)
    );

    // Stage 3: cold get (S3 fallback)
    store.clear_foyer();
    let stage_start = Instant::now();
    let mut s3_hits = 0_usize;
    for idx in 0..ops {
        let key = format!("seq-{idx:08x}");
        match store.get_kv(&namespace, &key) {
            Ok(GetOutcome::Hit { .. }) => s3_hits += 1,
            other => {
                eprintln!("cold get missed for {key}: {other:?}");
                return ExitCode::FAILURE;
            }
        }
    }
    let stage_secs = stage_start.elapsed().as_secs_f64();
    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"stage\":\"cold_get_s3_fallback\",\"hits\":{s3_hits},\"wall_s\":{:.3},\"ops_per_s\":{:.2}}}",
        stage_secs,
        (s3_hits as f64) / stage_secs.max(1e-9)
    );

    // Stage 4: drop store, recreate, restore_from_s3, warm get
    drop(store);
    let store2 = match build_store() {
        Ok(s) => s,
        Err(err) => {
            eprintln!("wombatkv-puffer-bench: failed to reopen store: {err}");
            return ExitCode::FAILURE;
        }
    };
    let stage_start = Instant::now();
    let restored = match store2.restore_from_s3(&namespace) {
        Ok(n) => n,
        Err(err) => {
            eprintln!("restore_from_s3 failed: {err}");
            return ExitCode::FAILURE;
        }
    };
    let stage_secs = stage_start.elapsed().as_secs_f64();
    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"stage\":\"restore_from_s3\",\"restored\":{restored},\"wall_s\":{:.3},\"keys_per_s\":{:.2}}}",
        stage_secs,
        (restored as f64) / stage_secs.max(1e-9)
    );
    if restored != ops {
        eprintln!("warning: restore_from_s3 returned {restored}, expected {ops}");
    }

    let stage_start = Instant::now();
    let mut post_restore_hits = 0_usize;
    for idx in 0..ops {
        let key = format!("seq-{idx:08x}");
        if let Ok(GetOutcome::Hit { .. }) = store2.get_kv(&namespace, &key) {
            post_restore_hits += 1;
        }
    }
    let stage_secs = stage_start.elapsed().as_secs_f64();
    println!(
        "{{\"scope\":\"wmbt_kv_bench\",\"stage\":\"post_restore_warm_get\",\"hits\":{post_restore_hits},\"wall_s\":{:.3},\"ops_per_s\":{:.2}}}",
        stage_secs,
        (post_restore_hits as f64) / stage_secs.max(1e-9)
    );

    // Final metrics report (process-global; collected across all ops above)
    print!("{}", metrics().to_json_lines());

    ExitCode::SUCCESS
}

fn build_store() -> Result<Arc<WombatKVKvStore<S3ObjectStore>>, String> {
    let s3_cfg = S3ObjectStoreConfig::from_env().map_err(|err| format!("{err:?}"))?;
    let s3 = S3ObjectStore::new(s3_cfg).map_err(|err| format!("{err:?}"))?;
    s3.ensure_bucket().map_err(|err| format!("{err:?}"))?;

    let mut foyer = FoyerCacheConfig::default();
    foyer.ssd_dir = std::env::var("WMBT_KV_PUFFER_DIR").map_or_else(
        |_| std::env::temp_dir().join(format!("wombatkv-bench-foyer-{}", run_id())),
        PathBuf::from,
    );
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_RAM_BYTES") {
        if let Ok(parsed) = value.parse::<u64>() {
            foyer.ram_bytes = parsed;
        }
    }
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_DISK_BYTES") {
        if let Ok(parsed) = value.parse::<u64>() {
            foyer.ssd_bytes = parsed;
        }
    }
    if let Ok(value) = std::env::var("WMBT_KV_PUFFER_BLOCK_SIZE_BYTES") {
        if let Ok(parsed) = value.parse::<usize>() {
            foyer.block_size = parsed;
        }
    }
    foyer.iouring = false; // match macOS smoke profile

    let s3_prefix =
        std::env::var("WMBT_KV_BENCH_S3_PREFIX").unwrap_or_else(|_| "kv/bench".to_string());
    let cfg = EmbedConfig {
        s3_prefix,
        foyer,
        write_through_s3: true,
        compression: wombatkv_node::compression::BlockCompressionConfig::from_env(),
    };

    Ok(Arc::new(WombatKVKvStore::new(cfg, s3).map_err(|err| format!("{err}"))?))
}

fn run_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    format!("{nanos:x}")
}
