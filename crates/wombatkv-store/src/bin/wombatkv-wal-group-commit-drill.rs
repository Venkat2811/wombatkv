#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::process::ExitCode;

use wombatkv_format::WalRecord;
use wombatkv_store::wal_store::{
    InMemoryObjectStore, WalBatchPolicy, WalGroupCommitStore, WalStore,
};

const RECORDS: u64 = 20;

fn main() -> ExitCode {
    let output_dir = env::args()
        .nth(1)
        .or_else(|| env::var("WOMBATKV_BENCH_ARTIFACT_ROOT").ok())
        .unwrap_or_else(|| "/tmp/wombatkv-bench/wal-group-commit-microbatching".to_string());

    if let Err(error) = fs::create_dir_all(&output_dir) {
        eprintln!("failed to create output dir `{output_dir}`: {error}");
        return ExitCode::from(2);
    }

    let baseline_store = InMemoryObjectStore::default();
    let mut baseline = WalStore::with_node_id("wal/tenant-a/fp", "node-a", baseline_store.clone());
    for idx in 0..RECORDS {
        baseline
            .publish_record(&fixture_record(idx))
            .expect("baseline single publish should succeed");
    }
    let baseline_objects = baseline_store.list_prefix("wal/tenant-a/fp/").expect("list baseline");
    let baseline_replay = baseline.replay_records().expect("replay baseline");

    let grouped_store = InMemoryObjectStore::default();
    let policy = WalBatchPolicy { flush_interval_ms: 500, max_records: 5, max_bytes: 128 * 1024 };
    let mut grouped =
        WalGroupCommitStore::new("wal/tenant-a/fp", "node-a", grouped_store.clone(), policy);
    for idx in 0..RECORDS {
        grouped
            .publish_record(&fixture_record(idx), idx.saturating_mul(10))
            .expect("grouped publish should succeed");
    }
    let _ = grouped.flush_now(10_000).expect("final grouped flush");
    let grouped_objects = grouped_store.list_prefix("wal/tenant-a/fp/").expect("list grouped");
    let grouped_replay = grouped.replay_records().expect("replay grouped");

    let baseline_count = baseline_objects.len();
    let grouped_count = grouped_objects.len();
    let reduced_by = baseline_count.saturating_sub(grouped_count);
    let reduction_pct =
        if baseline_count == 0 { 0.0 } else { (reduced_by as f64 / baseline_count as f64) * 100.0 };
    let strict_pass = grouped_count < baseline_count
        && baseline_replay.len() == usize::try_from(RECORDS).unwrap_or(usize::MAX)
        && grouped_replay.len() == usize::try_from(RECORDS).unwrap_or(usize::MAX);

    let report_json = serde_json::json!({
        "schema_version": 1,
        "target": "wal-group-commit-microbatching",
        "strict_pass": strict_pass,
        "records": RECORDS,
        "baseline": {
            "object_count": baseline_count,
            "replay_count": baseline_replay.len()
        },
        "grouped": {
            "object_count": grouped_count,
            "replay_count": grouped_replay.len(),
            "policy": {
                "flush_interval_ms": policy.flush_interval_ms,
                "max_records": policy.max_records,
                "max_bytes": policy.max_bytes
            }
        },
        "kpi": {
            "objects_reduced_by": reduced_by,
            "object_reduction_pct": reduction_pct
        }
    });

    let report_json_path = format!("{output_dir}/group-commit-report.json");
    let report_md_path = format!("{output_dir}/group-commit-report.md");
    let report_json_bytes = match serde_json::to_vec_pretty(&report_json) {
        Ok(bytes) => bytes,
        Err(error) => {
            eprintln!("failed to encode report json: {error}");
            return ExitCode::from(2);
        }
    };
    if let Err(error) = fs::write(&report_json_path, report_json_bytes) {
        eprintln!("failed to write `{report_json_path}`: {error}");
        return ExitCode::from(2);
    }

    let report_md = format!(
        "# WAL Group Commit Drill\n\n\
- strict_pass: {strict_pass}\n\
- records: {RECORDS}\n\
- baseline_object_count: {baseline_count}\n\
- grouped_object_count: {grouped_count}\n\
- objects_reduced_by: {reduced_by}\n\
- object_reduction_pct: {reduction_pct:.2}\n",
    );
    if let Err(error) = fs::write(&report_md_path, report_md) {
        eprintln!("failed to write `{report_md_path}`: {error}");
        return ExitCode::from(2);
    }

    println!("{report_json_path}");
    println!("{report_md_path}");

    if strict_pass {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn fixture_record(idx: u64) -> WalRecord {
    WalRecord {
        key: format!("k-{idx:04}").into_bytes(),
        meta: format!("m-{idx:04}").into_bytes(),
        payload: vec![b'x'; 64],
    }
}
