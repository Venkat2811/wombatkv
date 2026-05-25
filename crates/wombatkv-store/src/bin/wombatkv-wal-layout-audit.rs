#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::process::ExitCode;

use wombatkv_store::wal_store::{
    audit_wal_layout_counts, ObjectStore, S3ObjectStore, WalLayoutAuditCounts, WalStoreError,
};

fn main() -> ExitCode {
    let args = env::args().collect::<Vec<_>>();
    if args.len() > 3 {
        eprintln!("usage: tp-wal-layout-audit [namespace_prefix] [output_dir]");
        return ExitCode::from(2);
    }

    let namespace_prefix = args
        .get(1)
        .cloned()
        .or_else(|| env::var("WMBT_KV_WAL_NAMESPACE_PREFIX").ok())
        .unwrap_or_else(|| "wal/tenant-a/fp".to_string());
    let output_dir = args
        .get(2)
        .cloned()
        .or_else(|| env::var("WMBT_KV_WAL_LAYOUT_AUDIT_ARTIFACT_ROOT").ok())
        .unwrap_or_else(|| {
            "/tmp/wombatkv-bench/wal-path-shape-epoch-partition-alignment".to_string()
        });
    let backend = env::var("WMBT_KV_WAL_LAYOUT_AUDIT_BACKEND")
        .unwrap_or_else(|_| "inmemory-fixture".to_string());

    let keys = match load_keys(&backend, &namespace_prefix) {
        Ok(keys) => keys,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let counts = audit_wal_layout_counts(&namespace_prefix, &keys);
    let strict_pass = strict_pass(&backend, counts);

    if let Err(error) = fs::create_dir_all(&output_dir) {
        eprintln!("failed to create output dir `{output_dir}`: {error}");
        return ExitCode::from(2);
    }

    let report_json_path = format!("{output_dir}/layout-audit-report.json");
    let report_md_path = format!("{output_dir}/layout-audit-report.md");
    let report_json = serde_json::json!({
        "schema_version": 1,
        "target": "wal-path-shape-epoch-partition-alignment",
        "backend": backend,
        "namespace_prefix": namespace_prefix,
        "strict_pass": strict_pass,
        "counts": {
            "total_keys": counts.total_keys,
            "canonical_keys": counts.canonical_keys,
            "legacy_keys": counts.legacy_keys,
            "unknown_keys": counts.unknown_keys
        },
        "sample_keys": keys.into_iter().take(10).collect::<Vec<_>>()
    });
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
        "# WAL Path Shape Audit\n\n\
- backend: {backend}\n\
- namespace_prefix: {namespace_prefix}\n\
- strict_pass: {strict_pass}\n\
- total_keys: {}\n\
- canonical_keys: {}\n\
- legacy_keys: {}\n\
- unknown_keys: {}\n",
        counts.total_keys, counts.canonical_keys, counts.legacy_keys, counts.unknown_keys
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

fn load_keys(backend: &str, namespace_prefix: &str) -> Result<Vec<String>, String> {
    match backend {
        "inmemory-fixture" => Ok(fixture_keys(namespace_prefix)),
        "s3" => load_s3_keys(namespace_prefix),
        other => Err(format!(
            "unsupported WAL_LAYOUT_AUDIT_BACKEND `{other}`; expected `inmemory-fixture` or `s3`"
        )),
    }
}

fn load_s3_keys(namespace_prefix: &str) -> Result<Vec<String>, String> {
    let store = S3ObjectStore::from_env()
        .map_err(|error| format!("failed to build S3ObjectStore from env: {error:?}"))?;
    store.ensure_bucket().map_err(format_wal_store_error)?;
    store.list_prefix(namespace_prefix).map_err(format_wal_store_error)
}

fn format_wal_store_error(error: WalStoreError) -> String {
    format!("s3 namespace listing failed: {error:?}")
}

fn fixture_keys(namespace_prefix: &str) -> Vec<String> {
    vec![
        format!("{namespace_prefix}/00000000000170000000/chunk-node-a-00000000000000000009.bin"),
        format!("{namespace_prefix}/00000000000170000001/chunk-node-a-00000000000000000010.bin"),
        format!("{namespace_prefix}/chunk-node-a-00000000000000000007.bin"),
        format!("{namespace_prefix}/chunk-00000000000000000003.bin"),
        "wal/other-tenant/fp/chunk-node-a-00000000000000000001.bin".to_string(),
    ]
}

fn strict_pass(backend: &str, counts: WalLayoutAuditCounts) -> bool {
    if counts.total_keys == 0 || counts.canonical_keys == 0 || counts.unknown_keys != 0 {
        return false;
    }
    if backend == "inmemory-fixture" && counts.legacy_keys == 0 {
        return false;
    }
    true
}
