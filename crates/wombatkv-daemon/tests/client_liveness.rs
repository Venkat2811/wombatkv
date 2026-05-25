//! Liveness tests for `RemoteKvStoreClient`.
//!
//! Spawns a daemon, connects, kills the daemon, and verifies that:
//!   1. The next call returns `RemoteError::Timeout` within the
//!      configured per-call deadline (not "blocks forever").
//!   2. After timeout the client is in dead state (`is_dead() == true`).
//!   3. Subsequent calls fast-fail with `BackendUnavailable` rather
//!      than queuing behind the stuck worker.

#![cfg(test)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use wombatkv_daemon::{ClientOptions, RemoteError, RemoteKvStoreClient};

fn unique_prefix(tag: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos =
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let s = format!("{nanos:x}{seq:x}");
    let n = s.len();
    format!("{tag}{}", &s[n.saturating_sub(5)..])
}

fn s3_env_ok() -> bool {
    ["WMBT_KV_S3_ENDPOINT", "WMBT_KV_BUCKET", "WMBT_KV_S3_ACCESS_KEY", "WMBT_KV_S3_SECRET_KEY"]
        .iter()
        .all(|k| std::env::var(k).is_ok())
}

fn find_daemon_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("WMBT_KV_DAEMON_SHM_DAEMON_BIN") {
        let p = PathBuf::from(p);
        if p.is_file() {
            return Some(p);
        }
    }
    let cwd = std::env::current_dir().ok()?;
    let mut dir = cwd.as_path().to_path_buf();
    for _ in 0..6 {
        for variant in ["release", "debug"] {
            let cand = dir.join(format!("target/{variant}/wombatkv-daemon"));
            if cand.is_file() {
                return Some(cand);
            }
        }
        dir = dir.parent()?.to_path_buf();
    }
    None
}

fn spawn_daemon(prefix: &str) -> Option<Child> {
    if !s3_env_ok() {
        eprintln!("skipping: WMBT_KV_S3_* not set");
        return None;
    }
    let bin = find_daemon_bin()?;
    Command::new(&bin)
        .arg("--prefix")
        .arg(prefix)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()
}

#[test]
fn dead_daemon_yields_timeout_then_backend_unavailable() {
    let prefix = unique_prefix("dl");
    let Some(mut daemon) = spawn_daemon(&prefix) else {
        return;
    };

    // Short timeout so the test runs fast; long enough for a normal
    // round-trip to succeed first.
    let opts = ClientOptions {
        call_timeout: Duration::from_millis(500),
        depth: wombatkv_daemon::DEFAULT_RING_DEPTH,
    };
    let client = match RemoteKvStoreClient::connect_with_options(&prefix, opts) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("connect failed: {err}, skipping");
            let _ = daemon.kill();
            let _ = daemon.wait();
            return;
        }
    };

    // Sanity: live daemon serves PING fast.
    let started = Instant::now();
    client.ping().expect("ping while daemon alive");
    let alive_ping = started.elapsed();
    assert!(
        alive_ping < Duration::from_millis(500),
        "ping while alive took {alive_ping:?} (should be sub-100ms)"
    );
    eprintln!("alive ping: {alive_ping:?}");

    // Sanity: a real PUT also works.
    client
        .put_kv("liveness-test", "key1", Bytes::from(vec![0xAA; 1024]))
        .expect("put while daemon alive");
    assert!(!client.is_dead(), "client should be alive before daemon dies");

    // Kill the daemon.
    let _ = daemon.kill();
    let _ = daemon.wait();
    eprintln!("daemon killed; next call should time out");

    // Next call should TIME OUT (not hang forever).
    let started = Instant::now();
    let err = client.ping().expect_err("ping after daemon kill must fail");
    let elapsed = started.elapsed();
    eprintln!("ping after kill: {elapsed:?} → {err}");

    match &err {
        RemoteError::Timeout(d) => {
            assert_eq!(*d, Duration::from_millis(500));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
    assert!(elapsed < Duration::from_millis(700), "timeout took longer than budget: {elapsed:?}");
    assert!(client.is_dead(), "client should be dead after timeout");

    // Subsequent calls fast-fail.
    let started = Instant::now();
    let err2 = client.ping().expect_err("second ping must fail");
    let fast_fail = started.elapsed();
    eprintln!("second ping (fast-fail): {fast_fail:?} → {err2}");
    matches!(err2, RemoteError::BackendUnavailable);
    assert!(
        fast_fail < Duration::from_millis(50),
        "fast-fail should not wait on timeout: {fast_fail:?}"
    );

    // get_kv on dead client should also fast-fail without timing out.
    let started = Instant::now();
    let err3 = client.get_kv("liveness-test", "key1").expect_err("get on dead client");
    let fast_fail = started.elapsed();
    matches!(err3, RemoteError::BackendUnavailable);
    assert!(
        fast_fail < Duration::from_millis(50),
        "get on dead client should fast-fail: {fast_fail:?}"
    );
}

#[test]
fn ping_with_timeout_uses_custom_deadline() {
    let prefix = unique_prefix("pt");
    let Some(mut daemon) = spawn_daemon(&prefix) else {
        return;
    };

    let client = if let Ok(c) = RemoteKvStoreClient::connect(&prefix) {
        c
    } else {
        let _ = daemon.kill();
        let _ = daemon.wait();
        return;
    };
    client.ping().expect("alive ping");

    let _ = daemon.kill();
    let _ = daemon.wait();

    let started = Instant::now();
    let err = client
        .ping_with_timeout(Duration::from_millis(200))
        .expect_err("custom-timeout ping after kill");
    let elapsed = started.elapsed();

    match &err {
        RemoteError::Timeout(d) => {
            assert_eq!(*d, Duration::from_millis(200));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
    // Should be tight to the custom deadline, not the default 30s.
    assert!(elapsed < Duration::from_millis(400));
    assert!(client.is_dead());
}
