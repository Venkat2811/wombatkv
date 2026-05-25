//! Integration test for the `--tpc` + TCP-only-deployment fail-loud guarantee.
//!
//! Background: when `wombatkv-daemon` is started with `--tpc` (per-shard
//! compio runtime for the SHM listener) but there's no SHM client
//! co-located on the daemon host, the canonical case being a cross-host
//! TCP-only deployment, where the engine is on a different machine, the
//! SHM TPC shard tries to attach to `wk<prefix>r` / `wk<prefix>s` shared
//! segments that never get created (because there's no local engine to
//! create them). After MAX_OPEN_RETRIES outer × inner disruptor-mp attach
//! retries, the daemon MUST fail-loud per RFC 0011 P10 with the documented
//! error string. It MUST NOT silently retry forever or hang.
//!
//! Regression caught during cross-host validation: `wombatkv-daemon --tpc
//! --tcp 0.0.0.0:7878` served Cell-B tuned cross-host (6.2×) + bit-parity
//! (L∞=0) for ~10 min, then the SHM TPC shard exhausted its retry budget
//! and the daemon exited. The fail-loud surface this test pins ensures
//! the failure mode is observable rather than silent.
//!
//! ## Why integration test (vs DST)
//!
//! The DST harness today does not spawn the real daemon binary, the
//! `wombatkv_dst_runner_child.rs` doc-block ("What this is NOT (yet)")
//! says the child runner drives an in-process oracle + stand-in
//! `BTreeMap`, not the actual daemon. This bug is at the daemon-binary
//! startup-orchestration level, so it needs the real binary. A DST class
//! `DaemonTpcOnlyNoShmClient` is registered as captured intent for when
//! DST grows real-daemon-driving; this integration test is what catches
//! the regression today.
//!
//! ## Test variants
//!
//! 1. `tpc_tcp_only_emits_open_retry_within_10s`, production
//!    `MAX_OPEN_RETRIES=20` (default). Verifies the buggy code path
//!    is engaged by waiting for the documented `open_retry` event in
//!    stdout within 10 seconds. Catches regressions where the SHM
//!    shard stops trying to attach when `--tpc` is set.
//! 2. `tpc_tcp_only_emits_shard_error_within_90s`, sets
//!    `WMBT_KV_DAEMON_MAX_OPEN_RETRIES=2` (added) so the
//!    fail-loud sequence completes in <90s. Verifies the full
//!    documented RFC 0011 P10 error fragments are emitted. Note: the
//!    daemon process does NOT exit when the SHM shard dies, by
//!    design, SHM and TCP/HTTP listeners are independent paths, so
//!    the daemon keeps serving TCP clients with a broken SHM shard.
//!    The "loud" in fail-loud is about operator-actionable error
//!    surfacing, not process termination. This test also doubles as
//!    a regression test for the `WMBT_KV_DAEMON_MAX_OPEN_RETRIES`
//!    env override itself (the message must include the configured
//!    value, e.g. `"failed 2 times"`).

#![cfg(test)]

use std::io::Read;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

fn unique_port_offset() -> u16 {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    (seq % 4000) as u16
}

fn unique_prefix() -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos =
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let s = format!("{nanos:x}{seq:x}");
    let n = s.len();
    format!("xt{}", &s[n.saturating_sub(6)..])
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

fn s3_env_ok() -> bool {
    ["WMBT_KV_S3_ENDPOINT", "WMBT_KV_BUCKET", "WMBT_KV_S3_ACCESS_KEY", "WMBT_KV_S3_SECRET_KEY"]
        .iter()
        .all(|k| std::env::var(k).is_ok())
}

fn spawn_tpc_tcp_only(prefix: &str, tcp_port: u16, max_open_retries: u32) -> Option<Child> {
    if !s3_env_ok() {
        eprintln!(
            "SKIP: WMBT_KV_S3_ENDPOINT / WMBT_KV_BUCKET / WMBT_KV_S3_{{ACCESS,SECRET}}_KEY not set"
        );
        return None;
    }
    let Some(bin) = find_daemon_bin() else {
        eprintln!("SKIP: wombatkv-daemon binary not found under target/{{release,debug}}");
        return None;
    };
    let puffer = std::env::temp_dir().join(format!("wmbtkv-tpc-test-{prefix}"));
    let _ = std::fs::create_dir_all(&puffer);
    Command::new(&bin)
        .arg("--prefix")
        .arg(prefix)
        .arg("--tpc")
        .arg("--tcp")
        .arg(format!("127.0.0.1:{tcp_port}"))
        .env("WMBT_KV_PUFFER_DIR", &puffer)
        // Tighten the fail-loud window so the long-form test runs in CI.
        // Production default is 20; this test uses 2 so the daemon
        // exits within ~30 seconds even with inner disruptor-mp attach
        // retries factored in.
        .env("WMBT_KV_DAEMON_MAX_OPEN_RETRIES", max_open_retries.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::null())
        .spawn()
        .ok()
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

/// Read everything available from a child's stdout into a buffer
/// (non-blocking-ish, uses small reads with a deadline). Used to scan
/// for documented log markers without waiting on EOF.
fn drain_stream_until(
    stream: &mut impl Read,
    needle: &str,
    deadline: Instant,
    buf: &mut String,
) -> bool {
    let mut chunk = [0u8; 4096];
    while Instant::now() < deadline {
        match stream.read(&mut chunk) {
            Ok(0) => return buf.contains(needle),
            Ok(n) => {
                buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
                if buf.contains(needle) {
                    return true;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(_) => return buf.contains(needle),
        }
    }
    buf.contains(needle)
}

#[test]
fn tpc_tcp_only_emits_open_retry_within_10s() {
    let prefix = unique_prefix();
    let port = 17878u16 + unique_port_offset();
    // Use production-default max_open_retries=20 here, we're only
    // looking for the first open_retry event, not the fail-loud exit.
    let Some(mut child) = spawn_tpc_tcp_only(&prefix, port, 20) else {
        return;
    };
    // Stdout carries the daemon's JSONL events (the source code uses
    // println!, not eprintln!). Move stdout out so we can scan it.
    let mut stdout = child.stdout.take().expect("piped stdout");
    let _guard = ChildGuard(Some(child));

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut buf = String::new();
    let saw_open_retry = drain_stream_until(&mut stdout, "\"open_retry\"", deadline, &mut buf);
    assert!(
        saw_open_retry,
        "expected `\"open_retry\"` event in daemon stdout within 10s when running --tpc \
         in TCP-only mode (no co-located SHM client). Got stdout (first 4 KB):\n---\n{}\n---",
        &buf.chars().take(4096).collect::<String>(),
    );
    // The event should be tagged with our prefix.
    assert!(
        buf.contains(&format!("\"prefix\":\"{prefix}\"")),
        "open_retry event present but not tagged with prefix `{prefix}` (regression in SHM \
         shard naming?). stdout:\n{buf}"
    );
}

/// Wait for the SHM TPC shard's fail-loud `shard_error` event with the
/// documented RFC 0011 P10 error fragments. Uses
/// `WMBT_KV_DAEMON_MAX_OPEN_RETRIES=2` (added) so the fail-loud
/// window closes in <90s, fast enough for CI.
///
/// The daemon process does NOT exit when the SHM shard dies, by design,
/// SHM and TCP/HTTP listeners are independent paths, and the daemon keeps
/// serving TCP clients with a broken SHM shard. This test asserts the
/// loud-error surfacing, not process termination.
///
/// Also doubles as a regression test for the env override: the emitted
/// error must include the configured retry count (`"failed 2 times"`).
#[test]
fn tpc_tcp_only_emits_shard_error_within_90s() {
    let prefix = unique_prefix();
    let port = 18078u16 + unique_port_offset();
    let Some(mut child) = spawn_tpc_tcp_only(&prefix, port, 2) else {
        return;
    };
    // `shard_error` is written to stderr (eprintln!) at
    // wombatkv-daemon.rs:786, not stdout, drain stderr for the
    // assertion. The open_retry events on stdout are independently
    // covered by the other test.
    let mut stderr = child.stderr.take().expect("piped stderr");
    let _guard = ChildGuard(Some(child));

    let deadline = Instant::now() + Duration::from_secs(90);
    let mut buf = String::new();

    // Use the "failed N times" message-body fragment as the needle:
    // it's deeper inside the shard_error JSON line than the
    // `"shard_error"` event-name marker, so by the time we see it we
    // know the full message (event-name + retry count + RFC 0011 P10
    // hint) is in the buffer. The "2" doubles as a regression test
    // for the WMBT_KV_DAEMON_MAX_OPEN_RETRIES env override itself.
    let saw_full = drain_stream_until(&mut stderr, "failed 2 times", deadline, &mut buf);
    assert!(
        saw_full,
        "expected `\"shard_error\"` event with body `failed 2 times` in daemon stderr \
         within 90s. WMBT_KV_DAEMON_MAX_OPEN_RETRIES=2 was set; either the env override \
         is broken or the SHM TPC shard's fail-loud sequence regressed. \
         Got stderr (first 4 KB):\n---\n{}\n---",
        &buf.chars().take(4096).collect::<String>(),
    );

    assert!(
        buf.contains("\"shard_error\""),
        "saw the `failed 2 times` body fragment but no preceding `\"shard_error\"` event \
         marker, the event-name JSON shape may have regressed. stderr:\n{buf}"
    );

    // The shard_error message body must include the RFC 0011 P10
    // documented hint so operators get an actionable message.
    let hint_markers = ["Shared segment not found", "open_daemon for prefix", "RFC 0011 P10"];
    let hint_hit = hint_markers.iter().filter(|m| buf.contains(*m)).count();
    assert!(
        hint_hit >= 2,
        "shard_error fired but expected ≥2 of the documented RFC 0011 P10 hint markers \
         in the message; saw {hint_hit}. Markers checked: {hint_markers:?}. stderr:\n{buf}",
    );
}
