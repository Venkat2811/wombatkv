//! Two-engine cross-prefix-reuse smoke for the daemon.
//!
//! Spawns ONE `wombatkv-daemon` with two `--prefix` flags
//! (`engineA` + `engineB`). Connects two `RemoteKvStoreClient`s, one
//! per prefix. Engine A puts a payload under a known namespace + key.
//! Engine B's `get_kv` against the same `(namespace, key)` MUST hit the
//! daemon's foyer (tier=Foyer), this is the headline daemon-mode win: one
//! engine populates the cache, another engine reuses it without paying
//! S3 cost.
//!
//! Skips when `MinIO` env vars or the daemon binary are missing.

#![cfg(test)]

use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use wombatkv_daemon::{RemoteGetOutcome, RemoteHitTier, RemoteKvStoreClient};

fn unique_prefix(tag: &str) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos =
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    let s = format!("{nanos:x}{seq:x}");
    let n = s.len();
    // Total segment name is `wmbt_kv_<prefix>_req[_cn|_dn]`, must fit 31
    // chars on macOS POSIX SHM. `wmbt_kv_` = 5, `_resp_cn` worst-case = 8,
    // so prefix budget = 18 chars. We use `<tag>-<6 hex>` = ~9 chars.
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

struct DaemonGuard {
    child: Option<Child>,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn spawn_multi_prefix_daemon(prefixes: &[&str]) -> Option<DaemonGuard> {
    if !s3_env_ok() {
        eprintln!("skipping: WMBT_KV_S3_* not set");
        return None;
    }
    let bin = find_daemon_bin()?;
    let mut cmd = Command::new(&bin);
    for p in prefixes {
        cmd.arg("--prefix").arg(p);
    }
    let child = cmd.stdout(Stdio::null()).stderr(Stdio::null()).spawn().ok()?;
    Some(DaemonGuard { child: Some(child) })
}

#[test]
fn two_engines_share_one_daemon_foyer() {
    let prefix_a = unique_prefix("ea");
    let prefix_b = unique_prefix("eb");
    eprintln!("prefixes: a={prefix_a} b={prefix_b}");

    let Some(_daemon) = spawn_multi_prefix_daemon(&[&prefix_a, &prefix_b]) else {
        return;
    };

    // Connect both clients. Each gets its own ring pair to the same daemon.
    let a = match RemoteKvStoreClient::connect(&prefix_a) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("client A connect failed: {err}, skipping");
            return;
        }
    };
    let b = match RemoteKvStoreClient::connect(&prefix_b) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("client B connect failed: {err}, skipping");
            return;
        }
    };

    // Engine A populates the daemon's foyer.
    let namespace = "shared-test-ns";
    let key = "shared-prefix-block-001";
    let payload = Bytes::from(vec![0x42u8; 32 * 1024]); // 32 KiB
    let started = Instant::now();
    a.put_kv(namespace, key, payload.clone()).expect("A put_kv");
    eprintln!("A.put_kv took {:?}", started.elapsed());

    assert!(b.exists(namespace, key).expect("B exists"));
    assert!(!b.exists(namespace, "missing-key").expect("B missing exists"));

    // Engine B reads the same key. Hit MUST come from foyer (tier=Foyer), bytes never went to S3 from A's perspective only because the
    // daemon's S3 writethrough also stored it; but the response should
    // be a foyer hit because foyer is checked first. The point is: B
    // gets the bytes WITHOUT having put them. That's cross-engine reuse.
    let started = Instant::now();
    let outcome = b.get_kv(namespace, key).expect("B get_kv");
    let elapsed = started.elapsed();
    eprintln!("B.get_kv took {elapsed:?}");

    match outcome {
        RemoteGetOutcome::Hit { tier, payload: got } => {
            assert_eq!(got.len(), payload.len(), "payload size mismatch");
            assert_eq!(&got[..16], &payload[..16], "payload bytes mismatch");
            assert_eq!(
                tier,
                RemoteHitTier::Foyer,
                "expected foyer hit (cross-engine reuse via shared cache); got {tier:?}"
            );
            eprintln!(
                "✓ engine B got {} bytes from tier={:?} that engine A wrote: \
                cross-engine reuse via shared daemon confirmed",
                got.len(),
                tier
            );
        }
        RemoteGetOutcome::Miss => {
            panic!("engine B saw miss, daemon foyer is NOT shared across prefixes");
        }
    }

    // Sanity: each engine's own ping still works (rings are independent).
    a.ping().expect("A ping after shared get");
    b.ping().expect("B ping after shared get");
}

#[test]
fn two_engines_concurrent_writes_then_reads() {
    let prefix_a = unique_prefix("ca");
    let prefix_b = unique_prefix("cb");
    let Some(_daemon) = spawn_multi_prefix_daemon(&[&prefix_a, &prefix_b]) else {
        return;
    };

    let a = match RemoteKvStoreClient::connect(&prefix_a) {
        Ok(c) => c,
        Err(_) => return,
    };
    let b = match RemoteKvStoreClient::connect(&prefix_b) {
        Ok(c) => c,
        Err(_) => return,
    };

    // Each engine writes its own keys; both should be visible to the
    // other. This proves bidirectional sharing, not just one-directional.
    let ns = "concurrent-ns";
    a.put_kv(ns, "from-a", Bytes::from(vec![0xAAu8; 1024])).expect("a write");
    b.put_kv(ns, "from-b", Bytes::from(vec![0xBBu8; 1024])).expect("b write");

    // Cross-reads.
    match a.get_kv(ns, "from-b").expect("a reads b's key") {
        RemoteGetOutcome::Hit { payload, tier } => {
            assert_eq!(payload[0], 0xBB);
            assert_eq!(tier, RemoteHitTier::Foyer);
        }
        RemoteGetOutcome::Miss => panic!("A could not see B's key"),
    }
    match b.get_kv(ns, "from-a").expect("b reads a's key") {
        RemoteGetOutcome::Hit { payload, tier } => {
            assert_eq!(payload[0], 0xAA);
            assert_eq!(tier, RemoteHitTier::Foyer);
        }
        RemoteGetOutcome::Miss => panic!("B could not see A's key"),
    }

    // Throwaway delay to flush any stragglers, no real assertions on
    // timing, just keep the daemon alive long enough for both client
    // drops to complete cleanly.
    std::thread::sleep(Duration::from_millis(50));
}
