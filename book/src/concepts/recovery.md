# Recovery protocol

What happens when something breaks, and what WombatKV does about it
automatically vs what the operator has to do manually.

**Companion to:** [`consistency.md`](./consistency.md) (what's guaranteed
when nothing breaks).

---

## Failure classes covered by DST

WombatKV's DST harness (`crates/wombatkv-dst/`) exercises 16
deterministic failure classes against the substrate. The same classes
form this document's "what we tested" baseline.

| Class | Trigger | WombatKV behavior |
|---|---|---|
| TransientS3Failure | S3 GET returns 503 / throttle | Retry per `S3ErrorKind::is_retryable`; surface clean error after retry budget |
| CorruptBlockBytes | S3 returns bytes with bad BLAKE3 chain hash | Chain-hash verification rejects; warm-restore does NOT install corrupt KV |
| PartialChainSave | Crash between block PUT and chain-head PUT | Next bootstrap ignores orphan blocks; LOOKUP returns the last fully-published chain tip |
| ConcurrentSameKeySave | Two PUTs of same content-hash | Both succeed; content-addressing makes them idempotent |
| DaemonRestartMidLookup | Daemon restart during embedded-client lookup | Client retries against new daemon or surfaces DaemonUnavailable; no orphan-state panic |
| SidecarDriftAfterChain | Chain head publishes but sidecar PUT fails | Chain is visible without compressor frontier; engine recomputes frontier from chain bytes |
| FoyerEvictionMidGet | LRU evicts a block mid-lease | Lease's Arc keeps bytes alive past eviction (use-after-free safe) |
| TransportConnectionDropMidRPC | TCP/HTTP daemon connection dropped mid-RPC | Client reconnects cleanly or surfaces DaemonUnavailable |
| TransportPartialReadOnHeader | Daemon socket returns fewer bytes than requested | Accumulator-buffer loop re-reads; no off-by-one decode |
| TransportSlowWrite | Per-byte stalls on daemon socket write | Client-side timeout discipline; no head-of-line indefinite block |
| WireEnvelopeCorruption | Daemon ships bad-magic/version/CRC/oversized/truncated envelope | Client rejects cleanly, no state corruption |
| OldSidecarV3InBucket | Bucket has pre-RFC-0018 sidecar from prior alpha | Loader rejects with clear operator hint ("wipe sidecar bucket and re-save") |
| OldBlockV1InBucket | Bucket has pre-RFC-0018 block v1 from prior alpha | Same, clear hint, never load corrupt KV |
| SlateDbWriteFailure | L1 metadata put fails | Daemon surfaces error or falls back to S3-truth; never silently drops |
| SlateDbManifestCorruption | SlateDB on-disk manifest corrupt at startup | Daemon fails loudly with operator-actionable hint; never starts with empty index against populated bucket |
| MultiEnginePrefixConflict | Two engines claim same SHM prefix | Daemon serializes or refuses cleanly |

---

## What WombatKV does automatically

### On daemon startup

1. Validates the SHM segment-name budget (macOS POSIX SHM 31-char cap).
   Fail-loud if a prefix would overflow.
2. Builds `S3ObjectStoreConfig` from env (`WMBT_KV_BUCKET` required;
   no AWS_* fallback by design).
3. Opens (or creates) SlateDB at `WMBT_KV_SLATEDB_PATH`. Failure to
   open is logged + daemon continues with empty L1 (next read
   re-hydrates from S3-truth).
4. Bootstraps L0 radix from L1 SlateDB (~25ms for 27 blocks). If L1
   is empty, lazy-bootstrap from S3 on first lookup.
5. Spawns the optional LRU eviction worker
   (`WMBT_KV_NAMESPACE_MAX_BYTES`).

### On RPC delivery

1. Wire envelope: validates magic + version + CRC32C in
   `decode_envelope`. Mismatch → reject + close connection
   (drop-on-bad-frame semantics).
2. Block envelope (KVB1 v2): validates magic + version + body CRC32C
   + length. Mismatch → reject, do not install KV.
3. Sidecar envelope (RTT1 v4): same: CRC + version + magic checked.
4. Chain hash: BLAKE3-20 over `(parent, content_hash)` validates
   restore chain integrity.

### On daemon shutdown

1. SIGTERM/SIGINT/SIGHUP installs flips the shutdown flag.
2. Per-prefix worker loops poll the flag between request batches.
3. In-flight requests are dispatched + ACK'd before exit.
4. Async-PUT drain budget: `SHUTDOWN_DRAIN_TIMEOUT` (10s default;
   see `crates/wombatkv-daemon/src/constants.rs`).
5. SHM segments are `shm_unlink`'d on the way out.
6. SlateDB index closed.
7. Main joins on each worker.

If the drain exceeds 10s, the daemon logs "drain timeout" and exits
anyway; the operator can SIGKILL if a worker is permanently stuck.

---

## What the operator has to do manually

### "Bucket has stale v3 sidecars / v1 blocks from a prior alpha"

Wipe the bucket + restart. The alpha breaking-window policy doesn't
maintain back-compat readers.
Daemon will refuse to start against a mixed-version bucket and
print:

> ds4_session_install_raw_tail: unsupported sidecar version (alpha
> breaking-window: v3 envelopes don't load under v4; wipe sidecar
> bucket and re-save)

Action:
```bash
aws s3 rm s3://$WMBT_KV_BUCKET --recursive   # or mc rm --recursive
# Restart daemon; it will re-populate as engines write new state.
```

### "SlateDB manifest is corrupt"

Daemon logs the corruption + exits with non-zero status. Action:

```bash
# Move corrupt manifest aside for inspection
mv $WMBT_KV_SLATEDB_PATH ${WMBT_KV_SLATEDB_PATH}.corrupt-$(date +%s)
# Restart daemon; it will re-hydrate L1 from S3 (slower first
# bootstrap, ~minutes for large buckets, sub-second thereafter).
```

### "S3 bucket is unavailable"

Daemon retries per `S3ObjectStoreConfig` retry policy
(`WMBT_KV_S3_GET_RETRIES`). After retry budget, surfaces error to
caller. The engine sees the error and falls back to its own cold
prefill. WombatKV does not silently mask S3 unavailability as
"warm restore worked."

### "Daemon was killed in the middle of a multi-block PUT"

Next daemon startup ignores orphan blocks. The engine sees a
shorter chain than it last published; it must re-extend the chain
from its current tip. Idempotent retries via content-addressing
make this safe.

### "Two engines on same SHM prefix"

Daemon refuses the second attach. Operator must either:
- Use a different prefix per engine (`--prefix engineA --prefix engineB`),
  or
- Let the first engine finish + detach, then the second attaches.

### "Cross-host: host B's daemon doesn't see host A's writes"

This is the "cross-host RYW is eventual" gap documented in
`CONSISTENCY.md`. Today the fix is to restart host B's daemon
(re-bootstraps L1 from S3). Post-RFC 0019 (HELLO handshake), this
will be automatic.

---

## What the operator can monitor

### Health signals to watch

- Daemon stderr `wombatkv_shm_daemon` JSON events:
  - `event:"starting"`, daemon ready
  - `event:"ready_remote_only"`: TCP/HTTP listeners ready
  - `event:"shard_listening"`, per-shard compio bind succeeded
  - `event:"worker_exit_shutdown"`, clean shutdown
  - `event:"shard_error"`, listener crashed; daemon may have lost
    a transport

- `wombatkv-daemon[slatedb]: hydrated N blocks`: L1 bootstrap done
- `wombatkv-daemon[lru]: starting eviction worker`, eviction
  enabled
- `wombatkv-daemon: failed to open store: InvalidConfig(...)`, env
  config wrong; daemon exits

### Things that should NEVER appear

- `assertion failed`: DST coverage should catch these before
  production
- `panic`: `catch_unwind` boundaries in cabi prevent C-side
  propagation, but daemon panics indicate a logic bug; please
  capture a backtrace + file an issue

---

## DR strategy

S3 is the source of truth for WombatKV. The recommended
production posture:

1. **Bucket versioning + MFA-delete:** protects against
   accidental rm. AWS S3 + most S3-compatible stores support
   this natively.
2. **Cross-region replication:** for tier-1 availability beyond
   one region. Configure via S3 Replication Rules; WombatKV
   does not orchestrate this.
3. **Periodic layout audit:** `wombatkv-wal-layout-audit` walks
   the bucket and verifies key-shape invariants. Run weekly in
   prod as a tripwire for drift.
4. **Periodic restore test:** point a staging engine at a
   read-only mirror of the prod bucket once a month. If warm
   restore works, your DR strategy is real.

WombatKV does NOT provide:
- Backup orchestration (use AWS Backup / s3-batch-operations)
- Point-in-time recovery beyond S3 versioning
- Snapshot consistency across multiple buckets

---

## References

- `book/src/concepts/consistency.md`, formal consistency model
- `book/src/operations/dst.md`: DST primitives + failure classes
- RFC 0018, wire envelope discipline (CRC32C / version / magic)
- RFC 0019 (sketched, not implemented), cross-host HELLO handshake
- `crates/wombatkv-dst/src/fault.rs`, full fault enum + scheduler
