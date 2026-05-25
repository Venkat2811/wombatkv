# wombatkv-store

S3-backed object store + WAL primitives. The durable backstop tier
of WombatKV's storage hierarchy.

## What lives here

- `segment_store`, append-only segment writer for the WAL.
- `wal_store`: S3 ObjectStore wrapper with retry logic, MD5-etag
  conditional PUT, and write-replay sequencing.

Forbids `unsafe_code`.

## Stack position

```
wombatkv-store     ← you are here (durable tier)
  ↓ used by
wombatkv-node      (embedded KV)
  ↓ used by
wombatkv-cabi      (C ABI surface) + wombatkv-daemon (TCP/HTTP/SHM server)
```

## Test

```sh
# Pure unit tests:
cargo test -p wombatkv-store --release

# Live MinIO round-trip (requires minio on 127.0.0.1:9200):
cargo test -p wombatkv-store --release -- s3_minio
```

The live-MinIO tests cover real S3 conditional PUT, etag mismatch
retry, WAL replay, and the default-dev-credentials guardrail.
