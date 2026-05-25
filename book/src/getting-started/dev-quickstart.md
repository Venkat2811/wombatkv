# Dev environment

Local development bring-up for the WombatKV workspace.

## Prerequisites

- Rust toolchain matching [`rust-toolchain.toml`](../../../rust-toolchain.toml)
  (auto-installed by `rustup`)
- Docker (for the local MinIO loopback the integration tests use)
- macOS or Linux x86_64 (the two CI lanes)

## Bring up local MinIO

```sh
docker run -d --name minio-wombatkv -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
    -v $HOME/.minio-wombatkv:/data \
    quay.io/minio/minio server /data --console-address ":9001"
```

The `minioadmin/minioadmin` credentials are MinIO's documented dev
defaults; never use them outside loopback.

## Set env vars

Copy [`.env.example`](../../../.env.example) and source it:

```sh
cp .env.example .env
# edit WMBT_KV_BUCKET, WMBT_KV_S3_ENDPOINT if needed
source .env
```

The full env-var reference lives in [`../operations/env.md`](../operations/env.md).

## Build + test

```sh
make ci                  # fmt + clippy + lib tests + DST sweep + drift detectors
./scripts/dst-sweep.sh   # fault-injection sweep, all classes × 10 seeds
```

To run the live-MinIO integration tests against the loopback bucket:

```sh
make store-minio-integration
```

These tests require `WMBT_KV_BUCKET` + `WMBT_KV_S3_*` credentials set
in the shell environment; they auto-skip if any required var is missing.

## Hot-loop edits

```sh
cargo test -p wombatkv-node              # iterate on the high-level Rust API
cargo test -p wombatkv-cabi              # iterate on the C ABI surface
cargo test -p wombatkv-daemon            # iterate on the daemon transports
cargo run -p wombatkv-daemon -- --tcp 0.0.0.0:7878  # spin up the daemon
```

See [`../../../CONTRIBUTING.md`](../../../CONTRIBUTING.md) for the
contributor lifecycle.
