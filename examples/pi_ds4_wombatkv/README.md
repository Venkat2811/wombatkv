# pi-ds4 + WombatKV

Run [`pi`](https://pi.dev) with [`ds4`](https://github.com/antirez/ds4) as
the inference engine, with WombatKV as the cache system. No npm package,
no fork of pi-ds4, no plugin. Five environment variables and a Docker
image.

## Why

`pi-ds4` (mitsuhiko's pi extension for ds4) already exposes three config knobs
that make WombatKV integration zero-plugin:

| pi-ds4 config | What it does |
|---|---|
| `DS4_SUPPORT_REPO` | Repo pi-ds4 clones to build `ds4-server` |
| `DS4_SUPPORT_BRANCH` | Branch to check out before building |
| `DS4_SERVER_BINARY` | Path to a pre-built `ds4-server` binary; skips clone+build entirely |

Plus: pi-ds4 spawns `ds4-server` inheriting your `process.env`, so any
`DS4_WOMBATKV_*` or `WMBT_KV_*` env vars flow through automatically.

Result: WombatKV's value (cross-restart durability, cross-machine prefix
sharing) layers under `pi` without a plugin layer.

## Quickstart (5 minutes)

### Prerequisites

- `pi` installed (see <https://pi.dev>)
- Docker (for the pre-built `wombatkv/ds4` image), OR a local `wombatkv` checkout
- A MinIO or S3-compatible bucket. Local MinIO is fine:
  ```sh
  docker run -d -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minio -e MINIO_ROOT_PASSWORD=minio123 \
    --name minio quay.io/minio/minio server /data --console-address ":9001"
  ```

### Option B (recommended for first run), pre-built binary

```sh
# 1. Pull the wombatkv-enabled ds4-server binary.
./setup.sh

# 2. In your shell (or ~/.zshrc):
export DS4_SERVER_BINARY="$HOME/.wombatkv/bin/ds4-server"
export DS4_WOMBATKV_ENABLE=1
export WMBT_KV_BUCKET="wombatkv-${USER}"
export WMBT_KV_S3_ENDPOINT="http://localhost:9000"
export WMBT_KV_S3_ACCESS_KEY="minio"
export WMBT_KV_S3_SECRET_KEY="minio123"

# 3. Run pi. It'll spawn ds4-server with WombatKV.
pi
```

### Option A, let pi-ds4 build our fork

```sh
export DS4_SUPPORT_REPO="https://github.com/Venkat2811/ds4"
export DS4_SUPPORT_BRANCH="main"
export DS4_WOMBATKV_ENABLE=1
export WMBT_KV_BUCKET="wombatkv-${USER}"
export WMBT_KV_S3_ENDPOINT="http://localhost:9000"
export WMBT_KV_S3_ACCESS_KEY="minio"
export WMBT_KV_S3_SECRET_KEY="minio123"

pi
```

Note: Option A requires our fork's `Makefile` to default `WOMBATKV=1` and
resolve a `WOMBATKV_DIR` path automatically. If pi-ds4's build fails with
`WOMBATKV=1 requires WOMBATKV_DIR=<path>`, fall back to Option B until the
Makefile auto-discover lands.

## What you get

- Same-process warm: zero overhead vs pi-ds4 alone.
- Quit pi, restart, ask the same prompt: served from the local L1 cache.
- Quit pi, wipe `~/.pi/ds4/kv`, ask the same prompt: restored from the S3 bucket.
- Switch machines (same `WMBT_KV_BUCKET`): restored from the S3 bucket.
- A teammate with the same bucket inherits your prefill.

For headline numbers see the [top-level README](../../README.md). For
a one-machine cross-teammate demo, see `docker-compose.yml`.

## Verifying it works

```sh
# After running `pi` once with the env vars above, check the bucket has KV blocks.
mc alias set local http://localhost:9000 minio minio123
mc ls --recursive local/wombatkv-${USER}/wombatkv/v1/

# You should see entries like:
#   wombatkv/v1/<model-fingerprint>/blocks/<chain-hash>
```

If the bucket is empty after a `pi` session, the env didn't reach `ds4-server`.
Check:

```sh
ps aux | grep ds4-server
# look at the binary path, does it match $DS4_SERVER_BINARY?
# check env(1) on the running process to see if DS4_WOMBATKV_ENABLE is set.
cat /proc/$(pgrep ds4-server)/environ | tr '\0' '\n' | grep -E 'WOMBATKV|WMBT_KV'
```

## Configuration reference

The 5 env vars above are the minimum. See
[`book/src/operations/env.md`](../../book/src/operations/env.md) for the
full tuning surface.

## Acknowledgements

- [`mitsuhiko/pi-ds4`](https://github.com/mitsuhiko/pi-ds4): the pi extension and the three config knobs that make this zero-plugin.
- [`antirez/ds4`](https://github.com/antirez/ds4): the engine.
- [`pi.dev`](https://pi.dev): the harness.
- [`ovg-project/kvcached`](https://github.com/ovg-project/kvcached): the examples-not-plugins pattern.
