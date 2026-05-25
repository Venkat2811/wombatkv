# Quickstart: four modes

WombatKV has four end-to-end deployment modes. Pick one based on your
topology; minimum env vars per mode below.

## Mode 1. Embedded (in-process)

Engine links `libwombatkv.{so,dylib}` directly. Foyer cache + S3 client
both live in the engine process.

```sh
# Required:
export WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000
export WMBT_KV_S3_ACCESS_KEY=minioadmin
export WMBT_KV_S3_SECRET_KEY=minioadmin
export WMBT_KV_BUCKET=wombatkv-demo

# Engine-specific gate (ds4 example):
export DS4_WOMBATKV_ENABLE=1
ds4-server --model my-model.gguf
```

## Mode 2. Daemon-SHM (same host)

A `wombatkv-daemon` process serves engines on the same host over a
myelon disruptor SHM ring. Useful when multiple engines on one box
share one S3-backed cache.

```sh
# Daemon side:
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
WMBT_KV_S3_ACCESS_KEY=minioadmin WMBT_KV_S3_SECRET_KEY=minioadmin \
WMBT_KV_BUCKET=wombatkv-demo \
  wombatkv-daemon --prefix engine0

# Engine side:
export WMBT_KV_REMOTE_PREFIX=engine0
ds4-server --model my-model.gguf
```

## Mode 3. Daemon-TCP (cross-host)

Engine on machine A, daemon on machine B, length-prefixed rkyv frames
over TCP.

```sh
# Daemon side (machine B):
WMBT_KV_S3_ENDPOINT=http://127.0.0.1:9000 \
WMBT_KV_S3_ACCESS_KEY=minioadmin WMBT_KV_S3_SECRET_KEY=minioadmin \
WMBT_KV_BUCKET=wombatkv-demo \
  wombatkv-daemon --tcp 0.0.0.0:7878

# Engine side (machine A):
export DS4_WOMBATKV_DAEMON_TCP=machine-b.local:7878
ds4-server --model my-model.gguf
```

## Mode 4. Daemon-HTTP (cross-host via proxy)

Same wire envelope as TCP, wrapped in HTTP/1.1 POSTs. Use when the
link is fronted by an HTTP-aware proxy / load balancer.

```sh
# Daemon side:
wombatkv-daemon --http 0.0.0.0:7879

# Engine side:
export DS4_WOMBATKV_DAEMON_HTTP=machine-b.local:7879
ds4-server --model my-model.gguf
```

## Common to all modes

| env var | required | default |
|---|---|---|
| `WMBT_KV_S3_ENDPOINT` | yes | none |
| `WMBT_KV_S3_ACCESS_KEY` | yes | none |
| `WMBT_KV_S3_SECRET_KEY` | yes | none |
| `WMBT_KV_BUCKET` | yes | none (errors if unset) |
| `WMBT_KV_NAMESPACE` | no | `"default"` |
| `WMBT_KV_PUFFER_DIR` | no | `~/.wombatkv/puffer` |
| `WMBT_KV_FINGERPRINT24` | depends on engine | derived from model path |

See [Environment variables](../operations/env.md) for the full
inventory (78 env vars across 4 tiers).
