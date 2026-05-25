# wombatkv-daemon

Daemon-mode transports for WombatKV. Spawn one `wombatkv-daemon`
process; serve many engines over SHM (same host) + TCP (cross host)
+ HTTP/1.1 (cross host, load-balancer friendly).

## Modules

| module | what |
|---|---|
| `arena` | shared memory-mapped arena for the future zero-copy data plane |
| `lifecycle` | SHM-prefix server + heartbeat monitor + reopen-on-restart |
| `client` | `RemoteKvStoreClient` (SHM client) |
| `tcp_transport` | compio TPC server (SO_REUSEPORT + dispatch bridge) + `TcpKvClient` |
| `http_transport` | compio TPC server (SO_REUSEPORT + dispatch bridge) + `HttpKvClient` |
| `envelope` | RFC 0018 universal wire envelope: magic + version + CRC32C + len |
| `runtime_tpc` | per-shard compio runtime spawn + CPU pinning |

## CLI

```sh
wombatkv-daemon \
    --prefix <name>      # SHM prefix (repeatable for multiple engines)
    --tcp <addr>         # TCP listener (repeatable)
    --http <addr>        # HTTP listener (repeatable, --tpc                # opt-in: per-shard compio runtime for SHM
```

Or via env: `WMBT_KV_TCP=<a,b,c>` / `WMBT_KV_HTTP=<a,b,c>` /
`WMBT_KV_TCP_TPC_THREADS=N` (default 2) /
`WMBT_KV_HTTP_TPC_THREADS=N` (default 2) /
`WMBT_KV_TCP_DISPATCH_WORKERS=M` (default 8) /
`WMBT_KV_HTTP_DISPATCH_WORKERS=M` (default 8).

## TPC architecture

The compio-bridge variant (`serve_tcp_compio_bridge` /
`serve_http_compio_bridge`) shards the accept path with SO_REUSEPORT
across N OS threads each running a compio runtime, and ferries
decoded `WireRequest` frames to a separate sync worker pool via
`DispatchHandle` (flume). The dispatch closure is shared with the
SHM path so all transports land in one foyer + S3 backend.

Mirrors a prior playground's `compio_tcp.rs` + `transports/bridge.rs`.

## Wire format (RFC 0018 universal envelope, ```text
+---------+---------+---------+---------+--------------------+
| magic 4 | ver 4LE | crc 4LE | len 4LE | rkyv body[len] ... |
| 'WMBT'  | u32     | u32     | u32     |                    |
+---------+---------+---------+---------+--------------------+
```

CRC32C is Castagnoli (polynomial 0x82F63B78), same algorithm as
ds4's v4 sidecar + v2 block envelopes. Single CRC32C across the
stack.

## Test

```sh
# Lib tests (envelope + 10 wire corruption tests + transport
# concurrent-client correctness + TPC keep-alive pipelined):
cargo test -p wombatkv-daemon --release

# DST chaos build:
cargo build -p wombatkv-daemon --release --features dst
```

## RFC pointers

- RFC 0014, cabi TCP extension (TCP transport + ds4 env gate)
- RFC 0018, wire envelope discipline (universal 16-byte envelope)
