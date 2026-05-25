# Security Policy

## Reporting

Report suspected vulnerabilities by emailing vraman2811@gmail.com. Include the affected crate name (wombatkv-cabi, wombatkv-daemon, etc.), crate version, reproduction steps, and an impact assessment.

## Response Expectations

The project will make a best-effort acknowledgement within a few business days. There is no formal SLA and no bug bounty currently.

## Supported Versions

Security fixes target the latest 0.1.0-alpha.x line. Older alpha releases are not patched.

## Disclosure

Coordinated disclosure is preferred. The default disclosure window is 90 days unless a different timeline is agreed during triage.

## What Counts As Security

Examples relevant to WombatKV:

- Memory safety issues in `wombatkv-cabi` (the C ABI surface engines link against), including borrowed-pointer lifetime violations that could let an engine read freed memory.
- Data leakage across logical namespaces (`WMBT_KV_NAMESPACE` boundary), one tenant's blocks observable by another.
- Daemon credential mishandling, leakage of `WMBT_KV_S3_*` credentials in logs, error messages, or to wire peers.
- Denial of service through crafted wire frames (RFC 0018 envelope corruption that crashes the daemon, exhausts memory, or hangs a shard).
- SHM-segment hijacking (one process attaches to another's segment and reads/corrupts blocks).
- S3 bucket-key collisions across model fingerprints that allow one engine's blocks to be served as another's.
