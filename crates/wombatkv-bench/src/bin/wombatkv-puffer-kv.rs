#![forbid(unsafe_code)]
//! In-process smoke utility for the WombatKV puffer cache.
//!
//! The puffer cache (RAM + on-disk hybrid, internally backed by
//! foyer-rs) holds its working set in the process address space;
//! the on-disk tier persists individual blocks but the index is
//! rebuilt only inside the running process. That means
//! **subprocess invocations do not share state** -
//! `wombatkv-puffer-kv put` followed by a separate
//! `wombatkv-puffer-kv get` invocation will miss because the
//! second process opens a fresh cache.
//!
//! The intended deployment is library-mode: an inference engine
//! imports `wombatkv-node` as a Cargo dependency and holds a
//! single puffer-cache instance for its lifetime. ds4 uses this
//! pattern via the C ABI surface (libwombatkv.dylib).
//!
//! This binary is provided for:
//!   - one-shot `roundtrip` smoke tests (put + get in the same process)
//!   - manual `stats` / `clear` invocations against an existing on-disk dir
//!
//! Usage:
//! ```text
//!   wombatkv-puffer-kv roundtrip <key> [--from <path>]  # put + get in one process
//!   wombatkv-puffer-kv put <key> [--from <path>]        # in-process only
//!   wombatkv-puffer-kv get <key> [--to <path>]          # in-process only
//!   wombatkv-puffer-kv stats
//!   wombatkv-puffer-kv clear
//! ```
//!
//! Environment:
//!   `WMBT_KV_PUFFER_RAM_BYTES`         RAM-tier budget (default 1 GiB)
//!   `WMBT_KV_PUFFER_DIR`               on-disk tier dir (default /tmp/wombatkv-puffer)
//!   `WMBT_KV_PUFFER_DISK_BYTES`        on-disk tier budget (default 8 GiB)
//!   `WMBT_KV_PUFFER_BLOCK_SIZE_BYTES`  block size (default 64 MiB)
//!   `WMBT_KV_PUFFER_IOURING`           "1"/"0": `io_uring` on Linux

use std::env;
use std::fs;
use std::io::{Read, Write};
use std::process::ExitCode;

use bytes::Bytes;
use wombatkv_node::foyer_cache::{config_from_env, FoyerHybridCache};

fn main() -> ExitCode {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.is_empty() {
        return usage();
    }

    let (cmd, rest) = args.split_first().expect("checked above");
    let result = match cmd.as_str() {
        "roundtrip" => run_roundtrip(rest),
        "put" => run_put(rest),
        "get" => run_get(rest),
        "stats" => run_stats(rest),
        "clear" => run_clear(rest),
        "--help" | "-h" => return usage(),
        other => Err(format!("unknown subcommand: {other}")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("wombatkv-puffer-kv: {message}");
            ExitCode::FAILURE
        }
    }
}

fn usage() -> ExitCode {
    eprintln!(
        "usage: wombatkv-puffer-kv roundtrip <key> [--from <path>]\n\
                wombatkv-puffer-kv put <key> [--from <path>]   # in-process only\n\
                wombatkv-puffer-kv get <key> [--to <path>]     # in-process only\n\
                wombatkv-puffer-kv stats\n\
                wombatkv-puffer-kv clear"
    );
    ExitCode::FAILURE
}

fn run_roundtrip(args: &[String]) -> Result<(), String> {
    let mut iter = args.iter();
    let key = iter.next().ok_or_else(|| "roundtrip requires <key>".to_string())?.clone();
    let mut from_path: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--from" => {
                from_path =
                    Some(iter.next().ok_or_else(|| "--from requires a path".to_string())?.clone());
            }
            other => return Err(format!("unexpected roundtrip arg: {other}")),
        }
    }

    let payload = if let Some(path) = from_path {
        fs::read(&path).map_err(|err| format!("read {path}: {err}"))?
    } else {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf).map_err(|err| format!("read stdin: {err}"))?;
        buf
    };
    let original_bytes = payload.len();

    let cache = open_cache()?;
    cache.put(&key, Bytes::from(payload.clone()));
    let fetched =
        cache.get(&key).ok_or_else(|| "warm-hit miss; puffer cache returned None".to_string())?;

    if fetched.as_ref() != payload.as_slice() {
        return Err(format!(
            "byte mismatch: put {original_bytes} bytes, got {} bytes back",
            fetched.len()
        ));
    }

    println!(
        "{{\"key\":\"{}\",\"bytes\":{},\"hits\":{},\"misses\":{},\"inserts\":{}}}",
        key,
        original_bytes,
        cache.hits(),
        cache.misses(),
        cache.inserts()
    );
    Ok(())
}

fn open_cache() -> Result<std::sync::Arc<FoyerHybridCache>, String> {
    let cfg = config_from_env();
    FoyerHybridCache::open(cfg).map_err(|err| format!("puffer cache open failed: {err}"))
}

fn run_put(args: &[String]) -> Result<(), String> {
    let mut iter = args.iter();
    let key = iter.next().ok_or_else(|| "put requires <key>".to_string())?.clone();
    let mut from_path: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--from" => {
                from_path =
                    Some(iter.next().ok_or_else(|| "--from requires a path".to_string())?.clone());
            }
            other => return Err(format!("unexpected put arg: {other}")),
        }
    }

    let payload = if let Some(path) = from_path {
        fs::read(&path).map_err(|err| format!("read {path}: {err}"))?
    } else {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf).map_err(|err| format!("read stdin: {err}"))?;
        buf
    };

    let cache = open_cache()?;
    cache.put(&key, Bytes::from(payload));
    Ok(())
}

fn run_get(args: &[String]) -> Result<(), String> {
    let mut iter = args.iter();
    let key = iter.next().ok_or_else(|| "get requires <key>".to_string())?.clone();
    let mut to_path: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--to" => {
                to_path =
                    Some(iter.next().ok_or_else(|| "--to requires a path".to_string())?.clone());
            }
            other => return Err(format!("unexpected get arg: {other}")),
        }
    }

    let cache = open_cache()?;
    let payload = cache.get(&key).ok_or_else(|| format!("no entry for key {key}"))?;

    match to_path {
        Some(path) => {
            fs::write(&path, &payload).map_err(|err| format!("write {path}: {err}"))?;
        }
        None => {
            std::io::stdout().write_all(&payload).map_err(|err| format!("write stdout: {err}"))?;
        }
    }
    Ok(())
}

fn run_stats(args: &[String]) -> Result<(), String> {
    if !args.is_empty() {
        return Err("stats takes no arguments".to_string());
    }
    let cache = open_cache()?;
    println!(
        "{{\"hits\":{},\"misses\":{},\"inserts\":{},\"ssd_dir\":\"{}\"}}",
        cache.hits(),
        cache.misses(),
        cache.inserts(),
        cache.ssd_dir().display()
    );
    Ok(())
}

fn run_clear(args: &[String]) -> Result<(), String> {
    if !args.is_empty() {
        return Err("clear takes no arguments".to_string());
    }
    let cache = open_cache()?;
    cache.clear();
    Ok(())
}
