#![deny(unsafe_code)]
//! C ABI for `WombatKV`'s KV store. Companion to
//! `include/wombatkv.h`.
//!
//! Build: `cargo build -p wombatkv-cabi --release` produces
//! `target/release/libwombatkv.dylib` (macOS) or `.so` (Linux).
//!
//! All extern "C" functions are wrapped in `catch_unwind` so a Rust
//! panic on the puffer side never propagates into the C caller.
//!
//! All pointer-handling unsafe blocks are isolated to the `ffi`
//! submodule with audit comments. The rest of this crate is safe code.

pub mod config;
mod ffi;
pub use config::CabiConfig;
pub use ffi::*;
