#![forbid(unsafe_code)]

pub mod segment_store;
pub mod wal_store;

/// Returns crate identity for smoke tests.
#[must_use]
pub fn crate_id() -> &'static str {
    "wombatkv-store"
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_id_is_stable() {
        assert_eq!(super::crate_id(), "wombatkv-store");
    }
}
